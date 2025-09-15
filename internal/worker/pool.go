package worker

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"mongodb-benchmarking-tool/internal/config"
	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/generator"
	"mongodb-benchmarking-tool/internal/metrics"
)

// WorkerPool manages a pool of read and write workers
type WorkerPool struct {
	readers        []*ReadWorker
	writers        []*WriteWorker
	readerContexts []context.CancelFunc
	writerContexts []context.CancelFunc
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	rateLimiter    *rate.Limiter
	config         config.WorkloadConfig
	metrics        *metrics.MetricsCollector
	database       database.Database
	dataGenerator  *generator.DataGenerator
	workloadGen    *generator.WorkloadGenerator
	mu             sync.RWMutex
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(
	cfg config.WorkloadConfig,
	db database.Database,
	metricsCollector *metrics.MetricsCollector,
) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	// Create rate limiter for QPS control
	rateLimiter := rate.NewLimiter(rate.Limit(cfg.TargetQPS), cfg.TargetQPS/10)

	return &WorkerPool{
		ctx:            ctx,
		cancel:         cancel,
		rateLimiter:    rateLimiter,
		config:         cfg,
		metrics:        metricsCollector,
		database:       db,
		dataGenerator:  generator.NewDataGenerator(time.Now().UnixNano()),
		workloadGen:    generator.NewWorkloadGenerator(time.Now().UnixNano()),
		readers:        make([]*ReadWorker, 0),
		writers:        make([]*WriteWorker, 0),
		readerContexts: make([]context.CancelFunc, 0),
		writerContexts: make([]context.CancelFunc, 0),
	}
}

// Start initializes and starts all workers
func (wp *WorkerPool) Start() error {
	// Calculate worker distribution based on read/write ratio
	readWorkerCount := int(float64(wp.config.WorkerCount) * float64(wp.config.ReadWriteRatio.ReadPercent) / 100.0)
	writeWorkerCount := wp.config.WorkerCount - readWorkerCount

	// Ensure at least one worker of each type
	if readWorkerCount == 0 && wp.config.ReadWriteRatio.ReadPercent > 0 {
		readWorkerCount = 1
		writeWorkerCount--
	}
	if writeWorkerCount == 0 && wp.config.ReadWriteRatio.WritePercent > 0 {
		writeWorkerCount = 1
		readWorkerCount--
	}

	// Create read workers
	for i := 0; i < readWorkerCount; i++ {
		worker := NewReadWorker(
			i,
			wp.database,
			wp.workloadGen,
			wp.metrics,
			wp.rateLimiter,
			wp.config.QueryResultLimit,
		)
		wp.readers = append(wp.readers, worker)
	}

	// Create write workers
	for i := 0; i < writeWorkerCount; i++ {
		worker := NewWriteWorker(
			i,
			wp.database,
			wp.dataGenerator,
			wp.metrics,
			wp.rateLimiter,
		)
		wp.writers = append(wp.writers, worker)
	}

	// Start all workers with individual contexts
	for _, worker := range wp.readers {
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.readerContexts = append(wp.readerContexts, cancel)

		wp.wg.Add(1)
		go func(w *ReadWorker, ctx context.Context) {
			defer wp.wg.Done()
			w.Start(ctx)
		}(worker, workerCtx)
	}

	for _, worker := range wp.writers {
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.writerContexts = append(wp.writerContexts, cancel)

		wp.wg.Add(1)
		go func(w *WriteWorker, ctx context.Context) {
			defer wp.wg.Done()
			w.Start(ctx)
		}(worker, workerCtx)
	}

	return nil
}

// Stop gracefully stops all workers
func (wp *WorkerPool) Stop() {
	wp.cancel()
	wp.wg.Wait()
}

// ScaleWorkers dynamically adjusts the number of workers
func (wp *WorkerPool) ScaleWorkers(newWorkerCount int) error {
	currentCount := len(wp.readers) + len(wp.writers)

	if newWorkerCount > currentCount {
		// Scale up
		return wp.scaleUp(newWorkerCount - currentCount)
	} else if newWorkerCount < currentCount {
		// Scale down
		return wp.scaleDown(currentCount - newWorkerCount)
	}

	return nil // No change needed
}

// scaleUp adds more workers
func (wp *WorkerPool) scaleUp(additionalWorkers int) error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	readWorkerCount := int(float64(additionalWorkers) * float64(wp.config.ReadWriteRatio.ReadPercent) / 100.0)
	writeWorkerCount := additionalWorkers - readWorkerCount

	// Add read workers
	for i := 0; i < readWorkerCount; i++ {
		workerID := len(wp.readers) + i
		worker := NewReadWorker(
			workerID,
			wp.database,
			wp.workloadGen,
			wp.metrics,
			wp.rateLimiter,
			wp.config.QueryResultLimit,
		)
		wp.readers = append(wp.readers, worker)

		// Create individual context for this worker
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.readerContexts = append(wp.readerContexts, cancel)

		wp.wg.Add(1)
		go func(w *ReadWorker, ctx context.Context) {
			defer wp.wg.Done()
			w.Start(ctx)
		}(worker, workerCtx)
	}

	// Add write workers
	for i := 0; i < writeWorkerCount; i++ {
		workerID := len(wp.writers) + i
		worker := NewWriteWorker(
			workerID,
			wp.database,
			wp.dataGenerator,
			wp.metrics,
			wp.rateLimiter,
		)
		wp.writers = append(wp.writers, worker)

		// Create individual context for this worker
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.writerContexts = append(wp.writerContexts, cancel)

		wp.wg.Add(1)
		go func(w *WriteWorker, ctx context.Context) {
			defer wp.wg.Done()
			w.Start(ctx)
		}(worker, workerCtx)
	}

	return nil
}

// scaleDown removes workers by gracefully shutting them down
func (wp *WorkerPool) scaleDown(workersToRemove int) error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if workersToRemove <= 0 {
		return nil
	}

	// Calculate how many of each type to remove based on current ratio
	totalWorkers := len(wp.readers) + len(wp.writers)
	if totalWorkers <= workersToRemove {
		return nil // Don't remove all workers
	}

	readWorkersToRemove := int(float64(workersToRemove) * float64(len(wp.readers)) / float64(totalWorkers))
	writeWorkersToRemove := workersToRemove - readWorkersToRemove

	// Remove read workers from the end
	for i := 0; i < readWorkersToRemove && len(wp.readers) > 1; i++ {
		lastIndex := len(wp.readers) - 1

		// Cancel the worker's context to shut it down gracefully
		if lastIndex < len(wp.readerContexts) {
			wp.readerContexts[lastIndex]()
			wp.readerContexts = wp.readerContexts[:lastIndex]
		}

		// Remove from slice
		wp.readers = wp.readers[:lastIndex]
	}

	// Remove write workers from the end
	for i := 0; i < writeWorkersToRemove && len(wp.writers) > 1; i++ {
		lastIndex := len(wp.writers) - 1

		// Cancel the worker's context to shut it down gracefully
		if lastIndex < len(wp.writerContexts) {
			wp.writerContexts[lastIndex]()
			wp.writerContexts = wp.writerContexts[:lastIndex]
		}

		// Remove from slice
		wp.writers = wp.writers[:lastIndex]
	}

	return nil
}

// UpdateRateLimit changes the QPS target
func (wp *WorkerPool) UpdateRateLimit(newQPS int) {
	wp.rateLimiter = rate.NewLimiter(rate.Limit(newQPS), newQPS/10)
	wp.config.TargetQPS = newQPS
}

// GetWorkerStatus returns current worker status
func (wp *WorkerPool) GetWorkerStatus() WorkerStatus {
	return WorkerStatus{
		ReadWorkers:  len(wp.readers),
		WriteWorkers: len(wp.writers),
		TotalWorkers: len(wp.readers) + len(wp.writers),
		TargetQPS:    wp.config.TargetQPS,
		IsRunning:    wp.ctx.Err() == nil,
	}
}

// WorkerStatus represents the current state of the worker pool
type WorkerStatus struct {
	ReadWorkers  int  `json:"read_workers"`
	WriteWorkers int  `json:"write_workers"`
	TotalWorkers int  `json:"total_workers"`
	TargetQPS    int  `json:"target_qps"`
	IsRunning    bool `json:"is_running"`
}
