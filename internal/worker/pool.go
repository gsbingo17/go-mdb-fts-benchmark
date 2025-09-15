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
	readers       []*ReadWorker
	writers       []*WriteWorker
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	rateLimiter   *rate.Limiter
	config        config.WorkloadConfig
	metrics       *metrics.MetricsCollector
	database      database.Database
	dataGenerator *generator.DataGenerator
	workloadGen   *generator.WorkloadGenerator
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
		ctx:           ctx,
		cancel:        cancel,
		rateLimiter:   rateLimiter,
		config:        cfg,
		metrics:       metricsCollector,
		database:      db,
		dataGenerator: generator.NewDataGenerator(time.Now().UnixNano()),
		workloadGen:   generator.NewWorkloadGenerator(time.Now().UnixNano()),
		readers:       make([]*ReadWorker, 0),
		writers:       make([]*WriteWorker, 0),
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

	// Start all workers
	for _, worker := range wp.readers {
		wp.wg.Add(1)
		go func(w *ReadWorker) {
			defer wp.wg.Done()
			w.Start(wp.ctx)
		}(worker)
	}

	for _, worker := range wp.writers {
		wp.wg.Add(1)
		go func(w *WriteWorker) {
			defer wp.wg.Done()
			w.Start(wp.ctx)
		}(worker)
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

		wp.wg.Add(1)
		go func(w *ReadWorker) {
			defer wp.wg.Done()
			w.Start(wp.ctx)
		}(worker)
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

		wp.wg.Add(1)
		go func(w *WriteWorker) {
			defer wp.wg.Done()
			w.Start(wp.ctx)
		}(worker)
	}

	return nil
}

// scaleDown removes workers (simplified implementation)
func (wp *WorkerPool) scaleDown(workersToRemove int) error {
	// For simplicity, we'll just mark this as a TODO
	// In production, you'd implement proper worker shutdown
	// TODO: Implement worker shutdown and removal
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
