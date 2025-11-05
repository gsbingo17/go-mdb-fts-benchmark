package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
	readerWGs      []*sync.WaitGroup
	writerWGs      []*sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	
	// Operation counting (atomic counters)
	targetReadOps     int64
	targetWriteOps    int64
	completedReadOps  int64
	completedWriteOps int64
	completionChan    chan struct{}
	
	config         config.WorkloadConfig
	metrics        *metrics.MetricsCollector
	database       database.Database
	dataGenerator  *generator.DataGenerator
	workloadGen    *generator.WorkloadGenerator
	queryLimit     int
	benchmarkMode  string
	mu             sync.RWMutex
	started        bool
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(
	cfg config.WorkloadConfig,
	db database.Database,
	metricsCollector *metrics.MetricsCollector,
) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		ctx:            ctx,
		cancel:         cancel,
		completionChan: make(chan struct{}),
		config:         cfg,
		metrics:        metricsCollector,
		database:       db,
		dataGenerator:  generator.NewDataGenerator(time.Now().UnixNano()),
		workloadGen:    generator.NewWorkloadGenerator(time.Now().UnixNano()),
		queryLimit:     cfg.QueryResultLimit,
		benchmarkMode:  cfg.BenchmarkMode,
		readers:        make([]*ReadWorker, 0),
		writers:        make([]*WriteWorker, 0),
		readerContexts: make([]context.CancelFunc, 0),
		writerContexts: make([]context.CancelFunc, 0),
		readerWGs:      make([]*sync.WaitGroup, 0),
		writerWGs:      make([]*sync.WaitGroup, 0),
	}
}

// Start initializes and starts all workers
func (wp *WorkerPool) Start() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// Prevent double initialization
	if wp.started {
		slog.Info("Worker pool already started, skipping duplicate Start() call")
		return nil
	}

	wp.started = true
	slog.Info("Starting worker pool", "total_workers", wp.config.WorkerCount)

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

	// Create read workers with unique workload generators for cache reduction
	for i := 0; i < readWorkerCount; i++ {
		// CACHE REDUCTION: Each worker gets unique workload generator with different seed
		workerWorkloadGen := generator.NewWorkloadGenerator(time.Now().UnixNano() + int64(i)*1000000)
		worker := NewReadWorker(
			i,
			wp.database,
			workerWorkloadGen,
			wp.metrics,
			wp,
			wp.queryLimit,
			wp.benchmarkMode,
		)
		wp.readers = append(wp.readers, worker)
	}

	// Create write workers
	for i := 0; i < writeWorkerCount; i++ {
		// Create a unique data generator for each worker to avoid race conditions
		workerDataGen := generator.NewDataGenerator(time.Now().UnixNano() + int64(i))
		worker := NewWriteWorker(
			i,
			wp.database,
			workerDataGen,
			wp.metrics,
			wp,
		)
		wp.writers = append(wp.writers, worker)
	}

	// Start all workers with individual contexts and waitgroups
	for _, worker := range wp.readers {
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.readerContexts = append(wp.readerContexts, cancel)

		// Create individual waitgroup for this worker
		workerWG := &sync.WaitGroup{}
		wp.readerWGs = append(wp.readerWGs, workerWG)

		wp.wg.Add(1)
		workerWG.Add(1)
		go func(w *ReadWorker, ctx context.Context, wg *sync.WaitGroup) {
			defer wp.wg.Done()
			defer wg.Done()
			w.Start(ctx)
		}(worker, workerCtx, workerWG)
	}

	for _, worker := range wp.writers {
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.writerContexts = append(wp.writerContexts, cancel)

		// Create individual waitgroup for this worker
		workerWG := &sync.WaitGroup{}
		wp.writerWGs = append(wp.writerWGs, workerWG)

		wp.wg.Add(1)
		workerWG.Add(1)
		go func(w *WriteWorker, ctx context.Context, wg *sync.WaitGroup) {
			defer wp.wg.Done()
			defer wg.Done()
			w.Start(ctx)
		}(worker, workerCtx, workerWG)
	}

	return nil
}

// Stop gracefully stops all workers
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	wp.cancel()
	wp.started = false // Reset started flag so pool can be restarted
	wp.mu.Unlock()
	
	wp.wg.Wait()
	
	// Create new context for next start
	wp.ctx, wp.cancel = context.WithCancel(context.Background())
	
	// Clear worker slices for fresh start
	wp.mu.Lock()
	wp.readers = make([]*ReadWorker, 0)
	wp.writers = make([]*WriteWorker, 0)
	wp.readerContexts = make([]context.CancelFunc, 0)
	wp.writerContexts = make([]context.CancelFunc, 0)
	wp.readerWGs = make([]*sync.WaitGroup, 0)
	wp.writerWGs = make([]*sync.WaitGroup, 0)
	wp.wg = sync.WaitGroup{}
	wp.mu.Unlock()
}

// StartWithTarget starts the worker pool with specific operation targets
func (wp *WorkerPool) StartWithTarget(readOps, writeOps int64) error {
	// Set operation targets atomically (must use atomic since workers read atomically)
	atomic.StoreInt64(&wp.targetReadOps, readOps)
	atomic.StoreInt64(&wp.targetWriteOps, writeOps)
	atomic.StoreInt64(&wp.completedReadOps, 0)
	atomic.StoreInt64(&wp.completedWriteOps, 0)
	
	// Create new completion channel for this run
	wp.completionChan = make(chan struct{})
	
	// Start workers
	if err := wp.Start(); err != nil {
		return err
	}
	
	// Start monitoring for completion
	go wp.monitorCompletion()
	
	return nil
}

// monitorCompletion checks if all operations are complete and signals via channel
func (wp *WorkerPool) monitorCompletion() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			completedReads := atomic.LoadInt64(&wp.completedReadOps)
			completedWrites := atomic.LoadInt64(&wp.completedWriteOps)
			targetReads := atomic.LoadInt64(&wp.targetReadOps)
			targetWrites := atomic.LoadInt64(&wp.targetWriteOps)
			
			if completedReads >= targetReads && completedWrites >= targetWrites {
				// Signal completion
				select {
				case wp.completionChan <- struct{}{}:
					slog.Info("All operations completed",
						"read_ops", completedReads,
						"write_ops", completedWrites,
						"total_ops", completedReads+completedWrites)
				default:
					// Channel already signaled
				}
				return
			}
		}
	}
}

// CompletionSignal returns a channel that signals when all operations are complete
func (wp *WorkerPool) CompletionSignal() <-chan struct{} {
	return wp.completionChan
}

// GetProgress returns current progress toward operation targets
func (wp *WorkerPool) GetProgress() (completedReads, completedWrites, targetReads, targetWrites int64) {
	return atomic.LoadInt64(&wp.completedReadOps), 
	       atomic.LoadInt64(&wp.completedWriteOps), 
	       atomic.LoadInt64(&wp.targetReadOps), 
	       atomic.LoadInt64(&wp.targetWriteOps)
}

// ScaleWorkers is deprecated - not used in operation-count mode
func (wp *WorkerPool) ScaleWorkers(newWorkerCount int) error {
	slog.Warn("ScaleWorkers called but not supported in operation-count mode")
	return nil
}

// UpdateRateLimit is deprecated - not used in operation-count mode
func (wp *WorkerPool) UpdateRateLimit(newQPS int) {
	slog.Warn("UpdateRateLimit called but not supported in operation-count mode")
}

// GetWorkerStatus returns current worker status
func (wp *WorkerPool) GetWorkerStatus() WorkerStatus {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	readWorkers := len(wp.readers)
	writeWorkers := len(wp.writers)
	isRunning := wp.ctx.Err() == nil

	return WorkerStatus{
		ReadWorkers:  readWorkers,
		WriteWorkers: writeWorkers,
		TotalWorkers: readWorkers + writeWorkers,
		IsRunning:    isRunning,
	}
}

// WorkerStatus represents the current state of the worker pool
type WorkerStatus struct {
	ReadWorkers  int  `json:"read_workers"`
	WriteWorkers int  `json:"write_workers"`
	TotalWorkers int  `json:"total_workers"`
	IsRunning    bool `json:"is_running"`
}
