package worker

import (
	"context"
	"log/slog"
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
	readerWGs      []*sync.WaitGroup
	writerWGs      []*sync.WaitGroup
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
	started        bool
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
	slog.Info("Starting worker pool", "total_workers", wp.config.WorkerCount, "mode", wp.config.Mode)

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

	// Check if we're in cost_model mode
	isCostModelMode := wp.config.Mode == "cost_model"

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

		// Configure for cost_model mode if needed
		if isCostModelMode && len(wp.config.QueryParameters) > 0 {
			// Get base collection name from database
			baseCollection := wp.database.GetConnectionInfo().Collection

			// Pre-generate query shapes if using random queries (matches Java reference implementation)
			var queryParams []config.QueryParameters
			if wp.config.UseRandomQueries && wp.config.RandomTestRuns > 0 {
				// Generate N random query shapes once at startup using fixed seed
				seed := wp.config.RandomQuerySeed
				if seed == "" {
					seed = "quossa" // Default seed for reproducibility
				}
				slog.Info("Pre-generating random query shapes",
					"count", wp.config.RandomTestRuns,
					"seed", seed)
				queryParams = generator.GenerateQueryShapes(seed, wp.config.RandomTestRuns, wp.config.RandomQueryMaxParams)
			} else {
				// Use fixed query shapes from configuration
				queryParams = wp.config.QueryParameters
			}

			// Create QueryRequest with all multi-dimensional parameters
			queryRequest := generator.QueryRequest{
				BaseCollectionName: baseCollection,
				TextShards:         wp.config.TextShards,
				QueryParams:        queryParams, // Now contains either fixed or pre-generated shapes
				Limits:             wp.config.QueryResultLimits,
				UseRandomQueries:   false, // Never generate on-the-fly, always select from queryParams
				RandomMaxParams:    wp.config.RandomQueryMaxParams,
			}

			worker.SetCostModelMode(true, queryRequest)
		}

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
			wp.rateLimiter,
		)

		// Configure for cost_model mode if needed
		if isCostModelMode {
			// Find max textShard value for data distribution
			maxTextShard := 0
			for _, shard := range wp.config.TextShards {
				if shard > maxTextShard {
					maxTextShard = shard
				}
			}
			worker.SetCostModelMode(true, maxTextShard, wp.config.WorkerCount)
		}

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
	wp.cancel()
	wp.wg.Wait()
}

// ScaleWorkers dynamically adjusts the number of workers
func (wp *WorkerPool) ScaleWorkers(newWorkerCount int) error {
	wp.mu.RLock()
	currentCount := len(wp.readers) + len(wp.writers)
	wp.mu.RUnlock()

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
	if additionalWorkers <= 0 {
		return nil
	}

	wp.mu.Lock()
	defer wp.mu.Unlock()

	currentWorkers := len(wp.readers) + len(wp.writers)
	currentQPS := wp.config.TargetQPS
	newWorkerCount := currentWorkers + additionalWorkers

	// Calculate proportional QPS increase
	newQPS := int(float64(currentQPS) * float64(newWorkerCount) / float64(currentWorkers))

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

		// Create individual context and waitgroup for this worker
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.readerContexts = append(wp.readerContexts, cancel)

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

	// Add write workers
	for i := 0; i < writeWorkerCount; i++ {
		workerID := len(wp.writers) + i
		// Create a unique data generator for each worker to avoid race conditions
		workerDataGen := generator.NewDataGenerator(time.Now().UnixNano() + int64(workerID))
		worker := NewWriteWorker(
			workerID,
			wp.database,
			workerDataGen,
			wp.metrics,
			wp.rateLimiter,
		)
		wp.writers = append(wp.writers, worker)

		// Create individual context and waitgroup for this worker
		workerCtx, cancel := context.WithCancel(wp.ctx)
		wp.writerContexts = append(wp.writerContexts, cancel)

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

	// Update rate limiter to increase QPS proportionally
	wp.rateLimiter = rate.NewLimiter(rate.Limit(newQPS), newQPS/10)
	wp.config.TargetQPS = newQPS

	slog.Info("Scale-up completed",
		"readers_added", readWorkerCount,
		"writers_added", writeWorkerCount,
		"total_added", additionalWorkers,
		"total_readers", len(wp.readers),
		"total_writers", len(wp.writers),
		"total_workers", len(wp.readers)+len(wp.writers),
		"old_qps", currentQPS,
		"new_qps", newQPS)

	return nil
}

// scaleDown removes workers by gracefully shutting them down
func (wp *WorkerPool) scaleDown(workersToRemove int) error {
	if workersToRemove <= 0 {
		return nil
	}

	wp.mu.Lock()

	// Calculate how many of each type to remove based on current ratio
	totalWorkers := len(wp.readers) + len(wp.writers)
	if totalWorkers <= workersToRemove {
		wp.mu.Unlock()
		return nil // Don't remove all workers
	}

	currentQPS := wp.config.TargetQPS
	newWorkerCount := totalWorkers - workersToRemove

	// Calculate proportional QPS reduction
	newQPS := int(float64(currentQPS) * float64(newWorkerCount) / float64(totalWorkers))
	if newQPS < 10 {
		newQPS = 10 // Minimum QPS
	}

	readWorkersToRemove := int(float64(workersToRemove) * float64(len(wp.readers)) / float64(totalWorkers))
	writeWorkersToRemove := workersToRemove - readWorkersToRemove

	var workersToWait []*sync.WaitGroup
	var workerTypes []string

	// Remove read workers from the end
	for i := 0; i < readWorkersToRemove && len(wp.readers) > 1; i++ {
		lastIndex := len(wp.readers) - 1

		// Cancel the worker's context to initiate graceful shutdown
		if lastIndex < len(wp.readerContexts) {
			wp.readerContexts[lastIndex]()
		}

		// Collect waitgroup to wait for actual shutdown
		if lastIndex < len(wp.readerWGs) {
			workersToWait = append(workersToWait, wp.readerWGs[lastIndex])
			workerTypes = append(workerTypes, "read")
		}
	}

	// Remove write workers from the end
	for i := 0; i < writeWorkersToRemove && len(wp.writers) > 1; i++ {
		lastIndex := len(wp.writers) - 1

		// Cancel the worker's context to initiate graceful shutdown
		if lastIndex < len(wp.writerContexts) {
			wp.writerContexts[lastIndex]()
		}

		// Collect waitgroup to wait for actual shutdown
		if lastIndex < len(wp.writerWGs) {
			workersToWait = append(workersToWait, wp.writerWGs[lastIndex])
			workerTypes = append(workerTypes, "write")
		}
	}

	wp.mu.Unlock()

	// Wait for workers to actually stop (with timeout)
	shutdownTimeout := 10 * time.Second
	for i, workerWG := range workersToWait {
		done := make(chan struct{})
		go func(wg *sync.WaitGroup) {
			wg.Wait()
			close(done)
		}(workerWG)

		select {
		case <-done:
			// Worker stopped successfully
			slog.Info("Worker stopped successfully", "type", workerTypes[i], "timeout_used", false)
		case <-time.After(shutdownTimeout):
			// Worker didn't stop within timeout
			slog.Warn("Worker shutdown timeout", "type", workerTypes[i], "timeout", shutdownTimeout)
		}
	}

	// Re-acquire mutex to update tracking slices
	wp.mu.Lock()

	// Now remove from tracking slices (workers have actually stopped)
	for i := 0; i < readWorkersToRemove && len(wp.readers) > 1; i++ {
		lastIndex := len(wp.readers) - 1

		if lastIndex < len(wp.readerContexts) {
			wp.readerContexts = wp.readerContexts[:lastIndex]
		}
		if lastIndex < len(wp.readerWGs) {
			wp.readerWGs = wp.readerWGs[:lastIndex]
		}

		wp.readers = wp.readers[:lastIndex]
	}

	for i := 0; i < writeWorkersToRemove && len(wp.writers) > 1; i++ {
		lastIndex := len(wp.writers) - 1

		if lastIndex < len(wp.writerContexts) {
			wp.writerContexts = wp.writerContexts[:lastIndex]
		}
		if lastIndex < len(wp.writerWGs) {
			wp.writerWGs = wp.writerWGs[:lastIndex]
		}

		wp.writers = wp.writers[:lastIndex]
	}

	// Update rate limiter to reduce QPS proportionally
	wp.rateLimiter = rate.NewLimiter(rate.Limit(newQPS), newQPS/10)
	wp.config.TargetQPS = newQPS

	wp.mu.Unlock()

	slog.Info("Scale-down completed",
		"readers_removed", readWorkersToRemove,
		"writers_removed", writeWorkersToRemove,
		"total_removed", readWorkersToRemove+writeWorkersToRemove,
		"remaining_readers", len(wp.readers),
		"remaining_writers", len(wp.writers),
		"total_remaining", len(wp.readers)+len(wp.writers),
		"old_qps", currentQPS,
		"new_qps", newQPS)

	return nil
}

// UpdateRateLimit changes the QPS target
func (wp *WorkerPool) UpdateRateLimit(newQPS int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// Create new rate limiter
	newRateLimiter := rate.NewLimiter(rate.Limit(newQPS), newQPS/10)
	wp.rateLimiter = newRateLimiter
	wp.config.TargetQPS = newQPS

	// Update all existing workers to use the new rate limiter
	for _, reader := range wp.readers {
		reader.rateLimiter = newRateLimiter
	}

	for _, writer := range wp.writers {
		writer.rateLimiter = newRateLimiter
	}

	slog.Info("Rate limit updated",
		"new_qps", newQPS,
		"readers_updated", len(wp.readers),
		"writers_updated", len(wp.writers))
}

// GetWorkerStatus returns current worker status
func (wp *WorkerPool) GetWorkerStatus() WorkerStatus {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	readWorkers := len(wp.readers)
	writeWorkers := len(wp.writers)
	targetQPS := wp.config.TargetQPS
	isRunning := wp.ctx.Err() == nil

	return WorkerStatus{
		ReadWorkers:  readWorkers,
		WriteWorkers: writeWorkers,
		TotalWorkers: readWorkers + writeWorkers,
		TargetQPS:    targetQPS,
		IsRunning:    isRunning,
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
