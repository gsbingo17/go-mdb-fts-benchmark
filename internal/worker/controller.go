package worker

import (
	"mongodb-benchmarking-tool/internal/config"
	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/metrics"
)

// WorkloadController manages the worker pool for operation-count benchmarking
type WorkloadController struct {
	workerPool *WorkerPool
	config     config.WorkloadConfig
}

// NewWorkloadController creates a new workload controller
func NewWorkloadController(
	cfg config.WorkloadConfig,
	db database.Database,
	metricsCollector *metrics.MetricsCollector,
) *WorkloadController {
	workerPool := NewWorkerPool(cfg, db, metricsCollector)

	return &WorkloadController{
		workerPool: workerPool,
		config:     cfg,
	}
}

// Start starts the workload controller (deprecated - use StartWithTarget)
func (wc *WorkloadController) Start() error {
	return wc.workerPool.Start()
}

// StartWithTarget starts workers with specific operation targets
func (wc *WorkloadController) StartWithTarget(readOps, writeOps int64) error {
	return wc.workerPool.StartWithTarget(readOps, writeOps)
}

// CompletionSignal returns a channel that signals when all operations are complete
func (wc *WorkloadController) CompletionSignal() <-chan struct{} {
	return wc.workerPool.CompletionSignal()
}

// GetProgress returns current operation progress
func (wc *WorkloadController) GetProgress() (completedReads, completedWrites, targetReads, targetWrites int64) {
	return wc.workerPool.GetProgress()
}

// Stop stops the workload controller
func (wc *WorkloadController) Stop() {
	wc.workerPool.Stop()
}

// GetWorkerStatus returns current worker status
func (wc *WorkloadController) GetWorkerStatus() WorkerStatus {
	return wc.workerPool.GetWorkerStatus()
}
