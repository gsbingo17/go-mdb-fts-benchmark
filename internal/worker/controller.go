package worker

import (
	"log/slog"
	"sync"

	"mongodb-benchmarking-tool/internal/config"
	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/metrics"
)

// WorkloadController manages workload adjustments based on saturation feedback
type WorkloadController struct {
	workerPool *WorkerPool
	config     config.WorkloadConfig

	mu                sync.RWMutex
	adjustmentHistory []metrics.WorkloadAdjustment
}

// NewWorkloadController creates a new workload controller
func NewWorkloadController(
	cfg config.WorkloadConfig,
	db database.Database,
	metricsCollector *metrics.MetricsCollector,
) *WorkloadController {
	workerPool := NewWorkerPool(cfg, db, metricsCollector)

	return &WorkloadController{
		workerPool:        workerPool,
		config:            cfg,
		adjustmentHistory: make([]metrics.WorkloadAdjustment, 0, 100),
	}
}

// Start starts the workload controller
func (wc *WorkloadController) Start() error {
	return wc.workerPool.Start()
}

// Stop stops the workload controller
func (wc *WorkloadController) Stop() {
	wc.workerPool.Stop()
}

// HandleAdjustment processes workload adjustment requests
func (wc *WorkloadController) HandleAdjustment(adjustment metrics.WorkloadAdjustment) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	// Record adjustment history
	wc.adjustmentHistory = append(wc.adjustmentHistory, adjustment)

	// Keep only recent history
	if len(wc.adjustmentHistory) > 50 {
		wc.adjustmentHistory = wc.adjustmentHistory[len(wc.adjustmentHistory)-50:]
	}

	slog.Info("Processing workload adjustment",
		"type", adjustment.Type,
		"magnitude", adjustment.Magnitude,
		"reason", adjustment.Reason)

	switch adjustment.Type {
	case "scale_up":
		wc.scaleUpWorkers(adjustment.Magnitude)
	case "scale_down":
		wc.scaleDownWorkers(adjustment.Magnitude)
	case "qps_increase":
		wc.increaseQPS(adjustment.Magnitude)
	case "qps_decrease":
		wc.decreaseQPS(adjustment.Magnitude)
	default:
		slog.Warn("Unknown adjustment type", "type", adjustment.Type)
	}
}

// scaleUpWorkers increases the number of workers
func (wc *WorkloadController) scaleUpWorkers(percentage float64) {
	currentStatus := wc.workerPool.GetWorkerStatus()

	// Calculate new worker count
	increase := int(float64(currentStatus.TotalWorkers) * percentage / 100.0)
	if increase < 1 {
		increase = 1
	}

	newCount := currentStatus.TotalWorkers + increase

	slog.Info("Scaling up workers",
		"current", currentStatus.TotalWorkers,
		"increase", increase,
		"new_total", newCount)

	if err := wc.workerPool.ScaleWorkers(newCount); err != nil {
		slog.Error("Failed to scale up workers", "error", err)
	}
}

// scaleDownWorkers decreases the number of workers
func (wc *WorkloadController) scaleDownWorkers(percentage float64) {
	currentStatus := wc.workerPool.GetWorkerStatus()

	// Calculate new worker count (ensure minimum of 2 workers)
	decrease := int(float64(currentStatus.TotalWorkers) * percentage / 100.0)
	if decrease < 1 {
		decrease = 1
	}

	newCount := currentStatus.TotalWorkers - decrease
	if newCount < 2 {
		newCount = 2 // Minimum workers
	}

	slog.Info("Scaling down workers",
		"current", currentStatus.TotalWorkers,
		"decrease", decrease,
		"new_total", newCount)

	if err := wc.workerPool.ScaleWorkers(newCount); err != nil {
		slog.Error("Failed to scale down workers", "error", err)
	}
}

// increaseQPS increases the target QPS
func (wc *WorkloadController) increaseQPS(percentage float64) {
	currentStatus := wc.workerPool.GetWorkerStatus()

	// Calculate new QPS
	increase := int(float64(currentStatus.TargetQPS) * percentage / 100.0)
	if increase < 1 {
		increase = 1
	}

	newQPS := currentStatus.TargetQPS + increase

	slog.Info("Increasing QPS",
		"current", currentStatus.TargetQPS,
		"increase", increase,
		"new_qps", newQPS)

	wc.workerPool.UpdateRateLimit(newQPS)
	wc.config.TargetQPS = newQPS
}

// decreaseQPS decreases the target QPS
func (wc *WorkloadController) decreaseQPS(percentage float64) {
	currentStatus := wc.workerPool.GetWorkerStatus()

	// Calculate new QPS (ensure minimum of 10 QPS)
	decrease := int(float64(currentStatus.TargetQPS) * percentage / 100.0)
	if decrease < 1 {
		decrease = 1
	}

	newQPS := currentStatus.TargetQPS - decrease
	if newQPS < 10 {
		newQPS = 10 // Minimum QPS
	}

	slog.Info("Decreasing QPS",
		"current", currentStatus.TargetQPS,
		"decrease", decrease,
		"new_qps", newQPS)

	wc.workerPool.UpdateRateLimit(newQPS)
	wc.config.TargetQPS = newQPS
}

// GetWorkerStatus returns current worker status
func (wc *WorkloadController) GetWorkerStatus() WorkerStatus {
	return wc.workerPool.GetWorkerStatus()
}

// GetAdjustmentHistory returns recent adjustment history
func (wc *WorkloadController) GetAdjustmentHistory() []metrics.WorkloadAdjustment {
	wc.mu.RLock()
	defer wc.mu.RUnlock()

	// Return a copy to avoid race conditions
	history := make([]metrics.WorkloadAdjustment, len(wc.adjustmentHistory))
	copy(history, wc.adjustmentHistory)
	return history
}

// GetWorkerStats returns detailed worker statistics
func (wc *WorkloadController) GetWorkerStats() WorkerPoolStats {
	wc.mu.RLock()
	defer wc.mu.RUnlock()

	readerStats := make([]ReadWorkerStats, len(wc.workerPool.readers))
	for i, reader := range wc.workerPool.readers {
		readerStats[i] = reader.GetStats()
	}

	writerStats := make([]WriteWorkerStats, len(wc.workerPool.writers))
	for i, writer := range wc.workerPool.writers {
		writerStats[i] = writer.GetStats()
	}

	return WorkerPoolStats{
		ReadWorkers:       readerStats,
		WriteWorkers:      writerStats,
		AdjustmentHistory: wc.adjustmentHistory,
		CurrentConfig:     wc.config,
	}
}

// WorkerPoolStats contains comprehensive worker pool statistics
type WorkerPoolStats struct {
	ReadWorkers       []ReadWorkerStats            `json:"read_workers"`
	WriteWorkers      []WriteWorkerStats           `json:"write_workers"`
	AdjustmentHistory []metrics.WorkloadAdjustment `json:"adjustment_history"`
	CurrentConfig     config.WorkloadConfig        `json:"current_config"`
}
