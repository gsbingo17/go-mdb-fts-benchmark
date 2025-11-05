package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/generator"
	"mongodb-benchmarking-tool/internal/metrics"
)

// WriteWorker performs document insertion operations
type WriteWorker struct {
	id            int
	database      database.Database
	dataGenerator *generator.DataGenerator
	metrics       *metrics.MetricsCollector
	pool          *WorkerPool // Reference to pool for operation tracking
	operationLog  []OperationLog
}

// NewWriteWorker creates a new write worker
func NewWriteWorker(
	id int,
	db database.Database,
	dataGen *generator.DataGenerator,
	metricsCollector *metrics.MetricsCollector,
	pool *WorkerPool,
) *WriteWorker {
	return &WriteWorker{
		id:            id,
		database:      db,
		dataGenerator: dataGen,
		metrics:       metricsCollector,
		pool:          pool,
		operationLog:  make([]OperationLog, 0, 1000),
	}
}

// Start begins the write worker operations
func (ww *WriteWorker) Start(ctx context.Context) {
	slog.Info("Starting write worker", "worker_id", ww.id)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Write worker stopping", "worker_id", ww.id)
			return
		default:
			// Check if we've reached the target operation count
			if atomic.LoadInt64(&ww.pool.completedWriteOps) >= atomic.LoadInt64(&ww.pool.targetWriteOps) {
				slog.Debug("Write target reached, worker stopping", "worker_id", ww.id)
				return
			}

			// Execute document insertion (no rate limiting - run at max speed)
			if err := ww.insertDocument(ctx); err != nil {
				slog.Debug("Document insertion failed", "worker_id", ww.id, "error", err)
			}

			// Increment completed operations counter
			atomic.AddInt64(&ww.pool.completedWriteOps, 1)
		}
	}
}

// insertDocument performs a single document insertion
func (ww *WriteWorker) insertDocument(ctx context.Context) error {
	startTime := time.Now()

	// Generate document
	doc := ww.dataGenerator.GenerateDocument()

	// Insert the document
	err := ww.database.InsertDocument(ctx, doc)

	latency := time.Since(startTime)
	success := err == nil

	// Record metrics
	ww.metrics.RecordWrite(latency, success)

	// Log operation (with size limit)
	if len(ww.operationLog) < cap(ww.operationLog) {
		logEntry := OperationLog{
			Timestamp: startTime,
			Operation: "insert_document",
			Latency:   latency,
			Success:   success,
		}

		if err != nil {
			logEntry.Error = err.Error()
		}

		ww.operationLog = append(ww.operationLog, logEntry)
	}

	// Log insert operations to match read worker behavior
	if success {
		slog.Debug("Successful document insert",
			"worker_id", ww.id,
			"document_title", doc.Title[:min(50, len(doc.Title))],
			"latency_us", latency.Microseconds())
	} else {
		slog.Debug("Failed document insert",
			"worker_id", ww.id,
			"document_title", doc.Title[:min(50, len(doc.Title))],
			"latency_us", latency.Microseconds(),
			"error", err.Error())
	}

	// Log slow operations
	if latency > 500*time.Millisecond {
		slog.Warn("Slow write operation detected",
			"worker_id", ww.id,
			"latency_us", latency.Microseconds(),
			"document_title", doc.Title[:min(50, len(doc.Title))])
	}

	return err
}

// InsertBatch performs batch document insertion for data seeding
func (ww *WriteWorker) InsertBatch(ctx context.Context, batchSize int) error {
	startTime := time.Now()

	// Generate batch of documents
	docs := ww.dataGenerator.GenerateDocuments(batchSize)

	// Insert batch
	err := ww.database.InsertDocuments(ctx, docs)

	latency := time.Since(startTime)
	success := err == nil

	// Record metrics (count each document in the batch)
	for i := 0; i < len(docs); i++ {
		ww.metrics.RecordWrite(latency/time.Duration(len(docs)), success)
	}

	// Log batch operation
	if len(ww.operationLog) < cap(ww.operationLog) {
		logEntry := OperationLog{
			Timestamp:   startTime,
			Operation:   "insert_batch",
			Latency:     latency,
			ResultCount: len(docs),
			Success:     success,
		}

		if err != nil {
			logEntry.Error = err.Error()
		}

		ww.operationLog = append(ww.operationLog, logEntry)
	}

	slog.Info("Batch insert completed",
		"worker_id", ww.id,
		"batch_size", batchSize,
		"latency_ms", latency.Milliseconds(),
		"success", success)

	return err
}

// GetOperationLog returns the operation log for analysis
func (ww *WriteWorker) GetOperationLog() []OperationLog {
	return ww.operationLog
}

// ClearOperationLog clears the operation log
func (ww *WriteWorker) ClearOperationLog() {
	ww.operationLog = ww.operationLog[:0]
}

// GetStats returns worker-specific statistics
func (ww *WriteWorker) GetStats() WriteWorkerStats {
	totalOps := len(ww.operationLog)
	successfulOps := 0
	totalLatency := time.Duration(0)
	slowOps := 0
	batchOps := 0

	for _, op := range ww.operationLog {
		if op.Success {
			successfulOps++
		}
		totalLatency += op.Latency
		if op.Latency > 500*time.Millisecond {
			slowOps++
		}
		if op.Operation == "insert_batch" {
			batchOps++
		}
	}

	var avgLatency time.Duration
	if totalOps > 0 {
		avgLatency = totalLatency / time.Duration(totalOps)
	}

	return WriteWorkerStats{
		WorkerID:      ww.id,
		TotalOps:      totalOps,
		SuccessfulOps: successfulOps,
		FailedOps:     totalOps - successfulOps,
		AvgLatency:    avgLatency,
		SlowOps:       slowOps,
		BatchOps:      batchOps,
		SuccessRate:   float64(successfulOps) / float64(totalOps),
	}
}

// WriteWorkerStats contains statistics for a write worker
type WriteWorkerStats struct {
	WorkerID      int           `json:"worker_id"`
	TotalOps      int           `json:"total_ops"`
	SuccessfulOps int           `json:"successful_ops"`
	FailedOps     int           `json:"failed_ops"`
	AvgLatency    time.Duration `json:"avg_latency"`
	SlowOps       int           `json:"slow_ops"`
	BatchOps      int           `json:"batch_ops"`
	SuccessRate   float64       `json:"success_rate"`
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
