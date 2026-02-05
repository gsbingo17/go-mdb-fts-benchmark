package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/time/rate"

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
	rateLimiter   *rate.Limiter
	operationLog  []OperationLog

	// Cost model mode configuration
	isCostModelMode bool
	textShards      []int // Array of shard numbers (e.g., [1, 2, 3])
	workerCount     int
	baseCollection  string // Base collection name for shard tables
}

// NewWriteWorker creates a new write worker
func NewWriteWorker(
	id int,
	db database.Database,
	dataGen *generator.DataGenerator,
	metricsCollector *metrics.MetricsCollector,
	rateLimiter *rate.Limiter,
) *WriteWorker {
	return &WriteWorker{
		id:              id,
		database:        db,
		dataGenerator:   dataGen,
		metrics:         metricsCollector,
		rateLimiter:     rateLimiter,
		operationLog:    make([]OperationLog, 0, 1000),
		isCostModelMode: false,
		textShards:      nil,
		workerCount:     0,
		baseCollection:  "",
	}
}

// SetCostModelMode configures the worker for cost_model mode
func (ww *WriteWorker) SetCostModelMode(enabled bool, textShards []int, workerCount int, baseCollection string) {
	ww.isCostModelMode = enabled
	ww.textShards = textShards
	ww.workerCount = workerCount
	ww.baseCollection = baseCollection
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
			// Wait for rate limiter
			if err := ww.rateLimiter.Wait(ctx); err != nil {
				slog.Debug("Rate limiter wait interrupted", "worker_id", ww.id, "error", err)
				return
			}

			// Execute document insertion
			if err := ww.insertDocument(ctx); err != nil {
				slog.Debug("Document insertion failed", "worker_id", ww.id, "error", err)
			}
		}
	}
}

// insertDocument performs a single document insertion
func (ww *WriteWorker) insertDocument(ctx context.Context) error {
	startTime := time.Now()

	// Generate document and insert based on mode
	var err error
	var doc database.Document

	if ww.isCostModelMode {
		// In cost_model mode, insert into a specific shard table
		maxTextShard := 0
		for _, shard := range ww.textShards {
			if shard > maxTextShard {
				maxTextShard = shard
			}
		}
		doc = ww.dataGenerator.GenerateTokenDocument(maxTextShard, ww.workerCount, ww.id)

		// Select shard table using round-robin based on worker ID
		shardIndex := ww.id % len(ww.textShards)
		shardNumber := ww.textShards[shardIndex]
		collectionName := fmt.Sprintf("%s%d", ww.baseCollection, shardNumber)

		err = ww.database.InsertDocumentInCollection(ctx, collectionName, doc)
	} else {
		// Standard mode - insert into default table
		doc = ww.dataGenerator.GenerateDocument()
		err = ww.database.InsertDocument(ctx, doc)
	}

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

	// Log slow operations
	if latency > 500*time.Millisecond {
		titleField := doc.Title
		if ww.isCostModelMode {
			// In cost_model mode, use text1 field for logging
			titleField = doc.Text1
		}
		if len(titleField) > 50 {
			titleField = titleField[:50]
		}
		slog.Warn("Slow write operation detected",
			"worker_id", ww.id,
			"latency_ms", latency.Milliseconds(),
			"document_preview", titleField)
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
