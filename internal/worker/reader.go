package worker

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/time/rate"

	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/generator"
	"mongodb-benchmarking-tool/internal/metrics"
)

// ReadWorker performs text search operations
type ReadWorker struct {
	id            int
	database      database.Database
	workloadGen   *generator.WorkloadGenerator
	metrics       *metrics.MetricsCollector
	rateLimiter   *rate.Limiter
	operationLog  []OperationLog
	queryLimit    int
	benchmarkMode string // "text_search" or "field_query"
}

// OperationLog tracks individual operations for analysis
type OperationLog struct {
	Timestamp   time.Time     `json:"timestamp"`
	Operation   string        `json:"operation"`
	Query       string        `json:"query,omitempty"`
	Latency     time.Duration `json:"latency"`
	ResultCount int           `json:"result_count,omitempty"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
}

// NewReadWorker creates a new read worker
func NewReadWorker(
	id int,
	db database.Database,
	workloadGen *generator.WorkloadGenerator,
	metricsCollector *metrics.MetricsCollector,
	rateLimiter *rate.Limiter,
	queryLimit int,
	benchmarkMode string,
) *ReadWorker {
	return &ReadWorker{
		id:            id,
		database:      db,
		workloadGen:   workloadGen,
		metrics:       metricsCollector,
		rateLimiter:   rateLimiter,
		operationLog:  make([]OperationLog, 0, 1000),
		queryLimit:    queryLimit,
		benchmarkMode: benchmarkMode,
	}
}

// Start begins the read worker operations
func (rw *ReadWorker) Start(ctx context.Context) {
	slog.Info("Starting read worker", "worker_id", rw.id)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Read worker stopping", "worker_id", rw.id)
			return
		default:
			// Wait for rate limiter
			if err := rw.rateLimiter.Wait(ctx); err != nil {
				slog.Debug("Rate limiter wait interrupted", "worker_id", rw.id, "error", err)
				return
			}

			// Execute operation based on benchmark mode
			var err error
			switch rw.benchmarkMode {
			case "text_search":
				err = rw.executeTextSearch(ctx)
			case "field_query":
				err = rw.executeFieldQuery(ctx)
			default:
				err = rw.executeTextSearch(ctx) // Default to text search
			}

			if err != nil {
				slog.Debug("Operation failed", "worker_id", rw.id, "mode", rw.benchmarkMode, "error", err)
			}
		}
	}
}

// executeTextSearch performs a single text search operation
func (rw *ReadWorker) executeTextSearch(ctx context.Context) error {
	startTime := time.Now()

	// Generate search query
	query := rw.workloadGen.GenerateSearchQuery()

	// Execute the search with limit
	resultCount, err := rw.database.ExecuteTextSearch(ctx, query, rw.queryLimit)

	latency := time.Since(startTime)
	success := err == nil

	// Record metrics
	rw.metrics.RecordRead(latency, success)

	// Log operation (with size limit)
	if len(rw.operationLog) < cap(rw.operationLog) {
		logEntry := OperationLog{
			Timestamp:   startTime,
			Operation:   "text_search",
			Query:       query,
			Latency:     latency,
			ResultCount: resultCount,
			Success:     success,
		}

		if err != nil {
			logEntry.Error = err.Error()
		}

		rw.operationLog = append(rw.operationLog, logEntry)
	}

	// Log zero result queries to monitor search effectiveness
	if resultCount == 0 {
		slog.Debug("Zero results query",
			"worker_id", rw.id,
			"query", query,
			"latency_us", latency.Microseconds())
	} else {
		slog.Debug("Successful search",
			"worker_id", rw.id,
			"query", query,
			"result_count", resultCount,
			"latency_us", latency.Microseconds())
	}

	// Log slow queries
	if latency > 1000*time.Millisecond {
		slog.Warn("Slow query detected",
			"worker_id", rw.id,
			"query", query,
			"latency_us", latency.Microseconds(),
			"result_count", resultCount)
	}

	return err
}

// executeFieldQuery performs a single field equality query operation
func (rw *ReadWorker) executeFieldQuery(ctx context.Context) error {
	startTime := time.Now()

	// Generate field value for equality query
	fieldValue := rw.workloadGen.GenerateFieldValue()

	// Execute the field query with limit (search on "title" field)
	resultCount, err := rw.database.ExecuteFieldQuery(ctx, "title", fieldValue, rw.queryLimit)

	latency := time.Since(startTime)
	success := err == nil

	// Record metrics
	rw.metrics.RecordRead(latency, success)

	// Log operation (with size limit)
	if len(rw.operationLog) < cap(rw.operationLog) {
		logEntry := OperationLog{
			Timestamp:   startTime,
			Operation:   "field_query",
			Query:       fieldValue,
			Latency:     latency,
			ResultCount: resultCount,
			Success:     success,
		}

		if err != nil {
			logEntry.Error = err.Error()
		}

		rw.operationLog = append(rw.operationLog, logEntry)
	}

	// Log zero result queries to monitor search effectiveness
	if resultCount == 0 {
		slog.Debug("Zero results field query",
			"worker_id", rw.id,
			"field_value", fieldValue,
			"latency_us", latency.Microseconds())
	} else {
		slog.Debug("Successful field query",
			"worker_id", rw.id,
			"field_value", fieldValue,
			"result_count", resultCount,
			"latency_us", latency.Microseconds())
	}

	// Log slow queries
	if latency > 1000*time.Millisecond {
		slog.Warn("Slow field query detected",
			"worker_id", rw.id,
			"field_value", fieldValue,
			"latency_us", latency.Microseconds(),
			"result_count", resultCount)
	}

	return err
}

// GetOperationLog returns the operation log for analysis
func (rw *ReadWorker) GetOperationLog() []OperationLog {
	return rw.operationLog
}

// ClearOperationLog clears the operation log
func (rw *ReadWorker) ClearOperationLog() {
	rw.operationLog = rw.operationLog[:0]
}

// GetStats returns worker-specific statistics
func (rw *ReadWorker) GetStats() ReadWorkerStats {
	totalOps := len(rw.operationLog)
	successfulOps := 0
	totalLatency := time.Duration(0)
	slowQueries := 0

	for _, op := range rw.operationLog {
		if op.Success {
			successfulOps++
		}
		totalLatency += op.Latency
		if op.Latency > 1000*time.Millisecond {
			slowQueries++
		}
	}

	var avgLatency time.Duration
	if totalOps > 0 {
		avgLatency = totalLatency / time.Duration(totalOps)
	}

	return ReadWorkerStats{
		WorkerID:      rw.id,
		TotalOps:      totalOps,
		SuccessfulOps: successfulOps,
		FailedOps:     totalOps - successfulOps,
		AvgLatency:    avgLatency,
		SlowQueries:   slowQueries,
		SuccessRate:   float64(successfulOps) / float64(totalOps),
	}
}

// ReadWorkerStats contains statistics for a read worker
type ReadWorkerStats struct {
	WorkerID      int           `json:"worker_id"`
	TotalOps      int           `json:"total_ops"`
	SuccessfulOps int           `json:"successful_ops"`
	FailedOps     int           `json:"failed_ops"`
	AvgLatency    time.Duration `json:"avg_latency"`
	SlowQueries   int           `json:"slow_queries"`
	SuccessRate   float64       `json:"success_rate"`
}
