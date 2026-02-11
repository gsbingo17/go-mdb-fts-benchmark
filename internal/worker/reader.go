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
	id           int
	database     database.Database
	workloadGen  *generator.WorkloadGenerator
	metrics      *metrics.MetricsCollector
	rateLimiter  *rate.Limiter
	operationLog []OperationLog
	queryLimit   int

	// Cost model mode configuration
	isCostModelMode bool
	queryRequest    generator.QueryRequest    // Single request object with all query generation parameters
	geoQueryRequest generator.GeoQueryRequest // Geospatial query request parameters
	geoGenerator    *generator.GeoGenerator   // Geospatial query generator

	// Search type configuration
	searchType string // "text", "atlas_search", or "geospatial_search"
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
) *ReadWorker {
	return &ReadWorker{
		id:              id,
		database:        db,
		workloadGen:     workloadGen,
		metrics:         metricsCollector,
		rateLimiter:     rateLimiter,
		operationLog:    make([]OperationLog, 0, 1000),
		queryLimit:      queryLimit,
		isCostModelMode: false,
		searchType:      "text", // Default to $text search
	}
}

// SetCostModelMode configures the worker for cost_model mode with QueryRequest
func (rw *ReadWorker) SetCostModelMode(enabled bool, queryRequest generator.QueryRequest) {
	rw.isCostModelMode = enabled
	rw.queryRequest = queryRequest
}

// SetGeoCostModelMode configures the worker for cost_model mode with geospatial queries
func (rw *ReadWorker) SetGeoCostModelMode(enabled bool, geoQueryRequest generator.GeoQueryRequest, geoGen *generator.GeoGenerator) {
	rw.isCostModelMode = enabled
	rw.geoQueryRequest = geoQueryRequest
	rw.geoGenerator = geoGen
}

// SetSearchType configures the search type ("text" or "atlas_search")
func (rw *ReadWorker) SetSearchType(searchType string) {
	rw.searchType = searchType
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

			// Execute text search operation
			if err := rw.executeTextSearch(ctx); err != nil {
				slog.Debug("Text search failed", "worker_id", rw.id, "error", err)
			}
		}
	}
}

// executeTextSearch performs a single text search operation
func (rw *ReadWorker) executeTextSearch(ctx context.Context) error {
	startTime := time.Now()

	// Generate search query based on mode
	var query string
	var resultCount int
	var err error
	var collectionName string
	var textShard int
	var limit int

	if rw.isCostModelMode {
		// Handle geospatial search separately
		if rw.searchType == "geospatial_search" && rw.geoGenerator != nil {
			// Use GeoQueryRequest/GeoQueryResult pattern for geospatial queries
			geoResult := rw.workloadGen.GenerateGeoQueryRequest(rw.geoQueryRequest, rw.geoGenerator)

			resultCount, err = rw.database.ExecuteGeoSearchInCollection(
				ctx,
				geoResult.CollectionName,
				geoResult.RawQuery,
				geoResult.Limit,
			)
			query = geoResult.Query
			collectionName = geoResult.CollectionName
			textShard = geoResult.SelectedTextShard
			limit = geoResult.Limit
		} else {
			// Use QueryRequest/QueryResult pattern for text/atlas search
			queryResult := rw.workloadGen.GenerateTokenQueryRequest(rw.queryRequest)

			// Route to appropriate search method based on search_type
			if rw.searchType == "atlas_search" {
				resultCount, err = rw.database.ExecuteAtlasSearchInCollection(
					ctx,
					queryResult.CollectionName,
					queryResult.Query,
					queryResult.Limit,
					queryResult.Operator,
				)
			} else {
				// Default to $text search
				resultCount, err = rw.database.ExecuteTextSearchInCollection(
					ctx,
					queryResult.CollectionName,
					queryResult.Query,
					queryResult.Limit,
				)
			}
			query = queryResult.Query
			collectionName = queryResult.CollectionName
			textShard = queryResult.SelectedTextShard
			limit = queryResult.Limit
		}
	} else {
		query = rw.workloadGen.GenerateSearchQuery()

		// Route to appropriate search method based on search_type
		if rw.searchType == "atlas_search" {
			resultCount, err = rw.database.ExecuteAtlasSearch(ctx, query, rw.queryLimit)
		} else if rw.searchType == "geospatial_search" {
			resultCount, err = rw.database.ExecuteGeoSearch(ctx, query, rw.queryLimit)
		} else {
			// Default to $text search
			resultCount, err = rw.database.ExecuteTextSearch(ctx, query, rw.queryLimit)
		}
		limit = rw.queryLimit
	}

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
		if rw.isCostModelMode {
			slog.Debug("Zero results query",
				"worker_id", rw.id,
				"collection", collectionName,
				"text_shard", textShard,
				"limit", limit,
				"query", query,
				"search_type", rw.searchType,
				"latency_us", latency.Microseconds())
		} else {
			slog.Debug("Zero results query",
				"worker_id", rw.id,
				"query", query,
				"search_type", rw.searchType,
				"latency_us", latency.Microseconds())
		}
	} else {
		if rw.isCostModelMode {
			slog.Debug("Successful search",
				"worker_id", rw.id,
				"collection", collectionName,
				"text_shard", textShard,
				"limit", limit,
				"query", query,
				"search_type", rw.searchType,
				"result_count", resultCount,
				"latency_us", latency.Microseconds())
		} else {
			slog.Debug("Successful search",
				"worker_id", rw.id,
				"query", query,
				"search_type", rw.searchType,
				"result_count", resultCount,
				"latency_us", latency.Microseconds())
		}
	}

	// Log slow queries
	if latency > 1000*time.Millisecond {
		if rw.isCostModelMode {
			slog.Warn("Slow query detected",
				"worker_id", rw.id,
				"collection", collectionName,
				"text_shard", textShard,
				"limit", limit,
				"query", query,
				"search_type", rw.searchType,
				"latency_us", latency.Microseconds(),
				"result_count", resultCount)
		} else {
			slog.Warn("Slow query detected",
				"worker_id", rw.id,
				"query", query,
				"search_type", rw.searchType,
				"latency_us", latency.Microseconds(),
				"result_count", resultCount)
		}
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
