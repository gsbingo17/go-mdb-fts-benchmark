package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
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

	// Write test mode configuration
	isWriteTestMode bool
	writeOp         string   // Current write operation: "CREATE", "UPDATE", "DELETE"
	writeTokens     int      // Number of tokens per write document
	writeTokenSizes []int    // Token sizes to randomly select from
	writeCollection string   // Single collection for write tests
	preloadedIDs    []string // Pre-generated IDs for write operations
	preloadedIDsMu  sync.RWMutex
	writeRng        *rand.Rand // RNG for write operations
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
		isWriteTestMode: false,
		writeRng:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))),
	}
}

// SetCostModelMode configures the worker for cost_model mode
func (ww *WriteWorker) SetCostModelMode(enabled bool, textShards []int, workerCount int, baseCollection string) {
	ww.isCostModelMode = enabled
	ww.textShards = textShards
	ww.workerCount = workerCount
	ww.baseCollection = baseCollection
}

// SetWriteTestMode configures the worker for write test mode (CREATE/UPDATE/DELETE phases)
func (ww *WriteWorker) SetWriteTestMode(writeTokens int, writeTokenSizes []int, writeCollection string) {
	ww.isWriteTestMode = true
	ww.writeTokens = writeTokens
	ww.writeTokenSizes = writeTokenSizes
	ww.writeCollection = writeCollection
}

// SetWriteOp sets the current write operation phase (CREATE, UPDATE, DELETE)
func (ww *WriteWorker) SetWriteOp(op string) {
	ww.writeOp = op
}

// SetPreloadedIDs sets the pre-generated IDs for write operations
func (ww *WriteWorker) SetPreloadedIDs(ids []string) {
	ww.preloadedIDsMu.Lock()
	defer ww.preloadedIDsMu.Unlock()
	ww.preloadedIDs = ids
}

// GetPreloadedIDs returns a copy of the current preloaded IDs (thread-safe)
func (ww *WriteWorker) GetPreloadedIDs() []string {
	ww.preloadedIDsMu.RLock()
	defer ww.preloadedIDsMu.RUnlock()
	result := make([]string, len(ww.preloadedIDs))
	copy(result, ww.preloadedIDs)
	return result
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

// insertDocument performs a single document insertion or write test operation
func (ww *WriteWorker) insertDocument(ctx context.Context) error {
	startTime := time.Now()

	var err error
	var doc database.Document
	var opName string

	if ww.isWriteTestMode {
		// Write test mode: CREATE/UPDATE/DELETE operations
		err = ww.executeWriteTestOp(ctx)
		opName = fmt.Sprintf("write_test_%s", strings.ToLower(ww.writeOp))
	} else if ww.isCostModelMode {
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
		opName = "insert_document"
	} else {
		// Standard mode - insert into default table
		doc = ww.dataGenerator.GenerateDocument()
		err = ww.database.InsertDocument(ctx, doc)
		opName = "insert_document"
	}

	latency := time.Since(startTime)
	success := err == nil

	// Record metrics
	ww.metrics.RecordWrite(latency, success)

	// Log operation (with size limit)
	if len(ww.operationLog) < cap(ww.operationLog) {
		logEntry := OperationLog{
			Timestamp: startTime,
			Operation: opName,
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
		slog.Warn("Slow write operation detected",
			"worker_id", ww.id,
			"operation", opName,
			"latency_ms", latency.Milliseconds())
	}

	return err
}

// executeWriteTestOp performs a single write test operation based on the current phase
func (ww *WriteWorker) executeWriteTestOp(ctx context.Context) error {
	switch ww.writeOp {
	case "CREATE":
		return ww.executeCreate(ctx)
	case "UPDATE":
		return ww.executeUpdate(ctx)
	case "DELETE":
		return ww.executeDelete(ctx)
	default:
		return fmt.Errorf("unknown write operation: %s", ww.writeOp)
	}
}

// executeCreate generates a new document and upserts it into the write collection
func (ww *WriteWorker) executeCreate(ctx context.Context) error {
	// Generate a new ID
	idBytes := generator.NextPreloadIdBytes(ww.writeRng, generator.KeySize, 1, 0)
	id := generator.PreloadIdBytesToId(idBytes)

	// Randomly select token size
	tokenSize := ww.writeTokenSizes[ww.writeRng.Intn(len(ww.writeTokenSizes))]

	// Generate write document (create mode: update=false)
	doc := generator.MakeDatabaseTextWriteDocument(id, ww.writeTokens, tokenSize, false)

	// Upsert using ReplaceOne with upsert=true
	err := ww.database.ReplaceDocumentInCollection(ctx, ww.writeCollection, id, doc)
	if err != nil {
		return fmt.Errorf("CREATE failed: %w", err)
	}

	// Track the ID for later UPDATE/DELETE phases
	ww.preloadedIDsMu.Lock()
	ww.preloadedIDs = append(ww.preloadedIDs, id)
	ww.preloadedIDsMu.Unlock()

	return nil
}

// executeUpdate selects a random existing ID and updates the document with reversed tokens
func (ww *WriteWorker) executeUpdate(ctx context.Context) error {
	ww.preloadedIDsMu.RLock()
	numIDs := len(ww.preloadedIDs)
	if numIDs == 0 {
		ww.preloadedIDsMu.RUnlock()
		// If no preloaded IDs, generate a new one (may not exist in DB, but upsert handles it)
		idBytes := generator.NextPreloadIdBytes(ww.writeRng, generator.KeySize, 1, 0)
		id := generator.PreloadIdBytesToId(idBytes)

		tokenSize := ww.writeTokenSizes[ww.writeRng.Intn(len(ww.writeTokenSizes))]
		doc := generator.MakeDatabaseTextWriteDocument(id, ww.writeTokens, tokenSize, true)
		return ww.database.ReplaceDocumentInCollection(ctx, ww.writeCollection, id, doc)
	}
	// Select random ID from preloaded set
	id := ww.preloadedIDs[ww.writeRng.Intn(numIDs)]
	ww.preloadedIDsMu.RUnlock()

	// Randomly select token size
	tokenSize := ww.writeTokenSizes[ww.writeRng.Intn(len(ww.writeTokenSizes))]

	// Generate write document with reversed tokens (update mode: update=true)
	doc := generator.MakeDatabaseTextWriteDocument(id, ww.writeTokens, tokenSize, true)

	// Upsert using ReplaceOne with upsert=true
	err := ww.database.ReplaceDocumentInCollection(ctx, ww.writeCollection, id, doc)
	if err != nil {
		return fmt.Errorf("UPDATE failed: %w", err)
	}

	return nil
}

// executeDelete selects a random existing ID and deletes the document
func (ww *WriteWorker) executeDelete(ctx context.Context) error {
	ww.preloadedIDsMu.Lock()
	numIDs := len(ww.preloadedIDs)
	if numIDs == 0 {
		ww.preloadedIDsMu.Unlock()
		// Nothing to delete - generate a random ID and attempt deletion anyway
		idBytes := generator.NextPreloadIdBytes(ww.writeRng, generator.KeySize, 1, 0)
		id := generator.PreloadIdBytesToId(idBytes)
		return ww.database.DeleteDocumentInCollection(ctx, ww.writeCollection, id)
	}

	// Select random index and remove it from the list (swap with last, then shrink)
	idx := ww.writeRng.Intn(numIDs)
	id := ww.preloadedIDs[idx]
	ww.preloadedIDs[idx] = ww.preloadedIDs[numIDs-1]
	ww.preloadedIDs = ww.preloadedIDs[:numIDs-1]
	ww.preloadedIDsMu.Unlock()

	err := ww.database.DeleteDocumentInCollection(ctx, ww.writeCollection, id)
	if err != nil {
		return fmt.Errorf("DELETE failed: %w", err)
	}

	return nil
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

	successRate := 0.0
	if totalOps > 0 {
		successRate = float64(successfulOps) / float64(totalOps)
	}

	return WriteWorkerStats{
		WorkerID:      ww.id,
		TotalOps:      totalOps,
		SuccessfulOps: successfulOps,
		FailedOps:     totalOps - successfulOps,
		AvgLatency:    avgLatency,
		SlowOps:       slowOps,
		BatchOps:      batchOps,
		SuccessRate:   successRate,
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
