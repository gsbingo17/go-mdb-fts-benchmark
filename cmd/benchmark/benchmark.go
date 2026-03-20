package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"mongodb-benchmarking-tool/internal/config"
	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/generator"
	"mongodb-benchmarking-tool/internal/metrics"
	"mongodb-benchmarking-tool/internal/worker"

	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/time/rate"
)

// BenchmarkRunner orchestrates the complete benchmarking process
type BenchmarkRunner struct {
	config               *config.Config
	database             database.Database
	spannerClient        *database.SpannerClient // Reference to Spanner client if used
	metricsCollector     *metrics.MetricsCollector
	costCalculator       *metrics.CostCalculator
	exporter             *metrics.Exporter
	workloadController   *worker.WorkloadController
	saturationController *metrics.SaturationController
	costTracker          metrics.RealTimeCostTracker

	startTime            time.Time
	measurementStartTime time.Time
	measurementActive    bool
}

// getCollectionNameForShard returns the collection name for a specific shard number
func (br *BenchmarkRunner) getCollectionNameForShard(shard int) string {
	baseCollection := br.config.Database.Collection
	return fmt.Sprintf("%s%d", baseCollection, shard)
}

// getAllShardCollections returns all shard collection names based on textShards configuration
func (br *BenchmarkRunner) getAllShardCollections() []string {
	if br.config.Workload.Mode != "cost_model" {
		return []string{br.config.Database.Collection}
	}

	// TextShards is now an array - create collections for each shard value
	collections := make([]string, len(br.config.Workload.TextShards))
	for i, shardNum := range br.config.Workload.TextShards {
		collections[i] = br.getCollectionNameForShard(shardNum)
	}
	return collections
}

// NewBenchmarkRunner creates a new benchmark runner
func NewBenchmarkRunner(cfg *config.Config) (*BenchmarkRunner, error) {
	// Create database client
	var db database.Database
	var err error

	switch cfg.Database.Type {
	case "mongodb":
		db = database.NewMongoDBClient(cfg.Database)
	case "documentdb":
		docdbClient := database.NewDocumentDBClient(cfg.Database)

		// Set up DocumentDB monitoring if configured
		if cfg.Cost.Provider == "documentdb" {
			if monitor, err := metrics.NewDocumentDBMonitor(cfg.Cost.DocumentDB); err == nil {
				docdbClient.SetMonitor(monitor)
				slog.Info("DocumentDB CloudWatch monitoring enabled")
			} else {
				slog.Warn("Failed to create DocumentDB monitor, using fallback metrics", "error", err)
			}
		}

		db = docdbClient
	case "spanner":
		db = database.NewSpannerClient(cfg.Database)
		slog.Info("Google Cloud Spanner client created with built-in metrics collection")
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}

	// Save reference to Spanner client before wrapping (needed for table creation)
	var spannerClient *database.SpannerClient
	if cfg.Database.Type == "spanner" {
		spannerClient = db.(*database.SpannerClient)
	}

	// Create metrics components
	metricsCollector := metrics.NewMetricsCollector()
	costCalculator := metrics.NewCostCalculator(cfg.Cost)
	exporter := metrics.NewExporter(cfg.Metrics.ExportPath, cfg.Metrics.ExportFormat)

	// Create workload controller
	workloadController := worker.NewWorkloadController(cfg.Workload, db, metricsCollector)

	// Create enhanced database client with monitoring
	monitor, err := metrics.CreateDatabaseMonitor(*cfg)
	if err != nil {
		slog.Warn("Failed to create database monitor, using basic metrics", "error", err)
	} else {
		db = metrics.NewEnhancedDatabaseClient(db, monitor)
	}

	// Create cost tracker
	costTracker, err := metrics.CreateRealTimeCostTracker(*cfg)
	if err != nil {
		slog.Warn("Failed to create cost tracker, using basic cost calculation", "error", err)
	}

	runner := &BenchmarkRunner{
		config:             cfg,
		database:           db,
		spannerClient:      spannerClient,
		metricsCollector:   metricsCollector,
		costCalculator:     costCalculator,
		exporter:           exporter,
		workloadController: workloadController,
		costTracker:        costTracker,
	}

	// Create saturation controller with callback
	if monitor != nil {
		runner.saturationController = metrics.NewSaturationController(
			monitor,
			cfg.Workload,
			runner.handleWorkloadAdjustment,
		)
		// Set up measurement callback for stability-based metrics reset
		runner.saturationController.SetMeasurementCallback(runner.handleMeasurementStateChange)
	}

	return runner, nil
}

// Run executes the complete benchmark
func (br *BenchmarkRunner) Run(ctx context.Context) error {
	br.startTime = time.Now()

	slog.Info("Starting benchmark run",
		"duration", br.config.Workload.Duration,
		"target_qps", br.config.Workload.TargetQPS,
		"workers", br.config.Workload.WorkerCount,
		"database", br.config.Database.Type)

	// Phase 1: Connect to database
	if err := br.connectDatabase(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	// Phase 2: Setup data and indexes (skip for write-only workloads)
	if len(br.config.Workload.WriteOperations) > 0 {
		slog.Info("Skipping data setup for write workload (write benchmark manages its own collection)")
	} else {
		if err := br.setupData(ctx); err != nil {
			return fmt.Errorf("data setup failed: %w", err)
		}
	}

	// Phase 3: Warmup period (skip for write workloads)
	if len(br.config.Workload.WriteOperations) > 0 {
		slog.Info("Skipping warmup for write workload")
	} else if err := br.warmupPhase(ctx); err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	// Phase 4: Start monitoring and control systems
	if err := br.startMonitoring(ctx); err != nil {
		return fmt.Errorf("monitoring startup failed: %w", err)
	}

	// Phase 5: Execute main benchmark
	if len(br.config.Workload.WriteOperations) > 0 {
		if err := br.executeWriteBenchmark(ctx); err != nil {
			return fmt.Errorf("write benchmark execution failed: %w", err)
		}
	} else {
		if err := br.executeBenchmark(ctx); err != nil {
			return fmt.Errorf("benchmark execution failed: %w", err)
		}
	}

	// Phase 6: Collect final results
	if err := br.finalizeResults(ctx); err != nil {
		return fmt.Errorf("result finalization failed: %w", err)
	}

	return nil
}

// connectDatabase establishes database connection
func (br *BenchmarkRunner) connectDatabase(ctx context.Context) error {
	slog.Info("Connecting to database", "type", br.config.Database.Type)

	// Use a longer timeout for initial connection to handle network latency
	connectCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	slog.Info("Attempting database connection (may take up to 2 minutes for Atlas clusters)")
	if err := br.database.Connect(connectCtx); err != nil {
		return fmt.Errorf("failed to connect within 2 minutes: %w", err)
	}

	// Verify connection with a separate timeout
	pingCtx, pingCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pingCancel()

	slog.Info("Verifying database connection")
	if err := br.database.Ping(pingCtx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	slog.Info("Database connection established successfully")
	return nil
}

// seedData generates and inserts test data
func (br *BenchmarkRunner) seedData(ctx context.Context, count int) error {
	dataGen := generator.NewDataGenerator(time.Now().UnixNano())

	// Use configured seed for geospatial data generation, or timestamp if seed is 0
	geoSeed := br.config.Workload.GeoSeed
	if geoSeed == 0 {
		geoSeed = time.Now().UnixNano()
	}
	geoGen := generator.NewGeoGenerator(geoSeed)
	// Create separate DataGenerator with geo_seed for reproducible ID generation
	geoDataGen := generator.NewDataGenerator(geoSeed)
	slog.Info("Geospatial data generator initialized", "seed", geoSeed, "reproducible", br.config.Workload.GeoSeed != 0)

	batchSize := 1000

	// Check if we're in cost_model mode
	isCostModelMode := br.config.Workload.Mode == "cost_model"
	searchType := br.config.Workload.SearchType
	if searchType == "" {
		searchType = "text" // Default to text search
	}

	// Get max textShard value for data generation
	maxTextShard := 0
	if isCostModelMode && len(br.config.Workload.TextShards) > 0 {
		for _, shard := range br.config.Workload.TextShards {
			if shard > maxTextShard {
				maxTextShard = shard
			}
		}
		slog.Info("Document generation mode",
			"search_type", searchType,
			"text_shards", br.config.Workload.TextShards,
			"max_text_shard", maxTextShard,
			"worker_count", br.config.Workload.WorkerCount,
			"collections", br.getAllShardCollections())
	} else {
		slog.Info("Document generation mode",
			"search_type", searchType,
			"mode", "benchmark")
	}

	for i := 0; i < count; i += batchSize {
		remaining := count - i
		if remaining > batchSize {
			remaining = batchSize
		}

		if isCostModelMode {
			if searchType == "geospatial_search" {
				// CRITICAL: Geospatial uses SINGLE base collection, NOT sharded collections
				// All geospatial documents go to the base collection
				var geoDocs []database.Document
				for j := 0; j < remaining; j++ {
					// Generate deterministic ID using token-based pattern (same as Atlas/text search)
					threadID := (i + j) % br.config.Workload.WorkerCount
					idBytes := generator.NextPreloadIdBytes(geoDataGen.GetRng(), generator.KeySize, br.config.Workload.WorkerCount, threadID)
					id := generator.PreloadIdBytesToId(idBytes)

					// Generate geospatial document with token-based ID
					geoData := geoGen.GenerateGeoDocument(id)
					geoDoc := geoData.(bson.M)
					// Extract fields and populate Document struct to preserve _id
					geoDocs = append(geoDocs, database.Document{
						ID:       geoDoc["_id"].(string),
						Location: geoDoc["location"],
					})
				}

				// Insert all geospatial documents to base collection
				baseCollection := br.config.Database.Collection
				if err := br.database.InsertDocumentsInCollection(ctx, baseCollection, geoDocs); err != nil {
					return fmt.Errorf("batch insert failed for geospatial collection %s: %w", baseCollection, err)
				}
			} else {
				// Text/Atlas search: distribute documents across shard collections
				docsPerShard := make(map[int][]database.Document)

				for j := 0; j < remaining; j++ {
					threadID := (i + j) % br.config.Workload.WorkerCount

					// Generate token-based document for text/atlas search
					doc := dataGen.GenerateTokenDocument(
						maxTextShard,
						br.config.Workload.WorkerCount,
						threadID,
					)

					// Distribute documents round-robin across the textShards array values
					shardIndex := (i + j) % len(br.config.Workload.TextShards)
					shard := br.config.Workload.TextShards[shardIndex]
					docsPerShard[shard] = append(docsPerShard[shard], doc)
				}

				// Insert documents into their respective shard collections
				for shard, docs := range docsPerShard {
					if len(docs) > 0 {
						collectionName := br.getCollectionNameForShard(shard)
						if err := br.database.InsertDocumentsInCollection(ctx, collectionName, docs); err != nil {
							return fmt.Errorf("batch insert failed for collection %s: %w", collectionName, err)
						}
					}
				}
			}
		} else {
			// Generate standard documents for benchmarking
			var docs []database.Document
			if searchType == "geospatial_search" {
				// Generate geospatial documents with "location" field
				docs = make([]database.Document, remaining)
				for j := 0; j < remaining; j++ {
					// Generate deterministic ID using token-based pattern (same as Atlas/text search)
					threadID := (i + j) % br.config.Workload.WorkerCount
					idBytes := generator.NextPreloadIdBytes(geoDataGen.GetRng(), generator.KeySize, br.config.Workload.WorkerCount, threadID)
					id := generator.PreloadIdBytesToId(idBytes)

					// Generate geospatial document with token-based ID
					geoData := geoGen.GenerateGeoDocument(id)
					geoDoc := geoData.(bson.M)
					// Extract fields and populate Document struct to preserve _id
					docs[j] = database.Document{
						ID:       geoDoc["_id"].(string),
						Location: geoDoc["location"],
					}
				}
			} else {
				// Generate standard text search documents
				docs = dataGen.GenerateDocuments(remaining)
			}

			if err := br.database.InsertDocuments(ctx, docs); err != nil {
				return fmt.Errorf("batch insert failed: %w", err)
			}
		}

		if i%10000 == 0 {
			slog.Info("Data seeding progress", "inserted", i, "total", count, "mode", br.config.Workload.Mode)
		}
	}

	return nil
}

// setupData prepares the database with test data and indexes
func (br *BenchmarkRunner) setupData(ctx context.Context) error {
	slog.Info("Setting up database schema and data", "mode", br.config.Workload.Mode)

	// For Spanner, create tables first (required before any data operations)
	if br.config.Database.Type == "spanner" {
		if err := br.createSpannerTables(ctx); err != nil {
			return fmt.Errorf("failed to create Spanner tables: %w", err)
		}
	}

	// Check if we need to seed data FIRST (before creating indexes)
	// This is critical for Atlas Search which requires collections to exist before indexing
	var count int64
	var err error

	if br.config.Workload.Mode == "cost_model" {
		searchType := br.config.Workload.SearchType
		if searchType == "" {
			searchType = "text" // Default to text search
		}

		if searchType == "geospatial_search" {
			// CRITICAL FIX: Geospatial uses SINGLE base collection, NOT sharded collections
			// Count documents from base collection where geospatial data is actually stored
			baseCollection := br.config.Database.Collection
			count, err = br.database.CountDocumentsInCollection(ctx, baseCollection)
			if err != nil {
				// Collection doesn't exist yet - that's OK, count will be 0
				slog.Debug("Collection does not exist yet", "collection", baseCollection)
				count = 0
			}
			slog.Info("Counted existing documents in base collection (geospatial)",
				"collection", baseCollection,
				"count", count,
				"note", "geospatial uses single collection, not sharded")
		} else {
			// Text/Atlas search: count documents across shard collections
			collections := br.getAllShardCollections()
			for _, collectionName := range collections {
				collCount, err := br.database.CountDocumentsInCollection(ctx, collectionName)
				if err != nil {
					// Collection doesn't exist yet - that's OK, count will be 0
					slog.Debug("Collection does not exist yet", "collection", collectionName)
					continue
				}
				count += collCount
			}
			slog.Info("Counted existing documents across all shard collections",
				"total_count", count,
				"collections", collections)
		}
	} else {
		count, err = br.database.CountDocuments(ctx)
		if err != nil {
			// Collection doesn't exist yet - that's OK, count will be 0
			slog.Debug("Collection does not exist yet")
			count = 0
		}
	}

	requiredDocuments := int64(br.config.Workload.DatasetSize)
	if count < requiredDocuments {
		slog.Info("Seeding database with test data",
			"existing", count,
			"required", requiredDocuments,
			"mode", br.config.Workload.Mode)

		if err := br.seedData(ctx, int(requiredDocuments-count)); err != nil {
			return fmt.Errorf("data seeding failed: %w", err)
		}
	}

	// Create indexes AFTER data is seeded (required for Atlas Search)
	if err := br.createTextIndexForMode(ctx); err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	slog.Info("Database setup completed", "document_count", requiredDocuments, "mode", br.config.Workload.Mode)
	return nil
}

// createTextIndexForMode creates the appropriate text index based on the workload mode and search type
func (br *BenchmarkRunner) createTextIndexForMode(ctx context.Context) error {
	isCostModelMode := br.config.Workload.Mode == "cost_model"
	searchType := br.config.Workload.SearchType
	if searchType == "" {
		searchType = "text" // Default to $text search
	}

	// For Spanner, always use SEARCH indexes (uniform across all fields)
	if br.config.Database.Type == "spanner" {
		searchType = "spanner_search"
	}

	if isCostModelMode {
		// Get all shard collection names
		collections := br.getAllShardCollections()

		if searchType == "geospatial_search" {
			// CRITICAL FIX: Geospatial uses SINGLE base collection for data
			// Create index ONLY on base collection where data is actually stored
			// NOT on shard collections (text_shards is ignored for geospatial)
			baseCollection := br.config.Database.Collection
			slog.Info("Creating 2dsphere geospatial index on base collection",
				"search_type", searchType,
				"collection", baseCollection,
				"index_field", "location",
				"note", "text_shards ignored for geospatial - single collection architecture")

			if err := br.database.CreateGeoIndexForCollection(ctx, baseCollection, "location"); err != nil {
				return fmt.Errorf("failed to create geospatial index for base collection %s: %w", baseCollection, err)
			}

			slog.Info("Successfully created 2dsphere geospatial index on base collection",
				"collection", baseCollection,
				"index_type", "2dsphere")
		} else if searchType == "atlas_search" || searchType == "spanner_search" {
			// Atlas Search / Spanner SEARCH: uniform index across all shards (all fields indexed)
			indexType := "Atlas Search"
			if searchType == "spanner_search" {
				indexType = "Spanner SEARCH"
			}
			slog.Info(fmt.Sprintf("Creating uniform %s indexes for multi-collection setup", indexType),
				"search_type", searchType,
				"text_shards", br.config.Workload.TextShards,
				"base_collection", br.config.Database.Collection,
				"index_strategy", "uniform (all fields: text1, text2, text3)")

			for _, collectionName := range collections {
				slog.Info(fmt.Sprintf("Creating uniform %s index for collection", indexType),
					"collection", collectionName,
					"fields", "text1, text2, text3 (all fields)")
				if err := br.database.CreateSearchIndexForCollection(ctx, collectionName); err != nil {
					return fmt.Errorf("failed to create %s index for collection %s: %w", indexType, collectionName, err)
				}
			}

			slog.Info(fmt.Sprintf("Successfully created %s indexes for all shard collections", indexType),
				"count", len(collections),
				"index_type", searchType+"_uniform")
		} else {
			// $text search: progressive indexing based on shard number
			slog.Info("Creating progressive $text indexes for multi-collection setup",
				"search_type", "text",
				"text_shards", br.config.Workload.TextShards,
				"base_collection", br.config.Database.Collection,
				"index_strategy", "progressive")

			for i, collectionName := range collections {
				shardNumber := br.config.Workload.TextShards[i] // Use the actual shard number from array
				slog.Info("Creating progressive $text index for collection",
					"collection", collectionName,
					"shard", shardNumber,
					"fields", getIndexFieldsDescription(shardNumber))
				if err := br.database.CreateTextIndexForCollection(ctx, collectionName, shardNumber); err != nil {
					return fmt.Errorf("failed to create $text index for collection %s: %w", collectionName, err)
				}
			}

			slog.Info("Successfully created $text indexes for all shard collections",
				"count", len(collections),
				"index_type", "text_progressive")
		}

		return nil
	} else {
		// Benchmark mode: standard index on default collection
		if searchType == "geospatial_search" {
			slog.Info("Creating 2dsphere geospatial index on location field",
				"search_type", searchType)
			return br.database.CreateGeoIndex(ctx, "location")
		} else if searchType == "atlas_search" || searchType == "spanner_search" {
			indexType := "Atlas Search"
			if searchType == "spanner_search" {
				indexType = "Spanner SEARCH"
			}
			slog.Info(fmt.Sprintf("Creating %s index (standard fields)", indexType),
				"search_type", searchType)
			return br.database.CreateSearchIndex(ctx)
		} else {
			slog.Info("Creating standard $text index (title, content, search_terms)",
				"search_type", "text")
			return br.database.CreateTextIndex(ctx)
		}
	}
}

// warmupPhase runs a brief warmup to stabilize the system
func (br *BenchmarkRunner) warmupPhase(ctx context.Context) error {
	if br.config.Workload.WarmupDuration == 0 {
		return nil
	}

	slog.Info("Starting warmup phase", "duration", br.config.Workload.WarmupDuration)

	warmupCtx, cancel := context.WithTimeout(ctx, br.config.Workload.WarmupDuration)
	defer cancel()

	// Start workload at reduced intensity
	if err := br.workloadController.Start(); err != nil {
		return err
	}

	// Wait for warmup to complete
	<-warmupCtx.Done()

	// Reset metrics after warmup
	br.metricsCollector.Reset()

	slog.Info("Warmup phase completed")
	return nil
}

// startMonitoring begins all monitoring and control systems
func (br *BenchmarkRunner) startMonitoring(ctx context.Context) error {
	slog.Info("Starting monitoring and control systems")

	// Start saturation controller if available
	// For write workloads, the saturation controller is started later inside executeWriteBenchmark
	// (after preload completes) so it only monitors CPU during actual write phases
	if br.saturationController != nil && len(br.config.Workload.WriteOperations) == 0 {
		go br.saturationController.Start(ctx)
		slog.Info("CPU saturation controller started")
	} else if br.saturationController != nil && len(br.config.Workload.WriteOperations) > 0 {
		slog.Info("Deferring CPU saturation controller start until write phases begin (after preload)")
	}

	// Start metrics export routine
	go br.metricsExportLoop(ctx)
	slog.Info("Metrics export loop started")

	return nil
}

// executeBenchmark runs the main benchmark workload
func (br *BenchmarkRunner) executeBenchmark(ctx context.Context) error {
	benchmarkCtx, cancel := context.WithTimeout(ctx, br.config.Workload.Duration)
	defer cancel()

	slog.Info("Starting main benchmark execution")

	// Ensure workload is running (may already be started from warmup)
	if err := br.workloadController.Start(); err != nil {
		return err
	}

	// Monitor benchmark progress
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-benchmarkCtx.Done():
			slog.Info("Benchmark duration completed")
			return nil
		case <-ctx.Done():
			slog.Info("Benchmark cancelled")
			return ctx.Err()
		case <-ticker.C:
			br.logProgress()
		}
	}
}

// logProgress logs current benchmark progress
func (br *BenchmarkRunner) logProgress() {
	var metrics metrics.Metrics
	var elapsedTime time.Duration
	var phaseInfo string

	if br.measurementActive && !br.measurementStartTime.IsZero() {
		// In measurement phase - use measurement duration for accurate QPS calculation
		elapsedTime = time.Since(br.measurementStartTime)
		totalElapsed := time.Since(br.startTime)

		// Get metrics with measurement duration for accurate QPS
		metrics = br.metricsCollector.GetMetricsWithCustomDuration(elapsedTime)
		phaseInfo = fmt.Sprintf("measurement=%v total=%v", elapsedTime, totalElapsed)
	} else {
		// In ramp-up phase - use default duration calculation
		metrics = br.metricsCollector.GetCurrentMetrics()
		elapsedTime = metrics.Duration
		phaseInfo = fmt.Sprintf("total=%v", elapsedTime)
	}

	var saturationStatus string
	if br.saturationController != nil {
		status := br.saturationController.GetStatus()
		saturationStatus = fmt.Sprintf("cpu=%.1f%% target=%.1f%% stable=%v",
			status.CurrentCPU, status.TargetCPU, status.IsStable)
	}

	slog.Info("Benchmark progress",
		"elapsed", phaseInfo,
		"total_ops", metrics.TotalOps,
		"read_qps", fmt.Sprintf("%.1f", metrics.ReadQPS),
		"write_qps", fmt.Sprintf("%.1f", metrics.WriteQPS),
		"error_rate", fmt.Sprintf("%.2f%%", metrics.ErrorRate*100),
		"saturation", saturationStatus)
}

// metricsExportLoop periodically exports metrics
func (br *BenchmarkRunner) metricsExportLoop(ctx context.Context) {
	ticker := time.NewTicker(br.config.Metrics.ExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			br.exportCurrentMetrics()
		}
	}
}

// exportCurrentMetrics exports current metrics
func (br *BenchmarkRunner) exportCurrentMetrics() {
	// Use consistent duration calculation logic with logProgress()
	var metrics metrics.Metrics
	if br.measurementActive && !br.measurementStartTime.IsZero() {
		// In measurement phase - use measurement duration for accurate QPS calculation
		elapsedTime := time.Since(br.measurementStartTime)
		metrics = br.metricsCollector.GetMetricsWithCustomDuration(elapsedTime)
	} else {
		// In ramp-up or stabilization phase - use default duration calculation
		metrics = br.metricsCollector.GetCurrentMetrics()
	}

	costMetrics := br.calculateCurrentCost(metrics)
	systemState := br.getCurrentSystemState()
	phaseMetadata := br.getCurrentPhaseMetadata()

	// Validate state consistency before exporting
	br.validateStateConsistency(systemState, phaseMetadata)

	if err := br.exporter.ExportMetrics(metrics, costMetrics, systemState, phaseMetadata); err != nil {
		slog.Error("Failed to export metrics", "error", err)
	}
}

// calculateCurrentCost calculates current cost metrics
func (br *BenchmarkRunner) calculateCurrentCost(m metrics.Metrics) metrics.CostMetrics {
	if br.costTracker != nil {
		if projection, err := br.costTracker.GetCostProjection(context.Background(), m); err == nil {
			return metrics.CostMetrics{
				TotalCost:       projection.HourlyRate * m.Duration.Hours(),
				CostPerRead:     projection.CostPerOperation,
				CostPerWrite:    projection.CostPerOperation,
				HourlyCost:      projection.HourlyRate,
				Currency:        br.config.Cost.CurrencyCode,
				Provider:        br.config.Cost.Provider,
				CalculationMode: "realtime",
				Timestamp:       time.Now(),
			}
		}
	}

	// Fallback to basic cost calculation
	return br.costCalculator.CalculateCost(m)
}

// getCurrentPhaseMetadata determines the current benchmark phase and creates metadata
func (br *BenchmarkRunner) getCurrentPhaseMetadata() metrics.PhaseMetadata {
	// Get status once to ensure consistency between phase detection and metadata
	var status metrics.SaturationStatus
	var currentPhase metrics.BenchmarkPhase

	if br.saturationController != nil {
		status = br.saturationController.GetStatus()
		currentPhase = br.getCurrentPhaseWithStatus(status)
	} else {
		currentPhase = metrics.PhaseRampup
	}

	metadata := metrics.PhaseMetadata{
		CurrentPhase:           currentPhase,
		BenchmarkStartTime:     &br.startTime,
		TotalBenchmarkDuration: metrics.FormatDurationHuman(time.Since(br.startTime)),
	}

	if br.saturationController != nil {

		switch currentPhase {
		case metrics.PhaseRampup:
			metadata.RampupMetadata = &metrics.RampupMetadata{
				TargetCPU:          status.TargetCPU,
				CurrentCPU:         status.CurrentCPU,
				LastAdjustmentType: "qps_adjustment",
				AdjustmentReason:   fmt.Sprintf("CPU %.1f%% targeting %.1f%%", status.CurrentCPU, status.TargetCPU),
			}

		case metrics.PhaseStabilization:
			// Calculate stability progress and estimated completion time
			stabilityRequired := br.config.Workload.StabilityWindow
			stabilityElapsed := status.StabilityDuration
			progressPct := (float64(stabilityElapsed) / float64(stabilityRequired)) * 100
			if progressPct > 100 {
				progressPct = 100
			}

			estimatedCompletion := ""
			if progressPct < 100 {
				remaining := stabilityRequired - stabilityElapsed
				estimatedCompletion = time.Now().Add(remaining).Format("15:04:05")
			}

			metadata.StabilizationMetadata = &metrics.StabilizationMetadata{
				StabilityStartTime:        time.Now().Add(-stabilityElapsed),
				StabilityElapsed:          metrics.FormatDurationHuman(stabilityElapsed),
				StabilityRequired:         metrics.FormatDurationHuman(stabilityRequired),
				StabilityProgressPct:      progressPct,
				EstimatedMeasurementStart: estimatedCompletion,
				CPUInTargetRange:          status.IsStable,
			}

		case metrics.PhaseMeasurement:
			if !br.measurementStartTime.IsZero() {
				metadata.MeasurementStartTime = &br.measurementStartTime
				metadata.MeasurementDuration = metrics.FormatDurationHuman(time.Since(br.measurementStartTime))
			}

			confidence := "low"
			if status.StabilityDuration > 5*time.Minute {
				confidence = "high"
			} else if status.StabilityDuration > 2*time.Minute {
				confidence = "medium"
			}

			metadata.MeasurementMetadata = &metrics.MeasurementMetadata{
				MeasurementStartTime: br.measurementStartTime,
				CleanMetricsActive:   br.measurementActive,
				MetricsResetAt:       br.measurementStartTime,
				StabilityConfidence:  confidence,
				DataQuality:          "clean_stable_metrics",
			}
		}
	} else {
		// No saturation controller - assume basic rampup
		metadata.RampupMetadata = &metrics.RampupMetadata{
			TargetCPU:          br.config.Workload.SaturationTarget,
			CurrentCPU:         0.0,
			LastAdjustmentType: "manual",
			AdjustmentReason:   "saturation controller disabled",
		}
	}

	return metadata
}

// getCurrentPhase determines the current benchmark phase
func (br *BenchmarkRunner) getCurrentPhase() metrics.BenchmarkPhase {
	if br.saturationController != nil {
		status := br.saturationController.GetStatus()
		return br.getCurrentPhaseWithStatus(status)
	}

	// No saturation controller
	slog.Debug("Phase: rampup (no saturation controller)")
	return metrics.PhaseRampup
}

// getCurrentPhaseWithStatus determines the phase using a provided status to ensure consistency
func (br *BenchmarkRunner) getCurrentPhaseWithStatus(status metrics.SaturationStatus) metrics.BenchmarkPhase {
	// Debug logging to trace phase detection logic
	slog.Debug("Phase detection",
		"is_stable", status.IsStable,
		"measurement_active", br.measurementActive,
		"stability_duration", status.StabilityDuration,
		"current_cpu", status.CurrentCPU,
		"target_cpu", status.TargetCPU)

	// CRITICAL FIX: Measurement phase takes priority over stability status
	// Once measurement starts, stay in measurement phase even if stability fluctuates briefly
	if br.measurementActive {
		slog.Debug("Phase: measurement (measurement active - highest priority)")
		return metrics.PhaseMeasurement
	}

	if !status.IsStable {
		slog.Debug("Phase: rampup (not stable)")
		return metrics.PhaseRampup
	}

	// CPU stable but measurement not started = stabilization
	slog.Debug("Phase: stabilization (stable but measurement not active)")
	return metrics.PhaseStabilization
}

// getCurrentSystemState retrieves current system state from saturation controller
func (br *BenchmarkRunner) getCurrentSystemState() metrics.SystemState {
	if br.saturationController != nil {
		status := br.saturationController.GetStatus()

		// Determine stability confidence
		confidence := "low"
		if status.IsStable {
			if status.StabilityDuration > 5*time.Minute {
				confidence = "high"
			} else if status.StabilityDuration > 2*time.Minute {
				confidence = "medium"
			}
		}

		// Determine saturation status
		saturationStatus := "adjusting"
		if status.IsStable {
			saturationStatus = "stable"
		} else if status.CurrentCPU > 0 {
			if status.CurrentCPU < status.TargetCPU-5 {
				saturationStatus = "scaling_up"
			} else if status.CurrentCPU > status.TargetCPU+5 {
				saturationStatus = "scaling_down"
			} else {
				saturationStatus = "stabilizing"
			}
		}

		// Collect additional resource metrics from the enhanced database client
		var memoryUtilization, cacheUtilization float64
		var connectionCount int64

		if enhancedDB, ok := br.database.(*metrics.EnhancedDatabaseClient); ok {
			monitor := enhancedDB.GetMonitor()
			ctx := context.Background()

			// Get memory utilization
			if memory, err := monitor.GetMemoryUtilization(ctx); err == nil {
				memoryUtilization = memory
			}

			// Get cache utilization (may not be available for all providers)
			if cache, err := monitor.GetCacheUtilization(ctx); err == nil {
				cacheUtilization = cache
			}

			// Get connection count
			if connections, err := monitor.GetConnectionCount(ctx); err == nil {
				connectionCount = connections
			}
		}

		return metrics.SystemState{
			CPUUtilization:         status.CurrentCPU,
			TargetCPU:              status.TargetCPU,
			IsStable:               status.IsStable,
			StabilityDuration:      status.StabilityDuration,
			SaturationStatus:       saturationStatus,
			CPUDeviationFromTarget: status.CurrentCPU - status.TargetCPU,
			StabilityConfidence:    confidence,
			HistoryPoints:          status.HistoryPoints,

			// Enhanced resource metrics
			MemoryUtilization: memoryUtilization,
			CacheUtilization:  cacheUtilization,
			ConnectionCount:   connectionCount,

			Timestamp: time.Now(),
		}
	}

	// Return default system state if no saturation controller
	return metrics.SystemState{
		CPUUtilization:         0.0,
		TargetCPU:              br.config.Workload.SaturationTarget,
		IsStable:               false,
		StabilityDuration:      0,
		SaturationStatus:       "disabled",
		CPUDeviationFromTarget: 0.0,
		StabilityConfidence:    "none",
		HistoryPoints:          0,
		MemoryUtilization:      0.0,
		CacheUtilization:       0.0,
		ConnectionCount:        0,
		Timestamp:              time.Now(),
	}
}

// finalizeResults generates final benchmark results
func (br *BenchmarkRunner) finalizeResults(ctx context.Context) error {
	slog.Info("Finalizing benchmark results")

	// Stop workload
	br.workloadController.Stop()

	// Get final metrics
	finalMetrics := br.metricsCollector.GetCurrentMetrics()
	finalCostMetrics := br.calculateCurrentCost(finalMetrics)

	// Export final summary
	if err := br.exporter.ExportSummary(finalMetrics, finalCostMetrics); err != nil {
		slog.Error("Failed to export summary", "error", err)
	}

	// Log final results
	br.logFinalResults(finalMetrics, finalCostMetrics)

	return nil
}

// logFinalResults logs the final benchmark results
func (br *BenchmarkRunner) logFinalResults(metrics metrics.Metrics, costMetrics metrics.CostMetrics) {
	slog.Info("=== BENCHMARK COMPLETED ===")
	slog.Info("Performance Results",
		"duration", metrics.Duration,
		"total_operations", metrics.TotalOps,
		"read_operations", metrics.ReadOps,
		"write_operations", metrics.WriteOps,
		"average_qps", fmt.Sprintf("%.1f", metrics.TotalQPS),
		"error_rate", fmt.Sprintf("%.2f%%", metrics.ErrorRate*100))

	slog.Info("Cost Analysis",
		"total_cost", fmt.Sprintf("$%.6f", costMetrics.TotalCost),
		"cost_per_read", fmt.Sprintf("$%.8f", costMetrics.CostPerRead),
		"cost_per_write", fmt.Sprintf("$%.8f", costMetrics.CostPerWrite),
		"hourly_rate", fmt.Sprintf("$%.4f", costMetrics.HourlyCost),
		"provider", costMetrics.Provider)

	if br.saturationController != nil {
		status := br.saturationController.GetStatus()
		slog.Info("Saturation Results",
			"final_cpu", fmt.Sprintf("%.1f%%", status.CurrentCPU),
			"target_cpu", fmt.Sprintf("%.1f%%", status.TargetCPU),
			"achieved_stability", status.IsStable,
			"stability_duration", status.StabilityDuration)
	}
}

// handleMeasurementStateChange handles measurement state changes from saturation controller
func (br *BenchmarkRunner) handleMeasurementStateChange(active bool) {
	if active {
		// Saturation has stabilized - reset metrics to start clean measurement
		// Order is critical: Reset first, then set time, then set flag for atomic transition
		br.metricsCollector.Reset()          // ← Reset counters first
		br.measurementStartTime = time.Now() // ← Set measurement start time
		br.measurementActive = active        // ← Finally activate measurement flag
		slog.Info("Saturation achieved stability - starting measurement phase")
	} else {
		// Saturation became unstable - stop clean measurement
		br.measurementActive = active         // ← Deactivate measurement flag first
		br.measurementStartTime = time.Time{} // ← Reset measurement start time
		slog.Info("Saturation lost stability - ending measurement phase")
	}
}

// validateStateConsistency checks for inconsistencies between system state and phase metadata
func (br *BenchmarkRunner) validateStateConsistency(systemState metrics.SystemState, phaseMetadata metrics.PhaseMetadata) {
	// Check for the specific issue from the log: stable system but rampup phase
	if systemState.IsStable && systemState.StabilityDuration > time.Minute && phaseMetadata.CurrentPhase == metrics.PhaseRampup {
		slog.Warn("STATE INCONSISTENCY DETECTED",
			"issue", "system_stable_but_rampup_phase",
			"system_stable", systemState.IsStable,
			"stability_duration", systemState.StabilityDuration,
			"reported_phase", phaseMetadata.CurrentPhase,
			"measurement_active", br.measurementActive,
			"cpu_utilization", systemState.CPUUtilization,
			"target_cpu", systemState.TargetCPU,
			"saturation_status", systemState.SaturationStatus)
	}

	// Check for other potential inconsistencies
	if systemState.IsStable && phaseMetadata.CurrentPhase == metrics.PhaseRampup && systemState.StabilityDuration > 30*time.Second {
		slog.Debug("State transition delay detected",
			"stable_duration", systemState.StabilityDuration,
			"phase", phaseMetadata.CurrentPhase,
			"measurement_active", br.measurementActive)
	}

	// Validate measurement phase consistency
	if br.measurementActive && phaseMetadata.CurrentPhase != metrics.PhaseMeasurement {
		slog.Warn("MEASUREMENT STATE INCONSISTENCY",
			"measurement_active", br.measurementActive,
			"reported_phase", phaseMetadata.CurrentPhase,
			"expected_phase", metrics.PhaseMeasurement)
	}

	// Validate stabilization phase metadata
	if phaseMetadata.CurrentPhase == metrics.PhaseStabilization && phaseMetadata.StabilizationMetadata != nil {
		if !phaseMetadata.StabilizationMetadata.CPUInTargetRange {
			slog.Warn("STABILIZATION INCONSISTENCY",
				"phase", phaseMetadata.CurrentPhase,
				"cpu_in_target_range", phaseMetadata.StabilizationMetadata.CPUInTargetRange,
				"system_stable", systemState.IsStable)
		}
	}
}

// handleWorkloadAdjustment processes workload adjustment requests
func (br *BenchmarkRunner) handleWorkloadAdjustment(adjustment metrics.WorkloadAdjustment) {
	br.workloadController.HandleAdjustment(adjustment)
}

// Close closes all resources
func (br *BenchmarkRunner) Close() error {
	if br.database != nil {
		if err := br.database.Close(); err != nil {
			slog.Error("Failed to close database", "error", err)
		}
	}

	if br.costTracker != nil {
		// Cost tracker might need cleanup in the future
	}

	return nil
}

// createSpannerTables creates tables for Spanner database
func (br *BenchmarkRunner) createSpannerTables(ctx context.Context) error {
	if br.spannerClient == nil {
		return fmt.Errorf("Spanner client not available")
	}

	if br.config.Workload.Mode == "cost_model" {
		// Create tables for each shard
		slog.Info("Creating Spanner tables for shards", "text_shards", br.config.Workload.TextShards)
		for _, shardNum := range br.config.Workload.TextShards {
			tableName := br.getCollectionNameForShard(shardNum)
			slog.Info("Creating Spanner table", "table", tableName, "shard", shardNum)
			if err := br.spannerClient.CreateTable(ctx, tableName); err != nil {
				// Check if table already exists
				if !isTableAlreadyExistsError(err) {
					return fmt.Errorf("failed to create table %s: %w", tableName, err)
				}
				slog.Info("Table already exists", "table", tableName)
			}
		}
		slog.Info("Spanner tables created successfully", "count", len(br.config.Workload.TextShards))
	} else {
		// Create default table for benchmark mode
		tableName := br.config.Database.Collection
		slog.Info("Creating Spanner table", "table", tableName)
		if err := br.spannerClient.CreateTable(ctx, tableName); err != nil {
			if !isTableAlreadyExistsError(err) {
				return fmt.Errorf("failed to create table %s: %w", tableName, err)
			}
			slog.Info("Table already exists", "table", tableName)
		}
	}

	return nil
}

// isTableAlreadyExistsError checks if an error indicates a table already exists
func isTableAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return contains(errMsg, "already exists") || contains(errMsg, "Duplicate name")
}

// contains checks if a string contains a substring (case-insensitive helper)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			stringContains(s, substr)))
}

// stringContains is a simple substring check
func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// executeWriteBenchmark runs the write benchmark with sequential phases (CREATE → UPDATE → DELETE)
func (br *BenchmarkRunner) executeWriteBenchmark(ctx context.Context) error {
	writeOps := br.config.Workload.WriteOperations
	phaseDuration := br.config.Workload.WritePhaseDuration
	writeCollection := br.config.Workload.WriteCollection
	writeTokens := br.config.Workload.WriteTokens
	writeNoindexSize := br.config.Workload.WriteNoindexSize
	writeTokenSizes := br.config.Workload.WriteTokenSizes
	workerCount := br.config.Workload.WorkerCount
	targetQPS := br.config.Workload.TargetQPS

	totalDuration := phaseDuration * time.Duration(len(writeOps))

	datasetSize := br.config.Workload.DatasetSize

	slog.Info("Starting write benchmark",
		"phases", writeOps,
		"phase_duration", phaseDuration,
		"total_duration", totalDuration,
		"collection", writeCollection,
		"worker_count", workerCount,
		"target_qps", targetQPS,
		"write_tokens", writeTokens,
		"write_token_sizes", writeTokenSizes,
		"preload_count", datasetSize)

	// Phase 0: SETUP - create table (Spanner), create search index, then preload documents

	// Step 1: Create table (Spanner only) and search index
	if br.config.Database.Type == "spanner" && br.spannerClient != nil {
		// Spanner: create write-optimized table first
		slog.Info("Creating Spanner write table for write collection", "table", writeCollection)
		if err := br.spannerClient.CreateWriteTable(ctx, writeCollection); err != nil {
			if !isTableAlreadyExistsError(err) {
				return fmt.Errorf("failed to create Spanner write table %s: %w", writeCollection, err)
			}
			slog.Info("Write collection table already exists", "table", writeCollection)
		}

		// Spanner: create write-specific search index (text1_tokens only)
		slog.Info("Creating Spanner write search index on write collection", "collection", writeCollection)
		if err := br.spannerClient.CreateWriteSearchIndex(ctx, writeCollection); err != nil {
			slog.Warn("Failed to create Spanner write search index on write collection (may already exist)",
				"collection", writeCollection, "error", err)
		}
	} else {
		// MongoDB/Atlas: create search index on write collection
		slog.Info("Creating Atlas Search index on write collection", "collection", writeCollection)
		if err := br.database.CreateSearchIndexForCollection(ctx, writeCollection); err != nil {
			slog.Warn("Failed to create search index on write collection (may already exist)",
				"collection", writeCollection, "error", err)
		}
	}

	// Step 2: Preload dataset_size documents into write_collection so UPDATE/DELETE have data
	var allIDs []string

	if datasetSize > 0 {
		slog.Info("Phase 0: Preloading documents into write collection",
			"collection", writeCollection,
			"count", datasetSize)

		preloadRng := rand.New(rand.NewSource(time.Now().UnixNano()))
		batchSize := 1000
		preloadStart := time.Now()

		for i := 0; i < datasetSize; i += batchSize {
			remaining := datasetSize - i
			if remaining > batchSize {
				remaining = batchSize
			}

			for j := 0; j < remaining; j++ {
				// Generate deterministic ID
				idBytes := generator.NextPreloadIdBytes(preloadRng, generator.KeySize, 1, 0)
				id := generator.PreloadIdBytesToId(idBytes)

				// Randomly select token size
				tokenSize := writeTokenSizes[preloadRng.Intn(len(writeTokenSizes))]

				// Generate write document (create mode)
				doc := generator.MakeDatabaseTextWriteDocument(id, writeTokens, tokenSize, false, writeNoindexSize)

				// Upsert into write collection
				if err := br.database.ReplaceDocumentInCollection(ctx, writeCollection, id, doc); err != nil {
					slog.Warn("Preload document failed", "id", id, "error", err)
					continue
				}

				allIDs = append(allIDs, id)
			}

			if i%10000 == 0 && i > 0 {
				slog.Info("Preload progress", "inserted", i, "total", datasetSize)
			}
		}

		slog.Info("Phase 0: Preload completed",
			"documents", len(allIDs),
			"duration", time.Since(preloadStart))
	}

	// Reset metrics before starting write phases so preload/setup time doesn't affect QPS calculations
	br.metricsCollector.Reset()

	// Create a SHARED rate limiter for all write phases
	// This allows the saturation controller to adjust QPS and have it carry over between phases
	burst := targetQPS / 10
	if burst < 1 {
		burst = 1
	}
	sharedRateLimiter := rate.NewLimiter(rate.Limit(targetQPS), burst)

	// Start saturation controller NOW (after preload, before write phases)
	// This ensures CPU monitoring only covers actual write operations, not preload
	if br.saturationController != nil {
		// Override adjustment callback to directly adjust the shared write rate limiter
		// (The default callback goes to workloadController which manages read workers — not write workers)
		br.saturationController.SetAdjustmentCallback(func(adj metrics.WorkloadAdjustment) {
			currentLimit := float64(sharedRateLimiter.Limit())
			var newQPS float64
			switch adj.Type {
			case "qps_increase":
				newQPS = currentLimit * (1 + adj.Magnitude/100)
			case "qps_decrease":
				newQPS = currentLimit * (1 - adj.Magnitude/100)
				if newQPS < 10 {
					newQPS = 10
				}
			default:
				slog.Warn("Unknown adjustment type for write workload", "type", adj.Type)
				return
			}
			newBurst := int(newQPS) / 10
			if newBurst < 1 {
				newBurst = 1
			}
			sharedRateLimiter.SetLimit(rate.Limit(newQPS))
			sharedRateLimiter.SetBurst(newBurst)
			slog.Info("Write workload QPS adjusted by saturation controller",
				"type", adj.Type,
				"old_qps", fmt.Sprintf("%.1f", currentLimit),
				"new_qps", fmt.Sprintf("%.1f", newQPS),
				"cpu", fmt.Sprintf("%.1f%%", adj.CurrentCPU),
				"target_cpu", fmt.Sprintf("%.1f%%", adj.TargetCPU))
		})

		// Override measurement callback: do NOT reset metrics for write workloads
		// Write metrics accumulate across all phases (CREATE → UPDATE → DELETE)
		br.saturationController.SetMeasurementCallback(func(active bool) {
			if active {
				br.measurementStartTime = time.Now()
				br.measurementActive = true
				slog.Info("Saturation achieved stability during write workload - measurement tracking started (metrics NOT reset)")
			} else {
				br.measurementActive = false
				br.measurementStartTime = time.Time{}
				slog.Info("Saturation lost stability during write workload - measurement tracking stopped")
			}
		})

		go br.saturationController.Start(ctx)
		slog.Info("CPU saturation controller started for write workload (after preload)",
			"initial_qps", targetQPS,
			"target_cpu", br.config.Workload.SaturationTarget)
	}

	// Execute each write phase sequentially
	for phaseIdx, op := range writeOps {
		currentQPS := float64(sharedRateLimiter.Limit())
		slog.Info("=== STARTING WRITE PHASE ===",
			"phase", phaseIdx+1,
			"operation", op,
			"duration", phaseDuration,
			"total_phases", len(writeOps),
			"available_ids", len(allIDs),
			"current_qps", fmt.Sprintf("%.1f", currentQPS))

		// Create write workers for this phase
		workers := make([]*worker.WriteWorker, workerCount)
		workerWGs := make([]*sync.WaitGroup, workerCount)

		phaseCtx, phaseCancel := context.WithTimeout(ctx, phaseDuration)

		for i := 0; i < workerCount; i++ {
			dataGen := generator.NewDataGenerator(time.Now().UnixNano() + int64(i) + int64(phaseIdx)*1000)
			w := worker.NewWriteWorker(i, br.database, dataGen, br.metricsCollector, sharedRateLimiter)
			w.SetWriteTestMode(writeTokens, writeTokenSizes, writeCollection, writeNoindexSize)
			w.SetWriteOp(op)

			// Distribute existing IDs to workers for UPDATE/DELETE phases
			if len(allIDs) > 0 {
				workerIDs := distributeIDs(allIDs, workerCount, i)
				w.SetPreloadedIDs(workerIDs)
			}

			workers[i] = w
		}

		// Start all workers
		for i, w := range workers {
			wg := &sync.WaitGroup{}
			wg.Add(1)
			workerWGs[i] = wg
			go func(wk *worker.WriteWorker, wgp *sync.WaitGroup) {
				defer wgp.Done()
				wk.Start(phaseCtx)
			}(w, wg)
		}

		// Monitor phase progress
		ticker := time.NewTicker(30 * time.Second)
		phaseStart := time.Now()

	phaseLoop:
		for {
			select {
			case <-phaseCtx.Done():
				break phaseLoop
			case <-ctx.Done():
				phaseCancel()
				ticker.Stop()
				return ctx.Err()
			case <-ticker.C:
				elapsed := time.Since(phaseStart)
				m := br.metricsCollector.GetCurrentMetrics()
				slog.Info("Write phase progress",
					"phase", op,
					"elapsed", elapsed,
					"total_ops", m.TotalOps,
					"write_qps", fmt.Sprintf("%.1f", m.WriteQPS),
					"error_rate", fmt.Sprintf("%.2f%%", m.ErrorRate*100))
			}
		}
		ticker.Stop()

		// Wait for all workers to finish
		for _, wg := range workerWGs {
			wg.Wait()
		}

		phaseCancel()

		// Collect IDs from all workers for next phase
		// During CREATE: workers accumulate newly created IDs
		// During UPDATE: workers keep the same IDs (no change)
		// During DELETE: workers remove IDs as they delete documents
		allIDs = nil
		for _, w := range workers {
			workerIDs := w.GetPreloadedIDs()
			allIDs = append(allIDs, workerIDs...)
		}

		elapsed := time.Since(phaseStart)
		m := br.metricsCollector.GetCurrentMetrics()
		slog.Info("=== WRITE PHASE COMPLETED ===",
			"phase", phaseIdx+1,
			"operation", op,
			"duration", elapsed,
			"remaining_ids", len(allIDs),
			"total_ops", m.TotalOps,
			"write_qps", fmt.Sprintf("%.1f", m.WriteQPS))
	}

	slog.Info("Write benchmark completed",
		"total_phases", len(writeOps),
		"remaining_ids", len(allIDs))

	return nil
}

// distributeIDs splits a list of IDs across N workers, returning the portion for worker i
func distributeIDs(ids []string, numWorkers, workerIdx int) []string {
	if len(ids) == 0 || numWorkers <= 0 {
		return nil
	}

	// Shuffle to randomize distribution
	shuffled := make([]string, len(ids))
	copy(shuffled, ids)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	var result []string
	for i := workerIdx; i < len(shuffled); i += numWorkers {
		result = append(result, shuffled[i])
	}
	return result
}

// getIndexFieldsDescription returns a human-readable description of indexed fields for a given shard
func getIndexFieldsDescription(shardNumber int) string {
	switch shardNumber {
	case 1:
		return "text1"
	case 2:
		return "text1, text2"
	case 3:
		return "text1, text2, text3"
	default:
		return "text1, text2, text3 (all fields)"
	}
}
