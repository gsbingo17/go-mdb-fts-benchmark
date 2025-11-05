package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"mongodb-benchmarking-tool/internal/config"
	"mongodb-benchmarking-tool/internal/database"
	"mongodb-benchmarking-tool/internal/generator"
	"mongodb-benchmarking-tool/internal/metrics"
	"mongodb-benchmarking-tool/internal/worker"
)

// BenchmarkRunner orchestrates the complete benchmarking process
type BenchmarkRunner struct {
	config             *config.Config
	database           database.Database
	metricsCollector   *metrics.MetricsCollector
	costCalculator     *metrics.CostCalculator
	exporter           *metrics.Exporter
	workloadController *worker.WorkloadController
	costTracker        metrics.RealTimeCostTracker

	startTime time.Time
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
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}

	// Create metrics components
	metricsCollector := metrics.NewMetricsCollector()
	costCalculator := metrics.NewCostCalculator(cfg.Cost)
	exporter := metrics.NewExporter(cfg.Metrics.ExportPath, cfg.Metrics.ExportFormat)

	// Create workload controller
	workloadController := worker.NewWorkloadController(cfg.Workload, db, metricsCollector)

	// Create cost tracker
	costTracker, err := metrics.CreateRealTimeCostTracker(*cfg)
	if err != nil {
		slog.Warn("Failed to create cost tracker, using basic cost calculation", "error", err)
	}

	runner := &BenchmarkRunner{
		config:             cfg,
		database:           db,
		metricsCollector:   metricsCollector,
		costCalculator:     costCalculator,
		exporter:           exporter,
		workloadController: workloadController,
		costTracker:        costTracker,
	}

	return runner, nil
}

// Run executes the complete benchmark
func (br *BenchmarkRunner) Run(ctx context.Context) error {
	br.startTime = time.Now()

	slog.Info("Starting benchmark run",
		"target_operations", br.config.Workload.TargetOperations,
		"warmup_operations", br.config.Workload.WarmupOperations,
		"workers", br.config.Workload.WorkerCount,
		"database", br.config.Database.Type)

	// Phase 1: Connect to database
	if err := br.connectDatabase(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	// Phase 2: Setup data and indexes
	if err := br.setupData(ctx); err != nil {
		return fmt.Errorf("data setup failed: %w", err)
	}

	// Phase 3: Warmup period (if configured)
	if err := br.warmupPhase(ctx); err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	// Phase 4: Start metrics export
	go br.metricsExportLoop(ctx)

	// Phase 5: Execute main benchmark
	if err := br.executeBenchmark(ctx); err != nil {
		return fmt.Errorf("benchmark execution failed: %w", err)
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

// setupData prepares the database with test data and indexes
func (br *BenchmarkRunner) setupData(ctx context.Context) error {
	slog.Info("Setting up database schema and data", "benchmark_mode", br.config.Workload.BenchmarkMode)

	// Create indexes based on benchmark mode
	switch br.config.Workload.BenchmarkMode {
	case "text_search":
		if err := br.database.CreateTextIndex(ctx); err != nil {
			return fmt.Errorf("failed to create text index: %w", err)
		}
		slog.Info("Created text index for text search benchmark")
	case "field_query":
		if err := br.database.CreateFieldIndex(ctx, "title"); err != nil {
			return fmt.Errorf("failed to create field index: %w", err)
		}
		slog.Info("Created field index for field query benchmark")
	default:
		// Default to text search
		if err := br.database.CreateTextIndex(ctx); err != nil {
			return fmt.Errorf("failed to create text index: %w", err)
		}
		slog.Info("Created text index (default mode)")
	}

	// Check if we need to seed data
	count, err := br.database.CountDocuments(ctx)
	if err != nil {
		return fmt.Errorf("failed to count documents: %w", err)
	}

	requiredDocuments := int64(br.config.Workload.DatasetSize)
	if count < requiredDocuments {
		slog.Info("Seeding database with test data",
			"existing", count,
			"required", requiredDocuments)

		if err := br.seedData(ctx, int(requiredDocuments-count)); err != nil {
			return fmt.Errorf("data seeding failed: %w", err)
		}
	}

	slog.Info("Database setup completed", "document_count", requiredDocuments, "benchmark_mode", br.config.Workload.BenchmarkMode)
	return nil
}

// seedData generates and inserts test data
func (br *BenchmarkRunner) seedData(ctx context.Context, count int) error {
	dataGen := generator.NewDataGenerator(time.Now().UnixNano())
	batchSize := 1000

	for i := 0; i < count; i += batchSize {
		remaining := count - i
		if remaining > batchSize {
			remaining = batchSize
		}

		docs := dataGen.GenerateDocuments(remaining)
		if err := br.database.InsertDocuments(ctx, docs); err != nil {
			return fmt.Errorf("batch insert failed: %w", err)
		}

		if i%10000 == 0 {
			slog.Info("Data seeding progress", "inserted", i, "total", count)
		}
	}

	return nil
}

// warmupPhase runs warmup operations if configured
func (br *BenchmarkRunner) warmupPhase(ctx context.Context) error {
	if br.config.Workload.WarmupOperations == 0 {
		slog.Info("Skipping warmup phase (warmup_operations = 0)")
		return nil
	}

	slog.Info("Starting warmup phase", "operations", br.config.Workload.WarmupOperations)

	// Calculate warmup read/write operations based on ratio
	totalWarmup := br.config.Workload.WarmupOperations
	readPct := float64(br.config.Workload.ReadWriteRatio.ReadPercent) / 100.0
	warmupReadOps := int64(float64(totalWarmup) * readPct)
	warmupWriteOps := totalWarmup - warmupReadOps

	// Start workers with warmup target
	if err := br.workloadController.StartWithTarget(warmupReadOps, warmupWriteOps); err != nil {
		return fmt.Errorf("failed to start warmup: %w", err)
	}

	// Wait for warmup completion with progress logging
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			br.workloadController.Stop()
			return ctx.Err()
		case <-br.workloadController.CompletionSignal():
			slog.Info("Warmup phase completed",
				"operations", totalWarmup,
				"duration", time.Since(br.startTime))
			br.workloadController.Stop()
			time.Sleep(1 * time.Second) // Brief pause between phases
			br.metricsCollector.Reset() // Reset metrics after warmup
			return nil
		case <-ticker.C:
			readCompleted, writeCompleted, readTarget, writeTarget := br.workloadController.GetProgress()
			totalCompleted := readCompleted + writeCompleted
			progressPct := float64(totalCompleted) / float64(totalWarmup) * 100
			slog.Info("Warmup progress",
				"completed", totalCompleted,
				"target", totalWarmup,
				"progress", fmt.Sprintf("%.1f%%", progressPct),
				"reads", fmt.Sprintf("%d/%d", readCompleted, readTarget),
				"writes", fmt.Sprintf("%d/%d", writeCompleted, writeTarget))
		}
	}
}


// executeBenchmark runs the main benchmark workload
func (br *BenchmarkRunner) executeBenchmark(ctx context.Context) error {
	slog.Info("Starting main benchmark execution",
		"target_operations", br.config.Workload.TargetOperations)

	// Calculate read/write operations based on ratio
	totalOps := br.config.Workload.TargetOperations
	readPct := float64(br.config.Workload.ReadWriteRatio.ReadPercent) / 100.0
	targetReadOps := int64(float64(totalOps) * readPct)
	targetWriteOps := totalOps - targetReadOps

	slog.Info("Operation targets",
		"total", totalOps,
		"reads", targetReadOps,
		"writes", targetWriteOps,
		"ratio", fmt.Sprintf("%d:%d", br.config.Workload.ReadWriteRatio.ReadPercent, br.config.Workload.ReadWriteRatio.WritePercent))

	// Start workers with operation targets
	if err := br.workloadController.StartWithTarget(targetReadOps, targetWriteOps); err != nil {
		return fmt.Errorf("failed to start benchmark: %w", err)
	}

	// Monitor benchmark progress
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Benchmark cancelled")
			br.workloadController.Stop()
			return ctx.Err()
		case <-br.workloadController.CompletionSignal():
			slog.Info("All operations completed",
				"total_operations", totalOps,
				"duration", time.Since(br.startTime))
			return nil
		case <-ticker.C:
			br.logProgress()
		}
	}
}

// logProgress logs current benchmark progress
func (br *BenchmarkRunner) logProgress() {
	metrics := br.metricsCollector.GetCurrentMetrics()
	readCompleted, writeCompleted, readTarget, writeTarget := br.workloadController.GetProgress()
	
	totalCompleted := readCompleted + writeCompleted
	totalTarget := readTarget + writeTarget
	progressPct := float64(totalCompleted) / float64(totalTarget) * 100

	slog.Info("Benchmark progress",
		"elapsed", time.Since(br.startTime),
		"completed_ops", totalCompleted,
		"target_ops", totalTarget,
		"progress", fmt.Sprintf("%.1f%%", progressPct),
		"reads", fmt.Sprintf("%d/%d", readCompleted, readTarget),
		"writes", fmt.Sprintf("%d/%d", writeCompleted, writeTarget),
		"read_qps", fmt.Sprintf("%.1f", metrics.ReadQPS),
		"write_qps", fmt.Sprintf("%.1f", metrics.WriteQPS),
		"error_rate", fmt.Sprintf("%.2f%%", metrics.ErrorRate*100))
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
	currentMetrics := br.metricsCollector.GetCurrentMetrics()
	costMetrics := br.calculateCurrentCost(currentMetrics)

	// Create simplified system state (no saturation tracking)
	systemState := metrics.SystemState{
		CPUUtilization:         0.0,
		TargetCPU:              0.0,
		IsStable:               true, // Always stable in operation-count mode
		StabilityDuration:      time.Since(br.startTime),
		SaturationStatus:       "operation_count_mode",
		CPUDeviationFromTarget: 0.0,
		StabilityConfidence:    "n/a",
		HistoryPoints:          0,
		MemoryUtilization:      0.0,
		CacheUtilization:       0.0,
		ConnectionCount:        0,
		Timestamp:              time.Now(),
	}

	// Create simplified phase metadata (no saturation phases)
	readCompleted, writeCompleted, readTarget, writeTarget := br.workloadController.GetProgress()
	phaseMetadata := metrics.PhaseMetadata{
		CurrentPhase:           metrics.PhaseRampup, // Always in execution phase
		BenchmarkStartTime:     &br.startTime,
		TotalBenchmarkDuration: metrics.FormatDurationHuman(time.Since(br.startTime)),
		RampupMetadata: &metrics.RampupMetadata{
			TargetCPU:          0.0,
			CurrentCPU:         0.0,
			LastAdjustmentType: "operation_count",
			AdjustmentReason: fmt.Sprintf("Progress: %d/%d ops (%.1f%%)",
				readCompleted+writeCompleted,
				readTarget+writeTarget,
				float64(readCompleted+writeCompleted)/float64(readTarget+writeTarget)*100),
		},
	}

	if err := br.exporter.ExportMetrics(currentMetrics, costMetrics, systemState, phaseMetadata); err != nil {
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
