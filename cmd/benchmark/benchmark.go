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
	config               *config.Config
	database             database.Database
	metricsCollector     *metrics.MetricsCollector
	costCalculator       *metrics.CostCalculator
	exporter             *metrics.Exporter
	workloadController   *worker.WorkloadController
	saturationController *metrics.SaturationController
	costTracker          metrics.RealTimeCostTracker

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
		db = database.NewDocumentDBClient(cfg.Database)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
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

	// Phase 2: Setup data and indexes
	if err := br.setupData(ctx); err != nil {
		return fmt.Errorf("data setup failed: %w", err)
	}

	// Phase 3: Warmup period
	if err := br.warmupPhase(ctx); err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	// Phase 4: Start monitoring and control systems
	if err := br.startMonitoring(ctx); err != nil {
		return fmt.Errorf("monitoring startup failed: %w", err)
	}

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
	slog.Info("Setting up database schema and data")

	// Create text indexes
	if err := br.database.CreateTextIndex(ctx); err != nil {
		return fmt.Errorf("failed to create text index: %w", err)
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

	slog.Info("Database setup completed", "document_count", requiredDocuments)
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
	if br.saturationController != nil {
		go br.saturationController.Start(ctx)
		slog.Info("CPU saturation controller started")
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
	metrics := br.metricsCollector.GetCurrentMetrics()

	var saturationStatus string
	if br.saturationController != nil {
		status := br.saturationController.GetStatus()
		saturationStatus = fmt.Sprintf("cpu=%.1f%% target=%.1f%% stable=%v",
			status.CurrentCPU, status.TargetCPU, status.IsStable)
	}

	slog.Info("Benchmark progress",
		"elapsed", metrics.Duration,
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
	metrics := br.metricsCollector.GetCurrentMetrics()
	costMetrics := br.calculateCurrentCost(metrics)
	systemState := br.getCurrentSystemState()

	if err := br.exporter.ExportMetrics(metrics, costMetrics, systemState); err != nil {
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
