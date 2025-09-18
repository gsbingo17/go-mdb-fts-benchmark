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

	startTime            time.Time
	measurementStartTime time.Time
	measurementActive    bool
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
