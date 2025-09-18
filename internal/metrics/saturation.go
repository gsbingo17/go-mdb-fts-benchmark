package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"mongodb-benchmarking-tool/internal/config"
)

// SaturationController manages CPU saturation and workload adjustment
type SaturationController struct {
	monitor            DatabaseMonitor
	targetCPU          float64
	stabilityWindow    time.Duration
	adjustmentCooldown time.Duration
	adjustmentStep     float64

	// Internal state
	mu                 sync.RWMutex
	currentCPU         float64
	cpuHistory         []CPUReading
	isStable           bool
	stabilityStart     time.Time
	lastAdjustment     time.Time
	adjustmentCallback func(adjustment WorkloadAdjustment)

	// Measurement phase tracking
	measurementActive   bool
	measurementCallback func(active bool) // Callback for measurement state changes
}

// CPUReading represents a CPU utilization reading
type CPUReading struct {
	Timestamp time.Time
	Value     float64
}

// WorkloadAdjustment represents a workload scaling decision
type WorkloadAdjustment struct {
	Type       string    `json:"type"`        // "scale_up", "scale_down", "qps_increase", "qps_decrease"
	Magnitude  float64   `json:"magnitude"`   // Adjustment amount (percentage or absolute)
	Reason     string    `json:"reason"`      // Why the adjustment was made
	CurrentCPU float64   `json:"current_cpu"` // Current CPU utilization
	TargetCPU  float64   `json:"target_cpu"`  // Target CPU utilization
	Timestamp  time.Time `json:"timestamp"`
}

// NewSaturationController creates a new saturation controller
func NewSaturationController(
	monitor DatabaseMonitor,
	cfg config.WorkloadConfig,
	adjustmentCallback func(WorkloadAdjustment),
) *SaturationController {
	// Default adjustment cooldown if not specified
	cooldown := cfg.AdjustmentCooldown
	if cooldown == 0 {
		cooldown = 3 * time.Minute // Default 3 minutes
	}

	return &SaturationController{
		monitor:            monitor,
		targetCPU:          cfg.SaturationTarget,
		stabilityWindow:    cfg.StabilityWindow,
		adjustmentCooldown: cooldown,
		adjustmentStep:     5.0, // 5% adjustment steps
		cpuHistory:         make([]CPUReading, 0, 100),
		adjustmentCallback: adjustmentCallback,
		measurementActive:  false,
	}
}

// SetMeasurementCallback sets the callback for measurement state changes
func (sc *SaturationController) SetMeasurementCallback(callback func(active bool)) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.measurementCallback = callback
}

// Start begins monitoring and adjustment loop
func (sc *SaturationController) Start(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second) // Check every 60 seconds (increased from 30s)
	defer ticker.Stop()

	slog.Info("Starting saturation controller",
		"target_cpu", sc.targetCPU,
		"stability_window", sc.stabilityWindow,
		"adjustment_cooldown", sc.adjustmentCooldown)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Saturation controller stopping")
			return
		case <-ticker.C:
			if err := sc.checkAndAdjust(ctx); err != nil {
				slog.Error("Saturation check failed", "error", err)
			}
		}
	}
}

// checkAndAdjust performs a single check and adjustment cycle
func (sc *SaturationController) checkAndAdjust(ctx context.Context) error {
	// Get current CPU utilization
	cpu, err := sc.monitor.GetCPUUtilization(ctx)
	if err != nil {
		// Log the error but continue with a default value to avoid stopping the benchmark
		fmt.Printf("WARN: Failed to get CPU utilization, using default: %v\n", err)
		cpu = 50.0 // Use a moderate default value
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Update current state
	sc.currentCPU = cpu
	sc.cpuHistory = append(sc.cpuHistory, CPUReading{
		Timestamp: time.Now(),
		Value:     cpu,
	})

	// Keep only recent history (last hour)
	cutoff := time.Now().Add(-time.Hour)
	for i, reading := range sc.cpuHistory {
		if reading.Timestamp.After(cutoff) {
			sc.cpuHistory = sc.cpuHistory[i:]
			break
		}
	}

	slog.Debug("CPU utilization check",
		"current_cpu", cpu,
		"target_cpu", sc.targetCPU,
		"history_points", len(sc.cpuHistory),
		"measurement_active", sc.measurementActive)

	// CRITICAL FIX: Do NOT make workload adjustments during measurement phase
	// Measurement phase must maintain stable workload for clean metrics
	if sc.measurementActive {
		slog.Debug("Skipping workload adjustment during measurement phase",
			"current_cpu", cpu,
			"target_cpu", sc.targetCPU,
			"measurement_active", sc.measurementActive)
		// Still check stability even during measurement
		sc.checkStability()
		return nil
	}

	// Check if we need adjustment (with cooldown protection)
	tolerance := 5.0 // 5% tolerance
	var timeSinceLastAdjustment time.Duration

	// CRITICAL FIX: Handle zero time to prevent overflow
	if !sc.lastAdjustment.IsZero() {
		timeSinceLastAdjustment = time.Since(sc.lastAdjustment)
	} else {
		timeSinceLastAdjustment = time.Duration(0)
	}

	if cpu < sc.targetCPU-tolerance {
		// CPU too low - scale up workload (if cooldown allows)
		if sc.canAdjust() {
			adjustment := sc.calculateScaleUpAdjustment(cpu)
			sc.executeAdjustment(adjustment)
			sc.resetStability()
		} else {
			slog.Debug("Scale-up needed but in cooldown period",
				"current_cpu", cpu,
				"target_cpu", sc.targetCPU,
				"time_since_last_adjustment", timeSinceLastAdjustment,
				"cooldown_remaining", sc.adjustmentCooldown-timeSinceLastAdjustment)
		}
	} else if cpu > sc.targetCPU+tolerance {
		// CPU too high - scale down workload (if cooldown allows)
		if sc.canAdjust() {
			adjustment := sc.calculateScaleDownAdjustment(cpu)
			sc.executeAdjustment(adjustment)
			sc.resetStability()
		} else {
			slog.Debug("Scale-down needed but in cooldown period",
				"current_cpu", cpu,
				"target_cpu", sc.targetCPU,
				"time_since_last_adjustment", timeSinceLastAdjustment,
				"cooldown_remaining", sc.adjustmentCooldown-timeSinceLastAdjustment)
		}
	} else {
		// CPU in target range - check stability
		sc.checkStability()
	}

	return nil
}

// calculateScaleUpAdjustment determines how to increase workload (QPS only)
func (sc *SaturationController) calculateScaleUpAdjustment(currentCPU float64) WorkloadAdjustment {
	deficit := sc.targetCPU - currentCPU
	magnitude := (deficit / sc.targetCPU) * 100 // Convert to percentage

	// Conservative scaling: Cap the adjustment to prevent overshooting
	if magnitude > 10 {
		magnitude = 10 // Increased max since we're not adding workers
	}

	// Minimum meaningful adjustment
	if magnitude < 2 {
		magnitude = 2
	}

	// Always use QPS increase, never change worker count
	return WorkloadAdjustment{
		Type:       "qps_increase",
		Magnitude:  magnitude,
		Reason:     fmt.Sprintf("CPU %.1f%% below target %.1f%% (QPS-only scaling)", currentCPU, sc.targetCPU),
		CurrentCPU: currentCPU,
		TargetCPU:  sc.targetCPU,
		Timestamp:  time.Now(),
	}
}

// calculateScaleDownAdjustment determines how to decrease workload (QPS only)
func (sc *SaturationController) calculateScaleDownAdjustment(currentCPU float64) WorkloadAdjustment {
	excess := currentCPU - sc.targetCPU
	magnitude := (excess / sc.targetCPU) * 100 // Convert to percentage

	// Conservative scaling: Cap the adjustment
	if magnitude > 8 { // Increased max since we're not removing workers
		magnitude = 8
	}

	// Minimum meaningful adjustment
	if magnitude < 1 {
		magnitude = 1
	}

	// Always use QPS decrease, never change worker count
	return WorkloadAdjustment{
		Type:       "qps_decrease",
		Magnitude:  magnitude,
		Reason:     fmt.Sprintf("CPU %.1f%% above target %.1f%% (QPS-only scaling)", currentCPU, sc.targetCPU),
		CurrentCPU: currentCPU,
		TargetCPU:  sc.targetCPU,
		Timestamp:  time.Now(),
	}
}

// canAdjust checks if enough time has passed since the last adjustment
func (sc *SaturationController) canAdjust() bool {
	if sc.lastAdjustment.IsZero() {
		return true // No previous adjustment
	}
	return time.Since(sc.lastAdjustment) >= sc.adjustmentCooldown
}

// executeAdjustment sends the adjustment to the callback
func (sc *SaturationController) executeAdjustment(adjustment WorkloadAdjustment) {
	slog.Info("Executing workload adjustment",
		"type", adjustment.Type,
		"magnitude", adjustment.Magnitude,
		"reason", adjustment.Reason,
		"time_since_last_adjustment", time.Since(sc.lastAdjustment))

	// Update last adjustment time
	sc.lastAdjustment = time.Now()

	if sc.adjustmentCallback != nil {
		sc.adjustmentCallback(adjustment)
	}
}

// checkStability determines if the system is stable at target CPU
func (sc *SaturationController) checkStability() {
	previousStableState := sc.isStable
	stabilityConfirmed := false

	// Add comprehensive debug logging
	stabilityDuration := time.Duration(0)
	if sc.isStable && !sc.stabilityStart.IsZero() {
		stabilityDuration = time.Since(sc.stabilityStart)
	}

	slog.Debug("Checking stability",
		"current_cpu", sc.currentCPU,
		"target_cpu", sc.targetCPU,
		"is_stable", sc.isStable,
		"stability_duration", stabilityDuration,
		"stability_window", sc.stabilityWindow,
		"in_target_range", sc.isInTargetRange())

	if !sc.isStable {
		// Check if we can start stability period
		if sc.isInTargetRange() {
			sc.isStable = true
			sc.stabilityStart = time.Now()
			slog.Info("Starting stability period",
				"cpu", sc.currentCPU,
				"target_cpu", sc.targetCPU,
				"stability_window", sc.stabilityWindow)
		}
	} else {
		// System is stable, check if stability period is complete
		if stabilityDuration >= sc.stabilityWindow {
			// Check if we still maintain stability within the actual stability window
			if sc.isStabilityMaintainedInWindow() {
				stabilityConfirmed = true
				slog.Info("Stability window completed - stability confirmed",
					"duration", stabilityDuration,
					"stability_window", sc.stabilityWindow,
					"cpu", sc.currentCPU,
					"target_cpu", sc.targetCPU)
			} else {
				slog.Warn("Stability window completed but stability not maintained",
					"duration", stabilityDuration,
					"stability_window", sc.stabilityWindow,
					"cpu", sc.currentCPU,
					"target_cpu", sc.targetCPU)
				sc.resetStability()
			}
		} else {
			// Still in stability window, check if we're still in range
			if !sc.isInTargetRange() {
				slog.Warn("Lost stability during stability window",
					"duration", stabilityDuration,
					"cpu", sc.currentCPU,
					"target_cpu", sc.targetCPU)
				sc.resetStability()
			} else {
				slog.Debug("Stability window in progress",
					"duration", stabilityDuration,
					"remaining", sc.stabilityWindow-stabilityDuration,
					"cpu", sc.currentCPU)
			}
		}
	}

	// Handle measurement state transitions
	sc.handleMeasurementTransitions(previousStableState, stabilityConfirmed)
}

// handleMeasurementTransitions manages start/stop of measurement based on stability
func (sc *SaturationController) handleMeasurementTransitions(previousStable bool, stabilityConfirmed bool) {
	// Start measurement when stability is confirmed (after stability window)
	if stabilityConfirmed && !sc.measurementActive {
		sc.measurementActive = true
		slog.Info("Saturation stable - starting measurement phase")
		if sc.measurementCallback != nil {
			sc.measurementCallback(true)
		}
	}

	// Stop measurement if we lose stability
	if !sc.isStable && sc.measurementActive {
		sc.measurementActive = false
		slog.Info("Saturation unstable - stopping measurement phase")
		if sc.measurementCallback != nil {
			sc.measurementCallback(false)
		}
	}
}

// isInTargetRange checks if current CPU is within target range
func (sc *SaturationController) isInTargetRange() bool {
	tolerance := 5.0
	return sc.currentCPU >= sc.targetCPU-tolerance &&
		sc.currentCPU <= sc.targetCPU+tolerance
}

// isStabilityMaintained checks if stability has been maintained (legacy method)
func (sc *SaturationController) isStabilityMaintained() bool {
	if len(sc.cpuHistory) < 5 {
		return false
	}

	// Check last 5 readings for stability
	stabilityWindow := sc.cpuHistory[len(sc.cpuHistory)-5:]
	tolerance := 3.0 // Tighter tolerance for stability

	for _, reading := range stabilityWindow {
		if reading.Value < sc.targetCPU-tolerance ||
			reading.Value > sc.targetCPU+tolerance {
			return false
		}
	}

	return true
}

// isStabilityMaintainedInWindow checks if stability has been maintained within the actual stability window
func (sc *SaturationController) isStabilityMaintainedInWindow() bool {
	if sc.stabilityStart.IsZero() {
		return false
	}

	// Get readings since stability started
	stabilityStartTime := sc.stabilityStart
	readingsInWindow := make([]CPUReading, 0)

	for _, reading := range sc.cpuHistory {
		if reading.Timestamp.After(stabilityStartTime) || reading.Timestamp.Equal(stabilityStartTime) {
			readingsInWindow = append(readingsInWindow, reading)
		}
	}

	// Need at least 3 readings within the stability window
	if len(readingsInWindow) < 3 {
		slog.Debug("Insufficient readings in stability window",
			"readings_count", len(readingsInWindow),
			"stability_start", stabilityStartTime)
		return false
	}

	// Use same tolerance as initial stability check for consistency
	tolerance := 5.0
	outOfRangeCount := 0

	for _, reading := range readingsInWindow {
		if reading.Value < sc.targetCPU-tolerance || reading.Value > sc.targetCPU+tolerance {
			outOfRangeCount++
			slog.Debug("Reading outside target range during stability window",
				"reading_time", reading.Timestamp,
				"reading_value", reading.Value,
				"target_cpu", sc.targetCPU,
				"tolerance", tolerance)
		}
	}

	// Allow up to 20% of readings to be outside range to handle brief fluctuations
	maxOutOfRange := len(readingsInWindow) / 5
	if maxOutOfRange < 1 {
		maxOutOfRange = 1
	}

	isStable := outOfRangeCount <= maxOutOfRange

	slog.Debug("Stability window validation",
		"total_readings", len(readingsInWindow),
		"out_of_range_readings", outOfRangeCount,
		"max_allowed_out_of_range", maxOutOfRange,
		"is_stable", isStable,
		"stability_duration", time.Since(stabilityStartTime))

	return isStable
}

// resetStability resets the stability tracking
func (sc *SaturationController) resetStability() {
	sc.isStable = false
	sc.stabilityStart = time.Time{}
}

// GetStatus returns current controller status
func (sc *SaturationController) GetStatus() SaturationStatus {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	status := SaturationStatus{
		CurrentCPU:    sc.currentCPU,
		TargetCPU:     sc.targetCPU,
		IsStable:      sc.isStable,
		HistoryPoints: len(sc.cpuHistory),
		Timestamp:     time.Now(),
	}

	if sc.isStable && !sc.stabilityStart.IsZero() {
		status.StabilityDuration = time.Since(sc.stabilityStart)
	}

	return status
}

// SaturationStatus represents the current saturation state
type SaturationStatus struct {
	CurrentCPU        float64       `json:"current_cpu"`
	TargetCPU         float64       `json:"target_cpu"`
	IsStable          bool          `json:"is_stable"`
	StabilityDuration time.Duration `json:"stability_duration"`
	HistoryPoints     int           `json:"history_points"`
	Timestamp         time.Time     `json:"timestamp"`
}

// GetCPUHistory returns recent CPU history
func (sc *SaturationController) GetCPUHistory() []CPUReading {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Return a copy to avoid race conditions
	history := make([]CPUReading, len(sc.cpuHistory))
	copy(history, sc.cpuHistory)
	return history
}

// SetTargetCPU updates the target CPU utilization
func (sc *SaturationController) SetTargetCPU(target float64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.targetCPU = target
	sc.resetStability() // Reset stability when target changes

	slog.Info("Target CPU updated", "new_target", target)
}
