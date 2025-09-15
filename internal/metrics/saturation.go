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
	monitor         DatabaseMonitor
	targetCPU       float64
	stabilityWindow time.Duration
	adjustmentStep  float64

	// Internal state
	mu                 sync.RWMutex
	currentCPU         float64
	cpuHistory         []CPUReading
	isStable           bool
	stabilityStart     time.Time
	adjustmentCallback func(adjustment WorkloadAdjustment)
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
	return &SaturationController{
		monitor:            monitor,
		targetCPU:          cfg.SaturationTarget,
		stabilityWindow:    cfg.StabilityWindow,
		adjustmentStep:     5.0, // 5% adjustment steps
		cpuHistory:         make([]CPUReading, 0, 100),
		adjustmentCallback: adjustmentCallback,
	}
}

// Start begins monitoring and adjustment loop
func (sc *SaturationController) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	slog.Info("Starting saturation controller",
		"target_cpu", sc.targetCPU,
		"stability_window", sc.stabilityWindow)

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
		"history_points", len(sc.cpuHistory))

	// Check if we need adjustment
	tolerance := 5.0 // 5% tolerance
	if cpu < sc.targetCPU-tolerance {
		// CPU too low - scale up workload
		adjustment := sc.calculateScaleUpAdjustment(cpu)
		sc.executeAdjustment(adjustment)
		sc.resetStability()
	} else if cpu > sc.targetCPU+tolerance {
		// CPU too high - scale down workload
		adjustment := sc.calculateScaleDownAdjustment(cpu)
		sc.executeAdjustment(adjustment)
		sc.resetStability()
	} else {
		// CPU in target range - check stability
		sc.checkStability()
	}

	return nil
}

// calculateScaleUpAdjustment determines how to increase workload
func (sc *SaturationController) calculateScaleUpAdjustment(currentCPU float64) WorkloadAdjustment {
	deficit := sc.targetCPU - currentCPU
	magnitude := (deficit / sc.targetCPU) * 100 // Convert to percentage

	// Cap the adjustment to prevent overshooting
	if magnitude > 20 {
		magnitude = 20
	}

	adjustmentType := "qps_increase"
	if magnitude > 15 {
		adjustmentType = "scale_up" // Add more workers for large adjustments
	}

	return WorkloadAdjustment{
		Type:       adjustmentType,
		Magnitude:  magnitude,
		Reason:     fmt.Sprintf("CPU %.1f%% below target %.1f%%", currentCPU, sc.targetCPU),
		CurrentCPU: currentCPU,
		TargetCPU:  sc.targetCPU,
		Timestamp:  time.Now(),
	}
}

// calculateScaleDownAdjustment determines how to decrease workload
func (sc *SaturationController) calculateScaleDownAdjustment(currentCPU float64) WorkloadAdjustment {
	excess := currentCPU - sc.targetCPU
	magnitude := (excess / sc.targetCPU) * 100 // Convert to percentage

	// Cap the adjustment
	if magnitude > 15 {
		magnitude = 15
	}

	adjustmentType := "qps_decrease"
	if magnitude > 10 {
		adjustmentType = "scale_down" // Remove workers for large adjustments
	}

	return WorkloadAdjustment{
		Type:       adjustmentType,
		Magnitude:  magnitude,
		Reason:     fmt.Sprintf("CPU %.1f%% above target %.1f%%", currentCPU, sc.targetCPU),
		CurrentCPU: currentCPU,
		TargetCPU:  sc.targetCPU,
		Timestamp:  time.Now(),
	}
}

// executeAdjustment sends the adjustment to the callback
func (sc *SaturationController) executeAdjustment(adjustment WorkloadAdjustment) {
	slog.Info("Executing workload adjustment",
		"type", adjustment.Type,
		"magnitude", adjustment.Magnitude,
		"reason", adjustment.Reason)

	if sc.adjustmentCallback != nil {
		sc.adjustmentCallback(adjustment)
	}
}

// checkStability determines if the system is stable at target CPU
func (sc *SaturationController) checkStability() {
	if !sc.isStable {
		// Check if we can start stability period
		if sc.isInTargetRange() {
			sc.isStable = true
			sc.stabilityStart = time.Now()
			slog.Info("Starting stability period", "cpu", sc.currentCPU)
		}
	} else {
		// Check if stability period is complete
		if time.Since(sc.stabilityStart) >= sc.stabilityWindow {
			if sc.isStabilityMaintained() {
				slog.Info("Stability achieved",
					"duration", time.Since(sc.stabilityStart),
					"cpu", sc.currentCPU)
			} else {
				sc.resetStability()
			}
		}
	}
}

// isInTargetRange checks if current CPU is within target range
func (sc *SaturationController) isInTargetRange() bool {
	tolerance := 5.0
	return sc.currentCPU >= sc.targetCPU-tolerance &&
		sc.currentCPU <= sc.targetCPU+tolerance
}

// isStabilityMaintained checks if stability has been maintained
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

	if sc.isStable {
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
