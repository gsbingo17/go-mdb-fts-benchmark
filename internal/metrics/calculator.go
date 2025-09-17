package metrics

import (
	"encoding/json"
	"fmt"
	"time"

	"mongodb-benchmarking-tool/internal/config"
)

// BenchmarkPhase represents the current phase of the benchmark
type BenchmarkPhase string

const (
	PhaseRampup        BenchmarkPhase = "rampup"
	PhaseStabilization BenchmarkPhase = "stabilization"
	PhaseMeasurement   BenchmarkPhase = "measurement"
)

// PhaseMetadata contains detailed information about the current benchmark phase
type PhaseMetadata struct {
	CurrentPhase BenchmarkPhase `json:"current_phase"`

	// Timing information
	BenchmarkStartTime     *time.Time `json:"benchmark_start_time,omitempty"`
	MeasurementStartTime   *time.Time `json:"measurement_start_time,omitempty"`
	TotalBenchmarkDuration string     `json:"total_benchmark_duration,omitempty"`
	MeasurementDuration    string     `json:"measurement_duration,omitempty"`

	// Phase-specific details
	RampupMetadata        *RampupMetadata        `json:"rampup_metadata,omitempty"`
	StabilizationMetadata *StabilizationMetadata `json:"stabilization_metadata,omitempty"`
	MeasurementMetadata   *MeasurementMetadata   `json:"measurement_metadata,omitempty"`
}

// RampupMetadata contains information about the ramp-up phase
type RampupMetadata struct {
	TargetCPU          float64 `json:"target_cpu"`
	CurrentCPU         float64 `json:"current_cpu"`
	LastAdjustmentType string  `json:"last_adjustment_type,omitempty"`
	AdjustmentReason   string  `json:"adjustment_reason,omitempty"`
}

// StabilizationMetadata contains information about the stabilization phase
type StabilizationMetadata struct {
	StabilityStartTime        time.Time `json:"stability_start_time"`
	StabilityElapsed          string    `json:"stability_elapsed"`
	StabilityRequired         string    `json:"stability_required"`
	StabilityProgressPct      float64   `json:"stability_progress_percent"`
	EstimatedMeasurementStart string    `json:"estimated_measurement_start,omitempty"`
	CPUInTargetRange          bool      `json:"cpu_in_target_range"`
}

// MeasurementMetadata contains information about the measurement phase
type MeasurementMetadata struct {
	MeasurementStartTime time.Time `json:"measurement_start_time"`
	CleanMetricsActive   bool      `json:"clean_metrics_active"`
	MetricsResetAt       time.Time `json:"metrics_reset_at"`
	StabilityConfidence  string    `json:"stability_confidence"`
	DataQuality          string    `json:"data_quality"`
}

// Metrics represents a snapshot of performance metrics
type Metrics struct {
	ReadOps         int64         `json:"read_ops"`
	WriteOps        int64         `json:"write_ops"`
	TotalOps        int64         `json:"total_ops"`
	ErrorCount      int64         `json:"error_count"`
	Duration        time.Duration `json:"duration"`
	DurationHuman   string        `json:"duration_human"`
	ReadQPS         float64       `json:"read_qps"`
	WriteQPS        float64       `json:"write_qps"`
	TotalQPS        float64       `json:"total_qps"`
	AvgReadLatency  float64       `json:"avg_read_latency_ms"`
	AvgWriteLatency float64       `json:"avg_write_latency_ms"`
	ErrorRate       float64       `json:"error_rate"`
	Timestamp       time.Time     `json:"timestamp"`
}

// MarshalJSON provides custom JSON marshaling for Metrics
func (m Metrics) MarshalJSON() ([]byte, error) {
	type Alias Metrics
	return json.Marshal(&struct {
		Duration      string `json:"duration"`
		DurationHuman string `json:"duration_human"`
		*Alias
	}{
		Duration:      formatDuration(m.Duration),
		DurationHuman: formatDurationHuman(m.Duration),
		Alias:         (*Alias)(&m),
	})
}

// CostMetrics represents cost calculation results
type CostMetrics struct {
	TotalCost       float64   `json:"total_cost"`
	CostPerRead     float64   `json:"cost_per_read"`
	CostPerWrite    float64   `json:"cost_per_write"`
	HourlyCost      float64   `json:"hourly_cost"`
	StorageCost     float64   `json:"storage_cost"`
	ComputeCost     float64   `json:"compute_cost"`
	IOCost          float64   `json:"io_cost,omitempty"`
	Currency        string    `json:"currency"`
	Provider        string    `json:"provider"`
	CalculationMode string    `json:"calculation_mode"`
	Timestamp       time.Time `json:"timestamp"`
}

// MarshalJSON provides custom JSON marshaling for CostMetrics
func (c CostMetrics) MarshalJSON() ([]byte, error) {
	type Alias CostMetrics
	return json.Marshal(&struct {
		CostPerRead  string `json:"cost_per_read"`
		CostPerWrite string `json:"cost_per_write"`
		*Alias
	}{
		CostPerRead:  formatCost(c.CostPerRead),
		CostPerWrite: formatCost(c.CostPerWrite),
		Alias:        (*Alias)(&c),
	})
}

// formatDuration formats a duration as seconds with 1 decimal place
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// formatDurationHuman formats a duration in human-readable format
func formatDurationHuman(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}

	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60

	if minutes < 60 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}

	hours := minutes / 60
	minutes = minutes % 60

	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}

// FormatDurationHuman formats a duration in human-readable format (public wrapper)
func FormatDurationHuman(d time.Duration) string {
	return formatDurationHuman(d)
}

// formatCost formats cost values with appropriate precision
func formatCost(cost float64) string {
	if cost == 0 {
		return "0.000000"
	}
	if cost >= 0.001 {
		return fmt.Sprintf("%.6f", cost)
	}
	if cost >= 0.000001 {
		return fmt.Sprintf("%.8f", cost)
	}
	// Use scientific notation for very small values
	return fmt.Sprintf("%.2e", cost)
}

// SystemState represents the current saturation controller state
type SystemState struct {
	CPUUtilization         float64       `json:"cpu_utilization"`
	TargetCPU              float64       `json:"target_cpu"`
	IsStable               bool          `json:"is_stable"`
	StabilityDuration      time.Duration `json:"stability_duration"`
	SaturationStatus       string        `json:"saturation_controller_status"`
	CPUDeviationFromTarget float64       `json:"cpu_deviation_from_target"`
	StabilityConfidence    string        `json:"stability_confidence"`
	HistoryPoints          int           `json:"history_points"`

	// Memory and resource metrics
	MemoryUtilization float64 `json:"memory_utilization"`
	CacheUtilization  float64 `json:"cache_utilization"`
	ConnectionCount   int64   `json:"connection_count"`

	Timestamp time.Time `json:"timestamp"`
}

// MarshalJSON provides custom JSON marshaling for SystemState
func (s SystemState) MarshalJSON() ([]byte, error) {
	type Alias SystemState
	return json.Marshal(&struct {
		StabilityDuration string `json:"stability_duration"`
		*Alias
	}{
		StabilityDuration: formatDurationHuman(s.StabilityDuration),
		Alias:             (*Alias)(&s),
	})
}

// CostCalculator calculates operational costs
type CostCalculator struct {
	config config.CostConfig
}

// NewCostCalculator creates a new cost calculator
func NewCostCalculator(cfg config.CostConfig) *CostCalculator {
	return &CostCalculator{
		config: cfg,
	}
}

// CalculateCost calculates the cost metrics based on performance data
func (cc *CostCalculator) CalculateCost(metrics Metrics) CostMetrics {
	switch cc.config.Provider {
	case "atlas":
		return cc.calculateAtlasCost(metrics)
	case "documentdb":
		return cc.calculateDocumentDBCost(metrics)
	default:
		return CostMetrics{
			Currency:        cc.config.CurrencyCode,
			Provider:        cc.config.Provider,
			CalculationMode: cc.config.CalculationMode,
			Timestamp:       time.Now(),
		}
	}
}

// calculateAtlasCost calculates costs for MongoDB Atlas
func (cc *CostCalculator) calculateAtlasCost(metrics Metrics) CostMetrics {
	hourlyRate := cc.config.Atlas.HourlyCost
	hoursElapsed := metrics.Duration.Hours()

	computeCost := hourlyRate * hoursElapsed

	// Storage cost calculation (simplified)
	storageCost := 0.0 // Would need actual storage usage data

	totalCost := computeCost + storageCost

	var costPerRead, costPerWrite float64
	if metrics.ReadOps > 0 {
		costPerRead = totalCost * (float64(metrics.ReadOps) / float64(metrics.TotalOps)) / float64(metrics.ReadOps)
	}
	if metrics.WriteOps > 0 {
		costPerWrite = totalCost * (float64(metrics.WriteOps) / float64(metrics.TotalOps)) / float64(metrics.WriteOps)
	}

	return CostMetrics{
		TotalCost:       totalCost,
		CostPerRead:     costPerRead,
		CostPerWrite:    costPerWrite,
		HourlyCost:      hourlyRate,
		StorageCost:     storageCost,
		ComputeCost:     computeCost,
		Currency:        cc.config.CurrencyCode,
		Provider:        cc.config.Provider,
		CalculationMode: cc.config.CalculationMode,
		Timestamp:       time.Now(),
	}
}

// calculateDocumentDBCost calculates costs for AWS DocumentDB
func (cc *CostCalculator) calculateDocumentDBCost(metrics Metrics) CostMetrics {
	hourlyRate := cc.config.DocumentDB.HourlyCost
	hoursElapsed := metrics.Duration.Hours()

	computeCost := hourlyRate * hoursElapsed

	// I/O cost calculation (DocumentDB charges for I/O)
	ioCost := (float64(metrics.TotalOps) / 1000000.0) * cc.config.DocumentDB.IOCostPer1MReqs

	// Storage cost calculation (simplified)
	storageCost := 0.0 // Would need actual storage usage data

	totalCost := computeCost + storageCost + ioCost

	var costPerRead, costPerWrite float64
	if metrics.ReadOps > 0 {
		costPerRead = totalCost * (float64(metrics.ReadOps) / float64(metrics.TotalOps)) / float64(metrics.ReadOps)
	}
	if metrics.WriteOps > 0 {
		costPerWrite = totalCost * (float64(metrics.WriteOps) / float64(metrics.TotalOps)) / float64(metrics.WriteOps)
	}

	return CostMetrics{
		TotalCost:       totalCost,
		CostPerRead:     costPerRead,
		CostPerWrite:    costPerWrite,
		HourlyCost:      hourlyRate,
		StorageCost:     storageCost,
		ComputeCost:     computeCost,
		IOCost:          ioCost,
		Currency:        cc.config.CurrencyCode,
		Provider:        cc.config.Provider,
		CalculationMode: cc.config.CalculationMode,
		Timestamp:       time.Now(),
	}
}

// EstimateHourlyCost estimates the hourly cost based on current QPS
func (cc *CostCalculator) EstimateHourlyCost(currentQPS float64) float64 {
	switch cc.config.Provider {
	case "atlas":
		return cc.config.Atlas.HourlyCost
	case "documentdb":
		// DocumentDB cost includes I/O charges
		baseCost := cc.config.DocumentDB.HourlyCost
		ioCostPerHour := (currentQPS * 3600.0 / 1000000.0) * cc.config.DocumentDB.IOCostPer1MReqs
		return baseCost + ioCostPerHour
	default:
		return 0.0
	}
}

// ProjectMonthlyCost projects monthly cost based on current usage patterns
func (cc *CostCalculator) ProjectMonthlyCost(metrics Metrics) float64 {
	hoursInMonth := 24.0 * 30.0 // Approximate

	switch cc.config.Provider {
	case "atlas":
		return cc.config.Atlas.HourlyCost * hoursInMonth
	case "documentdb":
		baseCost := cc.config.DocumentDB.HourlyCost * hoursInMonth
		avgQPS := metrics.TotalQPS
		ioCostPerMonth := (avgQPS * 3600.0 * 24.0 * 30.0 / 1000000.0) * cc.config.DocumentDB.IOCostPer1MReqs
		return baseCost + ioCostPerMonth
	default:
		return 0.0
	}
}
