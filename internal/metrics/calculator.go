package metrics

import (
	"time"

	"mongodb-benchmarking-tool/internal/config"
)

// Metrics represents a snapshot of performance metrics
type Metrics struct {
	ReadOps         int64         `json:"read_ops"`
	WriteOps        int64         `json:"write_ops"`
	TotalOps        int64         `json:"total_ops"`
	ErrorCount      int64         `json:"error_count"`
	Duration        time.Duration `json:"duration"`
	ReadQPS         float64       `json:"read_qps"`
	WriteQPS        float64       `json:"write_qps"`
	TotalQPS        float64       `json:"total_qps"`
	AvgReadLatency  float64       `json:"avg_read_latency_ms"`
	AvgWriteLatency float64       `json:"avg_write_latency_ms"`
	ErrorRate       float64       `json:"error_rate"`
	Timestamp       time.Time     `json:"timestamp"`
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
