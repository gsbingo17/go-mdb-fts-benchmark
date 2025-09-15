package metrics

import (
	"testing"
	"time"

	"mongodb-benchmarking-tool/internal/config"
)

func TestNewCostCalculator(t *testing.T) {
	cfg := config.CostConfig{
		Provider:     "atlas",
		CurrencyCode: "USD",
		Atlas: config.AtlasCostConfig{
			HourlyCost: 0.50,
		},
	}

	calc := NewCostCalculator(cfg)
	if calc == nil {
		t.Fatal("Expected cost calculator to be created")
	}

	if calc.config.Provider != "atlas" {
		t.Errorf("Expected provider 'atlas', got '%s'", calc.config.Provider)
	}
}

func TestCalculateAtlasCost(t *testing.T) {
	cfg := config.CostConfig{
		Provider:     "atlas",
		CurrencyCode: "USD",
		Atlas: config.AtlasCostConfig{
			HourlyCost:       0.50,
			StorageCostPerGB: 0.25,
		},
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		ReadOps:  8000,
		WriteOps: 2000,
		TotalOps: 10000,
		Duration: 2 * time.Hour,
	}

	costMetrics := calc.CalculateCost(metrics)

	// Verify basic cost calculation
	expectedComputeCost := 0.50 * 2.0 // $0.50/hour * 2 hours
	if costMetrics.ComputeCost != expectedComputeCost {
		t.Errorf("Expected compute cost %f, got %f", expectedComputeCost, costMetrics.ComputeCost)
	}

	// Verify cost per operation
	expectedCostPerRead := (costMetrics.TotalCost * 0.8) / 8000 // 80% of cost for 8000 reads
	if abs(costMetrics.CostPerRead-expectedCostPerRead) > 0.000001 {
		t.Errorf("Expected cost per read %f, got %f", expectedCostPerRead, costMetrics.CostPerRead)
	}

	// Verify metadata
	if costMetrics.Provider != "atlas" {
		t.Errorf("Expected provider 'atlas', got '%s'", costMetrics.Provider)
	}

	if costMetrics.Currency != "USD" {
		t.Errorf("Expected currency 'USD', got '%s'", costMetrics.Currency)
	}
}

func TestCalculateDocumentDBCost(t *testing.T) {
	cfg := config.CostConfig{
		Provider:     "documentdb",
		CurrencyCode: "USD",
		DocumentDB: config.DocumentDBCostConfig{
			HourlyCost:      0.30,
			IOCostPer1MReqs: 0.20,
		},
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		ReadOps:  600000,  // 0.6M operations
		WriteOps: 400000,  // 0.4M operations
		TotalOps: 1000000, // 1M total operations
		Duration: 1 * time.Hour,
	}

	costMetrics := calc.CalculateCost(metrics)

	// Verify compute cost
	expectedComputeCost := 0.30 * 1.0 // $0.30/hour * 1 hour
	if costMetrics.ComputeCost != expectedComputeCost {
		t.Errorf("Expected compute cost %f, got %f", expectedComputeCost, costMetrics.ComputeCost)
	}

	// Verify I/O cost
	expectedIOCost := (1000000.0 / 1000000.0) * 0.20 // 1M ops = $0.20
	if costMetrics.IOCost != expectedIOCost {
		t.Errorf("Expected I/O cost %f, got %f", expectedIOCost, costMetrics.IOCost)
	}

	// Verify total cost includes I/O
	expectedTotal := expectedComputeCost + expectedIOCost
	if abs(costMetrics.TotalCost-expectedTotal) > 0.000001 {
		t.Errorf("Expected total cost %f, got %f", expectedTotal, costMetrics.TotalCost)
	}
}

func TestEstimateHourlyCost(t *testing.T) {
	tests := []struct {
		name        string
		config      config.CostConfig
		currentQPS  float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name: "atlas",
			config: config.CostConfig{
				Provider: "atlas",
				Atlas: config.AtlasCostConfig{
					HourlyCost: 0.50,
				},
			},
			currentQPS:  100,
			expectedMin: 0.50,
			expectedMax: 0.50,
		},
		{
			name: "documentdb with I/O",
			config: config.CostConfig{
				Provider: "documentdb",
				DocumentDB: config.DocumentDBCostConfig{
					HourlyCost:      0.30,
					IOCostPer1MReqs: 0.20,
				},
			},
			currentQPS:  1000, // 1000 QPS = 3.6M ops/hour
			expectedMin: 0.30, // Base cost
			expectedMax: 1.05, // Base + I/O cost (allow small variance)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewCostCalculator(tt.config)
			cost := calc.EstimateHourlyCost(tt.currentQPS)

			if cost < tt.expectedMin || cost > tt.expectedMax {
				t.Errorf("Expected cost between %f and %f, got %f",
					tt.expectedMin, tt.expectedMax, cost)
			}
		})
	}
}

func TestProjectMonthlyCost(t *testing.T) {
	cfg := config.CostConfig{
		Provider: "atlas",
		Atlas: config.AtlasCostConfig{
			HourlyCost: 0.50,
		},
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		TotalQPS: 100,
		Duration: 1 * time.Hour,
	}

	monthlyCost := calc.ProjectMonthlyCost(metrics)

	expectedMonthlyCost := 0.50 * 24 * 30 // $0.50/hour * 24 hours * 30 days
	if monthlyCost != expectedMonthlyCost {
		t.Errorf("Expected monthly cost %f, got %f", expectedMonthlyCost, monthlyCost)
	}
}

func TestProjectMonthlyDocumentDBCost(t *testing.T) {
	cfg := config.CostConfig{
		Provider: "documentdb",
		DocumentDB: config.DocumentDBCostConfig{
			HourlyCost:      0.30,
			IOCostPer1MReqs: 0.20,
		},
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		TotalQPS: 100, // 100 QPS = 360K ops/hour = 259.2M ops/month
		Duration: 1 * time.Hour,
	}

	monthlyCost := calc.ProjectMonthlyCost(metrics)

	// Base cost: $0.30 * 24 * 30 = $216
	expectedBaseCost := 0.30 * 24 * 30

	// I/O cost: 100 QPS * 3600 * 24 * 30 / 1M * $0.20 = ~$51.84
	expectedIOCost := (100 * 3600 * 24 * 30 / 1000000.0) * 0.20

	expectedTotal := expectedBaseCost + expectedIOCost

	if abs(monthlyCost-expectedTotal) > 1.0 { // Allow $1 tolerance
		t.Errorf("Expected monthly cost ~%f, got %f", expectedTotal, monthlyCost)
	}
}

func TestZeroOperations(t *testing.T) {
	cfg := config.CostConfig{
		Provider: "atlas",
		Atlas: config.AtlasCostConfig{
			HourlyCost: 0.50,
		},
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		ReadOps:  0,
		WriteOps: 0,
		TotalOps: 0,
		Duration: 1 * time.Hour,
	}

	costMetrics := calc.CalculateCost(metrics)

	// Cost per operation should be 0 when there are no operations
	if costMetrics.CostPerRead != 0 {
		t.Errorf("Expected cost per read 0 with no operations, got %f", costMetrics.CostPerRead)
	}

	if costMetrics.CostPerWrite != 0 {
		t.Errorf("Expected cost per write 0 with no operations, got %f", costMetrics.CostPerWrite)
	}

	// Total cost should still be computed based on time
	if costMetrics.TotalCost <= 0 {
		t.Error("Expected positive total cost even with no operations")
	}
}

func TestUnsupportedProvider(t *testing.T) {
	cfg := config.CostConfig{
		Provider:     "unsupported",
		CurrencyCode: "USD",
	}

	calc := NewCostCalculator(cfg)

	metrics := Metrics{
		ReadOps:  100,
		WriteOps: 100,
		Duration: 1 * time.Hour,
	}

	costMetrics := calc.CalculateCost(metrics)

	// Should return empty cost metrics with metadata
	if costMetrics.TotalCost != 0 {
		t.Error("Expected zero cost for unsupported provider")
	}

	if costMetrics.Provider != "unsupported" {
		t.Errorf("Expected provider 'unsupported', got '%s'", costMetrics.Provider)
	}
}
