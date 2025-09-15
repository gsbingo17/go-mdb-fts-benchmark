package metrics

import (
	"testing"
	"time"
)

func TestNewMetricsCollector(t *testing.T) {
	mc := NewMetricsCollector()
	if mc == nil {
		t.Fatal("Expected metrics collector to be created")
	}

	// Check initial state
	metrics := mc.GetCurrentMetrics()
	if metrics.ReadOps != 0 {
		t.Errorf("Expected initial read ops 0, got %d", metrics.ReadOps)
	}

	if metrics.WriteOps != 0 {
		t.Errorf("Expected initial write ops 0, got %d", metrics.WriteOps)
	}

	if metrics.ErrorCount != 0 {
		t.Errorf("Expected initial error count 0, got %d", metrics.ErrorCount)
	}
}

func TestRecordRead(t *testing.T) {
	mc := NewMetricsCollector()

	// Record successful reads
	mc.RecordRead(10*time.Millisecond, true)
	mc.RecordRead(20*time.Millisecond, true)
	mc.RecordRead(15*time.Millisecond, false) // Failed read

	metrics := mc.GetCurrentMetrics()

	if metrics.ReadOps != 3 {
		t.Errorf("Expected 3 read ops, got %d", metrics.ReadOps)
	}

	if metrics.ErrorCount != 1 {
		t.Errorf("Expected 1 error, got %d", metrics.ErrorCount)
	}

	if metrics.WriteOps != 0 {
		t.Errorf("Expected 0 write ops, got %d", metrics.WriteOps)
	}

	// Check average latency calculation
	expectedAvg := (10.0 + 20.0 + 15.0) / 3.0 // in milliseconds
	if abs(metrics.AvgReadLatency-expectedAvg) > 0.1 {
		t.Errorf("Expected avg read latency %.1f ms, got %.1f ms", expectedAvg, metrics.AvgReadLatency)
	}
}

func TestRecordWrite(t *testing.T) {
	mc := NewMetricsCollector()

	// Record successful writes
	mc.RecordWrite(30*time.Millisecond, true)
	mc.RecordWrite(40*time.Millisecond, true)
	mc.RecordWrite(25*time.Millisecond, false) // Failed write

	metrics := mc.GetCurrentMetrics()

	if metrics.WriteOps != 3 {
		t.Errorf("Expected 3 write ops, got %d", metrics.WriteOps)
	}

	if metrics.ErrorCount != 1 {
		t.Errorf("Expected 1 error, got %d", metrics.ErrorCount)
	}

	if metrics.ReadOps != 0 {
		t.Errorf("Expected 0 read ops, got %d", metrics.ReadOps)
	}

	// Check average latency calculation
	expectedAvg := (30.0 + 40.0 + 25.0) / 3.0 // in milliseconds
	if abs(metrics.AvgWriteLatency-expectedAvg) > 0.1 {
		t.Errorf("Expected avg write latency %.1f ms, got %.1f ms", expectedAvg, metrics.AvgWriteLatency)
	}
}

func TestQPSCalculation(t *testing.T) {
	mc := NewMetricsCollector()

	// Sleep to ensure some time passes
	time.Sleep(10 * time.Millisecond)

	// Record operations
	for i := 0; i < 100; i++ {
		mc.RecordRead(1*time.Millisecond, true)
	}

	for i := 0; i < 50; i++ {
		mc.RecordWrite(1*time.Millisecond, true)
	}

	metrics := mc.GetCurrentMetrics()

	if metrics.TotalOps != 150 {
		t.Errorf("Expected 150 total ops, got %d", metrics.TotalOps)
	}

	// QPS should be positive since time has elapsed
	if metrics.TotalQPS <= 0 {
		t.Errorf("Expected positive total QPS, got %f", metrics.TotalQPS)
	}

	if metrics.ReadQPS <= 0 {
		t.Errorf("Expected positive read QPS, got %f", metrics.ReadQPS)
	}

	if metrics.WriteQPS <= 0 {
		t.Errorf("Expected positive write QPS, got %f", metrics.WriteQPS)
	}

	// Check QPS calculation consistency
	expectedTotal := metrics.ReadQPS + metrics.WriteQPS
	if abs(metrics.TotalQPS-expectedTotal) > 0.01 {
		t.Errorf("QPS calculation inconsistent: total=%f, read+write=%f", metrics.TotalQPS, expectedTotal)
	}
}

func TestErrorRate(t *testing.T) {
	mc := NewMetricsCollector()

	// Record mix of successful and failed operations
	mc.RecordRead(1*time.Millisecond, true)   // success
	mc.RecordRead(1*time.Millisecond, true)   // success
	mc.RecordRead(1*time.Millisecond, false)  // error
	mc.RecordWrite(1*time.Millisecond, false) // error

	metrics := mc.GetCurrentMetrics()

	expectedErrorRate := 2.0 / 4.0 // 2 errors out of 4 operations
	if abs(metrics.ErrorRate-expectedErrorRate) > 0.01 {
		t.Errorf("Expected error rate %f, got %f", expectedErrorRate, metrics.ErrorRate)
	}
}

func TestReset(t *testing.T) {
	mc := NewMetricsCollector()

	// Record some operations
	mc.RecordRead(10*time.Millisecond, true)
	mc.RecordWrite(20*time.Millisecond, false)

	// Verify operations were recorded
	metrics := mc.GetCurrentMetrics()
	if metrics.TotalOps == 0 {
		t.Error("Expected operations to be recorded before reset")
	}

	// Reset and verify
	mc.Reset()
	metrics = mc.GetCurrentMetrics()

	if metrics.ReadOps != 0 {
		t.Errorf("Expected read ops 0 after reset, got %d", metrics.ReadOps)
	}

	if metrics.WriteOps != 0 {
		t.Errorf("Expected write ops 0 after reset, got %d", metrics.WriteOps)
	}

	if metrics.ErrorCount != 0 {
		t.Errorf("Expected error count 0 after reset, got %d", metrics.ErrorCount)
	}
}

func TestGetQPSWindow(t *testing.T) {
	mc := NewMetricsCollector()

	// Record operations and wait a bit
	mc.RecordRead(1*time.Millisecond, true)
	mc.RecordWrite(1*time.Millisecond, true)

	time.Sleep(50 * time.Millisecond)

	readQPS, writeQPS, totalQPS := mc.GetQPSWindow(10 * time.Millisecond)

	// Should have some QPS since time has elapsed
	if totalQPS <= 0 {
		t.Errorf("Expected positive QPS window, got %f", totalQPS)
	}

	if readQPS+writeQPS != totalQPS {
		t.Errorf("QPS window calculation inconsistent: read=%f, write=%f, total=%f",
			readQPS, writeQPS, totalQPS)
	}
}

func TestConcurrentAccess(t *testing.T) {
	mc := NewMetricsCollector()

	// Simulate concurrent access
	done := make(chan bool, 2)

	// Goroutine 1: Record reads
	go func() {
		for i := 0; i < 1000; i++ {
			mc.RecordRead(1*time.Millisecond, true)
		}
		done <- true
	}()

	// Goroutine 2: Record writes
	go func() {
		for i := 0; i < 1000; i++ {
			mc.RecordWrite(1*time.Millisecond, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	metrics := mc.GetCurrentMetrics()

	if metrics.ReadOps != 1000 {
		t.Errorf("Expected 1000 read ops, got %d", metrics.ReadOps)
	}

	if metrics.WriteOps != 1000 {
		t.Errorf("Expected 1000 write ops, got %d", metrics.WriteOps)
	}

	if metrics.TotalOps != 2000 {
		t.Errorf("Expected 2000 total ops, got %d", metrics.TotalOps)
	}
}

func TestPercentileLatency(t *testing.T) {
	mc := NewMetricsCollector()

	// Record operations with known latencies
	latencies := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	}

	for _, latency := range latencies {
		mc.RecordRead(latency, true)
	}

	// Test percentile calculation (simplified)
	p50 := mc.GetPercentileLatency(50, "read")
	if p50 == 0 {
		t.Error("Expected non-zero p50 latency")
	}

	p95 := mc.GetPercentileLatency(95, "read")
	if p95 == 0 {
		t.Error("Expected non-zero p95 latency")
	}

	// p95 should be >= p50
	if p95 < p50 {
		t.Errorf("Expected p95 (%v) >= p50 (%v)", p95, p50)
	}
}

// Helper function for floating point comparison
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
