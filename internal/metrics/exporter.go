package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Exporter handles metrics export in various formats
type Exporter struct {
	outputPath string
	format     string
}

// NewExporter creates a new metrics exporter
func NewExporter(outputPath, format string) *Exporter {
	return &Exporter{
		outputPath: outputPath,
		format:     format,
	}
}

// ExportMetrics exports performance and cost metrics with system state
func (e *Exporter) ExportMetrics(metrics Metrics, costMetrics CostMetrics, systemState SystemState) error {
	// Ensure output directory exists
	if err := os.MkdirAll(e.outputPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	switch e.format {
	case "json":
		return e.exportJSON(metrics, costMetrics, systemState)
	case "csv":
		return e.exportCSV(metrics, costMetrics, systemState)
	case "prometheus":
		return e.exportPrometheus(metrics, costMetrics, systemState)
	default:
		return fmt.Errorf("unsupported export format: %s", e.format)
	}
}

// exportJSON exports metrics in JSON format
func (e *Exporter) exportJSON(metrics Metrics, costMetrics CostMetrics, systemState SystemState) error {
	data := struct {
		Performance Metrics     `json:"performance"`
		Cost        CostMetrics `json:"cost"`
		SystemState SystemState `json:"system_state"`
		ExportTime  time.Time   `json:"export_time"`
	}{
		Performance: metrics,
		Cost:        costMetrics,
		SystemState: systemState,
		ExportTime:  time.Now(),
	}

	filename := filepath.Join(e.outputPath, fmt.Sprintf("metrics_%s.json",
		time.Now().Format("2006-01-02_15-04-05")))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}

// exportCSV exports metrics in CSV format
func (e *Exporter) exportCSV(metrics Metrics, costMetrics CostMetrics, systemState SystemState) error {
	filename := filepath.Join(e.outputPath, "metrics.csv")

	// Check if file exists to determine if we need headers
	needHeaders := true
	if _, err := os.Stat(filename); err == nil {
		needHeaders = false
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers if new file
	if needHeaders {
		headers := []string{
			"timestamp", "read_ops", "write_ops", "total_ops", "error_count",
			"read_qps", "write_qps", "total_qps", "avg_read_latency_ms", "avg_write_latency_ms",
			"error_rate", "total_cost", "cost_per_read", "cost_per_write", "provider",
			"cpu_utilization", "target_cpu", "is_stable", "stability_duration_seconds", "saturation_status",
			"memory_utilization", "cache_utilization", "connection_count",
		}
		if err := writer.Write(headers); err != nil {
			return fmt.Errorf("failed to write CSV headers: %w", err)
		}
	}

	// Write data row
	record := []string{
		metrics.Timestamp.Format(time.RFC3339),
		strconv.FormatInt(metrics.ReadOps, 10),
		strconv.FormatInt(metrics.WriteOps, 10),
		strconv.FormatInt(metrics.TotalOps, 10),
		strconv.FormatInt(metrics.ErrorCount, 10),
		strconv.FormatFloat(metrics.ReadQPS, 'f', 2, 64),
		strconv.FormatFloat(metrics.WriteQPS, 'f', 2, 64),
		strconv.FormatFloat(metrics.TotalQPS, 'f', 2, 64),
		strconv.FormatFloat(metrics.AvgReadLatency, 'f', 2, 64),
		strconv.FormatFloat(metrics.AvgWriteLatency, 'f', 2, 64),
		strconv.FormatFloat(metrics.ErrorRate, 'f', 4, 64),
		strconv.FormatFloat(costMetrics.TotalCost, 'f', 6, 64),
		strconv.FormatFloat(costMetrics.CostPerRead, 'f', 8, 64),
		strconv.FormatFloat(costMetrics.CostPerWrite, 'f', 8, 64),
		costMetrics.Provider,
		strconv.FormatFloat(systemState.CPUUtilization, 'f', 2, 64),
		strconv.FormatFloat(systemState.TargetCPU, 'f', 2, 64),
		strconv.FormatBool(systemState.IsStable),
		strconv.FormatFloat(systemState.StabilityDuration.Seconds(), 'f', 0, 64),
		systemState.SaturationStatus,
		strconv.FormatFloat(systemState.MemoryUtilization, 'f', 2, 64),
		strconv.FormatFloat(systemState.CacheUtilization, 'f', 2, 64),
		strconv.FormatInt(systemState.ConnectionCount, 10),
	}

	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}

	return nil
}

// exportPrometheus exports metrics in Prometheus format
func (e *Exporter) exportPrometheus(metrics Metrics, costMetrics CostMetrics, systemState SystemState) error {
	filename := filepath.Join(e.outputPath, "metrics.prom")

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create Prometheus file: %w", err)
	}
	defer file.Close()

	timestamp := metrics.Timestamp.Unix() * 1000 // Convert to milliseconds

	// Convert stability boolean to numeric value for Prometheus
	stabilityValue := 0.0
	if systemState.IsStable {
		stabilityValue = 1.0
	}

	// Write Prometheus metrics
	promMetrics := []string{
		fmt.Sprintf("benchmark_read_ops_total %d %d", metrics.ReadOps, timestamp),
		fmt.Sprintf("benchmark_write_ops_total %d %d", metrics.WriteOps, timestamp),
		fmt.Sprintf("benchmark_error_count_total %d %d", metrics.ErrorCount, timestamp),
		fmt.Sprintf("benchmark_read_qps %.2f %d", metrics.ReadQPS, timestamp),
		fmt.Sprintf("benchmark_write_qps %.2f %d", metrics.WriteQPS, timestamp),
		fmt.Sprintf("benchmark_total_qps %.2f %d", metrics.TotalQPS, timestamp),
		fmt.Sprintf("benchmark_read_latency_ms %.2f %d", metrics.AvgReadLatency, timestamp),
		fmt.Sprintf("benchmark_write_latency_ms %.2f %d", metrics.AvgWriteLatency, timestamp),
		fmt.Sprintf("benchmark_error_rate %.4f %d", metrics.ErrorRate, timestamp),
		fmt.Sprintf("benchmark_total_cost %.6f %d", costMetrics.TotalCost, timestamp),
		fmt.Sprintf("benchmark_cost_per_read %.8f %d", costMetrics.CostPerRead, timestamp),
		fmt.Sprintf("benchmark_cost_per_write %.8f %d", costMetrics.CostPerWrite, timestamp),
		fmt.Sprintf("benchmark_cpu_utilization %.2f %d", systemState.CPUUtilization, timestamp),
		fmt.Sprintf("benchmark_target_cpu %.2f %d", systemState.TargetCPU, timestamp),
		fmt.Sprintf("benchmark_is_stable %.0f %d", stabilityValue, timestamp),
		fmt.Sprintf("benchmark_stability_duration_seconds %.0f %d", systemState.StabilityDuration.Seconds(), timestamp),
		fmt.Sprintf("benchmark_memory_utilization %.2f %d", systemState.MemoryUtilization, timestamp),
		fmt.Sprintf("benchmark_cache_utilization %.2f %d", systemState.CacheUtilization, timestamp),
		fmt.Sprintf("benchmark_connection_count %d %d", systemState.ConnectionCount, timestamp),
	}

	for _, metric := range promMetrics {
		if _, err := file.WriteString(metric + "\n"); err != nil {
			return fmt.Errorf("failed to write Prometheus metric: %w", err)
		}
	}

	return nil
}

// ExportSummary creates a summary report of the benchmark run
func (e *Exporter) ExportSummary(finalMetrics Metrics, finalCostMetrics CostMetrics) error {
	filename := filepath.Join(e.outputPath, "benchmark_summary.txt")

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create summary file: %w", err)
	}
	defer file.Close()

	summary := fmt.Sprintf(`
Benchmark Summary Report
========================
Generated: %s
Duration: %s

Performance Metrics:
-------------------
Total Operations: %d
Read Operations: %d (%.1f%%)
Write Operations: %d (%.1f%%)
Error Count: %d
Error Rate: %.2f%%

QPS Metrics:
-----------
Average Read QPS: %.2f
Average Write QPS: %.2f
Total QPS: %.2f

Latency Metrics:
---------------
Average Read Latency: %.2f ms
Average Write Latency: %.2f ms

Cost Analysis:
-------------
Provider: %s
Total Cost: $%.6f %s
Cost Per Read: $%.8f
Cost Per Write: $%.8f
Hourly Cost: $%.4f
Calculation Mode: %s

Efficiency Metrics:
------------------
Operations per Dollar: %.0f
Reads per Dollar: %.0f
Writes per Dollar: %.0f
`,
		time.Now().Format("2006-01-02 15:04:05"),
		finalMetrics.Duration.String(),
		finalMetrics.TotalOps,
		finalMetrics.ReadOps,
		float64(finalMetrics.ReadOps)/float64(finalMetrics.TotalOps)*100,
		finalMetrics.WriteOps,
		float64(finalMetrics.WriteOps)/float64(finalMetrics.TotalOps)*100,
		finalMetrics.ErrorCount,
		finalMetrics.ErrorRate*100,
		finalMetrics.ReadQPS,
		finalMetrics.WriteQPS,
		finalMetrics.TotalQPS,
		finalMetrics.AvgReadLatency,
		finalMetrics.AvgWriteLatency,
		finalCostMetrics.Provider,
		finalCostMetrics.TotalCost,
		finalCostMetrics.Currency,
		finalCostMetrics.CostPerRead,
		finalCostMetrics.CostPerWrite,
		finalCostMetrics.HourlyCost,
		finalCostMetrics.CalculationMode,
		float64(finalMetrics.TotalOps)/finalCostMetrics.TotalCost,
		1.0/finalCostMetrics.CostPerRead,
		1.0/finalCostMetrics.CostPerWrite,
	)

	if _, err := file.WriteString(summary); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}

	return nil
}
