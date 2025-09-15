package metrics

import (
	"sync/atomic"
	"time"
)

// MetricsCollector collects real-time performance metrics
type MetricsCollector struct {
	// Operation counters
	readOps    atomic.Int64
	writeOps   atomic.Int64
	errorCount atomic.Int64

	// Latency tracking
	readLatencySum  atomic.Int64 // in nanoseconds
	writeLatencySum atomic.Int64 // in nanoseconds

	// QPS tracking
	startTime     time.Time
	lastResetTime atomic.Value // stores time.Time

	// Latency histograms (simplified implementation)
	readLatencies  []time.Duration
	writeLatencies []time.Duration
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	mc := &MetricsCollector{
		startTime:      time.Now(),
		readLatencies:  make([]time.Duration, 0, 10000),
		writeLatencies: make([]time.Duration, 0, 10000),
	}
	mc.lastResetTime.Store(time.Now())
	return mc
}

// RecordRead records a read operation with its latency and success status
func (mc *MetricsCollector) RecordRead(latency time.Duration, success bool) {
	mc.readOps.Add(1)
	mc.readLatencySum.Add(int64(latency))

	if !success {
		mc.errorCount.Add(1)
	}

	// Store latency for percentile calculations (with size limit)
	if len(mc.readLatencies) < cap(mc.readLatencies) {
		mc.readLatencies = append(mc.readLatencies, latency)
	}
}

// RecordWrite records a write operation with its latency and success status
func (mc *MetricsCollector) RecordWrite(latency time.Duration, success bool) {
	mc.writeOps.Add(1)
	mc.writeLatencySum.Add(int64(latency))

	if !success {
		mc.errorCount.Add(1)
	}

	// Store latency for percentile calculations (with size limit)
	if len(mc.writeLatencies) < cap(mc.writeLatencies) {
		mc.writeLatencies = append(mc.writeLatencies, latency)
	}
}

// GetCurrentMetrics returns current metrics snapshot
func (mc *MetricsCollector) GetCurrentMetrics() Metrics {
	now := time.Now()

	// Use reset time if available, otherwise use start time
	lastReset := mc.lastResetTime.Load().(time.Time)
	var duration time.Duration
	if !lastReset.IsZero() {
		duration = now.Sub(lastReset)
	} else {
		duration = now.Sub(mc.startTime)
	}

	return mc.getMetricsWithDuration(duration, now)
}

// GetMetricsWithCustomDuration returns metrics calculated with a specific duration
func (mc *MetricsCollector) GetMetricsWithCustomDuration(customDuration time.Duration) Metrics {
	return mc.getMetricsWithDuration(customDuration, time.Now())
}

// getMetricsWithDuration is the internal method that calculates metrics with a given duration
func (mc *MetricsCollector) getMetricsWithDuration(duration time.Duration, timestamp time.Time) Metrics {
	readOps := mc.readOps.Load()
	writeOps := mc.writeOps.Load()
	errors := mc.errorCount.Load()

	var avgReadLatency, avgWriteLatency float64

	if readOps > 0 {
		avgReadLatency = float64(mc.readLatencySum.Load()) / float64(readOps) / 1e6 // convert to ms
	}

	if writeOps > 0 {
		avgWriteLatency = float64(mc.writeLatencySum.Load()) / float64(writeOps) / 1e6 // convert to ms
	}

	// Avoid division by zero for very short durations
	var readQPS, writeQPS, totalQPS float64
	if duration.Seconds() > 0 {
		readQPS = float64(readOps) / duration.Seconds()
		writeQPS = float64(writeOps) / duration.Seconds()
		totalQPS = float64(readOps+writeOps) / duration.Seconds()
	}

	return Metrics{
		ReadOps:         readOps,
		WriteOps:        writeOps,
		TotalOps:        readOps + writeOps,
		ErrorCount:      errors,
		Duration:        duration,
		ReadQPS:         readQPS,
		WriteQPS:        writeQPS,
		TotalQPS:        totalQPS,
		AvgReadLatency:  avgReadLatency,
		AvgWriteLatency: avgWriteLatency,
		ErrorRate:       float64(errors) / float64(readOps+writeOps),
		Timestamp:       timestamp,
	}
}

// GetQPSWindow returns QPS for a specific time window
func (mc *MetricsCollector) GetQPSWindow(window time.Duration) (readQPS, writeQPS, totalQPS float64) {
	lastReset := mc.lastResetTime.Load().(time.Time)
	now := time.Now()
	windowDuration := now.Sub(lastReset)

	if windowDuration < window {
		return 0, 0, 0
	}

	readOps := mc.readOps.Load()
	writeOps := mc.writeOps.Load()

	readQPS = float64(readOps) / windowDuration.Seconds()
	writeQPS = float64(writeOps) / windowDuration.Seconds()
	totalQPS = float64(readOps+writeOps) / windowDuration.Seconds()

	return readQPS, writeQPS, totalQPS
}

// Reset resets the metrics counters
func (mc *MetricsCollector) Reset() {
	mc.readOps.Store(0)
	mc.writeOps.Store(0)
	mc.errorCount.Store(0)
	mc.readLatencySum.Store(0)
	mc.writeLatencySum.Store(0)
	mc.lastResetTime.Store(time.Now())
	mc.readLatencies = mc.readLatencies[:0]
	mc.writeLatencies = mc.writeLatencies[:0]
}

// GetPercentileLatency calculates latency percentiles (simplified implementation)
func (mc *MetricsCollector) GetPercentileLatency(percentile float64, operationType string) time.Duration {
	var latencies []time.Duration

	switch operationType {
	case "read":
		latencies = mc.readLatencies
	case "write":
		latencies = mc.writeLatencies
	default:
		return 0
	}

	if len(latencies) == 0 {
		return 0
	}

	// Simple percentile calculation (in production, use a proper histogram library)
	index := int(float64(len(latencies)) * percentile / 100.0)
	if index >= len(latencies) {
		index = len(latencies) - 1
	}

	return latencies[index]
}
