package database

import (
	"context"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SpannerMetricsCollector collects metrics from Google Cloud Monitoring
type SpannerMetricsCollector struct {
	client     *monitoring.MetricClient
	projectID  string
	instanceID string
	databaseID string
}

// NewSpannerMetricsCollector creates a new metrics collector
func NewSpannerMetricsCollector(projectID, instanceID, databaseID string, credentialsFile string) (*SpannerMetricsCollector, error) {
	ctx := context.Background()

	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}

	client, err := monitoring.NewMetricClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitoring client: %w", err)
	}

	return &SpannerMetricsCollector{
		client:     client,
		projectID:  projectID,
		instanceID: instanceID,
		databaseID: databaseID,
	}, nil
}

// Close closes the metrics collector
func (c *SpannerMetricsCollector) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// GetCPUUtilization fetches high priority CPU utilization from Cloud Monitoring
func (c *SpannerMetricsCollector) GetCPUUtilization(ctx context.Context) (float64, error) {
	now := time.Now()
	startTime := now.Add(-5 * time.Minute) // Look back 5 minutes
	endTime := now

	// Build the metric filter for high priority CPU
	// Metric: spanner.googleapis.com/instance/cpu/utilization_by_priority
	// Filter by: priority="high", instance, database
	filter := fmt.Sprintf(`
		metric.type = "spanner.googleapis.com/instance/cpu/utilization_by_priority"
		AND resource.labels.instance_id = "%s"
		AND resource.labels.database = "%s"
		AND metric.labels.priority = "high"
	`, c.instanceID, c.databaseID)

	// Create the time interval
	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	// Create the request
	req := &monitoringpb.ListTimeSeriesRequest{
		Name:     fmt.Sprintf("projects/%s", c.projectID),
		Filter:   filter,
		Interval: interval,
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod:  durationpb.New(60 * time.Second), // 1 minute alignment
			PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
		},
	}

	// Execute the request
	it := c.client.ListTimeSeries(ctx, req)

	// Get the latest value
	var latestValue float64
	var latestTime time.Time
	hasData := false

	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to fetch time series: %w", err)
		}

		// Get the most recent point
		if len(ts.Points) > 0 {
			for _, point := range ts.Points {
				pointTime := point.Interval.EndTime.AsTime()
				if pointTime.After(latestTime) {
					latestTime = pointTime
					latestValue = point.Value.GetDoubleValue()
					hasData = true
				}
			}
		}
	}

	if !hasData {
		return 0, fmt.Errorf("no CPU utilization data available")
	}

	// Convert from ratio to percentage (0.0-1.0 to 0-100)
	return latestValue * 100.0, nil
}

// GetMetrics fetches comprehensive metrics from Cloud Monitoring
func (c *SpannerMetricsCollector) GetMetrics(ctx context.Context) (DatabaseMetrics, error) {
	metrics := DatabaseMetrics{
		Timestamp: time.Now(),
	}

	// Get CPU utilization
	cpu, err := c.GetCPUUtilization(ctx)
	if err != nil {
		// Log error but don't fail - return partial metrics
		cpu = 0
	}
	metrics.CPUUtilization = cpu

	// Note: Spanner doesn't have direct memory utilization metrics
	// Memory is managed by Google Cloud
	metrics.MemoryUtilization = 0

	// Note: Connection count is managed by the client library
	// We can get this from the Spanner client session pool if needed
	metrics.ConnectionCount = 0

	// Get read/write latency if needed
	// These would require additional metric queries
	metrics.ReadLatency = 0
	metrics.WriteLatency = 0

	return metrics, nil
}

// GetReadLatency fetches read latency from Cloud Monitoring
func (c *SpannerMetricsCollector) GetReadLatency(ctx context.Context) (float64, error) {
	now := time.Now()
	startTime := now.Add(-5 * time.Minute)
	endTime := now

	filter := fmt.Sprintf(`
		metric.type = "spanner.googleapis.com/api/request_latencies"
		AND resource.labels.instance_id = "%s"
		AND resource.labels.database = "%s"
		AND metric.labels.method = "Read"
	`, c.instanceID, c.databaseID)

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:     fmt.Sprintf("projects/%s", c.projectID),
		Filter:   filter,
		Interval: interval,
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod:  durationpb.New(60 * time.Second),
			PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
		},
	}

	it := c.client.ListTimeSeries(ctx, req)

	var latestValue float64
	var latestTime time.Time
	hasData := false

	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to fetch read latency: %w", err)
		}

		if len(ts.Points) > 0 {
			for _, point := range ts.Points {
				pointTime := point.Interval.EndTime.AsTime()
				if pointTime.After(latestTime) {
					latestTime = pointTime
					latestValue = point.Value.GetDoubleValue()
					hasData = true
				}
			}
		}
	}

	if !hasData {
		return 0, fmt.Errorf("no read latency data available")
	}

	return latestValue, nil
}

// GetWriteLatency fetches write latency from Cloud Monitoring
func (c *SpannerMetricsCollector) GetWriteLatency(ctx context.Context) (float64, error) {
	now := time.Now()
	startTime := now.Add(-5 * time.Minute)
	endTime := now

	filter := fmt.Sprintf(`
		metric.type = "spanner.googleapis.com/api/request_latencies"
		AND resource.labels.instance_id = "%s"
		AND resource.labels.database = "%s"
		AND metric.labels.method = "Commit"
	`, c.instanceID, c.databaseID)

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:     fmt.Sprintf("projects/%s", c.projectID),
		Filter:   filter,
		Interval: interval,
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod:  durationpb.New(60 * time.Second),
			PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
		},
	}

	it := c.client.ListTimeSeries(ctx, req)

	var latestValue float64
	var latestTime time.Time
	hasData := false

	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to fetch write latency: %w", err)
		}

		if len(ts.Points) > 0 {
			for _, point := range ts.Points {
				pointTime := point.Interval.EndTime.AsTime()
				if pointTime.After(latestTime) {
					latestTime = pointTime
					latestValue = point.Value.GetDoubleValue()
					hasData = true
				}
			}
		}
	}

	if !hasData {
		return 0, fmt.Errorf("no write latency data available")
	}

	return latestValue, nil
}
