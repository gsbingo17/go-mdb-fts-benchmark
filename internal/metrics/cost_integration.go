package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"

	mongoConfig "mongodb-benchmarking-tool/internal/config"
)

// RealTimeCostTracker provides real-time cost tracking
type RealTimeCostTracker interface {
	GetCurrentHourlyCost(ctx context.Context) (float64, error)
	GetStorageCost(ctx context.Context) (float64, error)
	GetNetworkCost(ctx context.Context) (float64, error)
	GetTotalCostSinceStart(ctx context.Context, startTime time.Time) (float64, error)
	GetCostProjection(ctx context.Context, metrics Metrics) (CostProjection, error)
}

// CostProjection represents cost forecasting data
type CostProjection struct {
	HourlyRate        float64   `json:"hourly_rate"`
	DailyProjection   float64   `json:"daily_projection"`
	WeeklyProjection  float64   `json:"weekly_projection"`
	MonthlyProjection float64   `json:"monthly_projection"`
	CostPerOperation  float64   `json:"cost_per_operation"`
	BreakdownBy       string    `json:"breakdown_by"` // "compute", "storage", "io", "network"
	LastUpdated       time.Time `json:"last_updated"`
}

// AtlasRealTimeCostTracker implements cost tracking for MongoDB Atlas
type AtlasRealTimeCostTracker struct {
	baseURL             string
	publicKey           string
	privateKey          string
	groupID             string
	clusterName         string
	httpClient          *http.Client
	baseCostPerHour     float64
	configHourlyCost    float64
	configStorageCostGB float64
}

// NewAtlasRealTimeCostTracker creates a new Atlas cost tracker
func NewAtlasRealTimeCostTracker(cfg mongoConfig.AtlasCostConfig) *AtlasRealTimeCostTracker {
	return &AtlasRealTimeCostTracker{
		baseURL:             "https://cloud.mongodb.com/api/atlas/v1.0",
		publicKey:           cfg.PublicKey,
		privateKey:          cfg.PrivateKey,
		groupID:             cfg.GroupID,
		clusterName:         cfg.ClusterName,
		httpClient:          &http.Client{Timeout: 30 * time.Second},
		baseCostPerHour:     cfg.HourlyCost,
		configHourlyCost:    cfg.HourlyCost,
		configStorageCostGB: cfg.StorageCostPerGB,
	}
}

// GetCurrentHourlyCost retrieves current hourly cost from Atlas
func (act *AtlasRealTimeCostTracker) GetCurrentHourlyCost(ctx context.Context) (float64, error) {
	// Use configured hourly cost instead of Atlas API lookup
	return act.configHourlyCost, nil
}

// GetStorageCost retrieves storage cost from Atlas
func (act *AtlasRealTimeCostTracker) GetStorageCost(ctx context.Context) (float64, error) {
	// Use configured storage cost rate
	if act.configStorageCostGB == 0 {
		return 0.0, nil // No storage cost when configured as 0
	}

	storageGB, err := act.getStorageUsage(ctx)
	if err != nil {
		return 0, err
	}

	// Use configured storage pricing instead of hardcoded rate
	storageRate := act.configStorageCostGB              // Use config value
	hourlyCost := (storageGB * storageRate) / (24 * 30) // Convert to hourly

	return hourlyCost, nil
}

// GetNetworkCost retrieves network cost from Atlas
func (act *AtlasRealTimeCostTracker) GetNetworkCost(ctx context.Context) (float64, error) {
	// Atlas includes data transfer in base cost for most regions
	return 0.0, nil
}

// GetTotalCostSinceStart calculates total cost since benchmark start
func (act *AtlasRealTimeCostTracker) GetTotalCostSinceStart(ctx context.Context, startTime time.Time) (float64, error) {
	duration := time.Since(startTime).Hours()

	hourlyCost, err := act.GetCurrentHourlyCost(ctx)
	if err != nil {
		return 0, err
	}

	storageCost, err := act.GetStorageCost(ctx)
	if err != nil {
		storageCost = 0 // Continue without storage cost if unavailable
	}

	totalCost := (hourlyCost + storageCost) * duration
	return totalCost, nil
}

// GetCostProjection provides cost forecasting
func (act *AtlasRealTimeCostTracker) GetCostProjection(ctx context.Context, metrics Metrics) (CostProjection, error) {
	hourlyCost, err := act.GetCurrentHourlyCost(ctx)
	if err != nil {
		return CostProjection{}, err
	}

	storageCost, _ := act.GetStorageCost(ctx)
	totalHourlyRate := hourlyCost + storageCost

	var costPerOp float64
	if metrics.TotalOps > 0 {
		costPerOp = (totalHourlyRate * metrics.Duration.Hours()) / float64(metrics.TotalOps)
	}

	return CostProjection{
		HourlyRate:        totalHourlyRate,
		DailyProjection:   totalHourlyRate * 24,
		WeeklyProjection:  totalHourlyRate * 24 * 7,
		MonthlyProjection: totalHourlyRate * 24 * 30,
		CostPerOperation:  costPerOp,
		BreakdownBy:       "compute",
		LastUpdated:       time.Now(),
	}, nil
}

// getClusterInfo retrieves cluster information from Atlas API
func (act *AtlasRealTimeCostTracker) getClusterInfo(ctx context.Context) (*AtlasClusterInfo, error) {
	url := fmt.Sprintf("%s/groups/%s/clusters/%s",
		act.baseURL, act.groupID, act.clusterName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(act.publicKey, act.privateKey)

	resp, err := act.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Atlas API error: %d", resp.StatusCode)
	}

	var clusterInfo AtlasClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&clusterInfo); err != nil {
		return nil, err
	}

	return &clusterInfo, nil
}

// getStorageUsage retrieves storage usage from Atlas
func (act *AtlasRealTimeCostTracker) getStorageUsage(ctx context.Context) (float64, error) {
	// This would integrate with Atlas measurements API to get actual storage usage
	// For now, return estimated usage
	return 10.0, nil // 10GB estimated
}

// calculateClusterCost calculates cost based on cluster configuration
func (act *AtlasRealTimeCostTracker) calculateClusterCost(info *AtlasClusterInfo) float64 {
	// Atlas pricing based on instance size
	instanceCosts := map[string]float64{
		"M10": 0.08, // $0.08/hour
		"M20": 0.20, // $0.20/hour
		"M30": 0.54, // $0.54/hour
		"M40": 1.35, // $1.35/hour
		"M50": 2.70, // $2.70/hour
		"M60": 5.40, // $5.40/hour
	}

	if cost, exists := instanceCosts[info.InstanceSizeName]; exists {
		return cost * float64(info.NumShards)
	}

	return act.baseCostPerHour // Fallback to configured cost
}

// AtlasClusterInfo represents Atlas cluster information
type AtlasClusterInfo struct {
	Name             string  `json:"name"`
	InstanceSizeName string  `json:"instanceSizeName"`
	NumShards        int     `json:"numShards"`
	DiskSizeGB       float64 `json:"diskSizeGB"`
}

// DocumentDBRealTimeCostTracker implements cost tracking for AWS DocumentDB
type DocumentDBRealTimeCostTracker struct {
	cloudwatchClient *cloudwatch.Client
	clusterID        string
	region           string
	instanceType     string
	baseCostPerHour  float64
}

// NewDocumentDBRealTimeCostTracker creates a new DocumentDB cost tracker
func NewDocumentDBRealTimeCostTracker(cfg mongoConfig.DocumentDBCostConfig) (*DocumentDBRealTimeCostTracker, error) {
	awsConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	return &DocumentDBRealTimeCostTracker{
		cloudwatchClient: cloudwatch.NewFromConfig(awsConfig),
		clusterID:        cfg.ClusterID,
		region:           cfg.Region,
		instanceType:     cfg.InstanceType,
		baseCostPerHour:  cfg.HourlyCost,
	}, nil
}

// GetCurrentHourlyCost retrieves current hourly cost for DocumentDB
func (dct *DocumentDBRealTimeCostTracker) GetCurrentHourlyCost(ctx context.Context) (float64, error) {
	// Use configured hourly cost instead of AWS Pricing API lookup
	return dct.baseCostPerHour, nil
}

// GetStorageCost retrieves storage cost for DocumentDB
func (dct *DocumentDBRealTimeCostTracker) GetStorageCost(ctx context.Context) (float64, error) {
	// DocumentDB storage pricing is typically $0.10 per GB-month
	storageGB, err := dct.getStorageUsage(ctx)
	if err != nil {
		return 0, err
	}

	storageRate := 0.10                                 // $0.10 per GB per month
	hourlyCost := (storageGB * storageRate) / (24 * 30) // Convert to hourly

	return hourlyCost, nil
}

// GetNetworkCost retrieves network cost for DocumentDB
func (dct *DocumentDBRealTimeCostTracker) GetNetworkCost(ctx context.Context) (float64, error) {
	// DocumentDB data transfer costs are minimal for most use cases
	return 0.0, nil
}

// GetTotalCostSinceStart calculates total DocumentDB cost
func (dct *DocumentDBRealTimeCostTracker) GetTotalCostSinceStart(ctx context.Context, startTime time.Time) (float64, error) {
	duration := time.Since(startTime).Hours()

	computeCost, err := dct.GetCurrentHourlyCost(ctx)
	if err != nil {
		return 0, err
	}

	storageCost, _ := dct.GetStorageCost(ctx)
	ioCost, _ := dct.getIOCost(ctx, duration)

	totalCost := (computeCost+storageCost)*duration + ioCost
	return totalCost, nil
}

// GetCostProjection provides DocumentDB cost forecasting
func (dct *DocumentDBRealTimeCostTracker) GetCostProjection(ctx context.Context, metrics Metrics) (CostProjection, error) {
	computeCost, err := dct.GetCurrentHourlyCost(ctx)
	if err != nil {
		return CostProjection{}, err
	}

	storageCost, _ := dct.GetStorageCost(ctx)

	// Estimate I/O cost based on current QPS
	ioRate := (metrics.TotalQPS / 1000000.0) * 0.20 // $0.20 per million I/O requests

	totalHourlyRate := computeCost + storageCost + ioRate

	var costPerOp float64
	if metrics.TotalOps > 0 {
		costPerOp = (totalHourlyRate * metrics.Duration.Hours()) / float64(metrics.TotalOps)
	}

	return CostProjection{
		HourlyRate:        totalHourlyRate,
		DailyProjection:   totalHourlyRate * 24,
		WeeklyProjection:  totalHourlyRate * 24 * 7,
		MonthlyProjection: totalHourlyRate * 24 * 30,
		CostPerOperation:  costPerOp,
		BreakdownBy:       "compute_storage_io",
		LastUpdated:       time.Now(),
	}, nil
}

// getStorageUsage retrieves storage usage from CloudWatch
func (dct *DocumentDBRealTimeCostTracker) getStorageUsage(ctx context.Context) (float64, error) {
	// This would query CloudWatch for actual storage metrics
	return 20.0, nil // 20GB estimated
}

// getIOCost calculates I/O cost based on operations
func (dct *DocumentDBRealTimeCostTracker) getIOCost(ctx context.Context, hours float64) (float64, error) {
	// DocumentDB charges for I/O operations
	// This would integrate with CloudWatch to get actual I/O metrics
	estimatedIOPerHour := 100000.0 // 100K I/O operations per hour
	totalIO := estimatedIOPerHour * hours
	cost := (totalIO / 1000000.0) * 0.20 // $0.20 per million I/O

	return cost, nil
}

// getLocationFromRegion converts AWS region to pricing location
func (dct *DocumentDBRealTimeCostTracker) getLocationFromRegion() string {
	regionMap := map[string]string{
		"us-east-1":      "US East (N. Virginia)",
		"us-west-2":      "US West (Oregon)",
		"eu-west-1":      "Europe (Ireland)",
		"ap-southeast-1": "Asia Pacific (Singapore)",
	}

	if location, exists := regionMap[dct.region]; exists {
		return location
	}
	return "US East (N. Virginia)" // Default
}

// CreateRealTimeCostTracker creates the appropriate cost tracker
func CreateRealTimeCostTracker(cfg mongoConfig.Config) (RealTimeCostTracker, error) {
	switch cfg.Cost.Provider {
	case "atlas":
		return NewAtlasRealTimeCostTracker(cfg.Cost.Atlas), nil
	case "documentdb":
		return NewDocumentDBRealTimeCostTracker(cfg.Cost.DocumentDB)
	default:
		return nil, fmt.Errorf("unsupported cost provider: %s", cfg.Cost.Provider)
	}
}
