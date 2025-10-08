package config

import "time"

// Config represents the complete application configuration
type Config struct {
	Database DatabaseConfig `yaml:"database"`
	Workload WorkloadConfig `yaml:"workload"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Cost     CostConfig     `yaml:"cost"`
}

// DatabaseConfig holds database connection and setup parameters
type DatabaseConfig struct {
	Type             string `yaml:"type"` // "mongodb" or "documentdb"
	URI              string `yaml:"uri"`
	Database         string `yaml:"database"`
	Collection       string `yaml:"collection"`
	MaxPoolSize      int    `yaml:"max_pool_size"`
	MinPoolSize      int    `yaml:"min_pool_size"`
	MaxConnIdleTime  int    `yaml:"max_conn_idle_time"` // seconds
	ConnectTimeoutMs int    `yaml:"connect_timeout_ms"`
	SocketTimeoutMs  int    `yaml:"socket_timeout_ms"`
}

// WorkloadConfig defines the benchmark workload parameters
type WorkloadConfig struct {
	BenchmarkMode      string         `yaml:"benchmark_mode"` // "text_search" or "field_query"
	ReadWriteRatio     ReadWriteRatio `yaml:"read_write_ratio"`
	Duration           time.Duration  `yaml:"duration"`
	TargetQPS          int            `yaml:"target_qps"`
	WorkerCount        int            `yaml:"worker_count"`
	DatasetSize        int            `yaml:"dataset_size"`
	SaturationTarget   float64        `yaml:"saturation_target"` // CPU %
	WarmupDuration     time.Duration  `yaml:"warmup_duration"`
	StabilityWindow    time.Duration  `yaml:"stability_window"`
	AdjustmentCooldown time.Duration  `yaml:"adjustment_cooldown"` // Time to wait between adjustments
	QueryResultLimit   int            `yaml:"query_result_limit"`  // Limit on text search results
}

// ReadWriteRatio defines the ratio of read vs write operations
type ReadWriteRatio struct {
	ReadPercent  int `yaml:"read_percent"`
	WritePercent int `yaml:"write_percent"`
}

// MetricsConfig configures metrics collection and export
type MetricsConfig struct {
	CollectionInterval time.Duration `yaml:"collection_interval"`
	ExportInterval     time.Duration `yaml:"export_interval"`
	ExportFormat       string        `yaml:"export_format"` // "json", "csv", "prometheus"
	ExportPath         string        `yaml:"export_path"`
	EnablePrometheus   bool          `yaml:"enable_prometheus"`
	PrometheusPort     int           `yaml:"prometheus_port"`
}

// CostConfig defines cost calculation parameters
type CostConfig struct {
	Provider        string               `yaml:"provider"` // "atlas" or "documentdb"
	Atlas           AtlasCostConfig      `yaml:"atlas,omitempty"`
	DocumentDB      DocumentDBCostConfig `yaml:"documentdb,omitempty"`
	CurrencyCode    string               `yaml:"currency_code"`
	CalculationMode string               `yaml:"calculation_mode"` // "realtime" or "estimate"
}

// AtlasCostConfig holds MongoDB Atlas specific cost parameters
type AtlasCostConfig struct {
	PublicKey        string  `yaml:"public_key"`
	PrivateKey       string  `yaml:"private_key"`
	GroupID          string  `yaml:"group_id"`
	ClusterName      string  `yaml:"cluster_name"`
	HourlyCost       float64 `yaml:"hourly_cost"`
	StorageCostPerGB float64 `yaml:"storage_cost_per_gb"`
}

// DocumentDBCostConfig holds AWS DocumentDB specific cost parameters
type DocumentDBCostConfig struct {
	Region           string  `yaml:"region"`
	ClusterID        string  `yaml:"cluster_id"`
	InstanceType     string  `yaml:"instance_type"`
	HourlyCost       float64 `yaml:"hourly_cost"`
	StorageCostPerGB float64 `yaml:"storage_cost_per_gb"`
	IOCostPer1MReqs  float64 `yaml:"io_cost_per_1m_requests"`
}
