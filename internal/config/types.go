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
	Type             string `yaml:"type"` // "mongodb", "documentdb", or "spanner"
	URI              string `yaml:"uri"`
	Database         string `yaml:"database"`
	Collection       string `yaml:"collection"`
	MaxPoolSize      int    `yaml:"max_pool_size"`
	MinPoolSize      int    `yaml:"min_pool_size"`
	MaxConnIdleTime  int    `yaml:"max_conn_idle_time"` // seconds
	ConnectTimeoutMs int    `yaml:"connect_timeout_ms"`
	SocketTimeoutMs  int    `yaml:"socket_timeout_ms"`

	// Google Cloud Spanner specific fields
	ProjectID       string `yaml:"project_id"`       // GCP project ID
	InstanceID      string `yaml:"instance_id"`      // Spanner instance ID
	Table           string `yaml:"table"`            // Default table name (e.g., "SearchWords")
	CredentialsFile string `yaml:"credentials_file"` // Path to GCP credentials JSON file (optional)
	MaxSessions     int    `yaml:"max_sessions"`     // Maximum number of sessions in pool
	MinSessions     int    `yaml:"min_sessions"`     // Minimum number of sessions in pool
}

// WorkloadConfig defines the benchmark workload parameters
type WorkloadConfig struct {
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

	// Token-based FTS configuration (for cost_model mode)
	Mode                 string            `yaml:"mode"`        // "benchmark" or "cost_model"
	SearchType           string            `yaml:"search_type"` // "text", "atlas_search", or "geospatial_search"
	TextShards           []int             `yaml:"text_shards"` // Array of shard values to randomly select from
	QueryParameters      []QueryParameters `yaml:"query_parameters"`
	QueryResultLimits    []int             `yaml:"query_result_limits"` // Array of limit values to randomly select from
	UseRandomQueries     bool              `yaml:"use_random_queries"`
	RandomTestRuns       int               `yaml:"random_test_runs"`
	RandomQuerySeed      string            `yaml:"random_query_seed"` // Fixed seed for reproducible random queries (e.g., "quossa")
	RandomQueryMaxParams QueryParameters   `yaml:"random_query_max_parameters"`

	// Geospatial search configuration
	GeoQueryLimits      []int                `yaml:"geo_query_limits"`      // Array of limit values for geospatial queries
	GeoDistanceVariants []GeoDistanceVariant `yaml:"geo_distance_variants"` // Different distance filter combinations
	GeoSeed             int64                `yaml:"geo_seed"`              // Seed for geospatial data/query generation (0 = use timestamp for random)
}

// GeoDistanceVariant defines a geospatial query distance filter configuration
type GeoDistanceVariant struct {
	Type        string `yaml:"type"`         // "none", "min", "max", or "both"
	MinDistance bool   `yaml:"min_distance"` // Whether to include min distance filter
	MaxDistance bool   `yaml:"max_distance"` // Whether to include max distance filter
}

// QueryParameters defines the shape of a text search query
type QueryParameters struct {
	PositiveTerms   int    `yaml:"positive_terms"`
	NegativeTerms   int    `yaml:"negative_terms"`
	PositivePhrases int    `yaml:"positive_phrases"`
	NegativePhrases int    `yaml:"negative_phrases"`
	PhraseLength    int    `yaml:"phrase_length"`
	Operator        string `yaml:"operator"` // "AND" or "OR" for positive terms/phrases (default: "AND")
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
	Provider        string               `yaml:"provider"` // "atlas", "documentdb", or "gcp"
	Atlas           AtlasCostConfig      `yaml:"atlas,omitempty"`
	DocumentDB      DocumentDBCostConfig `yaml:"documentdb,omitempty"`
	GCP             GCPCostConfig        `yaml:"gcp,omitempty"`
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

// GCPCostConfig holds Google Cloud Platform specific cost parameters
type GCPCostConfig struct {
	HourlyCost       float64 `yaml:"hourly_cost"`
	StorageCostPerGB float64 `yaml:"storage_cost_per_gb"`
}
