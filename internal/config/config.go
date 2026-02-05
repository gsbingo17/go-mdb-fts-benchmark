package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads and parses a configuration file
func Load(configPath string) (*Config, error) {
	// Read the configuration file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Parse YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Apply defaults and validate
	if err := applyDefaults(&config); err != nil {
		return nil, fmt.Errorf("failed to apply defaults: %w", err)
	}

	if err := validate(&config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &config, nil
}

// applyDefaults sets default values for missing configuration options
func applyDefaults(config *Config) error {
	// Database defaults
	if config.Database.MaxPoolSize == 0 {
		config.Database.MaxPoolSize = 100
	}
	if config.Database.MinPoolSize == 0 {
		config.Database.MinPoolSize = 5
	}
	if config.Database.MaxConnIdleTime == 0 {
		config.Database.MaxConnIdleTime = 60 // 60 seconds
	}
	if config.Database.ConnectTimeoutMs == 0 {
		config.Database.ConnectTimeoutMs = 10000 // 10 seconds
	}
	if config.Database.SocketTimeoutMs == 0 {
		config.Database.SocketTimeoutMs = 30000 // 30 seconds
	}
	if config.Database.Database == "" {
		config.Database.Database = "benchmark"
	}
	if config.Database.Collection == "" {
		config.Database.Collection = "documents"
	}

	// Spanner-specific defaults
	if config.Database.Type == "spanner" {
		if config.Database.MaxSessions == 0 {
			config.Database.MaxSessions = 400
		}
		if config.Database.MinSessions == 0 {
			config.Database.MinSessions = 100
		}
		if config.Database.Table == "" {
			config.Database.Table = "SearchWords"
		}
		// For Spanner, Collection should match Table for shard naming consistency
		if config.Database.Collection == "documents" || config.Database.Collection == "" {
			config.Database.Collection = config.Database.Table
		}
	}

	// Workload defaults
	if config.Workload.ReadWriteRatio.ReadPercent == 0 && config.Workload.ReadWriteRatio.WritePercent == 0 {
		config.Workload.ReadWriteRatio.ReadPercent = 80
		config.Workload.ReadWriteRatio.WritePercent = 20
	}
	if config.Workload.Duration == 0 {
		config.Workload.Duration = 30 * time.Minute
	}
	if config.Workload.TargetQPS == 0 {
		config.Workload.TargetQPS = 100
	}
	if config.Workload.WorkerCount == 0 {
		config.Workload.WorkerCount = 10
	}
	if config.Workload.DatasetSize == 0 {
		config.Workload.DatasetSize = 100000
	}
	if config.Workload.SaturationTarget == 0 {
		config.Workload.SaturationTarget = 80.0 // 80% CPU
	}
	if config.Workload.WarmupDuration == 0 {
		config.Workload.WarmupDuration = 2 * time.Minute
	}
	if config.Workload.StabilityWindow == 0 {
		config.Workload.StabilityWindow = 30 * time.Minute
	}

	// Metrics defaults
	if config.Metrics.CollectionInterval == 0 {
		config.Metrics.CollectionInterval = 10 * time.Second
	}
	if config.Metrics.ExportInterval == 0 {
		config.Metrics.ExportInterval = 60 * time.Second
	}
	if config.Metrics.ExportFormat == "" {
		config.Metrics.ExportFormat = "json"
	}
	if config.Metrics.ExportPath == "" {
		config.Metrics.ExportPath = "./metrics"
	}
	if config.Metrics.PrometheusPort == 0 {
		config.Metrics.PrometheusPort = 8080
	}

	// Cost defaults
	if config.Cost.CurrencyCode == "" {
		config.Cost.CurrencyCode = "USD"
	}
	if config.Cost.CalculationMode == "" {
		config.Cost.CalculationMode = "estimate"
	}

	return nil
}

// validate checks if the configuration is valid
func validate(config *Config) error {
	// Database validation
	if config.Database.Type == "" {
		return fmt.Errorf("database.type is required")
	}
	if config.Database.Type != "mongodb" && config.Database.Type != "documentdb" && config.Database.Type != "spanner" {
		return fmt.Errorf("database.type must be 'mongodb', 'documentdb', or 'spanner'")
	}

	// Type-specific validation
	if config.Database.Type == "mongodb" || config.Database.Type == "documentdb" {
		if config.Database.URI == "" {
			return fmt.Errorf("database.uri is required for MongoDB and DocumentDB")
		}
	}

	if config.Database.Type == "spanner" {
		if config.Database.ProjectID == "" {
			return fmt.Errorf("database.project_id is required for Spanner")
		}
		if config.Database.InstanceID == "" {
			return fmt.Errorf("database.instance_id is required for Spanner")
		}
		if config.Database.Database == "" {
			return fmt.Errorf("database.database is required for Spanner")
		}
	}

	// Workload validation
	if config.Workload.ReadWriteRatio.ReadPercent+config.Workload.ReadWriteRatio.WritePercent != 100 {
		return fmt.Errorf("read_percent + write_percent must equal 100")
	}
	if config.Workload.ReadWriteRatio.ReadPercent < 0 || config.Workload.ReadWriteRatio.WritePercent < 0 {
		return fmt.Errorf("read_percent and write_percent must be non-negative")
	}
	if config.Workload.TargetQPS <= 0 {
		return fmt.Errorf("target_qps must be positive")
	}
	if config.Workload.WorkerCount <= 0 {
		return fmt.Errorf("worker_count must be positive")
	}
	if config.Workload.DatasetSize <= 0 {
		return fmt.Errorf("dataset_size must be positive")
	}
	if config.Workload.SaturationTarget <= 0 || config.Workload.SaturationTarget > 100 {
		return fmt.Errorf("saturation_target must be between 0 and 100")
	}

	// Metrics validation
	validFormats := map[string]bool{"json": true, "csv": true, "prometheus": true}
	if !validFormats[config.Metrics.ExportFormat] {
		return fmt.Errorf("metrics.export_format must be one of: json, csv, prometheus")
	}

	// Cost model mode specific validation
	if config.Workload.Mode == "cost_model" {
		if len(config.Workload.TextShards) == 0 {
			return fmt.Errorf("workload.text_shards array cannot be empty in cost_model mode")
		}
		if len(config.Workload.QueryResultLimits) == 0 {
			return fmt.Errorf("workload.query_result_limits array cannot be empty in cost_model mode")
		}
		// Validate textShards values are positive
		for i, shard := range config.Workload.TextShards {
			if shard <= 0 {
				return fmt.Errorf("workload.text_shards[%d] must be positive, got %d", i, shard)
			}
		}
		// Validate queryResultLimits values are positive
		for i, limit := range config.Workload.QueryResultLimits {
			if limit <= 0 {
				return fmt.Errorf("workload.query_result_limits[%d] must be positive, got %d", i, limit)
			}
		}
		// Validate query parameters exist
		if len(config.Workload.QueryParameters) == 0 && !config.Workload.UseRandomQueries {
			return fmt.Errorf("workload.query_parameters array cannot be empty when use_random_queries is false in cost_model mode")
		}
	}

	// Cost validation
	if config.Cost.Provider == "" {
		return fmt.Errorf("cost.provider is required")
	}
	if config.Cost.Provider != "atlas" && config.Cost.Provider != "documentdb" && config.Cost.Provider != "gcp" {
		return fmt.Errorf("cost.provider must be 'atlas', 'documentdb', or 'gcp'")
	}

	validModes := map[string]bool{"realtime": true, "estimate": true}
	if !validModes[config.Cost.CalculationMode] {
		return fmt.Errorf("cost.calculation_mode must be 'realtime' or 'estimate'")
	}

	// Provider-specific validation
	if config.Cost.Provider == "atlas" {
		if config.Cost.Atlas.GroupID == "" {
			return fmt.Errorf("cost.atlas.group_id is required for Atlas provider")
		}
		if config.Cost.Atlas.ClusterName == "" {
			return fmt.Errorf("cost.atlas.cluster_name is required for Atlas provider")
		}
	}

	if config.Cost.Provider == "documentdb" {
		if config.Cost.DocumentDB.Region == "" {
			return fmt.Errorf("cost.documentdb.region is required for DocumentDB provider")
		}
		if config.Cost.DocumentDB.ClusterID == "" {
			return fmt.Errorf("cost.documentdb.cluster_id is required for DocumentDB provider")
		}
	}

	// GCP provider has no specific required fields for now (future enhancement)

	return nil
}
