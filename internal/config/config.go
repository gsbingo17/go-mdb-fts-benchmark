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
	if config.Database.Type != "mongodb" && config.Database.Type != "documentdb" {
		return fmt.Errorf("database.type must be 'mongodb' or 'documentdb'")
	}
	if config.Database.URI == "" {
		return fmt.Errorf("database.uri is required")
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

	// Cost validation
	if config.Cost.Provider == "" {
		return fmt.Errorf("cost.provider is required")
	}
	if config.Cost.Provider != "atlas" && config.Cost.Provider != "documentdb" {
		return fmt.Errorf("cost.provider must be 'atlas' or 'documentdb'")
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

	return nil
}
