package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	// Create a temporary config file
	configContent := `
database:
  type: "mongodb"
  uri: "mongodb://localhost:27017"
  database: "test_db"
  collection: "test_collection"

workload:
  read_write_ratio:
    read_percent: 70
    write_percent: 30
  duration: "10m"
  target_qps: 50
  worker_count: 5
  dataset_size: 1000
  saturation_target: 75.0

metrics:
  collection_interval: "5s"
  export_format: "json"
  export_path: "./test_metrics"

cost:
  provider: "atlas"
  currency_code: "USD"
  calculation_mode: "estimate"
  atlas:
    group_id: "test_group"
    cluster_name: "test_cluster"
    hourly_cost: 0.5
`

	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yaml")

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Test loading the config
	config, err := Load(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify loaded values
	if config.Database.Type != "mongodb" {
		t.Errorf("Expected database type 'mongodb', got '%s'", config.Database.Type)
	}

	if config.Workload.ReadWriteRatio.ReadPercent != 70 {
		t.Errorf("Expected read percent 70, got %d", config.Workload.ReadWriteRatio.ReadPercent)
	}

	if config.Workload.Duration != 10*time.Minute {
		t.Errorf("Expected duration 10m, got %v", config.Workload.Duration)
	}

	if config.Cost.Provider != "atlas" {
		t.Errorf("Expected cost provider 'atlas', got '%s'", config.Cost.Provider)
	}
}

func TestApplyDefaults(t *testing.T) {
	config := &Config{
		Database: DatabaseConfig{
			Type: "mongodb",
			URI:  "mongodb://localhost:27017",
		},
		Cost: CostConfig{
			Provider: "atlas",
			Atlas: AtlasCostConfig{
				GroupID:     "test_group",
				ClusterName: "test_cluster",
			},
		},
	}

	err := applyDefaults(config)
	if err != nil {
		t.Fatalf("Failed to apply defaults: %v", err)
	}

	// Check that defaults were applied
	if config.Database.MaxPoolSize != 100 {
		t.Errorf("Expected default max pool size 100, got %d", config.Database.MaxPoolSize)
	}

	if config.Workload.ReadWriteRatio.ReadPercent != 80 {
		t.Errorf("Expected default read percent 80, got %d", config.Workload.ReadWriteRatio.ReadPercent)
	}

	if config.Workload.Duration != 30*time.Minute {
		t.Errorf("Expected default duration 30m, got %v", config.Workload.Duration)
	}

	if config.Metrics.ExportFormat != "json" {
		t.Errorf("Expected default export format 'json', got '%s'", config.Metrics.ExportFormat)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				Database: DatabaseConfig{
					Type: "mongodb",
					URI:  "mongodb://localhost:27017",
				},
				Workload: WorkloadConfig{
					ReadWriteRatio: ReadWriteRatio{
						ReadPercent:  80,
						WritePercent: 20,
					},
					TargetQPS:        100,
					WorkerCount:      10,
					DatasetSize:      1000,
					SaturationTarget: 80.0,
				},
				Metrics: MetricsConfig{
					ExportFormat: "json",
				},
				Cost: CostConfig{
					Provider:        "atlas",
					CalculationMode: "estimate",
					Atlas: AtlasCostConfig{
						GroupID:     "test_group",
						ClusterName: "test_cluster",
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid database type",
			config: Config{
				Database: DatabaseConfig{
					Type: "invalid",
					URI:  "mongodb://localhost:27017",
				},
				Cost: CostConfig{
					Provider: "atlas",
					Atlas: AtlasCostConfig{
						GroupID:     "test_group",
						ClusterName: "test_cluster",
					},
				},
			},
			expectError: true,
			errorMsg:    "database.type must be 'mongodb' or 'documentdb'",
		},
		{
			name: "invalid read/write ratio",
			config: Config{
				Database: DatabaseConfig{
					Type: "mongodb",
					URI:  "mongodb://localhost:27017",
				},
				Workload: WorkloadConfig{
					ReadWriteRatio: ReadWriteRatio{
						ReadPercent:  70,
						WritePercent: 40, // Should total 100
					},
					TargetQPS:        100,
					WorkerCount:      10,
					DatasetSize:      1000,
					SaturationTarget: 80.0,
				},
				Cost: CostConfig{
					Provider: "atlas",
					Atlas: AtlasCostConfig{
						GroupID:     "test_group",
						ClusterName: "test_cluster",
					},
				},
			},
			expectError: true,
			errorMsg:    "read_percent + write_percent must equal 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(&tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Error("Expected error when loading non-existent file")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "invalid.yaml")

	invalidYAML := `
database:
  type: "mongodb"
  uri: "mongodb://localhost:27017"
invalid_yaml: [unclosed_array
`

	err := os.WriteFile(configFile, []byte(invalidYAML), 0644)
	if err != nil {
		t.Fatalf("Failed to write invalid config: %v", err)
	}

	_, err = Load(configFile)
	if err == nil {
		t.Error("Expected error when loading invalid YAML")
	}
}
