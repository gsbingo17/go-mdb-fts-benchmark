# MongoDB/DocumentDB Text Search Benchmarking Tool

A comprehensive Go-based benchmarking tool to measure cost per read/write operation for $text-based full-text search on MongoDB Atlas and AWS DocumentDB.

## Features

- Generate sustained workloads to saturate target databases
- Maintain stable read/write QPS with configurable ratios
- Focus on $text-based full-text search operations
- Calculate cost per read/write operation
- Achieve sustained CPU bottleneck for 30+ minutes
- Real-time metrics collection and export
- Support for both MongoDB Atlas and AWS DocumentDB

## Quick Start

1. **Build the tool:**
   ```bash
   go build -o benchmark ./cmd/benchmark
   ```

2. **Configure the database connection:**
   - Copy and edit `configs/mongodb.yaml` for MongoDB Atlas
   - Copy and edit `configs/documentdb.yaml` for AWS DocumentDB

3. **Run the benchmark:**
   ```bash
   # For MongoDB Atlas
   ./benchmark -config configs/mongodb.yaml

   # For AWS DocumentDB
   ./benchmark -config configs/documentdb.yaml
   ```

## Configuration

The tool uses YAML configuration files. See `configs/` directory for examples.

### Database Configuration
- Connection URI and credentials
- Pool size and timeout settings
- Database and collection names

### Workload Configuration
- Read/write ratio (e.g., 80/20, 50/50)
- Target QPS and worker count
- Dataset size and duration
- CPU saturation targets

### Metrics Configuration
- Collection and export intervals
- Export formats (JSON, CSV, Prometheus)
- Metrics storage paths

### Cost Configuration
- Provider-specific settings (Atlas/DocumentDB)
- Hourly costs and pricing parameters
- Real-time vs estimate calculation modes

## Project Structure

```
├── cmd/benchmark/          # Main application entry point
├── internal/
│   ├── config/            # Configuration management
│   ├── database/          # Database abstraction and drivers
│   ├── generator/         # Data and workload generation
│   ├── metrics/           # Metrics collection and export
│   └── worker/            # Worker pool and operation handlers
├── configs/               # Configuration examples
└── README.md
```

## Development

### Requirements
- Go 1.25 or later
- MongoDB Atlas or AWS DocumentDB instance
- Appropriate database credentials

### Testing
```bash
# Run all tests
go test ./...

# Run with race detection
go test ./... -race

# Run benchmarks
go test -bench=. ./...
```

### Building
```bash
# Local build
go build ./cmd/benchmark

# Cross-compilation
GOOS=linux GOARCH=amd64 go build ./cmd/benchmark
```

## Documentation

For detailed documentation on configuration options, metrics, and cost calculations, see the inline code documentation and configuration examples.

## License

This project is provided as-is for benchmarking purposes.

<!-- DEBUG: CPU softirq: 1.70%
DEBUG: CPU user: 14.71%
DEBUG: CPU kernel: 2.23%
INFO: System Memory - Used: 933000 bytes, Available: 750032 bytes, Usage: 55.44%
DEBUG: CPU guest: 0.00%
DEBUG: Invalid cache fill ratio: 49.1341 (4913.41%)
DEBUG: CPU iowait: 1.15%
DEBUG: CPU irq: 0.00% 
time=2025-09-12T15:31:27.928+08:00 level=WARN msg="Failed to get cluster info, using base cost" error="Atlas API error: 401"
-->