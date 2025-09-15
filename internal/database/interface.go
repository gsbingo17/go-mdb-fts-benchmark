package database

import (
	"context"
	"time"
)

// Document represents a document in the benchmark collection
type Document struct {
	ID          string    `bson:"_id,omitempty"`
	Title       string    `bson:"title"`
	Content     string    `bson:"content"`
	Tags        []string  `bson:"tags"`
	CreatedAt   time.Time `bson:"created_at"`
	SearchTerms string    `bson:"search_terms"`
}

// DatabaseMetrics represents database performance metrics
type DatabaseMetrics struct {
	CPUUtilization    float64   `json:"cpu_utilization"`
	MemoryUtilization float64   `json:"memory_utilization"`
	ConnectionCount   int64     `json:"connection_count"`
	ReadLatency       float64   `json:"read_latency_ms"`
	WriteLatency      float64   `json:"write_latency_ms"`
	Timestamp         time.Time `json:"timestamp"`
}

// Database interface abstracts database operations for MongoDB and DocumentDB
type Database interface {
	// Connection management
	Connect(ctx context.Context) error
	Close() error
	Ping(ctx context.Context) error

	// Index management
	CreateTextIndex(ctx context.Context) error
	DropIndexes(ctx context.Context) error

	// Data operations
	ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error)
	InsertDocument(ctx context.Context, doc Document) error
	InsertDocuments(ctx context.Context, docs []Document) error
	CountDocuments(ctx context.Context) (int64, error)
	DropCollection(ctx context.Context) error

	// Metrics and monitoring
	GetMetrics(ctx context.Context) (DatabaseMetrics, error)

	// Configuration
	GetConnectionInfo() ConnectionInfo
}

// ConnectionInfo holds database connection details
type ConnectionInfo struct {
	Type       string `json:"type"`
	Host       string `json:"host"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}
