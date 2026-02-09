package database

import (
	"context"
	"time"
)

// Document represents a document in the benchmark collection
type Document struct {
	// Original fields (for benchmark mode)
	ID          string   `bson:"_id,omitempty"`
	Title       string   `bson:"title,omitempty"`
	Content     string   `bson:"content,omitempty"`
	Tags        []string `bson:"tags,omitempty"`
	SearchTerms string   `bson:"search_terms,omitempty"`

	// Token-based fields (for cost_model mode)
	Text1 string `bson:"text1,omitempty"`
	Text2 string `bson:"text2,omitempty"`
	Text3 string `bson:"text3,omitempty"`

	// Geospatial field (for geospatial_search mode)
	Location interface{} `bson:"location,omitempty"` // GeoJSON Point

	CreatedAt time.Time `bson:"created_at"`
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
	CreateTextIndexForCollection(ctx context.Context, collectionName string, shardNumber int) error
	DropIndexes(ctx context.Context) error
	DropIndexesForCollection(ctx context.Context, collectionName string) error

	// Atlas Search index management
	CreateSearchIndex(ctx context.Context) error
	CreateSearchIndexForCollection(ctx context.Context, collectionName string) error
	DropSearchIndexes(ctx context.Context) error
	DropSearchIndexesForCollection(ctx context.Context, collectionName string) error

	// Geospatial index management
	CreateGeoIndex(ctx context.Context, fieldName string) error
	CreateGeoIndexForCollection(ctx context.Context, collectionName string, fieldName string) error

	// Data operations
	ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error)
	ExecuteTextSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error)
	ExecuteAtlasSearch(ctx context.Context, query string, limit int) (int, error)
	ExecuteAtlasSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error)
	ExecuteGeoSearch(ctx context.Context, query interface{}, limit int) (int, error)
	ExecuteGeoSearchInCollection(ctx context.Context, collectionName string, query interface{}, limit int) (int, error)
	InsertDocument(ctx context.Context, doc Document) error
	InsertDocumentInCollection(ctx context.Context, collectionName string, doc Document) error
	InsertDocuments(ctx context.Context, docs []Document) error
	InsertDocumentsInCollection(ctx context.Context, collectionName string, docs []Document) error
	CountDocuments(ctx context.Context) (int64, error)
	CountDocumentsInCollection(ctx context.Context, collectionName string) (int64, error)
	DropCollection(ctx context.Context) error
	DropCollectionByName(ctx context.Context, collectionName string) error

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
