package database

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"mongodb-benchmarking-tool/internal/config"
)

// DocumentDBClient implements the Database interface for AWS DocumentDB
type DocumentDBClient struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	config     config.DatabaseConfig
	monitor    DatabaseMonitor // Add monitoring capability
}

// DatabaseMonitor interface for monitoring (avoiding circular imports)
type DatabaseMonitor interface {
	GetCPUUtilization(ctx context.Context) (float64, error)
	GetMemoryUtilization(ctx context.Context) (float64, error)
	GetConnectionCount(ctx context.Context) (int64, error)
}

// NewDocumentDBClient creates a new DocumentDB client
func NewDocumentDBClient(cfg config.DatabaseConfig) *DocumentDBClient {
	return &DocumentDBClient{
		config: cfg,
	}
}

// SetMonitor sets the monitoring client for real metrics collection
func (d *DocumentDBClient) SetMonitor(monitor DatabaseMonitor) {
	d.monitor = monitor
}

// Connect establishes connection to DocumentDB
func (d *DocumentDBClient) Connect(ctx context.Context) error {
	// DocumentDB requires TLS and uses MongoDB wire protocol
	clientOptions := options.Client().
		ApplyURI(d.config.URI).
		SetMaxPoolSize(uint64(d.config.MaxPoolSize)).
		SetMinPoolSize(uint64(d.config.MinPoolSize)).
		SetMaxConnIdleTime(time.Duration(d.config.MaxConnIdleTime) * time.Second).
		SetConnectTimeout(time.Duration(d.config.ConnectTimeoutMs) * time.Millisecond)

	// DocumentDB specific settings
	clientOptions.SetRetryWrites(false) // DocumentDB doesn't support retryable writes

	// Connect to DocumentDB
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to DocumentDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping DocumentDB: %w", err)
	}

	d.client = client
	d.database = client.Database(d.config.Database)
	d.collection = d.database.Collection(d.config.Collection)

	return nil
}

// Close closes the DocumentDB connection
func (d *DocumentDBClient) Close() error {
	if d.client != nil {
		return d.client.Disconnect(context.Background())
	}
	return nil
}

// Ping verifies the connection is alive
func (d *DocumentDBClient) Ping(ctx context.Context) error {
	if d.client == nil {
		return fmt.Errorf("not connected to DocumentDB")
	}
	return d.client.Ping(ctx, nil)
}

// CreateTextIndex creates a text search index
// Note: DocumentDB has limitations on text search compared to MongoDB
func (d *DocumentDBClient) CreateTextIndex(ctx context.Context) error {
	// DocumentDB supports text indexes but with limitations
	// DocumentDB does NOT support default_language option
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "title", Value: "text"},
			{Key: "content", Value: "text"},
			{Key: "search_terms", Value: "text"},
		},
		// DocumentDB compatibility: Remove unsupported options
		Options: nil, // No language specification - DocumentDB doesn't support SetDefaultLanguage
	}

	fmt.Printf("INFO: Creating DocumentDB-compatible text index (without language specification)\n")
	_, err := d.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create text index in DocumentDB: %w", err)
	}
	fmt.Printf("SUCCESS: DocumentDB text index created successfully\n")
	return nil
}

// CreateTextIndexForCollection creates a progressive text search index based on shard number
// Shard 1: index on text1 only
// Shard 2: index on text1, text2
// Shard 3: index on text1, text2, text3
func (d *DocumentDBClient) CreateTextIndexForCollection(ctx context.Context, collectionName string, shardNumber int) error {
	coll := d.database.Collection(collectionName)

	// Build progressive index based on shard number
	var keys bson.D
	switch shardNumber {
	case 1:
		keys = bson.D{{Key: "text1", Value: "text"}}
	case 2:
		keys = bson.D{
			{Key: "text1", Value: "text"},
			{Key: "text2", Value: "text"},
		}
	case 3:
		keys = bson.D{
			{Key: "text1", Value: "text"},
			{Key: "text2", Value: "text"},
			{Key: "text3", Value: "text"},
		}
	default:
		// Fallback for higher shard numbers: use all fields
		keys = bson.D{
			{Key: "text1", Value: "text"},
			{Key: "text2", Value: "text"},
			{Key: "text3", Value: "text"},
		}
	}

	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: nil, // DocumentDB doesn't support SetDefaultLanguage
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create progressive text index for DocumentDB collection %s (shard %d): %w", collectionName, shardNumber, err)
	}
	return nil
}

// DropIndexes drops all indexes except _id
func (d *DocumentDBClient) DropIndexes(ctx context.Context) error {
	err := d.collection.Indexes().DropAll(ctx)
	return err
}

// DropIndexesForCollection drops all indexes for a specific collection except _id
func (d *DocumentDBClient) DropIndexesForCollection(ctx context.Context, collectionName string) error {
	coll := d.database.Collection(collectionName)
	return coll.Indexes().DropAll(ctx)
}

// ExecuteTextSearch performs a text search query with optional result limit
func (d *DocumentDBClient) ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error) {
	// DocumentDB supports $text but may have different performance characteristics
	filter := bson.D{
		{Key: "$text", Value: bson.D{
			{Key: "$search", Value: query},
		}},
	}

	// Configure find options with limit if specified
	opts := options.Find()
	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	cursor, err := d.collection.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("text search failed in DocumentDB: %w", err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		count++
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %w", err)
	}

	return count, nil
}

// ExecuteTextSearchInCollection performs a text search query in a specific collection
func (d *DocumentDBClient) ExecuteTextSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error) {
	coll := d.database.Collection(collectionName)
	filter := bson.D{
		{Key: "$text", Value: bson.D{
			{Key: "$search", Value: query},
		}},
	}

	opts := options.Find()
	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("text search failed in DocumentDB collection %s: %w", collectionName, err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		count++
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %w", err)
	}

	return count, nil
}

// InsertDocument inserts a single document
func (d *DocumentDBClient) InsertDocument(ctx context.Context, doc Document) error {
	_, err := d.collection.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document to DocumentDB: %w", err)
	}
	return nil
}

// InsertDocumentInCollection inserts a single document into a specific collection
func (d *DocumentDBClient) InsertDocumentInCollection(ctx context.Context, collectionName string, doc Document) error {
	coll := d.database.Collection(collectionName)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document into DocumentDB collection %s: %w", collectionName, err)
	}
	return nil
}

// InsertDocuments inserts multiple documents in batch
func (d *DocumentDBClient) InsertDocuments(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	// Convert to interface slice
	documents := make([]interface{}, len(docs))
	for i, doc := range docs {
		documents[i] = doc
	}

	// DocumentDB may have different batch size limits
	_, err := d.collection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert documents to DocumentDB: %w", err)
	}
	return nil
}

// InsertDocumentsInCollection inserts multiple documents into a specific collection
func (d *DocumentDBClient) InsertDocumentsInCollection(ctx context.Context, collectionName string, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	coll := d.database.Collection(collectionName)
	documents := make([]interface{}, len(docs))
	for i, doc := range docs {
		documents[i] = doc
	}

	_, err := coll.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert documents into DocumentDB collection %s: %w", collectionName, err)
	}
	return nil
}

// CountDocuments returns the number of documents in the collection
func (d *DocumentDBClient) CountDocuments(ctx context.Context) (int64, error) {
	count, err := d.collection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents in DocumentDB: %w", err)
	}
	return count, nil
}

// CountDocumentsInCollection returns the number of documents in a specific collection
func (d *DocumentDBClient) CountDocumentsInCollection(ctx context.Context, collectionName string) (int64, error) {
	coll := d.database.Collection(collectionName)
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents in DocumentDB collection %s: %w", collectionName, err)
	}
	return count, nil
}

// DropCollection drops the entire collection
func (d *DocumentDBClient) DropCollection(ctx context.Context) error {
	return d.collection.Drop(ctx)
}

// DropCollectionByName drops a specific collection by name
func (d *DocumentDBClient) DropCollectionByName(ctx context.Context, collectionName string) error {
	coll := d.database.Collection(collectionName)
	return coll.Drop(ctx)
}

// GetMetrics retrieves database performance metrics
func (d *DocumentDBClient) GetMetrics(ctx context.Context) (DatabaseMetrics, error) {
	metrics := DatabaseMetrics{
		ReadLatency:  0.0, // Would be calculated from operation timing
		WriteLatency: 0.0, // Would be calculated from operation timing
		Timestamp:    time.Now(),
	}

	// Use real CloudWatch monitoring if available
	if d.monitor != nil {
		// Get CPU utilization from CloudWatch
		if cpu, err := d.monitor.GetCPUUtilization(ctx); err == nil {
			metrics.CPUUtilization = cpu
		} else {
			fmt.Printf("WARN: Failed to get DocumentDB CPU utilization: %v\n", err)
			metrics.CPUUtilization = 50.0 // Fallback value
		}

		// Get memory utilization from CloudWatch
		if memory, err := d.monitor.GetMemoryUtilization(ctx); err == nil {
			metrics.MemoryUtilization = memory
		} else {
			// Memory metrics may not be available for DocumentDB
			metrics.MemoryUtilization = 0.0
		}

		// Get connection count from CloudWatch
		if connections, err := d.monitor.GetConnectionCount(ctx); err == nil {
			metrics.ConnectionCount = connections
		} else {
			fmt.Printf("WARN: Failed to get DocumentDB connection count: %v\n", err)
			metrics.ConnectionCount = 50 // Reasonable fallback
		}
	} else {
		// Fallback values when monitor is not available
		fmt.Printf("WARN: DocumentDB monitor not configured, using fallback metrics\n")
		metrics.CPUUtilization = 50.0
		metrics.MemoryUtilization = 0.0
		metrics.ConnectionCount = 50
	}

	return metrics, nil
}

// GetConnectionInfo returns connection information
func (d *DocumentDBClient) GetConnectionInfo() ConnectionInfo {
	return ConnectionInfo{
		Type:       "documentdb",
		Host:       "aws-documentdb", // Simplified
		Database:   d.config.Database,
		Collection: d.config.Collection,
	}
}

// CreateSearchIndex - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) CreateSearchIndex(ctx context.Context) error {
	return fmt.Errorf("Atlas Search is not supported on AWS DocumentDB - use search_type: 'text' instead")
}

// CreateSearchIndexForCollection - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) CreateSearchIndexForCollection(ctx context.Context, collectionName string) error {
	return fmt.Errorf("Atlas Search is not supported on AWS DocumentDB - use search_type: 'text' instead")
}

// DropSearchIndexes - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) DropSearchIndexes(ctx context.Context) error {
	return fmt.Errorf("Atlas Search is not supported on AWS DocumentDB")
}

// DropSearchIndexesForCollection - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) DropSearchIndexesForCollection(ctx context.Context, collectionName string) error {
	return fmt.Errorf("Atlas Search is not supported on AWS DocumentDB")
}

// ExecuteAtlasSearch - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) ExecuteAtlasSearch(ctx context.Context, query string, limit int) (int, error) {
	return 0, fmt.Errorf("Atlas Search is not supported on AWS DocumentDB - use search_type: 'text' instead")
}

// ExecuteAtlasSearchInCollection - Atlas Search not supported on DocumentDB
func (d *DocumentDBClient) ExecuteAtlasSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error) {
	return 0, fmt.Errorf("Atlas Search is not supported on AWS DocumentDB - use search_type: 'text' instead")
}

// CreateGeoIndex creates a 2dsphere index on the specified field for the default collection
func (d *DocumentDBClient) CreateGeoIndex(ctx context.Context, fieldName string) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: fieldName, Value: "2dsphere"}},
	}

	_, err := d.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create 2dsphere index on field '%s': %w", fieldName, err)
	}
	fmt.Printf("INFO: Created 2dsphere geo index on field '%s'\n", fieldName)
	return nil
}

// CreateGeoIndexForCollection creates a 2dsphere index on the specified field for a specific collection
func (d *DocumentDBClient) CreateGeoIndexForCollection(ctx context.Context, collectionName string, fieldName string) error {
	coll := d.database.Collection(collectionName)
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: fieldName, Value: "2dsphere"}},
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create 2dsphere index on '%s.%s': %w", collectionName, fieldName, err)
	}
	fmt.Printf("INFO: Created 2dsphere geo index on collection '%s', field '%s'\n", collectionName, fieldName)
	return nil
}

// ExecuteGeoSearch performs geospatial search on the default collection
func (d *DocumentDBClient) ExecuteGeoSearch(ctx context.Context, query interface{}, limit int) (int, error) {
	return d.ExecuteGeoSearchInCollection(ctx, d.config.Collection, query, limit)
}

// ExecuteGeoSearchInCollection performs geospatial search using $nearSphere in a specific collection
func (d *DocumentDBClient) ExecuteGeoSearchInCollection(ctx context.Context, collectionName string, query interface{}, limit int) (int, error) {
	coll := d.database.Collection(collectionName)

	// Execute geospatial query with optional limit
	opts := options.Find()
	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	cursor, err := coll.Find(ctx, query, opts)
	if err != nil {
		return 0, fmt.Errorf("geospatial search failed in DocumentDB collection %s: %w", collectionName, err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		count++
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %w", err)
	}

	return count, nil
}
