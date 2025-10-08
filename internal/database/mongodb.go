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

// MongoDBClient implements the Database interface for MongoDB
type MongoDBClient struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	config     config.DatabaseConfig
}

// NewMongoDBClient creates a new MongoDB client
func NewMongoDBClient(cfg config.DatabaseConfig) *MongoDBClient {
	return &MongoDBClient{
		config: cfg,
	}
}

// Connect establishes connection to MongoDB
func (m *MongoDBClient) Connect(ctx context.Context) error {
	fmt.Printf("INFO: Configuring MongoDB connection with timeouts:\n")
	fmt.Printf("  - Connect timeout: %d ms\n", m.config.ConnectTimeoutMs)
	fmt.Printf("  - Socket timeout: %d ms\n", m.config.SocketTimeoutMs)
	fmt.Printf("  - Server selection timeout: 60 seconds\n")

	// Set up client options with increased timeouts for Atlas
	clientOptions := options.Client().
		ApplyURI(m.config.URI).
		SetMaxPoolSize(uint64(m.config.MaxPoolSize)).
		SetMinPoolSize(uint64(m.config.MinPoolSize)).
		SetMaxConnIdleTime(time.Duration(m.config.MaxConnIdleTime) * time.Second).
		SetConnectTimeout(time.Duration(m.config.ConnectTimeoutMs) * time.Millisecond).
		SetTimeout(time.Duration(m.config.SocketTimeoutMs) * time.Millisecond). // Socket timeout in v2
		SetServerSelectionTimeout(60 * time.Second).                            // Increase server selection timeout
		SetHeartbeatInterval(30 * time.Second).                                 // Increase heartbeat interval
		SetRetryWrites(true).                                                   // Enable retryable writes
		SetRetryReads(true)                                                     // Enable retryable reads

	fmt.Printf("INFO: Connecting to MongoDB Atlas...\n")
	// Connect to MongoDB
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	fmt.Printf("INFO: Verifying connection with ping...\n")
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	fmt.Printf("SUCCESS: MongoDB connection established and verified!\n")

	m.client = client
	m.database = client.Database(m.config.Database)
	m.collection = m.database.Collection(m.config.Collection)

	return nil
}

// Close closes the MongoDB connection
func (m *MongoDBClient) Close() error {
	if m.client != nil {
		return m.client.Disconnect(context.Background())
	}
	return nil
}

// Ping verifies the connection is alive
func (m *MongoDBClient) Ping(ctx context.Context) error {
	if m.client == nil {
		return fmt.Errorf("not connected to MongoDB")
	}
	return m.client.Ping(ctx, nil)
}

// CreateTextIndex creates a text search index on single field
func (m *MongoDBClient) CreateTextIndex(ctx context.Context) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "content", Value: "text"}, // Single field text index
		},
		Options: options.Index().SetDefaultLanguage("english"),
	}

	_, err := m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create text index: %w", err)
	}
	return nil
}

// CreateFieldIndex creates a regular index on a single field
func (m *MongoDBClient) CreateFieldIndex(ctx context.Context, field string) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: field, Value: 1}, // Ascending index
		},
	}

	_, err := m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create field index on %s: %w", field, err)
	}
	return nil
}

// DropIndexes drops all indexes except _id
func (m *MongoDBClient) DropIndexes(ctx context.Context) error {
	err := m.collection.Indexes().DropAll(ctx)
	return err
}

// ExecuteTextSearch performs a text search query with optional result limit
func (m *MongoDBClient) ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error) {
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

	cursor, err := m.collection.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("text search failed: %w", err)
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

// ExecuteFieldQuery performs a field equality query with optional result limit
func (m *MongoDBClient) ExecuteFieldQuery(ctx context.Context, field string, value interface{}, limit int) (int, error) {
	filter := bson.D{
		{Key: field, Value: value},
	}

	// Configure find options with limit if specified
	opts := options.Find()
	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	cursor, err := m.collection.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("field query failed: %w", err)
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
func (m *MongoDBClient) InsertDocument(ctx context.Context, doc Document) error {
	_, err := m.collection.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}
	return nil
}

// InsertDocuments inserts multiple documents in batch
func (m *MongoDBClient) InsertDocuments(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	// Convert to interface slice
	documents := make([]interface{}, len(docs))
	for i, doc := range docs {
		documents[i] = doc
	}

	_, err := m.collection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert documents: %w", err)
	}
	return nil
}

// CountDocuments returns the number of documents in the collection
func (m *MongoDBClient) CountDocuments(ctx context.Context) (int64, error) {
	count, err := m.collection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}
	return count, nil
}

// DropCollection drops the entire collection
func (m *MongoDBClient) DropCollection(ctx context.Context) error {
	return m.collection.Drop(ctx)
}

// GetMetrics retrieves database performance metrics (basic implementation)
func (m *MongoDBClient) GetMetrics(ctx context.Context) (DatabaseMetrics, error) {
	// Basic implementation - in production this would integrate with MongoDB Atlas API
	return DatabaseMetrics{
		CPUUtilization:    0.0, // Would be fetched from Atlas API
		MemoryUtilization: 0.0, // Would be fetched from Atlas API
		ConnectionCount:   0,   // Would be fetched from Atlas API
		ReadLatency:       0.0, // Would be calculated from operation timing
		WriteLatency:      0.0, // Would be calculated from operation timing
		Timestamp:         time.Now(),
	}, nil
}

// GetConnectionInfo returns connection information
func (m *MongoDBClient) GetConnectionInfo() ConnectionInfo {
	return ConnectionInfo{
		Type:       "mongodb",
		Host:       "mongodb-atlas", // Simplified
		Database:   m.config.Database,
		Collection: m.config.Collection,
	}
}
