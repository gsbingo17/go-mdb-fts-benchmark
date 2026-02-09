package database

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"mongodb-benchmarking-tool/internal/config"
)

// bsonToJSON converts a BSON document to a compact JSON string for logging
func bsonToJSON(doc interface{}) string {
	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Sprintf("<error serializing: %v>", err)
	}
	return string(jsonBytes)
}

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

// CreateTextIndex creates a text search index
func (m *MongoDBClient) CreateTextIndex(ctx context.Context) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "title", Value: "text"},
			{Key: "content", Value: "text"},
			{Key: "search_terms", Value: "text"},
		},
		Options: options.Index().SetDefaultLanguage("english"),
	}

	_, err := m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create text index: %w", err)
	}
	return nil
}

// CreateTokenTextIndex creates a text search index for token-based documents (cost_model mode)
func (m *MongoDBClient) CreateTokenTextIndex(ctx context.Context) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "text1", Value: "text"},
			{Key: "text2", Value: "text"},
			{Key: "text3", Value: "text"},
		},
		Options: options.Index().SetDefaultLanguage("english"),
	}

	_, err := m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create token text index: %w", err)
	}
	return nil
}

// CreateGeoIndex creates a 2dsphere index on the specified field for the default collection
func (m *MongoDBClient) CreateGeoIndex(ctx context.Context, fieldName string) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: fieldName, Value: "2dsphere"}},
	}

	_, err := m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create 2dsphere index on field '%s': %w", fieldName, err)
	}
	slog.Info("Created 2dsphere geo index", "field", fieldName)
	return nil
}

// CreateGeoIndexForCollection creates a 2dsphere index on the specified field for a specific collection
func (m *MongoDBClient) CreateGeoIndexForCollection(ctx context.Context, collectionName string, fieldName string) error {
	coll := m.database.Collection(collectionName)
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: fieldName, Value: "2dsphere"}},
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create 2dsphere index on '%s.%s': %w", collectionName, fieldName, err)
	}
	slog.Info("Created 2dsphere geo index", "collection", collectionName, "field", fieldName)
	return nil
}

// CreateTextIndexForCollection creates a progressive text search index based on shard number
// Shard 1: index on text1 only
// Shard 2: index on text1, text2
// Shard 3: index on text1, text2, text3
func (m *MongoDBClient) CreateTextIndexForCollection(ctx context.Context, collectionName string, shardNumber int) error {
	coll := m.database.Collection(collectionName)

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
		Options: options.Index().SetDefaultLanguage("english"),
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create progressive text index for collection %s (shard %d): %w", collectionName, shardNumber, err)
	}
	return nil
}

// DropIndexes drops all indexes except _id
func (m *MongoDBClient) DropIndexes(ctx context.Context) error {
	err := m.collection.Indexes().DropAll(ctx)
	return err
}

// DropIndexesForCollection drops all indexes for a specific collection except _id
func (m *MongoDBClient) DropIndexesForCollection(ctx context.Context, collectionName string) error {
	coll := m.database.Collection(collectionName)
	return coll.Indexes().DropAll(ctx)
}

// ExecuteTextSearch performs a text search query with optional result limit
func (m *MongoDBClient) ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error) {
	filter := bson.D{
		{Key: "$text", Value: bson.D{
			{Key: "$search", Value: query},
		}},
	}

	// Add textScore projection and sorting for proper FTS benchmarking
	projection := bson.D{
		{Key: "_id", Value: 1},
		{Key: "score", Value: bson.D{{Key: "$meta", Value: "textScore"}}},
	}

	// Configure find options with projection, sorting by relevance, and limit
	opts := options.Find().
		SetProjection(projection).
		SetSort(bson.D{{Key: "score", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})

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

// ExecuteGeoSearch performs geospatial search on the default collection
func (m *MongoDBClient) ExecuteGeoSearch(ctx context.Context, query interface{}, limit int) (int, error) {
	return m.ExecuteGeoSearchInCollection(ctx, m.config.Collection, query, limit)
}

// ExecuteGeoSearchInCollection performs geospatial search using $nearSphere in a specific collection
func (m *MongoDBClient) ExecuteGeoSearchInCollection(ctx context.Context, collectionName string, query interface{}, limit int) (int, error) {
	coll := m.database.Collection(collectionName)

	// Execute geospatial query with optional limit
	opts := options.Find()
	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	// Track query execution time
	startTime := time.Now()

	cursor, err := coll.Find(ctx, query, opts)
	if err != nil {
		return 0, fmt.Errorf("geospatial search failed in collection %s: %w", collectionName, err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		count++
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %w", err)
	}

	// Calculate latency
	latency := time.Since(startTime)

	// Debug log the query results after execution (matches Atlas Search pattern)
	slog.Debug("Geospatial query",
		"collection", collectionName,
		"limit", limit,
		"result_count", count,
		"latency_us", latency.Microseconds(),
		"latency", latency.String(),
		"raw_query", bsonToJSON(query))

	return count, nil
}

// ExecuteTextSearchInCollection performs a text search query in a specific collection
func (m *MongoDBClient) ExecuteTextSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error) {
	coll := m.database.Collection(collectionName)
	filter := bson.D{
		{Key: "$text", Value: bson.D{
			{Key: "$search", Value: query},
		}},
	}

	// Add textScore projection and sorting for proper FTS benchmarking
	projection := bson.D{
		{Key: "_id", Value: 1},
		{Key: "score", Value: bson.D{{Key: "$meta", Value: "textScore"}}},
	}

	// Configure find options with projection, sorting by relevance, and limit
	opts := options.Find().
		SetProjection(projection).
		SetSort(bson.D{{Key: "score", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})

	if limit > 0 {
		opts = opts.SetLimit(int64(limit))
	}

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("text search failed in collection %s: %w", collectionName, err)
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

// InsertDocumentInCollection inserts a single document into a specific collection
func (m *MongoDBClient) InsertDocumentInCollection(ctx context.Context, collectionName string, doc Document) error {
	coll := m.database.Collection(collectionName)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document into collection %s: %w", collectionName, err)
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

	// Use unordered inserts with w:1 write concern for maximum throughput during data seeding
	// - SetOrdered(false): Enables parallel batch processing (5-10x faster)
	// - w:1 write concern: Only waits for primary acknowledgment (reduces latency 100-500ms per batch)

	// Get collection with optimized write concern for bulk inserts
	collOpts := options.Collection().SetWriteConcern(writeconcern.W1())
	coll := m.database.Collection(m.config.Collection, collOpts)

	// Use unordered inserts for parallel processing
	insertOpts := options.InsertMany().SetOrdered(false)

	_, err := coll.InsertMany(ctx, documents, insertOpts)
	if err != nil {
		return fmt.Errorf("failed to insert documents: %w", err)
	}
	return nil
}

// InsertDocumentsInCollection inserts multiple documents into a specific collection
func (m *MongoDBClient) InsertDocumentsInCollection(ctx context.Context, collectionName string, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	documents := make([]interface{}, len(docs))
	for i, doc := range docs {
		documents[i] = doc
	}

	// Use unordered inserts with w:1 write concern for maximum throughput during data seeding
	// - SetOrdered(false): Enables parallel batch processing (5-10x faster)
	// - w:1 write concern: Only waits for primary acknowledgment (reduces latency 100-500ms per batch)

	// Get collection with optimized write concern for bulk inserts
	collOpts := options.Collection().SetWriteConcern(writeconcern.W1())
	coll := m.database.Collection(collectionName, collOpts)

	// Use unordered inserts for parallel processing
	insertOpts := options.InsertMany().SetOrdered(false)

	_, err := coll.InsertMany(ctx, documents, insertOpts)
	if err != nil {
		return fmt.Errorf("failed to insert documents into collection %s: %w", collectionName, err)
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

// CountDocumentsInCollection returns the number of documents in a specific collection
func (m *MongoDBClient) CountDocumentsInCollection(ctx context.Context, collectionName string) (int64, error) {
	coll := m.database.Collection(collectionName)
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents in collection %s: %w", collectionName, err)
	}
	return count, nil
}

// DropCollection drops the entire collection
func (m *MongoDBClient) DropCollection(ctx context.Context) error {
	return m.collection.Drop(ctx)
}

// DropCollectionByName drops a specific collection by name
func (m *MongoDBClient) DropCollectionByName(ctx context.Context, collectionName string) error {
	coll := m.database.Collection(collectionName)
	return coll.Drop(ctx)
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

// CreateSearchIndex creates Atlas Search index for default collection
func (m *MongoDBClient) CreateSearchIndex(ctx context.Context) error {
	return m.CreateSearchIndexForCollection(ctx, m.config.Collection)
}

// CreateSearchIndexForCollection creates a uniform Atlas Search index
// Unlike $text indexes, all collection shards use the SAME index definition
// indexing all three fields: text1, text2, text3
func (m *MongoDBClient) CreateSearchIndexForCollection(ctx context.Context, collectionName string) error {
	coll := m.database.Collection(collectionName)

	// Atlas Search index definition (Lucene-based)
	// All shards index all three fields for fair performance comparison
	indexDefinition := bson.D{
		{Key: "mappings", Value: bson.D{
			{Key: "dynamic", Value: false},
			{Key: "fields", Value: bson.D{
				{Key: "text1", Value: bson.D{
					{Key: "type", Value: "string"},
					{Key: "analyzer", Value: "lucene.standard"},
				}},
				{Key: "text2", Value: bson.D{
					{Key: "type", Value: "string"},
					{Key: "analyzer", Value: "lucene.standard"},
				}},
				{Key: "text3", Value: bson.D{
					{Key: "type", Value: "string"},
					{Key: "analyzer", Value: "lucene.standard"},
				}},
			}},
		}},
	}

	searchIndexModel := mongo.SearchIndexModel{
		Definition: indexDefinition,
		Options:    options.SearchIndexes().SetName("default"),
	}

	_, err := coll.SearchIndexes().CreateOne(ctx, searchIndexModel)
	if err != nil {
		return fmt.Errorf("failed to create Atlas Search index for collection %s: %w", collectionName, err)
	}

	return nil
}

// DropSearchIndexes drops all Atlas Search indexes for default collection
func (m *MongoDBClient) DropSearchIndexes(ctx context.Context) error {
	return m.DropSearchIndexesForCollection(ctx, m.config.Collection)
}

// DropSearchIndexesForCollection drops Atlas Search indexes for a specific collection
func (m *MongoDBClient) DropSearchIndexesForCollection(ctx context.Context, collectionName string) error {
	coll := m.database.Collection(collectionName)

	// List all search indexes
	cursor, err := coll.SearchIndexes().List(ctx, options.SearchIndexes().SetName(""))
	if err != nil {
		return fmt.Errorf("failed to list search indexes: %w", err)
	}
	defer cursor.Close(ctx)

	// Drop each index
	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		return fmt.Errorf("failed to decode search indexes: %w", err)
	}

	for _, idx := range indexes {
		if name, ok := idx["name"].(string); ok {
			if err := coll.SearchIndexes().DropOne(ctx, name); err != nil {
				return fmt.Errorf("failed to drop search index %s: %w", name, err)
			}
		}
	}

	return nil
}

// ExecuteAtlasSearch performs Atlas Search on default collection
func (m *MongoDBClient) ExecuteAtlasSearch(ctx context.Context, query string, limit int) (int, error) {
	return m.ExecuteAtlasSearchInCollection(ctx, m.config.Collection, query, limit)
}

// ExecuteAtlasSearchInCollection performs Atlas Search using $search aggregation
func (m *MongoDBClient) ExecuteAtlasSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error) {
	coll := m.database.Collection(collectionName)

	// Parse the query string into structured components
	parsed := ParseSearchQuery(query)

	// Build the appropriate Atlas Search query structure
	searchPaths := []string{"text1", "text2", "text3"}
	searchQuery := BuildAtlasSearchQuery(parsed, "default", searchPaths)

	// Debug log the raw Atlas Search query structure
	slog.Debug("Atlas Search query", "input_query", query, "raw_query", bsonToJSON(searchQuery))

	// Build $search aggregation pipeline
	pipeline := mongo.Pipeline{
		// Stage 1: $search with properly structured query
		{{Key: "$search", Value: searchQuery}},
	}

	// Stage 2: $limit (if specified)
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
	}

	// Stage 3: Project with search score
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.D{
		{Key: "_id", Value: 1},
		{Key: "score", Value: bson.D{{Key: "$meta", Value: "searchScore"}}},
	}}})

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("Atlas Search failed in collection %s: %w", collectionName, err)
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
