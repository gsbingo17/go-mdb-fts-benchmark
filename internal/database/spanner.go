package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"mongodb-benchmarking-tool/internal/config"
)

// SpannerClient implements the Database interface for Google Cloud Spanner
type SpannerClient struct {
	client           *spanner.Client
	adminClient      *database.DatabaseAdminClient
	metricsCollector *SpannerMetricsCollector
	config           config.DatabaseConfig
	projectID        string
	instanceID       string
	databaseName     string
	defaultTable     string
}

// NewSpannerClient creates a new Spanner client
func NewSpannerClient(cfg config.DatabaseConfig) *SpannerClient {
	return &SpannerClient{
		config:       cfg,
		projectID:    cfg.ProjectID,
		instanceID:   cfg.InstanceID,
		databaseName: cfg.Database,
		defaultTable: cfg.Table,
	}
}

// Connect establishes connection to Spanner
func (s *SpannerClient) Connect(ctx context.Context) error {
	slog.Info("Connecting to Google Cloud Spanner",
		"project", s.projectID,
		"instance", s.instanceID,
		"database", s.databaseName)

	// Determine credentials option
	var opts []option.ClientOption
	if s.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(s.config.CredentialsFile))
	}

	// Create data client
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		s.projectID, s.instanceID, s.databaseName)

	client, err := spanner.NewClient(ctx, dbPath, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Spanner client: %w", err)
	}
	s.client = client

	// Create admin client for DDL operations
	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to create Spanner admin client: %w", err)
	}
	s.adminClient = adminClient

	// Verify connection with a simple query
	if err := s.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping Spanner: %w", err)
	}

	// Create metrics collector for CPU monitoring
	metricsCollector, err := NewSpannerMetricsCollector(
		s.projectID,
		s.instanceID,
		s.databaseName,
		s.config.CredentialsFile,
	)
	if err != nil {
		slog.Warn("Failed to create metrics collector, metrics will be unavailable", "error", err)
		// Don't fail connection if metrics collector fails
	} else {
		s.metricsCollector = metricsCollector
		slog.Info("Metrics collector initialized successfully")
	}

	slog.Info("Spanner connection established successfully")
	return nil
}

// Close closes the Spanner connection
func (s *SpannerClient) Close() error {
	if s.metricsCollector != nil {
		s.metricsCollector.Close()
	}
	if s.client != nil {
		s.client.Close()
	}
	if s.adminClient != nil {
		return s.adminClient.Close()
	}
	return nil
}

// Ping verifies the connection is alive
func (s *SpannerClient) Ping(ctx context.Context) error {
	if s.client == nil {
		return fmt.Errorf("not connected to Spanner")
	}

	// Execute a simple query to verify connection
	stmt := spanner.Statement{SQL: "SELECT 1"}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

// getDatabasePath returns the full database path for admin operations
func (s *SpannerClient) getDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		s.projectID, s.instanceID, s.databaseName)
}

// getTableNameForShard returns the table name for a specific shard number
func (s *SpannerClient) getTableNameForShard(shard int) string {
	return fmt.Sprintf("%s%d", s.defaultTable, shard)
}

// CreateTable creates a single table with full-text search columns
func (s *SpannerClient) CreateTable(ctx context.Context, tableName string) error {
	slog.Info("Creating Spanner table", "table", tableName)

	// Build DDL statement for table with tokenized columns
	ddl := fmt.Sprintf(`
		CREATE TABLE %s (
			id STRING(36) NOT NULL,
			text1 STRING(MAX),
			text2 STRING(MAX),
			text3 STRING(MAX),
			created_at TIMESTAMP,
			text1_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(text1)) HIDDEN,
			text2_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(text2)) HIDDEN,
			text3_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(text3)) HIDDEN
		) PRIMARY KEY (id)
	`, tableName)

	// Execute DDL via Admin API
	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.getDatabasePath(),
		Statements: []string{ddl},
	})
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Wait for operation to complete
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("table creation failed for %s: %w", tableName, err)
	}

	slog.Info("Table created successfully", "table", tableName)
	return nil
}

// CreateTableForShard creates a table for a specific shard number
func (s *SpannerClient) CreateTableForShard(ctx context.Context, shardNumber int) error {
	tableName := s.getTableNameForShard(shardNumber)
	return s.CreateTable(ctx, tableName)
}

// DropTable drops a single table
func (s *SpannerClient) DropTable(ctx context.Context, tableName string) error {
	slog.Info("Dropping Spanner table", "table", tableName)

	ddl := fmt.Sprintf("DROP TABLE %s", tableName)

	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.getDatabasePath(),
		Statements: []string{ddl},
	})
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("table drop failed for %s: %w", tableName, err)
	}

	slog.Info("Table dropped successfully", "table", tableName)
	return nil
}

// DropCollection drops the default table
func (s *SpannerClient) DropCollection(ctx context.Context) error {
	return s.DropTable(ctx, s.defaultTable)
}

// DropCollectionByName drops a specific table by name
func (s *SpannerClient) DropCollectionByName(ctx context.Context, tableName string) error {
	return s.DropTable(ctx, tableName)
}

// CreateSearchIndex creates search index for default table
func (s *SpannerClient) CreateSearchIndex(ctx context.Context) error {
	return s.CreateSearchIndexForTable(ctx, s.defaultTable, 1)
}

// CreateSearchIndexForCollection creates search index for a specific collection (table)
// This is the interface method required for multi-shard support
func (s *SpannerClient) CreateSearchIndexForCollection(ctx context.Context, collectionName string) error {
	// Extract shard number from collection name (e.g., "SearchWords1" -> 1)
	// For uniform indexing, shard number doesn't affect index definition but is used for naming
	shardNumber := 1
	if len(collectionName) > len(s.defaultTable) {
		// Try to parse the shard number from the end of the collection name
		suffix := collectionName[len(s.defaultTable):]
		if n, err := fmt.Sscanf(suffix, "%d", &shardNumber); err == nil && n == 1 {
			// Successfully parsed shard number
		}
	}
	return s.CreateSearchIndexForTable(ctx, collectionName, shardNumber)
}

// CreateSearchIndexForTable creates a uniform search index
// ALL tables use the SAME index definition (text1, text2, text3)
func (s *SpannerClient) CreateSearchIndexForTable(ctx context.Context, tableName string, shardNumber int) error {
	indexName := fmt.Sprintf("SearchIndex%d", shardNumber)

	// Check if index already exists
	existingIndexes, err := s.listSearchIndexes(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to list existing indexes: %w", err)
	}

	for _, existing := range existingIndexes {
		if existing == indexName {
			slog.Info("Search index already exists, verifying readiness",
				"table", tableName,
				"index", indexName)
			// Verify existing index is ready
			if err := s.verifyIndexReady(ctx, tableName, indexName); err != nil {
				return fmt.Errorf("existing index not ready: %w", err)
			}
			return nil
		}
	}

	slog.Info("Creating search index",
		"table", tableName,
		"index", indexName,
		"fields", "text1, text2, text3")

	// Uniform index definition - ALL shards index ALL three fields
	ddl := fmt.Sprintf(`
		CREATE SEARCH INDEX %s ON %s(
			text1_tokens,
			text2_tokens,
			text3_tokens
		)
	`, indexName, tableName)

	// Execute DDL
	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.getDatabasePath(),
		Statements: []string{ddl},
	})
	if err != nil {
		return fmt.Errorf("failed to create search index %s: %w", indexName, err)
	}

	// Wait for completion (can take several minutes for large tables)
	slog.Info("Waiting for search index creation to complete (may take several minutes)...",
		"index", indexName)
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("search index creation failed for %s: %w", indexName, err)
	}

	// Verify index is ready for queries
	slog.Info("Verifying search index readiness", "index", indexName)
	if err := s.verifyIndexReady(ctx, tableName, indexName); err != nil {
		return fmt.Errorf("index verification failed: %w", err)
	}

	// Run warmup queries to ensure index is fully operational
	slog.Info("Running warmup queries on new index", "index", indexName)
	if err := s.warmupIndex(ctx, tableName); err != nil {
		slog.Warn("Index warmup failed (non-critical)", "error", err)
		// Don't fail on warmup errors - this is a best-effort optimization
	}

	slog.Info("Search index created and verified successfully",
		"table", tableName,
		"index", indexName)
	return nil
}

// listSearchIndexes returns all search index names for a table
func (s *SpannerClient) listSearchIndexes(ctx context.Context, tableName string) ([]string, error) {
	// Query INFORMATION_SCHEMA.INDEXES for search indexes
	stmt := spanner.Statement{
		SQL: `
			SELECT INDEX_NAME
			FROM INFORMATION_SCHEMA.INDEXES
			WHERE TABLE_NAME = @tableName 
			  AND INDEX_TYPE = 'SEARCH'
		`,
		Params: map[string]interface{}{
			"tableName": tableName,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var indexes []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list search indexes: %w", err)
		}

		var indexName string
		if err := row.Columns(&indexName); err != nil {
			return nil, fmt.Errorf("failed to read index name: %w", err)
		}
		indexes = append(indexes, indexName)
	}

	return indexes, nil
}

// DropSearchIndexes drops all search indexes for default table
func (s *SpannerClient) DropSearchIndexes(ctx context.Context) error {
	return s.DropSearchIndexesForTable(ctx, s.defaultTable)
}

// DropSearchIndexesForCollection drops search indexes for a specific collection (interface method)
func (s *SpannerClient) DropSearchIndexesForCollection(ctx context.Context, collectionName string) error {
	return s.DropSearchIndexesForTable(ctx, collectionName)
}

// DropSearchIndexesForTable drops search indexes for a specific table
func (s *SpannerClient) DropSearchIndexesForTable(ctx context.Context, tableName string) error {
	slog.Info("Dropping search indexes", "table", tableName)

	// List existing indexes first
	indexes, err := s.listSearchIndexes(ctx, tableName)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		slog.Info("No search indexes to drop", "table", tableName)
		return nil
	}

	// Build DROP INDEX statements
	var dropStatements []string
	for _, indexName := range indexes {
		ddl := fmt.Sprintf("DROP SEARCH INDEX %s", indexName)
		dropStatements = append(dropStatements, ddl)
		slog.Info("Dropping search index", "index", indexName)
	}

	// Execute all DROP statements
	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.getDatabasePath(),
		Statements: dropStatements,
	})
	if err != nil {
		return fmt.Errorf("failed to drop search indexes: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for index drop: %w", err)
	}

	slog.Info("Search indexes dropped successfully", "table", tableName, "count", len(indexes))
	return nil
}

// Placeholder methods - to be implemented
func (s *SpannerClient) CreateTextIndex(ctx context.Context) error {
	return fmt.Errorf("CreateTextIndex not applicable for Spanner - use CreateSearchIndex")
}

func (s *SpannerClient) CreateTextIndexForCollection(ctx context.Context, collectionName string, shardNumber int) error {
	return fmt.Errorf("CreateTextIndexForCollection not applicable for Spanner - use CreateSearchIndexForTable")
}

func (s *SpannerClient) DropIndexes(ctx context.Context) error {
	return s.DropSearchIndexes(ctx)
}

func (s *SpannerClient) DropIndexesForCollection(ctx context.Context, collectionName string) error {
	return s.DropSearchIndexesForTable(ctx, collectionName)
}

// ExecuteSpannerSearch executes a full-text search on the default table
func (s *SpannerClient) ExecuteSpannerSearch(ctx context.Context, query string, limit int) (int, error) {
	return s.ExecuteSpannerSearchInTable(ctx, s.defaultTable, query, limit, "AND")
}

// ExecuteSpannerSearchInTable executes a ranked full-text search on a specific table
// Uses Spanner's SCORE() function for relevance ranking, similar to Atlas Search
// Returns key-only results (id and score) sorted by relevance score
func (s *SpannerClient) ExecuteSpannerSearchInTable(ctx context.Context, tableName string, query string, limit int, operator string) (int, error) {
	if query == "" {
		return 0, fmt.Errorf("query cannot be empty")
	}

	// Build Spanner SEARCH query using SpannerQueryBuilder
	builder := NewSpannerQueryBuilder()
	searchQuery := builder.BuildSearchQuery(query, operator)

	if searchQuery == "" {
		return 0, fmt.Errorf("failed to build search query")
	}

	// Debug: Log the raw Spanner SEARCH query
	slog.Debug("Spanner SEARCH query generated",
		"table", tableName,
		"input_query", query,
		"operator", operator,
		"search_query", searchQuery,
		"limit", limit)

	// Use combined_tokens column directly (single TOKENLIST with all text fields)
	// Schema: combined_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(text1 || ' ' || text2 || ' ' || text3)) HIDDEN
	// This is simpler and more efficient than TOKENLIST_CONCAT([text1_tokens, text2_tokens, text3_tokens])

	// Build SQL with ranked search pattern (matching Atlas Search):
	// 1. SELECT id and score (key-only return with relevance score like Atlas Search)
	// 2. WHERE SEARCH() for filtering with built query
	// 3. ORDER BY SCORE() DESC for relevance ranking (using score alias to avoid recalculation)
	// 4. LIMIT for result set size
	sql := fmt.Sprintf(`
		SELECT
			id,
			SCORE(combined_tokens, @searchQuery) AS score
		FROM %s
		WHERE SEARCH(combined_tokens, @searchQuery)
		ORDER BY score DESC
		LIMIT @limit
	`, tableName)

	// Debug: Log the complete SQL query
	slog.Debug("Spanner SQL query",
		"table", tableName,
		"sql", sql,
		"searchQuery_param", searchQuery)

	stmt := spanner.Statement{
		SQL: sql,
		Params: map[string]interface{}{
			"searchQuery": searchQuery,
			"limit":       limit,
		},
	}

	// Execute query
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	// Count results
	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("search query failed: %w", err)
		}
		count++
	}

	return count, nil
}

// ExecuteTextSearch executes a text search (maps to Spanner search)
func (s *SpannerClient) ExecuteTextSearch(ctx context.Context, query string, limit int) (int, error) {
	return s.ExecuteSpannerSearch(ctx, query, limit)
}

// ExecuteTextSearchInCollection executes a text search in a specific table
func (s *SpannerClient) ExecuteTextSearchInCollection(ctx context.Context, collectionName string, query string, limit int) (int, error) {
	return s.ExecuteSpannerSearchInTable(ctx, collectionName, query, limit, "AND")
}

// ExecuteAtlasSearch executes an Atlas-style search (maps to Spanner search)
func (s *SpannerClient) ExecuteAtlasSearch(ctx context.Context, query string, limit int) (int, error) {
	return s.ExecuteSpannerSearch(ctx, query, limit)
}

// ExecuteAtlasSearchInCollection executes Atlas search in a specific table
func (s *SpannerClient) ExecuteAtlasSearchInCollection(ctx context.Context, collectionName string, query string, limit int, operator string) (int, error) {
	return s.ExecuteSpannerSearchInTable(ctx, collectionName, query, limit, operator)
}

// InsertDocument inserts a single document into the default table
func (s *SpannerClient) InsertDocument(ctx context.Context, doc Document) error {
	return s.InsertDocumentInCollection(ctx, s.defaultTable, doc)
}

// InsertDocumentInCollection inserts a single document into a specific table
func (s *SpannerClient) InsertDocumentInCollection(ctx context.Context, tableName string, doc Document) error {
	// Generate ID if not provided
	id := doc.ID
	if id == "" {
		id = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Create mutation
	m := spanner.Insert(
		tableName,
		[]string{"id", "text1", "text2", "text3", "created_at"},
		[]interface{}{id, doc.Text1, doc.Text2, doc.Text3, doc.CreatedAt},
	)

	// Apply mutation
	_, err := s.client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	return nil
}

// InsertDocuments inserts multiple documents into the default table
func (s *SpannerClient) InsertDocuments(ctx context.Context, docs []Document) error {
	return s.InsertDocumentsInCollection(ctx, s.defaultTable, docs)
}

// InsertDocumentsInCollection inserts multiple documents into a specific table using parallel chunked batch inserts
// Spanner has a 100MB transaction limit, so we split large batches into smaller chunks
// Uses parallel processing with worker pool for optimal throughput
func (s *SpannerClient) InsertDocumentsInCollection(ctx context.Context, tableName string, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	// Conservative chunk size to stay under 100MB transaction limit
	// Token-based documents with TOKENLIST overhead are ~460-765KB each
	// 100 docs × 500KB average = 50MB (safe 50MB margin below 100MB limit)
	chunkSize := 100

	// Use parallel workers for concurrent chunk processing
	maxWorkers := 10
	workerCount := maxWorkers
	if len(docs) < chunkSize*maxWorkers {
		// Reduce workers for small datasets
		workerCount = (len(docs) / chunkSize) + 1
		if workerCount > maxWorkers {
			workerCount = maxWorkers
		}
		if workerCount < 1 {
			workerCount = 1
		}
	}

	// Split documents into chunks
	var chunks [][]Document
	for i := 0; i < len(docs); i += chunkSize {
		end := i + chunkSize
		if end > len(docs) {
			end = len(docs)
		}
		chunks = append(chunks, docs[i:end])
	}

	slog.Info("Starting parallel batch insert",
		"table", tableName,
		"total_docs", len(docs),
		"chunk_size", chunkSize,
		"chunks", len(chunks),
		"workers", workerCount)

	// Create error channel and worker pool
	type chunkJob struct {
		index int
		docs  []Document
	}

	jobs := make(chan chunkJob, len(chunks))
	errors := make(chan error, len(chunks))

	// Track progress
	startTime := time.Now()
	var processedDocs int64
	var processedChunks int64

	// Start workers
	for w := 0; w < workerCount; w++ {
		go func(workerID int) {
			for job := range jobs {
				mutations := make([]*spanner.Mutation, 0, len(job.docs))

				for _, doc := range job.docs {
					// Generate ID if not provided
					id := doc.ID
					if id == "" {
						id = fmt.Sprintf("%d", time.Now().UnixNano())
					}

					m := spanner.Insert(
						tableName,
						[]string{"id", "text1", "text2", "text3", "created_at"},
						[]interface{}{id, doc.Text1, doc.Text2, doc.Text3, doc.CreatedAt},
					)
					mutations = append(mutations, m)
				}

				// Apply chunk in a single transaction
				_, err := s.client.Apply(ctx, mutations)
				if err != nil {
					errors <- fmt.Errorf("worker %d failed chunk %d: %w", workerID, job.index, err)
					return
				}

				// Track progress
				chunkNum := processedChunks + 1
				processedChunks++
				docsInChunk := int64(len(job.docs))
				processedDocs += docsInChunk

				// Log progress every 10 chunks or on last chunk
				if chunkNum%10 == 0 || chunkNum == int64(len(chunks)) {
					elapsed := time.Since(startTime)
					docsPerSec := float64(processedDocs) / elapsed.Seconds()
					slog.Info("Batch insert progress",
						"table", tableName,
						"chunks_done", chunkNum,
						"total_chunks", len(chunks),
						"docs_inserted", processedDocs,
						"total_docs", len(docs),
						"docs_per_sec", fmt.Sprintf("%.0f", docsPerSec),
						"elapsed", elapsed.Round(time.Second))
				}

				errors <- nil
			}
		}(w)
	}

	// Send jobs to workers
	for i, chunk := range chunks {
		jobs <- chunkJob{index: i, docs: chunk}
	}
	close(jobs)

	// Collect results
	var firstError error
	for i := 0; i < len(chunks); i++ {
		if err := <-errors; err != nil && firstError == nil {
			firstError = err
		}
	}

	if firstError != nil {
		return firstError
	}

	elapsed := time.Since(startTime)
	docsPerSec := float64(len(docs)) / elapsed.Seconds()
	slog.Info("Batch insert completed",
		"table", tableName,
		"total_docs", len(docs),
		"chunks", len(chunks),
		"workers", workerCount,
		"total_time", elapsed.Round(time.Second),
		"docs_per_sec", fmt.Sprintf("%.0f", docsPerSec))

	return nil
}

// CountDocuments counts documents in the default table
func (s *SpannerClient) CountDocuments(ctx context.Context) (int64, error) {
	return s.CountDocumentsInCollection(ctx, s.defaultTable)
}

// CountDocumentsInCollection counts documents in a specific table
func (s *SpannerClient) CountDocumentsInCollection(ctx context.Context, tableName string) (int64, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tableName),
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, fmt.Errorf("failed to read count: %w", err)
	}

	return count, nil
}

func (s *SpannerClient) GetMetrics(ctx context.Context) (DatabaseMetrics, error) {
	if s.metricsCollector == nil {
		return DatabaseMetrics{
			Timestamp: time.Now(),
		}, nil
	}

	return s.metricsCollector.GetMetrics(ctx)
}

func (s *SpannerClient) GetConnectionInfo() ConnectionInfo {
	return ConnectionInfo{
		Type:       "spanner",
		Host:       fmt.Sprintf("%s/%s", s.projectID, s.instanceID),
		Database:   s.databaseName,
		Collection: s.defaultTable,
	}
}

// verifyIndexReady checks if a search index is ready for use
func (s *SpannerClient) verifyIndexReady(ctx context.Context, tableName, indexName string) error {
	// Query INFORMATION_SCHEMA.INDEXES to check index state
	// Note: Spanner doesn't expose INDEX_STATE for search indexes in the same way as secondary indexes
	// The op.Wait() call already ensures the index is built, so we just verify it exists
	stmt := spanner.Statement{
		SQL: `
			SELECT INDEX_NAME, INDEX_TYPE
			FROM INFORMATION_SCHEMA.INDEXES
			WHERE TABLE_NAME = @tableName 
			  AND INDEX_NAME = @indexName
			  AND INDEX_TYPE = 'SEARCH'
		`,
		Params: map[string]interface{}{
			"tableName": tableName,
			"indexName": indexName,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return fmt.Errorf("search index %s not found on table %s", indexName, tableName)
	}
	if err != nil {
		return fmt.Errorf("failed to verify index readiness: %w", err)
	}

	var name, indexType string
	if err := row.Columns(&name, &indexType); err != nil {
		return fmt.Errorf("failed to read index info: %w", err)
	}

	slog.Info("Search index verified ready",
		"table", tableName,
		"index", indexName,
		"type", indexType)

	return nil
}

// warmupIndex runs sample queries to warm up the search index
func (s *SpannerClient) warmupIndex(ctx context.Context, tableName string) error {
	// Run a few simple test queries to ensure the index is fully operational
	// This helps avoid cold-start performance issues in benchmarks
	warmupQueries := []string{"test", "warmup", "benchmark"}

	for i, query := range warmupQueries {
		// Use a short timeout for warmup queries
		warmupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		_, err := s.ExecuteSpannerSearchInTable(warmupCtx, tableName, query, 10, "AND")
		if err != nil {
			// Log but don't fail - warmup is best-effort
			slog.Debug("Warmup query failed (non-critical)",
				"query_num", i+1,
				"query", query,
				"error", err)
			continue
		}

		slog.Debug("Warmup query completed",
			"query_num", i+1,
			"query", query)
	}

	return nil
}

// CreateGeoIndex - geospatial indexing not supported on Spanner
func (s *SpannerClient) CreateGeoIndex(ctx context.Context, fieldName string) error {
	return fmt.Errorf("geospatial indexing is not supported on Google Cloud Spanner")
}

// CreateGeoIndexForCollection - geospatial indexing not supported on Spanner
func (s *SpannerClient) CreateGeoIndexForCollection(ctx context.Context, collectionName string, fieldName string) error {
	return fmt.Errorf("geospatial indexing is not supported on Google Cloud Spanner")
}

// ExecuteGeoSearch - geospatial search not supported on Spanner
func (s *SpannerClient) ExecuteGeoSearch(ctx context.Context, query interface{}, limit int) (int, error) {
	return 0, fmt.Errorf("geospatial search is not supported on Google Cloud Spanner")
}

// ExecuteGeoSearchInCollection - geospatial search not supported on Spanner
func (s *SpannerClient) ExecuteGeoSearchInCollection(ctx context.Context, collectionName string, query interface{}, limit int) (int, error) {
	return 0, fmt.Errorf("geospatial search is not supported on Google Cloud Spanner")
}
