package generator

import (
	"fmt"
	"math"
	"math/rand"
	"mongodb-benchmarking-tool/internal/config"
	"strings"
	"time"
)

// QueryRequest encapsulates all parameters needed for query generation
type QueryRequest struct {
	BaseCollectionName string
	TextShards         []int                    // Array of shard values to randomly select from
	QueryParams        []config.QueryParameters // Multiple query shapes to randomly select from
	Limits             []int                    // Array of limit values to randomly select from
	UseRandomQueries   bool
	RandomMaxParams    config.QueryParameters
}

// QueryResult contains the generated query and all execution parameters
type QueryResult struct {
	Query             string
	CollectionName    string
	Limit             int
	SelectedTextShard int
	SelectedParams    config.QueryParameters
}

// WorkloadGenerator generates search queries and workload patterns
type WorkloadGenerator struct {
	searchTerms []string
	queryTypes  []QueryType
	rng         *rand.Rand
}

// QueryType represents different types of search queries
type QueryType struct {
	Name       string
	Template   string
	Complexity int     // 1-5 scale
	Weight     float64 // Probability weight
}

// NewWorkloadGenerator creates a new workload generator
func NewWorkloadGenerator(seed int64) *WorkloadGenerator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &WorkloadGenerator{
		searchTerms: getSearchTerms(),
		queryTypes:  getQueryTypes(),
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// GenerateSearchQuery creates a search query for text search operations
func (wg *WorkloadGenerator) GenerateSearchQuery() string {
	queryType := wg.selectQueryType()

	switch queryType.Name {
	case "single_term":
		return wg.generateSingleTermQuery()
	case "multi_term":
		return wg.generateMultiTermQuery()
	case "phrase":
		return wg.generatePhraseQuery()
	default:
		return wg.generateSingleTermQuery()
	}
}

// GenerateTextSearchQueries creates a batch of search queries
func (wg *WorkloadGenerator) GenerateTextSearchQueries(count int) []string {
	queries := make([]string, count)
	for i := 0; i < count; i++ {
		queries[i] = wg.GenerateSearchQuery()
	}
	return queries
}

// selectQueryType chooses a query type based on weights
func (wg *WorkloadGenerator) selectQueryType() QueryType {
	totalWeight := 0.0
	for _, qt := range wg.queryTypes {
		totalWeight += qt.Weight
	}

	random := wg.rng.Float64() * totalWeight
	currentWeight := 0.0

	for _, qt := range wg.queryTypes {
		currentWeight += qt.Weight
		if random <= currentWeight {
			return qt
		}
	}

	return wg.queryTypes[0] // Fallback
}

// generateSingleTermQuery creates a simple single-term search
func (wg *WorkloadGenerator) generateSingleTermQuery() string {
	term := wg.searchTerms[wg.rng.Intn(len(wg.searchTerms))]
	return term
}

// generateMultiTermQuery creates a multi-term search
func (wg *WorkloadGenerator) generateMultiTermQuery() string {
	termCount := wg.rng.Intn(3) + 2 // 2-4 terms
	terms := make([]string, termCount)

	for i := 0; i < termCount; i++ {
		terms[i] = wg.searchTerms[wg.rng.Intn(len(wg.searchTerms))]
	}

	return strings.Join(terms, " ")
}

// generatePhraseQuery creates a phrase search with quotes
func (wg *WorkloadGenerator) generatePhraseQuery() string {
	phrases := []string{
		"database performance",
		"text search",
		"cloud computing",
		"machine learning",
		"web development",
		"data analytics",
		"system architecture",
		"user experience",
		"api design",
		"best practices",
	}

	phrase := phrases[wg.rng.Intn(len(phrases))]
	return fmt.Sprintf("\"%s\"", phrase)
}

// GetWorkloadPattern returns a pattern for read/write operations
func (wg *WorkloadGenerator) GetWorkloadPattern(readPercent int) []string {
	pattern := make([]string, 100)

	for i := 0; i < readPercent; i++ {
		pattern[i] = "read"
	}
	for i := readPercent; i < 100; i++ {
		pattern[i] = "write"
	}

	// Shuffle the pattern
	wg.rng.Shuffle(len(pattern), func(i, j int) {
		pattern[i], pattern[j] = pattern[j], pattern[i]
	})

	return pattern
}

// EstimateQueryComplexity estimates the computational complexity of a query
func (wg *WorkloadGenerator) EstimateQueryComplexity(query string) int {
	complexity := 1

	// Basic heuristics for complexity estimation
	termCount := len(strings.Fields(query))
	complexity += termCount - 1

	// Phrase searches are more complex
	if strings.Contains(query, "\"") {
		complexity += 2
	}

	// Boolean operators add complexity
	if strings.Contains(query, "AND") || strings.Contains(query, "OR") {
		complexity += 3
	}

	return complexity
}

// getSearchTerms returns common search terms for workload generation
// Uses the same vocabulary as document generation to ensure search hits
func getSearchTerms() []string {
	return []string{
		// Technology terms - high frequency in both content and queries
		"database", "mongodb", "documentdb", "search", "index",
		"query", "performance", "optimization", "scaling", "cloud",
		"aws", "atlas", "aggregation", "pipeline", "sharding",
		"replication", "consistency", "availability", "durability",

		// Development terms - commonly searched
		"api", "rest", "graphql", "microservices", "architecture",
		"design", "patterns", "framework", "library", "tool",
		"testing", "deployment", "monitoring", "logging", "metrics",

		// Business terms - realistic search scenarios
		"analytics", "intelligence", "insights", "reporting", "dashboard",
		"visualization", "data", "information", "knowledge", "decision",
		"strategy", "planning", "execution", "management", "operations",

		// Performance terms - benchmark-relevant
		"latency", "throughput", "capacity", "scalability", "efficiency",
		"speed", "fast", "slow", "bottleneck", "optimization",
		"tuning", "profiling", "benchmarking", "testing", "validation",
	}
}

// getQueryTypes returns different types of queries with their characteristics
func getQueryTypes() []QueryType {
	return []QueryType{
		{
			Name:       "single_term",
			Template:   "{TERM}",
			Complexity: 1,
			Weight:     0.5, // 50% of queries
		},
		{
			Name:       "multi_term",
			Template:   "{TERM1} {TERM2} {TERM3}",
			Complexity: 2,
			Weight:     0.3, // 30% of queries
		},
		{
			Name:       "phrase",
			Template:   "\"{PHRASE}\"",
			Complexity: 3,
			Weight:     0.2, // 20% of queries
		},
	}
}

// GenerateRealisticWorkload creates a workload that simulates real-world usage patterns
func (wg *WorkloadGenerator) GenerateRealisticWorkload(duration time.Duration, qps float64) []WorkloadEvent {
	totalEvents := int(duration.Seconds() * qps)
	events := make([]WorkloadEvent, totalEvents)

	currentTime := time.Now()
	interval := time.Duration(float64(time.Second) / qps)

	for i := 0; i < totalEvents; i++ {
		events[i] = WorkloadEvent{
			Timestamp: currentTime.Add(time.Duration(i) * interval),
			Type:      wg.selectOperationType(),
			Query:     wg.GenerateSearchQuery(),
		}
	}

	return events
}

// WorkloadEvent represents a single operation in the workload
type WorkloadEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"` // "read" or "write"
	Query     string    `json:"query,omitempty"`
}

// selectOperationType chooses between read and write operations
func (wg *WorkloadGenerator) selectOperationType() string {
	// This would be configured based on read/write ratio
	// For now, default to 80/20 read/write
	if wg.rng.Float64() < 0.8 {
		return "read"
	}
	return "write"
}

// GenerateTokenQuery creates a token-based text search query for cost modeling
func (wg *WorkloadGenerator) GenerateTokenQuery(textShards int, params config.QueryParameters) string {
	return MakeDatabaseRawTextSearchQuery(
		wg.rng,
		textShards,
		params.PositiveTerms,
		params.NegativeTerms,
		params.PositivePhrases,
		params.NegativePhrases,
		params.PhraseLength,
	)
}

// GenerateTokenQueryRequest generates a complete query with randomly selected parameters
// from the provided pre-generated arrays, enabling comprehensive multi-dimensional testing.
// Query shapes are always pre-generated at startup for reproducibility (matching Java reference).
func (wg *WorkloadGenerator) GenerateTokenQueryRequest(req QueryRequest) QueryResult {
	// Step 1: Randomly select textShards from array
	textShard := req.TextShards[wg.rng.Intn(len(req.TextShards))]

	// Step 2: Randomly select query parameters from pre-generated shapes
	params := req.QueryParams[wg.rng.Intn(len(req.QueryParams))]

	// Step 3: Randomly select limit from array
	limit := req.Limits[wg.rng.Intn(len(req.Limits))]

	// Step 4: Generate query string
	query := wg.GenerateTokenQuery(textShard, params)

	// Step 5: Determine collection name
	collectionName := fmt.Sprintf("%s%d", req.BaseCollectionName, textShard)

	return QueryResult{
		Query:             query,
		CollectionName:    collectionName,
		Limit:             limit,
		SelectedTextShard: textShard,
		SelectedParams:    params,
	}
}

// SkewLow returns a random integer in [0, max], skewed toward lower values using a power law.
// This mimics the Java implementation's skew distribution for realistic query generation.
func SkewLow(rng *rand.Rand, max int, power float64) int {
	if max <= 0 {
		return 0
	}
	skewed := math.Pow(rng.Float64(), power)
	return int(skewed * float64(max+1))
}

// MakeRandomQueryParameters generates random query parameters with a skewed distribution.
// This creates realistic query patterns similar to the Java reference implementation.
func (wg *WorkloadGenerator) MakeRandomQueryParameters(maxParams config.QueryParameters) config.QueryParameters {
	// Skews for each parameter type (from Java reference)
	skews := []float64{3.0, 3.0, 2.2, 2.2, 2.2}

	var qp config.QueryParameters
	// Keep generating until we get at least one term/phrase
	for {
		qp.PositiveTerms = SkewLow(wg.rng, maxParams.PositiveTerms, skews[0])
		qp.NegativeTerms = SkewLow(wg.rng, maxParams.NegativeTerms, skews[1])
		qp.PositivePhrases = SkewLow(wg.rng, maxParams.PositivePhrases, skews[2])
		qp.NegativePhrases = SkewLow(wg.rng, maxParams.NegativePhrases, skews[3])
		// Phrase length should be at least 1
		qp.PhraseLength = 1 + SkewLow(wg.rng, maxParams.PhraseLength-1, skews[4])

		// Ensure at least one term/phrase is present
		if qp.PositiveTerms+qp.NegativeTerms+qp.PositivePhrases+qp.NegativePhrases > 0 {
			break
		}
	}
	return qp
}

// GenerateQueryShapes pre-generates query shapes for reproducible benchmarking.
// This matches the reference Java implementation's approach of generating shapes once at startup.
func GenerateQueryShapes(seed string, count int, maxParams config.QueryParameters) []config.QueryParameters {
	rng := MakeRandom(seed)
	shapes := make([]config.QueryParameters, count)

	// Skews for each parameter type (from Java reference)
	skews := []float64{3.0, 3.0, 2.2, 2.2, 2.2}

	for i := 0; i < count; i++ {
		var qp config.QueryParameters
		// Keep generating until we get at least one term/phrase
		for {
			qp.PositiveTerms = SkewLow(rng, maxParams.PositiveTerms, skews[0])
			qp.NegativeTerms = SkewLow(rng, maxParams.NegativeTerms, skews[1])
			qp.PositivePhrases = SkewLow(rng, maxParams.PositivePhrases, skews[2])
			qp.NegativePhrases = SkewLow(rng, maxParams.NegativePhrases, skews[3])
			// Phrase length should be at least 1
			qp.PhraseLength = 1 + SkewLow(rng, maxParams.PhraseLength-1, skews[4])

			// Ensure at least one term/phrase is present
			if qp.PositiveTerms+qp.NegativeTerms+qp.PositivePhrases+qp.NegativePhrases > 0 {
				break
			}
		}
		shapes[i] = qp
	}

	return shapes
}
