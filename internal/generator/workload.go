package generator

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

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
