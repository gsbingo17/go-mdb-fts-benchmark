package database

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ParsedQuery represents a parsed search query with structured components
type ParsedQuery struct {
	PositiveTerms   []string
	NegativeTerms   []string
	PositivePhrases []string
	NegativePhrases []string
}

// ParseSearchQuery parses a raw query string into structured components
// Handles formats like: itv09du abc12xy -def00ab "phrase one" -"phrase two"
// Tokens are 7-character alphanumeric strings (3 letters + 2 digits + 2 letters)
func ParseSearchQuery(query string) ParsedQuery {
	var parsed ParsedQuery

	// Remove extra whitespace
	query = strings.TrimSpace(query)
	if query == "" {
		return parsed
	}

	i := 0
	for i < len(query) {
		// Skip whitespace
		for i < len(query) && query[i] == ' ' {
			i++
		}
		if i >= len(query) {
			break
		}

		// Check for negative prefix
		isNegative := false
		if query[i] == '-' {
			isNegative = true
			i++
		}

		// Check for phrase (starts with quote)
		if i < len(query) && query[i] == '"' {
			// Find closing quote
			i++ // Skip opening quote
			start := i
			for i < len(query) && query[i] != '"' {
				i++
			}

			if i > start {
				phrase := query[start:i]
				if isNegative {
					parsed.NegativePhrases = append(parsed.NegativePhrases, phrase)
				} else {
					parsed.PositivePhrases = append(parsed.PositivePhrases, phrase)
				}
			}

			if i < len(query) {
				i++ // Skip closing quote
			}
		} else {
			// Regular term (no quotes)
			start := i
			for i < len(query) && query[i] != ' ' && query[i] != '"' {
				i++
			}

			if i > start {
				term := query[start:i]
				if isNegative {
					parsed.NegativeTerms = append(parsed.NegativeTerms, term)
				} else {
					parsed.PositiveTerms = append(parsed.PositiveTerms, term)
				}
			}
		}
	}

	return parsed
}

// BuildAtlasSearchQuery builds an Atlas Search query from parsed components
// Returns the appropriate $search structure based on query complexity
func BuildAtlasSearchQuery(parsed ParsedQuery, indexName string, searchPaths []string) bson.D {
	hasPositive := len(parsed.PositiveTerms) > 0 || len(parsed.PositivePhrases) > 0
	hasNegative := len(parsed.NegativeTerms) > 0 || len(parsed.NegativePhrases) > 0

	// Case 1: Simple single positive term (most common case)
	if len(parsed.PositiveTerms) == 1 && !hasNegative && len(parsed.PositivePhrases) == 0 {
		return bson.D{
			{Key: "index", Value: indexName},
			{Key: "text", Value: bson.D{
				{Key: "query", Value: parsed.PositiveTerms[0]},
				{Key: "path", Value: searchPaths},
			}},
		}
	}

	// Case 2: Simple single positive phrase
	if len(parsed.PositivePhrases) == 1 && !hasNegative && len(parsed.PositiveTerms) == 0 {
		return bson.D{
			{Key: "index", Value: indexName},
			{Key: "phrase", Value: bson.D{
				{Key: "query", Value: parsed.PositivePhrases[0]},
				{Key: "path", Value: searchPaths},
			}},
		}
	}

	// Case 3: Only negative terms/phrases (must use compound with wildcard or match all)
	if !hasPositive && hasNegative {
		mustNot := buildMustNotClauses(parsed, searchPaths)
		return bson.D{
			{Key: "index", Value: indexName},
			{Key: "compound", Value: bson.D{
				{Key: "mustNot", Value: mustNot},
			}},
		}
	}

	// Case 4: Complex query with must and/or mustNot
	compound := bson.D{}

	// Build must clauses
	if hasPositive {
		must := buildMustClauses(parsed, searchPaths)
		compound = append(compound, bson.E{Key: "must", Value: must})
	}

	// Build mustNot clauses
	if hasNegative {
		mustNot := buildMustNotClauses(parsed, searchPaths)
		compound = append(compound, bson.E{Key: "mustNot", Value: mustNot})
	}

	return bson.D{
		{Key: "index", Value: indexName},
		{Key: "compound", Value: compound},
	}
}

// buildMustClauses builds the 'must' array for compound queries
func buildMustClauses(parsed ParsedQuery, searchPaths []string) bson.A {
	must := bson.A{}

	// Add positive terms
	for _, term := range parsed.PositiveTerms {
		must = append(must, bson.D{
			{Key: "text", Value: bson.D{
				{Key: "query", Value: term},
				{Key: "path", Value: searchPaths},
			}},
		})
	}

	// Add positive phrases
	for _, phrase := range parsed.PositivePhrases {
		must = append(must, bson.D{
			{Key: "phrase", Value: bson.D{
				{Key: "query", Value: phrase},
				{Key: "path", Value: searchPaths},
			}},
		})
	}

	return must
}

// buildMustNotClauses builds the 'mustNot' array for compound queries
func buildMustNotClauses(parsed ParsedQuery, searchPaths []string) bson.A {
	mustNot := bson.A{}

	// Add negative terms
	for _, term := range parsed.NegativeTerms {
		mustNot = append(mustNot, bson.D{
			{Key: "text", Value: bson.D{
				{Key: "query", Value: term},
				{Key: "path", Value: searchPaths},
			}},
		})
	}

	// Add negative phrases
	for _, phrase := range parsed.NegativePhrases {
		mustNot = append(mustNot, bson.D{
			{Key: "phrase", Value: bson.D{
				{Key: "query", Value: phrase},
				{Key: "path", Value: searchPaths},
			}},
		})
	}

	return mustNot
}
