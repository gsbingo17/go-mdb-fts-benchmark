package database

import (
	"fmt"
	"strings"
)

// SpannerQueryBuilder builds Spanner SQL SEARCH queries from MongoDB-style query strings
type SpannerQueryBuilder struct{}

// NewSpannerQueryBuilder creates a new query builder
func NewSpannerQueryBuilder() *SpannerQueryBuilder {
	return &SpannerQueryBuilder{}
}

// BuildSearchQuery converts a MongoDB text search query to Spanner SEARCH syntax
// MongoDB query format: "word1 word2 -excluded \"exact phrase\""
// Spanner SEARCH format: SEARCH(tokens, 'word1 AND word2 -excluded AND "exact phrase"')
// operator: "AND" or "OR" for positive terms/phrases (default: "AND")
func (b *SpannerQueryBuilder) BuildSearchQuery(query string, operator string) string {
	if query == "" {
		return ""
	}

	// Normalize operator (default to AND if not specified or empty)
	if operator == "" {
		operator = "AND"
	}

	var positiveConditions []string
	var negativeConditions []string
	inQuote := false
	currentToken := strings.Builder{}
	isNegation := false

	// Parse the query string
	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		switch r {
		case '"':
			if inQuote {
				// End of phrase
				phrase := currentToken.String()
				if phrase != "" {
					if isNegation {
						negativeConditions = append(negativeConditions, fmt.Sprintf(`-"%s"`, phrase))
					} else {
						positiveConditions = append(positiveConditions, fmt.Sprintf(`"%s"`, phrase))
					}
				}
				currentToken.Reset()
				isNegation = false
				inQuote = false
			} else {
				// Start of phrase
				inQuote = true
			}

		case '-':
			if !inQuote && currentToken.Len() == 0 {
				// This is a negation operator
				isNegation = true
			} else {
				currentToken.WriteRune(r)
			}

		case ' ', '\t', '\n':
			if inQuote {
				currentToken.WriteRune(r)
			} else if currentToken.Len() > 0 {
				// End of token
				token := currentToken.String()
				if isNegation {
					negativeConditions = append(negativeConditions, fmt.Sprintf("-%s", token))
				} else {
					positiveConditions = append(positiveConditions, token)
				}
				currentToken.Reset()
				isNegation = false
			}

		default:
			currentToken.WriteRune(r)
		}
	}

	// Handle remaining token
	if currentToken.Len() > 0 {
		token := currentToken.String()
		if isNegation {
			negativeConditions = append(negativeConditions, fmt.Sprintf("-%s", token))
		} else {
			positiveConditions = append(positiveConditions, token)
		}
	}

	// Build final query
	var parts []string

	// Join positive conditions with the specified operator
	if len(positiveConditions) > 0 {
		if len(positiveConditions) == 1 {
			parts = append(parts, positiveConditions[0])
		} else {
			// Multiple positive conditions - use operator
			positiveQuery := strings.Join(positiveConditions, " "+operator+" ")
			if operator == "OR" && len(negativeConditions) > 0 {
				// Wrap OR conditions in parentheses when combined with NOT
				positiveQuery = "(" + positiveQuery + ")"
			}
			parts = append(parts, positiveQuery)
		}
	}

	// Negative conditions always use AND
	if len(negativeConditions) > 0 {
		parts = append(parts, strings.Join(negativeConditions, " AND "))
	}

	if len(parts) == 0 {
		return ""
	}

	// Join all parts with AND
	return strings.Join(parts, " AND ")
}

// BuildSearchQueryForFields builds a SEARCH query for specific tokenized fields
// For uniform indexing, all tables search across text1, text2, text3
func (b *SpannerQueryBuilder) BuildSearchQueryForFields(query string, fields []string, operator string) string {
	searchQuery := b.BuildSearchQuery(query, operator)
	if searchQuery == "" {
		return ""
	}

	// Build conditions for each field
	var fieldConditions []string
	for _, field := range fields {
		tokenField := field + "_tokens"
		condition := fmt.Sprintf("SEARCH(%s, '%s')", tokenField, searchQuery)
		fieldConditions = append(fieldConditions, condition)
	}

	// Join with OR (match in any field)
	return strings.Join(fieldConditions, " OR ")
}

// GetDefaultSearchFields returns the default fields for full-text search
func (b *SpannerQueryBuilder) GetDefaultSearchFields() []string {
	return []string{"text1", "text2", "text3"}
}
