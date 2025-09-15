package generator

import (
	"strings"
	"testing"
	"time"
)

func TestNewDataGenerator(t *testing.T) {
	gen := NewDataGenerator(12345)
	if gen == nil {
		t.Fatal("Expected generator to be created")
	}

	if gen.randomSeed != 12345 {
		t.Errorf("Expected seed 12345, got %d", gen.randomSeed)
	}

	// Test with zero seed (should use current time)
	gen2 := NewDataGenerator(0)
	if gen2.randomSeed == 0 {
		t.Error("Expected non-zero seed when passing 0")
	}
}

func TestGenerateDocument(t *testing.T) {
	gen := NewDataGenerator(12345)
	doc := gen.GenerateDocument()

	// Check that required fields are populated
	if doc.Title == "" {
		t.Error("Expected title to be generated")
	}

	if doc.Content == "" {
		t.Error("Expected content to be generated")
	}

	if len(doc.Tags) == 0 {
		t.Error("Expected tags to be generated")
	}

	if doc.SearchTerms == "" {
		t.Error("Expected search terms to be generated")
	}

	if doc.CreatedAt.IsZero() {
		t.Error("Expected created_at to be set")
	}

	// Check that content contains reasonable text
	if len(doc.Content) < 50 {
		t.Errorf("Expected content to be substantial, got %d characters", len(doc.Content))
	}

	// Check that tags are reasonable
	if len(doc.Tags) < 2 || len(doc.Tags) > 6 {
		t.Errorf("Expected 2-6 tags, got %d", len(doc.Tags))
	}

	// Check search terms
	searchWords := strings.Fields(doc.SearchTerms)
	if len(searchWords) < 5 || len(searchWords) > 14 {
		t.Errorf("Expected 5-14 search terms, got %d", len(searchWords))
	}
}

func TestGenerateDocuments(t *testing.T) {
	gen := NewDataGenerator(12345)
	docs := gen.GenerateDocuments(10)

	if len(docs) != 10 {
		t.Errorf("Expected 10 documents, got %d", len(docs))
	}

	// Check that documents are different
	titles := make(map[string]bool)
	for _, doc := range docs {
		if titles[doc.Title] {
			t.Error("Found duplicate title - documents should be unique")
		}
		titles[doc.Title] = true
	}
}

func TestGenerateTitle(t *testing.T) {
	gen := NewDataGenerator(12345)

	// Generate multiple titles to check variety
	titles := make(map[string]bool)
	for i := 0; i < 50; i++ {
		title := gen.generateTitle()
		if title == "" {
			t.Error("Expected non-empty title")
		}
		titles[title] = true
	}

	// Should have some variety (at least 20 different titles out of 50)
	if len(titles) < 20 {
		t.Errorf("Expected more variety in titles, got %d unique titles", len(titles))
	}

	// Check title format (should contain a space)
	title := gen.generateTitle()
	if !strings.Contains(title, " ") {
		t.Errorf("Expected title to contain a space, got '%s'", title)
	}
}

func TestGenerateContent(t *testing.T) {
	gen := NewDataGenerator(12345)
	content := gen.generateContent()

	if content == "" {
		t.Error("Expected non-empty content")
	}

	// Content should contain placeholder replacements
	if strings.Contains(content, "{TOPIC}") ||
		strings.Contains(content, "{ACTION}") ||
		strings.Contains(content, "{TECH}") ||
		strings.Contains(content, "{ADJECTIVE}") {
		t.Error("Content should not contain unreplaced placeholders")
	}

	// Content should end with a period (from generated sentences)
	if !strings.HasSuffix(content, ".") {
		t.Error("Expected content to end with a period")
	}
}

func TestGenerateTags(t *testing.T) {
	gen := NewDataGenerator(12345)
	tags := gen.generateTags()

	if len(tags) < 2 || len(tags) > 6 {
		t.Errorf("Expected 2-6 tags, got %d", len(tags))
	}

	// Check that all tags are non-empty
	for i, tag := range tags {
		if tag == "" {
			t.Errorf("Tag at index %d is empty", i)
		}
	}
}

func TestGenerateSearchTerms(t *testing.T) {
	gen := NewDataGenerator(12345)
	searchTerms := gen.generateSearchTerms()

	if searchTerms == "" {
		t.Error("Expected non-empty search terms")
	}

	words := strings.Fields(searchTerms)
	if len(words) < 5 || len(words) > 14 {
		t.Errorf("Expected 5-14 search terms, got %d", len(words))
	}

	// Check that all terms are from our valid vocabulary  
	commonTerms := getCommonSearchTerms()
	
	for _, word := range words {
		found := false
		for _, common := range commonTerms {
			if word == common {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Generated term '%s' not found in common vocabulary. Generated terms: %s", word, searchTerms)
		}
	}
}

func TestDeterministicGeneration(t *testing.T) {
	// Test that same seed produces same results
	gen1 := NewDataGenerator(12345)
	gen2 := NewDataGenerator(12345)

	doc1 := gen1.GenerateDocument()
	doc2 := gen2.GenerateDocument()

	if doc1.Title != doc2.Title {
		t.Error("Expected same title with same seed")
	}

	if doc1.Content != doc2.Content {
		t.Error("Expected same content with same seed")
	}

	// Tags might be in different order due to random generation,
	// but the set should be the same length
	if len(doc1.Tags) != len(doc2.Tags) {
		t.Error("Expected same number of tags with same seed")
	}
}

func TestPerformance(t *testing.T) {
	gen := NewDataGenerator(12345)

	start := time.Now()
	docs := gen.GenerateDocuments(1000)
	duration := time.Since(start)

	if len(docs) != 1000 {
		t.Errorf("Expected 1000 documents, got %d", len(docs))
	}

	// Should be able to generate 1000 documents in reasonable time (< 1 second)
	if duration > time.Second {
		t.Errorf("Document generation too slow: %v for 1000 documents", duration)
	}

	t.Logf("Generated 1000 documents in %v", duration)
}
