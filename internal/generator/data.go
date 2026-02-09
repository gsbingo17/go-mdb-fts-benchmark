package generator

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"mongodb-benchmarking-tool/internal/database"
)

// DataGenerator generates synthetic documents for benchmarking
type DataGenerator struct {
	wordLists  [][]string
	templates  []string
	randomSeed int64
	rng        *rand.Rand
}

// NewDataGenerator creates a new data generator
func NewDataGenerator(seed int64) *DataGenerator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &DataGenerator{
		wordLists:  getWordLists(),
		templates:  getContentTemplates(),
		randomSeed: seed,
		rng:        rand.New(rand.NewSource(seed)),
	}
}

// GetRng returns the internal random number generator for external use
// This is used for token-based ID generation in geospatial benchmarks
func (dg *DataGenerator) GetRng() *rand.Rand {
	return dg.rng
}

// GenerateDocument creates a single synthetic document with searchable content
func (dg *DataGenerator) GenerateDocument() database.Document {
	return database.Document{
		ID:          "", // Let MongoDB generate the ID
		Title:       dg.generateTitle(),
		Content:     dg.generateContent(),
		Tags:        dg.generateTags(),
		CreatedAt:   time.Now(),
		SearchTerms: dg.generateSearchTerms(),
	}
}

// GenerateTokenDocument creates a token-based document for cost modeling
func (dg *DataGenerator) GenerateTokenDocument(textShards, samplers, threadID int) database.Document {
	// Generate deterministic ID
	idBytes := NextPreloadIdBytes(dg.rng, KeySize, samplers, threadID)
	id := PreloadIdBytesToId(idBytes)

	doc := database.Document{
		ID:        id,
		CreatedAt: time.Now(),
	}

	// Generate token-based fields
	for i := 0; i < textShards && i < len(DatabaseSearchFields); i++ {
		fieldName := DatabaseSearchFields[i]
		fieldValue := MakeTextSearchFieldValue(idBytes, id, fieldName)

		switch i {
		case 0:
			doc.Text1 = fieldValue
		case 1:
			doc.Text2 = fieldValue
		case 2:
			doc.Text3 = fieldValue
		}
	}

	return doc
}

// GenerateDocuments creates multiple documents in batch
func (dg *DataGenerator) GenerateDocuments(count int) []database.Document {
	documents := make([]database.Document, count)
	for i := 0; i < count; i++ {
		documents[i] = dg.generateDocument()
	}
	return documents
}

// generateTitle creates a realistic document title
func (dg *DataGenerator) generateTitle() string {
	prefixes := []string{
		"Understanding", "Introduction to", "Advanced", "Complete Guide to",
		"Best Practices for", "Modern", "Efficient", "Optimizing",
		"Building", "Implementing", "Designing", "Scaling",
	}

	subjects := []string{
		"Database Performance", "Text Search", "Cloud Computing", "Data Analytics",
		"Machine Learning", "Web Development", "API Design", "Security",
		"Microservices", "DevOps", "System Architecture", "User Experience",
	}

	prefix := prefixes[dg.rng.Intn(len(prefixes))]
	subject := subjects[dg.rng.Intn(len(subjects))]

	return fmt.Sprintf("%s %s", prefix, subject)
}

// generateContent creates realistic document content with frequency-based searchable terms
func (dg *DataGenerator) generateContent() string {
	template := dg.templates[dg.rng.Intn(len(dg.templates))]

	// Replace placeholders with searchable terms from common vocabulary
	searchTerms := getCommonSearchTerms()
	content := template
	content = strings.ReplaceAll(content, "{TOPIC}", searchTerms[dg.rng.Intn(len(searchTerms))])
	content = strings.ReplaceAll(content, "{ACTION}", dg.getRandomWord("actions"))
	content = strings.ReplaceAll(content, "{TECH}", searchTerms[dg.rng.Intn(len(searchTerms))])
	content = strings.ReplaceAll(content, "{ADJECTIVE}", dg.getRandomWord("adjectives"))

	// Add frequency-based terms throughout the content
	content += " " + dg.generateContentWithFrequencyBasedTerms()

	// Add random sentences with embedded search terms for more content
	extraSentences := dg.rng.Intn(3) + 1 // 1-3 additional sentences
	for i := 0; i < extraSentences; i++ {
		content += " " + dg.generateSentenceWithSearchTerms()
	}

	return content
}

// generateContentWithFrequencyBasedTerms creates content using frequency-based term distribution
func (dg *DataGenerator) generateContentWithFrequencyBasedTerms() string {
	termFreqs := getTermFrequencyDistribution()
	selectedTerms := make([]string, 0, 10)

	// Select terms based on their frequency probabilities
	for _, tf := range termFreqs {
		if dg.rng.Float64() < tf.Frequency {
			selectedTerms = append(selectedTerms, tf.Term)
		}
	}

	// If no terms selected (very unlikely), add a few high-frequency terms
	if len(selectedTerms) == 0 {
		selectedTerms = []string{"database", "performance", "optimization"}
	}

	// Generate natural sentences using selected terms
	if len(selectedTerms) == 1 {
		return fmt.Sprintf("This solution focuses on %s capabilities.", selectedTerms[0])
	} else if len(selectedTerms) == 2 {
		return fmt.Sprintf("The system integrates %s and %s technologies effectively.", selectedTerms[0], selectedTerms[1])
	} else if len(selectedTerms) >= 3 {
		return fmt.Sprintf("Key components include %s, %s, and %s integration.", selectedTerms[0], selectedTerms[1], selectedTerms[2])
	}

	return ""
}

// generateTags creates relevant tags for the document
func (dg *DataGenerator) generateTags() []string {
	tagCount := dg.rng.Intn(5) + 2 // 2-6 tags
	tags := make([]string, tagCount)

	allTags := []string{
		"database", "mongodb", "documentdb", "search", "performance",
		"cloud", "aws", "atlas", "indexing", "optimization",
		"api", "javascript", "python", "golang", "nodejs",
		"react", "vue", "angular", "docker", "kubernetes",
	}

	for i := 0; i < tagCount; i++ {
		tags[i] = allTags[dg.rng.Intn(len(allTags))]
	}

	return tags
}

// generateSearchTerms creates terms optimized for text search
// Uses the same term pool as query generation to ensure search hits
func (dg *DataGenerator) generateSearchTerms() string {
	termCount := dg.rng.Intn(10) + 5 // 5-14 terms
	terms := make([]string, termCount)

	// Use the same search terms as the workload generator to ensure matches
	searchTerms := getCommonSearchTerms()

	for i := 0; i < termCount; i++ {
		terms[i] = searchTerms[dg.rng.Intn(len(searchTerms))]
	}

	return strings.Join(terms, " ")
}

// generateSentence creates a random sentence
func (dg *DataGenerator) generateSentence() string {
	starters := []string{
		"This approach", "The system", "Our solution", "The implementation",
		"This method", "The framework", "The architecture", "This strategy",
	}

	actions := []string{
		"provides", "enables", "supports", "implements", "delivers",
		"ensures", "guarantees", "optimizes", "enhances", "improves",
	}

	objects := []string{
		"high performance", "better scalability", "improved efficiency",
		"enhanced reliability", "optimal throughput", "reduced latency",
		"better user experience", "cost effectiveness", "data consistency",
	}

	starter := starters[dg.rng.Intn(len(starters))]
	action := actions[dg.rng.Intn(len(actions))]
	object := objects[dg.rng.Intn(len(objects))]

	return fmt.Sprintf("%s %s %s.", starter, action, object)
}

// generateSentenceWithSearchTerms creates a sentence that includes searchable terms
func (dg *DataGenerator) generateSentenceWithSearchTerms() string {
	searchTerms := getCommonSearchTerms()

	starters := []string{
		"Modern", "Advanced", "Efficient", "Scalable", "Robust",
		"Optimized", "Enterprise", "Cloud-based", "High-performance",
	}

	templates := []string{
		"When implementing %s solutions, developers need to consider %s and %s aspects.",
		"The %s architecture requires careful %s planning and %s optimization.",
		"Effective %s management involves %s monitoring and %s analysis.",
		"This %s approach delivers superior %s performance and %s reliability.",
		"Advanced %s techniques enable better %s scalability and %s efficiency.",
	}

	starter := starters[dg.rng.Intn(len(starters))]
	template := templates[dg.rng.Intn(len(templates))]

	// Fill template with search terms
	term1 := searchTerms[dg.rng.Intn(len(searchTerms))]
	term2 := searchTerms[dg.rng.Intn(len(searchTerms))]
	term3 := searchTerms[dg.rng.Intn(len(searchTerms))]

	sentence := fmt.Sprintf(template, term1, term2, term3)
	return starter + " " + sentence
}

// generateDocument is an internal helper that calls the public method
func (dg *DataGenerator) generateDocument() database.Document {
	return dg.GenerateDocument()
}

// getRandomWord gets a random word from a specific category
func (dg *DataGenerator) getRandomWord(category string) string {
	for _, wordList := range dg.wordLists {
		if len(wordList) > 0 && strings.Contains(wordList[0], category) {
			return wordList[dg.rng.Intn(len(wordList))]
		}
	}
	return "sample"
}

// getWordLists returns categorized word lists for content generation
func getWordLists() [][]string {
	return [][]string{
		// Topics
		{"database", "search", "performance", "cloud", "api", "security", "analytics"},
		// Actions
		{"optimize", "implement", "design", "build", "scale", "deploy", "monitor"},
		// Technology
		{"mongodb", "documentdb", "elasticsearch", "redis", "postgresql", "kubernetes"},
		// Adjectives
		{"efficient", "scalable", "reliable", "fast", "secure", "modern", "robust"},
	}
}

// getContentTemplates returns templates for document content
func getContentTemplates() []string {
	return []string{
		"In modern {TECH} development, it's crucial to {ACTION} {ADJECTIVE} solutions that can handle {TOPIC} effectively. This requires careful consideration of various factors including performance, scalability, and maintainability.",

		"When working with {TOPIC}, developers often need to {ACTION} systems that are both {ADJECTIVE} and efficient. The {TECH} ecosystem provides various tools and frameworks to achieve this goal.",

		"Building {ADJECTIVE} applications requires a deep understanding of {TOPIC} and how to {ACTION} appropriate solutions. Modern {TECH} platforms offer sophisticated capabilities for this purpose.",

		"The challenge of {TOPIC} in {TECH} environments requires developers to {ACTION} {ADJECTIVE} architectures that can scale effectively while maintaining high performance standards.",

		"Effective {TOPIC} management is essential for any {ADJECTIVE} {TECH} solution. Teams must {ACTION} robust systems that can handle varying loads and requirements.",
	}
}

// TermFrequency represents a term with its occurrence probability
type TermFrequency struct {
	Term      string
	Frequency float64 // Probability of appearing in a document (0.0 - 1.0)
}

// getTermFrequencyDistribution returns terms with their frequency distribution
// This creates realistic term distribution where some terms are common, others rare
func getTermFrequencyDistribution() []TermFrequency {
	return []TermFrequency{
		// High frequency terms (appear in 15-25% of documents) - Core database concepts
		{"database", 0.22}, {"performance", 0.20}, {"mongodb", 0.18}, {"data", 0.16},
		{"query", 0.15}, {"search", 0.15}, {"index", 0.14}, {"optimization", 0.13},
		{"application", 0.12}, {"system", 0.12},

		// Medium-high frequency terms (appear in 8-12% of documents) - Common technologies
		{"api", 0.11}, {"cloud", 0.10}, {"atlas", 0.10}, {"scaling", 0.09},
		{"architecture", 0.09}, {"framework", 0.08}, {"development", 0.08}, {"solution", 0.08},

		// Medium frequency terms (appear in 5-8% of documents) - Specific concepts
		{"aggregation", 0.07}, {"pipeline", 0.07}, {"microservices", 0.06}, {"analytics", 0.06},
		{"monitoring", 0.06}, {"deployment", 0.06}, {"testing", 0.05}, {"security", 0.05},
		{"latency", 0.05}, {"throughput", 0.05},

		// Low-medium frequency terms (appear in 3-5% of documents) - Technical details
		{"sharding", 0.04}, {"replication", 0.04}, {"consistency", 0.04}, {"availability", 0.04},
		{"durability", 0.04}, {"scalability", 0.04}, {"efficiency", 0.03}, {"reliability", 0.03},
		{"integration", 0.03}, {"configuration", 0.03},

		// Low frequency terms (appear in 1-3% of documents) - Advanced/specific
		{"kubernetes", 0.025}, {"docker", 0.025}, {"redis", 0.025}, {"elasticsearch", 0.025},
		{"prometheus", 0.02}, {"grafana", 0.02}, {"terraform", 0.02}, {"ansible", 0.02},
		{"jenkins", 0.02}, {"github", 0.02},

		// Technology stack terms
		{"javascript", 0.035}, {"python", 0.035}, {"golang", 0.03}, {"nodejs", 0.03},
		{"react", 0.025}, {"vue", 0.02}, {"angular", 0.02}, {"express", 0.02},

		// Web development terms
		{"rest", 0.04}, {"graphql", 0.03}, {"http", 0.035}, {"json", 0.04},
		{"xml", 0.015}, {"yaml", 0.02}, {"authentication", 0.025}, {"authorization", 0.02},

		// DevOps and deployment
		{"cicd", 0.015}, {"pipeline", 0.03}, {"automation", 0.025}, {"container", 0.03},
		{"orchestration", 0.015}, {"deployment", 0.04}, {"staging", 0.02}, {"production", 0.03},

		// Business and analytics terms
		{"intelligence", 0.025}, {"insights", 0.03}, {"reporting", 0.04}, {"dashboard", 0.035},
		{"visualization", 0.025}, {"metrics", 0.045}, {"kpi", 0.02}, {"analytics", 0.04},

		// Performance and optimization
		{"benchmark", 0.02}, {"profiling", 0.015}, {"tuning", 0.02}, {"bottleneck", 0.015},
		{"caching", 0.03}, {"compression", 0.015}, {"indexing", 0.025}, {"partitioning", 0.02},

		// Quality and testing
		{"testing", 0.04}, {"validation", 0.025}, {"verification", 0.015}, {"quality", 0.03},
		{"coverage", 0.02}, {"debugging", 0.025}, {"logging", 0.035}, {"tracing", 0.02},

		// Management and process
		{"strategy", 0.025}, {"planning", 0.03}, {"execution", 0.025}, {"management", 0.035},
		{"operations", 0.04}, {"maintenance", 0.025}, {"support", 0.03}, {"documentation", 0.03},

		// Very low frequency terms (appear in 0.5-1% of documents) - Specialized
		{"etcd", 0.008}, {"consul", 0.008}, {"vault", 0.01}, {"istio", 0.008},
		{"envoy", 0.006}, {"jaeger", 0.008}, {"zipkin", 0.006}, {"fluentd", 0.008},
		{"elk", 0.01}, {"kafka", 0.012}, {"rabbitmq", 0.01}, {"activemq", 0.006},
		{"nginx", 0.015}, {"apache", 0.012}, {"haproxy", 0.008}, {"traefik", 0.006},
	}
}

// getCommonSearchTerms returns the shared vocabulary used by both document generation and query generation
// This ensures search queries will find matching content in documents
func getCommonSearchTerms() []string {
	termFreqs := getTermFrequencyDistribution()
	terms := make([]string, len(termFreqs))

	for i, tf := range termFreqs {
		terms[i] = tf.Term
	}

	return terms
}
