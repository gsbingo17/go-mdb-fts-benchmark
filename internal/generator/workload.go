package generator

import (
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
	// Generate compound terms for fair comparison with field queries
	return wg.generateCompoundTerm()
}

// GenerateFieldValue generates a compound term for field queries
// FAIR BENCHMARKING: Same compound term generation as text search
func (wg *WorkloadGenerator) GenerateFieldValue() string {
	return wg.generateCompoundTerm()
}

// generateCompoundTerm creates compound terms by combining two base terms with underscore
// This provides fair comparison between text search and field queries with same string lengths
func (wg *WorkloadGenerator) generateCompoundTerm() string {
	baseTerms := getBaseTerms()

	term1 := baseTerms[wg.rng.Intn(len(baseTerms))]
	term2 := baseTerms[wg.rng.Intn(len(baseTerms))]

	// Avoid self-combinations for better diversity
	for term1 == term2 {
		term2 = baseTerms[wg.rng.Intn(len(baseTerms))]
	}

	return term1 + "_" + term2
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

// getBaseTerms returns base vocabulary for compound term generation
func getBaseTerms() []string {
	return []string{
		// Core Technology Terms
		"database", "mongodb", "documentdb", "search", "index", "query", "performance",
		"optimization", "scaling", "cloud", "aws", "atlas", "aggregation", "pipeline",
		"sharding", "replication", "consistency", "availability", "durability", "backup",
		"restore", "migration", "cluster", "node", "replica", "primary", "secondary",
		"failover", "recovery", "partition", "shard", "collection", "document", "field",
		"schema", "validation", "constraint", "relationship", "foreign", "security",

		// Development & Architecture
		"api", "rest", "graphql", "microservices", "architecture", "design", "patterns",
		"framework", "library", "tool", "testing", "deployment", "monitoring", "logging",
		"metrics", "observability", "tracing", "debugging", "profiling", "analytics",
		"service", "endpoint", "middleware", "gateway", "proxy", "load", "balancer",
		"cache", "session", "authentication", "authorization", "encryption", "protocol",

		// Infrastructure & Cloud
		"kubernetes", "docker", "container", "orchestration", "helm", "namespace",
		"infrastructure", "terraform", "ansible", "jenkins", "cicd", "devops",
		"automation", "provisioning", "configuration", "management", "network",
		"storage", "compute", "memory", "cpu", "bandwidth", "throughput", "latency",

		// Programming & Development
		"javascript", "typescript", "python", "java", "golang", "rust", "nodejs",
		"react", "vue", "angular", "spring", "django", "flask", "express", "async",
		"thread", "process", "concurrent", "parallel", "distributed", "event",

		// Business & Analytics
		"analytics", "intelligence", "insights", "reporting", "dashboard", "visualization",
		"data", "information", "strategy", "planning", "execution", "operations",
		"business", "enterprise", "customer", "user", "product", "feature",

		// Quality & Performance
		"testing", "validation", "verification", "quality", "assurance", "reliability",
		"scalability", "efficiency", "speed", "benchmarking", "tuning", "profiling",
		"bottleneck", "capacity", "measurement", "alerting", "notification", "incident",

		// Technologies & Tools
		"mysql", "postgresql", "redis", "elasticsearch", "cassandra", "dynamodb",
		"prometheus", "grafana", "kibana", "nginx", "apache", "kafka", "rabbitmq",
		"blockchain", "kubernetes", "serverless", "lambda", "microfunction",

		// Methodologies & Practices
		"agile", "scrum", "kanban", "waterfall", "sre", "engineering", "continuous",
		"integration", "delivery", "immutable", "declarative", "imperative", "reactive",

		// Emerging Technologies
		"machine", "learning", "artificial", "intelligence", "neural", "network",
		"deep", "algorithm", "model", "training", "inference", "blockchain",
		"cryptocurrency", "smart", "contract", "iot", "edge", "quantum",
	}
}

// getSearchTerms returns common search terms for workload generation
// CACHE REDUCTION: Expanded from ~54 to 500+ terms to minimize cache hits
func getSearchTerms() []string {
	return []string{
		// Technology Database Terms (Core)
		"database", "mongodb", "documentdb", "search", "index", "query", "performance",
		"optimization", "scaling", "cloud", "aws", "atlas", "aggregation", "pipeline",
		"sharding", "replication", "consistency", "availability", "durability", "backup",
		"restore", "migration", "cluster", "node", "replica", "primary", "secondary",
		"failover", "recovery", "partition", "shard", "collection", "document", "field",
		"schema", "validation", "constraint", "relationship", "foreign", "primary",

		// Development & Architecture Terms
		"api", "rest", "graphql", "microservices", "architecture", "design", "patterns",
		"framework", "library", "tool", "testing", "deployment", "monitoring", "logging",
		"metrics", "observability", "tracing", "debugging", "profiling", "analytics",
		"service", "endpoint", "middleware", "gateway", "proxy", "load", "balancer",
		"cache", "session", "authentication", "authorization", "security", "encryption",
		"protocol", "http", "https", "tcp", "udp", "websocket", "grpc", "json", "xml",

		// Cloud & Infrastructure Terms
		"kubernetes", "docker", "container", "orchestration", "helm", "namespace",
		"pod", "service", "ingress", "configmap", "secret", "volume", "persistent",
		"stateful", "stateless", "horizontal", "vertical", "autoscaling", "resource",
		"cpu", "memory", "storage", "network", "bandwidth", "throughput", "latency",
		"infrastructure", "terraform", "ansible", "jenkins", "pipeline", "cicd",
		"devops", "automation", "provisioning", "configuration", "management",

		// Programming Languages & Technologies
		"javascript", "typescript", "python", "java", "golang", "rust", "csharp",
		"cpp", "scala", "kotlin", "swift", "ruby", "php", "nodejs", "react", "angular",
		"vue", "spring", "django", "flask", "express", "nestjs", "fastapi", "gin",
		"fiber", "echo", "actix", "tokio", "async", "await", "promise", "coroutine",
		"thread", "process", "concurrent", "parallel", "distributed", "event",

		// Business & Analytics Terms
		"analytics", "intelligence", "insights", "reporting", "dashboard", "visualization",
		"data", "information", "knowledge", "decision", "strategy", "planning", "execution",
		"management", "operations", "business", "enterprise", "customer", "user", "client",
		"product", "feature", "requirement", "specification", "documentation", "training",
		"onboarding", "support", "maintenance", "upgrade", "version", "release", "deployment",
		"rollback", "hotfix", "patch", "bug", "issue", "ticket", "workflow", "process",

		// Performance & Quality Terms
		"latency", "throughput", "capacity", "scalability", "efficiency", "speed", "fast",
		"slow", "bottleneck", "optimization", "tuning", "profiling", "benchmarking",
		"testing", "validation", "verification", "quality", "assurance", "reliability",
		"availability", "durability", "consistency", "integrity", "accuracy", "precision",
		"recall", "performance", "benchmark", "metric", "measurement", "monitoring",
		"alerting", "notification", "incident", "response", "recovery", "resilience",

		// Machine Learning & AI Terms
		"machine", "learning", "artificial", "intelligence", "model", "training", "inference",
		"neural", "network", "deep", "algorithm", "feature", "classification", "regression",
		"clustering", "recommendation", "prediction", "forecasting", "optimization", "gradient",
		"descent", "backpropagation", "tensorflow", "pytorch", "sklearn", "pandas", "numpy",
		"matplotlib", "jupyter", "notebook", "dataset", "preprocessing", "normalization",
		"encoding", "embedding", "vector", "similarity", "distance", "cosine", "euclidean",

		// Security & Compliance Terms
		"security", "vulnerability", "threat", "risk", "compliance", "audit", "governance",
		"privacy", "gdpr", "hipaa", "sox", "pci", "encryption", "decryption", "hashing",
		"salt", "token", "jwt", "oauth", "saml", "ldap", "active", "directory", "firewall",
		"intrusion", "detection", "prevention", "antivirus", "malware", "phishing", "social",
		"engineering", "penetration", "testing", "vulnerability", "assessment", "scanning",

		// Networking & Communication Terms
		"network", "protocol", "tcp", "udp", "http", "https", "ssl", "tls", "dns", "dhcp",
		"ip", "ipv4", "ipv6", "subnet", "vlan", "vpn", "firewall", "router", "switch",
		"gateway", "proxy", "load", "balancer", "cdn", "edge", "region", "zone", "datacenter",
		"bandwidth", "latency", "packet", "frame", "header", "payload", "compression",
		"encryption", "certificate", "authority", "public", "private", "key", "signature",

		// Storage & Database Technologies
		"storage", "filesystem", "block", "object", "blob", "bucket", "volume", "snapshot",
		"backup", "archive", "lifecycle", "retention", "deletion", "purge", "cleanup",
		"mysql", "postgresql", "oracle", "sqlserver", "sqlite", "redis", "memcached",
		"elasticsearch", "solr", "cassandra", "dynamodb", "hbase", "neo4j", "graph",
		"timeseries", "influxdb", "prometheus", "grafana", "kibana", "logstash", "beats",

		// Development Methodologies & Practices
		"agile", "scrum", "kanban", "waterfall", "devops", "devsecops", "sre", "reliability",
		"engineering", "continuous", "integration", "delivery", "deployment", "testing",
		"automation", "infrastructure", "code", "configuration", "immutable", "declarative",
		"imperative", "idempotent", "stateful", "stateless", "reactive", "proactive",
		"monitoring", "observability", "telemetry", "metrics", "logs", "traces", "events",

		// User Experience & Interface Terms
		"user", "experience", "interface", "design", "usability", "accessibility", "responsive",
		"mobile", "desktop", "tablet", "touch", "gesture", "voice", "chatbot", "virtual",
		"assistant", "recommendation", "personalization", "customization", "preference",
		"profile", "dashboard", "widget", "component", "layout", "template", "theme",
		"branding", "typography", "color", "contrast", "readability", "navigation", "menu",

		// Industry & Domain Terms
		"fintech", "healthtech", "edtech", "retailtech", "logistics", "supply", "chain",
		"manufacturing", "automotive", "aerospace", "telecommunications", "media", "entertainment",
		"gaming", "social", "ecommerce", "marketplace", "payment", "billing", "subscription",
		"saas", "paas", "iaas", "platform", "ecosystem", "marketplace", "vendor", "partner",
		"integration", "interoperability", "standard", "specification", "compliance", "certification",

		// Emerging Technologies
		"blockchain", "cryptocurrency", "bitcoin", "ethereum", "smart", "contract", "nft",
		"metaverse", "virtual", "reality", "augmented", "reality", "mixed", "reality",
		"iot", "internet", "things", "sensor", "device", "embedded", "firmware", "edge",
		"computing", "fog", "computing", "serverless", "lambda", "function", "microfunction",
		"quantum", "computing", "5g", "6g", "satellite", "wireless", "cellular", "wifi",

		// Operations & Maintenance Terms
		"operations", "maintenance", "support", "helpdesk", "ticketing", "escalation",
		"sla", "slo", "sli", "uptime", "downtime", "outage", "incident", "postmortem",
		"root", "cause", "analysis", "troubleshooting", "diagnosis", "resolution", "workaround",
		"patch", "update", "upgrade", "migration", "rollback", "rollforward", "canary",
		"blue", "green", "deployment", "feature", "flag", "toggle", "experiment", "ab", "testing",

		// Miscellaneous Technical Terms
		"algorithm", "datastructure", "complexity", "efficiency", "optimization", "refactoring",
		"legacy", "technical", "debt", "documentation", "comment", "annotation", "metadata",
		"tag", "label", "category", "taxonomy", "ontology", "semantic", "syntax", "parser",
		"compiler", "interpreter", "runtime", "virtual", "machine", "garbage", "collection",
		"memory", "management", "allocation", "deallocation", "leak", "fragmentation", "compression",
	}
}

// getQueryTypes returns different types of queries with their characteristics
func getQueryTypes() []QueryType {
	return []QueryType{
		{
			Name:       "single_term",
			Template:   "{TERM}",
			Complexity: 1,
			Weight:     1.0, // 100% of queries - single term only
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
