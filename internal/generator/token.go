package generator

import (
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
	"strings"
)

const (
	// SearchTokenMaxRareness is the maximum rareness value for a token
	SearchTokenMaxRareness = 16
	// SearchTokensPerRareness is the number of unique tokens per rareness level
	SearchTokensPerRareness = 200
	// SearchTokensPerField is the total tokens per field (~50 KiB of text)
	// SearchTokensPerField = 6400
	SearchTokensPerField = 6400
	// KeySize is the size of the document ID in bytes
	KeySize = 6
)

var (
	// DatabaseSearchFields are the field names for text search
	DatabaseSearchFields = []string{"text1", "text2", "text3"}
)

// GetCollectionName returns the collection name for a given textShards value.
// This matches the Java implementation: DatabaseSearchWords{textShards}
func GetCollectionName(textShards int) string {
	return fmt.Sprintf("DatabaseSearchWords%d", textShards)
}

// CountTrailingZeros counts the number of trailing zero bits in a byte slice.
// This is used to determine document rareness level.
func CountTrailingZeros(data []byte) int {
	count := 0
	for i := len(data) - 1; i >= 0; i-- {
		b := data[i]
		if b == 0 {
			count += 8
		} else {
			count += bits.TrailingZeros8(b)
			break
		}
	}
	return count
}

// MakeRandom creates a new deterministic random number generator seeded from a string ID.
func MakeRandom(id string) *rand.Rand {
	h := fnv.New64a()
	h.Write([]byte(id))
	seed := h.Sum64()
	return rand.New(rand.NewSource(int64(seed)))
}

// NextPreloadIdBytes generates deterministic ID bytes for a sampler thread.
// This ensures each concurrent sampler generates unique, deterministic IDs.
func NextPreloadIdBytes(random *rand.Rand, idSize, samplers, threadID int) []byte {
	// Round idSize up to a multiple of 4 for deterministic results
	idSize4 := (idSize + 3) &^ 3

	// Generate bytes for all samplers
	idBytes := make([]byte, idSize4*samplers)
	if _, err := random.Read(idBytes); err != nil {
		panic(fmt.Sprintf("failed to generate random bytes: %v", err))
	}

	// Extract this sampler's slice
	ourOffset := idSize4 * threadID
	ourBytes := make([]byte, idSize4)
	copy(ourBytes, idBytes[ourOffset:ourOffset+idSize4])
	return ourBytes
}

// PreloadIdBytesToId converts a byte array to a Base64URL encoded string,
// handling special cases to avoid reserved IDs.
func PreloadIdBytesToId(preloadBytes []byte) string {
	id := base64.URLEncoding.EncodeToString(preloadBytes)
	// Base64URL can generate reserved IDs, so fix it when that happens
	if strings.HasPrefix(id, "__") && strings.HasSuffix(id, "__") {
		return "@@" + id[2:]
	}
	return id
}

// AppendSearchToken generates and appends a synthetic search token.
// Token format: 3 chars from hash + 2 digits for rareness + 2 chars for index
func AppendSearchToken(builder *strings.Builder, rareness, index, searchFieldHash int) {
	hash := 1109*searchFieldHash + 113*rareness + 739*index
	appendEncodedNumber(builder, hash, 3, 'a')
	appendEncodedNumber(builder, rareness, 2, '0')
	appendEncodedNumber(builder, index, 2, 'a')
}

// appendEncodedNumber encodes a number into a string with a specific base.
func appendEncodedNumber(builder *strings.Builder, number, digits int, zero rune) {
	var base int
	switch zero {
	case 'a':
		base = 26
	case '0':
		base = 10
	default:
		panic(fmt.Sprintf("Unsupported zero: %c", zero))
	}

	// Replicate Java's Math.abs logic, including the Integer.MIN_VALUE edge case
	if number == math.MinInt {
		number = math.MaxInt
	} else if number < 0 {
		number = -number
	}

	encodedNumber := make([]rune, digits)
	for i := 0; i < digits; i++ {
		remainder := number % base
		encodedNumber[digits-1-i] = zero + rune(remainder)
		number /= base
	}
	builder.WriteString(string(encodedNumber))
}

// MakeTextSearchFieldValue generates the text content for a specific field in a document.
func MakeTextSearchFieldValue(idBytes []byte, id string, fieldName string) string {
	maxRareness := min(CountTrailingZeros(idBytes), SearchTokenMaxRareness)

	h := fnv.New32a()
	h.Write([]byte(fieldName))
	searchFieldHash := h.Sum32()

	tokenCount := 0
	var fieldValue strings.Builder
	fieldValue.Grow(SearchTokensPerField * 8)

	// Generate tokens for each rareness level
	for rareness := 0; rareness <= maxRareness; rareness++ {
		for index := 0; index < SearchTokensPerRareness; index++ {
			AppendSearchToken(&fieldValue, rareness, index, int(searchFieldHash))
			fieldValue.WriteByte(' ')
			tokenCount++
		}
	}

	// Append random tokens until we reach the desired count
	random := MakeRandom(id)
	for tokenCount < SearchTokensPerField {
		appendEncodedNumber(&fieldValue, random.Int(), 7, 'a')
		fieldValue.WriteByte(' ')
		tokenCount++
	}

	return fieldValue.String()
}

// MakeDatabaseRawTextSearchQuery generates a raw query string for text search.
func MakeDatabaseRawTextSearchQuery(
	random *rand.Rand,
	textShards,
	positiveTerms,
	negativeTerms,
	positivePhrases,
	negativePhrases,
	phraseLength int,
) string {
	rareness := random.Intn(SearchTokenMaxRareness + 1)
	var searchQuery strings.Builder

	// Generate positive terms
	if positiveTerms > 0 {
		usedIndices := make(map[int]struct{})
		for len(usedIndices) < positiveTerms {
			positiveIndex := random.Intn(SearchTokensPerRareness)
			if _, exists := usedIndices[positiveIndex]; !exists {
				usedIndices[positiveIndex] = struct{}{}
				fieldName := DatabaseSearchFields[random.Intn(textShards)]
				h := fnv.New32a()
				h.Write([]byte(fieldName))
				searchFieldHash := h.Sum32()

				AppendSearchToken(&searchQuery, rareness, positiveIndex, int(searchFieldHash))
				searchQuery.WriteByte(' ')
			}
		}
	}

	// Generate negative terms
	if negativeTerms > 0 {
		usedIndices := make(map[int]struct{})
		for len(usedIndices) < negativeTerms {
			// 676 comes from 26^2, the max index encodable in two base-26 digits
			negativeIndex := random.Intn(676-SearchTokensPerRareness) + SearchTokensPerRareness
			if _, exists := usedIndices[negativeIndex]; !exists {
				usedIndices[negativeIndex] = struct{}{}
				fieldName := DatabaseSearchFields[random.Intn(textShards)]
				h := fnv.New32a()
				h.Write([]byte(fieldName))
				searchFieldHash := h.Sum32()

				searchQuery.WriteByte('-')
				AppendSearchToken(&searchQuery, rareness, negativeIndex, int(searchFieldHash))
				searchQuery.WriteByte(' ')
			}
		}
	}

	// Generate positive phrases
	for phrase := 0; phrase < positivePhrases; phrase++ {
		fieldName := DatabaseSearchFields[random.Intn(textShards)]
		h := fnv.New32a()
		h.Write([]byte(fieldName))
		searchFieldHash := h.Sum32()

		startIndex := random.Intn(SearchTokensPerRareness - phraseLength + 1)
		searchQuery.WriteByte('"')
		for offset := 0; offset < phraseLength; offset++ {
			if offset > 0 {
				searchQuery.WriteByte(' ')
			}
			AppendSearchToken(&searchQuery, rareness, startIndex+offset, int(searchFieldHash))
		}
		searchQuery.WriteString("\" ")
	}

	// Generate negative phrases
	for phrase := 0; phrase < negativePhrases; phrase++ {
		fieldName := DatabaseSearchFields[random.Intn(textShards)]
		h := fnv.New32a()
		h.Write([]byte(fieldName))
		searchFieldHash := h.Sum32()

		searchQuery.WriteString("-\"")
		if phraseLength == 1 {
			nonExistentIndex := random.Intn(676-SearchTokensPerRareness) + SearchTokensPerRareness
			AppendSearchToken(&searchQuery, rareness, nonExistentIndex, int(searchFieldHash))
		} else {
			index := random.Intn(SearchTokensPerRareness - phraseLength + 1)
			for count := phraseLength - 1; count >= 0; count-- {
				if count == 0 {
					index++ // Skip one index on the last token to guarantee a miss
				}
				AppendSearchToken(&searchQuery, rareness, index, int(searchFieldHash))
				if count > 0 {
					searchQuery.WriteByte(' ')
				}
				index++
			}
		}
		searchQuery.WriteString("\" ")
	}

	return strings.TrimSpace(searchQuery.String())
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
