// service/index_reader.go
// Layer 3C — Metadata Indexing & Search: Index Reader
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT THE INDEX READER DOES
// ─────────────────────────────────────────────────────────────────────────────
//
// The reader answers the question: "what content matches this query?"
//
// Full search flow:
//
//   1. Receive SearchQuery from user/application
//   2. Tokenize the raw query → ["suits", "season", "1"]
//   3. For each token, check local index cache first
//   4. For DHT tokens, issue FIND_VALUE via InnerCore (caller handles this)
//   5. Collect all IndexEntry results across all tokens
//   6. Score each result using multiple signals
//   7. Merge duplicates (same ContentID appearing under multiple tokens)
//   8. Apply filters (type, size)
//   9. Sort by score descending
//   10. Paginate and return SearchResponse
//
// ─────────────────────────────────────────────────────────────────────────────
// SCORING PHILOSOPHY
// ─────────────────────────────────────────────────────────────────────────────
//
// We do NOT use PageRank or neural embeddings — that's overkill here.
// We use a simple additive scoring model with clear signals:
//
//   Signal                          Weight    Reason
//   ─────────────────────────────────────────────────
//   Token match weight              0.40      Filename match > tag match > keyword match
//   Token coverage (%)              0.30      "suits season" matches more than just "suits"
//   Recency (newer = better)        0.20      Fresh content is more likely available
//   File type match                 0.10      Exact type filter match bonus
//
// This gives deterministic, explainable rankings.
// Users can understand why result A ranked above result B.
//
// ─────────────────────────────────────────────────────────────────────────────
// LOCAL CACHE
// ─────────────────────────────────────────────────────────────────────────────
//
// The reader maintains a local cache of IndexBuckets fetched from the DHT.
// Cache TTL matches IndexEntryTTL (30 min) so stale entries don't persist.
// Cache is invalidated when IndexWriter.Index() writes a new entry locally.
//
// This dramatically reduces DHT traffic for popular keywords.
// "suits" might be queried 100 times per hour — only 1 DHT lookup needed.

package service

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Scoring weights — change these to tune ranking behavior
// ─────────────────────────────────────────────────────────────────────────────

const (
	weightTokenMatchStrength = 0.40 // how strong is the individual token match
	weightTokenCoverage      = 0.30 // what fraction of query tokens matched
	weightRecency            = 0.20 // how recently was this content indexed
	weightTypeMatch          = 0.10 // bonus for exact type filter match
)

// Default result count when SearchQuery.MaxResults == 0
const defaultMaxResults = 20

// Cache entry TTL — how long we keep a fetched IndexBucket in memory
const cacheEntryTTL = 30 * time.Minute

// ─────────────────────────────────────────────────────────────────────────────
// CachedBucket — local cache entry for a fetched IndexBucket
// ─────────────────────────────────────────────────────────────────────────────

type CachedBucket struct {
	Bucket    IndexBucket
	FetchedAt time.Time
}

func (c *CachedBucket) isExpired() bool {
	return time.Since(c.FetchedAt) > cacheEntryTTL
}

// ─────────────────────────────────────────────────────────────────────────────
// IndexReader — the main struct
// ─────────────────────────────────────────────────────────────────────────────

type IndexReader struct {
	tokenizer *Tokenizer

	// Local cache: normalized keyword → CachedBucket
	cache map[string]*CachedBucket
	mu    sync.RWMutex

	// Stats tracking
	stats IndexStats
}

// NewIndexReader creates an IndexReader
func NewIndexReader() *IndexReader {
	return &IndexReader{
		tokenizer: NewTokenizer(),
		cache:     make(map[string]*CachedBucket),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SearchLocal — search only the local cache (no DHT I/O)
//
// Use this when you want instant results from what's already been fetched.
// Returns an empty response if the relevant buckets aren't cached yet.
//
// Typical usage:
//   // First try local
//   resp := reader.SearchLocal(query)
//   if resp.TotalFound > 0 {
//       return resp
//   }
//   // Then trigger DHT lookup and call FeedBucket() with results
// ─────────────────────────────────────────────────────────────────────────────

func (r *IndexReader) SearchLocal(query SearchQuery) SearchResponse {
	start := time.Now()

	tokens := r.tokenizer.TokenizeQuery(query.RawQuery)
	if len(tokens) == 0 {
		return SearchResponse{
			Query:  query.RawQuery,
			Tokens: tokens,
		}
	}

	// Collect entries from cache for each token
	// map: contentID → accumulated score context
	type accumulator struct {
		entry         IndexEntry
		matchedTokens []string
		totalWeight   float64
		tokensCovered int
	}

	merged := make(map[string]*accumulator)

	r.mu.RLock()
	for _, token := range tokens {
		cached, ok := r.cache[token]
		if !ok || cached.isExpired() {
			continue
		}

		for _, entry := range cached.Bucket.Entries {
			acc, exists := merged[entry.ContentID]
			if !exists {
				acc = &accumulator{entry: entry}
				merged[entry.ContentID] = acc
			}
			acc.matchedTokens = append(acc.matchedTokens, token)
			acc.totalWeight += entry.MatchWeight
			acc.tokensCovered++
		}
	}
	r.mu.RUnlock()

	// Score and filter results
	maxResults := query.MaxResults
	if maxResults == 0 {
		maxResults = defaultMaxResults
	}

	var results []SearchResult
	now := time.Now().Unix()

	for _, acc := range merged {
		// Apply type filter
		if query.TypeFilter != "" && acc.entry.Type != query.TypeFilter {
			continue
		}

		// Apply size filter
		if query.MaxSizeBytes > 0 && acc.entry.SizeBytes > query.MaxSizeBytes {
			continue
		}

		score := r.computeScore(acc.entry, acc.matchedTokens, len(tokens), acc.totalWeight, now, query.TypeFilter)

		results = append(results, SearchResult{
			ContentID:     acc.entry.ContentID,
			Name:          acc.entry.Name,
			Type:          acc.entry.Type,
			SizeBytes:     acc.entry.SizeBytes,
			Tags:          acc.entry.Tags,
			Score:         score,
			MatchedTokens: acc.matchedTokens,
			IndexedAt:     time.Unix(acc.entry.IndexedAt, 0),
		})
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	total := len(results)

	// Paginate
	page := query.Page
	start_idx := page * maxResults
	if start_idx >= len(results) {
		results = nil
	} else {
		end := start_idx + maxResults
		if end > len(results) {
			end = len(results)
		}
		results = results[start_idx:end]
	}

	r.mu.Lock()
	r.stats.LastSearchAt = time.Now().Unix()
	r.mu.Unlock()

	return SearchResponse{
		Query:      query.RawQuery,
		Tokens:     tokens,
		Results:    results,
		TotalFound: total,
		Page:       page,
		TookMs:     time.Since(start).Milliseconds(),
		FromCache:  true,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// GetDHTKeysForQuery — returns the DHT keys the caller needs to fetch
//
// The reader does NOT do DHT I/O. The caller:
//   1. Calls GetDHTKeysForQuery() to find out what to fetch
//   2. Does the DHT FIND_VALUE calls via InnerCore
//   3. Feeds results back with FeedBucket()
//   4. Calls SearchLocal() to get scored results
//
// This separation keeps the reader testable without a running network.
// ─────────────────────────────────────────────────────────────────────────────

type DHTLookupRequest struct {
	Keyword string // human-readable (for logging)
	DHTKey  []byte // the actual key to look up in the DHT
}

func (r *IndexReader) GetDHTKeysForQuery(query SearchQuery) []DHTLookupRequest {
	tokens := r.tokenizer.TokenizeQuery(query.RawQuery)
	if len(tokens) == 0 {
		return nil
	}

	var requests []DHTLookupRequest

	r.mu.RLock()
	for _, token := range tokens {
		// Skip tokens we already have fresh in cache
		cached, ok := r.cache[token]
		if ok && !cached.isExpired() {
			continue
		}

		key := IndexKey(token)
		requests = append(requests, DHTLookupRequest{
			Keyword: token,
			DHTKey:  key[:],
		})
	}
	r.mu.RUnlock()

	return requests
}

// ─────────────────────────────────────────────────────────────────────────────
// FeedBucket — feeds a DHT-fetched IndexBucket into the local cache
//
// Call this after a successful DHT FIND_VALUE for an index key.
// Once fed, SearchLocal() will include these entries in future searches.
// ─────────────────────────────────────────────────────────────────────────────

func (r *IndexReader) FeedBucket(keyword string, bucket IndexBucket) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache[keyword] = &CachedBucket{
		Bucket:    bucket,
		FetchedAt: time.Now(),
	}

	fmt.Printf("[INDEX READER] Cached %d entries for keyword '%s'\n", len(bucket.Entries), keyword)
}

// FeedEntry feeds a single IndexEntry into the local cache for a keyword
// Use this when you receive individual entries rather than full buckets
func (r *IndexReader) FeedEntry(keyword string, entry IndexEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cached, ok := r.cache[keyword]
	if !ok || cached.isExpired() {
		// Create new bucket for this keyword
		r.cache[keyword] = &CachedBucket{
			Bucket: IndexBucket{
				Keyword:   keyword,
				Entries:   []IndexEntry{entry},
				UpdatedAt: time.Now().Unix(),
			},
			FetchedAt: time.Now(),
		}
		return
	}

	// Check if this content is already in the bucket — update if so
	for i, e := range cached.Bucket.Entries {
		if e.ContentID == entry.ContentID {
			// Update with newer entry (higher weight wins)
			if entry.MatchWeight >= e.MatchWeight {
				cached.Bucket.Entries[i] = entry
			}
			return
		}
	}

	// Append new entry
	cached.Bucket.Entries = append(cached.Bucket.Entries, entry)
	cached.Bucket.UpdatedAt = time.Now().Unix()
}

// ─────────────────────────────────────────────────────────────────────────────
// InvalidateContent — remove all cached entries for a specific ContentID
//
// Call this when content is removed from the network.
// ─────────────────────────────────────────────────────────────────────────────

func (r *IndexReader) InvalidateContent(contentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for keyword, cached := range r.cache {
		original := len(cached.Bucket.Entries)
		filtered := cached.Bucket.Entries[:0]
		for _, e := range cached.Bucket.Entries {
			if e.ContentID != contentID {
				filtered = append(filtered, e)
			}
		}
		cached.Bucket.Entries = filtered
		removed += original - len(filtered)

		// Remove the entire bucket if empty
		if len(cached.Bucket.Entries) == 0 {
			delete(r.cache, keyword)
		}
	}

	if removed > 0 {
		fmt.Printf("[INDEX READER] Invalidated %d cache entries for content %s...\n",
			removed, contentID[:16])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// computeScore — the ranking function
//
// Takes all signals and produces a final 0.0 → 1.0 score.
// Higher score = better match = shown first.
// ─────────────────────────────────────────────────────────────────────────────

func (r *IndexReader) computeScore(
	entry IndexEntry,
	matchedTokens []string,
	totalQueryTokens int,
	totalMatchWeight float64,
	nowUnix int64,
	typeFilter string,
) float64 {
	if totalQueryTokens == 0 {
		return 0
	}

	// Signal 1: Average token match strength
	// How strong were the individual token matches?
	// A filename match (weight 1.0) is stronger than a keyword match (weight 0.5)
	avgWeight := totalMatchWeight / float64(len(matchedTokens))
	scoreMatchStrength := avgWeight * weightTokenMatchStrength

	// Signal 2: Token coverage
	// What fraction of the query tokens actually matched?
	// "suits season 1" → matched "suits" and "season" but not "1" → 2/3 coverage
	coverage := float64(len(matchedTokens)) / float64(totalQueryTokens)
	scoreCoverage := coverage * weightTokenCoverage

	// Signal 3: Recency
	// Newer content is more likely to be available and relevant.
	// We use a decay function: content indexed today scores 1.0, 30 days ago scores ~0.0
	// Formula: max(0, 1 - age_in_days / 30)
	ageSeconds := nowUnix - entry.IndexedAt
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	ageDays := float64(ageSeconds) / 86400.0
	recencyScore := 1.0 - (ageDays / 30.0)
	if recencyScore < 0 {
		recencyScore = 0
	}
	scoreRecency := recencyScore * weightRecency

	// Signal 4: Type match bonus
	// If the user filtered by type and this entry matches, give a small bonus
	scoreType := 0.0
	if typeFilter != "" && entry.Type == typeFilter {
		scoreType = weightTypeMatch
	}

	total := scoreMatchStrength + scoreCoverage + scoreRecency + scoreType

	// Clamp to [0.0, 1.0] — floating point can exceed 1.0 in edge cases
	if total > 1.0 {
		total = 1.0
	}
	if total < 0.0 {
		total = 0.0
	}

	return total
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats and diagnostics
// ─────────────────────────────────────────────────────────────────────────────

// GetStats returns current reader statistics
func (r *IndexReader) GetStats() IndexStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	totalEntries := 0
	for _, cached := range r.cache {
		totalEntries += len(cached.Bucket.Entries)
	}

	return IndexStats{
		TotalKeywords: len(r.cache),
		TotalEntries:  totalEntries,
		LastSearchAt:  r.stats.LastSearchAt,
	}
}

// GetCachedKeywords returns all keywords currently in the local cache
// Useful for debugging what this node knows about
func (r *IndexReader) GetCachedKeywords() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keywords := make([]string, 0, len(r.cache))
	for kw, cached := range r.cache {
		if !cached.isExpired() {
			keywords = append(keywords, kw)
		}
	}
	sort.Strings(keywords)
	return keywords
}

// PurgeExpiredCache evicts all expired cache entries
// Call this from a maintenance loop to prevent unbounded memory growth
func (r *IndexReader) PurgeExpiredCache() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	for kw, cached := range r.cache {
		if cached.isExpired() {
			delete(r.cache, kw)
			count++
		}
	}

	if count > 0 {
		fmt.Printf("[INDEX READER] Purged %d expired cache entries\n", count)
	}

	return count
}
