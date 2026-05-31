// service/index_types.go
// Layer 3C — Metadata Indexing & Search: Core Types
//
// ─────────────────────────────────────────────────────────────────────────────
// WHY THIS FILE IS SEPARATE FROM types.go
// ─────────────────────────────────────────────────────────────────────────────
//
// types.go belongs to Layer 3B (content storage).
// These types belong exclusively to Layer 3C (indexing & search).
//
// The separation matters because:
//   - Layer 3B can exist and function with zero knowledge of search
//   - Layer 3C depends on Layer 3B (it indexes ContentMeta), not the other way
//   - If we ever replace the search engine, we only touch Layer 3C files
//
// ─────────────────────────────────────────────────────────────────────────────
// HOW THE INDEX WORKS (mental model)
// ─────────────────────────────────────────────────────────────────────────────
//
// Think of it like a book's index at the back.
// The book itself (content) is Layer 3B.
// The index at the back (keyword → page list) is Layer 3C.
//
// When you store "Suits S01E01.mp4":
//   tokenizer extracts: ["suits", "s01e01", "mp4", "video"]
//   For each token, the index stores:
//     /index/suits    → [..., {ContentID: "abc", Name: "Suits S01E01.mp4", ...}]
//     /index/s01e01   → [..., {ContentID: "abc", ...}]
//
// When you search "suits":
//   1. Tokenize "suits" → ["suits"]
//   2. Fetch /index/suits from DHT → []IndexEntry
//   3. Score and rank entries
//   4. Return SearchResult list
//
// ─────────────────────────────────────────────────────────────────────────────
// DISTRIBUTED vs LOCAL index
// ─────────────────────────────────────────────────────────────────────────────
//
// The INDEX is distributed across the DHT just like content.
// Each node holds the index shards that fall in its Kademlia keyspace.
// A query for keyword "suits" does a FIND_VALUE on IndexKey("suits"),
// which routes to the responsible nodes.
//
// Local index: the IndexWriter maintains a local in-memory copy of
// index entries WE published. This lets us re-announce after restart.

package service

import "time"

// ─────────────────────────────────────────────────────────────────────────────
// IndexEntry — one item in the keyword → content mapping
//
// Stored as a list under DHT key: /index/<keyword_hash>
// Multiple pieces of content can match the same keyword.
// ─────────────────────────────────────────────────────────────────────────────

type IndexEntry struct {
	ContentID       string   `json:"content_id"`
	Name            string   `json:"name"`
	Type            string   `json:"type"`
	SizeBytes       int64    `json:"size_bytes"`
	Tags            []string `json:"tags,omitempty"`
	IndexedAt       int64    `json:"indexed_at"`
	UpdatedAt       int64    `json:"updated_at"`
	MatchWeight     float64  `json:"match_weight"`
	PublisherNodeID string   `json:"publisher_node_id"`
}

// ─────────────────────────────────────────────────────────────────────────────
// IndexBucket — the full value stored at one DHT index key
//
// Key:   /index/<keyword_hash>
// Value: IndexBucket
//
// A bucket holds ALL content entries that match one keyword.
// Example: IndexBucket for "suits" might hold 15 entries.
// ─────────────────────────────────────────────────────────────────────────────

type IndexBucket struct {
	Keyword   string       `json:"keyword"`
	Entries   []IndexEntry `json:"entries"`
	UpdatedAt int64        `json:"updated_at"`
}

// ─────────────────────────────────────────────────────────────────────────────
// SearchQuery — what the caller sends to SearchIndex.Search()
// ─────────────────────────────────────────────────────────────────────────────

type SearchQuery struct {
	RawQuery     string `json:"raw_query"`
	TypeFilter   string `json:"type_filter,omitempty"`
	MaxSizeBytes int64  `json:"max_size_bytes,omitempty"`
	MaxResults   int    `json:"max_results"`
	Page         int    `json:"page"`
}

// ─────────────────────────────────────────────────────────────────────────────
// SearchResult — one item in the search response
// ─────────────────────────────────────────────────────────────────────────────

type SearchResult struct {
	ContentID     string    `json:"content_id"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	SizeBytes     int64     `json:"size_bytes"`
	Tags          []string  `json:"tags,omitempty"`
	Score         float64   `json:"score"`
	MatchedTokens []string  `json:"matched_tokens"`
	IndexedAt     time.Time `json:"indexed_at"`
}

// ─────────────────────────────────────────────────────────────────────────────
// SearchResponse — the full response from a search query
// ─────────────────────────────────────────────────────────────────────────────

type SearchResponse struct {
	Query      string         `json:"query"`
	Tokens     []string       `json:"tokens"`
	Results    []SearchResult `json:"results"`
	TotalFound int            `json:"total_found"`
	Page       int            `json:"page"`
	TookMs     int64          `json:"took_ms"`
	FromCache  bool           `json:"from_cache"`
}

// ─────────────────────────────────────────────────────────────────────────────
// IndexStats — for observability / debugging
// ─────────────────────────────────────────────────────────────────────────────

type IndexStats struct {
	TotalKeywords int   `json:"total_keywords"`
	TotalEntries  int   `json:"total_entries"`
	LocallyOwned  int   `json:"locally_owned"`
	LastWriteAt   int64 `json:"last_write_at"`
	LastSearchAt  int64 `json:"last_search_at"`
}
