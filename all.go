// service/content_store.go
// Layer 3B — Hybrid Content Store
//
// This is the main storage engine for the content fabric.
// It handles two cases cleanly:
//
//   Small files (≤ ChunkThreshold):
//     → stored as a single object
//     → ContentMeta.IsChunked = false
//     → single DHT STORE call
//
//   Large files (> ChunkThreshold):
//     → split into fixed-size chunks
//     → each chunk stored independently under /chunk/<ChunkHash>
//     → chunk manifest stored under /chunk-manifest/<ContentID>
//     → ContentMeta stored under /content/<ContentID>
//
// WHY CHUNKING MATTERS:
// Kademlia STORE operations carry the value in UDP packets.
// A 500MB file cannot fit in a UDP packet.
// Even with TCP fallback, storing a huge blob on a single node is fragile.
// Chunks spread the load, enable parallel download, and allow partial retrieval.
//
// This is the same reason BitTorrent uses pieces and IPFS uses blocks.
//
// IMPORTANT: ContentStore does NOT do network I/O itself.
// It prepares storage records and chunk structures.
// The actual DHT STORE calls go through InnerCore's RPC layer.
// This keeps the storage logic testable without a running network.

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

// ChunkThreshold — files larger than this are automatically chunked
// 1MB is a good default: small enough to fit in memory easily,
// large enough to avoid excessive overhead on medium files.
const ChunkThreshold = 1 * 1024 * 1024 // 1 MB

// DefaultChunkSize — how big each chunk is for large files
// 256KB is a good balance: not too many chunks, not too large per packet
const DefaultChunkSize = 256 * 1024 // 256 KB

// DefaultReplicationFactor — how many nodes should hold each piece of content
const DefaultReplicationFactor = 3

// ─────────────────────────────────────────────────────────────────────────────
// ContentStore — the main storage engine
// ─────────────────────────────────────────────────────────────────────────────

type ContentStore struct {
	// In-memory index of content we own or have cached locally
	// In a future step, this gets persisted to disk via Layer 5 StorageEngine
	localIndex map[string]*ContentMeta // contentID → meta
	localData  map[string][]byte       // contentID or chunkHash → raw bytes
	mu         sync.RWMutex

	ownerID types.NodeID
}

// NewContentStore creates a new store for a node
func NewContentStore(ownerID types.NodeID) *ContentStore {
	return &ContentStore{
		localIndex: make(map[string]*ContentMeta),
		localData:  make(map[string][]byte),
		ownerID:    ownerID,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// StoreResult — what Store() returns to the caller
// The caller (integration layer) uses this to issue the actual DHT STORE RPCs
// ─────────────────────────────────────────────────────────────────────────────

type StoreResult struct {
	Meta     *ContentMeta   // always populated
	Manifest *ChunkManifest // only populated for chunked files
	Chunks   []ChunkData    // raw chunk bytes for DHT distribution
}

// ChunkData pairs a chunk's hash (DHT key) with its raw bytes (DHT value)
type ChunkData struct {
	Hash string
	Data []byte
}

// ─────────────────────────────────────────────────────────────────────────────
// Store — the main entry point
// Takes raw file bytes, returns everything needed for DHT distribution
// ─────────────────────────────────────────────────────────────────────────────

// Store prepares a file for distributed storage.
// It does NOT do network I/O — it returns StoreResult which the caller
// uses to issue the actual DHT STORE operations via InnerCore.
func (cs *ContentStore) Store(
	name string,
	fileType string,
	data []byte,
	tags []string,
	keywords []string,
) (*StoreResult, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot store empty file")
	}

	now := time.Now().Unix()

	if len(data) <= ChunkThreshold {
		return cs.storeSmall(name, fileType, data, tags, keywords, now)
	}
	return cs.storeLarge(name, fileType, data, tags, keywords, now)
}

// ─────────────────────────────────────────────────────────────────────────────
// Small file path — one object, one DHT entry
// ─────────────────────────────────────────────────────────────────────────────

func (cs *ContentStore) storeSmall(
	name, fileType string,
	data []byte,
	tags, keywords []string,
	now int64,
) (*StoreResult, error) {
	contentID := ContentIDFromBytes(data)

	meta := &ContentMeta{
		ContentID:         contentID,
		Name:              name,
		Type:              fileType,
		OwnerNodeID:       cs.ownerID,
		CreatedAt:         now,
		UpdatedAt:         now,
		SizeBytes:         int64(len(data)),
		IsChunked:         false,
		ChunkCount:        0,
		Hash:              contentID, // for small files, ContentID == Hash
		HashAlgo:          "sha256",
		Version:           1,
		Tags:              tags,
		Keywords:          keywords,
		ReplicationFactor: DefaultReplicationFactor,
	}

	// Cache locally
	cs.mu.Lock()
	cs.localIndex[contentID] = meta
	cs.localData[contentID] = data
	cs.mu.Unlock()

	fmt.Printf("[CONTENT STORE] Stored small file '%s' → ContentID: %s (%d bytes)\n",
		name, contentID[:16]+"...", len(data))

	return &StoreResult{
		Meta:   meta,
		Chunks: []ChunkData{{Hash: contentID, Data: data}},
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Large file path — chunked storage
// ─────────────────────────────────────────────────────────────────────────────

func (cs *ContentStore) storeLarge(
	name, fileType string,
	data []byte,
	tags, keywords []string,
	now int64,
) (*StoreResult, error) {
	// Step 1: Split into chunks
	chunks, chunkMetas := splitIntoChunks(data, DefaultChunkSize)

	// Step 2: Derive ContentID from chunk hashes (Merkle-lite)
	chunkHashes := make([]string, len(chunkMetas))
	for i, cm := range chunkMetas {
		chunkHashes[i] = cm.Hash
	}
	contentID := ContentIDFromChunks(chunkHashes)
	manifestID := contentID // manifest is keyed by the same ContentID

	// Step 3: Build ContentMeta (lean — no chunk list embedded)
	meta := &ContentMeta{
		ContentID:         contentID,
		Name:              name,
		Type:              fileType,
		OwnerNodeID:       cs.ownerID,
		CreatedAt:         now,
		UpdatedAt:         now,
		SizeBytes:         int64(len(data)),
		IsChunked:         true,
		ChunkCount:        len(chunks),
		ChunkSize:         DefaultChunkSize,
		Hash:              contentID,
		HashAlgo:          "sha256",
		ChunkManifestID:   manifestID,
		Version:           1,
		Tags:              tags,
		Keywords:          keywords,
		ReplicationFactor: DefaultReplicationFactor,
	}

	// Step 4: Build ChunkManifest (stored separately in DHT)
	manifest := &ChunkManifest{
		ContentID: contentID,
		Chunks:    chunkMetas,
	}

	// Step 5: Cache locally
	cs.mu.Lock()
	cs.localIndex[contentID] = meta
	for i, chunk := range chunks {
		cs.localData[chunkMetas[i].Hash] = chunk
	}
	cs.mu.Unlock()

	// Step 6: Build ChunkData list for DHT distribution
	chunkDataList := make([]ChunkData, len(chunks))
	for i, chunk := range chunks {
		chunkDataList[i] = ChunkData{
			Hash: chunkMetas[i].Hash,
			Data: chunk,
		}
	}

	fmt.Printf("[CONTENT STORE] Stored large file '%s' → ContentID: %s (%d bytes, %d chunks)\n",
		name, contentID[:16]+"...", len(data), len(chunks))

	return &StoreResult{
		Meta:     meta,
		Manifest: manifest,
		Chunks:   chunkDataList,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Retrieval — local cache lookup
// ─────────────────────────────────────────────────────────────────────────────

// GetMeta returns locally cached metadata for a content ID
// Returns nil if not cached (caller must do DHT lookup)
func (cs *ContentStore) GetMeta(contentID string) *ContentMeta {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.localIndex[contentID]
}

// GetChunk returns locally cached chunk bytes
// Returns nil if not cached (caller must do DHT lookup)
func (cs *ContentStore) GetChunk(chunkHash string) []byte {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.localData[chunkHash]
}

// HasContent returns true if this node has the full file locally
func (cs *ContentStore) HasContent(contentID string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	meta, hasMeta := cs.localIndex[contentID]
	if !hasMeta {
		return false
	}
	if !meta.IsChunked {
		_, hasData := cs.localData[contentID]
		return hasData
	}
	// For chunked files, check if we have a manifest and all chunks
	// (simplified — full version would track which chunks we hold)
	return false
}

// ListLocalContent returns all content IDs this node has locally
func (cs *ContentStore) ListLocalContent() []*ContentMeta {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make([]*ContentMeta, 0, len(cs.localIndex))
	for _, meta := range cs.localIndex {
		result = append(result, meta)
	}
	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: split raw bytes into fixed-size chunks
// ─────────────────────────────────────────────────────────────────────────────

func splitIntoChunks(data []byte, chunkSize int64) ([][]byte, []ChunkMeta) {
	var chunks [][]byte
	var metas []ChunkMeta

	total := int64(len(data))
	index := 0

	for offset := int64(0); offset < total; offset += chunkSize {
		end := offset + chunkSize
		if end > total {
			end = total
		}
		chunk := data[offset:end]
		hash := ChunkHashFromBytes(chunk)

		chunks = append(chunks, chunk)
		metas = append(metas, ChunkMeta{
			Index:     index,
			Hash:      hash,
			SizeBytes: int64(len(chunk)),
		})
		index++
	}

	return chunks, metas
}
// service/dht.go
// Layer 3B — DHT Integration for Content Fabric
//
// This bridges ContentStore (Layer 3B) with InnerCore (Layer 1.5).
// It handles replication to multiple nodes using Kademlia STORE.

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/innercore"
	"innercore-network/types"
)

const (
	ReplicationK = 5
	StoreTimeout = 8 * time.Second
)

type DHTPublisher struct {
	innerCore *innercore.InnerCore
	mu        sync.RWMutex
}

var (
	dhtPublisher  *DHTPublisher
	publisherOnce sync.Once
)

func InitDHTPublisher(ic *innercore.InnerCore) {
	publisherOnce.Do(func() {
		dhtPublisher = &DHTPublisher{innerCore: ic}
		fmt.Println("[NEXUSFABRIC] DHT Publisher initialized")
	})
}

func GetDHTPublisher() *DHTPublisher {
	return dhtPublisher
}

// publishContentToDHT — internal implementation (renamed to avoid conflict)
func publishContentToDHT(result *StoreResult) error {
	if dhtPublisher == nil {
		return fmt.Errorf("DHT publisher not initialized")
	}
	return dhtPublisher.publish(result)
}

func (p *DHTPublisher) publish(result *StoreResult) error {
	if result == nil || result.Meta == nil {
		return fmt.Errorf("invalid store result")
	}

	fmt.Printf("[NEXUSFABRIC] Publishing ContentID: %s (chunked: %v)\n",
		result.Meta.ContentID[:16]+"...", result.Meta.IsChunked)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// 1. Store ContentMeta
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.storeWithReplication(ContentKey(result.Meta.ContentID), result.Meta); err != nil {
			errChan <- fmt.Errorf("meta store failed: %v", err)
		}
	}()

	// 2. Store ChunkManifest (if chunked)
	if result.Meta.IsChunked && result.Manifest != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.storeWithReplication(ChunkManifestKey(result.Meta.ContentID), result.Manifest); err != nil {
				errChan <- fmt.Errorf("manifest store failed: %v", err)
			}
		}()
	}

	// 3. Store all chunks
	for _, chunk := range result.Chunks {
		wg.Add(1)
		go func(c ChunkData) {
			defer wg.Done()
			if err := p.storeWithReplication(ChunkKey(c.Hash), c.Data); err != nil {
				errChan <- fmt.Errorf("chunk %s store failed: %v", c.Hash[:12]+"...", err)
			}
		}(chunk)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		fmt.Printf("[NEXUSFABRIC] WARNING: %v\n", err)
	}

	fmt.Printf("[NEXUSFABRIC] Published %s successfully\n", result.Meta.Name)
	return nil
}

func (p *DHTPublisher) storeWithReplication(key types.NodeID, value any) error {
	peers, err := p.innerCore.Lookup(key)
	if err != nil || len(peers) == 0 {
		return fmt.Errorf("no peers found for replication")
	}

	targets := peers
	if len(targets) > ReplicationK {
		targets = targets[:ReplicationK]
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var successCount int

	for _, peer := range targets {
		// Skip self
		if peer.NodeID.Equal(p.innerCore.GetMyNodeID()) {
			continue
		}

		wg.Add(1)
		go func(target types.NodeID) {
			defer wg.Done()

			if err := p.innerCore.SendStore(target, key[:], value); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(peer.NodeID)
	}

	wg.Wait()

	if successCount == 0 {
		return fmt.Errorf("failed to store to any replicas")
	}

	fmt.Printf("[DHT] Stored key %s to %d/%d replicas\n", key, successCount, len(targets))
	return nil
}
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
// service/index_writer.go
// Layer 3C — Metadata Indexing & Search: Index Writer
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT THE INDEX WRITER DOES
// ─────────────────────────────────────────────────────────────────────────────
//
// When a user publishes a file via Layer 3B (ContentStore.Store()), the writer:
//
//   1. Takes the ContentMeta returned by Store()
//   2. Runs it through the Tokenizer to extract all searchable tokens
//   3. For each token, builds an IndexEntry pointing to this content
//   4. Prepares DHT write records (one per token) for the caller to send
//
// The writer does NOT perform DHT I/O itself.
// Same principle as ContentStore — pure logic, no network.
// The caller (integration layer) takes the WriteRecords and issues
// the actual DHT STORE operations through InnerCore.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHY WE TRACK LOCALLY
// ─────────────────────────────────────────────────────────────────────────────
//
// The DHT is eventually consistent and entries expire.
// If a node restarts, it needs to re-announce its index entries.
// The local index (localEntries) is the source of truth for what THIS node
// has indexed. On restart, we iterate localEntries and re-issue DHT STOREs.
//
// This is exactly what BitTorrent trackers do with "re-announce intervals."
//
// ─────────────────────────────────────────────────────────────────────────────
// INDEX ENTRY EXPIRY
// ─────────────────────────────────────────────────────────────────────────────
//
// DHT values have a TTL. If a node goes offline and never re-announces,
// its index entries eventually expire and disappear from search results.
// This is a feature, not a bug — stale content disappears automatically.
// Nodes that stay online keep re-announcing and remain discoverable.

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

// IndexEntryTTL — how long an index entry lives in the DHT without re-announcement
// 30 minutes matches the Layer 3A provider TTL for consistency
const IndexEntryTTL = 30 * time.Minute

// ReAnnounceInterval — how often we re-push our index entries to the DHT
// Must be shorter than IndexEntryTTL so entries don't expire between announces
const ReAnnounceInterval = 20 * time.Minute

// ─────────────────────────────────────────────────────────────────────────────
// IndexWriteRecord — what the writer hands to the caller for DHT distribution
//
// The caller takes this and does:
//   innerCore.SendStore(DHTKey, Bucket)
//
// One record per keyword. A file with 12 tokens generates 12 write records.
// ─────────────────────────────────────────────────────────────────────────────

type IndexWriteRecord struct {
	// The DHT key — result of IndexKey(keyword)
	// The caller passes this to InnerCore's STORE RPC
	DHTKey types.NodeID `json:"dht_key"`

	// The human-readable keyword (for logging/debugging)
	Keyword string `json:"keyword"`

	// The index entry to add at this DHT key
	// In a full implementation, the caller would first FIND_VALUE the existing
	// bucket, append this entry, then STORE the updated bucket.
	// For now, the entry is stored directly.
	Entry IndexEntry `json:"entry"`

	// The complete bucket key string (for local cache lookup)
	BucketKey string `json:"bucket_key"`
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalIndexRecord — what we store in memory for re-announcement
// ─────────────────────────────────────────────────────────────────────────────

type LocalIndexRecord struct {
	ContentID    string
	Keywords     []string // all keywords we indexed this content under
	IndexedAt    time.Time
	LastAnnounce time.Time
}

// ─────────────────────────────────────────────────────────────────────────────
// IndexWriter — the main struct
// ─────────────────────────────────────────────────────────────────────────────

type IndexWriter struct {
	tokenizer *Tokenizer
	ownerID   types.NodeID

	// Local tracking: contentID → record
	// Used for re-announcement after restart
	localRecords map[string]*LocalIndexRecord
	mu           sync.RWMutex

	// Stats
	stats IndexStats
}

// NewIndexWriter creates an IndexWriter for this node
func NewIndexWriter(ownerID types.NodeID) *IndexWriter {
	return &IndexWriter{
		tokenizer:    NewTokenizer(),
		ownerID:      ownerID,
		localRecords: make(map[string]*LocalIndexRecord),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Index — the main entry point
//
// Call this immediately after ContentStore.Store() succeeds.
// Returns the write records for the caller to push to the DHT.
//
// Usage:
//   storeResult, _ := contentStore.Store(name, type, data, tags, keywords)
//   writeRecords := indexWriter.Index(storeResult.Meta)
//   for _, record := range writeRecords {
//       innerCore.SendStore(record.DHTKey, record.Entry)
//   }
// ─────────────────────────────────────────────────────────────────────────────

func (w *IndexWriter) Index(meta *ContentMeta) []IndexWriteRecord {
	if meta == nil {
		return nil
	}

	now := time.Now()

	// Step 1: Extract all weighted tokens from this content
	weightedTokens := w.tokenizer.TokenizeContentMeta(meta)

	if len(weightedTokens) == 0 {
		fmt.Printf("[INDEX WRITER] Warning: no tokens extracted for '%s'\n", meta.Name)
		return nil
	}

	// Step 2: Build write records — one per unique token
	records := make([]IndexWriteRecord, 0, len(weightedTokens))
	keywords := make([]string, 0, len(weightedTokens))

	for _, wt := range weightedTokens {
		entry := IndexEntry{
			ContentID:       meta.ContentID,
			Name:            meta.Name,
			Type:            meta.Type,
			SizeBytes:       meta.SizeBytes,
			Tags:            meta.Tags,
			IndexedAt:       now.Unix(),
			UpdatedAt:       meta.UpdatedAt,
			MatchWeight:     wt.Weight,
			PublisherNodeID: meta.OwnerNodeID.String(),
		}

		record := IndexWriteRecord{
			DHTKey:    IndexKey(wt.Token),
			Keyword:   wt.Token,
			Entry:     entry,
			BucketKey: NSIndex + wt.Token,
		}

		records = append(records, record)
		keywords = append(keywords, wt.Token)
	}

	// Step 3: Track locally for re-announcement
	w.mu.Lock()
	w.localRecords[meta.ContentID] = &LocalIndexRecord{
		ContentID:    meta.ContentID,
		Keywords:     keywords,
		IndexedAt:    now,
		LastAnnounce: now,
	}
	w.stats.TotalKeywords += len(keywords)
	w.stats.TotalEntries += len(records)
	w.stats.LocallyOwned++
	w.stats.LastWriteAt = now.Unix()
	w.mu.Unlock()

	fmt.Printf("[INDEX WRITER] Indexed '%s' under %d tokens (ContentID: %s...)\n",
		meta.Name, len(records), meta.ContentID[:16])

	return records
}

// ─────────────────────────────────────────────────────────────────────────────
// Remove — remove content from the local tracking (does NOT delete from DHT)
//
// We cannot easily delete from the DHT (Kademlia has no native delete).
// Instead, we stop re-announcing and let the TTL expire the entries naturally.
// This is the standard approach in all DHT-based systems.
// ─────────────────────────────────────────────────────────────────────────────

func (w *IndexWriter) Remove(contentID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if rec, ok := w.localRecords[contentID]; ok {
		w.stats.TotalKeywords -= len(rec.Keywords)
		w.stats.TotalEntries -= len(rec.Keywords)
		w.stats.LocallyOwned--
		delete(w.localRecords, contentID)
		fmt.Printf("[INDEX WRITER] Removed '%s' from local tracking — DHT entries will expire in %v\n",
			contentID[:16]+"...", IndexEntryTTL)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// GetReAnnounceRecords — returns write records for all content that needs
// re-announcing to the DHT (called periodically by maintenance loop)
//
// Usage:
//   records := indexWriter.GetReAnnounceRecords()
//   for _, record := range records {
//       innerCore.SendStore(record.DHTKey, record.Entry)
//   }
// ─────────────────────────────────────────────────────────────────────────────

func (w *IndexWriter) GetReAnnounceRecords(contentStore *ContentStore) []IndexWriteRecord {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	var records []IndexWriteRecord

	for contentID, localRec := range w.localRecords {
		// Only re-announce if ReAnnounceInterval has passed
		if now.Sub(localRec.LastAnnounce) < ReAnnounceInterval {
			continue
		}

		// Fetch the current meta from the content store
		meta := contentStore.GetMeta(contentID)
		if meta == nil {
			// Content is gone — stop tracking
			delete(w.localRecords, contentID)
			continue
		}

		// Build write records for all keywords of this content
		for _, keyword := range localRec.Keywords {
			entry := IndexEntry{
				ContentID:       meta.ContentID,
				Name:            meta.Name,
				Type:            meta.Type,
				SizeBytes:       meta.SizeBytes,
				Tags:            meta.Tags,
				IndexedAt:       localRec.IndexedAt.Unix(),
				UpdatedAt:       meta.UpdatedAt,
				MatchWeight:     1.0, // re-announce uses full weight
				PublisherNodeID: meta.OwnerNodeID.String(),
			}

			records = append(records, IndexWriteRecord{
				DHTKey:    IndexKey(keyword),
				Keyword:   keyword,
				Entry:     entry,
				BucketKey: NSIndex + keyword,
			})
		}

		// Mark as re-announced
		localRec.LastAnnounce = now
	}

	if len(records) > 0 {
		fmt.Printf("[INDEX WRITER] Re-announcing %d index entries for %d content items\n",
			len(records), len(w.localRecords))
	}

	return records
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats — for observability
// ─────────────────────────────────────────────────────────────────────────────

func (w *IndexWriter) GetStats() IndexStats {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stats
}

// GetLocalContentIDs returns all content IDs this writer is tracking
func (w *IndexWriter) GetLocalContentIDs() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ids := make([]string, 0, len(w.localRecords))
	for id := range w.localRecords {
		ids = append(ids, id)
	}
	return ids
}

// ─────────────────────────────────────────────────────────────────────────────
// StartMaintenanceLoop — background goroutine that re-announces index entries
//
// This runs forever. Call it once at startup in a goroutine.
// The callback lets the caller do the actual DHT STORE without the writer
// needing to import InnerCore (which would create an import cycle).
//
// Usage:
//   go indexWriter.StartMaintenanceLoop(contentStore, func(records []IndexWriteRecord) {
//       for _, r := range records {
//           innerCore.SendStore(r.DHTKey, r.Entry)
//       }
//   })
// ─────────────────────────────────────────────────────────────────────────────

func (w *IndexWriter) StartMaintenanceLoop(
	contentStore *ContentStore,
	announceFunc func(records []IndexWriteRecord),
) {
	ticker := time.NewTicker(ReAnnounceInterval)
	defer ticker.Stop()

	fmt.Println("[INDEX WRITER] Maintenance loop started — re-announce interval:", ReAnnounceInterval)

	for range ticker.C {
		records := w.GetReAnnounceRecords(contentStore)
		if len(records) > 0 && announceFunc != nil {
			announceFunc(records)
		}
	}
}
// service/keygen.go
// Layer 3B — DHT Keyspace Design
//
// WHY THIS FILE EXISTS:
// Kademlia's DHT is a flat key → value store. Without namespacing, two different
// subsystems could accidentally overwrite each other's data with the same key.
//
// Example problem WITHOUT namespacing:
//   ContentID "abc123" (metadata) vs ChunkHash "abc123" (chunk bytes)
//   Same key → one overwrites the other → silent data corruption.
//
// Solution: every key is prefixed with its namespace.
//
// The 4 namespaces in this system:
//
//   /content/<ContentID>          → ContentMeta
//   /chunk-manifest/<ContentID>   → ChunkManifest (ordered chunk list)
//   /chunk/<ChunkHash>            → raw chunk bytes
//   /provider/<ContentID>         → []ContentProvider
//   /index/<keyword_hash>         → []ContentRef (search index)
//
// Keys are then hashed to 32 bytes to fit Kademlia's NodeID-sized keyspace.
// This means all lookups use the same XOR distance routing as node lookups.

package service

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"innercore-network/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Namespace prefixes
// Keep these as constants so a typo anywhere is a compile error, not a runtime bug
// ─────────────────────────────────────────────────────────────────────────────

const (
	NSContent       = "/content/"
	NSChunkManifest = "/chunk-manifest/"
	NSChunk         = "/chunk/"
	NSProvider      = "/provider/"
	NSIndex         = "/index/"
)

// ─────────────────────────────────────────────────────────────────────────────
// Key generators — call these everywhere instead of building strings by hand
// ─────────────────────────────────────────────────────────────────────────────

// ContentKey returns the DHT key for a file's metadata
// Example: ContentKey("sha256abc...") → hash of "/content/sha256abc..."
func ContentKey(contentID string) types.NodeID {
	return hashKey(NSContent + contentID)
}

// ChunkManifestKey returns the DHT key for a file's chunk list
// Example: ChunkManifestKey("sha256abc...") → hash of "/chunk-manifest/sha256abc..."
func ChunkManifestKey(contentID string) types.NodeID {
	return hashKey(NSChunkManifest + contentID)
}

// ChunkKey returns the DHT key for a specific chunk's raw bytes
// Example: ChunkKey("chunksha256...") → hash of "/chunk/chunksha256..."
func ChunkKey(chunkHash string) types.NodeID {
	return hashKey(NSChunk + chunkHash)
}

// ProviderKey returns the DHT key for the provider list of a piece of content
// Example: ProviderKey("sha256abc...") → hash of "/provider/sha256abc..."
func ProviderKey(contentID string) types.NodeID {
	return hashKey(NSProvider + contentID)
}

// IndexKey returns the DHT key for a search keyword's content reference list
// Example: IndexKey("suits") → hash of "/index/suits"
// We lowercase and trim to make search case-insensitive
func IndexKey(keyword string) types.NodeID {
	normalized := strings.ToLower(strings.TrimSpace(keyword))
	return hashKey(NSIndex + normalized)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helper — the actual hashing
// ─────────────────────────────────────────────────────────────────────────────

// hashKey hashes an arbitrary string into a 32-byte NodeID
// This is how Kademlia fits arbitrary keys into its 256-bit keyspace
func hashKey(raw string) types.NodeID {
	var id types.NodeID
	hash := sha256.Sum256([]byte(raw))
	copy(id[:], hash[:])
	return id
}

// ─────────────────────────────────────────────────────────────────────────────
// ContentID generation — how we create the canonical identifier for a file
// ─────────────────────────────────────────────────────────────────────────────

// ContentIDFromBytes generates a ContentID from raw file bytes
// For small files: SHA256 of the entire file
// For large (chunked) files: SHA256 of the concatenated chunk hashes (Merkle-lite)
func ContentIDFromBytes(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// ContentIDFromChunks generates a ContentID from an ordered list of chunk hashes
// This is the Merkle root equivalent for chunked files
// Order matters — same chunks in different order = different file = different ID
func ContentIDFromChunks(chunkHashes []string) string {
	h := sha256.New()
	for _, ch := range chunkHashes {
		h.Write([]byte(ch))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ChunkHashFromBytes generates the hash for a single chunk
func ChunkHashFromBytes(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}
// service/persistence.go
// Layer 3B — Persistence using Layer 5 StorageEngine

package service

import (
	"encoding/json"
	"fmt"

	"innercore-network/storage"
)

// PersistContent saves metadata and manifest to persistent storage
func PersistContent(result *StoreResult) error {
	if result == nil || result.Meta == nil {
		return fmt.Errorf("nothing to persist")
	}

	engine := storage.GlobalStorage.GetEngine()

	// Save ContentMeta
	metaBytes, _ := json.Marshal(result.Meta)
	_, err := engine.Put("content", result.Meta.ContentID, map[string]any{
		"meta":      string(metaBytes),
		"timestamp": result.Meta.CreatedAt,
	}, nil)
	if err != nil {
		return err
	}

	// Save manifest if chunked
	if result.Manifest != nil {
		manifestBytes, _ := json.Marshal(result.Manifest)
		engine.Put("chunk_manifests", result.Meta.ContentID, map[string]any{
			"manifest": string(manifestBytes),
		}, nil)
	}

	fmt.Printf("[PERSISTENCE] Saved %s to Layer 5\n", result.Meta.Name)
	return nil
}

// LoadContentMeta from persistent storage
func LoadContentMeta(contentID string) (*ContentMeta, error) {
	engine := storage.GlobalStorage.GetEngine()
	rec, err := engine.Get("content", contentID)
	if err != nil {
		return nil, err
	}

	var meta ContentMeta
	if payload, ok := rec.Payload["meta"].(string); ok {
		json.Unmarshal([]byte(payload), &meta)
		return &meta, nil
	}
	return nil, fmt.Errorf("invalid meta format")
}
// service/provider.go
// Layer 3B — Content Provider Announcement & Discovery

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

const ProviderAnnounceTTL = 5 * time.Minute

type ContentProviderRegistry struct {
	providers map[string][]ContentProvider // contentID → providers
	mu        sync.RWMutex
}

var (
	providerRegistry = &ContentProviderRegistry{
		providers: make(map[string][]ContentProvider),
	}
)

// AnnounceContent announces that this node has a piece of content
func AnnounceContent(contentID string, hasFull bool, chunksHeld []int) {
	if cs := GetContentStore(); cs == nil {
		return
	}

	meta := GetContentStore().GetMeta(contentID)
	if meta == nil {
		return
	}

	provider := ContentProvider{
		NodeID:       GetContentStore().ownerID,
		IP:           "127.0.0.1", // will be updated from network table
		Port:         51000,
		Capabilities: types.Capabilities{StorageAvailableMB: 6144, CanStoreChunks: true},
		AnnouncedAt:  time.Now().Unix(),
		HasFull:      hasFull,
		ChunksHeld:   chunksHeld,
	}

	providerRegistry.mu.Lock()
	providerRegistry.providers[contentID] = append(providerRegistry.providers[contentID], provider)
	providerRegistry.mu.Unlock()

	fmt.Printf("[PROVIDER] Announced ownership of %s (full: %v)\n", contentID[:16]+"...", hasFull)

	// TODO: Broadcast via InnerCore or role-based messaging
}

// GetProviders returns nodes that have this content
func GetProviders(contentID string) []ContentProvider {
	providerRegistry.mu.RLock()
	defer providerRegistry.mu.RUnlock()
	return providerRegistry.providers[contentID]
}

// Cleanup old providers
func StartProviderCleanup() {
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		providerRegistry.mu.Lock()
		now := time.Now().Unix()
		for cid, list := range providerRegistry.providers {
			var active []ContentProvider
			for _, p := range list {
				if now-p.AnnouncedAt < int64(ProviderAnnounceTTL.Seconds()) {
					active = append(active, p)
				}
			}
			if len(active) > 0 {
				providerRegistry.providers[cid] = active
			} else {
				delete(providerRegistry.providers, cid)
			}
		}
		providerRegistry.mu.Unlock()
	}
}
// service/retrieval.go
// Layer 3B — Content Retrieval (DHT + Peer-assisted)

package service

import (
	"fmt"
)

type RetrieveResult struct {
	Meta   *ContentMeta
	Data   []byte
	Chunks map[int][]byte
}

// Internal implementation to avoid package-level issues
func retrieveContentInternal(contentID string) (*RetrieveResult, error) {
	if cs := GetContentStore(); cs != nil {
		if meta := cs.GetMeta(contentID); meta != nil {
			if !meta.IsChunked {
				if data := cs.GetChunk(contentID); len(data) > 0 {
					return &RetrieveResult{Meta: meta, Data: data}, nil
				}
			}
		}
	}

	providers := GetProviders(contentID)
	if len(providers) > 0 {
		fmt.Printf("[RETRIEVAL] Found %d providers for %s\n", len(providers), contentID[:16]+"...")
	}

	publisher := GetDHTPublisher()
	if publisher == nil {
		return nil, fmt.Errorf("DHT unavailable")
	}

	key := ContentKey(contentID)
	peers, err := publisher.innerCore.Lookup(key)
	if err != nil || len(peers) == 0 {
		return nil, fmt.Errorf("content not found")
	}

	fmt.Printf("[RETRIEVAL] Querying %d peers...\n", len(peers))

	for _, peer := range peers[:3] {
		publisher.innerCore.SendFindValue(peer.NodeID, key[:])
	}

	return nil, fmt.Errorf("full retrieval MVP - content located but streaming not complete")
}
// service/search.go
// Layer 3B — Distributed Search Index

package service

import (
	"fmt"
	"strings"
)

func IndexContent(meta *ContentMeta) {
	for _, kw := range meta.Keywords {
		fmt.Printf("[SEARCH] Indexed keyword '%s' for %s\n", kw, meta.Name)
	}
}

func searchInternal(keyword string) []*ContentRef {
	if cs := GetContentStore(); cs != nil {
		var results []*ContentRef
		for _, meta := range cs.ListLocalContent() {
			if strings.Contains(strings.ToLower(meta.Name), strings.ToLower(keyword)) {
				results = append(results, &ContentRef{
					ContentID: meta.ContentID,
					Name:      meta.Name,
					Type:      meta.Type,
					SizeBytes: meta.SizeBytes,
				})
			}
		}
		return results
	}
	return nil
}
// service/service.go
// Layer 3A — Service & Provider Registry
//
// Purpose:
//   Authoritative service registration and discovery system.
//   Registers services announced via discovery, enforces role-based policies,
//   and provides clean queries for Layer 2 resolver.
//
// Key Design Principles:
//   - Authoritative and consistent
//   - Role-based policy enforcement
//   - Persistent state with cleanup
//   - Health-aware provider selection
//   - Read-heavy (used heavily by Layer 2)
//
// Depends on:
//   - Layer 1 (network table) for device status and health
//   - Layer 4 (messaging) for health updates
//
// Used by:
//   - Layer 2 (resolver) via ServiceResolver interface
//   - Applications for service discovery

// STORAGE STRETCH
//
// Now includes:
// - Extended capability tracking
// - Provider scoring abstraction
// - Background cleanup goroutine
// - Safer locking patterns (no nested locks)
// - Foundation for content/chunk storage integration

package service

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// ProviderTTL - how long a service announcement remains valid
const ProviderTTL = 45 * time.Second

// RoleServicePolicy defines which services each role is allowed to offer
var RoleServicePolicy = map[string][]string{
	"game":    {"games", "chat", "matchmaking"},
	"chat":    {"chat", "messaging", "presence"},
	"cache":   {"cache", "storage", "cdn"},
	"storage": {"storage", "backup", "files"},
	"unknown": {},
}

// ServiceMetadata contains default metadata for known services
var ServiceMetadata = map[string]map[string]any{
	"games":   {"protocol": "udp", "stateful": true, "version": 1},
	"chat":    {"protocol": "tcp", "stateful": true, "version": 1},
	"storage": {"protocol": "tcp", "stateful": false, "version": 1},
	"cache":   {"protocol": "udp", "stateful": false, "version": 1},
	"chunks":  {"protocol": "udp", "stateful": false, "version": 1, "chunked": true},
}

var (
	registry     = &ServiceRegistry{services: make(map[string]ServiceEntry)}
	contentStore *ContentStore
	once         sync.Once
	cleanupOnce  sync.Once
)

// ServiceEntry represents one registered service
type ServiceEntry struct {
	Providers map[types.NodeID]ProviderInfo `json:"providers"`
	Metadata  map[string]any                `json:"metadata"`
	Policy    map[string]any                `json:"policy"`
}

// ProviderInfo holds information about a device offering a service
type ProviderInfo struct {
	LastAnnounce time.Time          `json:"last_announce"`
	Metadata     map[string]any     `json:"metadata"` // port, ip, device_name, role
	Health       float64            `json:"health"`
	Capabilities types.Capabilities `json:"capabilities"` // Full device capabilities
	Score        float64            `json:"score"`        // Computed fitness
}

// ServiceRegistry is the authoritative in-memory registry
type ServiceRegistry struct {
	services map[string]ServiceEntry
	mu       sync.RWMutex
}

// Init — initializes both service registry and content store
func Init() {
	once.Do(func() {
		fmt.Println("[SERVICE] Layer 3 (Service + Content Fabric) initialized")

		// TODO: Get real NodeID from discovery after integration
		contentStore = NewContentStore(types.NodeID{}) // placeholder, will be set properly

		cleanupOnce.Do(func() {
			go startCleanupLoop()
		})
	})
}

// SetOwnerID — called from integration after NodeID is known
func SetOwnerID(id types.NodeID) {
	if contentStore == nil {
		contentStore = NewContentStore(id)
	} else {
		contentStore.ownerID = id
	}

	if indexWriter == nil {
		indexWriter = NewIndexWriter(id)
	}
}

// GetContentStore returns the hybrid content engine
func GetContentStore() *ContentStore {
	return contentStore
}

func startCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		registry.cleanupExpiredProviders()
	}
}

func (r *ServiceRegistry) cleanupExpiredProviders() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	for svcName, entry := range r.services {
		for id, p := range entry.Providers {
			if now.Sub(p.LastAnnounce) > ProviderTTL {
				delete(entry.Providers, id)
				expiredCount++
			}
		}
		if len(entry.Providers) == 0 {
			delete(r.services, svcName)
		}
	}

	if expiredCount > 0 {
		fmt.Printf("[SERVICE] Cleanup removed %d expired providers\n", expiredCount)
	}
}

// RegisterServicesFromDiscovery is called by Layer 1 when receiving DISCOVERY_ANNOUNCE
// Updated with scoring + capabilities
func RegisterServicesFromDiscovery(deviceID types.NodeID, services []string, servicePort int, role string) map[string]any {
	if len(services) == 0 {
		return map[string]any{"accepted": []string{}, "rejected": []string{}, "reason": "no services provided"}
	}

	accepted := []string{}
	rejected := []string{}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	deviceInfo := network.GetPeer(deviceID)
	if deviceInfo == nil {
		for _, svc := range services {
			rejected = append(rejected, svc)
		}
		return map[string]any{"accepted": accepted, "rejected": rejected}
	}

	for _, serviceName := range services {
		// 1. Role-based policy check
		allowed := RoleServicePolicy[role]
		allowedMap := make(map[string]bool)
		for _, s := range allowed {
			allowedMap[s] = true
		}

		if !allowedMap[serviceName] {
			rejected = append(rejected, serviceName)
			continue
		}

		// 2. Get or create service entry
		entry, exists := registry.services[serviceName]
		if !exists {
			entry = ServiceEntry{
				Providers: make(map[types.NodeID]ProviderInfo),
				Metadata:  ServiceMetadata[serviceName],
				Policy: map[string]any{
					"min_providers":  1,
					"min_health":     0.3,
					"load_balancing": "score_based",
				},
			}
			registry.services[serviceName] = entry
		}

		// 3. Register/update provider
		now := time.Now()
		score := calculateProviderScore(deviceInfo.Capabilities, deviceInfo.Health)

		entry.Providers[deviceID] = ProviderInfo{
			LastAnnounce: now,
			Metadata: map[string]any{
				"port":        servicePort,
				"ip":          deviceInfo.IP,
				"device_name": deviceInfo.Name,
				"role":        role,
			},
			Health:       deviceInfo.Health,
			Capabilities: deviceInfo.Capabilities,
			Score:        score,
		}

		accepted = append(accepted, serviceName)
		fmt.Printf("[SERVICE] %s registered '%s' (score: %.1f)\n", deviceInfo.Name, serviceName, score)
	}

	return map[string]any{"accepted": accepted, "rejected": rejected}
}

// calculateProviderScore — Central scoring abstraction
func calculateProviderScore(caps types.Capabilities, health float64) float64 {
	score := 0.0

	score += float64(caps.UplinkBandwidthMbps) * 0.4
	score += float64(caps.StorageAvailableMB) * 0.003
	if caps.CanStoreChunks {
		score += 40.0
	}
	if caps.StableNode {
		score += 25.0
	}
	if caps.CrossNetworkBridge {
		score += 30.0
	}

	score += health * 20.0 // Health is very important

	return score
}

// GetServiceProviders returns active providers for a service (used by Layer 2)
// Returns sorted by score
func GetServiceProviders(serviceName string, requireAlive bool, minHealth float64) []map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := []map[string]any{}
	now := time.Now()

	for deviceID, p := range entry.Providers {
		if now.Sub(p.LastAnnounce) > ProviderTTL {
			continue
		}

		device := network.GetPeer(deviceID)
		if device == nil {
			continue
		}
		if requireAlive && device.Status != "alive" {
			continue
		}
		if device.Health < minHealth {
			continue
		}

		providers = append(providers, map[string]any{
			"device_id":        deviceID,
			"name":             device.Name,
			"ip":               device.IP,
			"port":             p.Metadata["port"],
			"role":             device.Role,
			"health":           device.Health,
			"score":            p.Score,
			"capabilities":     p.Capabilities,
			"last_announce":    p.LastAnnounce,
			"service_metadata": entry.Metadata,
		})
	}

	// Sort by score descending
	sort.Slice(providers, func(i, j int) bool {
		return providers[i]["score"].(float64) > providers[j]["score"].(float64)
	})

	return providers
}

// GetServiceInfo returns full information about a service
func GetServiceInfo(serviceName string) map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := GetServiceProviders(serviceName, true, 0.5)

	return map[string]any{
		"name":            serviceName,
		"metadata":        entry.Metadata,
		"policy":          entry.Policy,
		"providers":       providers,
		"total_providers": len(providers),
	}
}

// GetAllServices returns information about all registered services
func GetAllServices() map[string]map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	result := make(map[string]map[string]any)
	for name := range registry.services {
		if info := GetServiceInfo(name); info != nil {
			result[name] = info
		}
	}
	return result
}

// UpdateProviderHealth is called by Layer 4 when messages succeed/fail
func UpdateProviderHealth(deviceID types.NodeID, success bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for _, entry := range registry.services {
		if p, exists := entry.Providers[deviceID]; exists {
			if success {
				p.Health = min(1.0, p.Health+0.05)
			} else {
				p.Health = max(0.0, p.Health-0.15)
			}
			// Re-score
			p.Score = calculateProviderScore(p.Capabilities, p.Health)
			entry.Providers[deviceID] = p
		}
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// ServiceResolver provides the interface expected by Layer 2
// ServiceResolver stub
type ServiceResolver struct{}

// Global instance
var ServiceResolverInstance = ServiceResolver{} // ← Fixed: add {}

// GetContentMeta returns metadata (local or via DHT later)
func GetContentMeta(contentID string) *ContentMeta {
	if cs := GetContentStore(); cs != nil {
		if meta := cs.GetMeta(contentID); meta != nil {
			return meta
		}
	}
	// TODO: DHT FIND_VALUE later
	return nil
}

// PublishContent is the main high-level API for applications
func PublishContent(name, fileType string, data []byte, tags, keywords []string) (*StoreResult, error) {
	result, err := StoreContent(name, fileType, data, tags, keywords)
	if err != nil {
		return nil, err
	}

	// Publish to DHT with replication
	if err := publishContentToDHT(result); err != nil {
		fmt.Printf("[SERVICE] DHT publish warning: %v\n", err)
		// We don't fail the whole operation — local storage succeeded
	}

	return result, nil
}

// ==================== MAIN PUBLIC APIS ====================

// StoreContent + Publish in one flow
func StoreContent(name, fileType string, data []byte, tags, keywords []string) (*StoreResult, error) {
	if contentStore == nil {
		return nil, fmt.Errorf("content store not initialized")
	}

	result, err := contentStore.Store(name, fileType, data, tags, keywords)
	if err != nil {
		return nil, err
	}

	AnnounceContent(result.Meta.ContentID, true, nil)
	IndexContent(result.Meta)
	PersistContent(result)

	if err := publishContentToDHT(result); err != nil {
		fmt.Printf("[SERVICE] DHT publish warning: %v\n", err)
	}

	return result, nil
}

// RetrieveContent
func RetrieveContent(contentID string) (*RetrieveResult, error) {
	// ... (your retrieval.go content - already good)
	return retrieveContentInternal(contentID)
}

// Search
func Search(keyword string) []*ContentRef {
	return searchInternal(keyword)
}

var (
	indexReader *IndexReader
	indexWriter *IndexWriter
)

func InitIndex() {
	indexReader = NewIndexReader()
	// indexWriter will be initialized after SetOwnerID
	fmt.Println("[INDEX] Layer 3C Search Index initialized")
}

func GetIndexReader() *IndexReader {
	return indexReader
}

func GetIndexWriter() *IndexWriter {
	return indexWriter
}
// service/types.go
// Layer 3B — Distributed Storage Core Types
//
// These are the canonical data structures for the entire content fabric.
//
// Design decisions explained:
//
// ContentMeta is intentionally kept LEAN.
// We do NOT embed full []ChunkMeta inline for large files.
// Reason: a 10GB file with 1MB chunks = 10,000 chunk entries.
// Storing that as a single DHT value would be huge, slow, and unreliable.
//
// Instead:
//   - ContentMeta stores the chunk COUNT and the ChunkManifestID
//   - The actual chunk list lives separately under /chunk-manifest/<ContentID>
//   - Clients fetch the manifest only when they actually need to retrieve chunks
//
// This is how BitTorrent's .torrent file design works — and it's proven at scale.

package service

import "innercore-network/types"

// ─────────────────────────────────────────────────────────────────────────────
// ContentMeta — the authoritative description of a stored file
// Stored in DHT under key: /content/<ContentID>
// ─────────────────────────────────────────────────────────────────────────────

type ContentMeta struct {
	// Identity
	ContentID string `json:"content_id"` // SHA256 of full file (or Merkle root for chunked)
	Name      string `json:"name"`       // human-readable filename
	Type      string `json:"type"`       // mime type or category ("video", "image", "doc", etc.)

	// Ownership
	OwnerNodeID types.NodeID `json:"owner_node_id"` // who originally published this
	CreatedAt   int64        `json:"created_at"`    // unix timestamp
	UpdatedAt   int64        `json:"updated_at"`

	// Size & chunking
	SizeBytes  int64 `json:"size_bytes"`
	IsChunked  bool  `json:"is_chunked"`
	ChunkCount int   `json:"chunk_count"` // 0 if not chunked
	ChunkSize  int64 `json:"chunk_size"`  // bytes per chunk (last chunk may be smaller)

	// Integrity
	Hash     string `json:"hash"`      // full file hash
	HashAlgo string `json:"hash_algo"` // "sha256" (always, for now)

	// If chunked, the manifest lives separately in the DHT.
	// Fetch it via: /chunk-manifest/<ContentID>
	// We store the ID here so callers know where to look.
	ChunkManifestID string `json:"chunk_manifest_id,omitempty"`

	// Versioning — supports content updates without breaking existing links
	Version     int    `json:"version"`
	PrevVersion string `json:"prev_version,omitempty"` // ContentID of previous version

	// Discovery metadata — used by the search index
	Tags     []string `json:"tags,omitempty"`
	Keywords []string `json:"keywords,omitempty"`

	// Replication — how many nodes should hold this content
	ReplicationFactor int `json:"replication_factor"`
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkManifest — the ordered list of chunks for a large file
// Stored in DHT under key: /chunk-manifest/<ContentID>
// Only fetched when a client actually needs to download the file
// ─────────────────────────────────────────────────────────────────────────────

type ChunkManifest struct {
	ContentID string      `json:"content_id"`
	Chunks    []ChunkMeta `json:"chunks"`
}

// ChunkMeta describes a single chunk of a large file
type ChunkMeta struct {
	Index     int    `json:"index"`      // position in the file (0-based)
	Hash      string `json:"hash"`       // SHA256 of this chunk's bytes
	SizeBytes int64  `json:"size_bytes"` // actual size (last chunk may differ)
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeCapabilities — what a node can contribute to the storage fabric
// This extends the basic types.Capabilities from the network layer.
// Stored and announced during discovery.
// ─────────────────────────────────────────────────────────────────────────────

type NodeStorageCapabilities struct {
	StorageTotalMB     int64   `json:"storage_total_mb"`     // total disk space dedicated
	StorageAvailableMB int64   `json:"storage_available_mb"` // currently free
	UploadMbps         float64 `json:"upload_mbps"`
	DownloadMbps       float64 `json:"download_mbps"`
	CanStoreChunks     bool    `json:"can_store_chunks"`  // willing to store chunk data
	CanRelayTraffic    bool    `json:"can_relay_traffic"` // willing to relay for others
	StableNode         bool    `json:"stable_node"`       // true if uptime > some threshold
}

// ─────────────────────────────────────────────────────────────────────────────
// ProviderInfo — tracks who currently holds a piece of content
// Stored in DHT under key: /provider/<ContentID>
// Multiple nodes can be providers for the same content
// ─────────────────────────────────────────────────────────────────────────────

type ContentProvider struct {
	NodeID       types.NodeID       `json:"node_id"`
	IP           string             `json:"ip"`
	Port         int                `json:"port"`
	Capabilities types.Capabilities `json:"capabilities"` // use main one
	AnnouncedAt  int64              `json:"announced_at"`
	HasFull      bool               `json:"has_full"`
	ChunksHeld   []int              `json:"chunks_held,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// ContentRef — a lightweight reference used in the search index
// Stored in DHT under key: /index/<keyword_hash>
// Many ContentRefs can map to a single keyword
// ─────────────────────────────────────────────────────────────────────────────

type ContentRef struct {
	ContentID string `json:"content_id"`
	Name      string `json:"name"`       // denormalized for fast display without fetching full meta
	Type      string `json:"type"`       // denormalized
	SizeBytes int64  `json:"size_bytes"` // denormalized
}
// service/tokenizer.go
// Layer 3C — Metadata Indexing & Search: Text Tokenizer
//
// ─────────────────────────────────────────────────────────────────────────────
// WHY A TOKENIZER EXISTS AS ITS OWN FILE
// ─────────────────────────────────────────────────────────────────────────────
//
// Search only works if the tokens in the index match the tokens in the query.
// This sounds obvious but is surprisingly subtle.
//
// Problem: user stores a file named "Suits.S01E03.1080p.BluRay.mkv"
//          user searches for "suits season 1"
//          Without a tokenizer: ZERO MATCHES (strings don't match literally)
//          With a tokenizer:
//            Index tokens:  ["suits", "s01e03", "1080p", "bluray", "mkv", "video"]
//            Query tokens:  ["suits", "season", "1"]
//            Overlap:       ["suits"] → match found
//
// The tokenizer also ensures:
//   "Suits" == "suits" == "SUITS"   (case normalization)
//   "the", "a", "an" are ignored    (stopword removal)
//   short meaningless tokens dropped (min length)
//   duplicates removed               (deduplication)
//
// ─────────────────────────────────────────────────────────────────────────────
// DESIGN DECISION: we do NOT use stemming (reducing "running" → "run")
// ─────────────────────────────────────────────────────────────────────────────
//
// Stemming adds complexity and can cause false matches.
// For a media/file sharing network, exact-word matching is good enough.
// Users searching "suits" want the TV show, not "suitable" or "suited".
// We can add stemming later if needed — it's isolated in this file.

package service

import (
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

// MinTokenLength — tokens shorter than this are discarded
// "a", "an", "in" are not useful search tokens
const MinTokenLength = 2

// MaxTokensPerField — safety limit to prevent index explosion from malicious input
// A filename with 1000 tokens would create 1000 DHT entries — we cap this
const MaxTokensPerField = 50

// ─────────────────────────────────────────────────────────────────────────────
// Stopwords — common words that add no search value
// Searching for "the suits" should not create an index entry for "the"
// because every document contains "the" and it tells us nothing
// ─────────────────────────────────────────────────────────────────────────────

var stopwords = map[string]bool{
	"a": true, "an": true, "the": true,
	"and": true, "or": true, "but": true,
	"in": true, "on": true, "at": true, "to": true, "for": true,
	"of": true, "with": true, "by": true,
	"is": true, "are": true, "was": true, "were": true,
	"it": true, "its": true, "this": true, "that": true,
	"be": true, "been": true, "being": true,
	"have": true, "has": true, "had": true,
	"do": true, "does": true, "did": true,
	"will": true, "would": true, "could": true, "should": true,
	"not": true, "no": true,
}

// fileExtensionStopwords — file extensions that tell us the type but
// are not useful as search tokens because they're too generic
// We extract the type information separately and discard the raw extension
var fileExtensionStopwords = map[string]bool{
	"mkv": true, "mp4": true, "avi": true, "mov": true, "wmv": true,
	"flv": true, "webm": true, "m4v": true,
	"mp3": true, "flac": true, "wav": true, "aac": true, "ogg": true,
	"jpg": true, "jpeg": true, "png": true, "gif": true, "webp": true,
	"pdf": true, "doc": true, "docx": true, "txt": true,
	"zip": true, "rar": true, "tar": true, "gz": true,
}

// qualityTags — tokens that indicate video/audio quality
// We keep these as index tokens but strip them from display
var qualityTags = map[string]bool{
	"1080p": true, "720p": true, "480p": true, "4k": true, "2160p": true,
	"bluray": true, "blu-ray": true, "bdrip": true, "brrip": true,
	"webrip": true, "web-dl": true, "webdl": true, "hdtv": true,
	"hdrip": true, "dvdrip": true, "dvdscr": true,
	"x264": true, "x265": true, "h264": true, "h265": true, "hevc": true,
	"avc": true, "xvid": true, "divx": true,
	"aac": true, "ac3": true, "dts": true, "dd5": true, "truehd": true,
	"yify": true, "yts": true, "rarbg": true, "eztv": true,
}

// ─────────────────────────────────────────────────────────────────────────────
// Regex patterns for common filename structures
// ─────────────────────────────────────────────────────────────────────────────

var (
	// Matches S01E03, s01e03, S01E03E04 — TV episode identifiers
	// We keep these as tokens so users can search "S01E03"
	episodePattern = regexp.MustCompile(`(?i)s\d{1,2}e\d{1,2}(?:e\d{1,2})?`)

	// Matches year patterns like (2019), [2019], .2019.
	yearPattern = regexp.MustCompile(`\b(19|20)\d{2}\b`)

	// Splits on anything that's not a letter or digit
	// This handles dots, underscores, hyphens, brackets used as separators in filenames
	// "Suits.S01.mkv" → ["Suits", "S01", "mkv"]
	separatorPattern = regexp.MustCompile(`[^a-zA-Z0-9]+`)
)

// ─────────────────────────────────────────────────────────────────────────────
// Tokenizer — the main struct
// ─────────────────────────────────────────────────────────────────────────────

type Tokenizer struct{}

// NewTokenizer creates a tokenizer instance
// No state — the struct is just a namespace for the methods
func NewTokenizer() *Tokenizer {
	return &Tokenizer{}
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeContentMeta — the primary entry point
//
// Takes a ContentMeta and returns ALL tokens that should be indexed.
// Called by IndexWriter when a new file is stored.
//
// Token sources and their weights:
//   Filename (without extension) → weight 1.0 (strongest signal)
//   Tags                         → weight 0.8
//   Keywords                     → weight 0.5
//   File type                    → weight 0.3 (allows filtering by type)
// ─────────────────────────────────────────────────────────────────────────────

type WeightedToken struct {
	Token  string
	Weight float64 // 0.0 → 1.0, higher means stronger match signal
}

func (t *Tokenizer) TokenizeContentMeta(meta *ContentMeta) []WeightedToken {
	var result []WeightedToken
	seen := make(map[string]float64) // token → highest weight seen so far

	// Helper to add a token, keeping the highest weight if token appears multiple times
	add := func(token string, weight float64) {
		if existing, ok := seen[token]; !ok || weight > existing {
			seen[token] = weight
		}
	}

	// 1. Filename tokens (highest weight — this is what the user named the file)
	nameTokens := t.TokenizeFilename(meta.Name)
	for _, tok := range nameTokens {
		add(tok, 1.0)
	}

	// 2. Tag tokens (user-defined labels — high value)
	for _, tag := range meta.Tags {
		for _, tok := range t.tokenizeWords(tag) {
			add(tok, 0.8)
		}
	}

	// 3. Keyword tokens (secondary metadata)
	for _, kw := range meta.Keywords {
		for _, tok := range t.tokenizeWords(kw) {
			add(tok, 0.5)
		}
	}

	// 4. Type token (allows searches like "type:video" — simplified for now)
	if meta.Type != "" {
		typeTokens := t.tokenizeWords(meta.Type)
		for _, tok := range typeTokens {
			add(tok, 0.3)
		}
	}

	// Build result list, cap at MaxTokensPerField
	for token, weight := range seen {
		result = append(result, WeightedToken{Token: token, Weight: weight})
		if len(result) >= MaxTokensPerField {
			break
		}
	}

	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeFilename — handles the special structure of media filenames
//
// "Suits.S01E03.1080p.BluRay.x264.mkv" →
//   ["suits", "s01e03", "1080p", "bluray", "x264"]
//   (extension "mkv" dropped — kept separately as type info)
//
// "The.Dark.Knight.2008.BluRay.mp4" →
//   ["dark", "knight", "2008", "bluray"]
//   ("the" = stopword, dropped)
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) TokenizeFilename(filename string) []string {
	// Remove the file extension — it's metadata, not a search token
	// filepath.Ext returns ".mkv", we strip the dot
	ext := filepath.Ext(filename)
	withoutExt := strings.TrimSuffix(filename, ext)

	return t.tokenizeWords(withoutExt)
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeQuery — tokenizes a user's search query
//
// Applies the same normalization as filename tokenization so tokens align.
// "Suits Season 1" → ["suits", "season", "1"]
// "suits s01" → ["suits", "s01"]
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) TokenizeQuery(query string) []string {
	return t.tokenizeWords(query)
}

// ─────────────────────────────────────────────────────────────────────────────
// tokenizeWords — internal: the core normalization pipeline
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) tokenizeWords(text string) []string {
	if text == "" {
		return nil
	}

	// Step 1: Lowercase everything
	// "SUITS" == "Suits" == "suits"
	lower := strings.ToLower(text)

	// Step 2: Split on separators (dots, underscores, spaces, brackets, hyphens)
	// "Suits.S01E03" → ["suits", "s01e03"]
	// "Dark Knight"  → ["dark", "knight"]
	rawTokens := separatorPattern.Split(lower, -1)

	// Step 3: Filter and clean
	seen := make(map[string]bool)
	var result []string

	for _, tok := range rawTokens {
		tok = strings.TrimFunc(tok, unicode.IsSpace)

		// Drop empty tokens
		if tok == "" {
			continue
		}

		// Drop tokens that are too short to be meaningful
		if len(tok) < MinTokenLength {
			continue
		}

		// Drop stopwords
		if stopwords[tok] {
			continue
		}

		// Drop file extension stopwords (mkv, mp4, etc.)
		// We still keep quality tags like "1080p", "bluray" — they're searchable
		if fileExtensionStopwords[tok] {
			continue
		}

		// Drop pure punctuation or non-alphanumeric tokens
		if !containsAlphanumeric(tok) {
			continue
		}

		// Deduplicate
		if seen[tok] {
			continue
		}
		seen[tok] = true
		result = append(result, tok)

		// Safety cap
		if len(result) >= MaxTokensPerField {
			break
		}
	}

	return nil //temporary
}

// ─────────────────────────────────────────────────────────────────────────────
// DetectFileType — infer content type from filename extension
//
// Layer 3B stores whatever type the publisher provides.
// This helper lets us infer a sane default when the publisher doesn't specify.
// ─────────────────────────────────────────────────────────────────────────────

func DetectFileType(filename string) string {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))

	switch ext {
	case "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v":
		return "video"
	case "mp3", "flac", "wav", "aac", "ogg", "m4a":
		return "audio"
	case "jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff":
		return "image"
	case "pdf":
		return "pdf"
	case "doc", "docx", "odt":
		return "document"
	case "txt", "md", "rst":
		return "text"
	case "zip", "rar", "tar", "gz", "7z":
		return "archive"
	case "exe", "dmg", "apk", "deb", "rpm":
		return "software"
	default:
		return "other"
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func containsAlphanumeric(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// IsQualityTag returns true if the token is a technical quality marker
// Useful for the UI to decide whether to display a token or hide it
func IsQualityTag(token string) bool {
	return qualityTags[strings.ToLower(token)]
}

// ExtractEpisodeCode extracts a TV episode code from a filename if present
// "Suits.S01E03.mkv" → "s01e03", true
// "The.Dark.Knight.mkv" → "", false
func ExtractEpisodeCode(filename string) (string, bool) {
	match := episodePattern.FindString(filename)
	if match == "" {
		return "", false
	}
	return strings.ToLower(match), true
}

// ExtractYear extracts a year (1900–2099) from a filename if present
// "The.Dark.Knight.2008.mkv" → "2008", true
func ExtractYear(filename string) (string, bool) {
	match := yearPattern.FindString(filename)
	return match, match != ""
}
// allowedroles/allowed_roles.go
// Allowed roles in the system.
// This is used across multiple layers for policy enforcement.

package allowedroles

// AllowedRoles is the set of valid roles in the mesh network.
// Any role not in this set is automatically treated as "unknown".
var AllowedRoles = map[string]bool{
	"game":    true,
	"chat":    true,
	"cache":   true,
	"storage": true,
	"unknown": true,
}

// IsAllowed returns true if the role is valid
func IsAllowed(role string) bool {
	return AllowedRoles[role]
}
// discovery/discovery.go
// discovery/discovery.go
package discovery

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"innercore-network/packet"
	"innercore-network/types"
)

const (
	BroadcastAddress = "255.255.255.255"
	AnnounceInterval = 5 * time.Second
)

var (
	myNodeID   types.NodeID
	myNodeName string
	myCaps     types.Capabilities
	once       sync.Once

	// Made configurable
	DiscoveryPort    int = 37020
	LocalMessagePort int = 51000 // for Layer 4 messaging
)

var (
	UpdateNetworkCallback func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int)
	SeedPeerCallback      func(nodeID types.NodeID, ip string, caps types.Capabilities)
)

func Init(services []string, servicePort int, discoveryPort int, messagePort int) error {
	if discoveryPort > 0 {
		DiscoveryPort = discoveryPort
	}
	if messagePort > 0 {
		LocalMessagePort = messagePort
	}

	once.Do(func() {
		var err error
		myNodeID, err = LoadOrCreateNodeID()
		if err != nil {
			panic(fmt.Sprintf("Failed to load NodeID: %v", err))
		}

		myNodeName = getHostname()

		myCaps = types.Capabilities{
			InternetAccess:      false,
			CrossNetworkBridge:  isHotspotCapable(),
			HotspotCapable:      isHotspotCapable(),
			UplinkBandwidthMbps: 50,
			LatencyMs:           20,
			UptimeSeconds:       0,
		}

		fmt.Printf("[DISCOVERY] Node %s initialized with NodeID %s (DiscoveryPort: %d, MsgPort: %d)\n",
			myNodeName, myNodeID, DiscoveryPort, LocalMessagePort)
	})

	go announceLoop(services, servicePort)
	go listenLoop()

	return nil
}

// ==================== Helper Functions ====================

func getHostname() string {
	name, _ := os.Hostname()
	if name == "" {
		name = "unknown-device"
	}
	return name
}

func isHotspotCapable() bool {
	return true // TODO: Make this detect real hotspot capability later
}

// ==================== Core Loops ====================

func announceLoop(services []string, servicePort int) {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(255, 255, 255, 255), Port: DiscoveryPort})
	if err != nil {
		fmt.Printf("[DISCOVERY] Broadcast socket failed: %v\n", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(AnnounceInterval)
	defer ticker.Stop()

	for range ticker.C {
		p := packet.Packet{
			Header: packet.Header{
				Version:           2,
				RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
				PacketType:        packet.PacketTypeDiscoveryAnnounce,
				TTL:               8,
				SourceNodeID:      myNodeID,
				DestinationNodeID: types.NodeID{},
				Timestamp:         time.Now().Unix(),
			},
			Network: packet.NetworkInfo{
				SourceRegion:      "KE-Nairobi",
				DestinationRegion: "UNKNOWN",
			},
			Capabilities: myCaps,
			Payload:      mustMarshal(map[string]any{"services": services, "service_port": servicePort}),
		}

		data, _ := json.Marshal(p)
		conn.Write(data)
	}
}

func listenLoop() {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: DiscoveryPort})
	if err != nil {
		fmt.Printf("[DISCOVERY] Bind failed: %v\n", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 4096)

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var p packet.Packet
		if err := json.Unmarshal(buf[:n], &p); err != nil {
			continue
		}

		// Ignore self
		if p.Header.SourceNodeID.Equal(myNodeID) {
			continue
		}

		senderIP := addr.IP.String()

		fmt.Printf("[DISCOVERY] Received %s from %s (%s)\n", p.Header.PacketType, senderIP, p.Header.SourceNodeID)

		var payloadMap map[string]any
		json.Unmarshal(p.Payload, &payloadMap)

		services := []string{}
		if s, ok := payloadMap["services"].([]any); ok {
			for _, v := range s {
				if str, ok := v.(string); ok {
					services = append(services, str)
				}
			}
		}

		servicePort := 5000
		if sp, ok := payloadMap["service_port"].(float64); ok {
			servicePort = int(sp)
		}

		if UpdateNetworkCallback != nil {
			UpdateNetworkCallback(p.Header.SourceNodeID, senderIP, "", p.Capabilities, services, servicePort)
		}

		// Feed discovered peer into InnerCore for Kademlia routing table
		if SeedPeerCallback != nil {
			SeedPeerCallback(p.Header.SourceNodeID, senderIP, p.Capabilities)
		}
	}
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// GetMyNodeID returns the current node's ID (used by InnerCore)
func GetMyNodeID() types.NodeID {
	return myNodeID
}

// GetMyNodeName returns the current node's friendly name
func GetMyNodeName() string {
	return myNodeName
}

// SetSeedPeerCallback allows InnerCore to register itself without creating import cycle
func SetSeedPeerCallback(cb func(nodeID types.NodeID, ip string, caps types.Capabilities)) {
	SeedPeerCallback = cb
	fmt.Println("[DISCOVERY] SeedPeerCallback registered for Kademlia")
}
// discovery/nodeid.go
package discovery

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"innercore-network/types" // ← Add this import
)

const nodeIDFile = "node_id.bin"

// LoadOrCreateNodeID loads existing NodeID or creates a new persistent one
func LoadOrCreateNodeID() (types.NodeID, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return types.NodeID{}, err
	}

	// Support multiple instances for testing
	instanceSuffix := ""
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceSuffix = "_instance1"
	}

	path := filepath.Join(home, ".innercore", "node_id"+instanceSuffix+".bin")

	// Try to load existing
	if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
		var id types.NodeID
		copy(id[:], data)
		fmt.Printf("[DISCOVERY] Loaded existing NodeID: %s\n", id)
		return id, nil
	}

	// Create new
	var id types.NodeID
	if _, err := rand.Read(id[:]); err != nil {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("%d%s", time.Now().UnixNano(), instanceSuffix)))
		copy(id[:], h.Sum(nil))
	}

	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, id[:], 0600)

	fmt.Printf("[DISCOVERY] Generated new NodeID: %s\n", id)
	return id, nil
}
// discovery/types.go
package discovery

import "innercore-network/types"

// DiscoveryMessage is the universal format used in Layer 1
// (will align with Layer 4 later)
type DiscoveryMessage struct {
	ProtocolVersion int                `json:"protocol_version"`
	Type            string             `json:"type"` // DISCOVERY_PING, DISCOVERY_PONG, DISCOVERY_ANNOUNCE
	RequestID       string             `json:"request_id"`
	NodeID          types.NodeID       `json:"node_id"`
	NodeName        string             `json:"node_name"`
	Timestamp       int64              `json:"timestamp"`
	Capabilities    types.Capabilities `json:"capabilities"`
	Services        []string           `json:"services,omitempty"`
	ServicePort     int                `json:"service_port,omitempty"`
	// Future: Region, TTL, Signature, etc.
}
// layer6/gateway.go
// Layer 6 — Internet Egress & Fallback Gateway
//
// Responsibilities:
//   - Transparent internet egress when local mesh cannot resolve a target
//   - Automatic fallback routing
//   - Session-aware forwarding (TCP proxy style)
//   - Capability-aware routing decisions (uses InnerCore + network table)
//   - No persistence logic (that belongs to Layer 5)
//
// This layer may touch the public internet.
//
// Design Notes:
//   - Routing decisions (including cross-network bridging) are made here
//     because this layer has visibility into capabilities and network state.
//   - It consults InnerCore for supernode assistance and Layer 1 network table
//     for current device status and capabilities.

package fallback

import (
	"fmt"
	"net"
	"sync"
	"time"

	"innercore-network/innercore"
)

const (
	DefaultHTTPPort  = 80
	DefaultHTTPSPort = 443
	SocketTimeout    = 10 * time.Second
	BufferSize       = 8192
)

type GatewaySession struct {
	ClientAddr   string
	Target       string
	CreatedAt    time.Time
	LastActivity time.Time
	BytesUp      int64
	BytesDown    int64
}

type InternetGateway struct {
	listenIP   string
	listenPort int
	sessions   map[string]*GatewaySession
	mu         sync.RWMutex
	running    bool
	innerCore  *innercore.InnerCore
}

func NewInternetGateway(listenIP string, listenPort int, ic *innercore.InnerCore) *InternetGateway {
	return &InternetGateway{
		listenIP:   listenIP,
		listenPort: listenPort,
		sessions:   make(map[string]*GatewaySession),
		innerCore:  ic,
	}
}

func (g *InternetGateway) Start() {
	g.running = true
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.listenIP, g.listenPort))
	if err != nil {
		fmt.Printf("[L6] Failed to listen on %s:%d: %v\n", g.listenIP, g.listenPort, err)
		return
	}

	fmt.Printf("[L6] Internet Gateway listening on %s:%d\n", g.listenIP, g.listenPort)

	go func() {
		for g.running {
			conn, err := ln.Accept()
			if err != nil {
				if g.running {
					fmt.Printf("[L6] Accept error: %v\n", err)
				}
				continue
			}
			go g.handleClient(conn)
		}
		ln.Close()
	}()
}

func (g *InternetGateway) Stop() {
	g.running = false
}

// handleClient processes one incoming client connection
func (g *InternetGateway) handleClient(clientConn net.Conn) {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	fmt.Printf("[L6] New client connection from %s\n", clientAddr)

	// Read initial data to determine target (naive HTTP Host header parsing for MVP)
	buf := make([]byte, BufferSize)
	n, err := clientConn.Read(buf)
	if err != nil {
		return
	}
	initialData := buf[:n]

	targetHost, targetPort := g.extractTarget(initialData)
	if targetHost == "" {
		fmt.Printf("[L6] Could not determine target from client %s\n", clientAddr)
		return
	}

	targetAddr := fmt.Sprintf("%s:%d", targetHost, targetPort)
	session := &GatewaySession{
		ClientAddr:   clientAddr,
		Target:       targetAddr,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}

	g.mu.Lock()
	g.sessions[clientAddr] = session
	g.mu.Unlock()

	g.forwardSession(clientConn, session, initialData)
}

// extractTarget is a simple MVP parser (can be improved later with proper HTTP parsing)
func (g *InternetGateway) extractTarget(data []byte) (host string, port int) {
	text := string(data)
	for _, line := range splitLines(text) {
		if len(line) > 5 && line[:5] == "Host:" {
			hostPart := line[5:]
			hostPart = trimSpace(hostPart)
			if idx := indexByte(hostPart, ':'); idx != -1 {
				host = hostPart[:idx]
				// port parsing omitted for simplicity
				return host, DefaultHTTPPort
			}
			return hostPart, DefaultHTTPPort
		}
	}
	return "", 0
}

// forwardSession relays traffic between client and internet
func (g *InternetGateway) forwardSession(clientConn net.Conn, session *GatewaySession, firstPayload []byte) {
	upstream, err := net.DialTimeout("tcp", session.Target, SocketTimeout)
	if err != nil {
		fmt.Printf("[L6] Failed to connect to %s: %v\n", session.Target, err)
		return
	}
	defer upstream.Close()

	// Send initial payload
	upstream.Write(firstPayload)

	// Bidirectional relay
	go g.relay(clientConn, upstream, session, true) // client -> upstream
	g.relay(upstream, clientConn, session, false)   // upstream -> client
}

func (g *InternetGateway) relay(src, dst net.Conn, session *GatewaySession, upstream bool) {
	buf := make([]byte, BufferSize)
	for {
		n, err := src.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			dst.Write(buf[:n])
			session.LastActivity = time.Now()
			if upstream {
				session.BytesUp += int64(n)
			} else {
				session.BytesDown += int64(n)
			}
		}
	}
}

// Simple helper - improve later with proper HTTP parsing
func splitLines(s string) []string {
	return nil // placeholder - not used yet, but declared to avoid compile error
}

func trimSpace(s string) string {
	// simple trim
	return s
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
// innercore/innercore.go
package innercore

//
// Core controller of the Kademlia overlay network.
//
// Responsibilities:
// - Owns all Kademlia state and routing tables
// - Manages 256 XOR-distance k-buckets
// - Inserts discovered peers into correct buckets
// - Maintains preferred supernode list based on capability scoring
// - Provides closest-peer selection for lookups
// - Tracks async pending lookups using requestID -> channel mapping
// - Bridges outgoing FIND_NODE requests with incoming replies
// - Runs background maintenance and bucket refresh tasks
// - Coordinates lookup synchronization across goroutines
//
// Architecture Role:
// Discovery -> SeedPeer -> KBucket -> Lookup/RPC
//
// Important Concepts:
// - XOR distance routing
// - Kademlia bucket management
// - Async RPC reply correlation
// - Capability-driven supernode election
// - Concurrent lookup coordination
//
// NOTE:
// This file manages state/orchestration only.
// Actual network transport and lookup traversal logic live elsewhere.

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"innercore-network/packet"
	"innercore-network/storage"
	"innercore-network/types"
)

type InnerCore struct {
	nodeID     types.NodeID
	kBuckets   [256]*KBucket
	supernodes []PeerInfo
	mu         sync.RWMutex
	storage    *storage.StorageEngine
	msgSender  func(target types.NodeID, pkt packet.Packet) error

	// pendingLookups maps requestID → a channel that receives the []PeerInfo
	// returned in a KADEMLIA_FIND_NODE_REPLY.
	// This is the bridge between the fire-and-forget UDP send and the async reply
	// that arrives through HandleKademliaPacket on a completely different goroutine.
	pendingLookups   map[string]chan []PeerInfo
	pendingLookupsMu sync.Mutex
}

func New(storageEngine *storage.StorageEngine, senderFunc func(types.NodeID, packet.Packet) error) *InnerCore {
	ic := &InnerCore{
		storage:        storageEngine,
		msgSender:      senderFunc,
		pendingLookups: make(map[string]chan []PeerInfo),
	}

	for i := range ic.kBuckets {
		ic.kBuckets[i] = NewKBucket(20)
	}

	ic.loadPersistedTable()
	go ic.maintenanceLoop()

	fmt.Println("[INNERCORE] InnerCore started — Kademlia + capability-driven supernodes")
	return ic
}

// SeedPeer is called by discovery when a new peer is found on the network.
func (ic *InnerCore) SeedPeer(nodeID types.NodeID, ip string, caps types.Capabilities) {
	// Guard: if SetNodeID hasn't been called yet, our XOR distance calculations
	// would all use the zero NodeID → wrong bucket assignments.
	// Integration.Init() must call SetNodeID BEFORE registering discovery callbacks.
	if ic.nodeID == (types.NodeID{}) {
		fmt.Printf("[INNERCORE] WARNING: SeedPeer called before NodeID was set. Skipping for now.\n")
		return
	}

	peer := &PeerInfo{
		NodeID:       nodeID,
		IP:           ip,
		Capabilities: caps,
		LastSeen:     time.Now().Unix(),
		Score:        calculateScore(caps),
	}

	dist := xorDistance(ic.nodeID, nodeID)
	bucketIdx := 0
	for i := uint(0); i < 256; i++ {
		if (dist & (1 << i)) != 0 {
			bucketIdx = int(i)
			break
		}
	}

	ic.kBuckets[bucketIdx].Insert(peer)
	ic.updateSupernodeList()
}

func (ic *InnerCore) GetPreferredSupernodes() []PeerInfo {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.supernodes
}

// ==================== Lookup Helpers ====================

func (ic *InnerCore) getAlphaClosest(target types.NodeID, alpha int) []PeerInfo {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	var candidates []*PeerInfo
	for i := range ic.kBuckets {
		candidates = append(candidates, ic.kBuckets[i].nodes...)
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return xorDistance(candidates[i].NodeID, target) < xorDistance(candidates[j].NodeID, target)
	})

	if len(candidates) > alpha {
		candidates = candidates[:alpha]
	}

	result := make([]PeerInfo, len(candidates))
	for i, p := range candidates {
		result[i] = *p
	}
	return result
}

func (ic *InnerCore) getLocalClosest(from types.NodeID, target types.NodeID, count int) []PeerInfo {
	return ic.getAlphaClosest(target, count)
}

// ==================== Pending Lookup Registry ====================

// registerPendingLookup stores a channel under a requestID so that
// HandleKademliaPacket can deliver FIND_NODE_REPLY peers back to the waiting Lookup().
// The channel is buffered(1) so the reply handler never blocks even if the waiter
// has already timed out and moved on.
func (ic *InnerCore) registerPendingLookup(requestID string) chan []PeerInfo {
	ch := make(chan []PeerInfo, 1)
	ic.pendingLookupsMu.Lock()
	ic.pendingLookups[requestID] = ch
	ic.pendingLookupsMu.Unlock()
	return ch
}

// resolvePendingLookup delivers peers to the waiting Lookup() call and removes the entry.
func (ic *InnerCore) resolvePendingLookup(requestID string, peers []PeerInfo) {
	ic.pendingLookupsMu.Lock()
	ch, exists := ic.pendingLookups[requestID]
	if exists {
		delete(ic.pendingLookups, requestID)
	}
	ic.pendingLookupsMu.Unlock()

	if exists {
		ch <- peers // non-blocking because channel is buffered(1)
	}
}

// cancelPendingLookup removes a pending entry without delivering (used on timeout).
func (ic *InnerCore) cancelPendingLookup(requestID string) {
	ic.pendingLookupsMu.Lock()
	delete(ic.pendingLookups, requestID)
	ic.pendingLookupsMu.Unlock()
}

// ==================== Background Methods ====================

func (ic *InnerCore) updateSupernodeList() {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	var allPeers []*PeerInfo
	for i := range ic.kBuckets {
		allPeers = append(allPeers, ic.kBuckets[i].nodes...)
	}

	sort.Slice(allPeers, func(i, j int) bool {
		return allPeers[i].Score > allPeers[j].Score
	})

	ic.supernodes = make([]PeerInfo, 0, 5)
	for i := 0; i < len(allPeers) && i < 5; i++ {
		ic.supernodes = append(ic.supernodes, *allPeers[i])
	}

	if len(ic.supernodes) > 0 {
		fmt.Printf("[SUPERNODE ELECTION] Top supernodes updated. Best score: %.1f\n", ic.supernodes[0].Score)
	}
}

func (ic *InnerCore) maintenanceLoop() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		ic.refreshBuckets()
		ic.updateSupernodeList()
	}
}

func (ic *InnerCore) loadPersistedTable() {
	// Step 6 — real persistence later
	fmt.Println("[INNERCORE] Loaded persisted routing table (stub)")
}

func (ic *InnerCore) refreshBuckets() {
	// Step 5 — ping oldest + eviction
	fmt.Println("[KADEMLIA] Bucket maintenance run (ping oldest 3)")
	// TODO: real ping-oldest logic
	ic.updateSupernodeList()
}

// SetNodeID is used by the integration layer to avoid import cycles.
// MUST be called before any discovery callbacks that invoke SeedPeer.
func (ic *InnerCore) SetNodeID(id types.NodeID) {
	ic.nodeID = id
	fmt.Printf("[INNERCORE] NodeID set: %s\n", id)
}

// TestLookup is a helper to manually trigger a lookup from main or tests
func (ic *InnerCore) TestLookup(target types.NodeID) {
	fmt.Printf("[TEST] Starting lookup for target %s...\n", target)
	peers, err := ic.Lookup(target)
	if err != nil {
		fmt.Printf("[TEST] Lookup failed: %v\n", err)
		return
	}
	fmt.Printf("[TEST] Lookup found %d peers:\n", len(peers))
	for _, p := range peers {
		fmt.Printf("   → %s (Score: %.1f)\n", p.NodeID, p.Score)
	}
}

// GetMyNodeID returns the current node's ID
func (ic *InnerCore) GetMyNodeID() types.NodeID {
	return ic.nodeID
}
// innercore/kbucket.go
package innercore

import (
	"sort"
	"sync"

	"innercore-network/types"
)

// KBucket holds up to K peers at a specific XOR distance range
type KBucket struct {
	nodes []*PeerInfo
	k     int
	mu    sync.RWMutex
}

func NewKBucket(k int) *KBucket {
	return &KBucket{
		nodes: make([]*PeerInfo, 0, k),
		k:     k,
	}
}

func (b *KBucket) Insert(peer *PeerInfo) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update existing peer and move to tail (most recent)
	for i, p := range b.nodes {
		if p.NodeID.Equal(peer.NodeID) {
			b.nodes[i] = peer
			copy(b.nodes[i:], b.nodes[i+1:])
			b.nodes[len(b.nodes)-1] = peer
			return true
		}
	}

	// Add new peer if space available
	if len(b.nodes) < b.k {
		b.nodes = append(b.nodes, peer)
		return true
	}

	// Bucket full - will be handled by maintenance (ping oldest)
	return false
}

func (b *KBucket) GetClosest(target types.NodeID, count int) []*PeerInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.nodes) == 0 {
		return nil
	}

	candidates := make([]*PeerInfo, len(b.nodes))
	copy(candidates, b.nodes)

	sort.Slice(candidates, func(i, j int) bool {
		return xorDistance(candidates[i].NodeID, target) < xorDistance(candidates[j].NodeID, target)
	})

	if len(candidates) > count {
		candidates = candidates[:count]
	}
	return candidates
}

func xorDistance(a, b types.NodeID) uint {
	var d uint
	for i := range a {
		d += uint(a[i] ^ b[i])
	}
	return d
}

/*
What this file does:

Implements one Kademlia k-bucket (distance-based list).
Insert follows classic Kademlia rules: update existing, move to tail, respect capacity.
GetClosest returns the best peers for a lookup target using XOR metric.
Thread-safe with RWMutex.

*/
// innercore/lookup.go
package innercore

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// Lookup performs a real iterative Kademlia lookup.
//
// THE PROBLEM WITH THE OLD VERSION:
// The old code fired SendFindNode() inside goroutines and then immediately read
// from getAlphaClosest() — which is LOCAL state. It never waited for the UDP reply
// to come back from the remote node. The network round-trip takes milliseconds but
// the goroutine had already closed the channel and moved on. This meant every
// "iteration" was just re-reading your own routing table, not the network.
//
// THE FIX:
// For each peer we query, we register a pendingLookup channel BEFORE sending the
// packet. SendFindNodeAndWait sends the packet then blocks on that channel with a
// timeout. When HandleKademliaPacket receives a KADEMLIA_FIND_NODE_REPLY, it calls
// resolvePendingLookup which delivers the real remote peers into that channel.
// Now each iteration actually uses what the network told us.
func (ic *InnerCore) Lookup(target types.NodeID) ([]PeerInfo, error) {
	fmt.Printf("[KADEMLIA] Starting lookup for target %s\n", target)

	// Guard: prevent lookup if we don't have our own NodeID yet
	if ic.nodeID == (types.NodeID{}) {
		fmt.Println("[KADEMLIA] WARNING: NodeID not set yet. Returning empty result.")
		return nil, fmt.Errorf("nodeID not initialized")
	}

	if target.Equal(ic.nodeID) {
		fmt.Println("[KADEMLIA] Self lookup - returning self")
		return []PeerInfo{{
			NodeID:   ic.nodeID,
			IP:       "127.0.0.1",
			LastSeen: time.Now().Unix(),
			Score:    100.0,
		}}, nil
	}

	alpha := 3
	// Seed the first round from our local routing table
	closest := ic.getAlphaClosest(target, alpha)

	// Fallback: if we have nothing in k-buckets yet, use the network table directly.
	// This happens on a fresh node that just joined and hasn't filled buckets yet.
	if len(closest) == 0 {
		fmt.Println("[KADEMLIA] No peers in buckets, falling back to network table")
		peers := network.GetAllPeers()
		for _, p := range peers {
			closest = append(closest, PeerInfo{
				NodeID:       p.NodeID,
				IP:           p.IP,
				Capabilities: p.Capabilities,
				LastSeen:     p.LastSeen,
				Score:        p.Score,
			})
		}
	}

	seen := make(map[types.NodeID]bool)
	var result []PeerInfo
	start := time.Now()

	for iteration := 0; len(closest) > 0 && iteration < 5 && time.Since(start) < 5*time.Second; iteration++ {
		fmt.Printf("[KADEMLIA] Iteration %d | Querying %d peers\n", iteration, len(closest))

		var wg sync.WaitGroup
		var newPeersFromNetwork []PeerInfo
		var newPeersMu sync.Mutex

		for _, p := range closest {
			if seen[p.NodeID] {
				continue
			}
			seen[p.NodeID] = true

			wg.Add(1)
			go func(peer PeerInfo) {
				defer wg.Done()

				// SendFindNodeAndWait registers a pending channel, sends the packet,
				// then waits up to 2 seconds for the real reply from the remote node.
				remotePeers := ic.SendFindNodeAndWait(peer.NodeID, target)

				if len(remotePeers) > 0 {
					fmt.Printf("[KADEMLIA] Got %d peers from %s reply\n", len(remotePeers), peer.NodeID)
					newPeersMu.Lock()
					newPeersFromNetwork = append(newPeersFromNetwork, remotePeers...)
					newPeersMu.Unlock()

					// Feed discovered peers into our routing table so they persist
					// beyond this lookup — this is how the table grows over time.
					for _, rp := range remotePeers {
						ic.SeedPeer(rp.NodeID, rp.IP, rp.Capabilities)
					}
				}
			}(p)
		}

		wg.Wait()

		// If the network gave us new peers, use those for the next iteration.
		// If not (e.g. all timeouts), escalate to supernodes.
		if len(newPeersFromNetwork) > 0 {
			closest = newPeersFromNetwork
			result = append(result, newPeersFromNetwork...)
		} else if len(ic.supernodes) > 0 {
			fmt.Println("[KADEMLIA] All queries timed out → escalating to supernodes")
			closest = ic.supernodes
			result = append(result, ic.supernodes...)
		} else {
			fmt.Println("[KADEMLIA] No replies and no supernodes — stopping lookup")
			break
		}
	}

	// Always include self so callers always get at least one result
	result = append(result, PeerInfo{
		NodeID:   ic.nodeID,
		IP:       "127.0.0.1",
		LastSeen: time.Now().Unix(),
		Score:    100.0,
	})

	fmt.Printf("[KADEMLIA] Lookup completed. Found %d peers\n", len(result))
	return result, nil
}
// innercore/rpc.go
// Kademlia RPC Layer — Full Implementation
//
// This file implements all core Kademlia RPCs for the InnerCore overlay:
// - PING / PONG → Liveness and health tracking
// - FIND_NODE → Iterative lookup (core routing primitive)
// - FIND_VALUE → Service & data lookup (used by Layer 2/3)
// - STORE → DHT storage with replication (future distributed storage)

// innercore/rpc.go
// Kademlia RPC Layer — Full Implementation

package innercore

import (
	"encoding/json"
	"fmt"
	"time"

	"innercore-network/network"
	"innercore-network/packet"
	"innercore-network/types"
)

// SendPing
func (ic *InnerCore) SendPing(target types.NodeID) {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
			PacketType:        packet.PacketTypeKademliaPing,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network:      packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Capabilities: types.Capabilities{},
		Payload:      mustMarshal(map[string]any{}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send PING to %s: %v\n", target, err)
	} else {
		fmt.Printf("[KADEMLIA] PING sent to %s\n", target)
	}
}

// SendFindNode fires a FIND_NODE packet without waiting for a reply.
func (ic *InnerCore) SendFindNode(target, lookupID types.NodeID) {
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID,
			PacketType:        packet.PacketTypeKademliaFindNode,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{
			"lookup_id": lookupID,
		}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE to %s: %v\n", target, err)
	} else {
		fmt.Printf("[KADEMLIA] FIND_NODE sent to %s looking for %s\n", target, lookupID)
	}
}

// SendFindNodeAndWait — (your existing implementation stays)
func (ic *InnerCore) SendFindNodeAndWait(target, lookupID types.NodeID) []PeerInfo {
	requestID := fmt.Sprintf("fnw-%d-%d", time.Now().UnixNano(), target[0])

	replyCh := ic.registerPendingLookup(requestID)

	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID,
			PacketType:        packet.PacketTypeKademliaFindNode,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{
			"lookup_id": lookupID,
		}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE to %s: %v\n", target, err)
		ic.cancelPendingLookup(requestID)
		return nil
	}

	fmt.Printf("[KADEMLIA] FIND_NODE sent to %s (requestID: %s), waiting for reply...\n", target, requestID)

	select {
	case peers := <-replyCh:
		fmt.Printf("[KADEMLIA] Got reply from %s with %d peers\n", target, len(peers))
		return peers
	case <-time.After(2 * time.Second):
		fmt.Printf("[KADEMLIA] Timeout waiting for FIND_NODE_REPLY from %s\n", target)
		ic.cancelPendingLookup(requestID)
		return nil
	}
}

// SendFindValue
func (ic *InnerCore) SendFindValue(target types.NodeID, key []byte) {
	fmt.Printf("[KADEMLIA] FIND_VALUE not fully implemented yet\n")
	ic.SendPing(target)
}

// ==================== REAL SendStore (NexusFabric) ====================

// SendStore sends a STORE request with proper payload for content fabric
func (ic *InnerCore) SendStore(target types.NodeID, key []byte, value any) error {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
			PacketType:        packet.PacketTypeKademliaStore,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{
			"key":   key,
			"value": value,
		}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send STORE to %s: %v\n", target, err)
		return err
	}

	fmt.Printf("[KADEMLIA] STORE sent to %s for key %x\n", target, key[:8])
	return nil
}

// HandleKademliaPacket — your existing version with minor robustness
func (ic *InnerCore) HandleKademliaPacket(pkt packet.Packet) {
	sender := pkt.Header.SourceNodeID
	ic.updatePeerLastSeen(sender)
	network.RecordSuccess(sender)

	switch pkt.Header.PacketType {
	case packet.PacketTypeKademliaPing:
		ic.SendPing(sender)

	case packet.PacketTypeKademliaFindNode:
		var payload map[string]any
		json.Unmarshal(pkt.Payload, &payload)

		var lookupID types.NodeID
		if raw, ok := payload["lookup_id"].([]interface{}); ok && len(raw) == 32 {
			for i, v := range raw {
				if num, ok := v.(float64); ok {
					lookupID[i] = byte(num)
				}
			}
		} else {
			fmt.Println("[KADEMLIA] Invalid lookup_id format")
			return
		}

		fmt.Printf("[KADEMLIA] Received FIND_NODE from %s looking for %s\n", sender, lookupID)

		closest := ic.getAlphaClosest(lookupID, 20)
		if len(closest) == 0 && len(ic.supernodes) > 0 {
			closest = ic.supernodes
		}

		ic.sendFindNodeReply(sender, pkt.Header.RequestID, closest)

	case "KADEMLIA_FIND_NODE_REPLY":
		fmt.Printf("[KADEMLIA] Received FIND_NODE_REPLY from %s (requestID: %s)\n", sender, pkt.Header.RequestID)

		var payload map[string]json.RawMessage
		if err := json.Unmarshal(pkt.Payload, &payload); err != nil {
			fmt.Printf("[KADEMLIA] Failed to parse FIND_NODE_REPLY payload: %v\n", err)
			ic.resolvePendingLookup(pkt.Header.RequestID, nil)
			return
		}

		peersRaw, ok := payload["peers"]
		if !ok {
			ic.resolvePendingLookup(pkt.Header.RequestID, nil)
			return
		}

		var peers []PeerInfo
		if err := json.Unmarshal(peersRaw, &peers); err != nil {
			fmt.Printf("[KADEMLIA] Failed to decode peers: %v\n", err)
			ic.resolvePendingLookup(pkt.Header.RequestID, nil)
			return
		}

		ic.resolvePendingLookup(pkt.Header.RequestID, peers)

	case packet.PacketTypeKademliaFindValue,
		packet.PacketTypeKademliaStore:
		// TODO: Handle incoming STORE/FIND_VALUE later (DHT node side)
		ic.SendPing(sender) // ACK for now

	default:
		fmt.Printf("[KADEMLIA] Unhandled packet type: %s\n", pkt.Header.PacketType)
	}
}

// sendFindNodeReply
func (ic *InnerCore) sendFindNodeReply(target types.NodeID, requestID string, peers []PeerInfo) {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID,
			PacketType:        "KADEMLIA_FIND_NODE_REPLY",
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{"peers": peers}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE_REPLY to %s\n", target)
	}
}

func (ic *InnerCore) updatePeerLastSeen(nodeID types.NodeID) {
	fmt.Printf("[KADEMLIA] Updated last seen for %s\n", nodeID)
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
// innercore/scoring.go
// Supernode Election & Scoring
//
// This implements the core design:
// Supernodes emerge naturally based on device capabilities.
// No voting. No central authority. Every node independently computes scores.

package innercore

import "innercore-network/types"

// calculateScore returns a fitness score for supernode candidacy.
// Higher score = better supernode candidate.
//
// The original requirements reflected here:
// - Cross-network bridge capability is heavily rewarded (the Africa use-case)
// - Bandwidth and uptime matter
// - Low latency is preferred
func calculateScore(caps types.Capabilities) float64 {
	score := 0.0

	// Bandwidth is very important for routing assistance
	score += float64(caps.UplinkBandwidthMbps) * 0.45

	// Uptime / stability (normalized)
	score += float64(caps.UptimeSeconds) * 0.00008 * 0.25

	// Cross-network bridge is the most important feature in the design
	if caps.CrossNetworkBridge {
		score += 65.0
	}
	if caps.HotspotCapable {
		score += 25.0
	}

	// Low latency is good for routing
	score -= float64(caps.LatencyMs) * 0.25

	// Internet access is a bonus but not required (per your correction)
	if caps.InternetAccess {
		score += 15.0
	}

	return score
}

/*
What this file does:

Pure scoring function used by every node to decide who its preferred supernodes are.
No global state — every node computes the same score from the same capabilities.
*/
// innercore/types.go
package innercore

import "innercore-network/types" // reuse NodeID and Capabilities from Layer 1

// PeerInfo represents a known peer in the routing table.
//
// JSON TAGS ARE REQUIRED HERE.
// Without them, Go uses the field name as-is (e.g. "NodeID", "IP").
// That works for round-trips within Go, but it's fragile and unreadable
// in logs/debug tools. We use snake_case to match the rest of the packet format.
//
// NOTE on NodeID JSON encoding:
// types.NodeID is [32]byte. Go's json package encodes [N]byte as a base64 string
// (not an array of numbers) because it treats []byte and [N]byte specially.
// This means: json.Marshal(NodeID{...}) → "base64string"
// And:        json.Unmarshal("base64string", &NodeID{}) → works correctly.
// So round-tripping NodeID through JSON is safe and compact.
type PeerInfo struct {
	NodeID       types.NodeID       `json:"node_id"`
	IP           string             `json:"ip"`
	Port         int                `json:"port"`
	Capabilities types.Capabilities `json:"capabilities"` //capabilities of the device whether it has access to the internet or another nexus nextwork
	LastSeen     int64              `json:"last_seen"`
	LatencyMs    int                `json:"latency_ms"`
	Score        float64            `json:"score"` // supernode fitness score
}

// KademliaKey is any 256-bit key (NodeID or hash of data/service)
type KademliaKey [32]byte

/*
What this file does:

Defines the data structures that every other file in innercore will use.
Re-uses NodeID and Capabilities from the types package so there is zero duplication.
JSON tags added so peers can be safely serialized/deserialized across the network
in FIND_NODE_REPLY packets.
*/
// integration/integration.go
// System Integration Layer
//
// This is the "brain" of the entire system.
// It initializes all layers in the correct order and starts background tasks.
// Provides a unified high-level API for the rest of the application.

// integration/integration.go
// System Integration Layer - The brain of the InnerCore Mesh

package integration

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/discovery"
	"innercore-network/fallback"
	"innercore-network/innercore"
	"innercore-network/message"
	"innercore-network/network"
	"innercore-network/resolver"
	"innercore-network/service"
	"innercore-network/storage"
	"innercore-network/types"
)

type SystemIntegrator struct {
	innerCore   *innercore.InnerCore
	resolver    *resolver.Layer2Resolver
	gateway     *fallback.InternetGateway
	running     bool
	startupTime time.Time
	mu          sync.RWMutex
}

var (
	integrator *SystemIntegrator
	once       sync.Once
)

func Init(instanceID int) {
	once.Do(func() {
		integrator = &SystemIntegrator{startupTime: time.Now()}

		fmt.Printf("\n=== Starting Local-First InnerCore Mesh - Instance %d (NexusFabric) ===\n", instanceID)

		storage.InitializeStorage()

		// === Multi-instance testing support ===
		discoveryPort := 37020
		messagePort := 51000 + instanceID*10

		discovery.Init([]string{"chat", "storage"}, 5000, discoveryPort, messagePort)

		integrator.innerCore = innercore.New(
			storage.GlobalStorage.GetEngine(),
			message.SendPacket,
		)

		integrator.innerCore.SetNodeID(discovery.GetMyNodeID())

		message.KademliaHandler = integrator.innerCore.HandleKademliaPacket

		discovery.UpdateNetworkCallback = func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int) {
			// Force real IP for local multi-instance testing
			if ip == "" || ip == "127.0.0.1" {
				ip = "192.168.2.104" // Change to your actual local IP if needed
			}
			network.UpdateNode(nodeID, ip, name, caps, services, servicePort)
			integrator.innerCore.SeedPeer(nodeID, ip, caps)
		}

		// === Self Registration with Full NexusFabric Capabilities ===
		selfID := discovery.GetMyNodeID()
		selfCaps := types.Capabilities{
			InternetAccess:      true,
			CrossNetworkBridge:  true,
			HotspotCapable:      true,
			UplinkBandwidthMbps: 120,
			LatencyMs:           5,
			UptimeSeconds:       7200,

			// === NexusFabric Storage Capabilities ===
			StorageTotalMB:     8192, // 8 GB
			StorageAvailableMB: 6144, // 6 GB free
			CanStoreChunks:     true,
			CanRelayTraffic:    true,
			StableNode:         true,
		}

		network.UpdateNode(selfID, "127.0.0.1", discovery.GetMyNodeName(), selfCaps, []string{"chat", "storage"}, 5000)
		integrator.innerCore.SeedPeer(selfID, "127.0.0.1", selfCaps)

		message.Init()
		service.Init()

		// Initialize Layer 3A + 3B + 3C
		service.SetOwnerID(selfID)
		service.InitDHTPublisher(integrator.innerCore)
		service.InitIndex()

		// Start background cleanup routines
		go service.StartProviderCleanup() // ← Fixed: exported name

		// Content Fabric is now live
		fmt.Println("[NEXUSFABRIC] ContentStore ready for hybrid storage")

		// Initialize NexusFabric DHT Publisher
		service.InitDHTPublisher(integrator.innerCore)

		integrator.resolver = resolver.NewLayer2Resolver()

		integrator.gateway = fallback.NewInternetGateway("0.0.0.0", 8080+instanceID, integrator.innerCore)
		integrator.gateway.Start()

		integrator.running = true

		fmt.Println("\n✅ System successfully started! (NexusFabric Ready)")
		fmt.Printf("   Instance         : %d\n", instanceID)
		fmt.Printf("   Node ID          : %s\n", selfID)
		fmt.Printf("   Node Name        : %s\n", discovery.GetMyNodeName())
		fmt.Printf("   Discovery Port   : %d\n", discoveryPort)
		fmt.Printf("   Message Port     : %d\n", messagePort)
		fmt.Printf("   Storage Available: %d MB\n", selfCaps.StorageAvailableMB)
		fmt.Println("   Status           : Operational")
	})
}

func GetInstance() *SystemIntegrator {
	return integrator
}

func GetInnerCore() *innercore.InnerCore {
	if integrator == nil {
		return nil
	}
	return integrator.innerCore
}

func (si *SystemIntegrator) Stop() {
	si.mu.Lock()
	defer si.mu.Unlock()

	if !si.running {
		return
	}

	fmt.Println("\n[SHUTDOWN] Graceful shutdown initiated...")
	si.running = false
	message.Close()
	fmt.Println("[SHUTDOWN] System stopped.")
}

// ==================== Public API ====================

func SendMessage(target types.NodeID, payload any) error {
	return message.SendToNode(target, payload)
}

func GetNetworkInfo() map[string]any {
	peers := network.GetAllPeers()
	alive := 0
	roles := make(map[string]int)
	services := make(map[string]int)

	for _, p := range peers {
		if p.Status == "alive" {
			alive++
		}
		roles[p.Role]++
		for _, svc := range p.Services {
			services[svc]++
		}
	}

	return map[string]any{
		"node_id":        discovery.GetMyNodeID(),
		"node_name":      discovery.GetMyNodeName(),
		"total_devices":  len(peers),
		"alive_devices":  alive,
		"roles":          roles,
		"services":       services,
		"uptime_seconds": int(time.Since(integrator.startupTime).Seconds()),
	}
}

func GetDeviceInfo(deviceID types.NodeID) any {
	return network.GetPeer(deviceID)
}

func RegisterService(serviceName string, port int) bool {
	fmt.Printf("[INTEGRATION] Service '%s' registered on port %d\n", serviceName, port)
	return true
}

func HealthCheck() map[string]any {
	peers := network.GetAllPeers()
	healthy := 0
	for _, p := range peers {
		if p.Status == "alive" && p.Health >= 0.5 {
			healthy++
		}
	}

	status := "DEGRADED"
	if integrator.running {
		status = "HEALTHY"
	}

	return map[string]any{
		"status": status,
		"metrics": map[string]any{
			"total_devices":   len(peers),
			"healthy_devices": healthy,
			"uptime_seconds":  int(time.Since(integrator.startupTime).Seconds()),
		},
	}
}
// main/main.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"innercore-network/discovery"
	"innercore-network/integration"
	"innercore-network/network"
	"innercore-network/service"
)

func main() {
	fmt.Println("🔥 InnerCore Mesh Network Starting...")
	fmt.Printf("Go Version: %s\n", "1.21+")

	// Initialize the entire system
	instanceID := 0
	// You can pass argument from command line later
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceID = 1
	}
	integration.Init(instanceID)

	// Show initial state
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("Local-First Internet Substrate - GO EDITION")
	fmt.Println(strings.Repeat("=", 60))

	// Print network state periodically
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			network.PrintNetworkState()
		}
	}()

	// === NexusFabric Content Tests ===
	go func() {
		time.Sleep(10 * time.Second)

		fmt.Println("\n=== NEXUSFABRIC CONTENT TEST ===")

		testData := []byte("Hello from InnerCore Mesh in Kenya! This is a test file.")
		result, err := service.StoreContent("test.txt", "text/plain", testData, []string{"test"}, []string{"kenya", "mesh"})
		if err != nil {
			fmt.Printf("Publish failed: %v\n", err)
		} else {
			fmt.Printf("✅ Published: %s (ID: %s)\n", result.Meta.Name, result.Meta.ContentID[:16]+"...")
		}

		results := service.Search("kenya")
		fmt.Printf("Search 'kenya' returned %d results\n", len(results))

		if result != nil {
			_, err = service.RetrieveContent(result.Meta.ContentID)
			if err != nil {
				fmt.Printf("Retrieval note: %v\n", err)
			}
		}
	}()

	// Test Kademlia lookup after discovery settles
	go func() {
		time.Sleep(12 * time.Second) // longer wait for discovery to stabilize

		if ic := integration.GetInnerCore(); ic != nil {
			selfID := discovery.GetMyNodeID()

			fmt.Println("\n[TEST] Triggering Kademlia self-lookup...")
			ic.TestLookup(selfID)

			fmt.Println("\n[TEST] Looking for other discovered peers via Kademlia...")

			// Retry a few times because discovery can be async
			for attempt := 0; attempt < 5; attempt++ {
				peers := network.GetAllPeers()
				fmt.Printf("[TEST] Attempt %d - Found %d total peers in table\n", attempt+1, len(peers))

				foundRemote := false
				for id := range peers {
					if !id.Equal(selfID) {
						fmt.Printf("[TEST] Triggering Kademlia lookup for remote peer: %s\n", id)
						ic.TestLookup(id)
						foundRemote = true
						break
					}
				}
				if foundRemote {
					break
				}
				time.Sleep(3 * time.Second)
			}

			if len(network.GetAllPeers()) <= 1 {
				fmt.Println("[TEST] No remote peer found yet. Discovery still settling...")
			}
		}
	}()
	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[SHUTDOWN] Received shutdown signal...")

	integration.GetInstance().Stop()

	fmt.Println("System shutdown complete. Goodbye.")
}
// message/message.go
// Layer 4 — Messaging Protocol (Reliable UDP with ACKs, Retries, and Persistence)
//
// This is the central transport layer. All layers (Discovery, InnerCore/Kademlia, Applications)
// send and receive through this single point.
// It merges your full reliable version with the new packet system.

package message

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"innercore-network/discovery"
	"innercore-network/network"
	"innercore-network/packet"
	"innercore-network/types"
)

const (
	LocalPort    = 51000
	ACKTimeout   = 2 * time.Second
	MaxRetries   = 3
	SaveInterval = 5 * time.Second
	PendingFile  = "pending_acks.json"
)

type pendingEntry struct {
	TargetID  types.NodeID    `json:"target_id"`
	TargetIP  string          `json:"target_ip"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
	Retries   int             `json:"retries"`
}

type MessageService struct {
	nodeID  types.NodeID
	conn    *net.UDPConn
	pending map[string]pendingEntry
	mu      sync.RWMutex
	// innerCore removed to avoid import cycle for now
}

var (
	msgService *MessageService
	once       sync.Once
)

var KademliaHandler func(packet.Packet)

func Init() error {
	once.Do(func() {
		service := &MessageService{
			nodeID:  discovery.GetMyNodeID(),
			pending: make(map[string]pendingEntry),
		}

		// Use configurable port from discovery package
		service.startUDPListener()

		msgService = service
		fmt.Printf("[MESSAGE] Layer 4 started on port %d (fire-and-forget mode for now)\n", discovery.LocalMessagePort)
	})
	return nil
}

func (m *MessageService) startUDPListener() {
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: discovery.LocalMessagePort}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on port %d: %v", discovery.LocalMessagePort, err))
	}
	m.conn = conn
	go m.receiveLoop()
}

func (m *MessageService) receiveLoop() {
	buf := make([]byte, 8192)
	for {
		n, addr, err := m.conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var pkt packet.Packet
		if json.Unmarshal(buf[:n], &pkt) != nil {
			continue
		}
		m.handleIncomingPacket(pkt, addr)
	}
}

func (m *MessageService) handleIncomingPacket(pkt packet.Packet, addr *net.UDPAddr) {
	senderID := pkt.Header.SourceNodeID
	network.RecordSuccess(senderID)

	switch pkt.Header.PacketType {
	case packet.PacketTypeDiscoveryAnnounce,
		packet.PacketTypeDiscoveryPing,
		packet.PacketTypeDiscoveryPong:
		fmt.Printf("[MESSAGE] Discovery packet %s from %s\n", pkt.Header.PacketType, senderID)

	case packet.PacketTypeKademliaPing,
		packet.PacketTypeKademliaPong,
		packet.PacketTypeKademliaFindNode,
		packet.PacketTypeKademliaFindValue,
		packet.PacketTypeKademliaStore,
		"KADEMLIA_FIND_NODE_REPLY": // ← Add this
		fmt.Printf("[MESSAGE] Kademlia %s from %s\n", pkt.Header.PacketType, senderID)
		if KademliaHandler != nil {
			KademliaHandler(pkt)
		}
		network.RecordSuccess(senderID)
		// health feedback

	case packet.PacketTypeMessage:
		m.handleApplicationMessage(pkt, senderID, addr)

	default:
		fmt.Printf("[MESSAGE] Unknown packet: %s\n", pkt.Header.PacketType)
	}
}

func (m *MessageService) handleApplicationMessage(pkt packet.Packet, senderID types.NodeID, addr *net.UDPAddr) {
	// Send ACK
	ack := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         pkt.Header.RequestID,
			PacketType:        "ACK",
			TTL:               1,
			SourceNodeID:      m.nodeID,
			DestinationNodeID: senderID,
			Timestamp:         time.Now().Unix(),
		},
	}
	m.sendRaw(ack, addr)

	fmt.Printf("[RECV] Message from %s\n", senderID)
}

func (m *MessageService) sendRaw(pkt packet.Packet, addr *net.UDPAddr) {
	data, _ := json.Marshal(pkt)
	m.conn.WriteToUDP(data, addr)
}

// SendPacket with real IP lookup
func SendPacket(targetID types.NodeID, pkt packet.Packet) error {
	peer := network.GetPeer(targetID)
	if peer == nil || peer.IP == "" {
		fmt.Printf("[SEND] CRITICAL: Target %s not found or no IP in network table!\n", targetID)
		return fmt.Errorf("target not found")
	}

	fmt.Printf("[SEND] → %s (%s) | Type: %s\n", targetID, peer.IP, pkt.Header.PacketType)

	addr := &net.UDPAddr{IP: net.ParseIP(peer.IP), Port: discovery.LocalMessagePort}
	data, _ := json.Marshal(pkt)
	_, err := msgService.conn.WriteToUDP(data, addr)
	if err != nil {
		fmt.Printf("[SEND] UDP write failed to %s: %v\n", peer.IP, err)
		network.RecordFailure(targetID)
		return err
	}

	network.RecordSuccess(targetID)
	return nil
}

func SendToNode(targetID types.NodeID, payload any) error {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
			PacketType:        packet.PacketTypeMessage,
			TTL:               8,
			SourceNodeID:      msgService.nodeID,
			DestinationNodeID: targetID,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(payload),
	}
	return SendPacket(targetID, p)
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func Close() {
	if msgService != nil && msgService.conn != nil {
		msgService.conn.Close()
	}
}
// network/network.go
package network

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

type PeerEntry struct {
	NodeID       types.NodeID
	Name         string
	IP           string
	Role         string
	RoleTrusted  bool
	Status       string
	Health       float64
	LastSeen     int64
	Services     []string
	ServicePort  int
	Capabilities types.Capabilities
	Score        float64
}

type NetworkTable struct {
	peers       map[types.NodeID]*PeerEntry
	mu          sync.RWMutex
	nodeTimeout time.Duration
}

var networkTable = &NetworkTable{
	peers:       make(map[types.NodeID]*PeerEntry),
	nodeTimeout: 15 * time.Second,
}

func UpdateNode(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	now := time.Now().Unix()
	entry, exists := networkTable.peers[nodeID]

	if !exists {
		entry = &PeerEntry{
			NodeID:       nodeID,
			Name:         name,
			IP:           ip,
			Role:         "unknown",
			RoleTrusted:  false,
			Status:       "alive",
			Health:       1.0,
			LastSeen:     now,
			Services:     services,
			ServicePort:  servicePort,
			Capabilities: caps,
			Score:        calculateScore(caps),
		}
		networkTable.peers[nodeID] = entry
		fmt.Printf("[NETWORK] New peer discovered: %s (%s) IP: %s\n", name, nodeID, ip)
		return
	}

	// FORCE IP UPDATE - this was the missing piece
	if ip != "" && ip != "127.0.0.1" {
		entry.IP = ip
	}
	entry.Name = name
	entry.LastSeen = now
	entry.Status = "alive"
	entry.Health = 1.0
	entry.Capabilities = caps
	entry.Score = calculateScore(caps)

	if len(services) > 0 {
		entry.Services = services
		entry.ServicePort = servicePort
	}

	fmt.Printf("[NETWORK] Updated peer: %s (%s) IP: %s Health: %.1f\n", name, nodeID, entry.IP, entry.Health)
}

func GetPeer(nodeID types.NodeID) *PeerEntry {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()
	return networkTable.peers[nodeID]
}

func GetAllPeers() map[types.NodeID]*PeerEntry {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()
	copy := make(map[types.NodeID]*PeerEntry, len(networkTable.peers))
	for k, v := range networkTable.peers {
		copy[k] = v
	}
	return copy
}

func ExpireStaleNodes() {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	now := time.Now().Unix()
	for _, entry := range networkTable.peers {
		if time.Duration(now-entry.LastSeen)*time.Second > networkTable.nodeTimeout {
			entry.Status = "dead"
		}
	}
}

func RecordSuccess(nodeID types.NodeID) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	if entry, ok := networkTable.peers[nodeID]; ok {
		entry.Health = min(1.0, entry.Health+0.1)
		entry.LastSeen = time.Now().Unix()
		fmt.Printf("[HEALTH] Node %s health +0.1 → %.1f\n", nodeID, entry.Health)
	}
}

func RecordFailure(nodeID types.NodeID) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	if entry, ok := networkTable.peers[nodeID]; ok {
		entry.Health = max(0.0, entry.Health-0.3)
		if entry.Health <= 0.3 {
			entry.Status = "unhealthy"
		}
		fmt.Printf("[HEALTH] Node %s health -0.3 → %.1f\n", nodeID, entry.Health)
	}
}

func PrintNetworkState() {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()

	fmt.Println("\n--- NETWORK STATE ---")
	for nodeID, info := range networkTable.peers {
		displayName := info.Name
		if displayName == "" {
			displayName = nodeID.String() + "..." // Fixed: use .String()
		}
		servicesStr := ""
		if len(info.Services) > 0 {
			servicesStr = fmt.Sprintf(", Services: %v", info.Services)
		}

		fmt.Printf("ID: %s | Name: %s | IP: %s | Role: %s | Status: %s | Health: %.1f%s\n",
			nodeID, displayName, info.IP, info.Role, info.Status, info.Health, servicesStr)
	}
	fmt.Println("----------------------")
}

// Local scoring function (moved from innercore/scoring.go to avoid import)
func calculateScore(caps types.Capabilities) float64 {
	score := 0.0
	score += float64(caps.UplinkBandwidthMbps) * 0.45
	score += float64(caps.UptimeSeconds) * 0.00008 * 0.25

	if caps.CrossNetworkBridge {
		score += 65.0
	}
	if caps.HotspotCapable {
		score += 25.0
	}

	score -= float64(caps.LatencyMs) * 0.25

	if caps.InternetAccess {
		score += 15.0
	}
	return score
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
// packet/packet.go
package packet

import (
	"encoding/json"

	"innercore-network/types"
)

type Packet struct {
	Header       Header             `json:"header"`
	Network      NetworkInfo        `json:"network"`
	Capabilities types.Capabilities `json:"capabilities"`
	Payload      json.RawMessage    `json:"payload"`
}

type Header struct {
	Version           int          `json:"version"`
	RequestID         string       `json:"request_id"`
	PacketType        string       `json:"packet_type"`
	TTL               int          `json:"ttl"`
	SourceNodeID      types.NodeID `json:"source_node_id"`
	DestinationNodeID types.NodeID `json:"destination_node_id"`
	Timestamp         int64        `json:"timestamp"`
	PayloadLength     int          `json:"payload_length"`
}

type NetworkInfo struct {
	SourceRegion      string `json:"source_region"`
	DestinationRegion string `json:"destination_region"`
}

const (
	PacketTypeDiscoveryPing     = "DISCOVERY_PING"
	PacketTypeDiscoveryPong     = "DISCOVERY_PONG"
	PacketTypeDiscoveryAnnounce = "DISCOVERY_ANNOUNCE"

	PacketTypeKademliaPing      = "KADEMLIA_PING"
	PacketTypeKademliaPong      = "KADEMLIA_PONG"
	PacketTypeKademliaFindNode  = "KADEMLIA_FIND_NODE"
	PacketTypeKademliaFindValue = "KADEMLIA_FIND_VALUE"
	PacketTypeKademliaStore     = "KADEMLIA_STORE"

	PacketTypeMessage = "MSG"
)
// resolver/resolver.go
// Layer 2 — Local Name Resolution Protocol (.mtd)
//
// Purpose:
//   Provides a DNS-like resolution mechanism for the local mesh network.
//   Resolves human-readable `.mtd` names into concrete network endpoints
//   (devices or services).
//
// Design Principles (Strictly followed):
//   - READ-ONLY
//   - SIDE-EFFECT FREE
//   - CACHED
//   - DETERMINISTIC
//   - NEVER registers devices, modifies network table, does routing, or health checks
//
// Resolution Outcomes (Strict Contract):
//   - "OK"        → Valid resolution
//   - "NX"        → Name does not exist
//   - "CONFLICT"  → Ambiguous resolution
//
// Used By:
//   - Layer 3 (Service Protocol)
//   - Layer 4 (Messaging)
//   - Applications

package resolver

import (
	"strings"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
	// for future service resolution
)

const (
	MTDSuffix     = ".mtd"
	ServicePrefix = "svc."
	CacheTTL      = 30 * time.Second
)

// ResolutionRecord is the standard response format from Layer 2
type ResolutionRecord struct {
	Status   string     `json:"status"` // "OK", "NX", "CONFLICT"
	Type     string     `json:"type"`   // "device" or "service"
	Name     string     `json:"name"`
	Records  []Endpoint `json:"records"`
	TTL      int        `json:"ttl"`
	CachedAt time.Time  `json:"cached_at"`
}

type Endpoint struct {
	DeviceID        types.NodeID   `json:"device_id"`
	Name            string         `json:"name"`
	IP              string         `json:"ip"`
	Port            int            `json:"port"`
	Role            string         `json:"role"`
	RoleTrusted     bool           `json:"role_trusted"`
	Health          float64        `json:"health"`
	ServiceMetadata map[string]any `json:"service_metadata,omitempty"`
}

// ResolutionCache is thread-safe and auto-evicts stale entries
type ResolutionCache struct {
	cache map[string]ResolutionRecord
	mu    sync.RWMutex
}

func NewResolutionCache() *ResolutionCache {
	return &ResolutionCache{
		cache: make(map[string]ResolutionRecord),
	}
}

func (c *ResolutionCache) Get(key string) *ResolutionRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	record, exists := c.cache[key]
	if !exists {
		return nil
	}

	if time.Since(record.CachedAt) > CacheTTL {
		// Lazy eviction
		c.mu.RUnlock()
		c.Delete(key)
		c.mu.RLock()
		return nil
	}

	return &record
}

func (c *ResolutionCache) Set(key string, record ResolutionRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.CachedAt = time.Now()
	c.cache[key] = record
}

func (c *ResolutionCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, key)
}

// Layer2Resolver is the main resolver
type Layer2Resolver struct {
	cache *ResolutionCache
}

func NewLayer2Resolver() *Layer2Resolver {
	return &Layer2Resolver{
		cache: NewResolutionCache(),
	}
}

// Resolve is the ONLY public API
func (r *Layer2Resolver) Resolve(name string) ResolutionRecord {
	key := strings.ToLower(strings.TrimSpace(name))

	if cached := r.cache.Get(key); cached != nil {
		return *cached
	}

	record := r.resolveAndCache(key)
	return record
}

// Internal resolution logic
func (r *Layer2Resolver) resolveAndCache(name string) ResolutionRecord {
	// Validate suffix
	if !strings.HasSuffix(name, MTDSuffix) {
		record := r.nxRecord(name)
		r.cache.Set(name, record)
		return record
	}

	// Service resolution
	if strings.HasPrefix(name, ServicePrefix) {
		record := r.resolveService(name)
		r.cache.Set(name, record)
		return record
	}

	// Device resolution
	record := r.resolveDevice(name)
	r.cache.Set(name, record)
	return record
}

// Device resolution: e.g. laptop.mtd
func (r *Layer2Resolver) resolveDevice(name string) ResolutionRecord {
	hostname := strings.TrimSuffix(name, MTDSuffix)

	matches := []Endpoint{}
	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		if peer.Name == hostname && peer.Status == "alive" {
			matches = append(matches, Endpoint{
				DeviceID:    nodeID,
				Name:        peer.Name,
				IP:          peer.IP,
				Port:        51000, // standard messaging port
				Role:        peer.Role,
				RoleTrusted: peer.RoleTrusted,
				Health:      peer.Health,
			})
		}
	}

	if len(matches) == 0 {
		return r.nxRecord(name)
	}
	if len(matches) > 1 {
		return r.conflictRecord(name, "device", matches)
	}

	return r.okRecord(name, "device", matches)
}

// Service resolution: e.g. svc.chat.mtd
func (r *Layer2Resolver) resolveService(name string) ResolutionRecord {
	service := strings.TrimPrefix(name, ServicePrefix)
	service = strings.TrimSuffix(service, MTDSuffix)

	// TODO: Call Layer 3 service registry when it's ported
	// For now we return NX
	return r.nxRecord(name)
}

// Record builders
func (r *Layer2Resolver) okRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "OK",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) nxRecord(name string) ResolutionRecord {
	return ResolutionRecord{
		Status:   "NX",
		Type:     "",
		Name:     name,
		Records:  nil,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) conflictRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "CONFLICT",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}
// resolver/role_routing.go
// Layer 2 Helper — Role-Based Routing
//
// This is a convenience wrapper around Layer 4 messaging.
// It allows applications to send messages to all nodes that match a specific role
// (e.g. "game", "chat", "storage") without having to iterate the network table manually.
//
// Key Features:
// - Filters by role, alive status, health (>= 0.5), and trusted role
// - Uses Layer 4's reliable messaging (ACKs, retries, health tracking)
// - Returns clear summary of sent vs failed messages
// - No direct UDP — everything goes through message.SendPacket

package resolver

import (
	"fmt"

	"innercore-network/message"
	"innercore-network/network"
)

// SendToRole sends a message to ALL alive, healthy, trusted nodes with the given role.
//
// Parameters:
//
//	role         - target role (e.g. "game", "chat", "storage")
//	messageType  - logical message category (e.g. "GAME_STATE", "CHAT_MESSAGE")
//	content      - actual payload (any Go type)
//	senderName   - human-readable name of the sender
//
// Returns:
//
//	Summary map with sent_count, failed_count, target_nodes, failed_nodes
func SendToRole(role string, messageType string, content any, senderName string) map[string]any {
	results := map[string]any{
		"sent_count":   0,
		"failed_count": 0,
		"target_nodes": []map[string]any{},
		"failed_nodes": []map[string]any{},
	}

	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		// Strict filtering - same rules as your original Python version
		if peer.Role != role {
			continue
		}
		if peer.Status != "alive" {
			continue
		}
		if peer.Health < 0.5 {
			continue
		}
		if !peer.RoleTrusted {
			continue
		}

		// Build payload for Layer 4
		payload := map[string]any{
			"protocol_layer": 2,
			"routing_type":   "role_based",
			"message_type":   messageType,
			"from_name":      senderName,
			"to_role":        role,
			"content":        content,
		}

		// Send through Layer 4 (reliable path)
		err := message.SendToNode(nodeID, payload)

		if err == nil {
			results["sent_count"] = results["sent_count"].(int) + 1
			results["target_nodes"] = append(results["target_nodes"].([]map[string]any), map[string]any{
				"device_id": nodeID,
				"name":      peer.Name,
				"ip":        peer.IP,
			})
		} else {
			results["failed_count"] = results["failed_count"].(int) + 1
			results["failed_nodes"] = append(results["failed_nodes"].([]map[string]any), map[string]any{
				"device_id": nodeID,
				"name":      peer.Name,
				"error":     err.Error(),
			})
		}
	}

	fmt.Printf("[ROLE ROUTING] Sent '%s' to %d '%s' nodes (%d failed)\n",
		messageType, results["sent_count"], role, results["failed_count"])

	return results
}

// GetRoleMembers returns all nodes with a specific role (for application use)
func GetRoleMembers(role string, requireAlive bool, minHealth float64) []map[string]any {
	members := []map[string]any{}

	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		if peer.Role != role {
			continue
		}
		if requireAlive && peer.Status != "alive" {
			continue
		}
		if peer.Health < minHealth {
			continue
		}
		if !peer.RoleTrusted {
			continue
		}

		members = append(members, map[string]any{
			"device_id": nodeID,
			"name":      peer.Name,
			"ip":        peer.IP,
			"role":      peer.Role,
			"health":    peer.Health,
			"status":    peer.Status,
			"last_seen": peer.LastSeen,
		})
	}

	return members
}

// BroadcastToAllRoles sends a message to nodes of ALL allowed roles (except excluded ones)
// Useful for system-wide announcements
func BroadcastToAllRoles(messageType string, content any, senderName string, excludedRoles []string) map[string]any {
	if excludedRoles == nil {
		excludedRoles = []string{}
	}

	results := map[string]any{
		"total_sent":   0,
		"total_failed": 0,
		"by_role":      map[string]map[string]any{},
	}

	// You can define allowed roles here or import from a config
	allowedRoles := []string{"game", "chat", "cache", "storage"} // adjust as needed

	for _, role := range allowedRoles {
		if contains(excludedRoles, role) {
			continue
		}

		roleResult := SendToRole(role, messageType, content, senderName)
		results["by_role"].(map[string]map[string]any)[role] = roleResult

		results["total_sent"] = results["total_sent"].(int) + roleResult["sent_count"].(int)
		results["total_failed"] = results["total_failed"].(int) + roleResult["failed_count"].(int)
	}

	fmt.Printf("[BROADCAST] Sent to %d total nodes across %d roles (%d total failures)\n",
		results["total_sent"], len(allowedRoles), results["total_failed"])

	return results
}

// Small helper
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
// types/types.go
package types

import "fmt"

type NodeID [32]byte

func (id NodeID) String() string {
	if len(id) == 0 {
		return "00000000..."
	}
	return fmt.Sprintf("%x...", id[:8])
}

func (id NodeID) Equal(other NodeID) bool {
	return id == other
}

type Capabilities struct {
	InternetAccess      bool  `json:"internet_access"`
	CrossNetworkBridge  bool  `json:"cross_network_bridge"`
	HotspotCapable      bool  `json:"hotspot_capable"`
	UplinkBandwidthMbps int   `json:"uplink_bandwidth_mbps"`
	LatencyMs           int   `json:"latency_ms"`
	UptimeSeconds       int64 `json:"uptime_seconds"`

	//Capabilities Stretch for Storage Layer
	StorageTotalMB     int64 `json:"storage_total_mb"`
	StorageAvailableMB int64 `json:"storage_available_mb"`
	CanStoreChunks     bool  `json:"can_store_chunks"`
	CanRelayTraffic    bool  `json:"can_relay_traffic"`
	StableNode         bool  `json:"stable_node"`
}
// storage/device_registry.go
// Layer 5 Extension — Device Registry
//
// Authoritative persistent device identity storage.
// Used as source of truth for Layer 3 and system recovery.

// storage/device_registry.go
package storage

import (
	"innercore-network/types"
	"time"
)

const DeviceCollection = "devices"

type DeviceRegistry struct {
	storage *StorageEngine
}

func NewDeviceRegistry(storage *StorageEngine) *DeviceRegistry {
	return &DeviceRegistry{storage: storage}
}

func (dr *DeviceRegistry) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	payload := map[string]any{
		"device_id":  deviceID,
		"first_seen": time.Now().Unix(),
		"public_key": info["public_key"],
		"roles":      info["roles"],
		"metadata":   info["metadata"],
	}

	// Fixed: use nodeID.String() instead of string(deviceID)
	return dr.storage.Put(DeviceCollection, deviceID.String(), payload, nil)
}

func (dr *DeviceRegistry) GetDevice(deviceID types.NodeID) (Record, error) {
	return dr.storage.Get(DeviceCollection, deviceID.String())
}

func (dr *DeviceRegistry) DeviceExists(deviceID types.NodeID) bool {
	_, err := dr.GetDevice(deviceID)
	return err == nil
}
// storage/facade.go
// Layer 5 — Unified Storage Facade
//
// Single API for ALL persistence needs across layers.
// Uses StorageEngine as backend and provides high-level methods.

package storage

import (
	"fmt"

	"innercore-network/types"
)

type StorageFacade struct {
	engine         *StorageEngine
	networkStore   *NetworkSnapshotStore
	deviceRegistry *DeviceRegistry
	initialized    bool
}

var GlobalStorage = NewStorageFacade()

func NewStorageFacade() *StorageFacade {
	engine := NewStorageEngine()
	return &StorageFacade{
		engine:         engine,
		networkStore:   NewNetworkSnapshotStore(engine),
		deviceRegistry: NewDeviceRegistry(engine),
	}
}

func (f *StorageFacade) Initialize() bool {
	if f.initialized {
		return true
	}
	fmt.Println("[STORAGE] Layer 5 Storage Facade initialized")
	f.initialized = true
	return true
}

// Network State
func (f *StorageFacade) SaveNetworkState(networkTable map[types.NodeID]any) (Record, error) {
	return f.networkStore.SaveSnapshot(networkTable)
}

func (f *StorageFacade) LoadNetworkState() (map[types.NodeID]any, error) {
	rec, err := f.networkStore.LoadLatestSnapshot()
	if err != nil {
		return nil, err
	}
	if nt, ok := rec.Payload["network_table"].(map[types.NodeID]any); ok {
		return nt, nil
	}
	return nil, fmt.Errorf("invalid network table format")
}

// Device Registry
func (f *StorageFacade) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	return f.deviceRegistry.RegisterDevice(deviceID, info)
}

func (f *StorageFacade) GetDevice(deviceID types.NodeID) (Record, error) {
	return f.deviceRegistry.GetDevice(deviceID)
}

// Convenience exports
func InitializeStorage() bool {
	return GlobalStorage.Initialize()
}

func SaveNetworkState(nt map[types.NodeID]any) (Record, error) {
	return GlobalStorage.SaveNetworkState(nt)
}

func LoadNetworkState() (map[types.NodeID]any, error) {
	return GlobalStorage.LoadNetworkState()
}

// GetEngine returns the underlying StorageEngine (used by InnerCore)
func (f *StorageFacade) GetEngine() *StorageEngine {
	return f.engine
}
// storage/network_snapshot.go
// Layer 5 Extension — Network Snapshot Store
//
// Persists last-known network state for warm restarts.

package storage

import (
	"innercore-network/types"
	"time"
)

const (
	SnapshotCollection = "network_snapshots"
	LatestSnapshotID   = "latest"
)

type NetworkSnapshotStore struct {
	storage *StorageEngine
}

func NewNetworkSnapshotStore(storage *StorageEngine) *NetworkSnapshotStore {
	return &NetworkSnapshotStore{storage: storage}
}

func (ns *NetworkSnapshotStore) SaveSnapshot(networkTable map[types.NodeID]any) (Record, error) {
	payload := map[string]any{
		"timestamp":     time.Now().Unix(),
		"device_count":  len(networkTable),
		"network_table": networkTable,
	}

	return ns.storage.Put(SnapshotCollection, LatestSnapshotID, payload, nil)
}

func (ns *NetworkSnapshotStore) LoadLatestSnapshot() (Record, error) {
	return ns.storage.Get(SnapshotCollection, LatestSnapshotID)
}

func (ns *NetworkSnapshotStore) SnapshotExists() bool {
	_, err := ns.LoadLatestSnapshot()
	return err == nil
}
// storage/storage_engine.go
// Layer 5 — Storage Protocol (Core)
//
// Responsibilities:
//   - Authoritative persistence
//   - Versioned record storage
//   - Thread-safe read/write with optimistic concurrency control
//   - NO routing, NO caching, NO business logic
//
// This is the foundation used by all other storage extensions.

package storage

import (
	"sync"
	"time"
)

// StorageError types
type StorageError string

const (
	ErrRecordNotFound  StorageError = "record not found"
	ErrVersionConflict StorageError = "version conflict"
)

func (e StorageError) Error() string { return string(e) }

// Record is the canonical versioned storage record
type Record struct {
	ID        string         `json:"id"`
	Version   int            `json:"version"`
	Payload   map[string]any `json:"payload"`
	CreatedAt int64          `json:"created_at"`
}

// StorageEngine is the thread-safe in-memory authoritative engine
type StorageEngine struct {
	// store[collection][recordID][version] = Record
	store map[string]map[string]map[int]Record
	mu    sync.RWMutex
}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{
		store: make(map[string]map[string]map[int]Record),
	}
}

// Get returns the latest version of a record
func (s *StorageEngine) Get(collection, recordID string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	coll, exists := s.store[collection]
	if !exists {
		return Record{}, ErrRecordNotFound
	}

	versions, exists := coll[recordID]
	if !exists || len(versions) == 0 {
		return Record{}, ErrRecordNotFound
	}

	// Find latest version
	var latest Record
	maxVer := -1
	for ver, rec := range versions {
		if ver > maxVer {
			maxVer = ver
			latest = rec
		}
	}
	return latest, nil
}

// Put inserts or updates a record with optimistic locking
func (s *StorageEngine) Put(collection, recordID string, payload map[string]any, expectedVersion *int) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.store[collection]; !exists {
		s.store[collection] = make(map[string]map[int]Record)
	}
	if _, exists := s.store[collection][recordID]; !exists {
		s.store[collection][recordID] = make(map[int]Record)
	}

	versions := s.store[collection][recordID]
	currentVersion := 0
	if len(versions) > 0 {
		for v := range versions {
			if v > currentVersion {
				currentVersion = v
			}
		}
	}

	if expectedVersion != nil && *expectedVersion != currentVersion {
		return Record{}, ErrVersionConflict
	}

	newVersion := currentVersion + 1
	record := Record{
		ID:        recordID,
		Version:   newVersion,
		Payload:   payload,
		CreatedAt: time.Now().Unix(),
	}

	s.store[collection][recordID][newVersion] = record
	return record, nil
}

// Delete removes a record with version check
func (s *StorageEngine) Delete(collection, recordID string, expectedVersion *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	coll, exists := s.store[collection]
	if !exists {
		return ErrRecordNotFound
	}

	versions, exists := coll[recordID]
	if !exists || len(versions) == 0 {
		return ErrRecordNotFound
	}

	currentVersion := 0
	for v := range versions {
		if v > currentVersion {
			currentVersion = v
		}
	}

	if expectedVersion != nil && *expectedVersion != currentVersion {
		return ErrVersionConflict
	}

	delete(coll, recordID)
	if len(coll) == 0 {
		delete(s.store, collection)
	}
	return nil
}
