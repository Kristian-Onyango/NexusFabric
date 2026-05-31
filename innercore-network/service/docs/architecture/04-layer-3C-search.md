---
title: Layer 3C — Metadata Indexing & Search
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
files: index_types.go, tokenizer.go, index_writer.go, index_reader.go
---

# <span style="color: #3498db;">Layer 3C — Metadata Indexing & Search</span>

Layer 3C answers a completely different question from Layer 3B:
- **Layer 3B** answers: "retrieve bytes by their hash"
- **Layer 3C** answers: "find content by human-readable text"

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Mental Model</span>

Think of it as the index at the back of a book:

| Component | Analogy |
|-----------|---------|
| The book (content) | Layer 3B |
| The index at the back | Layer 3C |

**Example flow:**
```
"Suits S01E01.mp4" is published →
Tokenizer extracts: ["suits", "s01e01", "1080p", "bluray"]
For each token, stored in DHT:
/index/suits → [{ContentID: "abc", Name: "Suits S01E01.mp4", ...}]
/index/s01e01 → [{ContentID: "abc", ...}]

User searches "suits" →
Tokenize "suits" → ["suits"]
Fetch /index/suits from DHT → entries
Score and rank → return SearchResponse
```

  

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Types</span>

### `IndexEntry`

One item in a keyword → content mapping.

```go
type IndexEntry struct {
    ContentID       string
    Name            string   // denormalized — avoids extra DHT fetch for display
    Type            string   // denormalized
    SizeBytes       int64    // denormalized
    Tags            []string
    IndexedAt       int64    // unix timestamp
    UpdatedAt       int64
    MatchWeight     float64  // 1.0 = filename match, 0.8 = tag, 0.5 = keyword
    PublisherNodeID string
}
```
```IndexBucket```

The full value stored at one DHT key. One bucket per keyword, holding all content that matches.

```go
type IndexBucket struct {
    Keyword   string       // normalized keyword
    Entries   []IndexEntry // all matching content
    UpdatedAt int64
}
```
```SearchQuery```

```go
type SearchQuery struct {
    RawQuery     string // "Suits Season 1" — tokenizer processes this
    TypeFilter   string // optional: "video", "image", etc. — empty = all types
    MaxSizeBytes int64  // optional: 0 = no limit
    MaxResults   int    // 0 defaults to 20
    Page         int    // 0-based pagination
}
```
```SearchResponse```
```go
type SearchResponse struct {
    Query      string         // original raw query
    Tokens     []string       // what the tokenizer extracted
    Results    []SearchResult // ranked results, best first
    TotalFound int            // before pagination
    Page       int
    TookMs     int64          // search latency in milliseconds
    FromCache  bool           // true if served from local cache
}
```

```SearchResult```
```go
type SearchResult struct {
    ContentID     string
    Name          string
    Type          string
    SizeBytes     int64
    Tags          []string
    Score         float64   // 0.0 → 1.0, higher = better match
    MatchedTokens []string  // which query tokens matched this result
    IndexedAt     time.Time
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> The Tokenizer</span>

The tokenizer bridges human language and machine-readable DHT keys. Without it, ```"Suits.S01E03.1080p.BluRay.mkv"``` and the query ```"suits"``` would share zero literal characters and never match.

```go
tokenizer := service.NewTokenizer()
```

Normalization Pipeline
Step	Input	Output
Input	"Suits.S01E03.1080p.BluRay.mkv"	-
Lowercase	-	"suits.s01e03.1080p.bluray.mkv"
Split on [^a-z0-9]+	-	["suits", "s01e03", "1080p", "bluray", "mkv"]
Drop empties	-	(none)
Drop len < 2	-	(none)
Drop stopwords	-	(none)
Drop extensions	-	["suits", "s01e03", "1080p", "bluray"]
Deduplicate	-	(no dupes)

Output: ```["suits", "s01e03", "1080p", "bluray"]```


<hr style="border: 1px solid #ecf0f1;">

<h3>Token Weights by Source</h3>

When tokenizing a ContentMeta, each source field produces tokens at a different weight:

Source	Weight	Reason
Filename (no extension)	1.0	The primary identifier — strongest signal
Tags	0.8	User-defined labels — high value
Keywords	0.5	Secondary metadata
File type	0.3	Enables type-based filtering
If a token appears in multiple sources, the highest weight wins.

<hr style="border: 1px solid #ecf0f1;">
<h3>Key Tokenizer Methods</h3>

```go
// Primary: tokenize a full ContentMeta (all fields, with weights)
tokens := tokenizer.TokenizeContentMeta(meta) // []WeightedToken

// Tokenize just a filename (strips extension, normalizes)
tokens := tokenizer.TokenizeFilename("Suits.S01E03.mkv") // ["suits", "s01e03"]

// Tokenize a user search query (same normalization as filenames)
tokens := tokenizer.TokenizeQuery("suits season 1") // ["suits", "season", "1"]

// Infer file type from extension
ftype := service.DetectFileType("movie.mp4") // "video"

// Extract TV episode code
code, ok := service.ExtractEpisodeCode("Suits.S01E03.mkv") // "s01e03", true

// Extract year from filename
year, ok := service.ExtractYear("The.Dark.Knight.2008.mkv") // "2008", true

// Check if a token is a technical quality tag
is := service.IsQualityTag("1080p") // true
```

<hr style="border: 1px solid #ecf0f1;">

<h3>Safety Limits</h3>

Limit	Value	Reason
MinTokenLength	2	Tokens shorter than 2 characters are discarded
MaxTokensPerField	50	Prevents DHT flooding from malicious input

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Index Writer</span>

```IndexWriter``` builds and maintains the distributed index when content is published.

```go
writer := service.NewIndexWriter(ownerNodeID)
```

```Index()``` — the main entry point
Call this immediately after a successful ```ContentStore.Store():```

```go
writeRecords := writer.Index(meta) // []IndexWriteRecord
```
Returns one IndexWriteRecord per unique token extracted from the content. A file producing 12 tokens generates 12 write records.

```go
type IndexWriteRecord struct {
    DHTKey    types.NodeID // result of IndexKey(keyword) — pass to InnerCore.SendStore()
    Keyword   string       // human-readable keyword (for logging)
    Entry     IndexEntry   // the index entry to store at this key
    BucketKey string       // full key string e.g. "/index/suits"
}
```
The caller distributes these to the DHT:

```go
writeRecords := writer.Index(storeResult.Meta)
for _, rec := range writeRecords {
    innerCore.SendStore(rec.DHTKey, rec.Entry)
    indexReader.FeedEntry(rec.Keyword, rec.Entry) // also update local cache
}
```

``` Remove()``` — stop tracking content
```go
writer.Remove(contentID)
```
This removes the content from local tracking. The DHT entries are not actively deleted (Kademlia has no native delete operation). Instead, the node stops re-announcing, and the entries expire naturally after ```IndexEntryTTL = 30 minutes```.

<hr style="border: 1px solid #ecf0f1;">
<h3>Re-announcement System</h3>

DHT values have a TTL. The writer tracks everything it has indexed and periodically re-publishes entries to keep them alive:

```go
// Get write records for content that needs re-announcing
records := writer.GetReAnnounceRecords(contentStore) // []IndexWriteRecord

// Start the automatic background re-announcement loop
go writer.StartMaintenanceLoop(contentStore, func(records []IndexWriteRecord) {
    for _, r := range records {
        innerCore.SendStore(r.DHTKey, r.Entry)
    }
})
```

Timeline:

t=0: Content indexed, entries stored to DHT

t=20m: ```ReAnnounceInterval``` fires → entries re-stored (still 10m left on TTL)

t=30m: ```IndexEntryTTL``` would expire, but re-announce already happened

t=40m: ```ReAnnounceInterval``` fires again → cycle continues

<hr style="border: 1px solid #ecf0f1;">

<h3>Writer Stats</h3>

```go
stats := writer.GetStats()
// IndexStats{
//   TotalKeywords: int,   // unique tokens tracked
//   TotalEntries:  int,   // total content-keyword pairs
//   LocallyOwned:  int,   // content items we originally published
//   LastWriteAt:   int64, // unix timestamp of last index write
// }

ids := writer.GetLocalContentIDs() // []string — all content IDs being tracked
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Index Reader</span>

``` IndexReader``` executes searches. It maintains a local cache of index buckets fetched from the DHT.

```go
reader := service.NewIndexReader()
```
<h3>Two-Phase Search Pattern</h3>

The reader never does DHT I/O directly. Instead, it uses a two-phase approach:

Phase 1: Try local cache

```go
resp := reader.SearchLocal(query)
if resp.TotalFound > 0 {
    return resp // served instantly from cache
}
```

Phase 2: Fetch from DHT, then search again

```go
// Find out which DHT keys need to be fetched
requests := reader.GetDHTKeysForQuery(query) // []DHTLookupRequest

for _, req := range requests {
    // Do the DHT lookup (caller's responsibility)
    bucket := innerCore.FindValue(req.DHTKey)

    // Feed the result back into the reader's cache
    reader.FeedBucket(req.Keyword, bucket)
}

// Now search again with populated cache
resp = reader.SearchLocal(query)
```
```SearchLocal()``` — full search with scoring and pagination

```go
query := service.SearchQuery{
    RawQuery:   "suits season 1",
    TypeFilter: "video",    // optional
    MaxResults: 20,
    Page:       0,
}

resp := reader.SearchLocal(query)
// resp.Results → []SearchResult sorted by Score descending
// resp.TotalFound → total before pagination
// resp.TookMs → latency in milliseconds
// resp.FromCache → true (SearchLocal only reads cache)
```
<hr style="border: 1px solid #ecf0f1;">

<h3>Scoring System</h3>

Each result is scored on a scale of 0.0 → 1.0 using four additive signals:

<table>
    <thead>
        <tr>
            <th>Signal</th>
            <th>Weight</th>
            <th>Formula</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Token match strength</td>
            <td>0.40</td>
            <td>Average MatchWeight of matched tokens</td>
        </tr>
        <tr>
            <td>Token coverage</td>
            <td>0.30</td>
            <td>matched_tokens / total_query_tokens</td>
        </tr>
        <tr>
            <td>Recency</td>
            <td>0.20</td>
            <td>max(0, 1 - age_in_days / 30)</td>
        </tr>
        <tr>
            <td>Type match bonus</td>
            <td>0.10</td>
            <td>1.0 if type filter matches, 0.0 otherwise</td>
        </tr>
    </tbody>
</table>

Example: query ```"suits season 1"``` → tokens ```["suits", "season", "1"]```

A result matching only "suits" (from filename, weight 1.0):

Match strength:``` 1.0 × 0.40 = 0.40```

Token coverage: ```1/3 × 0.30 = 0.10```

Recency (fresh):``` 1.0 × 0.20 = 0.20```

Type match: ```0.10 ```(if type filter matches)

Total: 0.80

A result matching ``` "suits" ``` (tag, weight 0.8) and ``` "season" ``` (keyword, weight 0.5):

Match strength: ```avg(0.8, 0.5) × 0.40 = 0.26```

Token coverage: ```2/3 × 0.30 = 0.20```

Recency (fresh): ```0.20```

Total: 0.66

The first result ranks higher despite fewer matched tokens because the filename match is stronger.

<hr style="border: 1px solid #ecf0f1;">

<h3>Cache Management</h3>

```go
// Feed a full index bucket into the cache (after DHT fetch)
reader.FeedBucket(keyword, bucket)

// Feed a single entry into the cache (after receiving individual entries)
reader.FeedEntry(keyword, entry)

// Remove all cached entries for a specific content ID (when content is deleted)
reader.InvalidateContent(contentID)

// Evict all expired cache entries (call from maintenance loop)
purged := reader.PurgeExpiredCache() // returns count of purged entries

// Inspect what's in the cache right now
keywords := reader.GetCachedKeywords() // []string

// Reader stats
stats := reader.GetStats()
```
Cache entries expire after ```cacheEntryTTL = 30 minutes```. Expiry is lazy — entries are checked on access, not on a timer. ```PurgeExpiredCache()``` should be called from a maintenance goroutine to prevent unbounded memory growth.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Constants</span>


<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
            <th>Meaning</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>IndexEntryTTL</td>
            <td>30m</td>
            <td>How long an index entry lives in DHT without re-announcement</td>
        </tr>
        <tr>
            <td>ReAnnounceInterval</td>
            <td>20m</td>
            <td>How often the writer re-pushes index entries (must be &lt; IndexEntryTTL)</td>
        </tr>
        <tr>
            <td>cacheEntryTTL</td>
            <td>30m</td>
            <td>How long the reader keeps a fetched bucket in local cache</td>
        </tr>
        <tr>
            <td>defaultMaxResults</td>
            <td>20</td>
            <td>Default result count when SearchQuery.MaxResults == 0</td>
        </tr>
        <tr>
            <td>MinTokenLength</td>
            <td>2</td>
            <td>Tokens shorter than this are discarded</td>
        </tr>
        <tr>
            <td>MaxTokensPerField</td>
            <td>50</td>
            <td>Max tokens extracted per content item</td>
        </tr>
    </tbody>
</table>

<h2>Scoring Weights</h2>
<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>weightTokenMatchStrength</td>
            <td>0.40</td>
        </tr>
        <tr>
            <td>weightTokenCoverage</td>
            <td>0.30</td>
        </tr>
        <tr>
            <td>weightRecency</td>
            <td>0.20</td>
        </tr>
        <tr>
            <td>weightTypeMatch</td>
            <td>0.10</td>
        </tr>
    </tbody>
</table>