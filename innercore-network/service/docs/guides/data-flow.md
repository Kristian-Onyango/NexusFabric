
```markdown
---
title: Data Flow Diagrams
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---
```
# <span style="color: #3498db;"> Data Flow Diagrams</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Publishing a File</span>
```
Application calls PublishContent("Suits.mkv", "video", bytes, tags, keywords)
│
▼
ContentStore.Store()
├── len(bytes) ≤ 1MB → storeSmall()
│ SHA256(bytes) → ContentID
│ Build ContentMeta (IsChunked: false)
│ Cache locally
│ Return StoreResult{Meta, Chunks: [{Hash: ContentID, Data: bytes}]}
│
└── len(bytes) > 1MB → storeLarge()
Split into 256KB chunks
SHA256 each chunk → ChunkMeta
ContentID = SHA256(all chunk hashes)
Build ContentMeta (IsChunked: true)
Build ChunkManifest
Cache locally
Return StoreResult{Meta, Manifest, Chunks: [{Hash, Data}, ...]}
│
▼
AnnounceContent(ContentID, hasFull=true)
│ Register self as provider in ContentProviderRegistry
│
▼
IndexContent(Meta) [Layer 3C bridge]
│ Tokenizer.TokenizeContentMeta() → WeightedTokens
│ IndexWriter.Index(Meta) → IndexWriteRecords
│ InnerCore.SendStore(DHTKey, Entry) for each token
│
▼
PersistContent(StoreResult) [Layer 5]
│ StorageEngine.Put("content", ContentID, meta)
│ StorageEngine.Put("chunk_manifests", ContentID, manifest)
│
▼
publishContentToDHT(StoreResult) [Layer 3B DHT]
│ InnerCore.Lookup(ContentKey) → up to 5 peers
│ SendStore(ContentKey, Meta) in parallel
│ SendStore(ChunkManifestKey, Manifest) if chunked
│ SendStore(ChunkKey, chunkBytes) for each chunk
│
▼
Done — content is live on the mesh
```
<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Searching for Content</span>
```
User searches "suits season 1"
│
▼
IndexReader.SearchLocal(query)
│ Tokenize("suits season 1") → ["suits", "season", "1"]
│ For each token: check local cache
│ cache hit → use cached IndexBucket
│ cache miss → add to DHTLookupRequest list
│ Merge results across all tokens
│ Score each result (match strength + coverage + recency + type)
│ Sort by score descending
│ Apply type filter, size filter
│ Paginate
│ Return SearchResponse
│
├── TotalFound > 0 → return immediately (cache hit)
│
└── TotalFound = 0 → caller fetches from DHT:
GetDHTKeysForQuery(query) → [{Keyword:"suits", DHTKey:...}, ...]
InnerCore.FindValue(DHTKey) → IndexBucket
IndexReader.FeedBucket("suits", bucket)
IndexReader.SearchLocal(query) → results now available
```

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Re-announcement Cycle</span>
```
Every 20 minutes (ReAnnounceInterval):
IndexWriter.GetReAnnounceRecords(contentStore)
│ For each locally tracked content:
│ if time since LastAnnounce >= 20m:
│ build IndexWriteRecord for each keyword
│ update LastAnnounce = now
│ Return []IndexWriteRecord
│
▼
Caller: InnerCore.SendStore(DHTKey, Entry) for each record
│
▼
DHT entries refreshed — TTL clock reset to 30m
```

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Provider Cleanup</span>
```
Every 30 seconds:
For each service in ServiceRegistry:
For each provider in service.Providers:
If time.Since(LastAnnounce) > 45s:
Delete provider
If no providers left:
Delete service entr

```
<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Retrieval Flow</span>
```
service.RetrieveContent(contentID)
│
▼
Check local cache (ContentStore.GetMeta)
│
├── Found → Check if full bytes available
│ ├── Small file → return Data
│ └── Large file → return Chunks map (reassembly future)
│
└── Not found → Query ContentProviderRegistry.GetProviders(contentID)
│
├── Providers found → Direct peer download
│
└── No providers → DHT FIND_VALUE via InnerCore
│
├── Found → return
│
└── Not found → Error
```