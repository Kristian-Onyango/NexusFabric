---
title: Layer 3B — Distributed Storage
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
files: types.go, keygen.go, content_store.go, dht.go, provider.go, persistence.go, retrieval.go
---

# <span style="color: #3498db;">Layer 3B — Distributed Storage</span>

Layer 3B is the hybrid content storage engine. It handles the full lifecycle of a file: chunking, hashing, local caching, DHT distribution, replication, provider announcement, and retrieval.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Types</span>

### `ContentMeta`

The authoritative description of a stored file. Stored in the DHT under `/content/<ContentID>`.

```go
type ContentMeta struct {
    ContentID         string       // SHA256 of full file, or Merkle root for chunked files
    Name              string       // human-readable filename
    Type              string       // "video", "image", "doc", etc.
    OwnerNodeID       types.NodeID // publisher's node ID
    CreatedAt         int64        // unix timestamp
    UpdatedAt         int64
    SizeBytes         int64
    IsChunked         bool
    ChunkCount        int          // 0 if not chunked
    ChunkSize         int64        // bytes per chunk (last chunk may be smaller)
    Hash              string       // full file SHA256
    HashAlgo          string       // always "sha256"
    ChunkManifestID   string       // fetch at /chunk-manifest/<ContentID>
    Version           int
    PrevVersion       string       // ContentID of previous version, if updated
    Tags              []string
    Keywords          []string
    ReplicationFactor int          // default: 3
}
```
 Design note: ```ContentMeta``` is intentionally lean. For a chunked file with 10,000 chunks, embedding the full chunk list would make this a massive DHT value. The chunk list lives separately in a ```ChunkManifest```.

<hr style="border: 1px solid #ecf0f1;">

```ChunkManifest```

The ordered list of chunks for a large file. Stored separately under ```/chunk-manifest/<ContentID>```. Only fetched when a client actually needs to download the file.

```
type ChunkManifest struct {
    ContentID string      // parent content ID
    Chunks    []ChunkMeta // ordered list
}

type ChunkMeta struct {
    Index     int    // 0-based position in file
    Hash      string // SHA256 of this chunk
    SizeBytes int64  // actual size (last chunk may differ)
}
```

<hr style="border: 1px solid #ecf0f1;">

```ContentProvider```

Tracks a node that is currently hosting a piece of content.

```
type ContentProvider struct {
    NodeID       types.NodeID       // who has it
    IP           string
    Port         int
    Capabilities types.Capabilities
    AnnouncedAt  int64              // unix timestamp
    HasFull      bool               // true = complete file, false = partial
    ChunksHeld   []int              // which chunk indexes (if partial)
}
```
<hr style="border: 1px solid #ecf0f1;">

```ContentRef```

A lightweight search index reference. Denormalized for fast display — avoids a second DHT lookup just to render a search result.

```
type ContentRef struct {
    ContentID string
    Name      string
    Type      string
    SizeBytes int64
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> The Hybrid Storage Model</span>

Files are stored using one of two paths based on their size:

<table>
    <thead>
        <tr>
            <th>File Size</th>
            <th>Path</th>
            <th>Behavior</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>≤ 1 MB (ChunkThreshold)</td>
            <td>Small file</td>
            <td>Single DHT STORE, IsChunked = false</td>
        </tr>
        <tr>
            <td>> 1 MB</td>
            <td>Large file</td>
            <td>Split into 256 KB chunks, each stored independently, IsChunked = true</td>
        </tr>
    </tbody>
</table>

> Why this matters: Kademlia STORE operations carry their value in UDP packets. A 500 MB file cannot fit in a UDP packet. Chunking spreads the load, enables parallel downloads, allows partial retrieval, and makes the system resilient.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ContentStore</span>

```ContentStore``` is the local storage engine. It handles chunking logic and local caching. It does not do network I/O.

```
// Create a store for this node
cs := service.NewContentStore(ownerNodeID)
```
```Store()``` — the main entry point

```
result, err := cs.Store(
    name,     // string — filename e.g. "Suits.S01E01.mkv"
    fileType, // string — "video", "image", etc.
    data,     // []byte — raw file bytes
    tags,     // []string — user labels e.g. ["tv", "drama"]
    keywords, // []string — secondary metadata e.g. ["suits", "harvey"]
)
```
Returns a ```StoreResult```:

```
type StoreResult struct {
    Meta     *ContentMeta   // always populated
    Manifest *ChunkManifest // only populated for chunked files
    Chunks   []ChunkData    // raw chunk bytes for DHT distribution
}

type ChunkData struct {
    Hash string // chunk's SHA256 — used as DHT key
    Data []byte // the raw bytes — the DHT value
}
```
The caller takes ```StoreResult``` and issues the actual DHT ```STORE``` operations through InnerCore.

<hr style="border: 1px solid #ecf0f1;">
Local cache lookups

```
// Returns nil if not cached locally — caller must do DHT FIND_VALUE
meta := cs.GetMeta(contentID)

// Returns nil if not cached locally
chunkBytes := cs.GetChunk(chunkHash)

// True only if this node has the full file ready
hasIt := cs.HasContent(contentID)

// All content this node holds locally
list := cs.ListLocalContent() // []*ContentMeta
```

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ContentID Generation</span>

Content is identified by a deterministic hash, not by filename or path:

```
// Small files: SHA256 of the entire file
contentID := ContentIDFromBytes(data)

// Large files: SHA256 of the concatenated chunk hashes (Merkle-lite)
contentID := ContentIDFromChunks([]string{chunkHash1, chunkHash2, ...})

// Individual chunk hash
chunkHash := ChunkHashFromBytes(chunkData)
```
This means:

> The same file always produces the same ContentID — automatic deduplication

> Changing any byte changes the ContentID — automatic integrity verification

> Order matters for chunked files — same chunks in different order = different ID

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> DHT Publisher</span>

```DHTPublisher``` handles replication to multiple nodes using Kademlia ```STORE``` operations:

```
// Initialize once at startup, after InnerCore is ready
service.InitDHTPublisher(innerCoreInstance)

publisher := service.GetDHTPublisher()
```

What gets stored to DHT:

<table>
    <thead>
        <tr>
            <th>What</th>
            <th>DHT Key</th>
            <th>DHT Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>File metadata</td>
            <td>/content/&lt;ContentID&gt;</td>
            <td>ContentMeta</td>
        </tr>
        <tr>
            <td>Chunk list</td>
            <td>/chunk-manifest/&lt;ContentID&gt;</td>
            <td>ChunkManifest</td>
        </tr>
        <tr>
            <td>Each chunk</td>
            <td>/chunk/&lt;ChunkHash&gt;</td>
            <td>raw bytes</td>
        </tr>
    </tbody>
</table>

All three are stored concurrently using goroutines.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Announcement</span>

After storing content, a node announces itself as a provider:

```
service.AnnounceContent(
    contentID, // string
    hasFull,   // bool — true if holding complete file
    chunks,    // []int — which chunk indexes (nil if hasFull)
)
```

Providers are tracked in ```ContentProviderRegistry``` with a TTL of 5 minutes. A background cleanup goroutine runs every 2 minutes and removes expired announcements.

```
// Start provider cleanup (called once at startup)
go service.StartProviderCleanup()

// Query current providers for a content ID
providers := service.GetProviders(contentID) // []ContentProvider
```

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Persistence</span>

Content metadata and manifests are persisted to the Layer 5 StorageEngine for survival across restarts:

```
// Save ContentMeta and ChunkManifest to Layer 5
err := service.PersistContent(result)

// Load ContentMeta from Layer 5
meta, err := service.LoadContentMeta(contentID)
```
Persistence uses two Layer 5 collections:

```"content"``` → stores serialized ```ContentMeta``` JSON

```"chunk_manifests"``` → stores serialized ```ChunkManifest``` JSON

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Retrieval</span>

```
result, err := service.RetrieveContent(contentID)
```

Retrieval follows a priority order:

> Local cache (ContentStore.GetMeta + GetChunk)

> Known providers (GetProviders → direct peer download)

> DHT FIND_VALUE via InnerCore (queries 3 closest peers)

> Error: "content not found"

Current status: Full chunk reassembly and streaming are marked for future work. The retrieval path successfully locates content and initiates FIND_VALUE RPCs, but full byte reassembly from multiple peers is not yet implemented.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> High-Level Public API</span>

The recommended entry point for applications:

```
// Store + index + announce + persist + DHT publish in one call
result, err := service.PublishContent(
    name, fileType, data, tags, keywords,
)

// Retrieve content by ID
result, err := service.RetrieveContent(contentID)

// Simple local keyword search (Layer 3C wrapper)
refs := service.Search(keyword) // []*ContentRef
```
```PublishContent()``` internally calls:

```contentStore.Store()``` — chunk and hash

```AnnounceContent()``` — register as provider

```IndexContent()``` — add to search index

```PersistContent()``` — save to Layer 5

```publishContentToDHT()``` — distribute to peers

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
            <td>ChunkThreshold</td>
            <td>1 MB</td>
            <td>Files larger than this are automatically chunked</td>
        </tr>
        <tr>
            <td>DefaultChunkSize</td>
            <td>256 KB</td>
            <td>Size of each chunk for large files</td>
        </tr>
        <tr>
            <td>DefaultReplicationFactor</td>
            <td>3</td>
            <td>Default number of nodes that should hold each file</td>
        </tr>
        <tr>
            <td>ReplicationK</td>
            <td>5</td>
            <td>Max number of peers targeted per DHT STORE operation</td>
        </tr>
        <tr>
            <td>StoreTimeout</td>
            <td>8s</td>
            <td>Timeout per DHT STORE attempt</td>
        </tr>
        <tr>
            <td>ProviderAnnounceTTL</td>
            <td>5m</td>
            <td>How long a provider announcement stays valid</td>
        </tr>
    </tbody>
</table>