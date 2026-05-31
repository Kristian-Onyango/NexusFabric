```---
title: Known Limitations & Future Work
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---
```
# <span style="color: #3498db;"> Known Limitations & Future Work</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Current Limitations</span>

| Area | Status | Notes |
|------|--------|-------|
| Chunk reassembly | Incomplete | `retrieval.go` locates chunks but does not yet reassemble them into a complete file |
| Streaming | Not started | Chunked retrieval architecture is in place; adaptive buffering is future work |
| DHT FIND_VALUE parsing | Stub | `rpc.go` in InnerCore sends FIND_VALUE but response parsing is not wired to Layer 3C |
| Search index persistence | Not started | `IndexWriter.localRecords` is in-memory only; lost on restart |
| Provider NodeID | Placeholder | `provider.go` uses `types.Capabilities` with stub values — real values should come from NodeStorageCapabilities |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Planned Enhancements</span>

| Feature | Description |
|---------|-------------|
| Distributed repair | Detect when a chunk has fewer than `ReplicationFactor` replicas and automatically re-replicate |
| Replica rebalancing | Redistribute chunks as nodes join and leave the mesh |
| Advanced search ranking | Add fuzzy matching, partial token matching, and boosting by provider count |
| Persistent index | Survive restarts by writing `localRecords` to Layer 5 StorageEngine |
| Full streaming engine | Chunk prioritization, adaptive buffering, partial playback support |
| Index pruning | Remove stale `IndexBucket` entries that point to content no longer available on the network |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Design Trade-offs</span>

| Decision | Rationale |
|----------|-----------|
| `MinTokenLength = 2` | Single-character queries (e.g., "a", "1") don't work. This is intentional to prevent DHT spam. |
| No DHT delete operation | Kademlia has no native delete. Entries expire via TTL instead. |
| Denormalized `IndexEntry` | Search results avoid a second DHT lookup. Trade-off: index entries are larger. |
| 256KB chunk size | Balance between UDP packet limits (8KB) and reducing number of chunks. ~4 chunks per MB. |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Summary: What You Now Have</span>
```
docs/
├── README.md # Entry point + doc map
├── architecture/
│ ├── 01-system-vision.md # System vision & principles
│ ├── 02-layer-3A-services.md # service.go documentation
│ ├── 03-layer-3B-storage.md # types, chunking, retrieval
│ ├── 04-layer-3C-search.md # tokenizer, index, search
│ └── 05-keyspace-design.md # keygen.go
├── guides/
│ ├── integration.md # Startup + API examples
│ └── data-flow.md # Sequence diagrams
└── reference/
├── constants.md # All constants in one place
└── known-limitations.md # Honest about what's broken
```