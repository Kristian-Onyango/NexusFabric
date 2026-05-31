```
---
title: Constants Reference
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---
```
# <span style="color: #3498db;"> Constants Reference</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Layer 3A — Service Registry</span>

| Constant | Value | Meaning |
|----------|-------|---------|
| `ProviderTTL` | 45s | How long a service provider stays registered without re-announcing |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Layer 3B — Distributed Storage</span>

| Constant | Value | Meaning |
|----------|-------|---------|
| `ChunkThreshold` | 1 MB | Files larger than this are automatically chunked |
| `DefaultChunkSize` | 256 KB | Size of each chunk for large files |
| `DefaultReplicationFactor` | 3 | Default number of nodes that should hold each file |
| `ReplicationK` | 5 | Max number of peers targeted per DHT STORE operation |
| `StoreTimeout` | 8s | Timeout per DHT STORE attempt |
| `ProviderAnnounceTTL` | 5m | How long a provider announcement stays valid |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Layer 3C — Metadata Indexing & Search</span>

| Constant | Value | Meaning |
|----------|-------|---------|
| `IndexEntryTTL` | 30m | How long an index entry lives in DHT without re-announcement |
| `ReAnnounceInterval` | 20m | How often the writer re-pushes index entries (must be < IndexEntryTTL) |
| `cacheEntryTTL` | 30m | How long the reader keeps a fetched bucket in local cache |
| `defaultMaxResults` | 20 | Default result count when SearchQuery.MaxResults == 0 |
| `MinTokenLength` | 2 | Tokens shorter than this are discarded |
| `MaxTokensPerField` | 50 | Max tokens extracted per content item |

<hr style="border: 1px solid #ecf0f1;">

### Scoring Weights (Layer 3C)

| Constant | Value |
|----------|-------|
| `weightTokenMatchStrength` | 0.40 |
| `weightTokenCoverage` | 0.30 |
| `weightRecency` | 0.20 |
| `weightTypeMatch` | 0.10 |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Role Service Policy</span>

| Role | Permitted Services |
|------|-------------------|
| `game` | games, chat, matchmaking |
| `chat` | chat, messaging, presence |
| `cache` | cache, storage, cdn |
| `storage` | storage, backup, files |
| `unknown` | (none) |