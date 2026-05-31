

```markdown
---
title: DHT Keyspace Design
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
file: keygen.go
---
```
# <span style="color: #3498db;"> DHT Keyspace Design</span>

All content-related keys are namespaced before being hashed into Kademlia's 256-bit keyspace. Without namespacing, a ContentID and a ChunkHash that happen to be identical would collide — silently corrupting data.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Namespaces</span>

| Namespace prefix | DHT value stored | Generator function |
|------------------|------------------|-------------------|
| `/content/<ContentID>` | `ContentMeta` | `ContentKey(contentID)` |
| `/chunk-manifest/<ContentID>` | `ChunkManifest` | `ChunkManifestKey(contentID)` |
| `/chunk/<ChunkHash>` | raw `[]byte` | `ChunkKey(chunkHash)` |
| `/provider/<ContentID>` | `[]ContentProvider` | `ProviderKey(contentID)` |
| `/index/<keyword>` | `IndexBucket` | `IndexKey(keyword)` |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Key Generation</span>

All keys go through the same pipeline:
raw string (e.g. "/content/abc123...")
↓
SHA256()
↓
types.NodeID (32 bytes)
↓
Kademlia routes using XOR distance

```text

This means content keys and node ID keys live in the same 256-bit keyspace and use the exact same XOR distance routing. No separate routing system is needed for content lookups.
```
<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Key Functions</span>

```go
// Examples
key := ContentKey("sha256hashoffile...")    // types.NodeID
key := ChunkKey("sha256ofchunkbytes...")    // types.NodeID
key := IndexKey("suits")                    // types.NodeID — normalized, lowercased
```
```IndexKey()``` normalizes its input before hashing:

```go
IndexKey("  SUITS  ") == IndexKey("suits") // true — case-insensitive, trimmed
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;">
 Why Namespacing Matters</span>

Without namespacing:

A file's ContentID = ```abc123```

A chunk's ChunkHash = ```abc123``` (unlikely but possible)

Both would map to the same DHT key → data corruption

With namespacing:

Content key = ```SHA256(/content/abc123)```

Chunk key = ```SHA256(/chunk/abc123)```
Different DHT keys → no collision