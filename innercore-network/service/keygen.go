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
