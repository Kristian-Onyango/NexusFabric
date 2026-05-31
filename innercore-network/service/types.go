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
