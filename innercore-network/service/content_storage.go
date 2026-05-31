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
