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
