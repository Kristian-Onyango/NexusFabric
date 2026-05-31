// service/provider.go
// Layer 3B — Content Provider Announcement & Discovery

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

const ProviderAnnounceTTL = 5 * time.Minute

type ContentProviderRegistry struct {
	providers map[string][]ContentProvider // contentID → providers
	mu        sync.RWMutex
}

var (
	providerRegistry = &ContentProviderRegistry{
		providers: make(map[string][]ContentProvider),
	}
)

// AnnounceContent announces that this node has a piece of content
func AnnounceContent(contentID string, hasFull bool, chunksHeld []int) {
	if cs := GetContentStore(); cs == nil {
		return
	}

	meta := GetContentStore().GetMeta(contentID)
	if meta == nil {
		return
	}

	provider := ContentProvider{
		NodeID:       GetContentStore().ownerID,
		IP:           "127.0.0.1", // will be updated from network table
		Port:         51000,
		Capabilities: types.Capabilities{StorageAvailableMB: 6144, CanStoreChunks: true},
		AnnouncedAt:  time.Now().Unix(),
		HasFull:      hasFull,
		ChunksHeld:   chunksHeld,
	}

	providerRegistry.mu.Lock()
	providerRegistry.providers[contentID] = append(providerRegistry.providers[contentID], provider)
	providerRegistry.mu.Unlock()

	fmt.Printf("[PROVIDER] Announced ownership of %s (full: %v)\n", contentID[:16]+"...", hasFull)

	// TODO: Broadcast via InnerCore or role-based messaging
}

// GetProviders returns nodes that have this content
func GetProviders(contentID string) []ContentProvider {
	providerRegistry.mu.RLock()
	defer providerRegistry.mu.RUnlock()
	return providerRegistry.providers[contentID]
}

// Cleanup old providers
func StartProviderCleanup() {
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		providerRegistry.mu.Lock()
		now := time.Now().Unix()
		for cid, list := range providerRegistry.providers {
			var active []ContentProvider
			for _, p := range list {
				if now-p.AnnouncedAt < int64(ProviderAnnounceTTL.Seconds()) {
					active = append(active, p)
				}
			}
			if len(active) > 0 {
				providerRegistry.providers[cid] = active
			} else {
				delete(providerRegistry.providers, cid)
			}
		}
		providerRegistry.mu.Unlock()
	}
}
