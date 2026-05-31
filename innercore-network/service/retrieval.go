// service/retrieval.go
// Layer 3B — Content Retrieval (DHT + Peer-assisted)

package service

import (
	"fmt"
)

type RetrieveResult struct {
	Meta   *ContentMeta
	Data   []byte
	Chunks map[int][]byte
}

// Internal implementation to avoid package-level issues
func retrieveContentInternal(contentID string) (*RetrieveResult, error) {
	if cs := GetContentStore(); cs != nil {
		if meta := cs.GetMeta(contentID); meta != nil {
			if !meta.IsChunked {
				if data := cs.GetChunk(contentID); len(data) > 0 {
					return &RetrieveResult{Meta: meta, Data: data}, nil
				}
			}
		}
	}

	providers := GetProviders(contentID)
	if len(providers) > 0 {
		fmt.Printf("[RETRIEVAL] Found %d providers for %s\n", len(providers), contentID[:16]+"...")
	}

	publisher := GetDHTPublisher()
	if publisher == nil {
		return nil, fmt.Errorf("DHT unavailable")
	}

	key := ContentKey(contentID)
	peers, err := publisher.innerCore.Lookup(key)
	if err != nil || len(peers) == 0 {
		return nil, fmt.Errorf("content not found")
	}

	fmt.Printf("[RETRIEVAL] Querying %d peers...\n", len(peers))

	for _, peer := range peers[:3] {
		publisher.innerCore.SendFindValue(peer.NodeID, key[:])
	}

	return nil, fmt.Errorf("full retrieval MVP - content located but streaming not complete")
}
