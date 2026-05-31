// service/dht.go
// Layer 3B — DHT Integration for Content Fabric
//
// This bridges ContentStore (Layer 3B) with InnerCore (Layer 1.5).
// It handles replication to multiple nodes using Kademlia STORE.

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/innercore"
	"innercore-network/types"
)

const (
	ReplicationK = 5
	StoreTimeout = 8 * time.Second
)

type DHTPublisher struct {
	innerCore *innercore.InnerCore
	mu        sync.RWMutex
}

var (
	dhtPublisher  *DHTPublisher
	publisherOnce sync.Once
)

func InitDHTPublisher(ic *innercore.InnerCore) {
	publisherOnce.Do(func() {
		dhtPublisher = &DHTPublisher{innerCore: ic}
		fmt.Println("[NEXUSFABRIC] DHT Publisher initialized")
	})
}

func GetDHTPublisher() *DHTPublisher {
	return dhtPublisher
}

// publishContentToDHT — internal implementation (renamed to avoid conflict)
func publishContentToDHT(result *StoreResult) error {
	if dhtPublisher == nil {
		return fmt.Errorf("DHT publisher not initialized")
	}
	return dhtPublisher.publish(result)
}

func (p *DHTPublisher) publish(result *StoreResult) error {
	if result == nil || result.Meta == nil {
		return fmt.Errorf("invalid store result")
	}

	fmt.Printf("[NEXUSFABRIC] Publishing ContentID: %s (chunked: %v)\n",
		result.Meta.ContentID[:16]+"...", result.Meta.IsChunked)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// 1. Store ContentMeta
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.storeWithReplication(ContentKey(result.Meta.ContentID), result.Meta); err != nil {
			errChan <- fmt.Errorf("meta store failed: %v", err)
		}
	}()

	// 2. Store ChunkManifest (if chunked)
	if result.Meta.IsChunked && result.Manifest != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.storeWithReplication(ChunkManifestKey(result.Meta.ContentID), result.Manifest); err != nil {
				errChan <- fmt.Errorf("manifest store failed: %v", err)
			}
		}()
	}

	// 3. Store all chunks
	for _, chunk := range result.Chunks {
		wg.Add(1)
		go func(c ChunkData) {
			defer wg.Done()
			if err := p.storeWithReplication(ChunkKey(c.Hash), c.Data); err != nil {
				errChan <- fmt.Errorf("chunk %s store failed: %v", c.Hash[:12]+"...", err)
			}
		}(chunk)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		fmt.Printf("[NEXUSFABRIC] WARNING: %v\n", err)
	}

	fmt.Printf("[NEXUSFABRIC] Published %s successfully\n", result.Meta.Name)
	return nil
}

func (p *DHTPublisher) storeWithReplication(key types.NodeID, value any) error {
	peers, err := p.innerCore.Lookup(key)
	if err != nil || len(peers) == 0 {
		return fmt.Errorf("no peers found for replication")
	}

	targets := peers
	if len(targets) > ReplicationK {
		targets = targets[:ReplicationK]
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var successCount int

	for _, peer := range targets {
		// Skip self
		if peer.NodeID.Equal(p.innerCore.GetMyNodeID()) {
			continue
		}

		wg.Add(1)
		go func(target types.NodeID) {
			defer wg.Done()

			if err := p.innerCore.SendStore(target, key[:], value); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(peer.NodeID)
	}

	wg.Wait()

	if successCount == 0 {
		return fmt.Errorf("failed to store to any replicas")
	}

	fmt.Printf("[DHT] Stored key %s to %d/%d replicas\n", key, successCount, len(targets))
	return nil
}
