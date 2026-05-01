// innercore/kbucket.go
package innercore

import (
	"sort"
	"sync"

	"innercore-network/types"
)

// KBucket holds up to K peers at a specific XOR distance range
type KBucket struct {
	nodes []*PeerInfo
	k     int
	mu    sync.RWMutex
}

func NewKBucket(k int) *KBucket {
	return &KBucket{
		nodes: make([]*PeerInfo, 0, k),
		k:     k,
	}
}

func (b *KBucket) Insert(peer *PeerInfo) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update existing peer and move to tail (most recent)
	for i, p := range b.nodes {
		if p.NodeID.Equal(peer.NodeID) {
			b.nodes[i] = peer
			copy(b.nodes[i:], b.nodes[i+1:])
			b.nodes[len(b.nodes)-1] = peer
			return true
		}
	}

	// Add new peer if space available
	if len(b.nodes) < b.k {
		b.nodes = append(b.nodes, peer)
		return true
	}

	// Bucket full - will be handled by maintenance (ping oldest)
	return false
}

func (b *KBucket) GetClosest(target types.NodeID, count int) []*PeerInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.nodes) == 0 {
		return nil
	}

	candidates := make([]*PeerInfo, len(b.nodes))
	copy(candidates, b.nodes)

	sort.Slice(candidates, func(i, j int) bool {
		return xorDistance(candidates[i].NodeID, target) < xorDistance(candidates[j].NodeID, target)
	})

	if len(candidates) > count {
		candidates = candidates[:count]
	}
	return candidates
}

func xorDistance(a, b types.NodeID) uint {
	var d uint
	for i := range a {
		d += uint(a[i] ^ b[i])
	}
	return d
}

/*
What this file does:

Implements one Kademlia k-bucket (distance-based list).
Insert follows classic Kademlia rules: update existing, move to tail, respect capacity.
GetClosest returns the best peers for a lookup target using XOR metric.
Thread-safe with RWMutex.

*/
