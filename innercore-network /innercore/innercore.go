// innercore/innercore.go
package innercore

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"innercore-network/packet"
	"innercore-network/storage"
	"innercore-network/types"
)

type InnerCore struct {
	nodeID     types.NodeID
	kBuckets   [256]*KBucket
	supernodes []PeerInfo
	mu         sync.RWMutex
	storage    *storage.StorageEngine
	msgSender  func(target types.NodeID, pkt packet.Packet) error

	// pendingLookups maps requestID → a channel that receives the []PeerInfo
	// returned in a KADEMLIA_FIND_NODE_REPLY.
	// This is the bridge between the fire-and-forget UDP send and the async reply
	// that arrives through HandleKademliaPacket on a completely different goroutine.
	pendingLookups   map[string]chan []PeerInfo
	pendingLookupsMu sync.Mutex
}

func New(storageEngine *storage.StorageEngine, senderFunc func(types.NodeID, packet.Packet) error) *InnerCore {
	ic := &InnerCore{
		storage:        storageEngine,
		msgSender:      senderFunc,
		pendingLookups: make(map[string]chan []PeerInfo),
	}

	for i := range ic.kBuckets {
		ic.kBuckets[i] = NewKBucket(20)
	}

	ic.loadPersistedTable()
	go ic.maintenanceLoop()

	fmt.Println("[INNERCORE] InnerCore started — Kademlia + capability-driven supernodes")
	return ic
}

// SeedPeer is called by discovery when a new peer is found on the network.
func (ic *InnerCore) SeedPeer(nodeID types.NodeID, ip string, caps types.Capabilities) {
	// Guard: if SetNodeID hasn't been called yet, our XOR distance calculations
	// would all use the zero NodeID → wrong bucket assignments.
	// Integration.Init() must call SetNodeID BEFORE registering discovery callbacks.
	if ic.nodeID == (types.NodeID{}) {
		fmt.Printf("[INNERCORE] WARNING: SeedPeer called before NodeID was set. Skipping for now.\n")
		return
	}

	peer := &PeerInfo{
		NodeID:       nodeID,
		IP:           ip,
		Capabilities: caps,
		LastSeen:     time.Now().Unix(),
		Score:        calculateScore(caps),
	}

	dist := xorDistance(ic.nodeID, nodeID)
	bucketIdx := 0
	for i := uint(0); i < 256; i++ {
		if (dist & (1 << i)) != 0 {
			bucketIdx = int(i)
			break
		}
	}

	ic.kBuckets[bucketIdx].Insert(peer)
	ic.updateSupernodeList()
}

func (ic *InnerCore) GetPreferredSupernodes() []PeerInfo {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.supernodes
}

// ==================== Lookup Helpers ====================

func (ic *InnerCore) getAlphaClosest(target types.NodeID, alpha int) []PeerInfo {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	var candidates []*PeerInfo
	for i := range ic.kBuckets {
		candidates = append(candidates, ic.kBuckets[i].nodes...)
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return xorDistance(candidates[i].NodeID, target) < xorDistance(candidates[j].NodeID, target)
	})

	if len(candidates) > alpha {
		candidates = candidates[:alpha]
	}

	result := make([]PeerInfo, len(candidates))
	for i, p := range candidates {
		result[i] = *p
	}
	return result
}

func (ic *InnerCore) getLocalClosest(from types.NodeID, target types.NodeID, count int) []PeerInfo {
	return ic.getAlphaClosest(target, count)
}

// ==================== Pending Lookup Registry ====================

// registerPendingLookup stores a channel under a requestID so that
// HandleKademliaPacket can deliver FIND_NODE_REPLY peers back to the waiting Lookup().
// The channel is buffered(1) so the reply handler never blocks even if the waiter
// has already timed out and moved on.
func (ic *InnerCore) registerPendingLookup(requestID string) chan []PeerInfo {
	ch := make(chan []PeerInfo, 1)
	ic.pendingLookupsMu.Lock()
	ic.pendingLookups[requestID] = ch
	ic.pendingLookupsMu.Unlock()
	return ch
}

// resolvePendingLookup delivers peers to the waiting Lookup() call and removes the entry.
func (ic *InnerCore) resolvePendingLookup(requestID string, peers []PeerInfo) {
	ic.pendingLookupsMu.Lock()
	ch, exists := ic.pendingLookups[requestID]
	if exists {
		delete(ic.pendingLookups, requestID)
	}
	ic.pendingLookupsMu.Unlock()

	if exists {
		ch <- peers // non-blocking because channel is buffered(1)
	}
}

// cancelPendingLookup removes a pending entry without delivering (used on timeout).
func (ic *InnerCore) cancelPendingLookup(requestID string) {
	ic.pendingLookupsMu.Lock()
	delete(ic.pendingLookups, requestID)
	ic.pendingLookupsMu.Unlock()
}

// ==================== Background Methods ====================

func (ic *InnerCore) updateSupernodeList() {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	var allPeers []*PeerInfo
	for i := range ic.kBuckets {
		allPeers = append(allPeers, ic.kBuckets[i].nodes...)
	}

	sort.Slice(allPeers, func(i, j int) bool {
		return allPeers[i].Score > allPeers[j].Score
	})

	ic.supernodes = make([]PeerInfo, 0, 5)
	for i := 0; i < len(allPeers) && i < 5; i++ {
		ic.supernodes = append(ic.supernodes, *allPeers[i])
	}

	if len(ic.supernodes) > 0 {
		fmt.Printf("[SUPERNODE ELECTION] Top supernodes updated. Best score: %.1f\n", ic.supernodes[0].Score)
	}
}

func (ic *InnerCore) maintenanceLoop() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		ic.refreshBuckets()
		ic.updateSupernodeList()
	}
}

func (ic *InnerCore) loadPersistedTable() {
	// Step 6 — real persistence later
	fmt.Println("[INNERCORE] Loaded persisted routing table (stub)")
}

func (ic *InnerCore) refreshBuckets() {
	// Step 5 — ping oldest + eviction
	fmt.Println("[KADEMLIA] Bucket maintenance run (ping oldest 3)")
	// TODO: real ping-oldest logic
	ic.updateSupernodeList()
}

// SetNodeID is used by the integration layer to avoid import cycles.
// MUST be called before any discovery callbacks that invoke SeedPeer.
func (ic *InnerCore) SetNodeID(id types.NodeID) {
	ic.nodeID = id
	fmt.Printf("[INNERCORE] NodeID set: %s\n", id)
}

// TestLookup is a helper to manually trigger a lookup from main or tests
func (ic *InnerCore) TestLookup(target types.NodeID) {
	fmt.Printf("[TEST] Starting lookup for target %s...\n", target)
	peers, err := ic.Lookup(target)
	if err != nil {
		fmt.Printf("[TEST] Lookup failed: %v\n", err)
		return
	}
	fmt.Printf("[TEST] Lookup found %d peers:\n", len(peers))
	for _, p := range peers {
		fmt.Printf("   → %s (Score: %.1f)\n", p.NodeID, p.Score)
	}
}
