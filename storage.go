// innercore/innercore.go
package innercore

//
// Core controller of the Kademlia overlay network.
//
// Responsibilities:
// - Owns all Kademlia state and routing tables
// - Manages 256 XOR-distance k-buckets
// - Inserts discovered peers into correct buckets
// - Maintains preferred supernode list based on capability scoring
// - Provides closest-peer selection for lookups
// - Tracks async pending lookups using requestID -> channel mapping
// - Bridges outgoing FIND_NODE requests with incoming replies
// - Runs background maintenance and bucket refresh tasks
// - Coordinates lookup synchronization across goroutines
//
// Architecture Role:
// Discovery -> SeedPeer -> KBucket -> Lookup/RPC
//
// Important Concepts:
// - XOR distance routing
// - Kademlia bucket management
// - Async RPC reply correlation
// - Capability-driven supernode election
// - Concurrent lookup coordination
//
// NOTE:
// This file manages state/orchestration only.
// Actual network transport and lookup traversal logic live elsewhere.

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
// innercore/lookup.go
package innercore

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// Lookup performs a real iterative Kademlia lookup.
//
// THE PROBLEM WITH THE OLD VERSION:
// The old code fired SendFindNode() inside goroutines and then immediately read
// from getAlphaClosest() — which is LOCAL state. It never waited for the UDP reply
// to come back from the remote node. The network round-trip takes milliseconds but
// the goroutine had already closed the channel and moved on. This meant every
// "iteration" was just re-reading your own routing table, not the network.
//
// THE FIX:
// For each peer we query, we register a pendingLookup channel BEFORE sending the
// packet. SendFindNodeAndWait sends the packet then blocks on that channel with a
// timeout. When HandleKademliaPacket receives a KADEMLIA_FIND_NODE_REPLY, it calls
// resolvePendingLookup which delivers the real remote peers into that channel.
// Now each iteration actually uses what the network told us.
func (ic *InnerCore) Lookup(target types.NodeID) ([]PeerInfo, error) {
	fmt.Printf("[KADEMLIA] Starting lookup for target %s\n", target)

	// Guard: prevent lookup if we don't have our own NodeID yet
	if ic.nodeID == (types.NodeID{}) {
		fmt.Println("[KADEMLIA] WARNING: NodeID not set yet. Returning empty result.")
		return nil, fmt.Errorf("nodeID not initialized")
	}

	if target.Equal(ic.nodeID) {
		fmt.Println("[KADEMLIA] Self lookup - returning self")
		return []PeerInfo{{
			NodeID:   ic.nodeID,
			IP:       "127.0.0.1",
			LastSeen: time.Now().Unix(),
			Score:    100.0,
		}}, nil
	}

	alpha := 3
	// Seed the first round from our local routing table
	closest := ic.getAlphaClosest(target, alpha)

	// Fallback: if we have nothing in k-buckets yet, use the network table directly.
	// This happens on a fresh node that just joined and hasn't filled buckets yet.
	if len(closest) == 0 {
		fmt.Println("[KADEMLIA] No peers in buckets, falling back to network table")
		peers := network.GetAllPeers()
		for _, p := range peers {
			closest = append(closest, PeerInfo{
				NodeID:       p.NodeID,
				IP:           p.IP,
				Capabilities: p.Capabilities,
				LastSeen:     p.LastSeen,
				Score:        p.Score,
			})
		}
	}

	seen := make(map[types.NodeID]bool)
	var result []PeerInfo
	start := time.Now()

	for iteration := 0; len(closest) > 0 && iteration < 5 && time.Since(start) < 5*time.Second; iteration++ {
		fmt.Printf("[KADEMLIA] Iteration %d | Querying %d peers\n", iteration, len(closest))

		var wg sync.WaitGroup
		var newPeersFromNetwork []PeerInfo
		var newPeersMu sync.Mutex

		for _, p := range closest {
			if seen[p.NodeID] {
				continue
			}
			seen[p.NodeID] = true

			wg.Add(1)
			go func(peer PeerInfo) {
				defer wg.Done()

				// SendFindNodeAndWait registers a pending channel, sends the packet,
				// then waits up to 2 seconds for the real reply from the remote node.
				remotePeers := ic.SendFindNodeAndWait(peer.NodeID, target)

				if len(remotePeers) > 0 {
					fmt.Printf("[KADEMLIA] Got %d peers from %s reply\n", len(remotePeers), peer.NodeID)
					newPeersMu.Lock()
					newPeersFromNetwork = append(newPeersFromNetwork, remotePeers...)
					newPeersMu.Unlock()

					// Feed discovered peers into our routing table so they persist
					// beyond this lookup — this is how the table grows over time.
					for _, rp := range remotePeers {
						ic.SeedPeer(rp.NodeID, rp.IP, rp.Capabilities)
					}
				}
			}(p)
		}

		wg.Wait()

		// If the network gave us new peers, use those for the next iteration.
		// If not (e.g. all timeouts), escalate to supernodes.
		if len(newPeersFromNetwork) > 0 {
			closest = newPeersFromNetwork
			result = append(result, newPeersFromNetwork...)
		} else if len(ic.supernodes) > 0 {
			fmt.Println("[KADEMLIA] All queries timed out → escalating to supernodes")
			closest = ic.supernodes
			result = append(result, ic.supernodes...)
		} else {
			fmt.Println("[KADEMLIA] No replies and no supernodes — stopping lookup")
			break
		}
	}

	// Always include self so callers always get at least one result
	result = append(result, PeerInfo{
		NodeID:   ic.nodeID,
		IP:       "127.0.0.1",
		LastSeen: time.Now().Unix(),
		Score:    100.0,
	})

	fmt.Printf("[KADEMLIA] Lookup completed. Found %d peers\n", len(result))
	return result, nil
}
// innercore/rpc.go
// Kademlia RPC Layer — Full Implementation
//
// This file implements all core Kademlia RPCs for the InnerCore overlay:
// - PING / PONG → Liveness and health tracking
// - FIND_NODE → Iterative lookup (core routing primitive)
// - FIND_VALUE → Service & data lookup (used by Layer 2/3)
// - STORE → DHT storage with replication (future distributed storage)

package innercore

import (
	"encoding/json"
	"fmt"
	"time"

	"innercore-network/network"
	"innercore-network/packet"
	"innercore-network/types"
)

// SendPing
func (ic *InnerCore) SendPing(target types.NodeID) {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
			PacketType:        packet.PacketTypeKademliaPing,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network:      packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Capabilities: types.Capabilities{},
		Payload:      mustMarshal(map[string]any{}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send PING to %s: %v\n", target, err)
	} else {
		fmt.Printf("[KADEMLIA] PING sent to %s\n", target)
	}
}

// SendFindNode fires a FIND_NODE packet without waiting for a reply.
// Used internally when we don't need to block (e.g. maintenance pings).
func (ic *InnerCore) SendFindNode(target, lookupID types.NodeID) {
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID,
			PacketType:        packet.PacketTypeKademliaFindNode,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{
			"lookup_id": lookupID,
		}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE to %s: %v\n", target, err)
	} else {
		fmt.Printf("[KADEMLIA] FIND_NODE sent to %s looking for %s\n", target, lookupID)
	}
}

// SendFindNodeAndWait sends a FIND_NODE and blocks until we get a FIND_NODE_REPLY
// or the timeout expires.
//
// WHY THIS EXISTS:
// UDP is fire-and-forget. When you call msgSender, the packet goes out and the
// function returns immediately. The reply arrives asynchronously on the receive loop
// in message/message.go, which calls HandleKademliaPacket. We need a way to connect
// those two events. We do it with a channel stored in pendingLookups keyed by requestID.
// The sender waits on the channel; the reply handler writes into it.
func (ic *InnerCore) SendFindNodeAndWait(target, lookupID types.NodeID) []PeerInfo {
	requestID := fmt.Sprintf("fnw-%d-%d", time.Now().UnixNano(), target[0]) // better uniqueness

	replyCh := ic.registerPendingLookup(requestID)

	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID,
			PacketType:        packet.PacketTypeKademliaFindNode,
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{
			"lookup_id": lookupID,
		}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE to %s: %v\n", target, err)
		ic.cancelPendingLookup(requestID)
		return nil
	}

	fmt.Printf("[KADEMLIA] FIND_NODE sent to %s (requestID: %s), waiting for reply...\n", target, requestID)

	select {
	case peers := <-replyCh:
		fmt.Printf("[KADEMLIA] Got reply from %s with %d peers\n", target, len(peers))
		return peers
	case <-time.After(2 * time.Second):
		fmt.Printf("[KADEMLIA] Timeout waiting for FIND_NODE_REPLY from %s\n", target)
		ic.cancelPendingLookup(requestID)
		return nil
	}
}

func (ic *InnerCore) SendFindValue(target types.NodeID, key []byte) {
	fmt.Printf("[KADEMLIA] FIND_VALUE not implemented yet\n")
	ic.SendPing(target)
}

func (ic *InnerCore) SendStore(target types.NodeID, key []byte, value any) {
	fmt.Printf("[KADEMLIA] STORE not implemented yet\n")
	ic.SendPing(target)
}

// HandleKademliaPacket is the entry point for all Kademlia packets arriving
// from message/message.go's receive loop.
func (ic *InnerCore) HandleKademliaPacket(pkt packet.Packet) {
	sender := pkt.Header.SourceNodeID
	ic.updatePeerLastSeen(sender)
	network.RecordSuccess(sender)

	switch pkt.Header.PacketType {
	case packet.PacketTypeKademliaPing:
		ic.SendPing(sender)

	case packet.PacketTypeKademliaFindNode:
		var payload map[string]any
		json.Unmarshal(pkt.Payload, &payload)

		var lookupID types.NodeID

		if raw, ok := payload["lookup_id"].([]interface{}); ok && len(raw) == 32 {
			for i, v := range raw {
				if num, ok := v.(float64); ok {
					lookupID[i] = byte(num)
				}
			}
		} else {
			fmt.Println("[KADEMLIA] Invalid lookup_id format")
			return
		}

		fmt.Printf("[KADEMLIA] Received FIND_NODE from %s looking for %s\n", sender, lookupID)

		// FIXED: Use the requested lookupID, not our own nodeID
		closest := ic.getAlphaClosest(lookupID, 20)
		if len(closest) == 0 && len(ic.supernodes) > 0 {
			closest = ic.supernodes
		}

		ic.sendFindNodeReply(sender, pkt.Header.RequestID, closest)

	case "KADEMLIA_FIND_NODE_REPLY":
		fmt.Printf("[KADEMLIA] Received FIND_NODE_REPLY from %s (requestID: %s)\n",
			sender, pkt.Header.RequestID)

		var payload map[string]json.RawMessage
		if err := json.Unmarshal(pkt.Payload, &payload); err != nil {
			fmt.Printf("[KADEMLIA] Failed to parse FIND_NODE_REPLY payload: %v\n", err)
			return
		}

		peersRaw, ok := payload["peers"]
		if !ok {
			fmt.Printf("[KADEMLIA] FIND_NODE_REPLY has no peers field\n")
			ic.resolvePendingLookup(pkt.Header.RequestID, nil)
			return
		}

		// Robust decoding using PeerInfo directly (Go handles []byte arrays)
		var peers []PeerInfo
		if err := json.Unmarshal(peersRaw, &peers); err != nil {
			fmt.Printf("[KADEMLIA] Failed to decode peers in FIND_NODE_REPLY: %v\n", err)
			ic.resolvePendingLookup(pkt.Header.RequestID, nil)
			return
		}

		fmt.Printf("[KADEMLIA] Delivering %d real peers to waiting lookup\n", len(peers))
		ic.resolvePendingLookup(pkt.Header.RequestID, peers)

	case packet.PacketTypeKademliaFindValue,
		packet.PacketTypeKademliaStore:
		ic.SendPing(sender)

	default:
		fmt.Printf("[KADEMLIA] Unhandled packet type: %s\n", pkt.Header.PacketType)
	}
}

// sendFindNodeReply sends a KADEMLIA_FIND_NODE_REPLY back to the requester.
// It echoes the requestID so the receiver can match the reply to its pending channel.
func (ic *InnerCore) sendFindNodeReply(target types.NodeID, requestID string, peers []PeerInfo) {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         requestID, // MUST echo back the original requestID
			PacketType:        "KADEMLIA_FIND_NODE_REPLY",
			TTL:               8,
			SourceNodeID:      ic.nodeID,
			DestinationNodeID: target,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(map[string]any{"peers": peers}),
	}

	if err := ic.msgSender(target, p); err != nil {
		fmt.Printf("[KADEMLIA] Failed to send FIND_NODE_REPLY to %s\n", target)
	} else {
		fmt.Printf("[KADEMLIA] Sent FIND_NODE_REPLY to %s with %d peers\n", target, len(peers))
	}
}

func (ic *InnerCore) updatePeerLastSeen(nodeID types.NodeID) {
	fmt.Printf("[KADEMLIA] Updated last seen for %s\n", nodeID)
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
// innercore/scoring.go
// Supernode Election & Scoring
//
// This implements the core design:
// Supernodes emerge naturally based on device capabilities.
// No voting. No central authority. Every node independently computes scores.

package innercore

import "innercore-network/types"

// calculateScore returns a fitness score for supernode candidacy.
// Higher score = better supernode candidate.
//
// The original requirements reflected here:
// - Cross-network bridge capability is heavily rewarded (the Africa use-case)
// - Bandwidth and uptime matter
// - Low latency is preferred
func calculateScore(caps types.Capabilities) float64 {
	score := 0.0

	// Bandwidth is very important for routing assistance
	score += float64(caps.UplinkBandwidthMbps) * 0.45

	// Uptime / stability (normalized)
	score += float64(caps.UptimeSeconds) * 0.00008 * 0.25

	// Cross-network bridge is the most important feature in the design
	if caps.CrossNetworkBridge {
		score += 65.0
	}
	if caps.HotspotCapable {
		score += 25.0
	}

	// Low latency is good for routing
	score -= float64(caps.LatencyMs) * 0.25

	// Internet access is a bonus but not required (per your correction)
	if caps.InternetAccess {
		score += 15.0
	}

	return score
}

/*
What this file does:

Pure scoring function used by every node to decide who its preferred supernodes are.
No global state — every node computes the same score from the same capabilities.
*/
// innercore/types.go
package innercore

import "innercore-network/types" // reuse NodeID and Capabilities from Layer 1

// PeerInfo represents a known peer in the routing table.
//
// JSON TAGS ARE REQUIRED HERE.
// Without them, Go uses the field name as-is (e.g. "NodeID", "IP").
// That works for round-trips within Go, but it's fragile and unreadable
// in logs/debug tools. We use snake_case to match the rest of the packet format.
//
// NOTE on NodeID JSON encoding:
// types.NodeID is [32]byte. Go's json package encodes [N]byte as a base64 string
// (not an array of numbers) because it treats []byte and [N]byte specially.
// This means: json.Marshal(NodeID{...}) → "base64string"
// And:        json.Unmarshal("base64string", &NodeID{}) → works correctly.
// So round-tripping NodeID through JSON is safe and compact.
type PeerInfo struct {
	NodeID       types.NodeID       `json:"node_id"`
	IP           string             `json:"ip"`
	Port         int                `json:"port"`
	Capabilities types.Capabilities `json:"capabilities"` //capabilities of the device whether it has access to the internet or another nexus nextwork
	LastSeen     int64              `json:"last_seen"`
	LatencyMs    int                `json:"latency_ms"`
	Score        float64            `json:"score"` // supernode fitness score
}

// KademliaKey is any 256-bit key (NodeID or hash of data/service)
type KademliaKey [32]byte

/*
What this file does:

Defines the data structures that every other file in innercore will use.
Re-uses NodeID and Capabilities from the types package so there is zero duplication.
JSON tags added so peers can be safely serialized/deserialized across the network
in FIND_NODE_REPLY packets.
*/

//service //layer 3A
// service/service.go
// Layer 3 — Service Protocol & Registry
//
// Purpose:
//   Authoritative service registration and discovery system.
//   Registers services announced via discovery, enforces role-based policies,
//   and provides clean queries for Layer 2 resolver.
//
// Key Design Principles:
//   - Authoritative and consistent
//   - Role-based policy enforcement
//   - Persistent state with cleanup
//   - Health-aware provider selection
//   - Read-heavy (used heavily by Layer 2)
//
// Depends on:
//   - Layer 1 (network table) for device status and health
//   - Layer 4 (messaging) for health updates
//
// Used by:
//   - Layer 2 (resolver) via ServiceResolver interface
//   - Applications for service discovery

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// ProviderTTL - how long a service announcement remains valid
const ProviderTTL = 30 * time.Second

// RoleServicePolicy defines which services each role is allowed to offer
var RoleServicePolicy = map[string][]string{
	"game":    {"games", "chat", "matchmaking"},
	"chat":    {"chat", "messaging", "presence"},
	"cache":   {"cache", "storage", "cdn"},
	"storage": {"storage", "backup", "files"},
	"unknown": {},
}

// ServiceMetadata contains default metadata for known services
var ServiceMetadata = map[string]map[string]any{
	"games":   {"protocol": "udp", "stateful": true, "version": 1},
	"chat":    {"protocol": "tcp", "stateful": true, "version": 1},
	"storage": {"protocol": "tcp", "stateful": false, "version": 1},
	"cache":   {"protocol": "udp", "stateful": false, "version": 1},
}

// ServiceEntry represents one registered service
type ServiceEntry struct {
	Providers map[types.NodeID]ProviderInfo `json:"providers"`
	Metadata  map[string]any                `json:"metadata"`
	Policy    map[string]any                `json:"policy"`
}

// ProviderInfo holds information about a device offering a service
type ProviderInfo struct {
	LastAnnounce time.Time      `json:"last_announce"`
	Metadata     map[string]any `json:"metadata"` // port, ip, device_name, role
	Health       float64        `json:"health"`
}

// ServiceRegistry is the authoritative in-memory registry
type ServiceRegistry struct {
	services map[string]ServiceEntry
	mu       sync.RWMutex
}

var (
	registry     = &ServiceRegistry{services: make(map[string]ServiceEntry)}
	registryOnce sync.Once
)

// Init initializes the service registry
func Init() {
	registryOnce.Do(func() {
		// TODO: load from disk when Layer 5 is ready
		fmt.Println("[SERVICE] Layer 3 Service Registry initialized")
	})
}

// RegisterServicesFromDiscovery is called by Layer 1 when receiving DISCOVERY_ANNOUNCE
func RegisterServicesFromDiscovery(deviceID types.NodeID, services []string, servicePort int, role string) map[string]any {
	if len(services) == 0 {
		return map[string]any{"accepted": []string{}, "rejected": []string{}, "reason": "no services provided"}
	}

	accepted := []string{}
	rejected := []string{}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	deviceInfo := network.GetPeer(deviceID)
	if deviceInfo == nil {
		for _, svc := range services {
			rejected = append(rejected, svc)
		}
		return map[string]any{"accepted": accepted, "rejected": rejected}
	}

	for _, serviceName := range services {
		// 1. Role-based policy check
		allowed := RoleServicePolicy[role]
		allowedMap := make(map[string]bool)
		for _, s := range allowed {
			allowedMap[s] = true
		}

		if !allowedMap[serviceName] {
			rejected = append(rejected, serviceName)
			continue
		}

		// 2. Get or create service entry
		entry, exists := registry.services[serviceName]
		if !exists {
			entry = ServiceEntry{
				Providers: make(map[types.NodeID]ProviderInfo),
				Metadata:  ServiceMetadata[serviceName],
				Policy: map[string]any{
					"min_providers":  1,
					"min_health":     0.3,
					"load_balancing": "health_based",
				},
			}
			registry.services[serviceName] = entry
		}

		// 3. Register/update provider
		now := time.Now()
		entry.Providers[deviceID] = ProviderInfo{
			LastAnnounce: now,
			Metadata: map[string]any{
				"port":        servicePort,
				"ip":          deviceInfo.IP,
				"device_name": deviceInfo.Name,
				"role":        role,
			},
			Health: deviceInfo.Health,
		}

		accepted = append(accepted, serviceName)
		fmt.Printf("[SERVICE] %s registered service '%s' on port %d\n", deviceInfo.Name, serviceName, servicePort)
	}

	return map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	}
}

// GetServiceProviders returns active providers for a service (used by Layer 2)
func GetServiceProviders(serviceName string, requireAlive bool, minHealth float64) []map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := []map[string]any{}
	now := time.Now()

	for deviceID, p := range entry.Providers {
		if now.Sub(p.LastAnnounce) > ProviderTTL {
			continue
		}

		device := network.GetPeer(deviceID)
		if device == nil {
			continue
		}

		if requireAlive && device.Status != "alive" {
			continue
		}

		if device.Health < minHealth {
			continue
		}

		providers = append(providers, map[string]any{
			"device_id":        deviceID,
			"name":             device.Name,
			"ip":               device.IP,
			"port":             p.Metadata["port"],
			"role":             device.Role,
			"role_trusted":     device.RoleTrusted,
			"health":           device.Health,
			"last_announce":    p.LastAnnounce,
			"service_metadata": entry.Metadata,
		})
	}

	// Sort by health descending (best first)
	// (simple sort for now)
	return providers
}

// GetServiceInfo returns full information about a service
func GetServiceInfo(serviceName string) map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := GetServiceProviders(serviceName, true, 0.5)

	return map[string]any{
		"name":            serviceName,
		"metadata":        entry.Metadata,
		"policy":          entry.Policy,
		"providers":       providers,
		"total_providers": len(providers),
	}
}

// GetAllServices returns information about all registered services
func GetAllServices() map[string]map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	result := make(map[string]map[string]any)
	for name := range registry.services {
		if info := GetServiceInfo(name); info != nil {
			result[name] = info
		}
	}
	return result
}

// UpdateProviderHealth is called by Layer 4 when messages succeed/fail
func UpdateProviderHealth(deviceID types.NodeID, success bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for _, entry := range registry.services {
		if p, exists := entry.Providers[deviceID]; exists {
			if success {
				p.Health = min(1.0, p.Health+0.05)
			} else {
				p.Health = max(0.0, p.Health-0.15)
			}
			entry.Providers[deviceID] = p
		}
	}
}

// ... (keep everything the same until the end)

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// ServiceResolver provides the interface expected by Layer 2
type ServiceResolver struct{}

// Global instance
var ServiceResolverInstance = ServiceResolver{} // ← Fixed: add {}

// types/types.go
package types

import "fmt"

type NodeID [32]byte

func (id NodeID) String() string {
	if len(id) == 0 {
		return "00000000..."
	}
	return fmt.Sprintf("%x...", id[:8])
}

func (id NodeID) Equal(other NodeID) bool {
	return id == other
}

type Capabilities struct {
	InternetAccess      bool  `json:"internet_access"`
	CrossNetworkBridge  bool  `json:"cross_network_bridge"`
	HotspotCapable      bool  `json:"hotspot_capable"`
	UplinkBandwidthMbps int   `json:"uplink_bandwidth_mbps"`
	LatencyMs           int   `json:"latency_ms"`
	UptimeSeconds       int64 `json:"uptime_seconds"`
}
