// allowedroles/allowed_roles.go
// Allowed roles in the system.
// This is used across multiple layers for policy enforcement.

package allowedroles

// AllowedRoles is the set of valid roles in the mesh network.
// Any role not in this set is automatically treated as "unknown".
var AllowedRoles = map[string]bool{
	"game":    true,
	"chat":    true,
	"cache":   true,
	"storage": true,
	"unknown": true,
}

// IsAllowed returns true if the role is valid
func IsAllowed(role string) bool {
	return AllowedRoles[role]
}
// discovery/discovery.go
// discovery/discovery.go
package discovery

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"innercore-network/packet"
	"innercore-network/types"
)

const (
	BroadcastAddress = "255.255.255.255"
	AnnounceInterval = 5 * time.Second
)

var (
	myNodeID   types.NodeID
	myNodeName string
	myCaps     types.Capabilities
	once       sync.Once

	// Made configurable
	DiscoveryPort    int = 37020
	LocalMessagePort int = 51000 // for Layer 4 messaging
)

var (
	UpdateNetworkCallback func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int)
	SeedPeerCallback      func(nodeID types.NodeID, ip string, caps types.Capabilities)
)

func Init(services []string, servicePort int, discoveryPort int, messagePort int) error {
	if discoveryPort > 0 {
		DiscoveryPort = discoveryPort
	}
	if messagePort > 0 {
		LocalMessagePort = messagePort
	}

	once.Do(func() {
		var err error
		myNodeID, err = LoadOrCreateNodeID()
		if err != nil {
			panic(fmt.Sprintf("Failed to load NodeID: %v", err))
		}

		myNodeName = getHostname()

		myCaps = types.Capabilities{
			InternetAccess:      false,
			CrossNetworkBridge:  isHotspotCapable(),
			HotspotCapable:      isHotspotCapable(),
			UplinkBandwidthMbps: 50,
			LatencyMs:           20,
			UptimeSeconds:       0,
		}

		fmt.Printf("[DISCOVERY] Node %s initialized with NodeID %s (DiscoveryPort: %d, MsgPort: %d)\n",
			myNodeName, myNodeID, DiscoveryPort, LocalMessagePort)
	})

	go announceLoop(services, servicePort)
	go listenLoop()

	return nil
}

// ==================== Helper Functions ====================

func getHostname() string {
	name, _ := os.Hostname()
	if name == "" {
		name = "unknown-device"
	}
	return name
}

func isHotspotCapable() bool {
	return true // TODO: Make this detect real hotspot capability later
}

// ==================== Core Loops ====================

func announceLoop(services []string, servicePort int) {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(255, 255, 255, 255), Port: DiscoveryPort})
	if err != nil {
		fmt.Printf("[DISCOVERY] Broadcast socket failed: %v\n", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(AnnounceInterval)
	defer ticker.Stop()

	for range ticker.C {
		p := packet.Packet{
			Header: packet.Header{
				Version:           2,
				RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
				PacketType:        packet.PacketTypeDiscoveryAnnounce,
				TTL:               8,
				SourceNodeID:      myNodeID,
				DestinationNodeID: types.NodeID{},
				Timestamp:         time.Now().Unix(),
			},
			Network: packet.NetworkInfo{
				SourceRegion:      "KE-Nairobi",
				DestinationRegion: "UNKNOWN",
			},
			Capabilities: myCaps,
			Payload:      mustMarshal(map[string]any{"services": services, "service_port": servicePort}),
		}

		data, _ := json.Marshal(p)
		conn.Write(data)
	}
}

func listenLoop() {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: DiscoveryPort})
	if err != nil {
		fmt.Printf("[DISCOVERY] Bind failed: %v\n", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 4096)

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var p packet.Packet
		if err := json.Unmarshal(buf[:n], &p); err != nil {
			continue
		}

		// Ignore self
		if p.Header.SourceNodeID.Equal(myNodeID) {
			continue
		}

		senderIP := addr.IP.String()

		fmt.Printf("[DISCOVERY] Received %s from %s (%s)\n", p.Header.PacketType, senderIP, p.Header.SourceNodeID)

		var payloadMap map[string]any
		json.Unmarshal(p.Payload, &payloadMap)

		services := []string{}
		if s, ok := payloadMap["services"].([]any); ok {
			for _, v := range s {
				if str, ok := v.(string); ok {
					services = append(services, str)
				}
			}
		}

		servicePort := 5000
		if sp, ok := payloadMap["service_port"].(float64); ok {
			servicePort = int(sp)
		}

		if UpdateNetworkCallback != nil {
			UpdateNetworkCallback(p.Header.SourceNodeID, senderIP, "", p.Capabilities, services, servicePort)
		}

		// Feed discovered peer into InnerCore for Kademlia routing table
		if SeedPeerCallback != nil {
			SeedPeerCallback(p.Header.SourceNodeID, senderIP, p.Capabilities)
		}
	}
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// GetMyNodeID returns the current node's ID (used by InnerCore)
func GetMyNodeID() types.NodeID {
	return myNodeID
}

// GetMyNodeName returns the current node's friendly name
func GetMyNodeName() string {
	return myNodeName
}

// SetSeedPeerCallback allows InnerCore to register itself without creating import cycle
func SetSeedPeerCallback(cb func(nodeID types.NodeID, ip string, caps types.Capabilities)) {
	SeedPeerCallback = cb
	fmt.Println("[DISCOVERY] SeedPeerCallback registered for Kademlia")
}
// discovery/nodeid.go
package discovery

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"innercore-network/types" // ← Add this import
)

const nodeIDFile = "node_id.bin"

// LoadOrCreateNodeID loads existing NodeID or creates a new persistent one
func LoadOrCreateNodeID() (types.NodeID, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return types.NodeID{}, err
	}

	// Support multiple instances for testing
	instanceSuffix := ""
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceSuffix = "_instance1"
	}

	path := filepath.Join(home, ".innercore", "node_id"+instanceSuffix+".bin")

	// Try to load existing
	if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
		var id types.NodeID
		copy(id[:], data)
		fmt.Printf("[DISCOVERY] Loaded existing NodeID: %s\n", id)
		return id, nil
	}

	// Create new
	var id types.NodeID
	if _, err := rand.Read(id[:]); err != nil {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("%d%s", time.Now().UnixNano(), instanceSuffix)))
		copy(id[:], h.Sum(nil))
	}

	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, id[:], 0600)

	fmt.Printf("[DISCOVERY] Generated new NodeID: %s\n", id)
	return id, nil
}
// discovery/types.go
package discovery

import "innercore-network/types"

// DiscoveryMessage is the universal format used in Layer 1
// (will align with Layer 4 later)
type DiscoveryMessage struct {
	ProtocolVersion int                `json:"protocol_version"`
	Type            string             `json:"type"` // DISCOVERY_PING, DISCOVERY_PONG, DISCOVERY_ANNOUNCE
	RequestID       string             `json:"request_id"`
	NodeID          types.NodeID       `json:"node_id"`
	NodeName        string             `json:"node_name"`
	Timestamp       int64              `json:"timestamp"`
	Capabilities    types.Capabilities `json:"capabilities"`
	Services        []string           `json:"services,omitempty"`
	ServicePort     int                `json:"service_port,omitempty"`
	// Future: Region, TTL, Signature, etc.
}
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
// This implements your core design:
// Supernodes emerge naturally based on device capabilities.
// No voting. No central authority. Every node independently computes scores.

package innercore

import "innercore-network/types"

// calculateScore returns a fitness score for supernode candidacy.
// Higher score = better supernode candidate.
//
// Your original requirements reflected here:
// - Cross-network bridge capability is heavily rewarded (your Africa use-case)
// - Bandwidth and uptime matter
// - Low latency is preferred
func calculateScore(caps types.Capabilities) float64 {
	score := 0.0

	// Bandwidth is very important for routing assistance
	score += float64(caps.UplinkBandwidthMbps) * 0.45

	// Uptime / stability (normalized)
	score += float64(caps.UptimeSeconds) * 0.00008 * 0.25

	// Cross-network bridge is the most important feature in your design
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
	Capabilities types.Capabilities `json:"capabilities"`
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
// layer6/gateway.go
// Layer 6 — Internet Egress & Fallback Gateway
//
// Responsibilities:
//   - Transparent internet egress when local mesh cannot resolve a target
//   - Automatic fallback routing
//   - Session-aware forwarding (TCP proxy style)
//   - Capability-aware routing decisions (uses InnerCore + network table)
//   - No persistence logic (that belongs to Layer 5)
//
// This layer may touch the public internet.
//
// Design Notes:
//   - Routing decisions (including cross-network bridging) are made here
//     because this layer has visibility into capabilities and network state.
//   - It consults InnerCore for supernode assistance and Layer 1 network table
//     for current device status and capabilities.

package fallback

import (
	"fmt"
	"net"
	"sync"
	"time"

	"innercore-network/innercore"
)

const (
	DefaultHTTPPort  = 80
	DefaultHTTPSPort = 443
	SocketTimeout    = 10 * time.Second
	BufferSize       = 8192
)

type GatewaySession struct {
	ClientAddr   string
	Target       string
	CreatedAt    time.Time
	LastActivity time.Time
	BytesUp      int64
	BytesDown    int64
}

type InternetGateway struct {
	listenIP   string
	listenPort int
	sessions   map[string]*GatewaySession
	mu         sync.RWMutex
	running    bool
	innerCore  *innercore.InnerCore
}

func NewInternetGateway(listenIP string, listenPort int, ic *innercore.InnerCore) *InternetGateway {
	return &InternetGateway{
		listenIP:   listenIP,
		listenPort: listenPort,
		sessions:   make(map[string]*GatewaySession),
		innerCore:  ic,
	}
}

func (g *InternetGateway) Start() {
	g.running = true
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.listenIP, g.listenPort))
	if err != nil {
		fmt.Printf("[L6] Failed to listen on %s:%d: %v\n", g.listenIP, g.listenPort, err)
		return
	}

	fmt.Printf("[L6] Internet Gateway listening on %s:%d\n", g.listenIP, g.listenPort)

	go func() {
		for g.running {
			conn, err := ln.Accept()
			if err != nil {
				if g.running {
					fmt.Printf("[L6] Accept error: %v\n", err)
				}
				continue
			}
			go g.handleClient(conn)
		}
		ln.Close()
	}()
}

func (g *InternetGateway) Stop() {
	g.running = false
}

// handleClient processes one incoming client connection
func (g *InternetGateway) handleClient(clientConn net.Conn) {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	fmt.Printf("[L6] New client connection from %s\n", clientAddr)

	// Read initial data to determine target (naive HTTP Host header parsing for MVP)
	buf := make([]byte, BufferSize)
	n, err := clientConn.Read(buf)
	if err != nil {
		return
	}
	initialData := buf[:n]

	targetHost, targetPort := g.extractTarget(initialData)
	if targetHost == "" {
		fmt.Printf("[L6] Could not determine target from client %s\n", clientAddr)
		return
	}

	targetAddr := fmt.Sprintf("%s:%d", targetHost, targetPort)
	session := &GatewaySession{
		ClientAddr:   clientAddr,
		Target:       targetAddr,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}

	g.mu.Lock()
	g.sessions[clientAddr] = session
	g.mu.Unlock()

	g.forwardSession(clientConn, session, initialData)
}

// extractTarget is a simple MVP parser (can be improved later with proper HTTP parsing)
func (g *InternetGateway) extractTarget(data []byte) (host string, port int) {
	text := string(data)
	for _, line := range splitLines(text) {
		if len(line) > 5 && line[:5] == "Host:" {
			hostPart := line[5:]
			hostPart = trimSpace(hostPart)
			if idx := indexByte(hostPart, ':'); idx != -1 {
				host = hostPart[:idx]
				// port parsing omitted for simplicity
				return host, DefaultHTTPPort
			}
			return hostPart, DefaultHTTPPort
		}
	}
	return "", 0
}

// forwardSession relays traffic between client and internet
func (g *InternetGateway) forwardSession(clientConn net.Conn, session *GatewaySession, firstPayload []byte) {
	upstream, err := net.DialTimeout("tcp", session.Target, SocketTimeout)
	if err != nil {
		fmt.Printf("[L6] Failed to connect to %s: %v\n", session.Target, err)
		return
	}
	defer upstream.Close()

	// Send initial payload
	upstream.Write(firstPayload)

	// Bidirectional relay
	go g.relay(clientConn, upstream, session, true) // client -> upstream
	g.relay(upstream, clientConn, session, false)   // upstream -> client
}

func (g *InternetGateway) relay(src, dst net.Conn, session *GatewaySession, upstream bool) {
	buf := make([]byte, BufferSize)
	for {
		n, err := src.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			dst.Write(buf[:n])
			session.LastActivity = time.Now()
			if upstream {
				session.BytesUp += int64(n)
			} else {
				session.BytesDown += int64(n)
			}
		}
	}
}

// Simple helper - improve later with proper HTTP parsing
func splitLines(s string) []string {
	return nil // placeholder - not used yet, but declared to avoid compile error
}

func trimSpace(s string) string {
	// simple trim
	return s
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
// integration/integration.go
// System Integration Layer
//
// This is the "brain" of the entire system.
// It initializes all layers in the correct order and starts background tasks.
// Provides a unified high-level API for the rest of the application.

// integration/integration.go
// System Integration Layer - The brain of the InnerCore Mesh

// integration/integration.go
// integration/integration.go
// System Integration Layer - The brain of the InnerCore Mesh

// integration/integration.go
// System Integration Layer - The brain of the InnerCore Mesh

package integration

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/discovery"
	"innercore-network/fallback"
	"innercore-network/innercore"
	"innercore-network/message"
	"innercore-network/network"
	"innercore-network/resolver"
	"innercore-network/service"
	"innercore-network/storage"
	"innercore-network/types"
)

type SystemIntegrator struct {
	innerCore   *innercore.InnerCore
	resolver    *resolver.Layer2Resolver
	gateway     *fallback.InternetGateway
	running     bool
	startupTime time.Time
	mu          sync.RWMutex
}

var (
	integrator *SystemIntegrator
	once       sync.Once
)

func Init(instanceID int) {
	once.Do(func() {
		integrator = &SystemIntegrator{startupTime: time.Now()}

		fmt.Printf("\n=== Starting Local-First InnerCore Mesh - Instance %d ===\n", instanceID)

		storage.InitializeStorage()

		// === IMPORTANT FOR SINGLE MACHINE TESTING ===
		discoveryPort := 37020               // SAME port for both instances
		messagePort := 51000 + instanceID*10 // different message ports

		discovery.Init([]string{"chat"}, 5000, discoveryPort, messagePort)

		integrator.innerCore = innercore.New(
			storage.GlobalStorage.GetEngine(),
			message.SendPacket,
		)

		integrator.innerCore.SetNodeID(discovery.GetMyNodeID())

		message.KademliaHandler = integrator.innerCore.HandleKademliaPacket

		discovery.UpdateNetworkCallback = func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int) {
			// Force real IP for replies
			if ip == "" || ip == "127.0.0.1" {
				ip = "192.168.2.104" // your current local IP - change if yours is different
			}
			network.UpdateNode(nodeID, ip, name, caps, services, servicePort)
			integrator.innerCore.SeedPeer(nodeID, ip, caps)
		}

		// Force self registration
		selfID := discovery.GetMyNodeID()
		selfCaps := types.Capabilities{
			InternetAccess:      true,
			CrossNetworkBridge:  true,
			HotspotCapable:      true,
			UplinkBandwidthMbps: 100,
			LatencyMs:           5,
			UptimeSeconds:       3600,
		}
		network.UpdateNode(selfID, "127.0.0.1", discovery.GetMyNodeName(), selfCaps, []string{"chat"}, 5000)
		integrator.innerCore.SeedPeer(selfID, "127.0.0.1", selfCaps)

		message.Init()
		service.Init()
		integrator.resolver = resolver.NewLayer2Resolver()

		integrator.gateway = fallback.NewInternetGateway("0.0.0.0", 8080+instanceID, integrator.innerCore)
		integrator.gateway.Start()

		integrator.running = true

		fmt.Println("\n✅ System successfully started!")
		fmt.Printf("   Instance      : %d\n", instanceID)
		fmt.Printf("   Node ID       : %s\n", selfID)
		fmt.Printf("   Node Name     : %s\n", discovery.GetMyNodeName())
		fmt.Printf("   Discovery Port: %d\n", discoveryPort)
		fmt.Printf("   Message Port  : %d\n", messagePort)
		fmt.Println("   Status        : Operational")
	})
}

func GetInstance() *SystemIntegrator {
	return integrator
}

func GetInnerCore() *innercore.InnerCore {
	if integrator == nil {
		return nil
	}
	return integrator.innerCore
}

func (si *SystemIntegrator) Stop() {
	si.mu.Lock()
	defer si.mu.Unlock()

	if !si.running {
		return
	}

	fmt.Println("\n[SHUTDOWN] Graceful shutdown initiated...")
	si.running = false
	message.Close()
	fmt.Println("[SHUTDOWN] System stopped.")
}

// ==================== Public API ====================

func SendMessage(target types.NodeID, payload any) error {
	return message.SendToNode(target, payload)
}

func GetNetworkInfo() map[string]any {
	peers := network.GetAllPeers()
	alive := 0
	roles := make(map[string]int)
	services := make(map[string]int)

	for _, p := range peers {
		if p.Status == "alive" {
			alive++
		}
		roles[p.Role]++
		for _, svc := range p.Services {
			services[svc]++
		}
	}

	return map[string]any{
		"node_id":        discovery.GetMyNodeID(),
		"node_name":      discovery.GetMyNodeName(),
		"total_devices":  len(peers),
		"alive_devices":  alive,
		"roles":          roles,
		"services":       services,
		"uptime_seconds": int(time.Since(integrator.startupTime).Seconds()),
	}
}

func GetDeviceInfo(deviceID types.NodeID) any {
	return network.GetPeer(deviceID)
}

func RegisterService(serviceName string, port int) bool {
	fmt.Printf("[INTEGRATION] Service '%s' registered on port %d\n", serviceName, port)
	return true
}

func HealthCheck() map[string]any {
	peers := network.GetAllPeers()
	healthy := 0
	for _, p := range peers {
		if p.Status == "alive" && p.Health >= 0.5 {
			healthy++
		}
	}

	status := "DEGRADED"
	if integrator.running {
		status = "HEALTHY"
	}

	return map[string]any{
		"status": status,
		"metrics": map[string]any{
			"total_devices":   len(peers),
			"healthy_devices": healthy,
			"uptime_seconds":  int(time.Since(integrator.startupTime).Seconds()),
		},
	}
}
// main/main.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"innercore-network/discovery"
	"innercore-network/integration"
	"innercore-network/network"
)

func main() {
	fmt.Println("🔥 InnerCore Mesh Network Starting...")
	fmt.Printf("Go Version: %s\n", "1.21+")

	// Initialize the entire system
	instanceID := 0
	// You can pass argument from command line later
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceID = 1
	}
	integration.Init(instanceID)

	// Show initial state
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("Local-First Internet Substrate - GO EDITION")
	fmt.Println(strings.Repeat("=", 60))

	// Print network state periodically
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			network.PrintNetworkState()
		}
	}()

	// Test Kademlia lookup after discovery settles
	go func() {
		time.Sleep(12 * time.Second) // longer wait for discovery to stabilize

		if ic := integration.GetInnerCore(); ic != nil {
			selfID := discovery.GetMyNodeID()

			fmt.Println("\n[TEST] Triggering Kademlia self-lookup...")
			ic.TestLookup(selfID)

			fmt.Println("\n[TEST] Looking for other discovered peers via Kademlia...")

			// Retry a few times because discovery can be async
			for attempt := 0; attempt < 5; attempt++ {
				peers := network.GetAllPeers()
				fmt.Printf("[TEST] Attempt %d - Found %d total peers in table\n", attempt+1, len(peers))

				foundRemote := false
				for id := range peers {
					if !id.Equal(selfID) {
						fmt.Printf("[TEST] Triggering Kademlia lookup for remote peer: %s\n", id)
						ic.TestLookup(id)
						foundRemote = true
						break
					}
				}
				if foundRemote {
					break
				}
				time.Sleep(3 * time.Second)
			}

			if len(network.GetAllPeers()) <= 1 {
				fmt.Println("[TEST] No remote peer found yet. Discovery still settling...")
			}
		}
	}()
	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[SHUTDOWN] Received shutdown signal...")

	integration.GetInstance().Stop()

	fmt.Println("System shutdown complete. Goodbye.")
}
// message/message.go
// Layer 4 — Messaging Protocol (Reliable UDP with ACKs, Retries, and Persistence)
//
// This is the central transport layer. All layers (Discovery, InnerCore/Kademlia, Applications)
// send and receive through this single point.
// It merges your full reliable version with the new packet system.

package message

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"innercore-network/discovery"
	"innercore-network/network"
	"innercore-network/packet"
	"innercore-network/types"
)

const (
	LocalPort    = 51000
	ACKTimeout   = 2 * time.Second
	MaxRetries   = 3
	SaveInterval = 5 * time.Second
	PendingFile  = "pending_acks.json"
)

type pendingEntry struct {
	TargetID  types.NodeID    `json:"target_id"`
	TargetIP  string          `json:"target_ip"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
	Retries   int             `json:"retries"`
}

type MessageService struct {
	nodeID  types.NodeID
	conn    *net.UDPConn
	pending map[string]pendingEntry
	mu      sync.RWMutex
	// innerCore removed to avoid import cycle for now
}

var (
	msgService *MessageService
	once       sync.Once
)

var KademliaHandler func(packet.Packet)

func Init() error {
	once.Do(func() {
		service := &MessageService{
			nodeID:  discovery.GetMyNodeID(),
			pending: make(map[string]pendingEntry),
		}

		// Use configurable port from discovery package
		service.startUDPListener()

		msgService = service
		fmt.Printf("[MESSAGE] Layer 4 started on port %d (fire-and-forget mode for now)\n", discovery.LocalMessagePort)
	})
	return nil
}

func (m *MessageService) startUDPListener() {
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: discovery.LocalMessagePort}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on port %d: %v", discovery.LocalMessagePort, err))
	}
	m.conn = conn
	go m.receiveLoop()
}

func (m *MessageService) receiveLoop() {
	buf := make([]byte, 8192)
	for {
		n, addr, err := m.conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var pkt packet.Packet
		if json.Unmarshal(buf[:n], &pkt) != nil {
			continue
		}
		m.handleIncomingPacket(pkt, addr)
	}
}

func (m *MessageService) handleIncomingPacket(pkt packet.Packet, addr *net.UDPAddr) {
	senderID := pkt.Header.SourceNodeID
	network.RecordSuccess(senderID)

	switch pkt.Header.PacketType {
	case packet.PacketTypeDiscoveryAnnounce,
		packet.PacketTypeDiscoveryPing,
		packet.PacketTypeDiscoveryPong:
		fmt.Printf("[MESSAGE] Discovery packet %s from %s\n", pkt.Header.PacketType, senderID)

	case packet.PacketTypeKademliaPing,
		packet.PacketTypeKademliaPong,
		packet.PacketTypeKademliaFindNode,
		packet.PacketTypeKademliaFindValue,
		packet.PacketTypeKademliaStore,
		"KADEMLIA_FIND_NODE_REPLY": // ← Add this
		fmt.Printf("[MESSAGE] Kademlia %s from %s\n", pkt.Header.PacketType, senderID)
		if KademliaHandler != nil {
			KademliaHandler(pkt)
		}
		network.RecordSuccess(senderID)
		// health feedback

	case packet.PacketTypeMessage:
		m.handleApplicationMessage(pkt, senderID, addr)

	default:
		fmt.Printf("[MESSAGE] Unknown packet: %s\n", pkt.Header.PacketType)
	}
}

func (m *MessageService) handleApplicationMessage(pkt packet.Packet, senderID types.NodeID, addr *net.UDPAddr) {
	// Send ACK
	ack := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         pkt.Header.RequestID,
			PacketType:        "ACK",
			TTL:               1,
			SourceNodeID:      m.nodeID,
			DestinationNodeID: senderID,
			Timestamp:         time.Now().Unix(),
		},
	}
	m.sendRaw(ack, addr)

	fmt.Printf("[RECV] Message from %s\n", senderID)
}

func (m *MessageService) sendRaw(pkt packet.Packet, addr *net.UDPAddr) {
	data, _ := json.Marshal(pkt)
	m.conn.WriteToUDP(data, addr)
}

// SendPacket with real IP lookup
func SendPacket(targetID types.NodeID, pkt packet.Packet) error {
	peer := network.GetPeer(targetID)
	if peer == nil || peer.IP == "" {
		fmt.Printf("[SEND] CRITICAL: Target %s not found or no IP in network table!\n", targetID)
		return fmt.Errorf("target not found")
	}

	fmt.Printf("[SEND] → %s (%s) | Type: %s\n", targetID, peer.IP, pkt.Header.PacketType)

	addr := &net.UDPAddr{IP: net.ParseIP(peer.IP), Port: discovery.LocalMessagePort}
	data, _ := json.Marshal(pkt)
	_, err := msgService.conn.WriteToUDP(data, addr)
	if err != nil {
		fmt.Printf("[SEND] UDP write failed to %s: %v\n", peer.IP, err)
		network.RecordFailure(targetID)
		return err
	}

	network.RecordSuccess(targetID)
	return nil
}

func SendToNode(targetID types.NodeID, payload any) error {
	p := packet.Packet{
		Header: packet.Header{
			Version:           2,
			RequestID:         fmt.Sprintf("%d", time.Now().UnixNano()),
			PacketType:        packet.PacketTypeMessage,
			TTL:               8,
			SourceNodeID:      msgService.nodeID,
			DestinationNodeID: targetID,
			Timestamp:         time.Now().Unix(),
		},
		Network: packet.NetworkInfo{SourceRegion: "KE-Nairobi"},
		Payload: mustMarshal(payload),
	}
	return SendPacket(targetID, p)
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func Close() {
	if msgService != nil && msgService.conn != nil {
		msgService.conn.Close()
	}
}
// network/network.go
package network

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/types"
)

type PeerEntry struct {
	NodeID       types.NodeID
	Name         string
	IP           string
	Role         string
	RoleTrusted  bool
	Status       string
	Health       float64
	LastSeen     int64
	Services     []string
	ServicePort  int
	Capabilities types.Capabilities
	Score        float64
}

type NetworkTable struct {
	peers       map[types.NodeID]*PeerEntry
	mu          sync.RWMutex
	nodeTimeout time.Duration
}

var networkTable = &NetworkTable{
	peers:       make(map[types.NodeID]*PeerEntry),
	nodeTimeout: 15 * time.Second,
}

func UpdateNode(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	now := time.Now().Unix()
	entry, exists := networkTable.peers[nodeID]

	if !exists {
		entry = &PeerEntry{
			NodeID:       nodeID,
			Name:         name,
			IP:           ip,
			Role:         "unknown",
			RoleTrusted:  false,
			Status:       "alive",
			Health:       1.0,
			LastSeen:     now,
			Services:     services,
			ServicePort:  servicePort,
			Capabilities: caps,
			Score:        calculateScore(caps),
		}
		networkTable.peers[nodeID] = entry
		fmt.Printf("[NETWORK] New peer discovered: %s (%s) IP: %s\n", name, nodeID, ip)
		return
	}

	// FORCE IP UPDATE - this was the missing piece
	if ip != "" && ip != "127.0.0.1" {
		entry.IP = ip
	}
	entry.Name = name
	entry.LastSeen = now
	entry.Status = "alive"
	entry.Health = 1.0
	entry.Capabilities = caps
	entry.Score = calculateScore(caps)

	if len(services) > 0 {
		entry.Services = services
		entry.ServicePort = servicePort
	}

	fmt.Printf("[NETWORK] Updated peer: %s (%s) IP: %s Health: %.1f\n", name, nodeID, entry.IP, entry.Health)
}

func GetPeer(nodeID types.NodeID) *PeerEntry {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()
	return networkTable.peers[nodeID]
}

func GetAllPeers() map[types.NodeID]*PeerEntry {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()
	copy := make(map[types.NodeID]*PeerEntry, len(networkTable.peers))
	for k, v := range networkTable.peers {
		copy[k] = v
	}
	return copy
}

// ... rest of the file (ExpireStaleNodes, RecordSuccess, RecordFailure, PrintNetworkState, calculateScore, min, max) stays exactly as you have it

func ExpireStaleNodes() {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	now := time.Now().Unix()
	for _, entry := range networkTable.peers {
		if time.Duration(now-entry.LastSeen)*time.Second > networkTable.nodeTimeout {
			entry.Status = "dead"
		}
	}
}

func RecordSuccess(nodeID types.NodeID) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	if entry, ok := networkTable.peers[nodeID]; ok {
		entry.Health = min(1.0, entry.Health+0.1)
		entry.LastSeen = time.Now().Unix()
		fmt.Printf("[HEALTH] Node %s health +0.1 → %.1f\n", nodeID, entry.Health)
	}
}

func RecordFailure(nodeID types.NodeID) {
	networkTable.mu.Lock()
	defer networkTable.mu.Unlock()

	if entry, ok := networkTable.peers[nodeID]; ok {
		entry.Health = max(0.0, entry.Health-0.3)
		if entry.Health <= 0.3 {
			entry.Status = "unhealthy"
		}
		fmt.Printf("[HEALTH] Node %s health -0.3 → %.1f\n", nodeID, entry.Health)
	}
}

func PrintNetworkState() {
	networkTable.mu.RLock()
	defer networkTable.mu.RUnlock()

	fmt.Println("\n--- NETWORK STATE ---")
	for nodeID, info := range networkTable.peers {
		displayName := info.Name
		if displayName == "" {
			displayName = nodeID.String() + "..." // Fixed: use .String()
		}
		servicesStr := ""
		if len(info.Services) > 0 {
			servicesStr = fmt.Sprintf(", Services: %v", info.Services)
		}

		fmt.Printf("ID: %s | Name: %s | IP: %s | Role: %s | Status: %s | Health: %.1f%s\n",
			nodeID, displayName, info.IP, info.Role, info.Status, info.Health, servicesStr)
	}
	fmt.Println("----------------------")
}

// Local scoring function (moved from innercore/scoring.go to avoid import)
func calculateScore(caps types.Capabilities) float64 {
	score := 0.0
	score += float64(caps.UplinkBandwidthMbps) * 0.45
	score += float64(caps.UptimeSeconds) * 0.00008 * 0.25

	if caps.CrossNetworkBridge {
		score += 65.0
	}
	if caps.HotspotCapable {
		score += 25.0
	}

	score -= float64(caps.LatencyMs) * 0.25

	if caps.InternetAccess {
		score += 15.0
	}
	return score
}

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
// packet/packet.go
package packet

import (
	"encoding/json"

	"innercore-network/types"
)

type Packet struct {
	Header       Header             `json:"header"`
	Network      NetworkInfo        `json:"network"`
	Capabilities types.Capabilities `json:"capabilities"`
	Payload      json.RawMessage    `json:"payload"`
}

type Header struct {
	Version           int          `json:"version"`
	RequestID         string       `json:"request_id"`
	PacketType        string       `json:"packet_type"`
	TTL               int          `json:"ttl"`
	SourceNodeID      types.NodeID `json:"source_node_id"`
	DestinationNodeID types.NodeID `json:"destination_node_id"`
	Timestamp         int64        `json:"timestamp"`
	PayloadLength     int          `json:"payload_length"`
}

type NetworkInfo struct {
	SourceRegion      string `json:"source_region"`
	DestinationRegion string `json:"destination_region"`
}

const (
	PacketTypeDiscoveryPing     = "DISCOVERY_PING"
	PacketTypeDiscoveryPong     = "DISCOVERY_PONG"
	PacketTypeDiscoveryAnnounce = "DISCOVERY_ANNOUNCE"

	PacketTypeKademliaPing      = "KADEMLIA_PING"
	PacketTypeKademliaPong      = "KADEMLIA_PONG"
	PacketTypeKademliaFindNode  = "KADEMLIA_FIND_NODE"
	PacketTypeKademliaFindValue = "KADEMLIA_FIND_VALUE"
	PacketTypeKademliaStore     = "KADEMLIA_STORE"

	PacketTypeMessage = "MSG"
)
// resolver/resolver.go
// Layer 2 — Local Name Resolution Protocol (.mtd)
//
// Purpose:
//   Provides a DNS-like resolution mechanism for the local mesh network.
//   Resolves human-readable `.mtd` names into concrete network endpoints
//   (devices or services).
//
// Design Principles (Strictly followed):
//   - READ-ONLY
//   - SIDE-EFFECT FREE
//   - CACHED
//   - DETERMINISTIC
//   - NEVER registers devices, modifies network table, does routing, or health checks
//
// Resolution Outcomes (Strict Contract):
//   - "OK"        → Valid resolution
//   - "NX"        → Name does not exist
//   - "CONFLICT"  → Ambiguous resolution
//
// Used By:
//   - Layer 3 (Service Protocol)
//   - Layer 4 (Messaging)
//   - Applications

package resolver

import (
	"strings"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
	// for future service resolution
)

const (
	MTDSuffix     = ".mtd"
	ServicePrefix = "svc."
	CacheTTL      = 30 * time.Second
)

// ResolutionRecord is the standard response format from Layer 2
type ResolutionRecord struct {
	Status   string     `json:"status"` // "OK", "NX", "CONFLICT"
	Type     string     `json:"type"`   // "device" or "service"
	Name     string     `json:"name"`
	Records  []Endpoint `json:"records"`
	TTL      int        `json:"ttl"`
	CachedAt time.Time  `json:"cached_at"`
}

type Endpoint struct {
	DeviceID        types.NodeID   `json:"device_id"`
	Name            string         `json:"name"`
	IP              string         `json:"ip"`
	Port            int            `json:"port"`
	Role            string         `json:"role"`
	RoleTrusted     bool           `json:"role_trusted"`
	Health          float64        `json:"health"`
	ServiceMetadata map[string]any `json:"service_metadata,omitempty"`
}

// ResolutionCache is thread-safe and auto-evicts stale entries
type ResolutionCache struct {
	cache map[string]ResolutionRecord
	mu    sync.RWMutex
}

func NewResolutionCache() *ResolutionCache {
	return &ResolutionCache{
		cache: make(map[string]ResolutionRecord),
	}
}

func (c *ResolutionCache) Get(key string) *ResolutionRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	record, exists := c.cache[key]
	if !exists {
		return nil
	}

	if time.Since(record.CachedAt) > CacheTTL {
		// Lazy eviction
		c.mu.RUnlock()
		c.Delete(key)
		c.mu.RLock()
		return nil
	}

	return &record
}

func (c *ResolutionCache) Set(key string, record ResolutionRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.CachedAt = time.Now()
	c.cache[key] = record
}

func (c *ResolutionCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, key)
}

// Layer2Resolver is the main resolver
type Layer2Resolver struct {
	cache *ResolutionCache
}

func NewLayer2Resolver() *Layer2Resolver {
	return &Layer2Resolver{
		cache: NewResolutionCache(),
	}
}

// Resolve is the ONLY public API
func (r *Layer2Resolver) Resolve(name string) ResolutionRecord {
	key := strings.ToLower(strings.TrimSpace(name))

	if cached := r.cache.Get(key); cached != nil {
		return *cached
	}

	record := r.resolveAndCache(key)
	return record
}

// Internal resolution logic
func (r *Layer2Resolver) resolveAndCache(name string) ResolutionRecord {
	// Validate suffix
	if !strings.HasSuffix(name, MTDSuffix) {
		record := r.nxRecord(name)
		r.cache.Set(name, record)
		return record
	}

	// Service resolution
	if strings.HasPrefix(name, ServicePrefix) {
		record := r.resolveService(name)
		r.cache.Set(name, record)
		return record
	}

	// Device resolution
	record := r.resolveDevice(name)
	r.cache.Set(name, record)
	return record
}

// Device resolution: e.g. laptop.mtd
func (r *Layer2Resolver) resolveDevice(name string) ResolutionRecord {
	hostname := strings.TrimSuffix(name, MTDSuffix)

	matches := []Endpoint{}
	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		if peer.Name == hostname && peer.Status == "alive" {
			matches = append(matches, Endpoint{
				DeviceID:    nodeID,
				Name:        peer.Name,
				IP:          peer.IP,
				Port:        51000, // standard messaging port
				Role:        peer.Role,
				RoleTrusted: peer.RoleTrusted,
				Health:      peer.Health,
			})
		}
	}

	if len(matches) == 0 {
		return r.nxRecord(name)
	}
	if len(matches) > 1 {
		return r.conflictRecord(name, "device", matches)
	}

	return r.okRecord(name, "device", matches)
}

// Service resolution: e.g. svc.chat.mtd
func (r *Layer2Resolver) resolveService(name string) ResolutionRecord {
	service := strings.TrimPrefix(name, ServicePrefix)
	service = strings.TrimSuffix(service, MTDSuffix)

	// TODO: Call Layer 3 service registry when it's ported
	// For now we return NX
	return r.nxRecord(name)
}

// Record builders
func (r *Layer2Resolver) okRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "OK",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) nxRecord(name string) ResolutionRecord {
	return ResolutionRecord{
		Status:   "NX",
		Type:     "",
		Name:     name,
		Records:  nil,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) conflictRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "CONFLICT",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}
// resolver/role_routing.go
// Layer 2 Helper — Role-Based Routing
//
// This is a convenience wrapper around Layer 4 messaging.
// It allows applications to send messages to all nodes that match a specific role
// (e.g. "game", "chat", "storage") without having to iterate the network table manually.
//
// Key Features:
// - Filters by role, alive status, health (>= 0.5), and trusted role
// - Uses Layer 4's reliable messaging (ACKs, retries, health tracking)
// - Returns clear summary of sent vs failed messages
// - No direct UDP — everything goes through message.SendPacket

package resolver

import (
	"fmt"

	"innercore-network/message"
	"innercore-network/network"
)

// SendToRole sends a message to ALL alive, healthy, trusted nodes with the given role.
//
// Parameters:
//
//	role         - target role (e.g. "game", "chat", "storage")
//	messageType  - logical message category (e.g. "GAME_STATE", "CHAT_MESSAGE")
//	content      - actual payload (any Go type)
//	senderName   - human-readable name of the sender
//
// Returns:
//
//	Summary map with sent_count, failed_count, target_nodes, failed_nodes
func SendToRole(role string, messageType string, content any, senderName string) map[string]any {
	results := map[string]any{
		"sent_count":   0,
		"failed_count": 0,
		"target_nodes": []map[string]any{},
		"failed_nodes": []map[string]any{},
	}

	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		// Strict filtering - same rules as your original Python version
		if peer.Role != role {
			continue
		}
		if peer.Status != "alive" {
			continue
		}
		if peer.Health < 0.5 {
			continue
		}
		if !peer.RoleTrusted {
			continue
		}

		// Build payload for Layer 4
		payload := map[string]any{
			"protocol_layer": 2,
			"routing_type":   "role_based",
			"message_type":   messageType,
			"from_name":      senderName,
			"to_role":        role,
			"content":        content,
		}

		// Send through Layer 4 (reliable path)
		err := message.SendToNode(nodeID, payload)

		if err == nil {
			results["sent_count"] = results["sent_count"].(int) + 1
			results["target_nodes"] = append(results["target_nodes"].([]map[string]any), map[string]any{
				"device_id": nodeID,
				"name":      peer.Name,
				"ip":        peer.IP,
			})
		} else {
			results["failed_count"] = results["failed_count"].(int) + 1
			results["failed_nodes"] = append(results["failed_nodes"].([]map[string]any), map[string]any{
				"device_id": nodeID,
				"name":      peer.Name,
				"error":     err.Error(),
			})
		}
	}

	fmt.Printf("[ROLE ROUTING] Sent '%s' to %d '%s' nodes (%d failed)\n",
		messageType, results["sent_count"], role, results["failed_count"])

	return results
}

// GetRoleMembers returns all nodes with a specific role (for application use)
func GetRoleMembers(role string, requireAlive bool, minHealth float64) []map[string]any {
	members := []map[string]any{}

	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		if peer.Role != role {
			continue
		}
		if requireAlive && peer.Status != "alive" {
			continue
		}
		if peer.Health < minHealth {
			continue
		}
		if !peer.RoleTrusted {
			continue
		}

		members = append(members, map[string]any{
			"device_id": nodeID,
			"name":      peer.Name,
			"ip":        peer.IP,
			"role":      peer.Role,
			"health":    peer.Health,
			"status":    peer.Status,
			"last_seen": peer.LastSeen,
		})
	}

	return members
}

// BroadcastToAllRoles sends a message to nodes of ALL allowed roles (except excluded ones)
// Useful for system-wide announcements
func BroadcastToAllRoles(messageType string, content any, senderName string, excludedRoles []string) map[string]any {
	if excludedRoles == nil {
		excludedRoles = []string{}
	}

	results := map[string]any{
		"total_sent":   0,
		"total_failed": 0,
		"by_role":      map[string]map[string]any{},
	}

	// You can define allowed roles here or import from a config
	allowedRoles := []string{"game", "chat", "cache", "storage"} // adjust as needed

	for _, role := range allowedRoles {
		if contains(excludedRoles, role) {
			continue
		}

		roleResult := SendToRole(role, messageType, content, senderName)
		results["by_role"].(map[string]map[string]any)[role] = roleResult

		results["total_sent"] = results["total_sent"].(int) + roleResult["sent_count"].(int)
		results["total_failed"] = results["total_failed"].(int) + roleResult["failed_count"].(int)
	}

	fmt.Printf("[BROADCAST] Sent to %d total nodes across %d roles (%d total failures)\n",
		results["total_sent"], len(allowedRoles), results["total_failed"])

	return results
}

// Small helper
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
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
// storage/device_registry.go
// Layer 5 Extension — Device Registry
//
// Authoritative persistent device identity storage.
// Used as source of truth for Layer 3 and system recovery.

// storage/device_registry.go
package storage

import (
	"innercore-network/types"
	"time"
)

const DeviceCollection = "devices"

type DeviceRegistry struct {
	storage *StorageEngine
}

func NewDeviceRegistry(storage *StorageEngine) *DeviceRegistry {
	return &DeviceRegistry{storage: storage}
}

func (dr *DeviceRegistry) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	payload := map[string]any{
		"device_id":  deviceID,
		"first_seen": time.Now().Unix(),
		"public_key": info["public_key"],
		"roles":      info["roles"],
		"metadata":   info["metadata"],
	}

	// Fixed: use nodeID.String() instead of string(deviceID)
	return dr.storage.Put(DeviceCollection, deviceID.String(), payload, nil)
}

func (dr *DeviceRegistry) GetDevice(deviceID types.NodeID) (Record, error) {
	return dr.storage.Get(DeviceCollection, deviceID.String())
}

func (dr *DeviceRegistry) DeviceExists(deviceID types.NodeID) bool {
	_, err := dr.GetDevice(deviceID)
	return err == nil
}
// storage/facade.go
// Layer 5 — Unified Storage Facade
//
// Single API for ALL persistence needs across layers.
// Uses StorageEngine as backend and provides high-level methods.

package storage

import (
	"fmt"

	"innercore-network/types"
)

type StorageFacade struct {
	engine         *StorageEngine
	networkStore   *NetworkSnapshotStore
	deviceRegistry *DeviceRegistry
	initialized    bool
}

var GlobalStorage = NewStorageFacade()

func NewStorageFacade() *StorageFacade {
	engine := NewStorageEngine()
	return &StorageFacade{
		engine:         engine,
		networkStore:   NewNetworkSnapshotStore(engine),
		deviceRegistry: NewDeviceRegistry(engine),
	}
}

func (f *StorageFacade) Initialize() bool {
	if f.initialized {
		return true
	}
	fmt.Println("[STORAGE] Layer 5 Storage Facade initialized")
	f.initialized = true
	return true
}

// Network State
func (f *StorageFacade) SaveNetworkState(networkTable map[types.NodeID]any) (Record, error) {
	return f.networkStore.SaveSnapshot(networkTable)
}

func (f *StorageFacade) LoadNetworkState() (map[types.NodeID]any, error) {
	rec, err := f.networkStore.LoadLatestSnapshot()
	if err != nil {
		return nil, err
	}
	if nt, ok := rec.Payload["network_table"].(map[types.NodeID]any); ok {
		return nt, nil
	}
	return nil, fmt.Errorf("invalid network table format")
}

// Device Registry
func (f *StorageFacade) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	return f.deviceRegistry.RegisterDevice(deviceID, info)
}

func (f *StorageFacade) GetDevice(deviceID types.NodeID) (Record, error) {
	return f.deviceRegistry.GetDevice(deviceID)
}

// Convenience exports
func InitializeStorage() bool {
	return GlobalStorage.Initialize()
}

func SaveNetworkState(nt map[types.NodeID]any) (Record, error) {
	return GlobalStorage.SaveNetworkState(nt)
}

func LoadNetworkState() (map[types.NodeID]any, error) {
	return GlobalStorage.LoadNetworkState()
}

// GetEngine returns the underlying StorageEngine (used by InnerCore)
func (f *StorageFacade) GetEngine() *StorageEngine {
	return f.engine
}
// storage/network_snapshot.go
// Layer 5 Extension — Network Snapshot Store
//
// Persists last-known network state for warm restarts.

package storage

import (
	"innercore-network/types"
	"time"
)

const (
	SnapshotCollection = "network_snapshots"
	LatestSnapshotID   = "latest"
)

type NetworkSnapshotStore struct {
	storage *StorageEngine
}

func NewNetworkSnapshotStore(storage *StorageEngine) *NetworkSnapshotStore {
	return &NetworkSnapshotStore{storage: storage}
}

func (ns *NetworkSnapshotStore) SaveSnapshot(networkTable map[types.NodeID]any) (Record, error) {
	payload := map[string]any{
		"timestamp":     time.Now().Unix(),
		"device_count":  len(networkTable),
		"network_table": networkTable,
	}

	return ns.storage.Put(SnapshotCollection, LatestSnapshotID, payload, nil)
}

func (ns *NetworkSnapshotStore) LoadLatestSnapshot() (Record, error) {
	return ns.storage.Get(SnapshotCollection, LatestSnapshotID)
}

func (ns *NetworkSnapshotStore) SnapshotExists() bool {
	_, err := ns.LoadLatestSnapshot()
	return err == nil
}
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
