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
