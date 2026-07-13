//layer 1

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
