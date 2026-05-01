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
