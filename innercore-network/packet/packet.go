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
