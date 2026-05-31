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
