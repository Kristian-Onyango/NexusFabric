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
