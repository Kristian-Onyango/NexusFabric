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
