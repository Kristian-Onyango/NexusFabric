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
