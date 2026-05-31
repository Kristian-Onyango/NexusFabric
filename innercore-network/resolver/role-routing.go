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
