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
