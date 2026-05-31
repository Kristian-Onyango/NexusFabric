// innercore/lookup.go
package innercore

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// Lookup performs a real iterative Kademlia lookup.
//
// THE PROBLEM WITH THE OLD VERSION:
// The old code fired SendFindNode() inside goroutines and then immediately read
// from getAlphaClosest() — which is LOCAL state. It never waited for the UDP reply
// to come back from the remote node. The network round-trip takes milliseconds but
// the goroutine had already closed the channel and moved on. This meant every
// "iteration" was just re-reading your own routing table, not the network.
//
// THE FIX:
// For each peer we query, we register a pendingLookup channel BEFORE sending the
// packet. SendFindNodeAndWait sends the packet then blocks on that channel with a
// timeout. When HandleKademliaPacket receives a KADEMLIA_FIND_NODE_REPLY, it calls
// resolvePendingLookup which delivers the real remote peers into that channel.
// Now each iteration actually uses what the network told us.
func (ic *InnerCore) Lookup(target types.NodeID) ([]PeerInfo, error) {
	fmt.Printf("[KADEMLIA] Starting lookup for target %s\n", target)

	// Guard: prevent lookup if we don't have our own NodeID yet
	if ic.nodeID == (types.NodeID{}) {
		fmt.Println("[KADEMLIA] WARNING: NodeID not set yet. Returning empty result.")
		return nil, fmt.Errorf("nodeID not initialized")
	}

	if target.Equal(ic.nodeID) {
		fmt.Println("[KADEMLIA] Self lookup - returning self")
		return []PeerInfo{{
			NodeID:   ic.nodeID,
			IP:       "127.0.0.1",
			LastSeen: time.Now().Unix(),
			Score:    100.0,
		}}, nil
	}

	alpha := 3
	// Seed the first round from our local routing table
	closest := ic.getAlphaClosest(target, alpha)

	// Fallback: if we have nothing in k-buckets yet, use the network table directly.
	// This happens on a fresh node that just joined and hasn't filled buckets yet.
	if len(closest) == 0 {
		fmt.Println("[KADEMLIA] No peers in buckets, falling back to network table")
		peers := network.GetAllPeers()
		for _, p := range peers {
			closest = append(closest, PeerInfo{
				NodeID:       p.NodeID,
				IP:           p.IP,
				Capabilities: p.Capabilities,
				LastSeen:     p.LastSeen,
				Score:        p.Score,
			})
		}
	}

	seen := make(map[types.NodeID]bool)
	var result []PeerInfo
	start := time.Now()

	for iteration := 0; len(closest) > 0 && iteration < 5 && time.Since(start) < 5*time.Second; iteration++ {
		fmt.Printf("[KADEMLIA] Iteration %d | Querying %d peers\n", iteration, len(closest))

		var wg sync.WaitGroup
		var newPeersFromNetwork []PeerInfo
		var newPeersMu sync.Mutex

		for _, p := range closest {
			if seen[p.NodeID] {
				continue
			}
			seen[p.NodeID] = true

			wg.Add(1)
			go func(peer PeerInfo) {
				defer wg.Done()

				// SendFindNodeAndWait registers a pending channel, sends the packet,
				// then waits up to 2 seconds for the real reply from the remote node.
				remotePeers := ic.SendFindNodeAndWait(peer.NodeID, target)

				if len(remotePeers) > 0 {
					fmt.Printf("[KADEMLIA] Got %d peers from %s reply\n", len(remotePeers), peer.NodeID)
					newPeersMu.Lock()
					newPeersFromNetwork = append(newPeersFromNetwork, remotePeers...)
					newPeersMu.Unlock()

					// Feed discovered peers into our routing table so they persist
					// beyond this lookup — this is how the table grows over time.
					for _, rp := range remotePeers {
						ic.SeedPeer(rp.NodeID, rp.IP, rp.Capabilities)
					}
				}
			}(p)
		}

		wg.Wait()

		// If the network gave us new peers, use those for the next iteration.
		// If not (e.g. all timeouts), escalate to supernodes.
		if len(newPeersFromNetwork) > 0 {
			closest = newPeersFromNetwork
			result = append(result, newPeersFromNetwork...)
		} else if len(ic.supernodes) > 0 {
			fmt.Println("[KADEMLIA] All queries timed out → escalating to supernodes")
			closest = ic.supernodes
			result = append(result, ic.supernodes...)
		} else {
			fmt.Println("[KADEMLIA] No replies and no supernodes — stopping lookup")
			break
		}
	}

	// Always include self so callers always get at least one result
	result = append(result, PeerInfo{
		NodeID:   ic.nodeID,
		IP:       "127.0.0.1",
		LastSeen: time.Now().Unix(),
		Score:    100.0,
	})

	fmt.Printf("[KADEMLIA] Lookup completed. Found %d peers\n", len(result))
	return result, nil
}
