Check on destination region to ensure that it is not fixed say like in Nairobi alone or the source as well.

message.go 
//check on this function 
	case packet.PacketTypeKademliaPing, packet.PacketTypeKademliaPong,
		packet.PacketTypeKademliaFindNode, packet.PacketTypeKademliaFindValue, packet.PacketTypeKademliaStore:
		m.innerCore.HandleKademliaPacket(pkt)
            to this check on it and fix it 
	case packet.PacketTypeKademliaPing, packet.PacketTypeKademliaPong,
		packet.PacketTypeKademliaFindNode, packet.PacketTypeKademliaFindValue, packet.PacketTypeKademliaStore:
		// TODO: Fix proper dispatch later
		fmt.Printf("[MESSAGE] Kademlia packet received: %s\n", pkt.Header.PacketType)

I gave myself a fixed IP address in lookup.go.There may be need to change that later on
		// Always include self
	result = append(result, PeerInfo{
		NodeID:   ic.nodeID,
		IP:       "127.0.0.1",
		LastSeen: time.Now().Unix(),
		Score:    100.0,
	})

message.go 
	//Need to check on this line of code later on since it makes no sense as of now
		message.KademliaHandler = integrator.innerCore.HandleKademliaPacket
