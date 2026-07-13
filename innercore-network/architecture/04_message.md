## Layer 4 — Messaging Protocol
```
================================================
PACKAGE: message
================================================

LAYER:
4

PURPOSE:
Universal transport layer for NexusFabric.

Acts as the communication backbone used by
all higher layers and applications.

RESPONSIBILITIES:

✓ Send packets
✓ Receive packets
✓ Packet routing
✓ Reliability (ACKs)
✓ Retries
✓ Delivery tracking
✓ Health feedback

USED BY:

Discovery
InnerCore
Storage
Chat
File Transfer
VoIP
Video Streaming

DEPENDENCIES:

discovery
network
packet
types

STATUS:

Transport Layer       ✓ Implemented
Packet Router         ✓ Implemented
Reliability Layer     ⚠ Partial
Persistence Layer     ⚠ Stub
```
## File: message.go
```
================================================
FILE: message.go
================================================

PURPOSE:

Central transport engine of NexusFabric.

Owns:

- UDP socket
- Packet sending
- Packet receiving
- Packet dispatching
- Reliability state

MAIN FLOW:

Application
      ↓
SendToNode()
      ↓
SendPacket()
      ↓
UDP Network
      ↓
receiveLoop()
      ↓
handleIncomingPacket()
      ↓
Application Handler
```
## Main Struct
```
================================================
STRUCT: MessageService
================================================

PURPOSE:

Owns all Layer 4 state.

FIELDS:

nodeID
Purpose:
Local node identity.

conn
Purpose:
UDP socket.

pending
Purpose:
Messages waiting for ACK.

mu
Purpose:
Thread safety for pending state.
```
## Exported Functions

***Init()***
```
FUNCTION:
Init()

PURPOSE:

Starts Layer 4.

RESPONSIBILITIES:

✓ Create MessageService
✓ Get local NodeID
✓ Open UDP listener
✓ Start receive loop

IMPORTANCE:

★★★★★ Critical

Without this Layer 4 does not exist.
```

***SendPacket()***
```
FUNCTION:

SendPacket()

PURPOSE:

Universal packet sender.

FLOW:

Target NodeID
       ↓
network.GetPeer()
       ↓
IP Address
       ↓
Marshal Packet
       ↓
UDP Send

IMPORTANCE:

★★★★★ Critical

Every layer eventually uses this.
```

***SendToNode()***
```
FUNCTION:
SendToNode()

PURPOSE:

Convenience helper for application messages.

RESPONSIBILITIES:

✓ Build message packet
✓ Generate request ID
✓ Set headers
✓ Send packet

USED BY:

Chat
File Transfer
Applications

IMPORTANCE:

★★★★★ Critical
```
***Close()***
```
FUNCTION:
Close()

PURPOSE:

Gracefully shutdown Layer 4.

RESPONSIBILITIES:

✓ Close UDP socket
✓ Release resources
```

## Internal Functions

***startUDPListener()***
```
PURPOSE:

Create UDP listener.

FLOW:

Bind Port
      ↓
Store Socket
      ↓
Start receiveLoop()
```
***receiveLoop()***
```
PURPOSE:

Main packet receive engine.

FLOW:

UDP Packet
      ↓
Read Buffer
      ↓
Decode Packet
      ↓
handleIncomingPacket()

IMPORTANCE:

★★★★★ Critical

Every incoming packet enters here.
```

***handleIncomingPacket()***
```
PURPOSE:

Packet router.

RESPONSIBILITIES:

Route packets to correct subsystem.

CURRENT ROUTES:

Discovery
Kademlia
Application Messages

FLOW:

Packet
   ↓
Packet Type
   ↓

Discovery
Kademlia
Message
```
***handleApplicationMessage()***
```
PURPOSE:

Handle incoming application message.

RESPONSIBILITIES:

✓ Send ACK
✓ Notify application

FLOW:

Receive Message
       ↓
Build ACK
       ↓
Send ACK
       ↓
Process Message
```

***sendRaw()***
```
PURPOSE:

Low-level UDP transmission.

RESPONSIBILITIES:

✓ Marshal packet
✓ Write to UDP socket

NOT USED DIRECTLY BY APPLICATIONS
```

## Global Variables

***msgService***
```
PURPOSE:

Singleton Layer 4 instance.
```
***KademliaHandler***
```
PURPOSE:

Callback registered by InnerCore.

FLOW:

Kademlia Packet
       ↓
Message Layer
       ↓
KademliaHandler()
       ↓
InnerCore
```

## Reliability Design
```
================================================
RELIABILITY SUBSYSTEM
================================================

CURRENT COMPONENTS:

✓ pending map
✓ ACK packet generation

MISSING COMPONENTS:

✗ ACK processing
✗ Retry engine
✗ Timeout handling
✗ Persistence recovery
✗ Duplicate suppression

CURRENT STATUS:

Partially Implemented
```

Future File Structure
```
message/

├── service.go
│
├── transport.go
│
├── router.go
│
├── reliability.go
│
├── handlers.go
│
├── persistence.go
│
├── types.go
│
└── constants.go
```

Chat App Integration
```
================================================
CHAT APP USAGE
================================================

Functions Used:

✓ message.Init()

✓ message.SendToNode()

✓ message.SendPacket()

Flow:

User Types Message
        ↓
Chat UI
        ↓
message.SendToNode()
        ↓
UDP Network
        ↓
Remote Node
        ↓
handleApplicationMessage()
        ↓
Chat UI
```

## Critical Functions To Remember
★★★★★ Init()

★★★★★ SendPacket()

★★★★★ SendToNode()

★★★★★ receiveLoop()

★★★★★ handleIncomingPacket()

★★★★☆ handleApplicationMessage()

★★★☆☆ sendRaw()

★★★☆☆ Close()

Future Features Layer 4 Must Support
```
CHAT
✓ Direct messages
✓ Group messages

FILE TRANSFER
✓ Chunk delivery
✓ Retransmission

VOIP
✓ Low-latency frames
✓ Jitter buffering

VIDEO
✓ Stream packets
✓ Adaptive quality

STORAGE
✓ Replication messages

ROUTING
✓ Kademlia RPCs