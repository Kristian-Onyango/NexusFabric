# packet/packet.go

```
================================================
PACKAGE: packet
================================================

LAYER:
Universal Packet Format

PURPOSE:

Defines the protocol used by all network traffic.

Every packet sent through NexusFabric uses
this structure.

USED BY:

Discovery
Message
InnerCore
Storage
Chat
File Transfer
VoIP
Video Streaming

STATUS:

✓ Core Protocol Definition
```

### Packet

```
================================================
STRUCT: Packet
================================================

PURPOSE:

Top-level network packet.

STRUCTURE:

Packet
│
├── Header
├── Network
├── Capabilities
└── Payload

RESPONSIBILITY:

Carry data between nodes.
```

### Header
```
================================================
STRUCT: Header
================================================

PURPOSE:

Routing and protocol metadata.

FIELDS:

Version
Purpose:
Protocol version.

RequestID
Purpose:
Unique message identifier.

PacketType
Purpose:
Identifies packet purpose.

TTL
Purpose:
Hop limit.

SourceNodeID
Purpose:
Sender identity.

DestinationNodeID
Purpose:
Target identity.

Timestamp
Purpose:
Creation time.

PayloadLength
Purpose:
Payload size.
```

### NetworkInfo
```
================================================
STRUCT: NetworkInfo
================================================

PURPOSE:

Network routing metadata.

FIELDS:

SourceRegion
Purpose:
Origin region.

DestinationRegion
Purpose:
Target region.

FUTURE USES:

Geo-routing
Regional optimization
Bridge selection
```

### Packet Types

```
================================================
PROTOCOL TYPES
================================================

DISCOVERY

DISCOVERY_PING
DISCOVERY_PONG
DISCOVERY_ANNOUNCE

PURPOSE:

Node discovery.

----------------------------------

KADEMLIA

KADEMLIA_PING
KADEMLIA_PONG
KADEMLIA_FIND_NODE
KADEMLIA_FIND_VALUE
KADEMLIA_STORE

PURPOSE:

Distributed routing and storage.

----------------------------------

MESSAGING

MSG

PURPOSE:

Application communication.

Chat
File Transfer
VoIP
Video Streaming
```