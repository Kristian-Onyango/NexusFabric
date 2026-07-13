## PACKAGE: discovery
```
================================================
PACKAGE: discovery
================================================

LAYER:
Layer 1 - Discovery

PURPOSE:
Find other NexusFabric nodes on the local network
and provide initial peers to higher layers.

RESPONSIBILITIES:
- Node identity management
- Broadcast node announcements
- Listen for peer announcements
- Feed peers into network/routing layers
- Provide local node information

DEPENDENCIES:
- packet
- types

USED BY:
- message
- kademlia (through callbacks)
- startup/bootstrap process

MAIN FILES:
- discovery.go
- nodeid.go
- types.go

MAIN EXPORTS:
- Init()
- GetMyNodeID()
- GetMyNodeName()
- SetSeedPeerCallback()

GLOBAL CALLBACKS:
- UpdateNetworkCallback
- SeedPeerCallback

CRITICALITY:
VERY HIGH

Without discovery:
- No peer discovery
- No Kademlia bootstrapping
- No network population
```

## FILE: discovery.go
```
================================================
FILE: discovery.go
================================================

PURPOSE:
Core discovery engine.

RESPONSIBILITIES:
- Initialize discovery subsystem
- Broadcast announcements
- Receive announcements
- Trigger peer registration callbacks

MAIN GLOBALS:

myNodeID
Purpose: Identity of local node.

myNodeName
Purpose: Friendly hostname.

myCaps
Purpose: Advertised capabilities.

DiscoveryPort
Purpose: UDP port used for discovery.

LocalMessagePort
Purpose: Messaging port advertised to peers.
```
### Exported Functions
#### Init()
```
FUNCTION:
Init()

PURPOSE:
Starts discovery subsystem.

RESPONSIBILITIES:
- Configure ports
- Load/create NodeID
- Build capability profile
- Start announce loop
- Start listener loop

CALLS:
LoadOrCreateNodeID()
announceLoop()
listenLoop()

IMPORTANCE:
CRITICAL

System startup entry point.
````
#### GetMyNodeID()
```
FUNCTION:
GetMyNodeID()

PURPOSE:
Returns current node identity.

RETURNS:
types.NodeID

USED BY:
message.Init()

IMPORTANCE:
CRITICAL

Many layers depend on local identity.
```

GetMyNodeName()
```
FUNCTION:
GetMyNodeName()

PURPOSE:
Returns hostname used as node name.
```

SetSeedPeerCallback()
```
FUNCTION:
GetMyNodeName()

PURPOSE:
Returns hostname used as node name.
```

## Internal Functions

These are NOT exported.

announceLoop()
```
PURPOSE:
Broadcasts discovery announcements.

INTERVAL:
Every 5 seconds.

PACKET TYPE:
DISCOVERY_ANNOUNCE

FLOW:

Ticker
 ↓
Create Packet
 ↓
Marshal JSON
 ↓
UDP Broadcast
```
listenLoop()
```
PURPOSE:
Receives discovery packets.

FLOW:

UDP Receive
 ↓
Unmarshal Packet
 ↓
Ignore Self
 ↓
Extract Services
 ↓
UpdateNetworkCallback()
 ↓
SeedPeerCallback()

IMPORTANCE:
CRITICAL

This is where discovered peers enter
the rest of the system.
```

### Actual Dependencies

From the imports:
```
import (
    "innercore-network/packet"
    "innercore-network/types"
)
```
Therefore:

DIRECT DEPENDENCIES
```
✓ packet
✓ types
````
Callback Relationships

This is where discovery connects to the rest of NexusFabric.
```
Discovery
    ↓
UpdateNetworkCallback
    ↓
Network Layer

Discovery
    ↓
SeedPeerCallback
    ↓
Kademlia Layer
```
This is probably one of the most important diagrams in the package.

### FILE: nodeid.go
```
================================================
FILE: nodeid.go
================================================

PURPOSE:
Persistent node identity management.
LoadOrCreateNodeID()
FUNCTION:
LoadOrCreateNodeID()

PURPOSE:
Load existing node identity from disk.
Create one if none exists.

STORAGE LOCATION:

~/.innercore/node_id.bin

TESTING SUPPORT:

instance1
 ↓
node_id_instance1.bin

RETURNS:
types.NodeID

IMPORTANCE:
CRITICAL

Node identity persistence.
```
Flow
```
Startup
   ↓
Load NodeID File

Exists?
 ├─ Yes → Load
 └─ No
       ↓
   Generate ID
       ↓
   Save File
       ↓
   Return
```

### FILE: types.go

```
================================================
FILE: types.go
================================================

PURPOSE:
Discovery protocol structures.

RESPONSIBILITIES:
- Message definitions
- Shared discovery formats

BUSINESS LOGIC:
None
Struct: DiscoveryMessage
STRUCT:
DiscoveryMessage

PURPOSE:
Discovery protocol payload.

FIELDS:

ProtocolVersion
Type
RequestID
NodeID
NodeName
Timestamp
Capabilities
Services
ServicePort
```

### CHAT APP 

```
================================================
CHAT APP RELEVANCE
================================================

USED DIRECTLY?

GetMyNodeID()
✓ Yes

GetMyNodeName()
✓ Yes

DiscoveryMessage
✗ No

announceLoop()
✗ No

listenLoop()
✗ No

SetSeedPeerCallback()
✗ No