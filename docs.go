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

## PACKAGE: innercore
```
================================================================
PACKAGE: innercore
================================================================
LAYER

Layer 1.5 — Distributed Routing & Overlay Intelligence

PURPOSE

Provides Kademlia-based routing, peer discovery persistence, supernode selection, distributed lookups, and DHT communication.

RESPONSIBILITIES

    Maintain XOR-distance routing table
    Store peers inside K-buckets
    Select closest peers
    Execute iterative Kademlia lookups
    Manage RPC communication
    Elect preferred supernodes
    Coordinate asynchronous lookup replies
    Provide DHT foundation for higher layers

DEPENDENCIES

    types
    network
    packet
    storage

USED BY

    Service Layer (Content Fabric)
    DHT Publisher
    Search Engine
    Future Chat Layer
    Future VoIP Layer
    Future Streaming Layer

MAIN ENTRY POINTS
    New()
    SeedPeer()
    Lookup()
    SendStore()
    SendFindNode()
    HandleKademliaPacket()

IMPORTANT STRUCTS

    InnerCore
    KBucket
    PeerInfo

FUTURE FEATURES
    Value lookups
    Distributed storage
    Bucket refresh protocol
    Peer eviction
    Provider records
    Network health analytics
    
NOTES

    InnerCore is effectively the routing engine of NexusFabric. Everything that needs distributed discovery eventually passes through it.
```

### FILE: kbucket.go
```
================================================================
FILE: kbucket.go
================================================================

PURPOSE

Implements a single Kademlia routing bucket. Each bucket stores peers within a specific XOR-distance range.
```
```
STRUCT: KBucket
PURPOSE: Container for peers that belong to the same distance zone.
```
FIELDS
```
nodes

Type: []*PeerInfo

Purpose: Stores peers currently assigned to this bucket.
```
```
k

Type: int

Purpose: Maximum bucket capacity.
```
```
mu

Type: sync.RWMutex

Purpose: Protects bucket from concurrent access.
```

USED BY:
```
    SeedPeer()
    Lookup()
    updateSupernodeList()
```
FUNCTIONS
```
FUNCTION: Insert()
PURPOSE: Adds a peer into the bucket.

BEHAVIOR

If peer exists:
    Update record
    Move peer to tail

If bucket has room:
    Add peer

If bucket full:
    Reject insertion

RETURNS
    bool

        true → inserted
        false → bucket full

FUTURE IMPORTANCE: Critical
```
```
FUNCTION: GetClosest()
PURPOSE: Returns nearest peers to a target NodeID.

PROCESS
    Copy bucket peers
    Sort by XOR distance
    Return N closest

USED BY:
    Lookup()
```

FLOW: Peer Insertion
```
Discovery
↓
SeedPeer()
↓
Determine XOR Distance
↓
Select Bucket
↓
Insert()
↓
Routing Table Updated
```
MISSING CAPABILITIES
```
Current
    Peer storage
    Distance sorting
    Thread safety

Missing
    Ping oldest node
    Bucket splitting
    Eviction policy

Future
    Adaptive bucket sizing
    Reputation-aware insertion
```

## FILE: lookup.go
```
================================================================
FILE: lookup.go
================================================================
PURPOSE

Executes real iterative Kademlia node lookups using live network responses rather than local routing data.
```
FUNCTION: Lookup()

PURPOSE

Locate peers closest to a target NodeID.

INPUT

    target NodeID

OUTPUT

    []PeerInfo
    error

### PROCESS

Step 1

Seed search from local buckets.

Step 2

Fallback to network peer table if buckets are empty.

Step 3

Query alpha peers concurrently.

Step 4

Wait for FIND_NODE replies.

Step 5

Insert discovered peers.

Step 6

Repeat until:

    max iterations reached
    timeout reached
    no new peers discovered

Step 7

Return final peer list.

USED BY:

    DHT Publisher
    Content Search
    Future Provider Discovery
    
### FLOW: Kademlia Lookup
```
Application
↓
Lookup(Target)
↓
getAlphaClosest()
↓
SendFindNodeAndWait()
↓
Remote Peer
↓
FIND_NODE_REPLY
↓
resolvePendingLookup()
↓
Next Iteration
↓
Final Results
```
### MISSING CAPABILITIES

Current

    Iterative lookup
    Parallel peer querying
    Timeout handling

Missing

    Alpha tuning
    Termination optimization
    Duplicate suppression refinement

Future

    FIND_VALUE traversal
    Provider discovery traversal
    Lookup caching

## FILE: rpc.go

```
================================================================
FILE: rpc.go
================================================================
PURPOSE

Implements Kademlia network protocol communication.

RESPONSIBILITIES
    PING
    FIND_NODE
    FIND_NODE_REPLY
    STORE
    FIND_VALUE (stub)
```

FUNCTION: 
```
***SendPing()***

PURPOSE: Verify peer liveness.

USED BY:
    Maintenance
    Future bucket refresh
 ```

FUNCTION: 
```
***SendFindNode()***

PURPOSE: Send asynchronous FIND_NODE request.

RETURNS: Immediately.
```

FUNCTION:
``` 
***SendFindNodeAndWait()***
PURPOSE: Send FIND_NODE and block until reply or timeout.

PROCESS

Register Pending Lookup
↓
Send Packet
↓
Wait On Channel
↓
Receive Reply
OR
Timeout

CRITICALITY: Extremely Critical

This function is the bridge between network RPC and iterative lookup.
```
FUNCTION: 
```
***HandleKademliaPacket()***

PURPOSE: Central RPC dispatcher.

HANDLES
    PING
    FIND_NODE
    FIND_NODE_REPLY
    STORE
    FIND_VALUE

USED BY:
    Message Layer

FLOW: FIND_NODE Request

Requester
↓
SendFindNode()
↓
UDP Transport
↓
HandleKademliaPacket()
↓
getAlphaClosest()
↓
sendFindNodeReply()
↓
Requester Receives Reply
```

MISSING CAPABILITIES

Current
```
    PING
    FIND_NODE
    STORE dispatch
```
Missing
```
    Real FIND_VALUE
    Real STORE persistence
    PONG packet type
```
Future
```
    Provider records
    Content retrieval RPC
    Replication management
```
## FILE: types.go
```
================================================================
FILE: types.go
================================================================
PURPOSE: Defines all shared InnerCore data structures.
```

STRUCT: 

    PeerInfo

PURPOSE: Represents a known network peer.
```
FIELDS
NodeID

Unique network identity.

IP: Peer address.

Port: Communication endpoint.

Capabilities: Device capability profile.

LastSeen: Last successful interaction.

LatencyMs: Measured latency.

Score: Supernode fitness score.

USED BY
    KBucket
    Lookup
    Supernode Election
    RPC Replies

MISSING CAPABILITIES
    Future
    Geo region
    Reliability score
    Storage contribution score
    Routing reputation
```

## FILE: scoring.go

```
================================================================
FILE: scoring.go
================================================================
PURPOSE: Computes supernode fitness scores from device capabilities.
```
FUNCTION: 

    calculateScore()

PURPOSE: 
Determine how useful a peer is as a supernode.

FACTORS:

Positive

    Bandwidth
    Uptime
    Cross-network bridge
    Hotspot capability
    Internet access

Negative

    Latency

USED BY:

    SeedPeer()
    updateSupernodeList()
    
<b>FLOW: Supernode Election</b>
```
Discovery
↓
Capabilities Collected
↓
calculateScore()
↓
Peer Score Updated
↓
updateSupernodeList()
↓
Top Nodes Become Supernodes
```

MISSING CAPABILITIES

    Current
    Capability scoring

Missing

    Historical reliability
    Packet success rate
    Storage contribution
    Routing contribution

Future

    AI-assisted node ranking
    Regional supernode balancing

# FILE: innercore.go
```
================================================================
FILE: innercore.go
================================================================

PURPOSE: The master controller of Layer 1.5.

This file owns all Kademlia state, routing structures, lookup coordination, supernode management, and background maintenance. It acts as the central orchestrator of the entire InnerCore subsystem.
```

```
================================================================
STRUCT: InnerCore
================================================================
PURPOSE: Central controller for the NexusFabric routing overlay.

Every Kademlia operation ultimately passes through this struct.
```

<b>ARCHITECTURE ROLE</b>
```
Discovery
↓
InnerCore
↓
KBuckets
↓
Lookup Engine
↓
RPC Layer
↓
DHT Operations
```

<b>FIELDS</b>

***nodeID***

Type:
    
    types.NodeID

Purpose: Identity of the local node.

Used for:

    XOR distance calculations
    Routing decisions
    Packet generation
    DHT lookups

***kBuckets***

Type:

    [256]*KBucket

Purpose: Stores routing information.

Each bucket represents one XOR-distance range.

Responsibilities:

    Peer placement
    Peer retrieval
    Routing optimization

***supernodes***

Type:

    []PeerInfo

Purpose: Stores highest scoring peers.

Used when:

    Routing tables are sparse
    Lookups fail
    Network assistance required

***mu***

Type:

    sync.RWMutex

Purpose:

Protects:

    supernodes
    routing state

***storage***

Type:

    *storage.StorageEngine

Purpose: Future integration point for:

    Routing table persistence
    DHT value persistence
    Cached lookup data

***msgSender***

Type:

    func(target NodeID, pkt Packet) error

Purpose:  Abstraction over transport layer.

Allows InnerCore to:

    Send FIND_NODE
    Send STORE
    Send PING

Without depending directly on UDP implementation.

***pendingLookups***

Type:

    map[string]chan []PeerInfo

Purpose: Tracks active FIND_NODE requests.

Key:

    requestID

Value:

    reply channel

Critical for asynchronous RPC coordination.

***pendingLookupsMu***

Type:

    sync.Mutex

Purpose: Protects pending lookup registry.

LIFECYCLE

Created:

    New()

Startup Tasks:

    Create 256 buckets
    Load persisted routing table
    Start maintenance loop

Destroyed:

    Node shutdown

### FUNCTIONS
```
================================================================
FUNCTION: New()
================================================================
PURPOSE: Creates a fully initialized InnerCore instance.

PROCESS

Create InnerCore
↓
Create 256 KBuckets
↓
Load Persisted Routing Table
↓
Start Maintenance Loop
↓
Return Controller

USED BY
    Integration Layer
    Node Bootstrap

FUTURE IMPORTANCE: Extremely Critical

This is the root object of Layer 1.5.

================================================================
FUNCTION: SetNodeID()
================================================================
PURPOSE: Assign local node identity after startup.

WHY IT EXISTS

Avoids import cycles between:

    Integration Layer
    Discovery Layer
    InnerCore

REQUIREMENT

Must execute before:

SeedPeer()

Otherwise peers may enter incorrect buckets.

================================================================
FUNCTION: SeedPeer()
================================================================
PURPOSE: Insert newly discovered peers into the routing system.

INPUTS
    NodeID
    IP
    Capabilities

PROCESS

Discovery Finds Peer
↓
Create PeerInfo
↓
Calculate Supernode Score
↓
Calculate XOR Distance
↓
Determine Bucket
↓
Insert Into Bucket
↓
Update Supernode Rankings

USED BY
    Discovery Layer
    Lookup Results
    Future Bootstrap Nodes

FUTURE IMPORTANCE:
    Critical: This is how the routing table grows.


================================================================
FUNCTION: GetPreferredSupernodes()
================================================================

PURPOSE: Return highest scoring routing peers.

OUTPUT
    []PeerInfo

USED BY
    Lookup escalation
    Future gateway selection
    Future relay selection

================================================================
FUNCTION: getAlphaClosest()
================================================================

PURPOSE: Select best candidate peers for lookup traversal.

PROCESS

Read All Buckets
↓
Combine Candidates
↓
Sort By XOR Distance
↓
Return Top Alpha

USED BY
    Lookup()
    FIND_NODE Handler

================================================================
FUNCTION: registerPendingLookup()
================================================================

PURPOSE: Create waiting channel for asynchronous FIND_NODE reply.

PROCESS

Generate Channel
↓
Store Under RequestID
↓
Return Channel

USED BY:
    SendFindNodeAndWait()

================================================================
FUNCTION: resolvePendingLookup()
================================================================

PURPOSE: Deliver incoming FIND_NODE_REPLY to waiting goroutine.

PROCESS

Receive Reply
↓
Find RequestID
↓
Remove Registry Entry
↓
Send Peers To Waiting Channel

IMPORTANCE: This is the bridge between:

    Outgoing FIND_NODE
and

    Incoming FIND_NODE_REPLY

Without this function iterative lookups cannot work.


================================================================
FUNCTION: cancelPendingLookup()
================================================================

PURPOSE: Cleanup timed-out lookup requests.

USED BY
    SendFindNodeAndWait()

BENEFIT: Prevents memory leaks.


================================================================
FUNCTION: updateSupernodeList()
================================================================

PURPOSE: Elect best supernodes from routing table.

PROCESS

Collect All Peers
↓
Sort By Score
↓
Take Top Five
↓
Replace Supernode List

ELECTION MODEL

    Not voting.
    Not consensus.
    Not authority.
    Pure capability ranking.
    Every node independently computes rankings.

USED BY
    SeedPeer()
    Maintenance Loop
    Refresh Buckets


================================================================
FUNCTION: maintenanceLoop()
================================================================

PURPOSE: Background housekeeping system.

INTERVAL

Every: 30 seconds

TASKS

    Refresh buckets
    Recompute supernodes

FUTURE TASKS
    Dead peer cleanup
    Replication audits
    Bucket health monitoring
    Latency measurements


================================================================
FUNCTION: refreshBuckets()
================================================================

PURPOSE: Maintain routing table health.

CURRENT: Stub implementation.

FUTURE DESIGN

For each bucket:

Oldest Peer
↓
Send PING
↓
Alive?
├─ Yes → Move To Tail
└─ No → Evict
↓
Insert Waiting Peer


================================================================
FUNCTION: loadPersistedTable()
================================================================
PURPOSE: Restore routing information after restart.

CURRENT
    Placeholder implementation.

FUTURE
    Load:

        KBuckets
        Peer scores
        Last seen timestamps

From storage layer.


================================================================
FUNCTION: GetMyNodeID()
================================================================
PURPOSE: Expose local node identity.

USED BY
    DHT Publisher
    Storage Layer
    Replication Logic
```

## FLOW
```
================================================================
FLOW: Peer Discovery
================================================================

Discovery Layer
↓
SeedPeer()
↓
calculateScore()
↓
Determine Bucket
↓
Insert()
↓
updateSupernodeList()
↓
Routing Table Expanded
```
```
================================================================
FLOW: FIND_NODE Reply Correlation
================================================================

Lookup()
↓
SendFindNodeAndWait()
↓
registerPendingLookup()
↓
Network Send
↓
Remote Node
↓
FIND_NODE_REPLY
↓
HandleKademliaPacket()
↓
resolvePendingLookup()
↓
Waiting Lookup Continues

This is one of the most important flows in all of InnerCore because it converts stateless UDP traffic into a coordinated lookup process.
```
```
================================================================
FLOW: Supernode Election
================================================================

Peer Discovery
↓
Capability Collection
↓
calculateScore()
↓
updateSupernodeList()
↓
Top 5 Peers Selected
↓
Preferred Routing Assistants
```
```
================================================================
FEATURE DEPENDENCIES
================================================================
DISCOVERY

✓ Directly Depends

DHT STORAGE

✓ Directly Depends

CONTENT FABRIC

✓ Directly Depends

SEARCH

✓ Directly Depends

CHAT

✓ Future Dependency

VOICE

✓ Future Dependency

VIDEO

✓ Future Dependency

INTERNET GATEWAY

✓ Future Dependency

CROSS-NETWORK BRIDGING

✓ Core Dependency
```

## MISSING CAPABILITIES
```
================================================================
MISSING CAPABILITIES
================================================================
CURRENT
    Routing table ownership
    KBucket management
    Supernode election
    FIND_NODE coordination
    Async lookup registry
    Maintenance scheduling

MISSING
    Real bucket eviction
    Routing table persistence
    Peer reliability scoring
    Latency tracking
    Bucket refresh protocol
    Bootstrap node support

FUTURE
    Provider records
    FIND_VALUE caching
    Geographic routing awareness
    Autonomous supernode promotion
    Network analytics
    Self-healing routing tables
    Multi-region overlay optimization
```
## ARCHITECTURAL NOTE
```
Among all Layer 1.5 files, innercore.go is the control tower. The other files provide capabilities (buckets, scoring, RPCs, lookups), but InnerCore coordinates them into a functioning distributed routing network. It is effectively the kernel of the Kademlia subsystem.
```

PACKAGE: resolver

LAYER

Layer 2 — Name Resolution & Logical Routing

PURPOSE

Provides DNS-like name resolution for NexusFabric.

Converts human-readable names into concrete network endpoints while remaining completely read-only and side-effect free.

Layer 2 sits between:
```
Network Knowledge
        ↓
Resolver
        ↓
Messaging / Services
```
RESPONSIBILITIES

    Resolve device names
    Resolve service names
    Cache resolution results
    Detect naming conflicts
    Provide role-based endpoint discovery
    Provide role-based messaging helpers

DESIGN RULES

Strictly enforced:

    ✓ Read Only
    ✓ Deterministic
    ✓ Cached
    ✓ Side Effect Free
    ✗ No Registration
    ✗ No Routing
    ✗ No Health Monitoring 
    ✗ No Network Modification

DEPENDENCIES

    network
    message
    types

USED BY

    Layer 3 Service Protocol
    Layer 4 Messaging
    Applications
    Future CLI Tools
    Future Web UI

MAIN ENTRY POINTS

    Resolve()
    SendToRole()
    GetRoleMembers()
    BroadcastToAllRoles()

IMPORTANT STRUCTS

    Layer2Resolver
    ResolutionCache
    ResolutionRecord
    Endpoint

FUTURE FEATURES

    Distributed Service Discovery
    Wildcard Resolution
    Resolver Federation
    Service Load Balancing
    Multi-Region Resolution

```
================================================================
FILE: resolver.go
================================================================
PURPOSE: Implements the core .mtd name resolution system.

Acts similarly to DNS but for NexusFabric devices and services.
```
Examples:
```
laptop.mtd
phone.mtd
tablet.mtd

svc.chat.mtd
svc.storage.mtd
svc.game.mtd
```
struct 1
```
================================================================
STRUCT: ResolutionRecord
================================================================
PURPOSE

Standard Layer 2 response object.

Every resolution request returns this format.
```

***FIELDS***

Status

Type:

    string

Possible Values:
```
OK
NX
CONFLICT
```
Meaning:

OK

    → Resolution successful

NX

    → Name not found

CONFLICT

    → Multiple matching records

***Type***

Type:

    string

Possible Values:

    device
    service

***Name***

Type:

    string

Purpose: Original requested name.

***Records***

Type:

    []Endpoint

Purpose: Resolved endpoints.

***TTL***

Type:

    int

Purpose: Cache lifetime.

***CachedAt***

Type:

    time.Time

Purpose: Cache creation timestamp.

```
================================================================
STRUCT: Endpoint
================================================================
PURPOSE: Represents a concrete destination.

Used by applications to connect to devices or services.
```
### FIELDS

***DeviceID***

Unique node identity.

***Name***

Human readable hostname.

***IP***

Device address.

***Port***

Service endpoint port.

***Role***

Assigned device role.

Examples:
```
chat
storage
cache
game
```
***RoleTrusted***

Whether role assignment is trusted.

***Health***

Node health score.

***ServiceMetadata***

Future service-specific information.

Examples:
```
Capacity
Load
Region
Version
```
USED BY
```
ResolutionRecord
Service Discovery
Messaging
```

struct 2
```
================================================================
STRUCT: ResolutionCache
================================================================
PURPOSE: Thread-safe local cache.

Reduces repeated network table scans.
```
### FIELDS

***cache***

Stores resolution records.

***mu***

Protects cache access.

FUNCTION: Get()
================================================================
PURPOSE: Retrieve cached resolution.

PROCESS
```
Lookup Key
↓
Exists?
├─ No → nil
└─ Yes
↓
TTL Valid?
├─ No → Delete Entry
└─ Yes → Return Record
```
SPECIAL FEATURE

    Uses lazy eviction.

    Expired entries removed only when accessed.

FUNCTION: Set()
================================================================
PURPOSE: Store resolution result.

PROCESS
```
Add Timestamp
↓
Insert Into Cache
```
FUNCTION: Delete()
================================================================
PURPOSE: Remove cache entry.

```
================================================================
STRUCT: Layer2Resolver
================================================================

PURPOSE: Main resolution engine.

Owns cache and resolution logic.
```
FIELDS

***cache***

Type:

    *ResolutionCache

Purpose: Stores recent lookups.

***LIFECYCLE***

Created:

    NewLayer2Resolver()

Lives: Entire application lifetime.


FUNCTION: Resolve()
================================================================

PURPOSE: Public entry point for all Layer 2 resolution.

INPUT

    name string

Example:

    laptop.mtd
    svc.chat.mtd
    OUTPUT
    ResolutionRecord

PROCESS
```
Normalize Name
↓
Check Cache
↓
Cache Hit?
├─ Yes → Return
└─ No
↓
resolveAndCache()
↓
Return Result
```
USED BY:

    Messaging
    Services
    Applications
    FUTURE IMPORTANCE

Extremely Critical

This is Layer 2's primary API.


FUNCTION: resolveAndCache()
================================================================

PURPOSE: Internal resolution dispatcher.

PROCESS
```
Validate Suffix
↓
Service Name?
├─ Yes → resolveService()
└─ No → resolveDevice()
↓
Cache Result
↓
Return Result
```


FUNCTION: resolveDevice()
================================================================
PURPOSE: Resolve device hostnames.

Example:

    laptop.mtd

PROCESS
```
Extract Hostname
↓
GetAllPeers()
↓
Search Matching Devices
↓
Filter Alive Nodes
↓
Build Endpoints
↓
Evaluate Result

Result Count

0 → NX

1 → OK

1 → CONFLICT
```
DATA SOURCE

    network.GetAllPeers()

Important:

Resolver does NOT discover peers.

It only reads existing network knowledge.

### FLOW: Device Resolution
```
Application
↓
Resolve("laptop.mtd")
↓
resolveDevice()
↓
Network Table Scan
↓
Match Found
↓
ResolutionRecord(OK)
```

FUNCTION: resolveService()
================================================================
PURPOSE: Future service lookup engine.

Examples:

    svc.chat.mtd
    svc.storage.mtd
    svc.game.mtd

CURRENT STATE: 

Stub implementation.

Always returns:

    NX

FUTURE DESIGN
```
Resolve()
↓
Service Registry
↓
Healthy Service Instances
↓
ResolutionRecord
```


FUNCTION: okRecord()
================================================================
PURPOSE

Build successful resolution response.

FUNCTION: nxRecord()
================================================================
PURPOSE

Build not-found response.

FUNCTION: conflictRecord()
================================================================
PURPOSE

Build ambiguous-resolution response.

EXAMPLE

Two devices:

    laptop.mtd
    laptop.mtd

Result:

CONFLICT

Returned records contain all candidates.


### FLOW: Service Resolution (Future)

```
Application
↓
Resolve("svc.chat.mtd")
↓
Service Registry
↓
Healthy Providers
↓
Endpoint Selection
↓
ResolutionRecord
```


# FILE: role_routing.go
```
PURPOSE

Provides logical role-based communication.

Instead of targeting:

Device A

Applications target:

All chat nodes
All storage nodes
All cache nodes

Layer 2 translates the role into actual device targets.
```
ARCHITECTURAL NOTE
```
This file is technically a helper layer.

It uses:

Layer 2 Discovery Logic
+
Layer 4 Messaging

to create group-oriented communication.
```      

FUNCTION: SendToRole()
================================================================
PURPOSE

Send a message to every healthy trusted node with a specified role.

INPUTS

    role
    messageType
    content
    senderName

FILTER RULES

Peer Must Have:

    ✓ Matching Role

    ✓ Alive Status

    ✓ Health >= 0.5

    ✓ Trusted Role

PROCESS
```
GetAllPeers()
↓
Filter By Role
↓
Build Payload
↓
SendToNode()
↓
Track Success / Failure
↓
Return Summary
```
OUTPUT

Summary Object:

    sent_count
    failed_count
    target_nodes
    failed_nodes

USED BY:

    Multiplayer Games
    Chat Services
    Storage Replication
    Cache Synchronization

FLOW: Role Message
```
Application
↓
SendToRole("chat")
↓
Find Chat Nodes
↓
Layer 4 Messaging
↓
Reliable Delivery
↓
Chat Nodes Receive
```


FUNCTION: GetRoleMembers()
================================================================
PURPOSE

Query membership of a role.

Example:

Who are my storage nodes?

PROCESS
```
GetAllPeers()
↓
Filter Conditions
↓
Build Result List
↓
Return Members
```
USED BY:

    Monitoring
    Dashboards
    Service Discovery
    Administration Tools


FUNCTION: BroadcastToAllRoles()
================================================================
PURPOSE

Send a system-wide announcement.

PROCESS
```
Allowed Roles
↓
For Each Role
↓
SendToRole()
↓
Aggregate Statistics
↓
Return Summary
```
CURRENT ROLES

    game
    chat
    cache
    storage

EXAMPLE

    System Maintenance Notice

↓

All Chat Nodes

All Storage Nodes

All Cache Nodes

All Game Nodes

FUNCTION: contains()
================================================================
PURPOSE

Small utility helper.

Checks if role exists in exclusion list.


### FLOW: Global Broadcast

```
Application
↓
BroadcastToAllRoles()
↓
Role: Chat
↓
Role: Storage
↓
Role: Cache
↓
Role: Game
↓
Aggregate Results
↓
Return Statistics
```

FEATURE DEPENDENCIES
================================================================
### DEVICE DISCOVERY

✓ Reads Results

### SERVICE PROTOCOL (LAYER 3)

✓ Core Dependency

### MESSAGING (LAYER 4)

✓ Core Dependency

### CHAT

✓ Uses Resolver

### FILE STORAGE

✓ Uses Resolver

### CACHE NETWORK

✓ Uses Resolver

### GAMING

✓ Uses Resolver

### VIDEO STREAMING

✓ Future Dependency

### VOICE

✓ Future Dependency


MISSING CAPABILITIES
================================================================

CURRENT
```
Device name resolution
Resolution cache
Conflict detection
Role discovery
Role-based messaging
Broadcast helpers
```
MISSING
```
Real service registry integration
Service endpoint resolution
Resolver statistics
Cache metrics
Wildcard queries
```
FUTURE
```
Geographic service selection
Load-balanced service resolution
Service version awareness
Resolver federation
Distributed resolver cache
Multi-region namespace support
```
ARCHITECTURAL NOTE
```
Layer 2 acts as the directory service of NexusFabric.

If Layer 1.5 answers:

"Where in the network should I search?"

Layer 2 answers:

"I already know the name. Tell me the endpoint."