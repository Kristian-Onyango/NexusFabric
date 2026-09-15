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
USED BY
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

It is the bridge between human-friendly names and machine-usable network destinations.
\
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
```
================================================
PACKAGE: storage
================================================

LAYER:
5

PURPOSE:

Persistent storage layer for NexusFabric.

Acts as the long-term memory of the network.

RESPONSIBILITIES:

✓ Store records
✓ Version records
✓ Persist devices
✓ Persist network state
✓ Recovery after restart
✓ Provide storage APIs to all layers

USED BY:

Network
Discovery
InnerCore
Messaging
Applications

DEPENDENCIES:

types
sync
time

STATUS:

Storage Engine      ✓ Implemented
Device Registry     ✓ Implemented
Network Snapshot    ✓ Implemented
Distributed DHT     ✗ Future
Disk Persistence    ✗ Future
````
### Architecture
```
================================================
STORAGE ARCHITECTURE
================================================

      Applications
          ↓
      StorageFacade
          ↓
     ┌─────────────────┐
     │ Device Registry │
     └─────────────────┘

     ┌─────────────────┐
     │ Network Store   │
     └─────────────────┘
          ↓
     StorageEngine

          ↓
        Records
```

## storage_engine.go
```
================================================
FILE: storage_engine.go
================================================

PURPOSE:

Core storage engine.

Acts as the database backend for NexusFabric.

RESPONSIBILITIES:

✓ Store records
✓ Version records
✓ Read records
✓ Delete records
✓ Thread safety

IMPORTANCE:

★★★★★ Critical

Everything in Layer 5 depends on this file.
Record
================================================
STRUCT: Record
================================================

PURPOSE:

Universal storage object.

FIELDS:

ID
Unique record identifier.

Version
Record version number.

Payload
Actual stored data.

CreatedAt
Creation timestamp.

USED BY:

Device Registry
Network Snapshots
Future DHT
Future File Storage
```
### StorageEngine
```
================================================
STRUCT: StorageEngine
================================================

PURPOSE:

Authoritative storage backend.

INTERNAL DESIGN:

store

collection
   ↓
recordID
   ↓
version
   ↓
Record

EXAMPLE:

devices
   ↓
node123
   ↓
v1
v2
v3
```
### Get()
```
FUNCTION:

Get()

PURPOSE:

Retrieve latest version of a record.

RETURNS:

Newest record version.

IMPORTANCE:

★★★★★ Critical
```
### Put()
```
FUNCTION:

Put()

PURPOSE:

Insert or update records.

FEATURES:

✓ Versioning
✓ Optimistic locking

FLOW:

Find Current Version
         ↓
Create New Version
         ↓
Store Record
```
### Delete()
```
FUNCTION:

Delete()

PURPOSE:

Remove records.

OPTIONAL:

Version conflict protection.
```
### Storage Errors
```
================================================
ERROR TYPES
================================================

ErrRecordNotFound

Purpose:
Record does not exist.

-------------------------

ErrVersionConflict

Purpose:
Attempted update on stale version.
```
## device_registry.go
```
================================================
FILE: device_registry.go
================================================

PURPOSE:

Persistent identity database.

Stores information about devices.

Acts as source of truth for node identity.

COLLECTION:

devices
```
### DeviceRegistry
```
================================================
STRUCT: DeviceRegistry
================================================

PURPOSE:

Manage registered devices.

BACKEND:

StorageEngine
```
### RegisterDevice()
```
FUNCTION:

RegisterDevice()

PURPOSE:

Store device information.

SAVED DATA:

device_id
first_seen
public_key
roles
metadata

USED FOR:

Identity recovery
Authentication
Future trust system

IMPORTANCE:

★★★★★ Critical
```
### GetDevice()
```
FUNCTION:

GetDevice()

PURPOSE:

Load stored device information.

INPUT:

NodeID

OUTPUT:

Record
```
### DeviceExists()
```
FUNCTION:

DeviceExists()

PURPOSE:

Check if device already exists.

RETURNS:

true
false
```
## network_snapshot.go
```
================================================
FILE: network_snapshot.go
================================================

PURPOSE:

Persist network state.

Used for fast startup and recovery.

COLLECTION:

network_snapshots
```
### NetworkSnapshotStore
```
================================================
STRUCT: NetworkSnapshotStore
================================================

PURPOSE:

Manage network snapshots.

BACKEND:

StorageEngine
```
### SaveSnapshot()
```
FUNCTION:

SaveSnapshot()

PURPOSE:

Store current network table.

SAVED DATA:

timestamp
device_count
network_table

USED FOR:

Warm restart
Recovery
Diagnostics
```
### LoadLatestSnapshot()
```
FUNCTION:

LoadLatestSnapshot()

PURPOSE:

Load most recent network snapshot.

RETURNS:

Latest network state.
```
### SnapshotExists()
```
FUNCTION:

SnapshotExists()

PURPOSE:

Check if snapshot exists.

RETURNS:

true
false
```
## facade.go
```
================================================
FILE: facade.go
================================================

PURPOSE:

Unified API for all storage operations.

Acts as the public entry point
for Layer 5.

IMPORTANCE:

★★★★★ Critical

Other layers should use this
instead of directly touching
StorageEngine.
```
StorageFacade
```
================================================
STRUCT: StorageFacade
================================================

PURPOSE:

Coordinates all storage modules.

CONTAINS:

StorageEngine

NetworkSnapshotStore

DeviceRegistry
```
### GlobalStorage
```
PURPOSE:

Singleton storage instance.

Used by entire system.
```
### Initialize()
```
FUNCTION:

Initialize()

PURPOSE:

Initialize Layer 5.

CALLED DURING:

System startup.
```
### SaveNetworkState()
```
FUNCTION:

SaveNetworkState()

PURPOSE:

Persist current network table.
```
### LoadNetworkState()
```
FUNCTION:

LoadNetworkState()

PURPOSE:

Restore previous network state.
```
### RegisterDevice()
```
FUNCTION:

RegisterDevice()

PURPOSE:

Persist device identity.
```
### GetDevice()
```
FUNCTION:

GetDevice()

PURPOSE:

Retrieve stored device.
```
### GetEngine()
```
FUNCTION:

GetEngine()

PURPOSE:

Expose underlying StorageEngine.

USED BY:

InnerCore
Advanced storage operations
```

### Layer 5 Data Flow

```
================================================
LAYER 5 DATA FLOW
================================================

Discovery
     ↓
RegisterDevice()
     ↓
DeviceRegistry
     ↓
StorageEngine

--------------------------------

Network
     ↓
SaveNetworkState()
     ↓
NetworkSnapshotStore
     ↓
StorageEngine

--------------------------------

Startup
     ↓
LoadNetworkState()
     ↓
Restore Previous State
```
### Current Reality
```
================================================
CURRENT IMPLEMENTATION STATUS
================================================

In-Memory Database
✓ Complete

Versioning
✓ Complete

Device Registry
✓ Complete

Network Snapshots
✓ Complete

Distributed DHT Storage
✗ Not Started

Disk Persistence
✗ Not Started

Replication
✗ Not Started

Chunk Storage
✗ Not Started

Content Addressing
✗ Not Started
One important observation
```
After reading these files, Layer 5 is not actually persistent yet.

Everything is stored in:

    store map[string]map[string]map[int]Record

which lives only in RAM.

So if the program exits:
```
All Device Registry Data Lost
All Snapshots Lost
All Records Lost
```
The architecture is correct, but true persistence (SQLite, BoltDB, BadgerDB, Pebble, or file storage) has not been connected yet. That's probably one of the first things you'll want to upgrade before building large-scale chat history or file storage.

Layer 6 — Internet Gateway Documentation
```
Purpose

Layer 6 is the bridge between the NexusFabric mesh network and the public internet.

Its responsibility is to provide internet access when a requested resource cannot be found inside the local mesh.

The layer acts as an internet egress gateway and performs:

Internet forwarding
TCP proxying
Session tracking
Traffic accounting
Fallback routing

This layer is the highest networking layer in the current architecture.
```
### File: gateway.go
```
Purpose

Implements the Internet Gateway.

The gateway accepts local client connections, determines the requested internet destination, opens a connection to that destination, and relays traffic between the client and the remote server.

Current implementation is a basic TCP proxy.
```
Constants
```
const (
    DefaultHTTPPort  = 80
    DefaultHTTPSPort = 443
    SocketTimeout    = 10 * time.Second
    BufferSize       = 8192
)
Responsibilities

Constant	                    Purpose
DefaultHTTPPort	            Default HTTP destination
DefaultHTTPSPort	        Default HTTPS destination
SocketTimeout	            Upstream connection timeout
BufferSize	                Traffic relay buffer size
```
GatewaySession
```
Purpose

Represents one active internet forwarding session.

type GatewaySession struct {
    ClientAddr   string
    Target       string
    CreatedAt    time.Time
    LastActivity time.Time
    BytesUp      int64
    BytesDown    int64
}
Fields

Field	                        Purpose 

ClientAddr	                Connected client
Target	                    Internet destination
CreatedAt	                Session start time
LastActivity	            Last packet time
BytesUp	                    Data sent
BytesDown	                Data received
```
### InternetGateway
```
Purpose

Main Layer 6 controller.

type InternetGateway struct {
    listenIP   string
    listenPort int
    sessions   map[string]*GatewaySession
    mu         sync.RWMutex
    running    bool
    innerCore  *innercore.InnerCore
}
Fields

Field	                        Purpose
listenIP	                Local bind address
listenPort	                Gateway listening port
sessions	                Active session table
mu	                        Thread safety
running	                    Gateway state
innerCore	                Access to routing intelligence
```
### Exported Functions
```
NewInternetGateway()

Purpose

Creates a new gateway instance.
```
Signature
```
func NewInternetGateway(
    listenIP string,
    listenPort int,
    ic *innercore.InnerCore,
) *InternetGateway
```
Returns

    Configured gateway ready to start.

Start()
```
Purpose

Starts listening for TCP connections.
```
Signature
```
func (g *InternetGateway) Start()
```

Responsibilities
```
Opens TCP listener
Accepts clients
Creates goroutine per connection
```
Calls:


    handleClient()

Stop()
```
Purpose

Stops accepting new clients.
```
Signature

    func (g *InternetGateway) Stop()


### Internal Functions

handleClient()
```
Purpose

Processes a newly connected client.

Flow

Client Connects
        ↓
Read First Request
        ↓
Extract Target Host
        ↓
Create Session
        ↓
Forward Session

Responsibilities

    Read initial packet
    Determine destination
    Create GatewaySession
    Start forwarding
```
extractTarget()
```
Purpose

Determines requested internet host.
```
Current Method

Parses HTTP:
```
Host: example.com
```
Returns:
```
host
port
```
Current Status

MVP implementation.

Future versions should use:
```
    Proper HTTP parser
    HTTPS CONNECT support
    DNS handling
```
forwardSession()
```
Purpose

Creates upstream connection.

Flow

Client
   ↓
Gateway
   ↓
Internet Server

Responsibilities
    Connect upstream
    Send first payload
    Start bidirectional relay
```
relay()
```
Purpose

Copies traffic between two sockets.

Flow

Client → Internet
Internet → Client
Tracks
Bytes uploaded
Bytes downloaded
Session activity
```
### Helper Functions

splitLines()
```
Purpose

Placeholder helper.

Current implementation:

return nil

Not implemented yet.
```
trimSpace()
```
Purpose

Placeholder string trimming helper.

Needs replacement with:

strings.TrimSpace()
```
indexByte()
```
Purpose

Searches for a byte inside a string.

Used by:

extractTarget()
```
### Dependencies
```
Imports
innercore-network/innercore
Used For

Access to:

    Supernodes
    Routing decisions
    Future bridge selection
```
### Current Data Flow
```
Client
   │
   ▼
Layer 6 Gateway
   │
   ▼
Target Internet Server
   │
   ▼
Response
   │
   ▼
Client
```
### Future Responsibilities
```
The current implementation is only the foundation.

Planned features:

Internet Bridge Selection

Mesh Node
    ↓
Supernode
    ↓
Internet

Instead of directly accessing the internet, nodes may choose the best supernode.
```
HTTPS Support

Add:
```
CONNECT
TLS Tunneling
```
DNS Fallback

When Layer 2 cannot resolve:
```
service.mtd
```
Layer 6 can query public DNS.

Traffic Policies

Support:
```
Rate limiting
Bandwidth quotas
Gateway permissions
Fair usage
Session Cleanup
```
Automatically remove:

    inactive sessions 

after timeout.

### Architecture Position
```
Layer 1  Discovery
Layer 1.5 InnerCore (Kademlia)
Layer 2  Resolver
Layer 3  Service Layer
Layer 4  Messaging
Layer 5  Storage
Layer 6  Internet Gateway
```
Layer 6 is the final escape hatch of NexusFabric. If the mesh cannot satisfy a request locally, Layer 6 attempts to reach the wider internet while preserving the same routing architecture

# network/network.go

```
================================================
PACKAGE: network
================================================

LAYER:
Network State Manager

PURPOSE:

Maintains live knowledge of all known peers.

Acts as the network directory.

USED BY:

Discovery
Message
InnerCore
Applications

DEPENDENCIES:

types
sync
time
fmt

STATUS:

✓ Implemented
```
Architecture
```
================================================
NETWORK TABLE
================================================

Discovery
     ↓
UpdateNode()
     ↓
NetworkTable
     ↓

Messaging
Routing
Storage
Applications
```
PeerEntry
```
================================================
STRUCT: PeerEntry
================================================

PURPOSE:

Represents a known peer.

FIELDS:

NodeID
Unique node identity.

Name
Human readable name.

IP
Current IP address.

Role
Assigned network role.

RoleTrusted
Role verification status.

Status
alive
dead
unhealthy

Health
Reliability score.

LastSeen
Last successful contact.

Services
Advertised services.

ServicePort
Application port.

Capabilities
Node abilities.

Score
Supernode score.
```
NetworkTable
```
================================================
STRUCT: NetworkTable
================================================

PURPOSE:

Stores all known peers.

FIELDS:

peers
Map of NodeID → PeerEntry

mu
Thread safety.

nodeTimeout
Peer expiration threshold.
```
UpdateNode()
```
FUNCTION:

UpdateNode()

PURPOSE:

Insert or refresh peer information.

CALLED BY:

Discovery Layer

RESPONSIBILITIES:

✓ Add new peers
✓ Update existing peers
✓ Refresh timestamps
✓ Update capabilities
✓ Calculate score

IMPORTANCE:

★★★★★ Critical
```
GetPeer()
```
FUNCTION:

GetPeer()

PURPOSE:

Retrieve one peer.

USED BY:

Message Layer

EXAMPLE:

SendPacket()
      ↓
GetPeer()
      ↓
Get IP
      ↓
Send UDP
```
GetAllPeers()
```
FUNCTION:

GetAllPeers()

PURPOSE:

Return copy of network table.

USED BY:

InnerCore lookups
Diagnostics
Applications
```
ExpireStaleNodes()
```
FUNCTION:

ExpireStaleNodes()

PURPOSE:

Mark inactive peers as dead.

RULE:

Current Time
-
LastSeen

>

nodeTimeout

RESULT:

Status = dead
```
RecordSuccess()
```
FUNCTION:

RecordSuccess()

PURPOSE:

Increase peer health score.

CALLED BY:

Successful messaging
Successful routing

EFFECT:

Health +0.1
LastSeen updated
```
RecordFailure()
```
FUNCTION:

RecordFailure()

PURPOSE:

Penalize unreliable peers.

EFFECT:

Health -0.3

IF:

Health <= 0.3

Status = unhealthy
```
PrintNetworkState()
```
FUNCTION:

PrintNetworkState()

PURPOSE:

Debugging utility.

OUTPUT:

NodeID
Name
IP
Role
Status
Health
Services

USED FOR:

Development
Testing
Diagnostics
```
### Relationship Between These Three Files
```
================================================
CORE FLOW
================================================

types
  ↓
Defines fundamental structures

packet
  ↓
Uses types to create protocol packets

network
  ↓
Uses types to track live peers

----------------------------------

Discovery
  ↓
network.UpdateNode()

Network Table
  ↓
Message Layer

Message Layer
  ↓
packet.Packet

Packet
  ↓
Remote Node
```

## types/types.go

```
================================================
PACKAGE: types
================================================

LAYER:
Shared Core Types

PURPOSE:

Defines universal data structures used across
the entire NexusFabric system.

Every package depends on this package.

USED BY:

Discovery
Network
Message
InnerCore
Storage
Applications

DEPENDENCIES:

fmt

STATUS:

✓ Core Foundation
```
### NodeID

```
================================================
TYPE: NodeID
================================================

DEFINITION:

[32]byte

PURPOSE:

Unique identity of every node in the network.

Equivalent to:

IP Address
+
Username
+
Machine Identity

but cryptographically unique.

USED FOR:

✓ Discovery
✓ Routing
✓ Messaging
✓ Storage
✓ Authentication

SIZE:

256 bits
32 bytes
```
### String()
```
FUNCTION:

NodeID.String()

PURPOSE:

Human readable display format.

INPUT:

Full 32-byte NodeID

OUTPUT:

Shortened hex string

EXAMPLE:

3fa9d7c81ab2e440...

USED FOR:

Logs
Debugging
Console Output
```
***Equal()***
```
FUNCTION:

NodeID.Equal()

PURPOSE:

Compare two NodeIDs.

RETURNS:

true
false

USED FOR:

Identity checks
Self detection
Routing
```
### Capabilities
```
================================================
STRUCT: Capabilities
================================================

PURPOSE:

Describes what a node can contribute
to the network.

USED FOR:

✓ Supernode Election
✓ Routing
✓ Storage Selection
✓ Relay Selection

FIELDS:

InternetAccess
Purpose:
Can reach internet.

CrossNetworkBridge
Purpose:
Can bridge isolated networks.

HotspotCapable
Purpose:
Can create local mesh access.

UplinkBandwidthMbps
Purpose:
Available upload bandwidth.

LatencyMs
Purpose:
Network responsiveness.

UptimeSeconds
Purpose:
Node stability.

StorageTotalMB
Purpose:
Total disk space.

StorageAvailableMB
Purpose:
Free disk space.

CanStoreChunks
Purpose:
Can participate in distributed storage.

CanRelayTraffic
Purpose:
Can forward traffic.

StableNode
Purpose:
Preferred long-term infrastructure node.
```
---
title: System Vision & Architecture Principles
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---

# <span style="color: #3498db;"> System Vision & Architecture Principles</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">1. Overall System Vision</span>

The system is evolving into:

> **A decentralized hybrid content network** that supports:
> - distributed storage
> - peer-to-peer content sharing
> - chunk-based large file distribution
> - metadata indexing
> - future decentralized search
> - capability-aware node participation
> - streaming-oriented retrieval

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">2. Core Architectural Principle</span>

The system **separates concerns** into independent layers.

>  **This is extremely important.** The system does NOT treat:
> - routing
> - storage
> - indexing
> - search
> - provider discovery
> 
> as the same thing.

Instead:

| Layer | Responsibility |
|-------|----------------|
| Kademlia | Routing & lookup |
| Layer 3A | Service registry & node capabilities |
| Layer 3B | Distributed storage |
| Layer 3C | Metadata indexing & search |
| Discovery Protocol | Peer availability tracking |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">3. Role of Kademlia</span>

Kademlia is used **ONLY** as:
- routing infrastructure
- distributed key lookup
- distributed key-value transport

Kademlia is **NOT**:
- the storage system itself
- the search engine
- the indexing system

Kademlia only provides: `key → value` lookup

Core operations:
- `FIND_NODE`
- `STORE`
- `FIND_VALUE`

The storage system is implemented **ON TOP** of Kademlia.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">4. Capability-Aware Participation</span>

Not all nodes are equal. The system uses capability-aware participation.

Nodes may advertise:
- storage capacity
- available storage
- bandwidth
- uptime
- willingness to store chunks

This influences:
- chunk placement
- provider selection
- retrieval prioritization

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">5. Node Role Policies</span>

| Role | Permitted Services |
|------|-------------------|
| `game` | games, chat, matchmaking |
| `chat` | chat, messaging, presence |
| `cache` | cache, storage, cdn |
| `storage` | storage, backup, files |
| `unknown` | (none) |

> ⚠️ Any service registration attempt that violates this policy is rejected.
---
title: Layer 3A — Service Registry
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
file: service.go
---

# <span style="color: #3498db;"> Layer 3A — Service Registry</span>

Layer 3A is the authoritative registry of which nodes are offering which network services (chat, storage, games, etc.). It includes capability tracking, provider scoring, and a background cleanup goroutine.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Architecture Diagram</span>

<table style="width: 100%; border-collapse: collapse; text-align: center;">
  <tr><td style="border: 1px solid #ddd; padding: 8px;">Layer 7: Applications (chat, file sharing, games, etc.)</td</tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">⬆️ uses</td</tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">
    <code>service/</code> package (Layers 3A + 3B + 3C)
    <table style="width: 100%; margin-top: 5px;">
      <tr>
        <td style="border: 1px solid #ccc;">Layer 3A<br>Service & Provider Registry</td>
        <td style="border: 1px solid #ccc;">Layer 3B<br>Distributed Storage Engine</td>
        <td style="border: 1px solid #ccc;">Layer 3C<br>Metadata Indexing & Search</td>
      </tr>
    </table>
  </td>
  </tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">⬆️ uses InnerCore (DHT)</td>
  </tr>
</table>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Design Principle</span>

> **No sub-layer does network I/O directly.** Every sub-layer prepares data structures and returns them to the caller. The caller issues the actual DHT operations through InnerCore. This keeps all business logic unit-testable without a running network.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Initialization</span>

Called once at system startup from `integration.go`:

```
service.Init()

// Called after NodeID is known (must be called after Init)
service.SetOwnerID(selfID)
```
```Init()``` sets up the registry and launches the background cleanup goroutine. <br>```SetOwnerID()``` wires the node's identity into the content store and index writer.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Registration</span>

Called automatically by the discovery layer when a DISCOVERY_ANNOUNCE packet is received:

```
result := service.RegisterServicesFromDiscovery(
    deviceID,    // types.NodeID of the announcing device
    services,    // []string{"chat", "presence"}
    servicePort, // int — the port the service runs on
    role,        // string — the node's declared role
)
// result["accepted"] → []string of accepted service names
// result["rejected"] → []string of rejected service names
```
<h3>What happens internally:</h3>

> The device's role is checked against RoleServicePolicy

>Each service is validated against the policy

>Accepted services are registered in ServiceRegistry with a ProviderInfo entry

>A provider score is computed using calculateProviderScore()

>A ProviderTTL timer starts — the entry expires after 45 seconds if not re-announced

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Scoring</span>

Provider selection is score-based, not health-only. Scores are recomputed every time capabilities or health change:

```
func calculateProviderScore(caps types.Capabilities, health float64) float64
```
<table>
    <thead>
        <tr>
            <th>Signal</th>
            <th>Weight</th>
            <th>Reason</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Upload bandwidth</td>
            <td>× 0.4</td>
            <td>Higher bandwidth = better provider</td>
        </tr>
        <tr>
            <td>Available storage</td>
            <td>× 0.003</td>
            <td>More storage = more reliable</td>
        </tr>
        <tr>
            <td>Can store chunks</td>
            <td>+40.0</td>
            <td>Core chunk hosting capability</td>
        </tr>
        <tr>
            <td>Stable node</td>
            <td>+25.0</td>
            <td>Long uptime = reliable source</td>
        </tr>
        <tr>
            <td>Cross-network bridge</td>
            <td>+30.0</td>
            <td>Critical for mesh bridging</td>
        </tr>
        <tr>
            <td>Health</td>
            <td>× 20.0</td>
            <td>Real-time success/failure feedback</td>
        </tr>
    </tbody>
</table>

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Querying Providers</span>

```
// Get all healthy providers for a service, sorted by score (best first)
providers := service.GetServiceProviders("chat", true, 0.5)

// Get full info about a service including its providers
info := service.GetServiceInfo("chat")

// Get all registered services
all := service.GetAllServices()
```
```GetServiceProviders() ```filters by:

Provider announced within the last ```ProviderTTL``` (45 seconds)

Device status "alive" (```if requireAlive = true```)

Device health ≥ ```minHealth```

Results are sorted descending by ```Score```.


<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Background Cleanup</span>

The cleanup goroutine runs every 30 seconds:

```
Every 30s:
  for each service:
    for each provider:
      if time.Since(LastAnnounce) > 45s:
        delete provider
    if no providers left:
      delete service entry
```

This prevents unbounded registry growth as nodes come and go.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Health Feedback</span>

Called by Layer 4 (messaging) after each send attempt:

```
service.UpdateProviderHealth(deviceID, success bool)
```
> On success: health +0.05 (capped at 1.0)

> On failure: health -0.15 (floored at 0.0)

Health changes trigger a score recomputation, so a degrading node automatically sinks in provider rankings.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ProviderInfo Structure</span>

```
type ProviderInfo struct {
    LastAnnounce time.Time          // when last seen
    Metadata     map[string]any     // port, ip, device_name, role
    Health       float64            // 0.0 → 1.0
    Capabilities types.Capabilities // full device capabilities
    Score        float64            // computed fitness score
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Constants</span>

<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
            <th>Meaning</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>ProviderTTL</td>
            <td>45s</td>
            <td>How long a service provider stays registered without re-announcing</td>
        </tr>
    </tbody>
</table>
---
title: Layer 3B — Distributed Storage
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
files: types.go, keygen.go, content_store.go, dht.go, provider.go, persistence.go, retrieval.go
---

# <span style="color: #3498db;">Layer 3B — Distributed Storage</span>

Layer 3B is the hybrid content storage engine. It handles the full lifecycle of a file: chunking, hashing, local caching, DHT distribution, replication, provider announcement, and retrieval.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Types</span>

### `ContentMeta`

The authoritative description of a stored file. Stored in the DHT under `/content/<ContentID>`.

```go
type ContentMeta struct {
    ContentID         string       // SHA256 of full file, or Merkle root for chunked files
    Name              string       // human-readable filename
    Type              string       // "video", "image", "doc", etc.
    OwnerNodeID       types.NodeID // publisher's node ID
    CreatedAt         int64        // unix timestamp
    UpdatedAt         int64
    SizeBytes         int64
    IsChunked         bool
    ChunkCount        int          // 0 if not chunked
    ChunkSize         int64        // bytes per chunk (last chunk may be smaller)
    Hash              string       // full file SHA256
    HashAlgo          string       // always "sha256"
    ChunkManifestID   string       // fetch at /chunk-manifest/<ContentID>
    Version           int
    PrevVersion       string       // ContentID of previous version, if updated
    Tags              []string
    Keywords          []string
    ReplicationFactor int          // default: 3
}
```
 Design note: ```ContentMeta``` is intentionally lean. For a chunked file with 10,000 chunks, embedding the full chunk list would make this a massive DHT value. The chunk list lives separately in a ```ChunkManifest```.

<hr style="border: 1px solid #ecf0f1;">

```ChunkManifest```

The ordered list of chunks for a large file. Stored separately under ```/chunk-manifest/<ContentID>```. Only fetched when a client actually needs to download the file.

```
type ChunkManifest struct {
    ContentID string      // parent content ID
    Chunks    []ChunkMeta // ordered list
}

type ChunkMeta struct {
    Index     int    // 0-based position in file
    Hash      string // SHA256 of this chunk
    SizeBytes int64  // actual size (last chunk may differ)
}
```

<hr style="border: 1px solid #ecf0f1;">

```ContentProvider```

Tracks a node that is currently hosting a piece of content.

```
type ContentProvider struct {
    NodeID       types.NodeID       // who has it
    IP           string
    Port         int
    Capabilities types.Capabilities
    AnnouncedAt  int64              // unix timestamp
    HasFull      bool               // true = complete file, false = partial
    ChunksHeld   []int              // which chunk indexes (if partial)
}
```
<hr style="border: 1px solid #ecf0f1;">

```ContentRef```

A lightweight search index reference. Denormalized for fast display — avoids a second DHT lookup just to render a search result.

```
type ContentRef struct {
    ContentID string
    Name      string
    Type      string
    SizeBytes int64
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> The Hybrid Storage Model</span>

Files are stored using one of two paths based on their size:

<table>
    <thead>
        <tr>
            <th>File Size</th>
            <th>Path</th>
            <th>Behavior</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>≤ 1 MB (ChunkThreshold)</td>
            <td>Small file</td>
            <td>Single DHT STORE, IsChunked = false</td>
        </tr>
        <tr>
            <td>> 1 MB</td>
            <td>Large file</td>
            <td>Split into 256 KB chunks, each stored independently, IsChunked = true</td>
        </tr>
    </tbody>
</table>

> Why this matters: Kademlia STORE operations carry their value in UDP packets. A 500 MB file cannot fit in a UDP packet. Chunking spreads the load, enables parallel downloads, allows partial retrieval, and makes the system resilient.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ContentStore</span>

```ContentStore``` is the local storage engine. It handles chunking logic and local caching. It does not do network I/O.

```
// Create a store for this node
cs := service.NewContentStore(ownerNodeID)
```
```Store()``` — the main entry point

```
result, err := cs.Store(
    name,     // string — filename e.g. "Suits.S01E01.mkv"
    fileType, // string — "video", "image", etc.
    data,     // []byte — raw file bytes
    tags,     // []string — user labels e.g. ["tv", "drama"]
    keywords, // []string — secondary metadata e.g. ["suits", "harvey"]
)
```
Returns a ```StoreResult```:

```
type StoreResult struct {
    Meta     *ContentMeta   // always populated
    Manifest *ChunkManifest // only populated for chunked files
    Chunks   []ChunkData    // raw chunk bytes for DHT distribution
}

type ChunkData struct {
    Hash string // chunk's SHA256 — used as DHT key
    Data []byte // the raw bytes — the DHT value
}
```
The caller takes ```StoreResult``` and issues the actual DHT ```STORE``` operations through InnerCore.

<hr style="border: 1px solid #ecf0f1;">
Local cache lookups

```
// Returns nil if not cached locally — caller must do DHT FIND_VALUE
meta := cs.GetMeta(contentID)

// Returns nil if not cached locally
chunkBytes := cs.GetChunk(chunkHash)

// True only if this node has the full file ready
hasIt := cs.HasContent(contentID)

// All content this node holds locally
list := cs.ListLocalContent() // []*ContentMeta
```

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ContentID Generation</span>

Content is identified by a deterministic hash, not by filename or path:

```
// Small files: SHA256 of the entire file
contentID := ContentIDFromBytes(data)

// Large files: SHA256 of the concatenated chunk hashes (Merkle-lite)
contentID := ContentIDFromChunks([]string{chunkHash1, chunkHash2, ...})

// Individual chunk hash
chunkHash := ChunkHashFromBytes(chunkData)
```
This means:

> The same file always produces the same ContentID — automatic deduplication

> Changing any byte changes the ContentID — automatic integrity verification

> Order matters for chunked files — same chunks in different order = different ID

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> DHT Publisher</span>

```DHTPublisher``` handles replication to multiple nodes using Kademlia ```STORE``` operations:

```
// Initialize once at startup, after InnerCore is ready
service.InitDHTPublisher(innerCoreInstance)

publisher := service.GetDHTPublisher()
```

What gets stored to DHT:

<table>
    <thead>
        <tr>
            <th>What</th>
            <th>DHT Key</th>
            <th>DHT Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>File metadata</td>
            <td>/content/&lt;ContentID&gt;</td>
            <td>ContentMeta</td>
        </tr>
        <tr>
            <td>Chunk list</td>
            <td>/chunk-manifest/&lt;ContentID&gt;</td>
            <td>ChunkManifest</td>
        </tr>
        <tr>
            <td>Each chunk</td>
            <td>/chunk/&lt;ChunkHash&gt;</td>
            <td>raw bytes</td>
        </tr>
    </tbody>
</table>

All three are stored concurrently using goroutines.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Announcement</span>

After storing content, a node announces itself as a provider:

```
service.AnnounceContent(
    contentID, // string
    hasFull,   // bool — true if holding complete file
    chunks,    // []int — which chunk indexes (nil if hasFull)
)
```

Providers are tracked in ```ContentProviderRegistry``` with a TTL of 5 minutes. A background cleanup goroutine runs every 2 minutes and removes expired announcements.

```
// Start provider cleanup (called once at startup)
go service.StartProviderCleanup()

// Query current providers for a content ID
providers := service.GetProviders(contentID) // []ContentProvider
```

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Persistence</span>

Content metadata and manifests are persisted to the Layer 5 StorageEngine for survival across restarts:

```
// Save ContentMeta and ChunkManifest to Layer 5
err := service.PersistContent(result)

// Load ContentMeta from Layer 5
meta, err := service.LoadContentMeta(contentID)
```
Persistence uses two Layer 5 collections:

```"content"``` → stores serialized ```ContentMeta``` JSON

```"chunk_manifests"``` → stores serialized ```ChunkManifest``` JSON

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Retrieval</span>

```
result, err := service.RetrieveContent(contentID)
```

Retrieval follows a priority order:

> Local cache (ContentStore.GetMeta + GetChunk)

> Known providers (GetProviders → direct peer download)

> DHT FIND_VALUE via InnerCore (queries 3 closest peers)

> Error: "content not found"

Current status: Full chunk reassembly and streaming are marked for future work. The retrieval path successfully locates content and initiates FIND_VALUE RPCs, but full byte reassembly from multiple peers is not yet implemented.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> High-Level Public API</span>

The recommended entry point for applications:

```
// Store + index + announce + persist + DHT publish in one call
result, err := service.PublishContent(
    name, fileType, data, tags, keywords,
)

// Retrieve content by ID
result, err := service.RetrieveContent(contentID)

// Simple local keyword search (Layer 3C wrapper)
refs := service.Search(keyword) // []*ContentRef
```
```PublishContent()``` internally calls:

```contentStore.Store()``` — chunk and hash

```AnnounceContent()``` — register as provider

```IndexContent()``` — add to search index

```PersistContent()``` — save to Layer 5

```publishContentToDHT()``` — distribute to peers

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Constants</span>

<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
            <th>Meaning</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>ChunkThreshold</td>
            <td>1 MB</td>
            <td>Files larger than this are automatically chunked</td>
        </tr>
        <tr>
            <td>DefaultChunkSize</td>
            <td>256 KB</td>
            <td>Size of each chunk for large files</td>
        </tr>
        <tr>
            <td>DefaultReplicationFactor</td>
            <td>3</td>
            <td>Default number of nodes that should hold each file</td>
        </tr>
        <tr>
            <td>ReplicationK</td>
            <td>5</td>
            <td>Max number of peers targeted per DHT STORE operation</td>
        </tr>
        <tr>
            <td>StoreTimeout</td>
            <td>8s</td>
            <td>Timeout per DHT STORE attempt</td>
        </tr>
        <tr>
            <td>ProviderAnnounceTTL</td>
            <td>5m</td>
            <td>How long a provider announcement stays valid</td>
        </tr>
    </tbody>
</table>
---
title: Layer 3C — Metadata Indexing & Search
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
files: index_types.go, tokenizer.go, index_writer.go, index_reader.go
---

# <span style="color: #3498db;">Layer 3C — Metadata Indexing & Search</span>

Layer 3C answers a completely different question from Layer 3B:
- **Layer 3B** answers: "retrieve bytes by their hash"
- **Layer 3C** answers: "find content by human-readable text"

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Mental Model</span>

Think of it as the index at the back of a book:

| Component | Analogy |
|-----------|---------|
| The book (content) | Layer 3B |
| The index at the back | Layer 3C |

**Example flow:**
```
"Suits S01E01.mp4" is published →
Tokenizer extracts: ["suits", "s01e01", "1080p", "bluray"]
For each token, stored in DHT:
/index/suits → [{ContentID: "abc", Name: "Suits S01E01.mp4", ...}]
/index/s01e01 → [{ContentID: "abc", ...}]

User searches "suits" →
Tokenize "suits" → ["suits"]
Fetch /index/suits from DHT → entries
Score and rank → return SearchResponse
```

  

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Types</span>

### `IndexEntry`

One item in a keyword → content mapping.

```go
type IndexEntry struct {
    ContentID       string
    Name            string   // denormalized — avoids extra DHT fetch for display
    Type            string   // denormalized
    SizeBytes       int64    // denormalized
    Tags            []string
    IndexedAt       int64    // unix timestamp
    UpdatedAt       int64
    MatchWeight     float64  // 1.0 = filename match, 0.8 = tag, 0.5 = keyword
    PublisherNodeID string
}
```
```IndexBucket```

The full value stored at one DHT key. One bucket per keyword, holding all content that matches.

```go
type IndexBucket struct {
    Keyword   string       // normalized keyword
    Entries   []IndexEntry // all matching content
    UpdatedAt int64
}
```
```SearchQuery```

```go
type SearchQuery struct {
    RawQuery     string // "Suits Season 1" — tokenizer processes this
    TypeFilter   string // optional: "video", "image", etc. — empty = all types
    MaxSizeBytes int64  // optional: 0 = no limit
    MaxResults   int    // 0 defaults to 20
    Page         int    // 0-based pagination
}
```
```SearchResponse```
```go
type SearchResponse struct {
    Query      string         // original raw query
    Tokens     []string       // what the tokenizer extracted
    Results    []SearchResult // ranked results, best first
    TotalFound int            // before pagination
    Page       int
    TookMs     int64          // search latency in milliseconds
    FromCache  bool           // true if served from local cache
}
```

```SearchResult```
```go
type SearchResult struct {
    ContentID     string
    Name          string
    Type          string
    SizeBytes     int64
    Tags          []string
    Score         float64   // 0.0 → 1.0, higher = better match
    MatchedTokens []string  // which query tokens matched this result
    IndexedAt     time.Time
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> The Tokenizer</span>

The tokenizer bridges human language and machine-readable DHT keys. Without it, ```"Suits.S01E03.1080p.BluRay.mkv"``` and the query ```"suits"``` would share zero literal characters and never match.

```go
tokenizer := service.NewTokenizer()
```

Normalization Pipeline
Step	Input	Output
Input	"Suits.S01E03.1080p.BluRay.mkv"	-
Lowercase	-	"suits.s01e03.1080p.bluray.mkv"
Split on [^a-z0-9]+	-	["suits", "s01e03", "1080p", "bluray", "mkv"]
Drop empties	-	(none)
Drop len < 2	-	(none)
Drop stopwords	-	(none)
Drop extensions	-	["suits", "s01e03", "1080p", "bluray"]
Deduplicate	-	(no dupes)

Output: ```["suits", "s01e03", "1080p", "bluray"]```


<hr style="border: 1px solid #ecf0f1;">

<h3>Token Weights by Source</h3>

When tokenizing a ContentMeta, each source field produces tokens at a different weight:

Source	Weight	Reason
Filename (no extension)	1.0	The primary identifier — strongest signal
Tags	0.8	User-defined labels — high value
Keywords	0.5	Secondary metadata
File type	0.3	Enables type-based filtering
If a token appears in multiple sources, the highest weight wins.

<hr style="border: 1px solid #ecf0f1;">
<h3>Key Tokenizer Methods</h3>

```go
// Primary: tokenize a full ContentMeta (all fields, with weights)
tokens := tokenizer.TokenizeContentMeta(meta) // []WeightedToken

// Tokenize just a filename (strips extension, normalizes)
tokens := tokenizer.TokenizeFilename("Suits.S01E03.mkv") // ["suits", "s01e03"]

// Tokenize a user search query (same normalization as filenames)
tokens := tokenizer.TokenizeQuery("suits season 1") // ["suits", "season", "1"]

// Infer file type from extension
ftype := service.DetectFileType("movie.mp4") // "video"

// Extract TV episode code
code, ok := service.ExtractEpisodeCode("Suits.S01E03.mkv") // "s01e03", true

// Extract year from filename
year, ok := service.ExtractYear("The.Dark.Knight.2008.mkv") // "2008", true

// Check if a token is a technical quality tag
is := service.IsQualityTag("1080p") // true
```

<hr style="border: 1px solid #ecf0f1;">

<h3>Safety Limits</h3>

Limit	Value	Reason
MinTokenLength	2	Tokens shorter than 2 characters are discarded
MaxTokensPerField	50	Prevents DHT flooding from malicious input

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Index Writer</span>

```IndexWriter``` builds and maintains the distributed index when content is published.

```go
writer := service.NewIndexWriter(ownerNodeID)
```

```Index()``` — the main entry point
Call this immediately after a successful ```ContentStore.Store():```

```go
writeRecords := writer.Index(meta) // []IndexWriteRecord
```
Returns one IndexWriteRecord per unique token extracted from the content. A file producing 12 tokens generates 12 write records.

```go
type IndexWriteRecord struct {
    DHTKey    types.NodeID // result of IndexKey(keyword) — pass to InnerCore.SendStore()
    Keyword   string       // human-readable keyword (for logging)
    Entry     IndexEntry   // the index entry to store at this key
    BucketKey string       // full key string e.g. "/index/suits"
}
```
The caller distributes these to the DHT:

```go
writeRecords := writer.Index(storeResult.Meta)
for _, rec := range writeRecords {
    innerCore.SendStore(rec.DHTKey, rec.Entry)
    indexReader.FeedEntry(rec.Keyword, rec.Entry) // also update local cache
}
```

``` Remove()``` — stop tracking content
```go
writer.Remove(contentID)
```
This removes the content from local tracking. The DHT entries are not actively deleted (Kademlia has no native delete operation). Instead, the node stops re-announcing, and the entries expire naturally after ```IndexEntryTTL = 30 minutes```.

<hr style="border: 1px solid #ecf0f1;">
<h3>Re-announcement System</h3>

DHT values have a TTL. The writer tracks everything it has indexed and periodically re-publishes entries to keep them alive:

```go
// Get write records for content that needs re-announcing
records := writer.GetReAnnounceRecords(contentStore) // []IndexWriteRecord

// Start the automatic background re-announcement loop
go writer.StartMaintenanceLoop(contentStore, func(records []IndexWriteRecord) {
    for _, r := range records {
        innerCore.SendStore(r.DHTKey, r.Entry)
    }
})
```

Timeline:

t=0: Content indexed, entries stored to DHT

t=20m: ```ReAnnounceInterval``` fires → entries re-stored (still 10m left on TTL)

t=30m: ```IndexEntryTTL``` would expire, but re-announce already happened

t=40m: ```ReAnnounceInterval``` fires again → cycle continues

<hr style="border: 1px solid #ecf0f1;">

<h3>Writer Stats</h3>

```go
stats := writer.GetStats()
// IndexStats{
//   TotalKeywords: int,   // unique tokens tracked
//   TotalEntries:  int,   // total content-keyword pairs
//   LocallyOwned:  int,   // content items we originally published
//   LastWriteAt:   int64, // unix timestamp of last index write
// }

ids := writer.GetLocalContentIDs() // []string — all content IDs being tracked
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Index Reader</span>

``` IndexReader``` executes searches. It maintains a local cache of index buckets fetched from the DHT.

```go
reader := service.NewIndexReader()
```
<h3>Two-Phase Search Pattern</h3>

The reader never does DHT I/O directly. Instead, it uses a two-phase approach:

Phase 1: Try local cache

```go
resp := reader.SearchLocal(query)
if resp.TotalFound > 0 {
    return resp // served instantly from cache
}
```

Phase 2: Fetch from DHT, then search again

```go
// Find out which DHT keys need to be fetched
requests := reader.GetDHTKeysForQuery(query) // []DHTLookupRequest

for _, req := range requests {
    // Do the DHT lookup (caller's responsibility)
    bucket := innerCore.FindValue(req.DHTKey)

    // Feed the result back into the reader's cache
    reader.FeedBucket(req.Keyword, bucket)
}

// Now search again with populated cache
resp = reader.SearchLocal(query)
```
```SearchLocal()``` — full search with scoring and pagination

```go
query := service.SearchQuery{
    RawQuery:   "suits season 1",
    TypeFilter: "video",    // optional
    MaxResults: 20,
    Page:       0,
}

resp := reader.SearchLocal(query)
// resp.Results → []SearchResult sorted by Score descending
// resp.TotalFound → total before pagination
// resp.TookMs → latency in milliseconds
// resp.FromCache → true (SearchLocal only reads cache)
```
<hr style="border: 1px solid #ecf0f1;">

<h3>Scoring System</h3>

Each result is scored on a scale of 0.0 → 1.0 using four additive signals:

<table>
    <thead>
        <tr>
            <th>Signal</th>
            <th>Weight</th>
            <th>Formula</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Token match strength</td>
            <td>0.40</td>
            <td>Average MatchWeight of matched tokens</td>
        </tr>
        <tr>
            <td>Token coverage</td>
            <td>0.30</td>
            <td>matched_tokens / total_query_tokens</td>
        </tr>
        <tr>
            <td>Recency</td>
            <td>0.20</td>
            <td>max(0, 1 - age_in_days / 30)</td>
        </tr>
        <tr>
            <td>Type match bonus</td>
            <td>0.10</td>
            <td>1.0 if type filter matches, 0.0 otherwise</td>
        </tr>
    </tbody>
</table>

Example: query ```"suits season 1"``` → tokens ```["suits", "season", "1"]```

A result matching only "suits" (from filename, weight 1.0):

Match strength:``` 1.0 × 0.40 = 0.40```

Token coverage: ```1/3 × 0.30 = 0.10```

Recency (fresh):``` 1.0 × 0.20 = 0.20```

Type match: ```0.10 ```(if type filter matches)

Total: 0.80

A result matching ``` "suits" ``` (tag, weight 0.8) and ``` "season" ``` (keyword, weight 0.5):

Match strength: ```avg(0.8, 0.5) × 0.40 = 0.26```

Token coverage: ```2/3 × 0.30 = 0.20```

Recency (fresh): ```0.20```

Total: 0.66

The first result ranks higher despite fewer matched tokens because the filename match is stronger.

<hr style="border: 1px solid #ecf0f1;">

<h3>Cache Management</h3>

```go
// Feed a full index bucket into the cache (after DHT fetch)
reader.FeedBucket(keyword, bucket)

// Feed a single entry into the cache (after receiving individual entries)
reader.FeedEntry(keyword, entry)

// Remove all cached entries for a specific content ID (when content is deleted)
reader.InvalidateContent(contentID)

// Evict all expired cache entries (call from maintenance loop)
purged := reader.PurgeExpiredCache() // returns count of purged entries

// Inspect what's in the cache right now
keywords := reader.GetCachedKeywords() // []string

// Reader stats
stats := reader.GetStats()
```
Cache entries expire after ```cacheEntryTTL = 30 minutes```. Expiry is lazy — entries are checked on access, not on a timer. ```PurgeExpiredCache()``` should be called from a maintenance goroutine to prevent unbounded memory growth.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Constants</span>


<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
            <th>Meaning</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>IndexEntryTTL</td>
            <td>30m</td>
            <td>How long an index entry lives in DHT without re-announcement</td>
        </tr>
        <tr>
            <td>ReAnnounceInterval</td>
            <td>20m</td>
            <td>How often the writer re-pushes index entries (must be &lt; IndexEntryTTL)</td>
        </tr>
        <tr>
            <td>cacheEntryTTL</td>
            <td>30m</td>
            <td>How long the reader keeps a fetched bucket in local cache</td>
        </tr>
        <tr>
            <td>defaultMaxResults</td>
            <td>20</td>
            <td>Default result count when SearchQuery.MaxResults == 0</td>
        </tr>
        <tr>
            <td>MinTokenLength</td>
            <td>2</td>
            <td>Tokens shorter than this are discarded</td>
        </tr>
        <tr>
            <td>MaxTokensPerField</td>
            <td>50</td>
            <td>Max tokens extracted per content item</td>
        </tr>
    </tbody>
</table>

<h2>Scoring Weights</h2>
<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>weightTokenMatchStrength</td>
            <td>0.40</td>
        </tr>
        <tr>
            <td>weightTokenCoverage</td>
            <td>0.30</td>
        </tr>
        <tr>
            <td>weightRecency</td>
            <td>0.20</td>
        </tr>
        <tr>
            <td>weightTypeMatch</td>
            <td>0.10</td>
        </tr>
    </tbody>
</table>


```markdown
---
title: DHT Keyspace Design
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
file: keygen.go
---
```
# <span style="color: #3498db;"> DHT Keyspace Design</span>

All content-related keys are namespaced before being hashed into Kademlia's 256-bit keyspace. Without namespacing, a ContentID and a ChunkHash that happen to be identical would collide — silently corrupting data.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Namespaces</span>

| Namespace prefix | DHT value stored | Generator function |
|------------------|------------------|-------------------|
| `/content/<ContentID>` | `ContentMeta` | `ContentKey(contentID)` |
| `/chunk-manifest/<ContentID>` | `ChunkManifest` | `ChunkManifestKey(contentID)` |
| `/chunk/<ChunkHash>` | raw `[]byte` | `ChunkKey(chunkHash)` |
| `/provider/<ContentID>` | `[]ContentProvider` | `ProviderKey(contentID)` |
| `/index/<keyword>` | `IndexBucket` | `IndexKey(keyword)` |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Key Generation</span>

All keys go through the same pipeline:
raw string (e.g. "/content/abc123...")
↓
SHA256()
↓
types.NodeID (32 bytes)
↓
Kademlia routes using XOR distance

```text

This means content keys and node ID keys live in the same 256-bit keyspace and use the exact same XOR distance routing. No separate routing system is needed for content lookups.
```
<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Key Functions</span>

```go
// Examples
key := ContentKey("sha256hashoffile...")    // types.NodeID
key := ChunkKey("sha256ofchunkbytes...")    // types.NodeID
key := IndexKey("suits")                    // types.NodeID — normalized, lowercased
```
```IndexKey()``` normalizes its input before hashing:

```go
IndexKey("  SUITS  ") == IndexKey("suits") // true — case-insensitive, trimmed
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;">
 Why Namespacing Matters</span>

Without namespacing:

A file's ContentID = ```abc123```

A chunk's ChunkHash = ```abc123``` (unlikely but possible)

Both would map to the same DHT key → data corruption

With namespacing:

Content key = ```SHA256(/content/abc123)```

Chunk key = ```SHA256(/chunk/abc123)```
Different DHT keys → no collision