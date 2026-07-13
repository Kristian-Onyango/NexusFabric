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