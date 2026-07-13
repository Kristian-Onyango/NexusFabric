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