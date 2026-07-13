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
