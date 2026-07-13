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