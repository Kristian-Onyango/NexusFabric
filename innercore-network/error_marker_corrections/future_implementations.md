SendPacket() is now correctly using the remote port

This:

port := peer.MessagePort

if port == 0 {
    port = discovery.LocalMessagePort
}

addr := &net.UDPAddr{
    IP:   net.ParseIP(peer.IP),
    Port: port,
}

is correct for the immediate goal.

The important difference is:

Before
Node A
51000
  ↓
Node B
51000 ❌
Now
Node A
51000
  ↓
PeerEntry says B = 51010
  ↓
Node B
51010 ✅

The fallback is also reasonable during development.

Eventually, though, I'd prefer a missing MessagePort to be treated as "peer information incomplete" rather than silently assuming 51000. Otherwise an old/stale peer announcement can produce confusing failures.

But don't remove the fallback yet. It's useful while we're getting the system working



Your message.go improvement is actually quite interesting

You added:

msgPort := addr.Port
if msgPort == 0 {
    msgPort = 51000
}

network.UpdateNode(
    senderID,
    remoteIP,
    "",
    types.Capabilities{},
    nil,
    5000,
    msgPort,
)

This means the receiver can learn:

"The UDP source port this packet actually came from is X."

That's useful because the actual source port is stronger evidence than a hardcoded value.

However, be careful about one thing:

addr.Port is the source UDP port of the packet, not necessarily the node's advertised Layer-4 port in every future architecture.

Right now they are effectively the same because your messaging layer sends from LocalMessagePort.

So for the current system:

source UDP port = message port

Good.

Later, if you introduce NAT, relays, proxies, multiplexing, etc., that assumption may stop being true.

For the current MVP, keep it.

// message.go
type UDPAddr struct {
 IP IP
 Port int
 Zone string // IPv6 scoped addressing zone
}

UDPAddr represents the address of a UDP end point.

func (a *net.UDPAddr) AddrPort() netip.AddrPort
func (a *net.UDPAddr) Network() string
func (a *net.UDPAddr) String() string

net.UDPAddr on pkg.go.dev
```
IMPLEMENT LINE 88
```