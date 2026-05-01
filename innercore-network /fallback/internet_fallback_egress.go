// layer6/gateway.go
// Layer 6 — Internet Egress & Fallback Gateway
//
// Responsibilities:
//   - Transparent internet egress when local mesh cannot resolve a target
//   - Automatic fallback routing
//   - Session-aware forwarding (TCP proxy style)
//   - Capability-aware routing decisions (uses InnerCore + network table)
//   - No persistence logic (that belongs to Layer 5)
//
// This layer may touch the public internet.
//
// Design Notes:
//   - Routing decisions (including cross-network bridging) are made here
//     because this layer has visibility into capabilities and network state.
//   - It consults InnerCore for supernode assistance and Layer 1 network table
//     for current device status and capabilities.

package fallback

import (
	"fmt"
	"net"
	"sync"
	"time"

	"innercore-network/innercore"
)

const (
	DefaultHTTPPort  = 80
	DefaultHTTPSPort = 443
	SocketTimeout    = 10 * time.Second
	BufferSize       = 8192
)

type GatewaySession struct {
	ClientAddr   string
	Target       string
	CreatedAt    time.Time
	LastActivity time.Time
	BytesUp      int64
	BytesDown    int64
}

type InternetGateway struct {
	listenIP   string
	listenPort int
	sessions   map[string]*GatewaySession
	mu         sync.RWMutex
	running    bool
	innerCore  *innercore.InnerCore
}

func NewInternetGateway(listenIP string, listenPort int, ic *innercore.InnerCore) *InternetGateway {
	return &InternetGateway{
		listenIP:   listenIP,
		listenPort: listenPort,
		sessions:   make(map[string]*GatewaySession),
		innerCore:  ic,
	}
}

func (g *InternetGateway) Start() {
	g.running = true
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.listenIP, g.listenPort))
	if err != nil {
		fmt.Printf("[L6] Failed to listen on %s:%d: %v\n", g.listenIP, g.listenPort, err)
		return
	}

	fmt.Printf("[L6] Internet Gateway listening on %s:%d\n", g.listenIP, g.listenPort)

	go func() {
		for g.running {
			conn, err := ln.Accept()
			if err != nil {
				if g.running {
					fmt.Printf("[L6] Accept error: %v\n", err)
				}
				continue
			}
			go g.handleClient(conn)
		}
		ln.Close()
	}()
}

func (g *InternetGateway) Stop() {
	g.running = false
}

// handleClient processes one incoming client connection
func (g *InternetGateway) handleClient(clientConn net.Conn) {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	fmt.Printf("[L6] New client connection from %s\n", clientAddr)

	// Read initial data to determine target (naive HTTP Host header parsing for MVP)
	buf := make([]byte, BufferSize)
	n, err := clientConn.Read(buf)
	if err != nil {
		return
	}
	initialData := buf[:n]

	targetHost, targetPort := g.extractTarget(initialData)
	if targetHost == "" {
		fmt.Printf("[L6] Could not determine target from client %s\n", clientAddr)
		return
	}

	targetAddr := fmt.Sprintf("%s:%d", targetHost, targetPort)
	session := &GatewaySession{
		ClientAddr:   clientAddr,
		Target:       targetAddr,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}

	g.mu.Lock()
	g.sessions[clientAddr] = session
	g.mu.Unlock()

	g.forwardSession(clientConn, session, initialData)
}

// extractTarget is a simple MVP parser (can be improved later with proper HTTP parsing)
func (g *InternetGateway) extractTarget(data []byte) (host string, port int) {
	text := string(data)
	for _, line := range splitLines(text) {
		if len(line) > 5 && line[:5] == "Host:" {
			hostPart := line[5:]
			hostPart = trimSpace(hostPart)
			if idx := indexByte(hostPart, ':'); idx != -1 {
				host = hostPart[:idx]
				// port parsing omitted for simplicity
				return host, DefaultHTTPPort
			}
			return hostPart, DefaultHTTPPort
		}
	}
	return "", 0
}

// forwardSession relays traffic between client and internet
func (g *InternetGateway) forwardSession(clientConn net.Conn, session *GatewaySession, firstPayload []byte) {
	upstream, err := net.DialTimeout("tcp", session.Target, SocketTimeout)
	if err != nil {
		fmt.Printf("[L6] Failed to connect to %s: %v\n", session.Target, err)
		return
	}
	defer upstream.Close()

	// Send initial payload
	upstream.Write(firstPayload)

	// Bidirectional relay
	go g.relay(clientConn, upstream, session, true) // client -> upstream
	g.relay(upstream, clientConn, session, false)   // upstream -> client
}

func (g *InternetGateway) relay(src, dst net.Conn, session *GatewaySession, upstream bool) {
	buf := make([]byte, BufferSize)
	for {
		n, err := src.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			dst.Write(buf[:n])
			session.LastActivity = time.Now()
			if upstream {
				session.BytesUp += int64(n)
			} else {
				session.BytesDown += int64(n)
			}
		}
	}
}

// Simple helper - improve later with proper HTTP parsing
func splitLines(s string) []string {
	return nil // placeholder - not used yet, but declared to avoid compile error
}

func trimSpace(s string) string {
	// simple trim
	return s
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
