// integration/integration.go
// System Integration Layer
//
// This is the "brain" of the entire system.
// It initializes all layers in the correct order and starts background tasks.
// Provides a unified high-level API for the rest of the application.

// integration/integration.go
// System Integration Layer - The brain of the InnerCore Mesh

package integration

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/discovery"
	"innercore-network/fallback"
	"innercore-network/innercore"
	"innercore-network/message"
	"innercore-network/network"
	"innercore-network/resolver"
	"innercore-network/service"
	"innercore-network/storage"
	"innercore-network/types"
)

type SystemIntegrator struct {
	innerCore   *innercore.InnerCore
	resolver    *resolver.Layer2Resolver
	gateway     *fallback.InternetGateway
	running     bool
	startupTime time.Time
	mu          sync.RWMutex
}

var (
	integrator *SystemIntegrator
	once       sync.Once
)

func Init(instanceID int) {
	once.Do(func() {
		integrator = &SystemIntegrator{startupTime: time.Now()}

		fmt.Printf("\n=== Starting Local-First InnerCore Mesh - Instance %d (NexusFabric) ===\n", instanceID)

		storage.InitializeStorage()

		// === Multi-instance testing support ===
		discoveryPort := 37020
		messagePort := 51000 + instanceID*10

		discovery.Init([]string{"chat", "storage"}, 5000, discoveryPort, messagePort)

		integrator.innerCore = innercore.New(
			storage.GlobalStorage.GetEngine(),
			message.SendPacket,
		)

		integrator.innerCore.SetNodeID(discovery.GetMyNodeID())

		message.KademliaHandler = integrator.innerCore.HandleKademliaPacket

		discovery.UpdateNetworkCallback = func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int) {
			// Force real IP for local multi-instance testing
			if ip == "" || ip == "127.0.0.1" {
				ip = "192.168.2.104" // Change to your actual local IP if needed
			}
			network.UpdateNode(nodeID, ip, name, caps, services, servicePort)
			integrator.innerCore.SeedPeer(nodeID, ip, caps)
		}

		// === Self Registration with Full NexusFabric Capabilities ===
		selfID := discovery.GetMyNodeID()
		selfCaps := types.Capabilities{
			InternetAccess:      true,
			CrossNetworkBridge:  true,
			HotspotCapable:      true,
			UplinkBandwidthMbps: 120,
			LatencyMs:           5,
			UptimeSeconds:       7200,

			// === NexusFabric Storage Capabilities ===
			StorageTotalMB:     8192, // 8 GB
			StorageAvailableMB: 6144, // 6 GB free
			CanStoreChunks:     true,
			CanRelayTraffic:    true,
			StableNode:         true,
		}

		network.UpdateNode(selfID, "127.0.0.1", discovery.GetMyNodeName(), selfCaps, []string{"chat", "storage"}, 5000)
		integrator.innerCore.SeedPeer(selfID, "127.0.0.1", selfCaps)

		message.Init()
		service.Init()

		// Initialize Layer 3A + 3B + 3C
		service.SetOwnerID(selfID)
		service.InitDHTPublisher(integrator.innerCore)
		service.InitIndex()

		// Start background cleanup routines
		go service.StartProviderCleanup() // ← Fixed: exported name

		// Content Fabric is now live
		fmt.Println("[NEXUSFABRIC] ContentStore ready for hybrid storage")

		// Initialize NexusFabric DHT Publisher
		service.InitDHTPublisher(integrator.innerCore)

		integrator.resolver = resolver.NewLayer2Resolver()

		integrator.gateway = fallback.NewInternetGateway("0.0.0.0", 8080+instanceID, integrator.innerCore)
		integrator.gateway.Start()

		integrator.running = true

		fmt.Println("\n✅ System successfully started! (NexusFabric Ready)")
		fmt.Printf("   Instance         : %d\n", instanceID)
		fmt.Printf("   Node ID          : %s\n", selfID)
		fmt.Printf("   Node Name        : %s\n", discovery.GetMyNodeName())
		fmt.Printf("   Discovery Port   : %d\n", discoveryPort)
		fmt.Printf("   Message Port     : %d\n", messagePort)
		fmt.Printf("   Storage Available: %d MB\n", selfCaps.StorageAvailableMB)
		fmt.Println("   Status           : Operational")
	})
}

func GetInstance() *SystemIntegrator {
	return integrator
}

func GetInnerCore() *innercore.InnerCore {
	if integrator == nil {
		return nil
	}
	return integrator.innerCore
}

func (si *SystemIntegrator) Stop() {
	si.mu.Lock()
	defer si.mu.Unlock()

	if !si.running {
		return
	}

	fmt.Println("\n[SHUTDOWN] Graceful shutdown initiated...")
	si.running = false
	message.Close()
	fmt.Println("[SHUTDOWN] System stopped.")
}

// ==================== Public API ====================

func SendMessage(target types.NodeID, payload any) error {
	return message.SendToNode(target, payload)
}

func GetNetworkInfo() map[string]any {
	peers := network.GetAllPeers()
	alive := 0
	roles := make(map[string]int)
	services := make(map[string]int)

	for _, p := range peers {
		if p.Status == "alive" {
			alive++
		}
		roles[p.Role]++
		for _, svc := range p.Services {
			services[svc]++
		}
	}

	return map[string]any{
		"node_id":        discovery.GetMyNodeID(),
		"node_name":      discovery.GetMyNodeName(),
		"total_devices":  len(peers),
		"alive_devices":  alive,
		"roles":          roles,
		"services":       services,
		"uptime_seconds": int(time.Since(integrator.startupTime).Seconds()),
	}
}

func GetDeviceInfo(deviceID types.NodeID) any {
	return network.GetPeer(deviceID)
}

func RegisterService(serviceName string, port int) bool {
	fmt.Printf("[INTEGRATION] Service '%s' registered on port %d\n", serviceName, port)
	return true
}

func HealthCheck() map[string]any {
	peers := network.GetAllPeers()
	healthy := 0
	for _, p := range peers {
		if p.Status == "alive" && p.Health >= 0.5 {
			healthy++
		}
	}

	status := "DEGRADED"
	if integrator.running {
		status = "HEALTHY"
	}

	return map[string]any{
		"status": status,
		"metrics": map[string]any{
			"total_devices":   len(peers),
			"healthy_devices": healthy,
			"uptime_seconds":  int(time.Since(integrator.startupTime).Seconds()),
		},
	}
}
