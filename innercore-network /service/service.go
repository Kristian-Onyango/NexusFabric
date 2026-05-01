// service/service.go
// Layer 3 — Service Protocol & Registry
//
// Purpose:
//   Authoritative service registration and discovery system.
//   Registers services announced via discovery, enforces role-based policies,
//   and provides clean queries for Layer 2 resolver.
//
// Key Design Principles:
//   - Authoritative and consistent
//   - Role-based policy enforcement
//   - Persistent state with cleanup
//   - Health-aware provider selection
//   - Read-heavy (used heavily by Layer 2)
//
// Depends on:
//   - Layer 1 (network table) for device status and health
//   - Layer 4 (messaging) for health updates
//
// Used by:
//   - Layer 2 (resolver) via ServiceResolver interface
//   - Applications for service discovery

package service

import (
	"fmt"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// ProviderTTL - how long a service announcement remains valid
const ProviderTTL = 30 * time.Second

// RoleServicePolicy defines which services each role is allowed to offer
var RoleServicePolicy = map[string][]string{
	"game":    {"games", "chat", "matchmaking"},
	"chat":    {"chat", "messaging", "presence"},
	"cache":   {"cache", "storage", "cdn"},
	"storage": {"storage", "backup", "files"},
	"unknown": {},
}

// ServiceMetadata contains default metadata for known services
var ServiceMetadata = map[string]map[string]any{
	"games":   {"protocol": "udp", "stateful": true, "version": 1},
	"chat":    {"protocol": "tcp", "stateful": true, "version": 1},
	"storage": {"protocol": "tcp", "stateful": false, "version": 1},
	"cache":   {"protocol": "udp", "stateful": false, "version": 1},
}

// ServiceEntry represents one registered service
type ServiceEntry struct {
	Providers map[types.NodeID]ProviderInfo `json:"providers"`
	Metadata  map[string]any                `json:"metadata"`
	Policy    map[string]any                `json:"policy"`
}

// ProviderInfo holds information about a device offering a service
type ProviderInfo struct {
	LastAnnounce time.Time      `json:"last_announce"`
	Metadata     map[string]any `json:"metadata"` // port, ip, device_name, role
	Health       float64        `json:"health"`
}

// ServiceRegistry is the authoritative in-memory registry
type ServiceRegistry struct {
	services map[string]ServiceEntry
	mu       sync.RWMutex
}

var (
	registry     = &ServiceRegistry{services: make(map[string]ServiceEntry)}
	registryOnce sync.Once
)

// Init initializes the service registry
func Init() {
	registryOnce.Do(func() {
		// TODO: load from disk when Layer 5 is ready
		fmt.Println("[SERVICE] Layer 3 Service Registry initialized")
	})
}

// RegisterServicesFromDiscovery is called by Layer 1 when receiving DISCOVERY_ANNOUNCE
func RegisterServicesFromDiscovery(deviceID types.NodeID, services []string, servicePort int, role string) map[string]any {
	if len(services) == 0 {
		return map[string]any{"accepted": []string{}, "rejected": []string{}, "reason": "no services provided"}
	}

	accepted := []string{}
	rejected := []string{}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	deviceInfo := network.GetPeer(deviceID)
	if deviceInfo == nil {
		for _, svc := range services {
			rejected = append(rejected, svc)
		}
		return map[string]any{"accepted": accepted, "rejected": rejected}
	}

	for _, serviceName := range services {
		// 1. Role-based policy check
		allowed := RoleServicePolicy[role]
		allowedMap := make(map[string]bool)
		for _, s := range allowed {
			allowedMap[s] = true
		}

		if !allowedMap[serviceName] {
			rejected = append(rejected, serviceName)
			continue
		}

		// 2. Get or create service entry
		entry, exists := registry.services[serviceName]
		if !exists {
			entry = ServiceEntry{
				Providers: make(map[types.NodeID]ProviderInfo),
				Metadata:  ServiceMetadata[serviceName],
				Policy: map[string]any{
					"min_providers":  1,
					"min_health":     0.3,
					"load_balancing": "health_based",
				},
			}
			registry.services[serviceName] = entry
		}

		// 3. Register/update provider
		now := time.Now()
		entry.Providers[deviceID] = ProviderInfo{
			LastAnnounce: now,
			Metadata: map[string]any{
				"port":        servicePort,
				"ip":          deviceInfo.IP,
				"device_name": deviceInfo.Name,
				"role":        role,
			},
			Health: deviceInfo.Health,
		}

		accepted = append(accepted, serviceName)
		fmt.Printf("[SERVICE] %s registered service '%s' on port %d\n", deviceInfo.Name, serviceName, servicePort)
	}

	return map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	}
}

// GetServiceProviders returns active providers for a service (used by Layer 2)
func GetServiceProviders(serviceName string, requireAlive bool, minHealth float64) []map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := []map[string]any{}
	now := time.Now()

	for deviceID, p := range entry.Providers {
		if now.Sub(p.LastAnnounce) > ProviderTTL {
			continue
		}

		device := network.GetPeer(deviceID)
		if device == nil {
			continue
		}

		if requireAlive && device.Status != "alive" {
			continue
		}

		if device.Health < minHealth {
			continue
		}

		providers = append(providers, map[string]any{
			"device_id":        deviceID,
			"name":             device.Name,
			"ip":               device.IP,
			"port":             p.Metadata["port"],
			"role":             device.Role,
			"role_trusted":     device.RoleTrusted,
			"health":           device.Health,
			"last_announce":    p.LastAnnounce,
			"service_metadata": entry.Metadata,
		})
	}

	// Sort by health descending (best first)
	// (simple sort for now)
	return providers
}

// GetServiceInfo returns full information about a service
func GetServiceInfo(serviceName string) map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	entry, exists := registry.services[serviceName]
	if !exists {
		return nil
	}

	providers := GetServiceProviders(serviceName, true, 0.5)

	return map[string]any{
		"name":            serviceName,
		"metadata":        entry.Metadata,
		"policy":          entry.Policy,
		"providers":       providers,
		"total_providers": len(providers),
	}
}

// GetAllServices returns information about all registered services
func GetAllServices() map[string]map[string]any {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	result := make(map[string]map[string]any)
	for name := range registry.services {
		if info := GetServiceInfo(name); info != nil {
			result[name] = info
		}
	}
	return result
}

// UpdateProviderHealth is called by Layer 4 when messages succeed/fail
func UpdateProviderHealth(deviceID types.NodeID, success bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for _, entry := range registry.services {
		if p, exists := entry.Providers[deviceID]; exists {
			if success {
				p.Health = min(1.0, p.Health+0.05)
			} else {
				p.Health = max(0.0, p.Health-0.15)
			}
			entry.Providers[deviceID] = p
		}
	}
}

// ... (keep everything the same until the end)

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// ServiceResolver provides the interface expected by Layer 2
type ServiceResolver struct{}

// Global instance
var ServiceResolverInstance = ServiceResolver{} // ← Fixed: add {}
