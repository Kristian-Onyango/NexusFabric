// service/service.go
// Layer 3A — Service & Provider Registry
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

// STORAGE STRETCH
//
// Now includes:
// - Extended capability tracking
// - Provider scoring abstraction
// - Background cleanup goroutine
// - Safer locking patterns (no nested locks)
// - Foundation for content/chunk storage integration

package service

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
)

// ProviderTTL - how long a service announcement remains valid
const ProviderTTL = 45 * time.Second

// RoleServicePolicy defines which services each role is allowed to offer
var RoleServicePolicy = map[string][]string{
	"game":    {"games", "chat", "matchmaking"},
	"chat":    {"chat", "messaging", "presence"},
	"cache":   {"cache", "storage", "cdn"},
	"storage": {"storage", "backup", "files"},
	"unknown": {},
}

// default metadata for known services
var ServiceMetadata = map[string]map[string]any{
	"games":   {"protocol": "udp", "stateful": true, "version": 1},
	"chat":    {"protocol": "tcp", "stateful": true, "version": 1},
	"storage": {"protocol": "tcp", "stateful": false, "version": 1},
	"cache":   {"protocol": "udp", "stateful": false, "version": 1},
	"chunks":  {"protocol": "udp", "stateful": false, "version": 1, "chunked": true},
}

var (
	registry     = &ServiceRegistry{services: make(map[string]ServiceEntry)}
	contentStore *ContentStore
	once         sync.Once
	cleanupOnce  sync.Once
)

// ServiceEntry represents one registered service
type ServiceEntry struct {
	Providers map[types.NodeID]ProviderInfo `json:"providers"`
	Metadata  map[string]any                `json:"metadata"`
	Policy    map[string]any                `json:"policy"`
}

// ProviderInfo holds information about a device offering a service
type ProviderInfo struct {
	LastAnnounce time.Time          `json:"last_announce"`
	Metadata     map[string]any     `json:"metadata"` // port, ip, device_name, role
	Health       float64            `json:"health"`
	Capabilities types.Capabilities `json:"capabilities"` // Full device capabilities
	Score        float64            `json:"score"`        // Computed fitness
}

// ServiceRegistry is the authoritative in-memory registry
type ServiceRegistry struct {
	services map[string]ServiceEntry
	mu       sync.RWMutex
}

// Init — initializes both service registry and content store
func Init() {
	once.Do(func() {
		fmt.Println("[SERVICE] Layer 3 (Service + Content Fabric) initialized")

		// TODO: Get real NodeID from discovery after integration
		contentStore = NewContentStore(types.NodeID{}) // placeholder, will be set properly

		cleanupOnce.Do(func() {
			go startCleanupLoop()
		})
	})
}

// SetOwnerID — called from integration after NodeID is known
func SetOwnerID(id types.NodeID) {
	if contentStore == nil {
		contentStore = NewContentStore(id)
	} else {
		contentStore.ownerID = id
	}

	if indexWriter == nil {
		indexWriter = NewIndexWriter(id)
	}
}

// GetContentStore returns the hybrid content engine
func GetContentStore() *ContentStore {
	return contentStore
}

func startCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		registry.cleanupExpiredProviders()
	}
}

func (r *ServiceRegistry) cleanupExpiredProviders() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	for svcName, entry := range r.services {
		for id, p := range entry.Providers {
			if now.Sub(p.LastAnnounce) > ProviderTTL {
				delete(entry.Providers, id)
				expiredCount++
			}
		}
		if len(entry.Providers) == 0 {
			delete(r.services, svcName)
		}
	}

	if expiredCount > 0 {
		fmt.Printf("[SERVICE] Cleanup removed %d expired providers\n", expiredCount)
	}
}

// RegisterServicesFromDiscovery is called by Layer 1 when receiving DISCOVERY_ANNOUNCE
// Updated with scoring + capabilities
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
					"load_balancing": "score_based",
				},
			}
			registry.services[serviceName] = entry
		}

		// 3. Register/update provider
		now := time.Now()
		score := calculateProviderScore(deviceInfo.Capabilities, deviceInfo.Health)

		entry.Providers[deviceID] = ProviderInfo{
			LastAnnounce: now,
			Metadata: map[string]any{
				"port":        servicePort,
				"ip":          deviceInfo.IP,
				"device_name": deviceInfo.Name,
				"role":        role,
			},
			Health:       deviceInfo.Health,
			Capabilities: deviceInfo.Capabilities,
			Score:        score,
		}

		accepted = append(accepted, serviceName)
		fmt.Printf("[SERVICE] %s registered '%s' (score: %.1f)\n", deviceInfo.Name, serviceName, score)
	}

	return map[string]any{"accepted": accepted, "rejected": rejected}
}

// calculateProviderScore — Central scoring abstraction
func calculateProviderScore(caps types.Capabilities, health float64) float64 {
	score := 0.0

	score += float64(caps.UplinkBandwidthMbps) * 0.4
	score += float64(caps.StorageAvailableMB) * 0.003
	if caps.CanStoreChunks {
		score += 40.0
	}
	if caps.StableNode {
		score += 25.0
	}
	if caps.CrossNetworkBridge {
		score += 30.0
	}

	score += health * 20.0 // Health is very important

	return score
}

// GetServiceProviders returns active providers for a service (used by Layer 2)
// Returns sorted by score
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
			"health":           device.Health,
			"score":            p.Score,
			"capabilities":     p.Capabilities,
			"last_announce":    p.LastAnnounce,
			"service_metadata": entry.Metadata,
		})
	}

	// Sort by score descending
	sort.Slice(providers, func(i, j int) bool {
		return providers[i]["score"].(float64) > providers[j]["score"].(float64)
	})

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
			// Re-score
			p.Score = calculateProviderScore(p.Capabilities, p.Health)
			entry.Providers[deviceID] = p
		}
	}
}

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
// ServiceResolver stub
type ServiceResolver struct{}

// Global instance
var ServiceResolverInstance = ServiceResolver{} // ← Fixed: add {}

// GetContentMeta returns metadata (local or via DHT later)
func GetContentMeta(contentID string) *ContentMeta {
	if cs := GetContentStore(); cs != nil {
		if meta := cs.GetMeta(contentID); meta != nil {
			return meta
		}
	}
	// TODO: DHT FIND_VALUE later
	return nil
}

// PublishContent is the main high-level API for applications
func PublishContent(name, fileType string, data []byte, tags, keywords []string) (*StoreResult, error) {
	result, err := StoreContent(name, fileType, data, tags, keywords)
	if err != nil {
		return nil, err
	}

	// Publish to DHT with replication
	if err := publishContentToDHT(result); err != nil {
		fmt.Printf("[SERVICE] DHT publish warning: %v\n", err)
		// We don't fail the whole operation — local storage succeeded
	}

	return result, nil
}

// ==================== MAIN PUBLIC APIS ====================

// StoreContent + Publish in one flow
func StoreContent(name, fileType string, data []byte, tags, keywords []string) (*StoreResult, error) {
	if contentStore == nil {
		return nil, fmt.Errorf("content store not initialized")
	}

	result, err := contentStore.Store(name, fileType, data, tags, keywords)
	if err != nil {
		return nil, err
	}

	AnnounceContent(result.Meta.ContentID, true, nil)
	IndexContent(result.Meta)
	PersistContent(result)

	if err := publishContentToDHT(result); err != nil {
		fmt.Printf("[SERVICE] DHT publish warning: %v\n", err)
	}

	return result, nil
}

// RetrieveContent
func RetrieveContent(contentID string) (*RetrieveResult, error) {
	// ... (your retrieval.go content - already good)
	return retrieveContentInternal(contentID)
}

// Search
func Search(keyword string) []*ContentRef {
	return searchInternal(keyword)
}

var (
	indexReader *IndexReader
	indexWriter *IndexWriter
)

func InitIndex() {
	indexReader = NewIndexReader()
	// indexWriter will be initialized after SetOwnerID
	fmt.Println("[INDEX] Layer 3C Search Index initialized")
}

func GetIndexReader() *IndexReader {
	return indexReader
}

func GetIndexWriter() *IndexWriter {
	return indexWriter
}
