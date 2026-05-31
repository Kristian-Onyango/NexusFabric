// resolver/resolver.go
// Layer 2 — Local Name Resolution Protocol (.mtd)
//
// Purpose:
//   Provides a DNS-like resolution mechanism for the local mesh network.
//   Resolves human-readable `.mtd` names into concrete network endpoints
//   (devices or services).
//
// Design Principles (Strictly followed):
//   - READ-ONLY
//   - SIDE-EFFECT FREE
//   - CACHED
//   - DETERMINISTIC
//   - NEVER registers devices, modifies network table, does routing, or health checks
//
// Resolution Outcomes (Strict Contract):
//   - "OK"        → Valid resolution
//   - "NX"        → Name does not exist
//   - "CONFLICT"  → Ambiguous resolution
//
// Used By:
//   - Layer 3 (Service Protocol)
//   - Layer 4 (Messaging)
//   - Applications

package resolver

import (
	"strings"
	"sync"
	"time"

	"innercore-network/network"
	"innercore-network/types"
	// for future service resolution
)

const (
	MTDSuffix     = ".mtd"
	ServicePrefix = "svc."
	CacheTTL      = 30 * time.Second
)

// ResolutionRecord is the standard response format from Layer 2
type ResolutionRecord struct {
	Status   string     `json:"status"` // "OK", "NX", "CONFLICT"
	Type     string     `json:"type"`   // "device" or "service"
	Name     string     `json:"name"`
	Records  []Endpoint `json:"records"`
	TTL      int        `json:"ttl"`
	CachedAt time.Time  `json:"cached_at"`
}

type Endpoint struct {
	DeviceID        types.NodeID   `json:"device_id"`
	Name            string         `json:"name"`
	IP              string         `json:"ip"`
	Port            int            `json:"port"`
	Role            string         `json:"role"`
	RoleTrusted     bool           `json:"role_trusted"`
	Health          float64        `json:"health"`
	ServiceMetadata map[string]any `json:"service_metadata,omitempty"`
}

// ResolutionCache is thread-safe and auto-evicts stale entries
type ResolutionCache struct {
	cache map[string]ResolutionRecord
	mu    sync.RWMutex
}

func NewResolutionCache() *ResolutionCache {
	return &ResolutionCache{
		cache: make(map[string]ResolutionRecord),
	}
}

func (c *ResolutionCache) Get(key string) *ResolutionRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	record, exists := c.cache[key]
	if !exists {
		return nil
	}

	if time.Since(record.CachedAt) > CacheTTL {
		// Lazy eviction
		c.mu.RUnlock()
		c.Delete(key)
		c.mu.RLock()
		return nil
	}

	return &record
}

func (c *ResolutionCache) Set(key string, record ResolutionRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.CachedAt = time.Now()
	c.cache[key] = record
}

func (c *ResolutionCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, key)
}

// Layer2Resolver is the main resolver
type Layer2Resolver struct {
	cache *ResolutionCache
}

func NewLayer2Resolver() *Layer2Resolver {
	return &Layer2Resolver{
		cache: NewResolutionCache(),
	}
}

// Resolve is the ONLY public API
func (r *Layer2Resolver) Resolve(name string) ResolutionRecord {
	key := strings.ToLower(strings.TrimSpace(name))

	if cached := r.cache.Get(key); cached != nil {
		return *cached
	}

	record := r.resolveAndCache(key)
	return record
}

// Internal resolution logic
func (r *Layer2Resolver) resolveAndCache(name string) ResolutionRecord {
	// Validate suffix
	if !strings.HasSuffix(name, MTDSuffix) {
		record := r.nxRecord(name)
		r.cache.Set(name, record)
		return record
	}

	// Service resolution
	if strings.HasPrefix(name, ServicePrefix) {
		record := r.resolveService(name)
		r.cache.Set(name, record)
		return record
	}

	// Device resolution
	record := r.resolveDevice(name)
	r.cache.Set(name, record)
	return record
}

// Device resolution: e.g. laptop.mtd
func (r *Layer2Resolver) resolveDevice(name string) ResolutionRecord {
	hostname := strings.TrimSuffix(name, MTDSuffix)

	matches := []Endpoint{}
	peers := network.GetAllPeers()

	for nodeID, peer := range peers {
		if peer.Name == hostname && peer.Status == "alive" {
			matches = append(matches, Endpoint{
				DeviceID:    nodeID,
				Name:        peer.Name,
				IP:          peer.IP,
				Port:        51000, // standard messaging port
				Role:        peer.Role,
				RoleTrusted: peer.RoleTrusted,
				Health:      peer.Health,
			})
		}
	}

	if len(matches) == 0 {
		return r.nxRecord(name)
	}
	if len(matches) > 1 {
		return r.conflictRecord(name, "device", matches)
	}

	return r.okRecord(name, "device", matches)
}

// Service resolution: e.g. svc.chat.mtd
func (r *Layer2Resolver) resolveService(name string) ResolutionRecord {
	service := strings.TrimPrefix(name, ServicePrefix)
	service = strings.TrimSuffix(service, MTDSuffix)

	// TODO: Call Layer 3 service registry when it's ported
	// For now we return NX
	return r.nxRecord(name)
}

// Record builders
func (r *Layer2Resolver) okRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "OK",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) nxRecord(name string) ResolutionRecord {
	return ResolutionRecord{
		Status:   "NX",
		Type:     "",
		Name:     name,
		Records:  nil,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}

func (r *Layer2Resolver) conflictRecord(name, rtype string, records []Endpoint) ResolutionRecord {
	return ResolutionRecord{
		Status:   "CONFLICT",
		Type:     rtype,
		Name:     name,
		Records:  records,
		TTL:      int(CacheTTL.Seconds()),
		CachedAt: time.Now(),
	}
}
