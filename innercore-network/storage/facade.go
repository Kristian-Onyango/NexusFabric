// storage/facade.go
// Layer 5 — Unified Storage Facade
//
// Single API for ALL persistence needs across layers.
// Uses StorageEngine as backend and provides high-level methods.

package storage

import (
	"fmt"

	"innercore-network/types"
)

type StorageFacade struct {
	engine         *StorageEngine
	networkStore   *NetworkSnapshotStore
	deviceRegistry *DeviceRegistry
	initialized    bool
}

var GlobalStorage = NewStorageFacade()

func NewStorageFacade() *StorageFacade {
	engine := NewStorageEngine()
	return &StorageFacade{
		engine:         engine,
		networkStore:   NewNetworkSnapshotStore(engine),
		deviceRegistry: NewDeviceRegistry(engine),
	}
}

func (f *StorageFacade) Initialize() bool {
	if f.initialized {
		return true
	}
	fmt.Println("[STORAGE] Layer 5 Storage Facade initialized")
	f.initialized = true
	return true
}

// Network State
func (f *StorageFacade) SaveNetworkState(networkTable map[types.NodeID]any) (Record, error) {
	return f.networkStore.SaveSnapshot(networkTable)
}

func (f *StorageFacade) LoadNetworkState() (map[types.NodeID]any, error) {
	rec, err := f.networkStore.LoadLatestSnapshot()
	if err != nil {
		return nil, err
	}
	if nt, ok := rec.Payload["network_table"].(map[types.NodeID]any); ok {
		return nt, nil
	}
	return nil, fmt.Errorf("invalid network table format")
}

// Device Registry
func (f *StorageFacade) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	return f.deviceRegistry.RegisterDevice(deviceID, info)
}

func (f *StorageFacade) GetDevice(deviceID types.NodeID) (Record, error) {
	return f.deviceRegistry.GetDevice(deviceID)
}

// Convenience exports
func InitializeStorage() bool {
	return GlobalStorage.Initialize()
}

func SaveNetworkState(nt map[types.NodeID]any) (Record, error) {
	return GlobalStorage.SaveNetworkState(nt)
}

func LoadNetworkState() (map[types.NodeID]any, error) {
	return GlobalStorage.LoadNetworkState()
}

// GetEngine returns the underlying StorageEngine (used by InnerCore)
func (f *StorageFacade) GetEngine() *StorageEngine {
	return f.engine
}
