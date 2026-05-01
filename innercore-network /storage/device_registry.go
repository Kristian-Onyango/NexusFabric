// storage/device_registry.go
// Layer 5 Extension — Device Registry
//
// Authoritative persistent device identity storage.
// Used as source of truth for Layer 3 and system recovery.

// storage/device_registry.go
package storage

import (
	"innercore-network/types"
	"time"
)

const DeviceCollection = "devices"

type DeviceRegistry struct {
	storage *StorageEngine
}

func NewDeviceRegistry(storage *StorageEngine) *DeviceRegistry {
	return &DeviceRegistry{storage: storage}
}

func (dr *DeviceRegistry) RegisterDevice(deviceID types.NodeID, info map[string]any) (Record, error) {
	payload := map[string]any{
		"device_id":  deviceID,
		"first_seen": time.Now().Unix(),
		"public_key": info["public_key"],
		"roles":      info["roles"],
		"metadata":   info["metadata"],
	}

	// Fixed: use nodeID.String() instead of string(deviceID)
	return dr.storage.Put(DeviceCollection, deviceID.String(), payload, nil)
}

func (dr *DeviceRegistry) GetDevice(deviceID types.NodeID) (Record, error) {
	return dr.storage.Get(DeviceCollection, deviceID.String())
}

func (dr *DeviceRegistry) DeviceExists(deviceID types.NodeID) bool {
	_, err := dr.GetDevice(deviceID)
	return err == nil
}
