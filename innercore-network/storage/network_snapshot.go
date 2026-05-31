// storage/network_snapshot.go
// Layer 5 Extension — Network Snapshot Store
//
// Persists last-known network state for warm restarts.

package storage

import (
	"innercore-network/types"
	"time"
)

const (
	SnapshotCollection = "network_snapshots"
	LatestSnapshotID   = "latest"
)

type NetworkSnapshotStore struct {
	storage *StorageEngine
}

func NewNetworkSnapshotStore(storage *StorageEngine) *NetworkSnapshotStore {
	return &NetworkSnapshotStore{storage: storage}
}

func (ns *NetworkSnapshotStore) SaveSnapshot(networkTable map[types.NodeID]any) (Record, error) {
	payload := map[string]any{
		"timestamp":     time.Now().Unix(),
		"device_count":  len(networkTable),
		"network_table": networkTable,
	}

	return ns.storage.Put(SnapshotCollection, LatestSnapshotID, payload, nil)
}

func (ns *NetworkSnapshotStore) LoadLatestSnapshot() (Record, error) {
	return ns.storage.Get(SnapshotCollection, LatestSnapshotID)
}

func (ns *NetworkSnapshotStore) SnapshotExists() bool {
	_, err := ns.LoadLatestSnapshot()
	return err == nil
}
