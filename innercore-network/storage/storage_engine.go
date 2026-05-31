// storage/storage_engine.go
// Layer 5 — Storage Protocol (Core)
//
// Responsibilities:
//   - Authoritative persistence
//   - Versioned record storage
//   - Thread-safe read/write with optimistic concurrency control
//   - NO routing, NO caching, NO business logic
//
// This is the foundation used by all other storage extensions.

package storage

import (
	"sync"
	"time"
)

// StorageError types
type StorageError string

const (
	ErrRecordNotFound  StorageError = "record not found"
	ErrVersionConflict StorageError = "version conflict"
)

func (e StorageError) Error() string { return string(e) }

// Record is the canonical versioned storage record
type Record struct {
	ID        string         `json:"id"`
	Version   int            `json:"version"`
	Payload   map[string]any `json:"payload"`
	CreatedAt int64          `json:"created_at"`
}

// StorageEngine is the thread-safe in-memory authoritative engine
type StorageEngine struct {
	// store[collection][recordID][version] = Record
	store map[string]map[string]map[int]Record
	mu    sync.RWMutex
}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{
		store: make(map[string]map[string]map[int]Record),
	}
}

// Get returns the latest version of a record
func (s *StorageEngine) Get(collection, recordID string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	coll, exists := s.store[collection]
	if !exists {
		return Record{}, ErrRecordNotFound
	}

	versions, exists := coll[recordID]
	if !exists || len(versions) == 0 {
		return Record{}, ErrRecordNotFound
	}

	// Find latest version
	var latest Record
	maxVer := -1
	for ver, rec := range versions {
		if ver > maxVer {
			maxVer = ver
			latest = rec
		}
	}
	return latest, nil
}

// Put inserts or updates a record with optimistic locking
func (s *StorageEngine) Put(collection, recordID string, payload map[string]any, expectedVersion *int) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.store[collection]; !exists {
		s.store[collection] = make(map[string]map[int]Record)
	}
	if _, exists := s.store[collection][recordID]; !exists {
		s.store[collection][recordID] = make(map[int]Record)
	}

	versions := s.store[collection][recordID]
	currentVersion := 0
	if len(versions) > 0 {
		for v := range versions {
			if v > currentVersion {
				currentVersion = v
			}
		}
	}

	if expectedVersion != nil && *expectedVersion != currentVersion {
		return Record{}, ErrVersionConflict
	}

	newVersion := currentVersion + 1
	record := Record{
		ID:        recordID,
		Version:   newVersion,
		Payload:   payload,
		CreatedAt: time.Now().Unix(),
	}

	s.store[collection][recordID][newVersion] = record
	return record, nil
}

// Delete removes a record with version check
func (s *StorageEngine) Delete(collection, recordID string, expectedVersion *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	coll, exists := s.store[collection]
	if !exists {
		return ErrRecordNotFound
	}

	versions, exists := coll[recordID]
	if !exists || len(versions) == 0 {
		return ErrRecordNotFound
	}

	currentVersion := 0
	for v := range versions {
		if v > currentVersion {
			currentVersion = v
		}
	}

	if expectedVersion != nil && *expectedVersion != currentVersion {
		return ErrVersionConflict
	}

	delete(coll, recordID)
	if len(coll) == 0 {
		delete(s.store, collection)
	}
	return nil
}
