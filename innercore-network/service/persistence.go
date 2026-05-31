// service/persistence.go
// Layer 3B — Persistence using Layer 5 StorageEngine

package service

import (
	"encoding/json"
	"fmt"

	"innercore-network/storage"
)

// PersistContent saves metadata and manifest to persistent storage
func PersistContent(result *StoreResult) error {
	if result == nil || result.Meta == nil {
		return fmt.Errorf("nothing to persist")
	}

	engine := storage.GlobalStorage.GetEngine()

	// Save ContentMeta
	metaBytes, _ := json.Marshal(result.Meta)
	_, err := engine.Put("content", result.Meta.ContentID, map[string]any{
		"meta":      string(metaBytes),
		"timestamp": result.Meta.CreatedAt,
	}, nil)
	if err != nil {
		return err
	}

	// Save manifest if chunked
	if result.Manifest != nil {
		manifestBytes, _ := json.Marshal(result.Manifest)
		engine.Put("chunk_manifests", result.Meta.ContentID, map[string]any{
			"manifest": string(manifestBytes),
		}, nil)
	}

	fmt.Printf("[PERSISTENCE] Saved %s to Layer 5\n", result.Meta.Name)
	return nil
}

// LoadContentMeta from persistent storage
func LoadContentMeta(contentID string) (*ContentMeta, error) {
	engine := storage.GlobalStorage.GetEngine()
	rec, err := engine.Get("content", contentID)
	if err != nil {
		return nil, err
	}

	var meta ContentMeta
	if payload, ok := rec.Payload["meta"].(string); ok {
		json.Unmarshal([]byte(payload), &meta)
		return &meta, nil
	}
	return nil, fmt.Errorf("invalid meta format")
}
