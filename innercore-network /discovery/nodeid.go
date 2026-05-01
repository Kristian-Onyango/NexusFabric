// discovery/nodeid.go
package discovery

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"innercore-network/types" // ← Add this import
)

const nodeIDFile = "node_id.bin"

// LoadOrCreateNodeID loads existing NodeID or creates a new persistent one
func LoadOrCreateNodeID() (types.NodeID, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return types.NodeID{}, err
	}

	// Support multiple instances for testing
	instanceSuffix := ""
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceSuffix = "_instance1"
	}

	path := filepath.Join(home, ".innercore", "node_id"+instanceSuffix+".bin")

	// Try to load existing
	if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
		var id types.NodeID
		copy(id[:], data)
		fmt.Printf("[DISCOVERY] Loaded existing NodeID: %s\n", id)
		return id, nil
	}

	// Create new
	var id types.NodeID
	if _, err := rand.Read(id[:]); err != nil {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("%d%s", time.Now().UnixNano(), instanceSuffix)))
		copy(id[:], h.Sum(nil))
	}

	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, id[:], 0600)

	fmt.Printf("[DISCOVERY] Generated new NodeID: %s\n", id)
	return id, nil
}
