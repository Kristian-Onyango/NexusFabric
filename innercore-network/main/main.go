// main/main.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"innercore-network/discovery"
	"innercore-network/integration"
	"innercore-network/network"
	"innercore-network/service"
)

func main() {
	fmt.Println("🔥 InnerCore Mesh Network Starting...")
	fmt.Printf("Go Version: %s\n", "1.21+")

	// Initialize the entire system
	instanceID := 0
	// You can pass argument from command line later
	if len(os.Args) > 1 && os.Args[1] == "1" {
		instanceID = 1
	}
	integration.Init(instanceID)

	// Show initial state
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("Local-First Internet Substrate - GO EDITION")
	fmt.Println(strings.Repeat("=", 60))

	// Print network state periodically
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			network.PrintNetworkState()
		}
	}()

	// === NexusFabric Content Tests ===
	go func() {
		time.Sleep(10 * time.Second)

		fmt.Println("\n=== NEXUSFABRIC CONTENT TEST ===")

		testData := []byte("Hello from InnerCore Mesh in Kenya! This is a test file.")
		result, err := service.StoreContent("test.txt", "text/plain", testData, []string{"test"}, []string{"kenya", "mesh"})
		if err != nil {
			fmt.Printf("Publish failed: %v\n", err)
		} else {
			fmt.Printf("✅ Published: %s (ID: %s)\n", result.Meta.Name, result.Meta.ContentID[:16]+"...")
		}

		results := service.Search("kenya")
		fmt.Printf("Search 'kenya' returned %d results\n", len(results))

		if result != nil {
			_, err = service.RetrieveContent(result.Meta.ContentID)
			if err != nil {
				fmt.Printf("Retrieval note: %v\n", err)
			}
		}
	}()

	// Test Kademlia lookup after discovery settles
	go func() {
		time.Sleep(12 * time.Second) // longer wait for discovery to stabilize

		if ic := integration.GetInnerCore(); ic != nil {
			selfID := discovery.GetMyNodeID()

			fmt.Println("\n[TEST] Triggering Kademlia self-lookup...")
			ic.TestLookup(selfID)

			fmt.Println("\n[TEST] Looking for other discovered peers via Kademlia...")

			// Retry a few times because discovery can be async
			for attempt := 0; attempt < 5; attempt++ {
				peers := network.GetAllPeers()
				fmt.Printf("[TEST] Attempt %d - Found %d total peers in table\n", attempt+1, len(peers))

				foundRemote := false
				for id := range peers {
					if !id.Equal(selfID) {
						fmt.Printf("[TEST] Triggering Kademlia lookup for remote peer: %s\n", id)
						ic.TestLookup(id)
						foundRemote = true
						break
					}
				}
				if foundRemote {
					break
				}
				time.Sleep(3 * time.Second)
			}

			if len(network.GetAllPeers()) <= 1 {
				fmt.Println("[TEST] No remote peer found yet. Discovery still settling...")
			}
		}
	}()
	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[SHUTDOWN] Received shutdown signal...")

	integration.GetInstance().Stop()

	fmt.Println("System shutdown complete. Goodbye.")
}
