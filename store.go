package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Create or open the BoltDB stores for Raft logs and stable storage.
func NewBoltStores(baseDir string) (*raftboltdb.BoltStore, *raftboltdb.BoltStore, error) {
	// Ensure the base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil { // Changed to 0755 for directory permissions
		log.Printf("[ERROR] Failed to create base directory %s: %v", baseDir, err)
		return nil, nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Log store
	logStorePath := filepath.Join(baseDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		log.Printf("[ERROR] Failed to create log store at %s: %v", logStorePath, err)
		return nil, nil, fmt.Errorf("failed to create log store: %w", err)
	}
	log.Printf("[INFO] Created/Opened log store: %s", logStorePath)


	// Stable store (for configuration like current term, voted for)
	stableStorePath := filepath.Join(baseDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		logStore.Close() // Clean up previous store if stable store fails
		log.Printf("[ERROR] Failed to create stable store at %s: %v", stableStorePath, err)
		return nil, nil, fmt.Errorf("failed to create stable store: %w", err)
	}
	log.Printf("[INFO] Created/Opened stable store: %s", stableStorePath)

	return logStore, stableStore, nil
}
