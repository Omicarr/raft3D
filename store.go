package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// NewBoltStores creates or opens the BoltDB stores for Raft logs and stable storage.
// It takes the node-specific data directory where the 'db' subdirectory will be created.
func NewBoltStores(nodeRaftDir string) (*raftboltdb.BoltStore, *raftboltdb.BoltStore, error) {
	dbDir := filepath.Join(nodeRaftDir, "db") // Create a 'db' subdirectory inside node dir

	// Ensure the db directory exists
	if err := os.MkdirAll(dbDir, 0750); err != nil { // Use 0750 permissions
		log.Printf("[ERROR] Failed to create BoltDB directory %s: %v", dbDir, err)
		return nil, nil, fmt.Errorf("failed to create bolt store directory: %w", err)
	}

	// Log store
	logStorePath := filepath.Join(dbDir, "raft-log.bolt") // Use .bolt extension
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		log.Printf("[ERROR] Failed to create BoltDB log store at %s: %v", logStorePath, err)
		return nil, nil, fmt.Errorf("failed to create log store: %w", err)
	}
	log.Printf("[INFO] BoltDB log store created/opened: %s", logStorePath)

	// Stable store (for configuration like current term, voted for)
	stableStorePath := filepath.Join(dbDir, "raft-stable.bolt") // Use .bolt extension
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		_ = logStore.Close() // Clean up previous store if stable store fails
		log.Printf("[ERROR] Failed to create BoltDB stable store at %s: %v", stableStorePath, err)
		return nil, nil, fmt.Errorf("failed to create stable store: %w", err)
	}
	log.Printf("[INFO] BoltDB stable store created/opened: %s", stableStorePath)

	return logStore, stableStore, nil
}
