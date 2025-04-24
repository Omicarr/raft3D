// this is node.go
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const (
	// raftTimeout specifies how long we wait for Raft operations.
	raftTimeout = 10 * time.Second

	// retainSnapshotCount specifies how many snapshots are retained. Must be at least 1.
	retainSnapshotCount = 2

	// raftLogCacheSize specifies the maximum number of logs to cache in memory.
	// This is used to reduce disk I/O for the recently committed entries.
	// raftLogCacheSize = 512 // raft-boltdb doesn't use this directly
)

// SetupRaft initializes and returns a Raft node.
func SetupRaft(config *Config, fsm raft.FSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	// Adjust Raft timings if needed for your network environment
	// raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	// raftConfig.ElectionTimeout = 1000 * time.Millisecond
	// raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	// raftConfig.CommitTimeout = 50 * time.Millisecond

	// Configure snapshotting
	raftConfig.SnapshotInterval = 20 * time.Second // How often Raft checks if a snapshot is needed
	raftConfig.SnapshotThreshold = 50            // How many log entries before triggering a snapshot

	log.Printf("[INFO] Raft Configuration:")
	log.Printf("  Local ID: %s", raftConfig.LocalID)
	log.Printf("  Snapshot Interval: %s", raftConfig.SnapshotInterval)
	log.Printf("  Snapshot Threshold: %d", raftConfig.SnapshotThreshold)

	// Setup Raft communication (TCP transport)
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		log.Printf("[ERROR] Failed to resolve Raft TCP address '%s': %v", config.RaftAddr, err)
		return nil, fmt.Errorf("resolve raft address '%s': %w", config.RaftAddr, err)
	}
	// Third argument is maxPool (number of connections to keep open), 0 means no limit
	// Use a reasonable number like 3 for a small cluster.
	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		log.Printf("[ERROR] Failed to create Raft TCP transport on %s: %v", config.RaftAddr, err)
		return nil, fmt.Errorf("create raft transport: %w", err)
	}
	log.Printf("[INFO] Raft TCP transport created, listening on: %s", transport.LocalAddr())

	// Create snapshot store. Path is the node-specific data directory.
	// config.RaftDir is already node-specific path from LoadConfig
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		log.Printf("[ERROR] Failed to create snapshot store in '%s': %v", config.RaftDir, err)
		_ = transport.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}
	log.Printf("[INFO] Raft snapshot store created in '%s'", config.RaftDir)

	// Create log and stable stores using BoltDB. Path is the node-specific data directory.
	logStore, stableStore, err := NewBoltStores(config.RaftDir) // NewBoltStores creates its own 'db' subdir
	if err != nil {
		log.Printf("[ERROR] Failed to create BoltDB stores in '%s': %v", config.RaftDir, err)
		_ = transport.Close()
		// Snapshot store doesn't have an explicit Close method
		return nil, fmt.Errorf("create bolt stores: %w", err)
	}
	log.Printf("[INFO] Raft BoltDB stores created/opened.")

	// Instantiate the Raft system
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Printf("[ERROR] Failed to create Raft instance: %v", err)
		// Attempt to close stores on error
		if cerr := logStore.Close(); cerr != nil {
			log.Printf("[WARN] Failed to close log store during Raft init error: %v", cerr)
		}
		if cerr := stableStore.Close(); cerr != nil {
			log.Printf("[WARN] Failed to close stable store during Raft init error: %v", cerr)
		}
		_ = transport.Close()
		return nil, fmt.Errorf("create raft instance: %w", err)
	}
	log.Printf("[INFO] Raft instance created successfully for node %s", config.NodeID)

	// --- Cluster Bootstrapping or Joining ---
	if config.Bootstrap {
		// Check if peer info is provided for multi-node bootstrap
		peerIDs := strings.Split(config.PeerIDs, ",")
		peerAddrs := strings.Split(config.PeerAddrs, ",")
		isMultiNode := len(peerIDs) > 0 && len(peerIDs) == len(peerAddrs) && config.PeerIDs != "" && config.PeerAddrs != ""

		var bootstrapConfig raft.Configuration
		if isMultiNode {
			// --- Multi-node bootstrap ---
			servers := make([]raft.Server, 0, len(peerIDs))
			foundSelf := false
			for i, idStr := range peerIDs {
				addrStr := strings.TrimSpace(peerAddrs[i])
				idTrimmed := strings.TrimSpace(idStr)
				if idTrimmed == "" || addrStr == "" {
					return nil, fmt.Errorf("invalid peer configuration during bootstrap: ID='%s', Addr='%s'", idTrimmed, addrStr)
				}
				server := raft.Server{
					ID:      raft.ServerID(idTrimmed),
					Address: raft.ServerAddress(addrStr),
				}
				servers = append(servers, server)
				// Check if this server entry corresponds to the current node
				if idTrimmed == config.NodeID {
					foundSelf = true
					// Ensure the address listed for self matches the actual transport address
					if server.Address != transport.LocalAddr() {
						log.Printf("[WARN] Bootstrap peer address '%s' for self node '%s' differs from transport local address '%s'. Using transport address.", server.Address, config.NodeID, transport.LocalAddr())
						servers[len(servers)-1].Address = transport.LocalAddr() // Correct it in the list
					}
				}
			}
			// The current node MUST be included in the peer list for bootstrap
			if !foundSelf {
				return nil, fmt.Errorf("node ID '%s' not found in bootstrap peer IDs: %s", config.NodeID, config.PeerIDs)
			}
			bootstrapConfig.Servers = servers
			log.Printf("[INFO] Attempting multi-node bootstrap with configuration: %+v", bootstrapConfig.Servers)
		} else {
			// --- Single-node bootstrap ---
			log.Printf("[INFO] Attempting single-node bootstrap for node: %s at %s", config.NodeID, transport.LocalAddr())
			bootstrapConfig = raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftConfig.LocalID,
						Address: transport.LocalAddr(), // Use the address the transport is actually listening on
					},
				},
			}
		}

		// Attempt bootstrap
		bootstrapFuture := raftNode.BootstrapCluster(bootstrapConfig)
		if err := bootstrapFuture.Error(); err != nil {
			// Check if the error indicates the cluster is already bootstrapped or has state
			// This is common on restarts if bootstrap flag is accidentally left on.
			if errors.Is(err, raft.ErrCantBootstrap) {
				log.Printf("[WARN] Bootstrap requested, but cluster cannot be bootstrapped (likely already exists or has state): %v. Continuing...", err)
			} else {
				// Other potentially fatal bootstrap errors
				log.Printf("[ERROR] Failed to bootstrap cluster: %v", err)
				_ = raftNode.Shutdown().Error() // Attempt shutdown on fatal bootstrap error
				return nil, fmt.Errorf("bootstrap cluster failed: %w", err)
			}
		} else {
			log.Printf("[INFO] Cluster bootstrap initiated successfully.")
			// Wait briefly for leadership after bootstrap
			time.Sleep(2 * time.Second)
		}
	} else if config.JoinAddr != "" {
		// --- Explicit Join Attempt ---
		log.Printf("[INFO] Node %s attempting to join existing cluster via Raft address: %s", config.NodeID, config.JoinAddr)

		// Check if already part of the cluster - prevents unnecessary AddVoter calls on restart
		currentConfigFuture := raftNode.GetConfiguration()
		if err := currentConfigFuture.Error(); err == nil {
			for _, srv := range currentConfigFuture.Configuration().Servers {
				if srv.ID == raftConfig.LocalID || srv.Address == transport.LocalAddr() {
					log.Printf("[INFO] Node %s (%s) is already part of the Raft cluster configuration. Skipping AddVoter.", config.NodeID, transport.LocalAddr())
					return raftNode, nil // Already joined
				}
			}
		} else {
			log.Printf("[WARN] Could not get current Raft configuration before join attempt (this is normal if joining for the first time): %v", err)
		}

		// Attempt to join the cluster by adding self as a voter.
		// This RPC needs to reach the *leader*. The client library handles redirection.
		// The node at JoinAddr doesn't have to be the leader, just a member we can talk to initially.
		// AddVoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration)
		// Using 0 for prevIndex means Raft will figure it out.
		log.Printf("[INFO] Calling AddVoter for self (ID: %s, Addr: %s)", raftConfig.LocalID, transport.LocalAddr())
		addVoterFuture := raftNode.AddVoter(raftConfig.LocalID, transport.LocalAddr(), 0, raftTimeout)
		if err := addVoterFuture.Error(); err != nil {
			log.Printf("[ERROR] Failed to add self as voter to cluster (check leader reachability and logs): %v", err)
			// Decide if this is fatal. For robustness, maybe allow startup and retry later?
			// return nil, fmt.Errorf("failed to add voter: %w", err)
			log.Printf("[WARN] Continuing startup despite AddVoter error. Node %s may join later via peer discovery.", config.NodeID)
		} else {
			log.Printf("[INFO] Successfully added node %s as a voter via AddVoter.", config.NodeID)
		}
	} else {
		// Neither bootstrap nor join: This node expects to restart using existing state
		log.Printf("[INFO] Node %s starting without bootstrap or explicit join. Expecting existing state in %s.", config.NodeID, config.RaftDir)
	}

	log.Printf("[INFO] Raft setup complete for node %s.", config.NodeID)
	return raftNode, nil
}

// applyCommand serializes and applies a command to the Raft log.
// This MUST be called on the LEADER node. Middleware should ensure this.
// It returns the response from FSM.Apply (which could be an error).
func applyCommand(r *raft.Raft, cmdType CommandType, payload interface{}) error {
	if r.State() != raft.Leader {
		// This should ideally be caught by middleware, but serves as a final safeguard
		log.Printf("[WARN] applyCommand called on non-leader node (%s). Returning ErrNotLeader.", r.State())
		return raft.ErrNotLeader
	}

	cmdBytes, err := serializeCommand(cmdType, payload)
	if err != nil {
		log.Printf("[ERROR] Failed to serialize command %s: %v", cmdType, err)
		return fmt.Errorf("command serialization failed: %w", err)
	}

	// Apply the command to the Raft log. This is asynchronous.
	applyFuture := r.Apply(cmdBytes, raftTimeout)

	// Wait for the Apply operation to complete (committed and applied locally, or timeout/error).
	// ApplyFuture.Error() returns Raft-level errors.
	if err := applyFuture.Error(); err != nil {
		log.Printf("[ERROR] Raft Apply failed for command %s: %v", cmdType, err)
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// ApplyFuture.Response() returns the interface{} returned by your FSM.Apply().
	// Check if the FSM itself returned an error (e.g., validation error).
	response := applyFuture.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		log.Printf("[INFO] Command %s applied to Raft log, but FSM returned error: %v", cmdType, responseErr)
		// Return the specific error from the FSM logic
		return responseErr
	}

	// If response is not an error, or is nil, command was successful at FSM level too
	log.Printf("[DEBUG] Successfully applied command %s via Raft.", cmdType)
	return nil
}
