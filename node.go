package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"errors"

	"github.com/hashicorp/raft"
)

const (
	raftTimeout = 10 * time.Second
)

// SetupRaft initializes and returns a Raft node.
func SetupRaft(config *Config, fsm raft.FSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	// raftConfig.HeartbeatTimeout = 1000 * time.Millisecond // Example: Adjust timing if needed
	// raftConfig.ElectionTimeout = 1000 * time.Millisecond
	// raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.SnapshotInterval = 20 * time.Second // How often to check if snapshot is needed
	raftConfig.SnapshotThreshold = 5              // Number of logs after which a snapshot is taken

	log.Printf("[INFO] Raft Configuration:")
	log.Printf("  Local ID: %s", raftConfig.LocalID)
	log.Printf("  Snapshot Interval: %s", raftConfig.SnapshotInterval)
	log.Printf("  Snapshot Threshold: %d", raftConfig.SnapshotThreshold)


	// Setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		log.Printf("[ERROR] Failed to resolve Raft TCP address %s: %v", config.RaftAddr, err)
		return nil, fmt.Errorf("resolve raft address: %w", err)
	}
	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		log.Printf("[ERROR] Failed to create Raft TCP transport on %s: %v", config.RaftAddr, err)
		return nil, fmt.Errorf("create raft transport: %w", err)
	}
	log.Printf("[INFO] Raft TCP transport created at %s", config.RaftAddr)

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDir, 2, os.Stderr)
	if err != nil {
		log.Printf("[ERROR] Failed to create snapshot store in %s: %v", config.RaftDir, err)
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}
	log.Printf("[INFO] Raft snapshot store created in %s", config.RaftDir)


	// Create log and stable stores
	logStore, stableStore, err := NewBoltStores(filepath.Join(config.RaftDir, "db")) // Subdir for bolt files
	if err != nil {
		log.Printf("[ERROR] Failed to create BoltDB stores: %v", err)
		return nil, fmt.Errorf("create bolt stores: %w", err)
	}

	// Instantiate the Raft system
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Printf("[ERROR] Failed to create Raft instance: %v", err)
		// Attempt to close stores on error
		_ = logStore.Close()
		_ = stableStore.Close()
		_ = transport.Close()
		return nil, fmt.Errorf("create raft instance: %w", err)
	}
	log.Printf("[INFO] Raft instance created successfully for node %s", config.NodeID)


	// --- Cluster Bootstrapping or Joining ---
	if config.Bootstrap {
		// Check if peer info is provided for multi-node bootstrap
		peerIDs := strings.Split(config.PeerIDs, ",")
		peerAddrs := strings.Split(config.PeerAddrs, ",")

		// Default to single-node bootstrap if peers are not specified correctly
		if len(peerIDs) != len(peerAddrs) || config.PeerIDs == "" || config.PeerAddrs == "" {
			log.Printf("[INFO] Bootstrapping cluster as a single node: %s at %s", config.NodeID, config.RaftAddr)
			bootstrapConfig := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(config.NodeID),
						Address: transport.LocalAddr(),
					},
				},
			}
			bootstrapFuture := raftNode.BootstrapCluster(bootstrapConfig)
			if err := bootstrapFuture.Error(); err != nil {
				// Check if the error indicates the cluster is already bootstrapped
				if errors.Is(err, raft.ErrCantBootstrap) {
					log.Printf("[WARN] Bootstrap requested, but cluster appears already bootstrapped or has existing state. Continuing...")
				} else {
					log.Printf("[ERROR] Failed to bootstrap cluster: %v", err)
					_ = raftNode.Shutdown().Error() // Attempt shutdown
					return nil, fmt.Errorf("bootstrap cluster: %w", err)
				}
			} else {
				log.Printf("[INFO] Cluster bootstrapped successfully with node %s", config.NodeID)
			}
		} else {
			// Multi-node bootstrap
			servers := make([]raft.Server, 0, len(peerIDs))
			foundSelf := false
			for i, id := range peerIDs {
				addrStr := strings.TrimSpace(peerAddrs[i])
				idStr := strings.TrimSpace(id)
				if idStr == "" || addrStr == "" {
					log.Printf("[ERROR] Invalid peer configuration during bootstrap: ID='%s', Addr='%s'", idStr, addrStr)
					return nil, fmt.Errorf("invalid peer configuration for bootstrap")
				}
				server := raft.Server{
					ID:      raft.ServerID(idStr),
					Address: raft.ServerAddress(addrStr),
				}
				servers = append(servers, server)
				if idStr == config.NodeID {
					foundSelf = true
                    // Ensure own address matches the transport's address
                    if server.Address != transport.LocalAddr() {
                        log.Printf("[WARN] Bootstrap peer address '%s' for self node '%s' differs from transport local address '%s'. Using transport address.", server.Address, config.NodeID, transport.LocalAddr())
                        servers[len(servers)-1].Address = transport.LocalAddr() // Correct it
                    }
				}
			}

			if !foundSelf {
				log.Printf("[ERROR] Node ID '%s' not found in bootstrap peer IDs: %s", config.NodeID, config.PeerIDs)
				return nil, fmt.Errorf("node ID %s not included in bootstrap peer list", config.NodeID)
			}

            log.Printf("[INFO] Bootstrapping multi-node cluster with configuration: %+v", servers)
			bootstrapConfig := raft.Configuration{Servers: servers}
			bootstrapFuture := raftNode.BootstrapCluster(bootstrapConfig)
			if err := bootstrapFuture.Error(); err != nil {
				if errors.Is(err, raft.ErrCantBootstrap) {
					log.Printf("[WARN] Bootstrap requested, but cluster appears already bootstrapped or has existing state. Continuing...")
				} else {
					log.Printf("[ERROR] Failed to bootstrap multi-node cluster: %v", err)
					_ = raftNode.Shutdown().Error()
					return nil, fmt.Errorf("bootstrap multi-node cluster: %w", err)
				}
			} else {
				log.Printf("[INFO] Multi-node cluster bootstrapped successfully.")
			}
		}
	}
	// --- Joining Logic is handled implicitly by Raft if state exists, ---
	// --- or explicitly via AddVoter if needed (e.g., triggered by an API call or join command). ---
	// --- The -join flag here is primarily for *discovery* - finding an initial node to talk to. ---
	// --- Actual joining (AddVoter) often happens after initial contact. ---
	// --- For simplicity here, we won't automatically call AddVoter on startup based on -join. ---
	// --- A production system might have a discovery service or require manual AddVoter calls via an API/tool. ---
	// --- If JOIN_ADDR is set, it implies this node *should* eventually be part of the cluster. ---
    // --- Let's add the explicit join attempt if JOIN_ADDR is provided ---
    if !config.Bootstrap && config.JoinAddr != "" {
        log.Printf("[INFO] Attempting to join cluster via node at %s", config.JoinAddr)

        // Check if already part of the cluster
        currentConfigFuture := raftNode.GetConfiguration()
        if err := currentConfigFuture.Error(); err == nil {
            isMember := false
            for _, srv := range currentConfigFuture.Configuration().Servers {
                if srv.ID == raft.ServerID(config.NodeID) {
                    isMember = true
                    break
                }
            }
            if isMember {
                 log.Printf("[INFO] Node %s is already part of the Raft cluster configuration. No join needed.", config.NodeID)
                 return raftNode, nil // Already joined
            }
        } else {
             log.Printf("[WARN] Could not get current Raft configuration before join attempt: %v", err)
        }


        addVoterFuture := raftNode.AddVoter(raft.ServerID(config.NodeID), raft.ServerAddress(config.RaftAddr), 0, 0)
        if err := addVoterFuture.Error(); err != nil {
            log.Printf("[ERROR] Failed to add self (%s at %s) as voter to cluster via %s: %v", config.NodeID, config.RaftAddr, config.JoinAddr, err)
			// Note: This might fail if the target node isn't the leader.
			// A more robust join would involve querying the leader and retrying.
            // For this example, we'll log the error and continue; the node might still sync state if previously added.
            // return nil, fmt.Errorf("failed to add voter: %w", err) // Could make this fatal if needed
		} else {
            log.Printf("[INFO] Successfully added node %s as a voter to the cluster.", config.NodeID)
        }
	}


	log.Printf("[INFO] Raft setup complete for node %s.", config.NodeID)
	return raftNode, nil
}

// Helper function to apply a command to the Raft log.
// This should only be called on the LEADER node.
func applyCommand(r *raft.Raft, cmdType CommandType, payload interface{}) error {
	if r.State() != raft.Leader {
		return errors.New("not the leader") // Or return specific error type
	}

	cmdBytes, err := serializeCommand(cmdType, payload)
	if err != nil {
		log.Printf("[ERROR] Failed to serialize command %s: %v", cmdType, err)
		return fmt.Errorf("failed to serialize command: %w", err)
	}

	applyFuture := r.Apply(cmdBytes, raftTimeout)
	if err := applyFuture.Error(); err != nil {
		log.Printf("[ERROR] Failed to apply command %s via Raft: %v", cmdType, err)
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// The response from Apply() is the return value of FSM.Apply()
	response := applyFuture.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		log.Printf("[ERROR] FSM Apply returned an error for command %s: %v", cmdType, responseErr)
		return responseErr // Return the specific error from the FSM logic
	}

	log.Printf("[INFO] Successfully applied command %s", cmdType)
	return nil
}
