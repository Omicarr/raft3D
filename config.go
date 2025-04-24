package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration parameters for a Raft3D node.
type Config struct {
	NodeID     string `envconfig:"NODE_ID" required:"true"`     // Unique ID for this node (e.g., "node1", "node2")
	HTTPAddr   string `envconfig:"HTTP_ADDR" default:":8081"`   // Address for the HTTP API server (default :8081 for node1)
	RaftAddr   string `envconfig:"RAFT_ADDR" required:"true"`   // Address for Raft peer communication (e.g., ":12001")
	RaftDir    string `envconfig:"RAFT_DIR" default:"./data"`   // Base directory for Raft data (logs, snapshots)
	JoinAddr   string `envconfig:"JOIN_ADDR"`                   // Raft address of an existing node to join (optional)
	Bootstrap  bool   `envconfig:"BOOTSTRAP" default:"false"` // Bootstrap the cluster if true (only for the first node or all initial nodes)
	PeerIDs    string `envconfig:"PEER_IDS"`                    // Comma-separated list of Node IDs in the cluster (used if bootstrap=true for multi-node)
	PeerAddrs  string `envconfig:"PEER_ADDRS"`                  // Comma-separated list of Raft addresses (used if bootstrap=true for multi-node)
}

// LoadConfig loads configuration from environment variables and command-line flags.
func LoadConfig() (*Config, error) {
	var cfg Config

	// 1. Environment Variables (using envconfig)
	// Prefix RAFT3D_, e.g., RAFT3D_NODE_ID, RAFT3D_HTTP_ADDR
	err := envconfig.Process("RAFT3D", &cfg)
	if err != nil {
		log.Printf("Error processing environment variables: %v", err)
		// Don't return error yet, allow flags to override or provide values
	}

	// 2. Command-line Flags (override environment variables)
	nodeID := flag.String("node-id", cfg.NodeID, "Node ID (unique in cluster, overrides RAFT3D_NODE_ID)")
	httpAddr := flag.String("http-addr", cfg.HTTPAddr, "HTTP API server address (overrides RAFT3D_HTTP_ADDR)")
	raftAddr := flag.String("raft-addr", cfg.RaftAddr, "Raft communication address (overrides RAFT3D_RAFT_ADDR)")
	raftDir := flag.String("raft-dir", cfg.RaftDir, "Raft data base directory (overrides RAFT3D_RAFT_DIR)")
	joinAddr := flag.String("join-addr", cfg.JoinAddr, "Raft Address of an existing node to join (overrides RAFT3D_JOIN_ADDR)")
	bootstrap := flag.Bool("bootstrap", cfg.Bootstrap, "Bootstrap the cluster (overrides RAFT3D_BOOTSTRAP)")
	peerIDs := flag.String("peer-ids", cfg.PeerIDs, "Comma-separated Node IDs (for multi-node bootstrap, overrides RAFT3D_PEER_IDS)")
	peerAddrs := flag.String("peer-addrs", cfg.PeerAddrs, "Comma-separated Raft addresses (for multi-node bootstrap, overrides RAFT3D_PEER_ADDRS)")

	flag.Parse()

	// Update cfg with flag values if they were provided
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}
	if *raftAddr != "" {
		cfg.RaftAddr = *raftAddr
	}
	if *raftDir != "" {
		cfg.RaftDir = *raftDir
	}
	if *joinAddr != "" {
		cfg.JoinAddr = *joinAddr
	}
	// Use flag value if set, otherwise keep env/default
	cfg.Bootstrap = *bootstrap
	if *peerIDs != "" {
		cfg.PeerIDs = *peerIDs
	}
	if *peerAddrs != "" {
		cfg.PeerAddrs = *peerAddrs
	}

	// 3. Validation
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required (set --node-id or RAFT3D_NODE_ID)")
	}
	if cfg.RaftAddr == "" {
		return nil, fmt.Errorf("raft address is required (set --raft-addr or RAFT3D_RAFT_ADDR)")
	}

	// Check bootstrap conditions
	if cfg.Bootstrap {
		isMultiNodeBootstrap := cfg.PeerIDs != "" && cfg.PeerAddrs != "" && len(strings.Split(cfg.PeerIDs, ",")) == len(strings.Split(cfg.PeerAddrs, ","))
		if !isMultiNodeBootstrap {
			log.Printf("[WARN] Bootstrap flag is set, but peer info (--peer-ids/--peer-addrs) is missing or mismatched. Assuming single-node bootstrap for Node ID: %s", cfg.NodeID)
			// Clear potentially inconsistent peer info for single node bootstrap
			cfg.PeerIDs = ""
			cfg.PeerAddrs = ""
		} else {
			log.Printf("[INFO] Multi-node bootstrap specified with %d peers.", len(strings.Split(cfg.PeerIDs, ",")))
		}
		if cfg.JoinAddr != "" {
			log.Printf("[WARN] Both --bootstrap and --join-addr are set. --join-addr will be ignored during bootstrap.")
			cfg.JoinAddr = "" // Ignore join address if bootstrapping
		}
	} else if cfg.JoinAddr == "" {
		log.Printf("[INFO] Neither --bootstrap nor --join-addr specified. Node %s attempting restart or awaiting manual join.", cfg.NodeID)
	} else {
		log.Printf("[INFO] --join-addr specified (%s). Node %s will attempt explicit join.", cfg.JoinAddr, cfg.NodeID)
	}

	// Ensure node-specific data directory exists. This is CRUCIAL for multi-node testing.
	nodeDataDir := filepath.Join(cfg.RaftDir, cfg.NodeID)
	if err := os.MkdirAll(nodeDataDir, 0750); err != nil { // Use 0750 permissions
		return nil, fmt.Errorf("failed to create node-specific raft directory '%s': %w", nodeDataDir, err)
	}
	cfg.RaftDir = nodeDataDir // IMPORTANT: Update config to use the node-specific path

	// Log final configuration
	log.Printf("[INFO] ---- Configuration ----")
	log.Printf("  Node ID:     %s", cfg.NodeID)
	log.Printf("  HTTP Addr:   %s", cfg.HTTPAddr)
	log.Printf("  Raft Addr:   %s", cfg.RaftAddr)
	log.Printf("  Raft Dir:    %s", cfg.RaftDir) // This is now node-specific
	log.Printf("  Bootstrap:   %t", cfg.Bootstrap)
	log.Printf("  Join Addr:   %s", cfg.JoinAddr)
	log.Printf("  Peer IDs:    '%s'", cfg.PeerIDs)
	log.Printf("  Peer Addrs:  '%s'", cfg.PeerAddrs)
	log.Printf("-------------------------")

	return &cfg, nil
}
