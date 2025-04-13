package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	NodeID     string `envconfig:"NODE_ID" required:"true"`     // Unique ID for this node (e.g., "node1", "node2")
	HTTPAddr   string `envconfig:"HTTP_ADDR" default:":8080"`   // Address for the HTTP API server
	RaftAddr   string `envconfig:"RAFT_ADDR" required:"true"`   // Address for Raft peer communication (e.g., ":12000")
	RaftDir    string `envconfig:"RAFT_DIR" default:"./data"`   // Directory for Raft data (logs, snapshots)
	JoinAddr   string `envconfig:"JOIN_ADDR"`                   // Address of an existing node to join (optional, for discovery)
	Bootstrap  bool   `envconfig:"BOOTSTRAP" default:"false"` // Bootstrap the cluster if true (only for the first node)
	PeerIDs    string `envconfig:"PEER_IDS"`                    // Comma-separated list of Node IDs in the cluster (used if bootstrap=true)
	PeerAddrs  string `envconfig:"PEER_ADDRS"`                  // Comma-separated list of Raft addresses (used if bootstrap=true)
}

func LoadConfig() (*Config, error) {
	var cfg Config

	// 1. Environment Variables (using envconfig)
	err := envconfig.Process("RAFT3D", &cfg) // Prefix RAFT3D_, e.g., RAFT3D_NODE_ID
	if err != nil {
		log.Printf("Error processing environment variables: %v", err)
		// Don't return error yet, allow flags to override or provide values
	}

	// 2. Command-line Flags (override environment variables)
	// Define flags corresponding to Config fields
	nodeID := flag.String("node-id", cfg.NodeID, "Node ID (unique in cluster)")
	httpAddr := flag.String("http-addr", cfg.HTTPAddr, "HTTP API server address (e.g., ':8080')")
	raftAddr := flag.String("raft-addr", cfg.RaftAddr, "Raft communication address (e.g., ':12000')")
	raftDir := flag.String("raft-dir", cfg.RaftDir, "Raft data directory")
	joinAddr := flag.String("join-addr", cfg.JoinAddr, "Address of an existing node to join (optional)")
	bootstrap := flag.Bool("bootstrap", cfg.Bootstrap, "Bootstrap the cluster (only for the first node)")
	peerIDs := flag.String("peer-ids", cfg.PeerIDs, "Comma-separated Node IDs (for bootstrap)")
	peerAddrs := flag.String("peer-addrs", cfg.PeerAddrs, "Comma-separated Raft addresses (for bootstrap)")

	flag.Parse()

	// Update cfg with flag values
	cfg.NodeID = *nodeID
	cfg.HTTPAddr = *httpAddr
	cfg.RaftAddr = *raftAddr
	cfg.RaftDir = *raftDir
	cfg.JoinAddr = *joinAddr
	cfg.Bootstrap = *bootstrap
	cfg.PeerIDs = *peerIDs
	cfg.PeerAddrs = *peerAddrs


	// 3. Validation
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required (set --node-id or RAFT3D_NODE_ID)")
	}
	if cfg.RaftAddr == "" {
		return nil, fmt.Errorf("raft address is required (set --raft-addr or RAFT3D_RAFT_ADDR)")
	}
	if cfg.Bootstrap {
		if cfg.PeerIDs == "" || cfg.PeerAddrs == "" {
			log.Printf("[WARN] Bootstrap flag is set, but --peer-ids/RAFT3D_PEER_IDS or --peer-addrs/RAFT3D_PEER_ADDRS is missing. Assuming single-node bootstrap.")
			// Allow single node bootstrap without peers specified
		}
        if cfg.JoinAddr != "" {
            log.Printf("[WARN] Both --bootstrap and --join-addr are set. --join-addr will be ignored.")
            cfg.JoinAddr = ""
        }
	} else if cfg.JoinAddr == "" {
		// If not bootstrapping and not joining, check if Raft state already exists
		// If not, it likely needs either bootstrap or join.
		// However, raft library itself handles restart logic, so don't error here yet.
		log.Printf("[INFO] Neither --bootstrap nor --join-addr specified. Attempting to restart or join existing cluster state if possible.")
	}

	// Ensure data directory exists for the specific node
	cfg.RaftDir = filepath.Join(cfg.RaftDir, cfg.NodeID)
	if err := os.MkdirAll(cfg.RaftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create node-specific raft directory %s: %w", cfg.RaftDir, err)
	}


	log.Printf("[INFO] Configuration loaded:")
	log.Printf("  Node ID:     %s", cfg.NodeID)
	log.Printf("  HTTP Addr:   %s", cfg.HTTPAddr)
	log.Printf("  Raft Addr:   %s", cfg.RaftAddr)
	log.Printf("  Raft Dir:    %s", cfg.RaftDir)
	log.Printf("  Bootstrap:   %t", cfg.Bootstrap)
	log.Printf("  Join Addr:   %s", cfg.JoinAddr)
    log.Printf("  Peer IDs:    %s", cfg.PeerIDs)
    log.Printf("  Peer Addrs:  %s", cfg.PeerAddrs)


	return &cfg, nil
}
