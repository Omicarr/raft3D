package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"net/http"

	"github.com/hashicorp/raft"
)

func main() {
	// Configure logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[INFO] Starting Raft3D Node...")

	// Load configuration
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	// Create the FSM
	fsm := NewRaft3DFSM()
	log.Println("[INFO] Finite State Machine created.")

	// Setup Raft
	raftNode, err := SetupRaft(cfg, fsm)
	if err != nil {
		log.Fatalf("[FATAL] Failed to setup Raft: %v", err)
	}
	log.Println("[INFO] Raft node setup complete.")

	// Start the HTTP server
	httpServer := NewHttpServer(raftNode, fsm, cfg.HTTPAddr)
	go func() {
		log.Printf("[INFO] Attempting to start HTTP server on %s...", cfg.HTTPAddr)
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] HTTP server failed: %v", err)
		}
		log.Println("[INFO] HTTP server shut down.")
	}()

	// Wait for leader election (optional, but helpful for demos)
	log.Println("[INFO] Waiting for node to potentially become leader...")
	waitForLeader(raftNode, 15*time.Second) // Wait up to 15 seconds

	// Wait for termination signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Printf("[INFO] Received signal %s. Shutting down node %s...", sig, cfg.NodeID)

	// Attempt graceful shutdown
	shutdownFuture := raftNode.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		log.Printf("[WARN] Error during Raft shutdown for node %s: %v", cfg.NodeID, err)
	} else {
		log.Printf("[INFO] Raft node %s shut down gracefully.", cfg.NodeID)
	}

	// Ideally, you would also gracefully shut down the HTTP server here
	// (e.g., using server.Shutdown(context.Background()))

	log.Printf("[INFO] Node %s exiting.", cfg.NodeID)
}

// waitForLeader waits until the node becomes leader or timeout occurs
func waitForLeader(r *raft.Raft, timeout time.Duration) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if r.State() == raft.Leader {
				log.Printf("[INFO] Node %s became leader.", r.String())
				return
			}
            leaderAddr := r.Leader()
            if leaderAddr != "" {
                log.Printf("[INFO] Node %s is %s. Current leader Raft address: %s", r.String(), r.State(), leaderAddr)
            } else {
                 log.Printf("[INFO] Node %s is %s. Leader unknown.", r.String(), r.State())
            }
		case <-timer.C:
			log.Printf("[WARN] Timed out waiting for leadership after %s. Current state: %s", timeout, r.State())
			return
		}
	}
}
