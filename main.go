package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
)

func main() {
	// Configure logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[INFO] ===================================")
	log.Println("[INFO]    Starting Raft3D Node")
	log.Println("[INFO] ===================================")

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

	// Create the HTTP server
	httpServer := NewHttpServer(raftNode, fsm, cfg.HTTPAddr)

	// Start the HTTP server in a separate goroutine
	httpServerErrChan := make(chan error, 1)
	go func() {
		log.Printf("[INFO] Attempting to start HTTP server goroutine (listening on %s)...", cfg.HTTPAddr)
		// Start blocks unless an error occurs or Shutdown is called
		err := httpServer.Start()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[ERROR] HTTP server failed: %v", err)
			httpServerErrChan <- err // Send error back to main goroutine
		} else {
			log.Println("[INFO] HTTP server goroutine finished.")
			httpServerErrChan <- nil // Signal clean exit
		}
		close(httpServerErrChan)
	}()

	// Optional: Wait briefly for cluster stabilization (leader election/discovery)
	log.Println("[INFO] Waiting briefly for cluster stabilization...")
	waitForLeaderOrPeer(raftNode, 15*time.Second) // Wait up to 15 seconds

	// Wait for termination signal (Ctrl+C or SIGTERM) or HTTP server error
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-signalChan:
		log.Printf("[INFO] Received OS signal %s. Initiating graceful shutdown...", sig)
	case err := <-httpServerErrChan:
		if err != nil {
			log.Printf("[ERROR] HTTP server exited unexpectedly: %v. Initiating shutdown...", err)
		} else {
			log.Printf("[INFO] HTTP server shut down cleanly before signal received. Initiating shutdown...")
		}
	}

	// --- Graceful Shutdown Sequence ---
	log.Println("[INFO] Starting graceful shutdown...")

	// 1. Shutdown HTTP Server first to stop accepting new requests
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Timeout for shutdown
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] Error during HTTP server graceful shutdown: %v", err)
	} else {
		log.Printf("[INFO] HTTP server shut down gracefully.")
	}

	// 2. Shutdown Raft Node
	log.Printf("[INFO] Shutting down Raft node...")
	shutdownFuture := raftNode.Shutdown() // Initiate Raft shutdown
	if err := shutdownFuture.Error(); err != nil {
		log.Printf("[WARN] Error during Raft node graceful shutdown: %v", err)
	} else {
		log.Printf("[INFO] Raft node shut down gracefully.")
	}

	// Note: BoltDB stores are closed implicitly by raftNode.Shutdown() successful completion.

	log.Println("[INFO] ===================================")
	log.Printf("[INFO]    Node %s Exiting.", cfg.NodeID)
	log.Println("[INFO] ===================================")
}

// waitForLeaderOrPeer waits until the node becomes leader, discovers a leader,
// or the timeout occurs. It's mainly for observing startup behavior.
func waitForLeaderOrPeer(r *raft.Raft, timeout time.Duration) {
	ticker := time.NewTicker(2 * time.Second) // Check every 2 seconds
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	log.Printf("[INFO] Monitoring Raft state for up to %s...", timeout)
	for {
		select {
		case <-ticker.C:
			state := r.State()
			leader := r.Leader()
			stats := r.Stats() // Get stats for term etc.

			if state == raft.Leader {
				log.Printf("[INFO] Became leader (Term: %v). Stabilization likely complete.", stats["term"])
				return // Exit loop once leader
			}
			if leader != "" {
				log.Printf("[INFO] Current state: %s, Leader: %s (Term: %v)", state, leader, stats["term"])
				// Could return here if just knowing *a* leader is enough
			} else {
				log.Printf("[INFO] Current state: %s, Leader unknown (Term: %v). Still searching...", state, stats["term"])
			}
		case <-timer.C:
			stats := r.Stats()
			log.Printf("[WARN] Timed out waiting for definite leader after %s. Current state: %s, Leader: %s, Term: %v", timeout, r.State(), r.Leader(), stats["term"])
			return // Exit loop after timeout
		}
	}
}
