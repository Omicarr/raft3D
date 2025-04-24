package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
)

// httpServer wraps the Raft node and FSM for handling HTTP requests.
type httpServer struct {
	raftNode *raft.Raft
	fsm      *Raft3DFSM
	httpAddr string // Own HTTP address for redirection checks
	server   *http.Server
}

// NewHttpServer creates a new HTTP server instance.
func NewHttpServer(node *raft.Raft, fsm *Raft3DFSM, httpAddr string) *httpServer {
	return &httpServer{
		raftNode: node,
		fsm:      fsm,
		httpAddr: httpAddr,
	}
}

// Start configures routes and starts the HTTP server.
func (s *httpServer) Start() error {
	router := mux.NewRouter()

	// Middleware
	router.Use(loggingMiddleware)
	router.Use(s.leaderCheckMiddleware) // Handles write request redirection

	// API Endpoints
	api := router.PathPrefix("/api/v1").Subrouter()

	// Printers
	api.HandleFunc("/printers", s.handleGetPrinters).Methods("GET")
	api.HandleFunc("/printers", s.handleCreatePrinter).Methods("POST")

	// Filaments
	api.HandleFunc("/filaments", s.handleGetFilaments).Methods("GET")
	api.HandleFunc("/filaments", s.handleCreateFilament).Methods("POST")

	// Print Jobs
	// Use regex for UUID-like IDs: [a-fA-F0-9-]+
	jobIDRegex := "{job_id:[a-fA-F0-9-]+}"
	api.HandleFunc("/print_jobs", s.handleGetPrintJobs).Methods("GET") // Allow filtering by ?status=...
	api.HandleFunc("/print_jobs", s.handleCreatePrintJob).Methods("POST") // Status set by FSM
	api.HandleFunc("/print_jobs/"+jobIDRegex, s.handleGetPrintJob).Methods("GET")
	api.HandleFunc("/print_jobs/"+jobIDRegex+"/status", s.handleUpdatePrintJobStatus).Methods("PUT") // Use PUT for state change

	// Raft status endpoint (read-only, doesn't need leader check - attached directly to router)
	router.HandleFunc("/status", s.handleRaftStatus).Methods("GET")

	s.server = &http.Server{
		Addr:    s.httpAddr,
		Handler: router,
	}

	log.Printf("[INFO] Starting HTTP server, listening on %s", s.httpAddr)
	// ListenAndServe blocks until the server is shut down or an error occurs
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server ListenAndServe error: %w", err)
	}
	// If we get here, it's likely due to a graceful shutdown
	log.Printf("[INFO] HTTP server on %s stopped listening.", s.httpAddr)
	return nil // Returns nil on graceful shutdown (ErrServerClosed)
}

// Shutdown gracefully shuts down the HTTP server.
func (s *httpServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		log.Printf("[WARN] HTTP server Shutdown called but server is nil.")
		return nil // Server wasn't started or already shut down
	}
	log.Printf("[INFO] Attempting graceful shutdown of HTTP server on %s...", s.httpAddr)
	// Shutdown initiates graceful shutdown, waits for connections to finish
	err := s.server.Shutdown(ctx)
	if err != nil {
		log.Printf("[ERROR] HTTP server graceful shutdown failed: %v", err)
		return err
	}
	log.Printf("[INFO] HTTP server on %s shut down complete.", s.httpAddr)
	return nil
}

// --- Middleware ---

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Log request start
		log.Printf("[HTTP] --> %s %s from %s", r.Method, r.RequestURI, r.RemoteAddr)

		// Serve the request
		next.ServeHTTP(w, r)

		// Log request completion
		// TODO: Capture status code for more informative logging
		log.Printf("[HTTP] <-- %s %s completed in %v", r.Method, r.RequestURI, time.Since(start))
	})
}

// leaderCheckMiddleware checks if the current node is the leader for write operations.
// If not, it redirects the client to the leader's presumed HTTP address.
func (s *httpServer) leaderCheckMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow read-only methods on any node (followers might serve slightly stale data)
		if r.Method == "GET" || r.Method == "HEAD" || r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		// Check leadership status for write methods (POST, PUT, DELETE, PATCH)
		if s.raftNode.State() == raft.Leader {
			log.Printf("[DEBUG] Node is leader. Processing %s %s locally.", r.Method, r.RequestURI)
			next.ServeHTTP(w, r) // Process the request locally
			return
		}

		// --- Not the leader ---
		leaderRaftAddr := s.raftNode.Leader() // Get leader's Raft address (host:port)
		if leaderRaftAddr == "" {
			log.Printf("[WARN] Not leader, and leader address is currently unknown. Cannot process write %s %s", r.Method, r.RequestURI)
			respondError(w, http.StatusServiceUnavailable, "Raft leader currently unavailable")
			return
		}

		// --- Determine Leader's HTTP Address ---
		// This uses a convention. Replace with a more robust method if needed.
		leaderHttpAddr := s.deriveHttpAddrFromRaftAddr(string(leaderRaftAddr))
		if leaderHttpAddr == "" {
			log.Printf("[ERROR] Not leader (%s), failed to determine HTTP address for leader Raft address %s", s.raftNode.State(), leaderRaftAddr)
			respondError(w, http.StatusServiceUnavailable, "Cannot determine leader HTTP address")
			return
		}

		// --- Avoid Redirect Loop ---
		// Check if the derived leader HTTP address is the same as this server's address
		if leaderHttpAddr == s.httpAddr {
			// This might happen briefly during leader transition or if derivation logic is flawed
			log.Printf("[ERROR] Detected potential redirect loop! Leader Raft address %s maps to own HTTP address %s, but node state is %s. Aborting redirect.", leaderRaftAddr, s.httpAddr, s.raftNode.State())
			respondError(w, http.StatusInternalServerError, "Internal server error (redirect loop detected)")
			return
		}

		// --- Construct Redirect URL ---
		// Assume http for simplicity. Production might need scheme detection (http vs https).
		scheme := "http"
		redirectURL := url.URL{
			Scheme:   scheme,
			Host:     leaderHttpAddr, // The calculated HTTP host:port of the leader
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery, // Preserve original query parameters
		}

		log.Printf("[INFO] Not leader (%s). Redirecting write request %s %s to leader at %s (Raft: %s)",
			s.raftNode.State(), r.Method, r.RequestURI, leaderHttpAddr, leaderRaftAddr)

		// --- Perform Redirect ---
		// Use 307 Temporary Redirect: ensures the client resends the same method and body
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
	})
}

// deriveHttpAddrFromRaftAddr uses a convention to guess the HTTP address from the Raft address.
// **WARNING:** This is fragile. A better approach involves storing HTTP addresses
// in the Raft state, using configuration, or a service discovery mechanism.
// Convention Used Here: HTTP Port = Raft Port - 3920
// Example: Raft :12001 -> HTTP :8081, Raft :12002 -> HTTP :8082
func (s *httpServer) deriveHttpAddrFromRaftAddr(raftAddr string) string {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		log.Printf("[WARN] Cannot derive HTTP addr: Failed parsing Raft address '%s': %v", raftAddr, err)
		return ""
	}

	raftPort, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("[WARN] Cannot derive HTTP addr: Failed parsing Raft port '%s': %v", portStr, err)
		return ""
	}

	// Apply the convention
	httpPort := raftPort - 3920 // Adjust this offset based on your port choices
	if httpPort <= 0 || httpPort > 65535 {
		log.Printf("[WARN] Cannot derive HTTP addr: Calculated invalid HTTP port %d from Raft port %d", httpPort, raftPort)
		return ""
	}

	// If the original host was empty (e.g., ":12001"), use a sensible default or localhost.
	// For clients to connect, specifying the host might be necessary.
	// If running locally, '127.0.0.1' is safer than empty host for redirection.
	// In production, you'd use the node's actual reachable IP or hostname.
	finalHost := host
	if finalHost == "" {
		finalHost = "127.0.0.1" // Assume localhost if Raft address host is empty
	}

	return net.JoinHostPort(finalHost, strconv.Itoa(httpPort))
}

// --- Handlers ---

// GET /status - Provides current Raft status of this node.
func (s *httpServer) handleRaftStatus(w http.ResponseWriter, r *http.Request) {
	// Get Raft stats (includes leader, term, commit index etc.)
	raftStats := s.raftNode.Stats()
	leaderAddr := s.raftNode.Leader()

	// Include FSM state counts for basic overview
	s.fsm.mu.RLock() // Use read lock for FSM access
	numPrinters := len(s.fsm.printers)
	numFilaments := len(s.fsm.filaments)
	numJobs := len(s.fsm.printJobs)
	s.fsm.mu.RUnlock()

	status := map[string]interface{}{
		"node_id":       raftStats["id"],
		"state":         s.raftNode.State().String(),
		"term":          raftStats["term"],
		"leader_raft":   leaderAddr,
		"leader_http":   s.deriveHttpAddrFromRaftAddr(string(leaderAddr)), // Attempt to derive HTTP addr
		"commit_index":  raftStats["commit_index"],
		"applied_index": raftStats["last_applied"],
		"snapshot_index": raftStats["last_snapshot_index"],
		"snapshot_term": raftStats["last_snapshot_term"],
		"fsm_state": map[string]int{
			"printers":  numPrinters,
			"filaments": numFilaments,
			"printJobs": numJobs,
		},
		// "peers":         raftStats["peers"], // This can be verbose
	}
	respondJSON(w, http.StatusOK, status)
}

// GET /api/v1/printers
func (s *httpServer) handleGetPrinters(w http.ResponseWriter, r *http.Request) {
	// Followers can serve reads, but data might be slightly stale vs leader.
	printers := s.fsm.GetPrinters()
	respondJSON(w, http.StatusOK, printers)
}

// POST /api/v1/printers
func (s *httpServer) handleCreatePrinter(w http.ResponseWriter, r *http.Request) {
	var reqPrinter Printer
	if err := json.NewDecoder(r.Body).Decode(&reqPrinter); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: %v", err)
		return
	}
	defer r.Body.Close()

	// Assign unique ID server-side
	reqPrinter.ID = uuid.NewString() // Use google/uuid

	// Basic API-level validation
	if strings.TrimSpace(reqPrinter.Company) == "" || strings.TrimSpace(reqPrinter.Model) == "" {
		respondError(w, http.StatusBadRequest, "Missing required fields: company, model")
		return
	}

	payload := CreatePrinterPayload{Printer: reqPrinter}

	// Apply the command via Raft (leaderCheckMiddleware ensures this runs on leader)
	err := applyCommand(s.raftNode, CmdCreatePrinter, payload)
	if err != nil {
		// Check specific raft errors first
		if errors.Is(err, raft.ErrNotLeader) {
			respondError(w, http.StatusServiceUnavailable, "Failed command: Not the leader (applyCommand check failed)")
		} else {
			// Could be FSM error or other Raft error
			respondError(w, http.StatusInternalServerError, "Failed to create printer via Raft: %v", err)
		}
		return
	}

	// Return the created printer (with ID)
	respondJSON(w, http.StatusCreated, reqPrinter)
}

// GET /api/v1/filaments
func (s *httpServer) handleGetFilaments(w http.ResponseWriter, r *http.Request) {
	filaments := s.fsm.GetFilaments()
	respondJSON(w, http.StatusOK, filaments)
}

// POST /api/v1/filaments
func (s *httpServer) handleCreateFilament(w http.ResponseWriter, r *http.Request) {
	var reqFilament Filament
	if err := json.NewDecoder(r.Body).Decode(&reqFilament); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: %v", err)
		return
	}
	defer r.Body.Close()

	// Assign unique ID & ensure initial remaining weight makes sense
	reqFilament.ID = uuid.NewString()

	// Sensible default for remaining weight if not provided but total is
	if reqFilament.TotalWeightInGrams > 0 && reqFilament.RemainingWeightInGrams <= 0 {
		reqFilament.RemainingWeightInGrams = reqFilament.TotalWeightInGrams
	}
	// Cap remaining at total
	if reqFilament.RemainingWeightInGrams > reqFilament.TotalWeightInGrams {
		reqFilament.RemainingWeightInGrams = reqFilament.TotalWeightInGrams
	}

	// Basic API validation
	validTypes := map[string]bool{"PLA": true, "PETG": true, "ABS": true, "TPU": true} // Example types
	if !validTypes[reqFilament.Type] {
		respondError(w, http.StatusBadRequest, "Invalid filament type. Options: PLA, PETG, ABS, TPU")
		return
	}
	if strings.TrimSpace(reqFilament.Color) == "" {
		respondError(w, http.StatusBadRequest, "Missing required field: color")
		return
	}
	if reqFilament.TotalWeightInGrams < 0 {
		respondError(w, http.StatusBadRequest, "total_weight_in_grams cannot be negative")
		return
	}
	if reqFilament.RemainingWeightInGrams < 0 || reqFilament.RemainingWeightInGrams > reqFilament.TotalWeightInGrams {
		respondError(w, http.StatusBadRequest, "remaining_weight_in_grams must be between 0 and total_weight_in_grams")
		return
	}

	payload := CreateFilamentPayload{Filament: reqFilament}

	err := applyCommand(s.raftNode, CmdCreateFilament, payload)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			respondError(w, http.StatusServiceUnavailable, "Failed command: Not the leader")
		} else {
			respondError(w, http.StatusInternalServerError, "Failed to create filament via Raft: %v", err)
		}
		return
	}

	respondJSON(w, http.StatusCreated, reqFilament)
}

// GET /api/v1/print_jobs?status=<Status>
func (s *httpServer) handleGetPrintJobs(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	allJobs := s.fsm.GetPrintJobs() // Reads local FSM state

	if statusFilter != "" {
		filteredJobs := make([]PrintJob, 0)
		targetStatus := PrintJobStatus(statusFilter) // Convert query param to type
		for _, job := range allJobs {
			if job.Status == targetStatus {
				filteredJobs = append(filteredJobs, job)
			}
		}
		respondJSON(w, http.StatusOK, filteredJobs)
	} else {
		respondJSON(w, http.StatusOK, allJobs)
	}
}

// GET /api/v1/print_jobs/{job_id}
func (s *httpServer) handleGetPrintJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["job_id"]

	job, ok := s.fsm.GetPrintJob(jobID) // Reads local FSM state
	if !ok {
		respondError(w, http.StatusNotFound, "Print job with ID '%s' not found", jobID)
		return
	}
	respondJSON(w, http.StatusOK, job)
}

// POST /api/v1/print_jobs
func (s *httpServer) handleCreatePrintJob(w http.ResponseWriter, r *http.Request) {
	var reqJob PrintJob
	if err := json.NewDecoder(r.Body).Decode(&reqJob); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: %v", err)
		return
	}
	defer r.Body.Close()

	// Assign unique ID. Status will be set to Queued by FSM Apply.
	reqJob.ID = uuid.NewString()
	reqJob.Status = "" // Ensure client cannot set initial status

	// Basic API validation (more complex checks happen in FSM Apply)
	if reqJob.PrinterID == "" || reqJob.FilamentID == "" || strings.TrimSpace(reqJob.Filepath) == "" {
		respondError(w, http.StatusBadRequest, "Missing required fields: printer_id, filament_id, filepath")
		return
	}
	if reqJob.PrintWeightInGrams <= 0 {
		respondError(w, http.StatusBadRequest, "print_weight_in_grams must be positive")
		return
	}

	payload := CreatePrintJobPayload{PrintJob: reqJob}

	// Apply command via Raft
	err := applyCommand(s.raftNode, CmdCreatePrintJob, payload)
	if err != nil {
		// Check for specific FSM validation errors passed back through applyCommand
		errMsg := err.Error()
		if strings.Contains(errMsg, "insufficient filament weight") ||
			strings.Contains(errMsg, "printer does not exist") ||
			strings.Contains(errMsg, "filament does not exist") {
			respondError(w, http.StatusBadRequest, "Cannot create job: %v", err) // Return 400 for validation errors
		} else if errors.Is(err, raft.ErrNotLeader) {
			respondError(w, http.StatusServiceUnavailable, "Failed command: Not the leader")
		} else {
			// Other Raft or internal errors
			respondError(w, http.StatusInternalServerError, "Failed to create print job via Raft: %v", err)
		}
		return
	}

	// Read back the job from FSM to get the final state (with StatusQueued)
	// Needs slight delay/retry or Barrier read for strong consistency here?
	// For simplicity, try direct read, but it might occasionally fail if Apply hasn't finished on FSM yet.
	time.Sleep(50 * time.Millisecond) // Small delay hack - NOT ideal for production
	createdJob, ok := s.fsm.GetPrintJob(reqJob.ID)
	if !ok {
		log.Printf("[ERROR] Job %s not found in FSM immediately after successful Apply", reqJob.ID)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve created job state after apply")
		return
	}

	respondJSON(w, http.StatusCreated, createdJob)
}

// PUT /api/v1/print_jobs/{job_id}/status
func (s *httpServer) handleUpdatePrintJobStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["job_id"]

	var updateReq struct {
		Status PrintJobStatus `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&updateReq); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: %v", err)
		return
	}
	defer r.Body.Close()

	// Validate the status value from the request body
	var nextStatus PrintJobStatus
	switch updateReq.Status {
	case StatusRunning, StatusDone, StatusCancelled:
		nextStatus = updateReq.Status
	// case StatusQueued: // Optionally disallow setting back to Queued
	// 	respondError(w, http.StatusBadRequest, "Cannot manually set status back to Queued")
	// 	return
	default:
		respondError(w, http.StatusBadRequest, "Invalid status value in request body. Options: Running, Done, Cancelled")
		return
	}

	payload := UpdatePrintJobStatusPayload{
		JobID:  jobID,
		Status: nextStatus,
	}

	// Apply command via Raft
	err := applyCommand(s.raftNode, CmdUpdatePrintJobStatus, payload)
	if err != nil {
		// Check for specific FSM validation errors
		errMsg := err.Error()
		if strings.Contains(errMsg, "invalid status transition") ||
			strings.Contains(errMsg, "job does not exist") ||
			strings.Contains(errMsg, "cannot transition from a terminal state") ||
			strings.Contains(errMsg, "internal error: filament not found") { // Catch filament reduction error
			respondError(w, http.StatusBadRequest, "Cannot update job status: %v", err)
		} else if errors.Is(err, raft.ErrNotLeader) {
			respondError(w, http.StatusServiceUnavailable, "Failed command: Not the leader")
		} else {
			respondError(w, http.StatusInternalServerError, "Failed to update job status via Raft: %v", err)
		}
		return
	}

	// Read back the updated job from FSM
	time.Sleep(50 * time.Millisecond) // Small delay hack
	updatedJob, ok := s.fsm.GetPrintJob(jobID)
	if !ok {
		log.Printf("[ERROR] Job %s not found in FSM after successful status update Apply", jobID)
		respondError(w, http.StatusInternalServerError, "Failed to retrieve updated job state after apply")
		return
	}

	respondJSON(w, http.StatusOK, updatedJob) // Return updated job
}

// --- Response Helpers ---

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	response, err := json.MarshalIndent(payload, "", "  ") // Use Indent for readability
	if err != nil {
		log.Printf("[ERROR] Failed to marshal JSON response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error": "Failed to marshal JSON response"}`)) // Raw JSON
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(response) // Best effort write
}

// respondError logs the error and sends a JSON error response.
func respondError(w http.ResponseWriter, code int, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	// Log the error server-side for debugging
	log.Printf("[HTTP ERROR %d] %s", code, message)
	// Send JSON error response to client
	respondJSON(w, code, map[string]string{"error": message})
}
