package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
)

type httpServer struct {
	raftNode *raft.Raft
	fsm      *Raft3DFSM
	httpAddr string // Own HTTP address for redirection checks
}

func NewHttpServer(node *raft.Raft, fsm *Raft3DFSM, httpAddr string) *httpServer {
	return &httpServer{
		raftNode: node,
		fsm:      fsm,
		httpAddr: httpAddr,
	}
}

func (s *httpServer) Start() error {
	router := mux.NewRouter()

	// Middleware to log requests
	router.Use(loggingMiddleware)
	// Middleware to check leader status and redirect writes if necessary
	router.Use(s.leaderCheckMiddleware)

	// API Endpoints
	api := router.PathPrefix("/api/v1").Subrouter()

	// Printers
	api.HandleFunc("/printers", s.handleGetPrinters).Methods("GET")
	api.HandleFunc("/printers", s.handleCreatePrinter).Methods("POST")

	// Filaments
	api.HandleFunc("/filaments", s.handleGetFilaments).Methods("GET")
	api.HandleFunc("/filaments", s.handleCreateFilament).Methods("POST")

	// Print Jobs
	api.HandleFunc("/print_jobs", s.handleGetPrintJobs).Methods("GET")
	api.HandleFunc("/print_jobs", s.handleCreatePrintJob).Methods("POST")
	api.HandleFunc("/print_jobs/{job_id}/status", s.handleUpdatePrintJobStatus).Methods("POST") // Using POST as per spec

	log.Printf("[INFO] Starting HTTP server on %s", s.httpAddr)
	return http.ListenAndServe(s.httpAddr, router)
}

// --- Middleware ---

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("[HTTP] Started %s %s from %s", r.Method, r.RequestURI, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("[HTTP] Completed %s %s in %v", r.Method, r.RequestURI, time.Since(start))
	})
}

func (s *httpServer) leaderCheckMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check only for write methods (POST, PUT, DELETE, PATCH)
		// GET requests can often be served by followers (though potentially stale)
		// The spec uses POST for update, so include it.
		if r.Method != "POST" && r.Method != "PUT" && r.Method != "DELETE" && r.Method != "PATCH" {
			next.ServeHTTP(w, r)
			return
		}

		// Allow bootstrap/join related internal communication even if not leader? Maybe not needed here.

		if s.raftNode.State() == raft.Leader {
			log.Printf("[DEBUG] Node is leader. Processing %s %s locally.", r.Method, r.RequestURI)
			next.ServeHTTP(w, r)
			return
		}

		// Not the leader, find the leader and redirect
		leaderAddr := s.raftNode.Leader() // This is the Raft address
		if leaderAddr == "" {
			log.Printf("[ERROR] Not leader, and leader address is unknown. Cannot process %s %s", r.Method, r.RequestURI)
			http.Error(w, "Leader unavailable", http.StatusServiceUnavailable)
			return
		}

		// --- Attempt to find Leader's HTTP Address ---
		// This is the tricky part. Raft knows the leader's Raft address, but not necessarily its HTTP address.
		// We need a way to map Raft address/ID to HTTP address.
		// Option 1: Store this mapping *in the Raft state itself* (e.g., in a separate map in the FSM).
		// Option 2: Use configuration/discovery service.
		// Option 3: Make an assumption (e.g., HTTP port = Raft port - 1000). (Simplest but brittle)

		// Let's try Option 3 for simplicity in this example, assuming a convention:
		leaderHttpAddr := s.deriveHttpAddrFromRaftAddr(string(leaderAddr))
		if leaderHttpAddr == "" {
			log.Printf("[ERROR] Not leader (%s), and could not determine HTTP address for leader Raft address %s", s.raftNode.State(), leaderAddr)
			http.Error(w, "Could not determine leader HTTP address", http.StatusServiceUnavailable)
			return
		}

		// Avoid redirecting to self if something is misconfigured
		if leaderHttpAddr == s.httpAddr {
			log.Printf("[ERROR] Detected redirect loop. Leader Raft address %s maps to own HTTP address %s, but node state is %s", leaderAddr, s.httpAddr, s.raftNode.State())
			http.Error(w, "Internal server error (redirect loop detected)", http.StatusInternalServerError)
			return
		}


		// Construct the redirect URL
		// Ensure scheme (http/https) is correct - assume http for now
		redirectURL := url.URL{
			Scheme: "http", // Adjust if using HTTPS
			Host:   leaderHttpAddr,
			Path:   r.URL.Path,
			RawQuery: r.URL.RawQuery,
		}

		log.Printf("[INFO] Not leader (%s). Redirecting %s %s to leader at %s (%s)",
			s.raftNode.State(), r.Method, r.RequestURI, leaderHttpAddr, leaderAddr)

		// Use 307 Temporary Redirect to preserve the method and body
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
	})
}

// deriveHttpAddrFromRaftAddr - Simple example assuming HTTP port is Raft port - 4000
// Replace this with a more robust mechanism in production!
func (s *httpServer) deriveHttpAddrFromRaftAddr(raftAddr string) string {
    parts := strings.Split(raftAddr, ":")
    if len(parts) != 2 {
        log.Printf("[WARN] Could not parse Raft address '%s' into host:port", raftAddr)
        return "" // Cannot determine
    }
    host := parts[0]
    if host == "" { // Handle ":12000" case
        host = "localhost" // Or get local IP
    }
    raftPortStr := parts[1]
    var raftPort int
    _, err := fmt.Sscan(raftPortStr, &raftPort)
    if err != nil {
         log.Printf("[WARN] Could not parse Raft port '%s': %v", raftPortStr, err)
         return ""
    }

    // Convention: HTTP port = Raft port - 4000 (e.g., Raft 12000 -> HTTP 8000)
    httpPort := raftPort - 4000
    if httpPort <= 0 {
        log.Printf("[WARN] Calculated invalid HTTP port %d from Raft port %d", httpPort, raftPort)
        return ""
    }
    return fmt.Sprintf("%s:%d", host, httpPort)
}


// --- Handlers ---

// GET /api/v1/printers
func (s *httpServer) handleGetPrinters(w http.ResponseWriter, r *http.Request) {
	// Read requests can be served by followers, but might be stale.
	// For strong consistency, check leader or use raft.Barrier() before reading.
	// Here, we read directly from the local FSM state.
	printers := s.fsm.GetPrinters()
	respondJSON(w, http.StatusOK, printers)
}

// POST /api/v1/printers
func (s *httpServer) handleCreatePrinter(w http.ResponseWriter, r *http.Request) {
	var reqPrinter Printer
	if err := json.NewDecoder(r.Body).Decode(&reqPrinter); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Assign unique ID
	reqPrinter.ID = uuid.New().String()

	// Basic validation
	if reqPrinter.Company == "" || reqPrinter.Model == "" {
		respondError(w, http.StatusBadRequest, "Missing required fields: company, model")
		return
	}

	payload := CreatePrinterPayload{Printer: reqPrinter}

	// Apply the command via Raft (handled by leaderCheckMiddleware ensuring this runs on leader)
	err := applyCommand(s.raftNode, CmdCreatePrinter, payload)
	if err != nil {
		// Check if it's a "not leader" error (should be caught by middleware, but defensive check)
		if errors.Is(err, raft.ErrNotLeader) || strings.Contains(err.Error(), "not the leader") {
			respondError(w, http.StatusServiceUnavailable, "Not the leader") // Should ideally redirect, but middleware failed?
		} else {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create printer via Raft: %v", err))
		}
		return
	}

	respondJSON(w, http.StatusCreated, reqPrinter) // Return the created printer
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
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Assign unique ID & initial remaining weight
	reqFilament.ID = uuid.New().String()
	if reqFilament.RemainingWeightInGrams == 0 && reqFilament.TotalWeightInGrams > 0 {
		reqFilament.RemainingWeightInGrams = reqFilament.TotalWeightInGrams // Default remaining to total
	}

	// Basic validation
	validTypes := map[string]bool{"PLA": true, "PETG": true, "ABS": true, "TPU": true}
	if !validTypes[reqFilament.Type] {
		respondError(w, http.StatusBadRequest, "Invalid filament type. Options: PLA, PETG, ABS, TPU")
		return
	}
	if reqFilament.Color == "" {
		respondError(w, http.StatusBadRequest, "Missing required field: color")
		return
	}
	if reqFilament.TotalWeightInGrams <= 0 {
		respondError(w, http.StatusBadRequest, "total_weight_in_grams must be positive")
		return
	}
     if reqFilament.RemainingWeightInGrams < 0 || reqFilament.RemainingWeightInGrams > reqFilament.TotalWeightInGrams {
        respondError(w, http.StatusBadRequest, "remaining_weight_in_grams must be between 0 and total_weight_in_grams")
		return
     }


	payload := CreateFilamentPayload{Filament: reqFilament}

	err := applyCommand(s.raftNode, CmdCreateFilament, payload)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create filament via Raft: %v", err))
		return
	}

	respondJSON(w, http.StatusCreated, reqFilament)
}

// GET /api/v1/print_jobs
func (s *httpServer) handleGetPrintJobs(w http.ResponseWriter, r *http.Request) {
    // Optional filtering (example)
    statusFilter := r.URL.Query().Get("status")

    allJobs := s.fsm.GetPrintJobs()

    if statusFilter != "" {
        filteredJobs := make([]PrintJob, 0)
        for _, job := range allJobs {
            if string(job.Status) == statusFilter {
                filteredJobs = append(filteredJobs, job)
            }
        }
         respondJSON(w, http.StatusOK, filteredJobs)
    } else {
	    respondJSON(w, http.StatusOK, allJobs)
    }
}

// POST /api/v1/print_jobs
func (s *httpServer) handleCreatePrintJob(w http.ResponseWriter, r *http.Request) {
	var reqJob PrintJob
	if err := json.NewDecoder(r.Body).Decode(&reqJob); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Assign unique ID, Status is set by FSM
	reqJob.ID = uuid.New().String()
	reqJob.Status = "" // Clear any client-set status

	// Basic validation (more complex validation happens in FSM Apply)
	if reqJob.PrinterID == "" || reqJob.FilamentID == "" || reqJob.Filepath == "" {
		respondError(w, http.StatusBadRequest, "Missing required fields: printer_id, filament_id, filepath")
		return
	}
	if reqJob.PrintWeightInGrams <= 0 {
		respondError(w, http.StatusBadRequest, "print_weight_in_grams must be positive")
		return
	}

	payload := CreatePrintJobPayload{PrintJob: reqJob}

	err := applyCommand(s.raftNode, CmdCreatePrintJob, payload)
	if err != nil {
		// Check for specific FSM errors (like insufficient weight)
		// The error returned by applyCommand *is* the FSM error if one occurred.
        if strings.Contains(err.Error(), "insufficient filament weight") ||
           strings.Contains(err.Error(), "printer does not exist") ||
           strings.Contains(err.Error(), "filament does not exist") {
            respondError(w, http.StatusBadRequest, fmt.Sprintf("Cannot create job: %v", err))
        } else {
            respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create print job via Raft: %v", err))
        }
		return
	}

    // Read back the job from FSM to get the assigned status (Queued)
    s.fsm.mu.RLock()
    createdJob, ok := s.fsm.printJobs[reqJob.ID]
    s.fsm.mu.RUnlock()
    if !ok {
         // This is unexpected if applyCommand succeeded
         log.Printf("[ERROR] Job %s not found in FSM immediately after successful Apply", reqJob.ID)
         respondError(w, http.StatusInternalServerError, "Failed to retrieve created job state")
         return
    }

	respondJSON(w, http.StatusCreated, createdJob)
}

// POST /api/v1/print_jobs/{job_id}/status?status=<new_status>
func (s *httpServer) handleUpdatePrintJobStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["job_id"]
	if !ok {
		respondError(w, http.StatusBadRequest, "Missing job_id in path")
		return
	}

	statusStr := r.URL.Query().Get("status")
	if statusStr == "" {
		respondError(w, http.StatusBadRequest, "Missing 'status' query parameter")
		return
	}

	// Validate the status string
	var nextStatus PrintJobStatus
	switch statusStr {
	case string(StatusRunning):
		nextStatus = StatusRunning
	case string(StatusDone):
		nextStatus = StatusDone
	case string(StatusCancelled):
		nextStatus = StatusCancelled
	default:
		respondError(w, http.StatusBadRequest, "Invalid status value. Options: Running, Done, Cancelled")
		return
	}

	payload := UpdatePrintJobStatusPayload{
		JobID:  jobID,
		Status: nextStatus,
	}

	err := applyCommand(s.raftNode, CmdUpdatePrintJobStatus, payload)
	if err != nil {
        // Check for specific FSM errors (like invalid transition)
        if strings.Contains(err.Error(), "invalid status transition") ||
           strings.Contains(err.Error(), "job does not exist") {
            respondError(w, http.StatusBadRequest, fmt.Sprintf("Cannot update job status: %v", err))
        } else {
		    respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update job status via Raft: %v", err))
        }
		return
	}

    // Read back the updated job from FSM
    s.fsm.mu.RLock()
    updatedJob, ok := s.fsm.printJobs[jobID]
    s.fsm.mu.RUnlock()
     if !ok {
         // Should not happen if applyCommand succeeded and didn't error on "job does not exist"
         log.Printf("[ERROR] Job %s not found in FSM after successful status update Apply", jobID)
         respondError(w, http.StatusInternalServerError, "Failed to retrieve updated job state")
         return
    }

	respondJSON(w, http.StatusOK, updatedJob) // Return updated job
}

// --- Response Helpers ---

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	response, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal JSON response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "Failed to marshal JSON response"}`)) // Raw JSON
		return
	}
	w.WriteHeader(status)
	w.Write(response)
}

func respondError(w http.ResponseWriter, code int, message string) {
	log.Printf("[HTTP ERROR %d] %s", code, message)
	respondJSON(w, code, map[string]string{"error": message})
}
