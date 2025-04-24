package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// Raft3DFSM implements the raft.FSM interface. It holds the application state.
type Raft3DFSM struct {
	mu        sync.RWMutex
	printers  map[string]Printer  // printer_id -> Printer
	filaments map[string]Filament // filament_id -> Filament
	printJobs map[string]PrintJob // job_id -> PrintJob
}

// NewRaft3DFSM creates a new, initialized FSM.
func NewRaft3DFSM() *Raft3DFSM {
	return &Raft3DFSM{
		printers:  make(map[string]Printer),
		filaments: make(map[string]Filament),
		printJobs: make(map[string]PrintJob),
	}
}

// Apply applies a Raft log entry to the FSM.
// This is the core method where state changes happen based on committed log entries.
// Raft guarantees that Apply calls are serialized and deterministic.
func (f *Raft3DFSM) Apply(logEntry *raft.Log) interface{} {
	f.mu.Lock() // Lock for writing
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("[ERROR] FSM Apply: Failed to unmarshal command: %v", err)
		// Returning the error makes raft.Apply() return it to the caller (leader)
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	log.Printf("[DEBUG] FSM Apply: Processing command type %s", cmd.Type)

	switch cmd.Type {
	case CmdCreatePrinter:
		var payload CreatePrinterPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Printf("[ERROR] FSM Apply CreatePrinter: Failed payload unmarshal: %v", err)
			return fmt.Errorf("failed payload unmarshal: %w", err)
		}
		if _, exists := f.printers[payload.Printer.ID]; exists {
			log.Printf("[WARN] FSM Apply CreatePrinter: Printer ID %s already exists", payload.Printer.ID)
			// Idempotency: If it exists, it's not really an error for Raft state
			return nil // Or return a specific error if overwriting is disallowed
		}
		f.printers[payload.Printer.ID] = payload.Printer
		log.Printf("[INFO] FSM Apply: Created printer %s", payload.Printer.ID)
		return nil // Success

	case CmdCreateFilament:
		var payload CreateFilamentPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Printf("[ERROR] FSM Apply CreateFilament: Failed payload unmarshal: %v", err)
			return fmt.Errorf("failed payload unmarshal: %w", err)
		}
		if _, exists := f.filaments[payload.Filament.ID]; exists {
			log.Printf("[WARN] FSM Apply CreateFilament: Filament ID %s already exists", payload.Filament.ID)
			return nil // Idempotency
		}
		// Basic validation (more complex logic could be added)
		if payload.Filament.RemainingWeightInGrams > payload.Filament.TotalWeightInGrams {
			payload.Filament.RemainingWeightInGrams = payload.Filament.TotalWeightInGrams
		}
		if payload.Filament.RemainingWeightInGrams < 0 {
			payload.Filament.RemainingWeightInGrams = 0
		}
		f.filaments[payload.Filament.ID] = payload.Filament
		log.Printf("[INFO] FSM Apply: Created filament %s", payload.Filament.ID)
		return nil

	case CmdCreatePrintJob:
		var payload CreatePrintJobPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Printf("[ERROR] FSM Apply CreatePrintJob: Failed payload unmarshal: %v", err)
			return fmt.Errorf("failed payload unmarshal: %w", err)
		}
		job := payload.PrintJob // Get the job data

		// --- Business Logic Checks (Crucial to be done here in Apply for consistency) ---
		if _, printerExists := f.printers[job.PrinterID]; !printerExists {
			errMsg := fmt.Sprintf("printer %s does not exist", job.PrinterID)
			log.Printf("[ERROR] FSM Apply CreatePrintJob: %s", errMsg)
			return errors.New(errMsg) // Return specific error
		}

		filament, filamentExists := f.filaments[job.FilamentID]
		if !filamentExists {
			errMsg := fmt.Sprintf("filament %s does not exist", job.FilamentID)
			log.Printf("[ERROR] FSM Apply CreatePrintJob: %s", errMsg)
			return errors.New(errMsg)
		}

		// Calculate currently reserved filament weight by other Queued/Running jobs
		reservedWeight := 0
		for _, existingJob := range f.printJobs {
			// Only consider jobs for the *same* filament that are *not* finished/cancelled
			if existingJob.ID != job.ID && // Exclude the job being created itself from reservation check
				existingJob.FilamentID == job.FilamentID &&
				(existingJob.Status == StatusQueued || existingJob.Status == StatusRunning) {
				reservedWeight += existingJob.PrintWeightInGrams
			}
		}

		availableWeight := filament.RemainingWeightInGrams - reservedWeight
		if job.PrintWeightInGrams > availableWeight {
			errMsg := fmt.Sprintf("insufficient filament weight for job %s: requested %dg, available %dg (total %dg, reserved %dg)",
				job.ID, job.PrintWeightInGrams, availableWeight, filament.RemainingWeightInGrams, reservedWeight)
			log.Printf("[ERROR] FSM Apply CreatePrintJob: %s", errMsg)
			return errors.New(errMsg)
		}
		// --- End Business Logic Checks ---

		if _, jobExists := f.printJobs[job.ID]; jobExists {
			log.Printf("[WARN] FSM Apply CreatePrintJob: Job ID %s already exists", job.ID)
			return nil // Idempotency
		}

		// Set initial status (important: controlled by FSM, not client)
		job.Status = StatusQueued
		f.printJobs[job.ID] = job
		log.Printf("[INFO] FSM Apply: Created print job %s for printer %s with status %s", job.ID, job.PrinterID, job.Status)
		return nil

	case CmdUpdatePrintJobStatus:
		var payload UpdatePrintJobStatusPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: Failed payload unmarshal: %v", err)
			return fmt.Errorf("failed payload unmarshal: %w", err)
		}

		job, jobExists := f.printJobs[payload.JobID]
		if !jobExists {
			errMsg := fmt.Sprintf("job %s does not exist", payload.JobID)
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: %s", errMsg)
			return errors.New(errMsg)
		}

		// --- State Transition Validation ---
		validTransition := false
		currentStatus := job.Status
		nextStatus := payload.Status

		switch currentStatus {
		case StatusQueued:
			if nextStatus == StatusRunning || nextStatus == StatusCancelled {
				validTransition = true
			}
		case StatusRunning:
			// Allow transition to Done or Cancelled
			if nextStatus == StatusDone || nextStatus == StatusCancelled {
				validTransition = true
			}
		case StatusDone, StatusCancelled:
			// No transitions allowed *from* terminal states
			validTransition = false
			log.Printf("[WARN] FSM Apply UpdatePrintJobStatus: Ignoring transition from terminal state %s for job %s", currentStatus, payload.JobID)
			return errors.New("cannot transition from a terminal state") // Return error for invalid transition
		}

		if !validTransition {
			errMsg := fmt.Sprintf("invalid status transition from %s to %s for job %s", currentStatus, nextStatus, payload.JobID)
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: %s", errMsg)
			return errors.New(errMsg)
		}
		// --- End State Transition Validation ---

		previousStatus := job.Status // Store previous status for logic below
		job.Status = nextStatus
		f.printJobs[payload.JobID] = job // Update the job in the map

		// --- Post-Transition Logic (Filament Weight Reduction) ---
		// Reduce weight ONLY when transitioning FROM Running TO Done
		if previousStatus == StatusRunning && nextStatus == StatusDone {
			filament, filamentExists := f.filaments[job.FilamentID]
			if !filamentExists {
				// This shouldn't happen if CreatePrintJob worked, but defensive check
				errMsg := fmt.Sprintf("internal error: filament %s not found during 'Done' update for job %s", job.FilamentID, job.ID)
				log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus (Done): %s", errMsg)
				// Return error to signal inconsistency potential
				return errors.New(errMsg)
			}
			filament.RemainingWeightInGrams -= job.PrintWeightInGrams
			if filament.RemainingWeightInGrams < 0 {
				log.Printf("[WARN] FSM Apply UpdatePrintJobStatus (Done): Filament %s remaining weight went negative (%d) after job %s. Setting to 0.",
					filament.ID, filament.RemainingWeightInGrams, job.ID)
				filament.RemainingWeightInGrams = 0 // Clamp to zero
			}
			f.filaments[job.FilamentID] = filament // Update filament state
			log.Printf("[INFO] FSM Apply: Reduced filament %s weight by %dg upon job %s completion. Remaining: %dg",
				filament.ID, job.PrintWeightInGrams, job.ID, filament.RemainingWeightInGrams)
		}
		// --- End Post-Transition Logic ---

		log.Printf("[INFO] FSM Apply: Updated job %s status to %s", payload.JobID, nextStatus)
		return nil

	default:
		log.Printf("[ERROR] FSM Apply: Unrecognized command type: %s", cmd.Type)
		return fmt.Errorf("unrecognized command type: %s", cmd.Type)
	}
}

// fsmSnapshot implements raft.FSMSnapshot interface for checkpointing.
// It holds the serialized state.
type fsmSnapshot struct {
	printersData  []byte
	filamentsData []byte
	printJobsData []byte
}

// Snapshot returns a snapshot of the current FSM state.
// Raft calls this periodically to truncate its log.
func (f *Raft3DFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock() // Use RLock for read-only access
	defer f.mu.RUnlock()

	log.Printf("[INFO] FSM Snapshot: Creating snapshot of current state")

	// Create copies of the maps to avoid holding lock during marshaling
	p := make(map[string]Printer, len(f.printers))
	for k, v := range f.printers {
		p[k] = v
	}
	fl := make(map[string]Filament, len(f.filaments))
	for k, v := range f.filaments {
		fl[k] = v
	}
	pj := make(map[string]PrintJob, len(f.printJobs))
	for k, v := range f.printJobs {
		pj[k] = v
	}

	// Serialize components individually
	printersBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("snapshot failed: marshal printers: %w", err)
	}
	filamentsBytes, err := json.Marshal(fl)
	if err != nil {
		return nil, fmt.Errorf("snapshot failed: marshal filaments: %w", err)
	}
	jobsBytes, err := json.Marshal(pj)
	if err != nil {
		return nil, fmt.Errorf("snapshot failed: marshal print jobs: %w", err)
	}

	log.Printf("[DEBUG] FSM Snapshot: State serialized successfully")
	return &fsmSnapshot{
		printersData:  printersBytes,
		filamentsData: filamentsBytes,
		printJobsData: jobsBytes,
	}, nil
}

// Restore restores the FSM state from a snapshot.
// Called when a node starts up or falls behind.
func (f *Raft3DFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock() // Lock for writing, replacing entire state
	defer f.mu.Unlock()

	log.Printf("[INFO] FSM Restore: Restoring state from snapshot...")

	// Read the entire snapshot data - assumes Persist writes a structured format
	data, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to read snapshot data: %v", err)
		return fmt.Errorf("failed to read snapshot: %w", err)
	}
	// No need to close rc here, ReadAll does it if successful

	// Temporary struct to deserialize the snapshot data wrapper
	var snapshotData struct {
		PrintersData  json.RawMessage `json:"printers"`
		FilamentsData json.RawMessage `json:"filaments"`
		PrintJobsData json.RawMessage `json:"printJobs"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to unmarshal snapshot wrapper: %v", err)
		return fmt.Errorf("failed to unmarshal snapshot wrapper: %w", err)
	}

	// Deserialize each part
	var printers map[string]Printer
	if err := json.Unmarshal(snapshotData.PrintersData, &printers); err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to unmarshal printers: %v", err)
		return fmt.Errorf("failed to unmarshal printers: %w", err)
	}
	var filaments map[string]Filament
	if err := json.Unmarshal(snapshotData.FilamentsData, &filaments); err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to unmarshal filaments: %v", err)
		return fmt.Errorf("failed to unmarshal filaments: %w", err)
	}
	var printJobs map[string]PrintJob
	if err := json.Unmarshal(snapshotData.PrintJobsData, &printJobs); err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to unmarshal print jobs: %v", err)
		return fmt.Errorf("failed to unmarshal print jobs: %w", err)
	}

	// Replace the FSM's state entirely
	f.printers = printers
	if f.printers == nil { // Ensure maps are initialized if snapshot was empty
		f.printers = make(map[string]Printer)
	}
	f.filaments = filaments
	if f.filaments == nil {
		f.filaments = make(map[string]Filament)
	}
	f.printJobs = printJobs
	if f.printJobs == nil {
		f.printJobs = make(map[string]PrintJob)
	}

	log.Printf("[INFO] FSM Restore: State restored successfully. Printers: %d, Filaments: %d, Jobs: %d",
		len(f.printers), len(f.filaments), len(f.printJobs))
	return nil
}

// --- fsmSnapshot Methods ---

// Persist saves the FSM snapshot state to a sink (usually a file).
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Printf("[DEBUG] fsmSnapshot Persist: Writing snapshot data to sink")
	err := func() error {
		// Wrap the already serialized byte slices into a structure for final JSON marshaling
		snapshotWrapper := struct {
			PrintersData  json.RawMessage `json:"printers"`
			FilamentsData json.RawMessage `json:"filaments"`
			PrintJobsData json.RawMessage `json:"printJobs"`
		}{
			PrintersData:  json.RawMessage(s.printersData),
			FilamentsData: json.RawMessage(s.filamentsData),
			PrintJobsData: json.RawMessage(s.printJobsData),
		}

		// Encode the wrapper structure as JSON
		snapshotBytes, err := json.Marshal(snapshotWrapper)
		if err != nil {
			return fmt.Errorf("persist failed: marshal snapshot wrapper: %w", err)
		}

		// Write the JSON data to the sink
		if _, err := sink.Write(snapshotBytes); err != nil {
			return fmt.Errorf("persist failed: write to sink: %w", err)
		}

		log.Printf("[DEBUG] fsmSnapshot Persist: Snapshot data written successfully (%d bytes)", len(snapshotBytes))
		// Close the sink (important!) - indicates success
		return sink.Close()
	}()

	if err != nil {
		log.Printf("[ERROR] fsmSnapshot Persist: Error during persistence, cancelling sink: %v", err)
		_ = sink.Cancel() // Best effort cancel on error
		return err
	}
	return nil
}

// Release is called when Raft is finished with the snapshot.
func (s *fsmSnapshot) Release() {
	log.Printf("[DEBUG] fsmSnapshot Release: Releasing snapshot resources (no-op)")
	// No explicit resources to release here as we stored byte slices.
}

// --- FSM Helper Methods (for HTTP handlers or direct access) ---
// These methods read state. Use RLock for concurrency safety.

func (f *Raft3DFSM) GetPrinters() []Printer {
	f.mu.RLock()
	defer f.mu.RUnlock()
	printers := make([]Printer, 0, len(f.printers))
	for _, p := range f.printers {
		printers = append(printers, p)
	}
	return printers
}

func (f *Raft3DFSM) GetFilaments() []Filament {
	f.mu.RLock()
	defer f.mu.RUnlock()
	filaments := make([]Filament, 0, len(f.filaments))
	for _, fl := range f.filaments {
		filaments = append(filaments, fl)
	}
	return filaments
}

func (f *Raft3DFSM) GetPrintJobs() []PrintJob {
	f.mu.RLock()
	defer f.mu.RUnlock()
	jobs := make([]PrintJob, 0, len(f.printJobs))
	for _, j := range f.printJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// GetPrintJob returns a single print job by ID and a boolean indicating if found.
func (f *Raft3DFSM) GetPrintJob(id string) (PrintJob, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	job, ok := f.printJobs[id]
	return job, ok
}
