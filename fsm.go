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

// Raft3DFSM implements the raft.FSM interface.
type Raft3DFSM struct {
	mu        sync.RWMutex
	printers  map[string]Printer  // printer_id -> Printer
	filaments map[string]Filament // filament_id -> Filament
	printJobs map[string]PrintJob // job_id -> PrintJob
}

// fsmSnapshot implements raft.FSMSnapshot interface for checkpointing.
type fsmSnapshot struct {
	printers  map[string]Printer
	filaments map[string]Filament
	printJobs map[string]PrintJob
}

// Apply applies a Raft log entry to the FSM.
// This is the core method where state changes happen based on committed log entries.
// Raft guarantees that Apply calls are serialized.
func (f *Raft3DFSM) Apply(logEntry *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("[ERROR] FSM Apply: Failed to unmarshal command: %v", err)
		// Returning the error makes raft.Apply return it
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
		// Basic validation
		if payload.Filament.RemainingWeightInGrams > payload.Filament.TotalWeightInGrams {
			payload.Filament.RemainingWeightInGrams = payload.Filament.TotalWeightInGrams
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

		// --- Business Logic Checks (Crucial to be done here in Apply) ---
		if _, printerExists := f.printers[job.PrinterID]; !printerExists {
			log.Printf("[ERROR] FSM Apply CreatePrintJob: Printer %s does not exist", job.PrinterID)
			return errors.New("printer does not exist")
		}

		filament, filamentExists := f.filaments[job.FilamentID]
		if !filamentExists {
			log.Printf("[ERROR] FSM Apply CreatePrintJob: Filament %s does not exist", job.FilamentID)
			return errors.New("filament does not exist")
		}

		// Calculate currently reserved filament weight by other Queued/Running jobs
		reservedWeight := 0
		for _, existingJob := range f.printJobs {
			if existingJob.FilamentID == job.FilamentID &&
				(existingJob.Status == StatusQueued || existingJob.Status == StatusRunning) {
				reservedWeight += existingJob.PrintWeightInGrams
			}
		}

		availableWeight := filament.RemainingWeightInGrams - reservedWeight
		if job.PrintWeightInGrams > availableWeight {
			errMsg := fmt.Sprintf("insufficient filament weight: requested %dg, available %dg (reserved %dg)",
				job.PrintWeightInGrams, availableWeight, reservedWeight)
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
		log.Printf("[INFO] FSM Apply: Created print job %s with status %s", job.ID, job.Status)
		return nil

	case CmdUpdatePrintJobStatus:
		var payload UpdatePrintJobStatusPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: Failed payload unmarshal: %v", err)
			return fmt.Errorf("failed payload unmarshal: %w", err)
		}

		job, jobExists := f.printJobs[payload.JobID]
		if !jobExists {
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: Job %s does not exist", payload.JobID)
			return errors.New("job does not exist")
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
			if nextStatus == StatusDone || nextStatus == StatusCancelled {
				validTransition = true
			}
		case StatusDone, StatusCancelled:
			// No transitions allowed from terminal states
			validTransition = false
		}

		if !validTransition {
			errMsg := fmt.Sprintf("invalid status transition from %s to %s for job %s", currentStatus, nextStatus, payload.JobID)
			log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus: %s", errMsg)
			return errors.New(errMsg)
		}
		// --- End State Transition Validation ---

		job.Status = nextStatus
		f.printJobs[payload.JobID] = job // Update the job in the map

		// --- Post-Transition Logic (Filament Weight Reduction) ---
		if nextStatus == StatusDone {
			filament, filamentExists := f.filaments[job.FilamentID]
			if !filamentExists {
				// This shouldn't happen if CreatePrintJob worked, but defensive check
				log.Printf("[ERROR] FSM Apply UpdatePrintJobStatus (Done): Filament %s not found for job %s", job.FilamentID, job.ID)
				// Decide how to handle - maybe still update status but log error?
				// For now, return error to signal inconsistency potential
				return errors.New("internal error: filament not found during 'Done' update")
			}
			filament.RemainingWeightInGrams -= job.PrintWeightInGrams
			if filament.RemainingWeightInGrams < 0 {
				log.Printf("[WARN] FSM Apply UpdatePrintJobStatus (Done): Filament %s remaining weight went negative (%d) after job %s. Setting to 0.",
					filament.ID, filament.RemainingWeightInGrams, job.ID)
				filament.RemainingWeightInGrams = 0
			}
			f.filaments[job.FilamentID] = filament // Update filament state
			log.Printf("[INFO] FSM Apply: Reduced filament %s weight by %dg. Remaining: %dg",
				filament.ID, job.PrintWeightInGrams, filament.RemainingWeightInGrams)
		}
		// --- End Post-Transition Logic ---

		log.Printf("[INFO] FSM Apply: Updated job %s status to %s", payload.JobID, nextStatus)
		return nil

	default:
		log.Printf("[ERROR] FSM Apply: Unrecognized command type: %s", cmd.Type)
		return fmt.Errorf("unrecognized command type: %s", cmd.Type)
	}
}

// Snapshot returns a snapshot of the current FSM state.
// Raft calls this periodically to truncate its log.
func (f *Raft3DFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock() // Use RLock for read-only access
	defer f.mu.RUnlock()

	log.Printf("[INFO] FSM Snapshot: Creating snapshot")

	// Deep copy data to avoid race conditions during serialization
	// (though RLock helps, copying is safer if serialization is slow)
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

	return &fsmSnapshot{
		printers:  p,
		filaments: fl,
		printJobs: pj,
	}, nil
}

// Restore restores the FSM state from a snapshot.
// Called when a node starts up or falls behind.
func (f *Raft3DFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Printf("[INFO] FSM Restore: Restoring state from snapshot")

	// Read the entire snapshot data
	data, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to read snapshot data: %v", err)
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	// Temporary struct to deserialize into
	var state struct {
		Printers  map[string]Printer  `json:"printers"`
		Filaments map[string]Filament `json:"filaments"`
		PrintJobs map[string]PrintJob `json:"printJobs"`
	}

	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[ERROR] FSM Restore: Failed to unmarshal snapshot JSON: %v", err)
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Replace the FSM's state entirely
	f.printers = state.Printers
	if f.printers == nil { // Ensure maps are initialized if snapshot was empty
		f.printers = make(map[string]Printer)
	}
	f.filaments = state.Filaments
	if f.filaments == nil {
		f.filaments = make(map[string]Filament)
	}
	f.printJobs = state.PrintJobs
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
	log.Printf("[DEBUG] fsmSnapshot Persist: Writing snapshot data")
	err := func() error {
		// Combine all data into a single struct for easier JSON handling
		state := struct {
			Printers  map[string]Printer  `json:"printers"`
			Filaments map[string]Filament `json:"filaments"`
			PrintJobs map[string]PrintJob `json:"printJobs"`
		}{
			Printers:  s.printers,
			Filaments: s.filaments,
			PrintJobs: s.printJobs,
		}

		// Encode data as JSON
		snapshotBytes, err := json.Marshal(state)
		if err != nil {
			log.Printf("[ERROR] fsmSnapshot Persist: Failed to marshal state: %v", err)
			return err
		}

		// Write the JSON data to the sink
		if _, err := sink.Write(snapshotBytes); err != nil {
			log.Printf("[ERROR] fsmSnapshot Persist: Failed to write to sink: %v", err)
			return err
		}

		log.Printf("[DEBUG] fsmSnapshot Persist: Snapshot data written successfully")
		// Close the sink (important!)
		return sink.Close()
	}()

	if err != nil {
		log.Printf("[ERROR] fsmSnapshot Persist: Error during persistence, cancelling sink: %v", err)
		_ = sink.Cancel() // Best effort cancel
		return err
	}

	return nil
}

// Release is called when Raft is finished with the snapshot.
func (s *fsmSnapshot) Release() {
	log.Printf("[DEBUG] fsmSnapshot Release: Releasing snapshot resources")
	// No-op in this implementation as data is in memory
}

// NewRaft3DFSM creates a new, initialized FSM.
func NewRaft3DFSM() *Raft3DFSM {
	return &Raft3DFSM{
		printers:  make(map[string]Printer),
		filaments: make(map[string]Filament),
		printJobs: make(map[string]PrintJob),
	}
}

// --- FSM Helper Methods (for HTTP handlers) ---
// These methods read state. Use RLock for safety.

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
