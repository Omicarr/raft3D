package main

import "encoding/json"

// --- Data Objects ---

type Printer struct {
	ID      string `json:"id"`
	Company string `json:"company"`
	Model   string `json:"model"`
}

type Filament struct {
	ID                   string `json:"id"`
	Type                 string `json:"type"` // PLA, PETG, ABS, TPU
	Color                string `json:"color"`
	TotalWeightInGrams   int    `json:"total_weight_in_grams"`
	RemainingWeightInGrams int    `json:"remaining_weight_in_grams"`
}

type PrintJobStatus string

const (
	StatusQueued    PrintJobStatus = "Queued"
	StatusRunning   PrintJobStatus = "Running"
	StatusCancelled PrintJobStatus = "Cancelled"
	StatusDone      PrintJobStatus = "Done"
)

type PrintJob struct {
	ID                 string         `json:"id"`
	PrinterID          string         `json:"printer_id"`
	FilamentID         string         `json:"filament_id"`
	Filepath           string         `json:"filepath"`
	PrintWeightInGrams int            `json:"print_weight_in_grams"`
	Status             PrintJobStatus `json:"status"`
}

// --- Raft Commands ---
// Used to serialize commands before applying to Raft log

type CommandType string

const (
	CmdCreatePrinter        CommandType = "CreatePrinter"
	CmdCreateFilament       CommandType = "CreateFilament"
	CmdCreatePrintJob       CommandType = "CreatePrintJob"
	CmdUpdatePrintJobStatus CommandType = "UpdatePrintJobStatus"
)

// Command is the generic structure sent over the wire/log
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"` // Store specific command data here as raw JSON bytes
}

// --- Command Payloads ---
// Specific data structures for each command type

type CreatePrinterPayload struct {
	Printer Printer `json:"printer"`
}

type CreateFilamentPayload struct {
	Filament Filament `json:"filament"`
}

type CreatePrintJobPayload struct {
	PrintJob PrintJob `json:"print_job"`
}

type UpdatePrintJobStatusPayload struct {
	JobID  string         `json:"job_id"`
	Status PrintJobStatus `json:"status"`
}

// Helper to serialize commands into the generic Command structure for Raft
func serializeCommand(cmdType CommandType, payload interface{}) ([]byte, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	cmd := Command{
		Type:    cmdType,
		Payload: payloadBytes, // Assign the marshaled specific payload here
	}
	// Marshal the generic command structure
	return json.Marshal(cmd)
}
