package platform

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

const nativeDiagnosticLimit = 64

type nativeToolError struct {
	stage       string
	cause       error
	unknown     bool
	operationID string
}

func (failure *nativeToolError) Error() string { return failure.cause.Error() }
func (failure *nativeToolError) Unwrap() error { return failure.cause }

type nativeDiagnosticRecord struct {
	Event              string `json:"event"`
	Action             string `json:"action"`
	Stage              string `json:"stage"`
	RequestID          string `json:"request_id"`
	Phase              string `json:"phase"`
	RPCError           bool   `json:"rpc_error"`
	Ambiguous          bool   `json:"ambiguous"`
	Rejected           bool   `json:"rejected"`
	ExecutionCompleted bool   `json:"execution_completed"`
}

type nativeDiagnosticBudget struct {
	mu    sync.Mutex
	count uint32
}

func nativeDiagnosticAction(path string) string {
	switch path {
	case "/v1/databases/schema":
		return "schema"
	case "/v1/databases/checkpoint":
		return "checkpoint"
	case "/v1/databases/compact":
		return "compact"
	case "/v1/databases/diagnostics":
		return "diagnostics"
	default:
		return ""
	}
}

func nativeDiagnosticPhase(phase string) string {
	switch phase {
	case "active", "renew_intent", "renew_pending", "renew_worker_pending",
		"grant_intent", "grant_pending", "acquire_pending", "migrate_drain",
		"suspend_drain", "revoke_pending", "suspended":
		return phase
	default:
		return "unknown"
	}
}

func (budget *nativeDiagnosticBudget) emit(record nativeDiagnosticRecord, writer io.Writer) {
	if !labelPattern.MatchString(record.RequestID) || nativeDiagnosticAction("/v1/databases/"+record.Action) == "" {
		return
	}
	switch record.Stage {
	case "authorize", "placement", "metadata", "worker_lookup", "open_session", "tool_rpc", "close_session":
	default:
		return
	}
	record.Event = "management_native_tool_error"
	record.Phase = nativeDiagnosticPhase(record.Phase)
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.count >= nativeDiagnosticLimit {
		return
	}
	budget.count++
	_ = json.NewEncoder(writer).Encode(record)
}

func nativeToolRecord(path, stage, phase, requestID string, cause error) nativeDiagnosticRecord {
	record := nativeDiagnosticRecord{
		Action: nativeDiagnosticAction(path), Stage: stage, Phase: phase, RequestID: requestID,
	}
	var rpc *SQLRPCError
	if errors.As(cause, &rpc) {
		record.RPCError = true
		record.Ambiguous = rpc.Ambiguous
		record.Rejected = rpc.evidence.Rejected()
		record.ExecutionCompleted = rpc.evidence.Executed()
	}
	return record
}

func (management *Management) nativeToolFailure(path, stage, phase, requestID string, cause error) error {
	management.nativeDiagnostics.emit(nativeToolRecord(path, stage, phase, requestID, cause), os.Stderr)
	return &nativeToolError{stage: stage, cause: cause}
}
