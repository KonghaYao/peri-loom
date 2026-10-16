package controller

import (
	"context"
	"encoding/json"
	"errors"
)

type grantIntent struct {
	Database      DatabaseID `json:"database"`
	WorkerID      string     `json:"worker_id"`
	Epoch         uint64     `json:"epoch"`
	ExpectedEpoch uint64     `json:"expected_epoch"`
	LeaseID       string     `json:"lease_id"`
	TTLMS         uint64     `json:"ttl_ms"`
	Role          string     `json:"role"`
}

type renewIntent struct {
	Binding  Binding `json:"binding"`
	Sequence uint64  `json:"sequence"`
	TTLMS    uint64  `json:"ttl_ms"`
}

type leaseIntent struct {
	Kind   string          `json:"kind"`
	Intent json.RawMessage `json:"intent"`
}

type leaseRequest struct {
	RequestID string          `json:"request_id"`
	Nonce     string          `json:"nonce"`
	Intent    json.RawMessage `json:"intent"`
}

type authorityAction struct {
	Command  string        `json:"command"`
	Intent   *leaseIntent  `json:"intent,omitempty"`
	Request  *leaseRequest `json:"request,omitempty"`
	Database *DatabaseID   `json:"database,omitempty"`
}

type revokeAction struct {
	Command string `json:"command"`
	Request struct {
		RequestID     string     `json:"request_id"`
		Database      DatabaseID `json:"database"`
		ExpectedEpoch uint64     `json:"expected_epoch"`
	} `json:"request"`
}

type workerAction struct {
	Command       string      `json:"command"`
	Database      *DatabaseID `json:"database,omitempty"`
	ExpectedEpoch *uint64     `json:"expected_epoch,omitempty"`
	Lease         *Grant      `json:"lease,omitempty"`
	Role          string      `json:"role,omitempty"`
}

// workerReport is the ownership state a worker reports about itself. It is
// decoded strictly, so every field a worker sends has to be modelled here: an
// unmodelled field is a heartbeat the platform cannot verify, and the fix is to
// add the field, never to stop checking.
//
// The worker describes the databases it can account for and names the ones it
// cannot under failures, so a database absent from databases is a database whose
// state is unknown — never one the worker released. No reader may derive
// "detached" from that absence.
type workerReport struct {
	WorkerID  string                `json:"worker_id"`
	Databases []Actual              `json:"databases"`
	Failures  []ownershipFailure    `json:"failures"`
	Resources workerReportResources `json:"resources"`
}

// ownershipFailure names one database the worker could not describe, with the
// worker's own reason. The reason stays a label for operators: it is never
// parsed into a decision.
type ownershipFailure struct {
	Database DatabaseID `json:"database"`
	Error    string     `json:"error"`
}

// workerReportResources is the worker's residency report for the part of its
// registry it could describe.
type workerReportResources struct {
	LoadedDatabases        int                    `json:"loaded_databases"`
	LoadingDatabases       int                    `json:"loading_databases"`
	ActiveSessions         int                    `json:"active_sessions"`
	ResidentCount          int                    `json:"resident_count"`
	OldestResidentAgeMS    uint64                 `json:"oldest_resident_age_ms"`
	QuarantinedCount       int                    `json:"quarantined_count"`
	OldestQuarantinedAgeMS uint64                 `json:"oldest_quarantined_age_ms"`
	Databases              []workerReportDatabase `json:"databases"`
}

// workerReportDatabase is one resident database of the report. resource_error
// marks a database whose resource sample failed: the entry is described but its
// resources are not, which is a partial report rather than a released database.
// resources itself stays opaque — no decision reads it, and every metric counter
// modelled here would turn a new worker metric into a failed heartbeat.
type workerReportDatabase struct {
	Identifier     DatabaseID      `json:"identifier"`
	Epoch          uint64          `json:"epoch"`
	Lifecycle      string          `json:"lifecycle"`
	ActiveSessions int             `json:"active_sessions"`
	IdleMS         *uint64         `json:"idle_ms"`
	Resources      json.RawMessage `json:"resources"`
	ResourceError  *string         `json:"resource_error"`
}

func (controller *Controller) authorityState(ctx context.Context, database DatabaseID) (Actual, error) {
	identifier, err := RandomID()
	if err != nil {
		return Actual{}, err
	}
	var actual Actual
	err = controller.transport.Authority(ctx, identifier, authorityAction{Command: "get_database_state", Database: &database}, &actual)
	if err == nil && (actual.Database != database || (actual.Binding != nil && (actual.Binding.Database != database || actual.Binding.Epoch != actual.Epoch))) {
		err = errors.New("authority state identity mismatch")
	}
	return actual, err
}

func (controller *Controller) workerState(ctx context.Context, worker Worker, database DatabaseID) (Actual, error) {
	identifier, err := RandomID()
	if err != nil {
		return Actual{}, err
	}
	var actual Actual
	err = controller.transport.Worker(ctx, worker, identifier, workerAction{Command: "get_database_state", Database: &database}, &actual)
	if err == nil && (actual.Database != database || (actual.Binding != nil && (actual.Binding.Database != database || actual.Binding.WorkerID != worker.ID || actual.Binding.Epoch != actual.Epoch))) {
		err = errors.New("worker state identity mismatch")
	}
	return actual, err
}

func sameLease(previous, current Binding) bool {
	previous.RenewalSequence = current.RenewalSequence
	return previous == current
}

func serving(actual Actual, placement Placement) bool {
	return placement.Grant != nil && actual.Binding != nil && *actual.Binding == placement.Grant.Binding && actual.Mode == placement.Role && actual.RemainingTTLMS > 0
}

func (controller *Controller) sendWorker(ctx context.Context, worker Worker, identifier string, action workerAction) error {
	var result json.RawMessage
	return controller.transport.Worker(ctx, worker, identifier, action, &result)
}
