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

type workerReport struct {
	WorkerID  string   `json:"worker_id"`
	Databases []Actual `json:"databases"`
	Resources struct {
		LoadedDatabases  int             `json:"loaded_databases"`
		LoadingDatabases int             `json:"loading_databases"`
		ActiveSessions   int             `json:"active_sessions"`
		Databases        json.RawMessage `json:"databases"`
	} `json:"resources"`
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
