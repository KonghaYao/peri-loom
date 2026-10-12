package platform

import (
	"context"
	"errors"
	"time"

	controller "peri-loom/controller"
)

const defaultToolCleanupTimeout = 5 * time.Second

func (management *Management) authorityMaintenance(ctx context.Context, database controller.DatabaseID, requestID string, operation any) (any, error) {
	var result any
	err := management.config.Authority.Authority(ctx, requestID, map[string]any{"command": "maintenance", "request_id": requestID, "database": database, "operation": operation}, &result)
	return result, err
}

func (management *Management) maintenance(ctx context.Context, principal Principal, input managementRequest, path string) (any, error) {
	if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
		return nil, err
	}
	var operation map[string]any
	switch path {
	case "/v1/databases/snapshot":
		operation = map[string]any{"operation": "snapshot", "name": input.Name}
	case "/v1/databases/restore":
		operation = map[string]any{"operation": "restore", "snapshot": input.Snapshot}
	case "/v1/databases/restore-point":
		operation = map[string]any{"operation": "restore_pitr", "sequence": input.Sequence}
	case "/v1/databases/pitr":
		operation = map[string]any{"operation": "pitr", "sequence": input.Sequence}
		if input.TimestampMS != 0 {
			operation = map[string]any{"operation": "pitr_time", "timestamp_ms": input.TimestampMS}
		}
	case "/v1/databases/drop-snapshot":
		operation = map[string]any{"operation": "drop_snapshot", "name": input.Name}
	case "/v1/databases/retain-after":
		operation = map[string]any{"operation": "retain_after", "sequence": input.Sequence}
	case "/v1/databases/gc":
		operation = map[string]any{"operation": "gc_collect", "safety_window_ms": input.SafetyWindowMS}
	default:
		return nil, errors.New("unknown maintenance operation")
	}
	result, err := management.authorityMaintenance(ctx, input.Database, input.RequestID, operation)
	if err != nil {
		return nil, err
	}
	if path == "/v1/databases/restore" || path == "/v1/databases/restore-point" {
		if err := management.config.Controller.Reconcile(ctx); err != nil {
			return nil, err
		}
		placement, err := management.config.Controller.Status(input.Database)
		if err != nil || placement.Phase != "active" {
			return nil, errors.New("restored ownership has not converged")
		}
		if err := management.setDatabaseState(input.Database, "active"); err != nil {
			return nil, err
		}
		return map[string]any{"restored": result, "placement": placement}, nil
	}
	return result, nil
}

func (management *Management) inspect(ctx context.Context, principal Principal, input managementRequest, path string) (any, error) {
	if err := management.config.Identity.Authorize(principal, input.Database, "view"); err != nil {
		return nil, err
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	if _, blocked := isolatedOperation(state, input.Database, true); blocked {
		return management.operations(principal, input, "/v1/databases/operations")
	}
	if path == "/v1/databases/gc-inspect" {
		return management.authorityMaintenance(ctx, input.Database, input.RequestID, map[string]any{"operation": "gc_inspect"})
	}
	if path == "/v1/databases/refs" {
		return management.authorityMaintenance(ctx, input.Database, input.RequestID, map[string]any{"operation": "refs"})
	}
	placement, err := management.config.Controller.Status(input.Database)
	if err != nil {
		return nil, err
	}
	metadata, err := management.config.Placements.Snapshot()
	if err != nil {
		return nil, err
	}
	refs, refsErr := management.authorityMaintenance(ctx, input.Database, input.RequestID, map[string]any{"operation": "refs"})
	if refsErr != nil {
		refs = map[string]string{"status": "unavailable", "reason": "authority_inspection_failed"}
	}
	var diagnostics any = map[string]string{"status": "unavailable", "reason": "placement_not_active", "scope": "native_control_ownership_state"}
	if placement.Phase == "active" {
		diagnostics = map[string]string{"status": "unavailable", "reason": "native_inspection_failed", "scope": "native_control_ownership_state"}
		worker, exists := metadata.Workers[placement.DesiredWorker]
		requestID, identifierErr := controller.RandomID()
		if exists && identifierErr == nil {
			var native controller.Actual
			diagnosticErr := management.config.Authority.Worker(ctx, worker, requestID, map[string]any{"command": "get_database_state", "database": input.Database}, &native)
			if diagnosticErr == nil && native.Database == input.Database && (native.Binding == nil || native.Binding.Database == input.Database && native.Binding.WorkerID == worker.ID && native.Binding.Epoch == native.Epoch) {
				diagnostics = map[string]any{"status": "available", "scope": "native_control_ownership_state", "read_only": true, "state": native}
			}
		}
	}
	return map[string]any{"placement": placement, "worker": metadata.Workers[placement.DesiredWorker], "refs": refs, "diagnostics": diagnostics}, nil
}

func (management *Management) nativeTool(ctx context.Context, principal Principal, input managementRequest, path string) (output any, failure error) {
	stage, phase := "authorize", "unknown"
	registered, businessCompleted := false, false
	operationID := ""
	var permit *admissionPermit
	defer func() {
		if registered {
			if err := finishOperation(management.config.State, input.Database, operationID, failure, businessCompleted); err != nil {
				output, failure = nil, &SQLRPCError{Ambiguous: true, cause: err}
			}
		}
		if permit != nil {
			if registered && failure != nil {
				permit.retain()
				management.config.Resources.holdOperation(input.Database, operationID, permit)
			} else {
				permit.finish()
			}
		}
		if failure != nil {
			failure = management.nativeToolFailure(path, stage, phase, input.RequestID, failure)
			var tool *nativeToolError
			var rpc *SQLRPCError
			if registered && errors.As(failure, &tool) && (!errors.As(failure, &rpc) || rpc.Ambiguous) {
				tool.unknown = true
			}
			if errors.As(failure, &tool) {
				tool.operationID = operationID
			}
		}
	}()
	action := "checkpoint"
	if path == "/v1/databases/schema" || path == "/v1/databases/diagnostics" {
		action = "view"
	}
	if err := management.config.Identity.Authorize(principal, input.Database, action); err != nil {
		return nil, err
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	if record, blocked := isolatedOperation(state, input.Database, true); blocked {
		if action == "view" {
			if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
				return nil, err
			}
			return map[string]any{"status": "isolated", "operation": record}, nil
		}
		return nil, ErrDatabaseQuarantined
	}
	stage = "placement"
	placement, err := management.config.Controller.Status(input.Database)
	phase = placement.Phase
	if err != nil || placement.Phase != "active" {
		return nil, errors.New("database is not active")
	}
	stage = "metadata"
	metadata, err := management.config.Placements.Snapshot()
	if err != nil {
		return nil, err
	}
	stage = "worker_lookup"
	worker, exists := metadata.Workers[placement.DesiredWorker]
	if !exists {
		return nil, errors.New("worker missing")
	}
	var opened struct {
		Session  string `json:"session"`
		Readonly bool   `json:"readonly"`
	}
	stage = "open_session"
	if management.config.Resources != nil {
		descriptor := state.Databases[input.Database.Key()]
		var admitted bool
		permit, admitted = management.config.Resources.admission.reserveLane(descriptor.Project, state.Projects[descriptor.Project].Quotas, true, false, true)
		if !admitted {
			return nil, errors.New("native resource admission unavailable")
		}
	}
	operationID, err = registerOperation(management.config.State, input.Database, input.RequestID, "management", nativeDiagnosticAction(path), stage, worker.ID, "", map[string]any{"path": path, "database": input.Database})
	if err != nil {
		return nil, err
	}
	registered = true
	if err := management.config.Upstream.Call(ctx, worker, input.RequestID+"-open", map[string]any{"command": "open_session", "database": input.Database}, &opened); err != nil {
		return nil, err
	}
	if !upstreamSessionID(opened.Session) {
		return nil, &SQLRPCError{Ambiguous: true}
	}
	if err := updateOperation(management.config.State, input.Database, operationID, func(record *OperationRecord) {
		record.Session = opened.Session
		record.Stage = "tool_rpc"
		record.CleanupOutcome = "pending"
	}); err != nil {
		return nil, &SQLRPCError{Ambiguous: true, cause: err}
	}
	defer func() {
		if failure != nil {
			return
		}
		stage = "close_session"
		if err := updateOperation(management.config.State, input.Database, operationID, func(record *OperationRecord) {
			record.Stage = stage
			record.BusinessOutcome = "completed"
		}); err != nil {
			output, failure = nil, &SQLRPCError{Ambiguous: true, cause: err}
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), defaultToolCleanupTimeout)
		defer cancel()
		var closed struct {
			Closed bool `json:"closed"`
		}
		if err := management.config.Upstream.Call(cleanup, worker, input.RequestID+"-close", map[string]any{"command": "close_session", "session": opened.Session}, &closed); err != nil {
			output, failure = nil, err
			return
		}
		if !closed.Closed {
			output, failure = nil, &SQLRPCError{Ambiguous: true, cause: errors.New("native tool Close acknowledgement missing")}
		}
	}()
	command := map[string]any{"session": opened.Session}
	switch path {
	case "/v1/databases/schema":
		command["command"] = "query"
		command["sql"] = "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name;"
	case "/v1/databases/checkpoint":
		command["command"] = "checkpoint"
	case "/v1/databases/compact":
		command["command"] = "compact"
	case "/v1/databases/diagnostics":
		command["command"] = "diagnostics"
	}
	var result any
	stage = "tool_rpc"
	err = management.config.Upstream.Call(ctx, worker, input.RequestID, command, &result)
	businessCompleted = err == nil
	return result, err
}
