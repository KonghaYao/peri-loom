package platform

import (
	"context"
	"errors"
	"time"

	controller "peri-loom/controller"
)

func operationAdmissionFailure(failure error) gatewayResult {
	if errors.Is(failure, ErrDatabaseQuarantined) {
		return gatewayRefusal(409, "database_isolated", rejectionQuarantine)
	}
	if errors.Is(failure, ErrOperationExists) {
		return gatewayFailure(409, "operation_recorded")
	}
	if errors.Is(failure, ErrOperationCapacity) {
		return gatewayRefusal(503, "operation_registration_unavailable", rejectionOperationCapacity)
	}
	if errors.Is(failure, ErrOperationQueueFull) {
		return gatewayRefusal(503, "operation_registration_unavailable", rejectionOperationQueueFull)
	}
	return gatewayFailure(503, "operation_registration_unavailable")
}

func (gateway *Gateway) isolatedSessionFailure(database controller.DatabaseID) gatewayResult {
	result := gatewayFailure(502, "outcome_unknown")
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		return result
	}
	if record, found := isolatedOperation(state, database, true); found {
		result.operationID, result.operationStage, result.operationObservation = record.OperationID, record.Stage, &record
		result.rpc = sqlRecordedEvidence(record)
	}
	return result
}

// admissionFailure keeps the refusal code and attaches the isolated operation
// that refused the request, so a client that later sees 409 database_isolated
// can name the operation it must inspect instead of guessing.
func (gateway *Gateway) admissionFailure(database controller.DatabaseID, failure error) gatewayResult {
	result := operationAdmissionFailure(failure)
	if result.code != "database_isolated" {
		return result
	}
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		return result
	}
	if record, found := isolatedOperation(state, database, false); found {
		result.operationID, result.operationStage, result.operationObservation = record.OperationID, record.Stage, &record
	}
	return result
}

func (gateway *Gateway) closeTrackedSession(ctx context.Context, session *gatewaySession, requestID string, permit *admissionPermit) error {
	command, err := freezeCommand(map[string]any{"command": "close_session", "session": session.upstream})
	if err != nil {
		return &SQLRPCError{cause: err}
	}
	operationID, err := registerOperation(gateway.config.State, session.database, requestID, "gateway", "close", "close_session", session.worker.ID, session.upstream, command)
	if err != nil {
		return &SQLRPCError{cause: err}
	}
	var closed struct {
		Closed bool `json:"closed"`
	}
	err = gateway.callSession(ctx, session, session.binding, requestID, command, &closed)
	if err == nil && !closed.Closed {
		err = &SQLRPCError{Ambiguous: true}
	}
	result := gatewayResult{session: session, dispatched: true}
	if err != nil {
		result = gateway.upstreamFailure(err, session, permit)
	}
	gateway.settleOperation(session.database, operationID, "close_session", &result, permit)
	if result.code == "outcome_unknown" {
		return &SQLRPCError{Ambiguous: true, cause: err}
	}
	return err
}

func (gateway *Gateway) settleOperation(database controller.DatabaseID, operationID, stage string, result *gatewayResult, permit *admissionPermit) {
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	result.operationID, result.operationStage = operationID, stage
	err := updateOperationRecords(gateway.config.State, func(edit *operationEdit) error {
		key := operationKey(database, operationID)
		record, exists := edit.record(key)
		if !exists || record.State != "in_flight" {
			return ErrOperationExists
		}
		applyOperationOutcome(&record, *result, stage)
		edit.put(key, record)
		if stage == "close_session" && result.code == "" {
			keys := edit.matching(func(_ string, opened OperationRecord) bool {
				return opened.Source == "gateway" && opened.Action == "open" && opened.State == "terminal" && opened.Session == record.Session && opened.Worker == record.Worker && opened.Database == record.Database && opened.CleanupOutcome == "pending"
			})
			for _, openedKey := range keys {
				opened, _ := edit.record(openedKey)
				opened.CleanupOutcome = "completed"
				edit.put(openedKey, opened)
			}
		}
		return nil
	})
	if err != nil {
		finish := result.finish
		*result = gateway.unknown(result.session, permit)
		result.finish, result.operationID, result.operationStage, result.operationDatabase = finish, operationID, stage, database
	}
	if result.code == "outcome_unknown" {
		gateway.holdOperation(database, operationID, permit)
	}
}

// beginDispatchOperation records the durable intent for one worker call. An
// operation that provably cannot commit keeps no intent: its terminal record is
// written once, after the call, by settlePendingOperation. classify reports the
// worker's own verdict on the statement about to be sent, never a guess from its
// text, and may be nil for an action that has no statement to classify.
func (gateway *Gateway) beginDispatchOperation(session *gatewaySession, action, stage, requestID string, writeCursor bool, command frozenCommand, classify func() bool) (string, *pendingOperation, error) {
	if nonCommittingOperation(action, writeCursor) {
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway"); err != nil {
			return "", nil, err
		}
		return "", pendingDispatchOperation(session, action, stage, requestID, command), nil
	}
	if classify != nil {
		// Classifying is a prepare on the worker, so it is asked only after the
		// admission rule an unregistered operation must pass: an isolated database
		// or an exhausted capacity still refuses the request before any SQL reaches
		// the worker, and that refusal is the same one the committing path reports.
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway"); err != nil {
			return "", nil, err
		}
		if classify() {
			return "", pendingDispatchOperation(session, action, stage, requestID, command), nil
		}
	}
	operationID, err := registerOperation(gateway.config.State, session.database, requestID, "gateway", action, stage, session.worker.ID, session.upstream, command)
	return operationID, nil, err
}

func pendingDispatchOperation(session *gatewaySession, action, stage, requestID string, command frozenCommand) *pendingOperation {
	return &pendingOperation{database: session.database, requestID: requestID, action: action, stage: stage, worker: session.worker.ID, session: session.upstream, digest: command.digest()}
}

// settlePendingOperation settles a dispatched operation that was never registered.
// A pending operation is one the platform proved cannot commit: its action is in
// nonCommittingOperation, or the worker classified its statement read-only. Its
// audit row is already the durable completion barrier, so a known outcome writes no
// second record; the sidecar would only put another forced commit between a read and
// its reply. An uncertain outcome is the exception: the record is what lets recovery
// and admission isolate the namespace, so it is still written and quarantined.
func (gateway *Gateway) settlePendingOperation(pending *pendingOperation, result *gatewayResult, permit *admissionPermit) {
	if result.code != "outcome_unknown" {
		return
	}
	operationID, err := settleNonCommittingOperation(gateway.config.State, pending.database, pending.requestID, "gateway", pending.action, pending.stage, pending.worker, pending.session, pending.digest, func(record *OperationRecord) {
		applyOperationOutcome(record, *result, pending.stage)
	})
	result.operationID, result.operationStage = operationID, pending.stage
	if err != nil {
		finish := result.finish
		*result = gateway.unknown(result.session, permit)
		result.finish, result.operationID, result.operationStage, result.operationDatabase = finish, operationID, pending.stage, pending.database
	}
	gateway.holdOperation(pending.database, operationID, permit)
}

// applyOperationOutcome derives the durable record of a dispatched operation
// from its gateway result.
func applyOperationOutcome(record *OperationRecord, result gatewayResult, stage string) {
	if result.session != nil {
		record.Session = result.session.upstream
	}
	record.CursorClientStopped = result.cursorClientStopped
	record.CursorDispatchStopped = result.cursorDispatchStopped
	record.CursorNativeOutcome = result.cursorNativeOutcome
	if result.rpc != nil {
		record.Ambiguous, record.Rejected, record.ExecutionCompleted = result.rpc.Ambiguous, result.rpc.evidence.Rejected(), result.rpc.evidence.Executed()
	}
	if result.code == "outcome_unknown" {
		record.State, record.MayHaveCommitted = "quarantined", true
		if stage == "close_session" {
			record.BusinessOutcome, record.CleanupOutcome = "not_applicable", "unknown"
		} else {
			record.BusinessOutcome = "unknown"
		}
		return
	}
	record.State = "terminal"
	record.BusinessOutcome = "not_executed"
	if sqlExecutedOutcome(result) {
		record.BusinessOutcome = "completed"
		record.ExecutionProven = true
	}
	// A proven rollback is the one outcome a successful close must not report as
	// execution: the worker acknowledged that the write never committed, and the
	// durable record is where that distinction stays queryable.
	if record.CursorNativeOutcome == cursorOutcomeRolledBack {
		record.BusinessOutcome, record.ExecutionProven = "not_executed", false
	}
	if record.Action == "open" && result.code == "" {
		record.CleanupOutcome = "pending"
	}
	if stage == "close_session" {
		record.BusinessOutcome = "not_applicable"
		if result.code == "" {
			record.CleanupOutcome = "completed"
		}
	}
}

func (gateway *Gateway) holdOperation(database controller.DatabaseID, operationID string, permit *admissionPermit) {
	gateway.operationMu.Lock()
	defer gateway.operationMu.Unlock()
	state, err := gateway.config.State.Snapshot()
	if err == nil {
		record := state.Operations[operationKey(database, operationID)]
		if record.LocalResourcesRetired {
			releaseOperationPermit(permit)
			return
		}
		if record.Action == "close" {
			// An unresolved close keeps its unknown but hands the project's
			// session-close lane back, so the cleanup that resolves it — this
			// session's retry, or any sibling's cleanup — can still reserve it.
			permit.releaseLane()
		}
	}
	// The moment an unresolved outcome starts keeping a namespace isolated is
	// the moment its age starts, and the durable record cannot carry that fact.
	gateway.cleanup.noteIsolation(operationKey(database, operationID), time.Now())
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	gateway.operationHolds[operationKey(database, operationID)] = permit
}

func releaseOperationPermit(permit *admissionPermit) {
	permit.releaseRetained()
}
