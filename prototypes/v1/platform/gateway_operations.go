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
	if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", "close", "close_session"); err != nil {
		return &SQLRPCError{cause: err}
	}
	pending := pendingOutcomeOperation(session.database, session.worker.ID, session.upstream, "close", "close_session", requestID, command)
	var closed struct {
		Closed bool `json:"closed"`
	}
	if session.fusedClose {
		// The fused open already closed the worker session; only the platform's
		// own close record is still owed. It is settled exactly like a completed
		// close so the pending open it discharges and the retry account agree.
		// The marker stays set when the record cannot be written: a later retry
		// must skip the worker call again, because that session does not exist.
		result := gatewayResult{session: session, dispatched: true, value: map[string]bool{"closed": true}}
		gateway.settlePendingOperation(pending, &result, permit, nil)
		if result.code == "outcome_unknown" {
			return &SQLRPCError{Ambiguous: true}
		}
		session.fusedClose = false
		return nil
	}
	err = gateway.callSession(ctx, session, session.binding, requestID, command, &closed)
	if err == nil && !closed.Closed {
		err = &SQLRPCError{Ambiguous: true}
	}
	result := gatewayResult{session: session, dispatched: true}
	if err != nil {
		result = gateway.upstreamFailure(err, session, permit)
	}
	gateway.settlePendingOperation(pending, &result, permit, nil)
	if result.code == "outcome_unknown" {
		return &SQLRPCError{Ambiguous: true, cause: err}
	}
	return err
}

func (gateway *Gateway) settleOperation(database controller.DatabaseID, operationID, stage string, result *gatewayResult, permit *admissionPermit) {
	gateway.settleOperationCaller(database, operationID, stage, result, permit, "")
}

func (gateway *Gateway) settleOperationCaller(database controller.DatabaseID, operationID, stage string, result *gatewayResult, permit *admissionPermit, requestID string) {
	caller := newDurabilityCaller(gateway.config.State, operationID, requestID, "terminal", "gateway")
	_ = gateway.settleOperationUsingCaller(database, operationID, stage, result, permit, func(change func(*operationEdit) error) (func() error, error) {
		return nil, updateOperationRecords(gateway.config.State, change)
	}, caller)
}

func (gateway *Gateway) settleOperationUsing(database controller.DatabaseID, operationID, stage string, result *gatewayResult, permit *admissionPermit, submit func(func(*operationEdit) error) (func() error, error)) error {
	return gateway.settleOperationUsingCaller(database, operationID, stage, result, permit, submit, nil)
}

func (gateway *Gateway) settleOperationUsingCaller(database controller.DatabaseID, operationID, stage string, result *gatewayResult, permit *admissionPermit, submit func(func(*operationEdit) error) (func() error, error), caller *durabilityCaller) error {
	unlock := lockObservedOperationNamespace(database.Namespace, caller)
	result.operationID, result.operationStage = operationID, stage
	// observed keeps the committed copy of a record whose outcome the reply must
	// quote; the edit below only exposes a value copy, and the reply is written
	// after this function returns. It is request-local, so no lock is shared.
	var observed *OperationStatus
	wait, err := submit(func(edit *operationEdit) error {
		edit.caller = caller
		key := operationKey(database, operationID)
		record, exists := edit.record(key)
		if !exists || record.State != "in_flight" {
			return ErrOperationExists
		}
		applyOperationOutcome(&record, *result, stage)
		edit.put(key, record)
		if stage == "open_session" && result.code == "outcome_unknown" {
			status := record.OperationStatus
			observed = &status
		}
		if stage == "close_session" && result.code == "" {
			dischargePendingOpenSessions(edit, record)
		}
		return nil
	})
	// The record is ordered for one namespace before the fence drops, so several
	// settlements of one database can share the barrier they wait for afterwards.
	unlock()
	if err == nil && wait != nil {
		err = wait()
	}
	if err != nil {
		finish := result.finish
		*result = gateway.unknown(result.session, permit)
		result.finish, result.operationID, result.operationStage, result.operationDatabase = finish, operationID, stage, database
	} else if observed != nil {
		result.operationObservation = observed
	}
	if result.code == "outcome_unknown" {
		gateway.holdOperation(database, operationID, permit)
	}
	return err
}

// beginDispatchOperation records the durable intent for one worker call. An
// operation that provably cannot commit keeps no intent: its terminal record is
// written once, after the call, by settlePendingOperation. classify reports the
// worker's own verdict on the statement about to be sent, never a guess from its
// text, and may be nil for an action that has no statement to classify.
func (gateway *Gateway) beginDispatchOperation(session *gatewaySession, action, stage, requestID string, writeCursor bool, command frozenCommand, classify func() bool) (string, *pendingOperation, error) {
	if deferredRecordOperation(action) {
		pending := pendingOutcomeOperation(session.database, session.worker.ID, session.upstream, action, stage, requestID, command)
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", action, stage); err != nil {
			return "", nil, err
		}
		return "", pending, nil
	}
	if nonCommittingOperation(action, writeCursor) {
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", action, stage); err != nil {
			return "", nil, err
		}
		return "", pendingDispatchOperation(session, action, stage, requestID, command), nil
	}
	if classify != nil {
		// Classifying is a prepare on the worker, so it is asked only after the
		// admission rule an unregistered operation must pass: an isolated database
		// or an exhausted capacity still refuses the request before any SQL reaches
		// the worker, and that refusal is the same one the committing path reports.
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", action, stage); err != nil {
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

// pendingOutcomeOperation is the pending form of an action whose only durable
// trace is the record written after its call: it skips the intent, but keeps that
// record even when the call succeeds.
func pendingOutcomeOperation(database controller.DatabaseID, worker, session, action, stage, requestID string, command frozenCommand) *pendingOperation {
	return &pendingOperation{database: database, requestID: requestID, action: action, stage: stage, worker: worker, session: session, digest: command.digest(), outcome: true}
}

// settlePendingOperation settles a dispatched operation that was never registered.
// A pending operation is one the platform proved cannot commit: its action is in
// nonCommittingOperation, or the worker classified its statement read-only. Its
// audit row is already the durable completion barrier, so a known outcome writes no
// second record; the sidecar would only put another forced commit between a read and
// its reply. An uncertain outcome is the exception: the record is what lets recovery
// and admission isolate the namespace, so it is still written and quarantined. An
// action in deferredRecordOperation is the other exception: it skips the intent, so
// the record after the call is the only trace of the resource it opened or closed.
//
// deferAfter hands the barrier of a known outcome to the reply-independent account;
// the retirement path passes nil because its retry decision needs the result.
func (gateway *Gateway) settlePendingOperation(pending *pendingOperation, result *gatewayResult, permit *admissionPermit, deferAfter *deferredDurability) {
	_ = gateway.settlePendingOperationUsing(pending, result, permit, func(change func(*operationEdit) error) (func() error, error) {
		return submitOperationRecords(gateway.config.State, change)
	}, deferAfter)
}

func (gateway *Gateway) settlePendingOperationUsing(pending *pendingOperation, result *gatewayResult, permit *admissionPermit, submit func(func(*operationEdit) error) (func() error, error), deferAfter *deferredDurability) error {
	unknown := result.code == "outcome_unknown"
	if !unknown && !pending.outcome {
		return nil
	}
	// The edit may be applied in the audit drain after a deferred reply, while
	// the request is still writing its live result, so it reads a snapshot.
	settled := *result
	// A known outcome may be paid after the reply; a quarantine record may not,
	// because admission answers from it before the caller can be told anything.
	settle := submit
	if deferAfter != nil && !unknown {
		settle = func(change func(*operationEdit) error) (func() error, error) {
			wait, err := submit(change)
			if err != nil || wait == nil {
				return wait, err
			}
			label := "operation " + pending.action + " " + pending.requestID
			return func() error { return deferAfter.settle(label, wait) }, nil
		}
	}
	// observed keeps the committed copy of the record whose outcome the reply must
	// quote: an open whose reply was lost is answered from its own record, so the
	// reply cannot claim a commit the record does not.
	var observed *OperationStatus
	operationID, err := settleNonCommittingOperationUsing(gateway.config.State, pending.database, pending.requestID, "gateway", pending.action, pending.stage, pending.worker, pending.session, pending.digest, func(edit *operationEdit, record *OperationRecord) error {
		applyOperationOutcome(record, settled, pending.stage)
		if pending.stage == "open_session" && unknown {
			status := record.OperationStatus
			observed = &status
		}
		if pending.action == "close" && !unknown {
			dischargePendingOpenSessions(edit, *record)
		}
		return nil
	}, settle)
	result.operationID, result.operationStage = operationID, pending.stage
	if err != nil {
		finish := result.finish
		*result = gateway.unknown(result.session, permit)
		result.finish, result.operationID, result.operationStage, result.operationDatabase = finish, operationID, pending.stage, pending.database
	} else if observed != nil {
		result.operationObservation = observed
	}
	if unknown || err != nil {
		gateway.holdOperation(pending.database, operationID, permit)
	}
	return err
}

// dischargePendingOpenSessions marks the open records a successful close finished.
// It runs inside the close's own edit, so one barrier covers the close and every
// open it discharges, and the same rule decides it whether the close carried an
// intent or only its outcome record.
func dischargePendingOpenSessions(edit *operationEdit, closed OperationRecord) {
	keys := edit.matching(func(_ string, opened OperationRecord) bool {
		return opened.Source == "gateway" && opened.Action == "open" && opened.State == "terminal" && opened.Session == closed.Session && opened.Worker == closed.Worker && opened.Database == closed.Database && opened.CleanupOutcome == "pending"
	})
	for _, openedKey := range keys {
		opened, _ := edit.record(openedKey)
		opened.CleanupOutcome = "completed"
		edit.put(openedKey, opened)
	}
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
		// Opening a session writes no durable data and submits no SQL. Its lost
		// reply is still honestly unknown and ambiguous, but the operation cannot
		// have committed, so it must not isolate the namespace: isolation is for
		// work whose durable effect is in doubt. A worker session the open may have
		// created is reclaimed by the worker's idle sweep, and the record keeps
		// cleanup_outcome=unknown so that residue stays visible.
		if stage == "open_session" && record.Action == "open" {
			record.State, record.BusinessOutcome, record.MayHaveCommitted = "terminal", "unknown", false
			record.CleanupOutcome, record.ExecutionProven = "unknown", false
			return
		}
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
