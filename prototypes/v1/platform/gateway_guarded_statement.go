package platform

import (
	"context"
	"encoding/json"

	controller "peri-loom/controller"
)

// guardedStatement executes one statement through the worker's guarded
// read-only admission. The worker prepares, binds and steps the same statement,
// and re-asks the engine whether it may write before every step - including the
// step after an automatic reprepare - so a schema change cannot turn the read
// that was classified into a write that runs. A read-only statement is answered
// by this one call: it needs no durable intent and no second dispatch.
//
// A requires_intent reply is the only outcome that moves the request to the
// committing path. It means the worker refused the statement before running any
// program the engine classified as writing, so the platform writes the durable
// intent only then. A transport error is not that proof: it keeps the existing
// unknown/operation semantics and is never replayed as a statement.
func (gateway *Gateway) guardedStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID, stage string, command map[string]any, frozen frozenCommand, permit *admissionPermit, quota Quotas) gatewayResult {
	if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway"); err != nil {
		return operationAdmissionFailure(err)
	}
	guarded := make(map[string]any, len(command))
	for key, value := range command {
		guarded[key] = value
	}
	guarded["command"] = "guarded_readonly_statement"
	guardedCommand, err := freezeCommand(guarded)
	if err != nil {
		return operationAdmissionFailure(err)
	}
	probeID, err := controller.RandomID()
	if err != nil {
		return gatewayFailure(503, "upstream_unavailable")
	}
	// The guarded call provably cannot commit: a writable statement is refused
	// before it steps. Its record is therefore written only if the call turns
	// ambiguous, by the same pending dispatch the non-committing operations use.
	pending := pendingDispatchOperation(session, "statement", stage, requestID, guardedCommand)
	var reply json.RawMessage
	leaveWorker := requestPhaseSpan(ctx, requestPhaseWorker)
	err = gateway.sendSession(ctx, session, binding, probeID, guardedCommand, &reply)
	leaveWorker()
	if err != nil {
		result := gateway.upstreamFailure(err, session, permit)
		result.operationDeferred = pending
		return result
	}
	if len(reply) > quota.MaxResultBytes {
		return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget, operationDeferred: pending}
	}
	requiresIntent, statementResult := guardedStatementReply(reply)
	if requiresIntent {
		operationID, _, err := gateway.beginDispatchOperation(session, "statement", stage, requestID, false, frozen, nil)
		if err != nil {
			return operationAdmissionFailure(err)
		}
		result := gateway.dispatchStatement(ctx, session, binding, requestID, frozen, quota, permit)
		result.operationID, result.operationStage, result.operationDatabase = operationID, stage, session.database
		result.operationPending = operationID != ""
		return result
	}
	if !statementResult {
		result := gateway.unknown(session, permit)
		result.operationDeferred = pending
		return result
	}
	return gatewayResult{value: reply, dispatched: true, operationDeferred: pending}
}

// guardedStatementReply separates the only two replies the guarded call may
// return. A requires_intent reply must be exactly that flag: an unexpected
// field could be a statement result we failed to understand, and guessing would
// let a write through without its durable intent.
func guardedStatementReply(reply json.RawMessage) (requiresIntent, statementResult bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(reply, &fields) != nil {
		return false, false
	}
	if raw, exists := fields["requires_intent"]; exists {
		return len(fields) == 1 && string(raw) == "true", false
	}
	return false, validStatementResult(reply)
}

// dispatchStatement sends the frozen committing statement and validates its
// reply. Both the proven-write path and the guarded path's requires_intent arm
// use it, so the reply contract cannot drift between them.
func (gateway *Gateway) dispatchStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID string, frozen frozenCommand, quota Quotas, permit *admissionPermit) gatewayResult {
	var reply json.RawMessage
	if err := gateway.callSession(ctx, session, binding, requestID, frozen, &reply); err != nil {
		return gateway.upstreamFailure(err, session, permit)
	}
	if len(reply) > quota.MaxResultBytes {
		return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget}
	}
	if !validStatementResult(reply) {
		return gateway.unknown(session, permit)
	}
	return gatewayResult{value: reply, dispatched: true}
}
