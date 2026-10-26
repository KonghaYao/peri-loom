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
func (gateway *Gateway) guardedStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID, stage string, key committingStatementKey, command map[string]any, frozen frozenCommand, permit *admissionPermit, quota Quotas) gatewayResult {
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
	reason, statementResult := guardedStatementReply(reply)
	if reason != "" {
		// SELECT inside a transaction needs an intent for that session only.
		// Remembering that refusal would make every other session pay for it.
		if reason == guardedEngineWritable {
			gateway.committingStatements.remember(key)
		}
		return gateway.committingStatement(ctx, session, binding, requestID, stage, frozen, permit, quota)
	}
	if !statementResult {
		result := gateway.unknown(session, permit)
		result.operationDeferred = pending
		return result
	}
	return gatewayResult{value: reply, dispatched: true, operationDeferred: pending}
}

type guardedIntentReason string

const (
	guardedEngineWritable     guardedIntentReason = "engine_writable"
	guardedTransactionContext guardedIntentReason = "transaction_context"
	guardedConnectionControl  guardedIntentReason = "connection_control"
)

// A refusal needs both the exact flag and a known reason. An old, partial or
// extended reply is not proof that SQL never ran, so it cannot be replayed.
func guardedStatementReply(reply json.RawMessage) (reason guardedIntentReason, statementResult bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(reply, &fields) != nil {
		return "", false
	}
	if raw, exists := fields["requires_intent"]; exists {
		if len(fields) != 2 || string(raw) != "true" || json.Unmarshal(fields["reason"], &reason) != nil {
			return "", false
		}
		switch reason {
		case guardedEngineWritable, guardedTransactionContext, guardedConnectionControl:
			return reason, false
		default:
			return "", false
		}
	}
	return "", validStatementResult(reply)
}

// Cached and freshly proven writes share the same intent-before-dispatch
// boundary. Cache hits must not skip durable registration or outcome tracking.
func (gateway *Gateway) committingStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID, stage string, frozen frozenCommand, permit *admissionPermit, quota Quotas) gatewayResult {
	leaveIntent := requestPhaseSpan(ctx, requestPhaseIntent)
	operationID, _, err := gateway.beginDispatchOperation(session, "statement", stage, requestID, false, frozen, nil)
	leaveIntent()
	if err != nil {
		return operationAdmissionFailure(err)
	}
	result := gateway.dispatchStatement(ctx, session, binding, requestID, frozen, quota, permit)
	result.operationID, result.operationStage, result.operationDatabase = operationID, stage, session.database
	result.operationPending = operationID != ""
	return result
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
