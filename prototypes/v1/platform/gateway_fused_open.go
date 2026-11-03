package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	controller "peri-loom/controller"
)

// A fused open is the platform-internal request the Hrana adapter makes when a
// pipeline needs its first session for a top-level execute: the gateway asks the
// worker to open the session and run that one guarded read-only statement in the
// same request. The statement travels in the request context, never in a public
// body, so the external protocol is unchanged.
//
// The statement is still read-only by the worker's own admission: a statement
// the engine classifies as writing is refused before it steps, and the platform
// then takes the ordinary committing path with a durable intent. A fused request
// therefore cannot commit, and an unknown answer is classified like an unknown
// open (no isolation, no replay).
type fusedOpenStatement struct {
	SQL       string
	Args      []sqlArgument
	NamedArgs []namedSQLArgument
	WantRows  bool
}

type fusedOpenStatementContextKey struct{}

func withFusedOpenStatement(ctx context.Context, statement fusedOpenStatement) context.Context {
	return context.WithValue(ctx, fusedOpenStatementContextKey{}, statement)
}

func fusedOpenStatementFrom(ctx context.Context) (fusedOpenStatement, bool) {
	statement, ok := ctx.Value(fusedOpenStatementContextKey{}).(fusedOpenStatement)
	return statement, ok
}

// fusedStatementResult is the answer of a fused statement, handed from the open
// call to the statement call that the adapter makes immediately after it. The
// session is the public id the adapter addresses, so a marker can only be
// consumed by the session it was opened for.
type fusedStatementResult struct {
	Session string
	SQL     string
	Reply   json.RawMessage
}

type fusedStatementResultContextKey struct{}

func withFusedStatementResult(ctx context.Context, result fusedStatementResult) context.Context {
	return context.WithValue(ctx, fusedStatementResultContextKey{}, result)
}

func fusedStatementResultFrom(ctx context.Context) (fusedStatementResult, bool) {
	result, ok := ctx.Value(fusedStatementResultContextKey{}).(fusedStatementResult)
	return result, ok
}

// fusedOpenCommand is the private worker command. The shape is validated by the
// upstream before it is sent, so a hand-built body cannot reach a worker with a
// field the protocol does not describe.
func fusedOpenCommand(database controller.DatabaseID, statement fusedOpenStatement) map[string]any {
	args := statement.Args
	if args == nil {
		args = []sqlArgument{}
	}
	namedArgs := statement.NamedArgs
	if namedArgs == nil {
		namedArgs = []namedSQLArgument{}
	}
	return map[string]any{
		"command":    "open_guarded_statement",
		"database":   database,
		"sql":        statement.SQL,
		"args":       args,
		"named_args": namedArgs,
		"want_rows":  statement.WantRows,
	}
}

// fusedOpenCommandValid mirrors the worker request struct. It is deliberately
// as strict as the guarded block validator: an old worker rejects the command
// before reading a session, and that refusal is the only proof that nothing ran.
func fusedOpenCommandValid(operation string, encoded []byte) bool {
	if operation != "open_guarded_statement" {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var command struct {
		Command   string                `json:"command"`
		Database  controller.DatabaseID `json:"database"`
		SQL       string                `json:"sql"`
		Args      []sqlArgument         `json:"args"`
		NamedArgs []namedSQLArgument    `json:"named_args"`
		WantRows  *bool                 `json:"want_rows"`
	}
	if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF ||
		command.Command != operation || command.SQL == "" || command.WantRows == nil {
		return false
	}
	if command.Database.Validate() != nil || !validSQLArguments(command.Args) ||
		len(command.Args)+len(command.NamedArgs) > 999 {
		return false
	}
	for _, argument := range command.NamedArgs {
		if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
			return false
		}
	}
	return true
}

// fusedOpenSupport answers whether this worker parses the fused command. A
// probe failure is cached as unsupported, exactly like the guarded block
// capability: the probe executes no SQL, and an upgraded worker is picked up
// after the negative TTL.
func (gateway *Gateway) fusedOpenSupported(ctx context.Context, worker controller.Worker) bool {
	key := "fused\x00" + guardedBatchCapabilityKey(worker)
	if supported, known := gateway.fusedOpen.lookup(key, time.Now()); known {
		return supported
	}
	supported := gateway.probeFusedOpen(ctx, worker)
	if ctx.Err() == nil {
		gateway.fusedOpen.remember(key, supported, time.Now())
	}
	return supported
}

func (gateway *Gateway) probeFusedOpen(ctx context.Context, worker controller.Worker) bool {
	requestID, err := controller.RandomID()
	if err != nil {
		return false
	}
	var reply json.RawMessage
	if err := gateway.config.Upstream.Call(ctx, worker, requestID, map[string]any{"command": "capabilities"}, &reply); err != nil {
		return false
	}
	var capabilities struct {
		Protocol *int `json:"readonly_batch_protocol"`
		Fused    *int `json:"open_guarded_statement"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&capabilities) != nil || decoder.Decode(new(any)) != io.EOF ||
		capabilities.Fused == nil || *capabilities.Fused != 1 {
		return false
	}
	return true
}

// markFusedOpenUnsupported records a worker that refused the command after the
// cache said it would parse it. The refusal proves nothing ran, so this only
// changes which call the next open makes.
func (gateway *Gateway) markFusedOpenUnsupported(worker controller.Worker) {
	gateway.fusedOpen.remember("fused\x00"+guardedBatchCapabilityKey(worker), false, time.Now())
}

// fusedStatementAnswer is the statement part of a fused open reply. The three
// statuses mirror the guarded statement's own outcomes: one statement result, a
// pre-execution refusal that needs an intent, or a settled/unsettled failure.
type fusedStatementAnswerKind int

const (
	fusedStatementOK fusedStatementAnswerKind = iota
	fusedStatementRequiresIntent
	fusedStatementError
)

type fusedStatementAnswer struct {
	kind               fusedStatementAnswerKind
	result             json.RawMessage
	reason             guardedIntentReason
	message            string
	executionCompleted *bool
}

type fusedStatementReply struct {
	Status             string               `json:"status"`
	Reason             *guardedIntentReason `json:"reason"`
	Result             json.RawMessage      `json:"result"`
	Error              *string              `json:"error"`
	ExecutionCompleted *bool                `json:"execution_completed"`
}

// parseFusedStatementAnswer accepts only the complete shapes the worker emits.
// A partial answer is not evidence, so it is rejected rather than repaired.
func parseFusedStatementAnswer(reply json.RawMessage) (fusedStatementAnswer, bool) {
	var parsed fusedStatementReply
	decoder := json.NewDecoder(bytes.NewReader(reply))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&parsed) != nil || decoder.Decode(new(any)) != io.EOF {
		return fusedStatementAnswer{}, false
	}
	switch parsed.Status {
	case "ok":
		if parsed.Reason != nil || parsed.Error != nil || parsed.ExecutionCompleted != nil ||
			!validStatementResult(parsed.Result) {
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{kind: fusedStatementOK, result: parsed.Result}, true
	case "requires_intent":
		if parsed.Reason == nil || parsed.Result != nil || parsed.Error != nil || parsed.ExecutionCompleted != nil {
			return fusedStatementAnswer{}, false
		}
		switch *parsed.Reason {
		case guardedEngineWritable, guardedTransactionContext, guardedConnectionControl:
		default:
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{kind: fusedStatementRequiresIntent, reason: *parsed.Reason}, true
	case "error":
		if parsed.Error == nil || *parsed.Error == "" || parsed.Result != nil || parsed.Reason != nil {
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{
			kind:               fusedStatementError,
			message:            *parsed.Error,
			executionCompleted: parsed.ExecutionCompleted,
		}, true
	default:
		return fusedStatementAnswer{}, false
	}
}

// fusedGuardedStatement completes one statement from the reply the open already
// received. Every check the single-statement path performs still runs here: the
// admission rule, the result budget, the intent-before-dispatch boundary for a
// writable statement, and the same evidence classification for a failure. Only
// the worker round trip is gone.
func (gateway *Gateway) fusedGuardedStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID, stage string, key committingStatementKey, frozen frozenCommand, fused fusedStatementResult, permit *admissionPermit, quota Quotas) gatewayResult {
	if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", "statement", stage); err != nil {
		return operationAdmissionFailure(err)
	}
	pending := pendingDispatchOperation(session, "statement", stage, requestID, frozen)
	answer, valid := parseFusedStatementAnswer(fused.Reply)
	if !valid {
		// The open handler accepted this payload before publishing it, so this
		// branch is defensive: it still refuses to replay a statement whose
		// answer cannot be read.
		result := gateway.unknown(session, permit)
		result.operationDeferred = pending
		return result
	}
	switch answer.kind {
	case fusedStatementOK:
		if len(answer.result) > quota.MaxResultBytes {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget, operationDeferred: pending}
		}
		return gatewayResult{value: answer.result, dispatched: true, validatedResult: true, operationDeferred: pending}
	case fusedStatementRequiresIntent:
		if answer.reason == guardedEngineWritable {
			gateway.committingStatements.remember(key)
		}
		return gateway.committingStatement(ctx, session, binding, requestID, stage, frozen, permit, quota)
	default:
		present := answer.executionCompleted != nil
		completed := present && *answer.executionCompleted
		result := gateway.upstreamFailure(sqlWorkerFailureFromReply(present, completed, answer.message), session, permit)
		if result.code == "outcome_unknown" {
			result.operationDeferred = pending
		}
		return result
	}
}
