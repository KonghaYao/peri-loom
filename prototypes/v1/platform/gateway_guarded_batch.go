package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	controller "peri-loom/controller"
)

// A worker capability is a property of one worker binary at one address. The
// positive answer lives as long as this gateway does; a negative answer is
// re-asked after the TTL so an upgraded worker is picked up without a restart.
// A command the worker refuses after a positive answer (a downgraded binary) is
// still handled: the refusal proves nothing ran, and the block falls back to
// single statements.
const guardedBatchCapabilityTTL = 60 * time.Second

type workerCapability struct {
	// level is the private command protocol the worker answered, zero when it
	// refused the command. A positive level is a property of that binary; a
	// zero level is re-asked after the TTL.
	level   int
	checked time.Time
}

type readonlyBatchCapabilityCache struct {
	mutex   sync.Mutex
	entries map[string]workerCapability
}

func (cache *readonlyBatchCapabilityCache) lookup(key string, now time.Time) (bool, bool) {
	level, known := cache.lookupLevel(key, now)
	return level >= 1, known
}

func (cache *readonlyBatchCapabilityCache) lookupLevel(key string, now time.Time) (int, bool) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	entry, found := cache.entries[key]
	if !found {
		return 0, false
	}
	if entry.level == 0 && now.Sub(entry.checked) >= guardedBatchCapabilityTTL {
		return 0, false
	}
	return entry.level, true
}

func (cache *readonlyBatchCapabilityCache) remember(key string, supported bool, now time.Time) {
	level := 0
	if supported {
		level = 1
	}
	cache.rememberLevel(key, level, now)
}

func (cache *readonlyBatchCapabilityCache) rememberLevel(key string, level int, now time.Time) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if cache.entries == nil {
		cache.entries = make(map[string]workerCapability)
	}
	cache.entries[key] = workerCapability{level: level, checked: now}
}

func guardedBatchCapabilityKey(worker controller.Worker) string {
	return worker.ID + "\x00" + worker.Address
}

// hranaBatchWorker returns the worker and the writer profile of a Hrana
// session's native binding. The fields are set once when the session opens, so
// reading them under the gateway lock is enough to plan a block.
func (gateway *Gateway) hranaBatchWorker(public string) (controller.Worker, bool, bool) {
	gateway.mu.Lock()
	session := gateway.sessions[public]
	gateway.mu.Unlock()
	if session == nil {
		return controller.Worker{}, false, false
	}
	return session.worker, session.writerProtocol, true
}

// readonlyBatchSupported answers whether this worker parses the block command.
// A probe failure is cached as unsupported, never as a statement outcome: the
// probe cannot execute SQL.
func (gateway *Gateway) readonlyBatchSupported(ctx context.Context, worker controller.Worker) bool {
	key := guardedBatchCapabilityKey(worker)
	if supported, known := gateway.readonlyBatch.lookup(key, time.Now()); known {
		return supported
	}
	supported := gateway.probeReadonlyBatch(ctx, worker)
	// A cancelled request fails the probe for a reason that says nothing about
	// the worker, so it does not become a negative answer.
	if ctx.Err() == nil {
		gateway.readonlyBatch.remember(key, supported, time.Now())
	}
	return supported
}

func (gateway *Gateway) probeReadonlyBatch(ctx context.Context, worker controller.Worker) bool {
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
		capabilities.Protocol == nil || *capabilities.Protocol != 1 {
		return false
	}
	return true
}

// markReadonlyBatchUnsupported records a worker that refused the command after
// the cache said it would parse it, so the remaining blocks of this pipeline and
// the next TTL of traffic stay on single statements.
func (gateway *Gateway) markReadonlyBatchUnsupported(worker controller.Worker) {
	gateway.readonlyBatch.remember(guardedBatchCapabilityKey(worker), false, time.Now())
}

// reserveBatchCompletions holds one completion slot per entry of a planned
// block before the first entry is dispatched, so a block can never outrun the
// audit capacity it will need. A sink without admission support still batches;
// it reserves nothing because it records through the plain Record contract.
func (gateway *Gateway) reserveBatchCompletions(count int) ([]AuditReservation, bool) {
	admission, ok := gateway.config.Audit.(AuditAdmission)
	if !ok {
		return nil, true
	}
	reservations := make([]AuditReservation, 0, count)
	for index := 0; index < count; index++ {
		reservation, err := admission.ReserveCompletion()
		if err != nil {
			for _, held := range reservations {
				held.Release()
			}
			return nil, false
		}
		reservations = append(reservations, reservation)
	}
	return reservations, true
}

// reserveCompletionFor prefers the slot the block reserved for this entry. Only
// a statement consumes a block slot: a probe inside the same pipeline entry is
// not the client action it answers, so it keeps its own admission path.
func (gateway *Gateway) reserveCompletionFor(ctx context.Context, action string, admission AuditAdmission) (AuditReservation, error) {
	if action == "statement" && !hranaProbeRequest(ctx, action) {
		if step, ok := guardedBatchStepFrom(ctx); ok {
			if reservation, claimed := step.batch.claimReservation(step.index); claimed {
				return reservation, nil
			}
		}
	}
	return admission.ReserveCompletion()
}

// guardedBatchCommand is the one logical command of a block. It is assembled
// before the durable record is registered, so the digest covers these bytes and
// only these bytes reach the worker.
func guardedBatchCommand(session *gatewaySession, batch *guardedBatch) map[string]any {
	statements := make([]map[string]any, len(batch.statements))
	for index, statement := range batch.statements {
		statements[index] = map[string]any{
			"sql":        statement.sql,
			"args":       statement.args,
			"named_args": statement.namedArgs,
			"want_rows":  statement.wantRows,
		}
	}
	return map[string]any{
		"command":          "guarded_readonly_batch",
		"session":          session.upstream,
		"statements":       statements,
		"max_result_bytes": batch.maxResultBytes,
	}
}

type guardedBatchReply struct {
	Items []guardedBatchReplyItem `json:"items"`
	Next  *int                    `json:"next"`
}

type guardedBatchReplyItem struct {
	Index              *int                 `json:"index"`
	Status             string               `json:"status"`
	Result             json.RawMessage      `json:"result"`
	Reason             *guardedIntentReason `json:"reason"`
	Error              *string              `json:"error"`
	ExecutionCompleted *bool                `json:"execution_completed"`
}

// parseGuardedBatchReply turns the worker's block answer into the items the
// per-entry dispatch consumes. The reply is accepted only in its complete form:
// contiguous indexes, `next` equal to the number of decided entries, and one
// settled shape per entry. Anything else is an unknown block, not a guess.
func parseGuardedBatchReply(reply json.RawMessage, statements []guardedBatchStatement) ([]guardedBatchItem, int, bool) {
	var parsed guardedBatchReply
	decoder := json.NewDecoder(bytes.NewReader(reply))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&parsed) != nil || decoder.Decode(new(any)) != io.EOF || parsed.Next == nil {
		return nil, 0, false
	}
	next := *parsed.Next
	if next < 0 || next > len(statements) || len(parsed.Items) != next {
		return nil, 0, false
	}
	items := make([]guardedBatchItem, len(parsed.Items))
	for position, item := range parsed.Items {
		if item.Index == nil || *item.Index != position {
			return nil, 0, false
		}
		switch item.Status {
		case "ok":
			if item.Reason != nil || item.Error != nil || item.ExecutionCompleted != nil {
				return nil, 0, false
			}
			reason, statement, autocommit, autocommitKnown := guardedStatementReply(item.Result)
			if reason != "" || !statement {
				return nil, 0, false
			}
			items[position] = guardedBatchItem{status: guardedBatchStatusOK, result: item.Result, autocommit: autocommit, autocommitKnown: autocommitKnown}
		case "requires_intent":
			if item.Reason == nil || item.Result != nil || item.Error != nil || item.ExecutionCompleted != nil {
				return nil, 0, false
			}
			switch *item.Reason {
			case guardedEngineWritable, guardedTransactionContext, guardedConnectionControl:
			default:
				return nil, 0, false
			}
			items[position] = guardedBatchItem{status: guardedBatchStatusRequiresIntent, reason: *item.Reason}
		case "error":
			if item.Error == nil || *item.Error == "" || item.Result != nil || item.Reason != nil {
				return nil, 0, false
			}
			present := item.ExecutionCompleted != nil
			completed := present && *item.ExecutionCompleted
			items[position] = guardedBatchItem{
				status:  guardedBatchStatusError,
				failure: sqlWorkerFailureFromReply(present, completed, *item.Error),
			}
		default:
			return nil, 0, false
		}
	}
	return items, next, true
}

// dispatchGuardedBatch sends the block once. Every way it can fail is recorded
// on the block: a refusal that proves nothing ran leaves the block undelivered
// so the first entry takes the single-statement path, while an ambiguous answer
// is stored and returned for every later entry instead of re-dispatching.
func (gateway *Gateway) dispatchGuardedBatch(ctx context.Context, batch *guardedBatch, session *gatewaySession, binding controller.Binding, requestID, stage string, permit *admissionPermit) {
	frozen, err := freezeCommand(guardedBatchCommand(session, batch))
	if err != nil || len(frozen.bytes()) > guardedBatchRequestBodyLimit {
		// The planner stays under the limit, so this only guards a future caller.
		// Nothing was sent and the first entry runs on its own.
		return
	}
	pending := pendingDispatchOperation(session, "statement", stage, requestID, frozen)
	probeID, err := controller.RandomID()
	if err != nil {
		batch.broken = true
		batch.brokenResult = gatewayFailure(503, "upstream_unavailable")
		batch.brokenResult.dispatched = false
		return
	}
	var reply json.RawMessage
	leaveWorker := requestPhaseSpan(ctx, requestPhaseWorker)
	leaveNative := requestPhaseSpan(ctx, requestPhaseNative)
	err = gateway.sendSession(ctx, session, binding, probeID, frozen, &reply)
	leaveNative()
	leaveWorker()
	batch.mutex.Lock()
	batch.dispatched = true
	batch.pending = pending
	batch.mutex.Unlock()
	if err != nil {
		var failure *SQLRPCError
		if errors.As(err, &failure) && failure.unsupportedCommand {
			// The worker rejected the command before reading a session, so no
			// statement ran. Remember the refusal and let the entry run alone.
			gateway.markReadonlyBatchUnsupported(session.worker)
			batch.mutex.Lock()
			batch.dispatched = false
			batch.next = 0
			batch.mutex.Unlock()
			return
		}
		result := gateway.upstreamFailure(err, session, permit)
		result.operationDeferred = pending
		batch.mutex.Lock()
		batch.broken = true
		batch.brokenResult = result
		batch.mutex.Unlock()
		return
	}
	items, next, valid := parseGuardedBatchReply(reply, batch.statements)
	if !valid {
		result := gateway.unknown(session, permit)
		result.operationDeferred = pending
		batch.mutex.Lock()
		batch.broken = true
		batch.brokenResult = result
		batch.mutex.Unlock()
		return
	}
	batch.mutex.Lock()
	batch.items, batch.next = items, next
	batch.mutex.Unlock()
}

// guardedBatchStatement answers one entry of a planned block. Every entry still
// passes the caller's full dispatch checks; only the worker call is shared. A
// false return means this entry is not covered by a decided block item and the
// caller must run the single-statement path.
func (gateway *Gateway) guardedBatchStatement(ctx context.Context, step guardedBatchStep, key committingStatementKey, session *gatewaySession, binding controller.Binding, requestID, stage, sql string, frozen frozenCommand, permit *admissionPermit, quota Quotas) (gatewayResult, bool) {
	if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", "statement", stage); err != nil {
		return operationAdmissionFailure(err), true
	}
	batch := step.batch
	batch.mutex.Lock()
	dispatched := batch.dispatched
	batch.mutex.Unlock()
	if !dispatched {
		if step.index != 0 {
			return gatewayResult{}, false
		}
		gateway.dispatchGuardedBatch(ctx, batch, session, binding, requestID, stage, permit)
		batch.mutex.Lock()
		dispatched = batch.dispatched
		batch.mutex.Unlock()
		if !dispatched {
			return gatewayResult{}, false
		}
	}
	batch.mutex.Lock()
	if batch.broken {
		// A broken block answer proves nothing about the entries it covered, so
		// the session loses the stateless proof.
		session.pristine = false
		result := batch.brokenResult
		batch.mutex.Unlock()
		return result, true
	}
	next := batch.next
	pending := batch.pending
	if step.index < 0 || step.index >= next || step.index >= len(batch.items) {
		batch.mutex.Unlock()
		return gatewayResult{}, false
	}
	item := batch.items[step.index]
	batch.mutex.Unlock()
	if step.index >= len(batch.statements) || batch.statements[step.index].sql != sql {
		// A consumed entry must be the statement the block planned. A mismatch
		// would misattribute a settled result, so the answer stays unknown.
		result := gateway.unknown(session, permit)
		result.operationDeferred = pending
		return result, true
	}
	switch item.status {
	case guardedBatchStatusOK:
		session.noteStatementAutocommit(item.autocommit, item.autocommitKnown)
		if len(item.result) > quota.MaxResultBytes {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget, operationDeferred: pending}, true
		}
		return gatewayResult{value: item.result, dispatched: true, validatedResult: true, operationDeferred: pending}, true
	case guardedBatchStatusRequiresIntent:
		if item.reason == guardedEngineWritable {
			gateway.committingStatements.remember(key)
		}
		return gateway.committingStatement(ctx, session, binding, requestID, stage, frozen, permit, quota), true
	case guardedBatchStatusError:
		// A settled block error ran something whose autocommit flag the block
		// answer does not carry, so the session keeps the synchronous close.
		session.pristine = false
		result := gateway.upstreamFailure(item.failure, session, permit)
		if result.code == "outcome_unknown" {
			result.operationDeferred = pending
		}
		return result, true
	}
	result := gateway.unknown(session, permit)
	result.operationDeferred = pending
	return result, true
}

// guardedBatchCommandValid is the worker-facing shape the SQL upstream accepts
// for the two private block commands. It mirrors the Rust request structs so a
// hand-built body cannot reach a worker the protocol cannot describe.
func guardedBatchCommandValid(operation string, encoded []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if operation == "capabilities" {
		var command struct {
			Command string `json:"command"`
		}
		if decoder.Decode(&command) != nil || command.Command != operation || decoder.Decode(new(any)) != io.EOF {
			return false
		}
		return true
	}
	if operation != "guarded_readonly_batch" {
		return false
	}
	var command struct {
		Command    string `json:"command"`
		Session    string `json:"session"`
		Statements []struct {
			SQL       string             `json:"sql"`
			Args      []sqlArgument      `json:"args"`
			NamedArgs []namedSQLArgument `json:"named_args"`
			WantRows  *bool              `json:"want_rows"`
		} `json:"statements"`
		MaxResultBytes json.Number `json:"max_result_bytes"`
	}
	if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF ||
		command.Command != operation || !upstreamSessionID(command.Session) ||
		len(command.Statements) == 0 || len(command.Statements) > guardedBatchSize ||
		command.MaxResultBytes == "" {
		return false
	}
	if maximum, err := strconv.ParseUint(string(command.MaxResultBytes), 10, 63); err != nil || maximum > 1<<62 {
		return false
	}
	for _, statement := range command.Statements {
		if statement.SQL == "" || statement.WantRows == nil {
			return false
		}
		if !validSQLArguments(statement.Args) {
			return false
		}
		for _, argument := range statement.NamedArgs {
			if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
				return false
			}
		}
	}
	return true
}
