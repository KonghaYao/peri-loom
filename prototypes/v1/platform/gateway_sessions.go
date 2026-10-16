package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	controller "peri-loom/controller"
)

func (gateway *Gateway) open(ctx context.Context, principal Principal, database controller.DatabaseID, requestID string, permit *admissionPermit, quota Quotas) (result gatewayResult) {
	if gateway.authorize(principal, database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	// 持久隔离的原因优先于 placement 故障，避免客户端把 Unknown 误当成可重试的路由问题。
	if err := readOperationState(gateway.config.State, func(state State) error {
		if _, isolated := isolatedOperation(state, database, false); isolated {
			return ErrDatabaseQuarantined
		}
		return nil
	}); err != nil {
		return operationAdmissionFailure(err)
	}
	worker, binding, err := gateway.route(database)
	if err != nil {
		return gatewayFailure(409, "placement_unavailable")
	}
	if !principalBindingValid(principal, binding) {
		return gatewayFailure(403, "token_binding_mismatch")
	}
	if principal.Role == "read_only" && binding.Role != "read_only" {
		return gatewayFailure(403, "readonly_placement_required")
	}
	if gateway.authorize(principal, database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	openCommand, err := freezeCommand(map[string]any{"command": "open_session", "database": database})
	if err != nil {
		return operationAdmissionFailure(err)
	}
	operationID, err := registerOperation(gateway.config.State, database, requestID, "gateway", "open", "open_session", worker.ID, "", openCommand)
	if err != nil {
		return operationAdmissionFailure(err)
	}
	defer func() {
		result.operationID, result.operationStage, result.operationDatabase = operationID, "open_session", database
		result.operationPending = true
	}()
	var reply struct {
		Session  string `json:"session"`
		Readonly *bool  `json:"readonly"`
	}
	var writerState *SessionWriterState
	writerProtocol := gateway.admission.eligible(quota)
	if writerProtocol {
		probeID, probeErr := controller.RandomID()
		if probeErr != nil {
			return gatewayFailure(503, "upstream_unavailable")
		}
		if probeErr = gateway.config.Upstream.(WriterSQLUpstream).WriterProfile(ctx, worker, probeID); probeErr != nil {
			return gatewayFailure(503, "writer_profile_unavailable")
		}
		currentWorker, currentBinding, routeErr := gateway.route(database)
		if routeErr != nil || !sameGatewayBinding(binding, currentBinding) || worker.Address != currentWorker.Address {
			return gatewayFailure(409, "placement_unavailable")
		}
		if !principalBindingValid(principal, currentBinding) || gateway.authorize(principal, database) != nil {
			return gatewayFailure(403, "forbidden")
		}
		binding = currentBinding
		ctx = bindSQLOpenObservation(ctx, gateway.config.OpenObserver, requestID)
		writerState, err = gateway.config.Upstream.(WriterSQLUpstream).CallWriter(ctx, worker, requestID, openCommand, &reply)
	} else {
		ctx = bindSQLOpenObservation(ctx, gateway.config.OpenObserver, requestID)
		err = gateway.config.Upstream.Call(ctx, worker, requestID, openCommand, &reply)
	}
	if err != nil {
		return observeSQLOpenResult(ctx, requestID, sqlOpenUpstreamResult, gateway.upstreamFailure(err, nil, permit), err)
	}
	if !upstreamSessionID(reply.Session) || reply.Readonly == nil || *reply.Readonly != (binding.Role == "read_only") {
		return observeSQLOpenResult(ctx, requestID, sqlOpenReply, gateway.unknown(nil, permit), nil)
	}
	identifier, err := controller.RandomID()
	if err != nil {
		return observeSQLOpenResult(ctx, requestID, sqlOpenPublicID, gateway.unknown(nil, permit), nil)
	}
	session := &gatewaySession{public: identifier, upstream: reply.Session, worker: worker, binding: binding, database: database, project: principal.Project, subject: principal.Subject, keyID: principal.KeyID, writerProtocol: writerProtocol, hrana: ctx.Value(hranaSessionContextKey{}) != nil, lastUsed: time.Now()}
	if writerProtocol && (!session.acceptWriterState(writerState, binding) || writerState.Owner) {
		return observeSQLOpenResult(ctx, requestID, sqlOpenWriterBinding, gateway.unknown(nil, permit), nil)
	}
	session.mu.Lock()
	gateway.mu.Lock()
	if _, exists := gateway.sessions[identifier]; exists {
		gateway.mu.Unlock()
		session.mu.Unlock()
		return observeSQLOpenResult(ctx, requestID, sqlOpenCollision, gateway.unknown(nil, permit), nil)
	}
	gateway.sessions[identifier] = session
	gateway.mu.Unlock()
	permit.keepSession = true
	return gatewayResult{value: map[string]any{"session": identifier, "readonly": *reply.Readonly}, dispatched: true, session: session, finish: func() {
		session.lastUsed = time.Now()
		session.mu.Unlock()
	}}
}

func (gateway *Gateway) dispatch(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool) gatewayResult {
	gateway.mu.Lock()
	session := gateway.sessions[request.Session]
	gateway.mu.Unlock()
	if session == nil || session.subject != principal.Subject || session.project != principal.Project || session.keyID != principal.KeyID {
		return gatewayFailure(403, "invalid_session")
	}
	if !session.mu.TryLock() {
		return gatewayFailure(409, "session_busy")
	}
	if permit == nil {
		if failure := gateway.writerDispatchGuard(principal, session, action); failure.code != "" {
			session.mu.Unlock()
			return failure
		}
		var ok bool
		if action == "close" {
			permit, ok = gateway.admission.reserve(principal.Project, quota, false, true)
		} else {
			permit, ok = gateway.admission.reserveWriter(principal.Project, quota, session.writerProtocol && session.writerKnown && session.writerOwner)
		}
		if !ok {
			session.mu.Unlock()
			return gatewayFailure(429, "quota_exceeded")
		}
	}
	result := gateway.dispatchLocked(ctx, principal, request, requestID, action, quota, permit, ndjson, session)
	result.permit = permit
	result.session = session
	result.finish = func() {
		session.lastUsed = time.Now()
		session.mu.Unlock()
	}
	return result
}

func (gateway *Gateway) dispatchLocked(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool, session *gatewaySession) (result gatewayResult) {
	currentBinding := session.binding
	// Authorization is verified again after routing, before the worker call.
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	if session.closed {
		return gatewayFailure(403, "invalid_session")
	}
	if session.poisoned {
		return gateway.isolatedSessionFailure(session.database)
	}
	// The placement store is read once per dispatch and the same snapshot backs
	// every binding check below: re-reading it only queues the request behind
	// placement writers, and both checks must agree on one snapshot to be
	// meaningful.
	var (
		worker   controller.Worker
		binding  controller.Binding
		routed   bool
		routeErr error
	)
	lookup := func() error {
		if !routed {
			routed = true
			worker, binding, routeErr = gateway.route(session.database)
		}
		return routeErr
	}
	if principal.Database != nil {
		if err := lookup(); err != nil {
			return gatewayFailure(409, "placement_unavailable")
		}
		if !principalBindingValid(principal, binding) {
			return gatewayFailure(403, "token_binding_mismatch")
		}
	}
	if action != "close" {
		if session.moved {
			return gatewayFailure(409, "session_moved")
		}
		if err := lookup(); err != nil {
			return gatewayFailure(409, "placement_unavailable")
		}
		if !sameGatewayBinding(session.binding, binding) || worker.Address != session.worker.Address {
			session.moved = true
			return gatewayFailure(409, "session_moved")
		}
		currentBinding = binding
		if principal.Role == "read_only" && binding.Role != "read_only" {
			return gatewayFailure(403, "readonly_placement_required")
		}
	}
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	// The worker command of this dispatch is assembled here, completely, before
	// anything durable names it: the record registered below binds the bytes this
	// switch produces, so no later branch may rewrite a field the digest covers.
	command := map[string]any{"command": action, "session": session.upstream}
	switch action {
	case "cursor_open":
		command["command"] = "open_read_cursor"
		if request.WriteCursor {
			command["command"] = "open_write_cursor"
		}
		args := request.Args
		if args == nil {
			args = []sqlArgument{}
		}
		namedArgs := request.NamedArgs
		if namedArgs == nil {
			namedArgs = []namedSQLArgument{}
		}
		command["args"] = args
		command["named_args"] = namedArgs
	case "cursor_fetch":
		command["command"] = "fetch_read_cursor"
		if request.WriteCursor {
			command["command"] = "fetch_write_cursor"
		}
		command["max_rows"] = request.MaxRows
	case "cursor_close":
		command["command"] = "close_read_cursor"
		if request.WriteCursor {
			command["command"] = "close_write_cursor"
		}
	case "close":
		command["command"] = "close_session"
	case "session_state":
		command["command"] = "session_autocommit"
	case "statement":
		args := request.Args
		if args == nil {
			args = []sqlArgument{}
		}
		namedArgs := request.NamedArgs
		if namedArgs == nil {
			namedArgs = []namedSQLArgument{}
		}
		command["args"] = args
		command["named_args"] = namedArgs
		// The row projection is always materialized as its effective value: the
		// worker reads an absent want_rows as true, so omitting it would let one
		// logical command carry two digests.
		command["want_rows"] = request.WantRows == nil || *request.WantRows
	}
	stage := "tool_rpc"
	if action == "close" {
		stage = "close_session"
	} else if action == "execute" || action == "query" || action == "statement" || action == "describe" || action == "cursor_open" {
		command["sql"] = request.SQL
	}
	frozen, err := freezeCommand(command)
	if err != nil {
		return operationAdmissionFailure(err)
	}
	// A platform probe cannot commit and is not a client action, so it takes no
	// operation record; the client request that needed the answer records its
	// own. The read-only admission check still refuses an isolated database.
	if hranaProbeRequest(ctx, action) {
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway"); err != nil {
			return operationAdmissionFailure(err)
		}
	} else {
		operationID, pending, err := gateway.beginDispatchOperation(session, action, stage, requestID, request.WriteCursor, frozen)
		if err != nil {
			return operationAdmissionFailure(err)
		}
		defer func() {
			result.operationID, result.operationStage, result.operationDatabase = operationID, stage, session.database
			result.operationPending, result.operationDeferred = operationID != "", pending
		}()
	}
	if action == "close" {
		var reply struct {
			Closed bool `json:"closed"`
		}
		err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Closed {
			return gateway.unknown(session, permit)
		}
		gateway.removeSession(session)
		return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
	}
	if action == "cursor_open" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes || !validCursorOpenResult(reply) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true}
	}
	if action == "cursor_fetch" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes || !validCursorFetchResponse(reply, request.MaxRows, request.WriteCursor) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true}
	}
	if action == "cursor_close" {
		var reply struct {
			Closed bool `json:"closed"`
		}
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Closed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
	}
	if action == "describe" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true}
		}
		if !validDescribeResult(reply) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true}
	}
	if action == "session_state" {
		var reply struct {
			Autocommit *bool `json:"autocommit"`
		}
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if reply.Autocommit == nil {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"autocommit": *reply.Autocommit}, dispatched: true}
	}
	if action == "statement" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true}
		}
		if !validStatementResult(reply) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true}
	}
	if action == "execute" {
		var reply struct {
			Executed bool `json:"executed"`
		}
		err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Executed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"executed": true}, dispatched: true}
	}
	var reply json.RawMessage
	err = gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
	if err != nil {
		return gateway.upstreamFailure(err, session, permit)
	}
	if len(reply) > quota.MaxResultBytes {
		return gatewayResult{status: 413, code: "result_limit", dispatched: true}
	}
	var rows [][]json.RawMessage
	if len(reply) == 0 || reply[0] != '[' || json.Unmarshal(reply, &rows) != nil {
		return gateway.unknown(session, permit)
	}
	if ndjson {
		stream, err := gatewayRows(requestID, rows, quota.MaxResultBytes)
		if err != nil {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true}
		}
		return gatewayResult{dispatched: true, stream: stream}
	}
	return gatewayResult{value: reply, dispatched: true}
}

func (gateway *Gateway) unknown(session *gatewaySession, permit *admissionPermit) gatewayResult {
	permit.retain()
	if session != nil {
		session.poisoned = true
		noteUnowned(session, time.Now())
	}
	return gatewayResult{status: 502, code: "outcome_unknown", dispatched: true, session: session}
}

// logUnknownOutcome records the transition that poisons a session and isolates
// its namespace. The 502 the caller sees lives for one request, and the refusals
// that follow are the same isolation seen again, so without this line a single
// unproven reply leaves an isolated namespace and no trace in the process log.
// The caller holds the session lock that the result's finish releases.
func logUnknownOutcome(requestID, action string, result *gatewayResult) {
	database, session, worker := "", "", ""
	switch {
	case result.session != nil:
		database, session, worker = result.session.database.Key(), result.session.public, result.session.worker.ID
	case result.operationDatabase.Namespace.Tenant != "":
		database = result.operationDatabase.Key()
	}
	// The worker's own verdict is what separates "it never entered execution" from
	// "it may have committed", and its failure text is the only account of why the
	// reply could not be used: the line that outlives the request is where an
	// operator reconstructs both, so an absent verdict says so instead of leaving
	// the fields empty.
	rpc, observation := "none", "none"
	if result.rpc != nil {
		rpc = fmt.Sprintf("ambiguous=%t,rejected=%t,execution_completed=%t", result.rpc.Ambiguous, result.rpc.evidence.Rejected(), result.rpc.evidence.Executed())
		switch {
		case result.rpc.workerFailure != "":
			observation = workerFailureLog(result.rpc.workerFailure)
		case result.rpc.cause != nil:
			observation = workerFailureLog(result.rpc.cause.Error())
		}
	}
	log.Printf("SQL outcome unknown: request_id=%s action=%s stage=%s database=%s session=%s worker=%s rpc=%s observation=%s",
		requestID, action, result.operationStage, database, session, worker, rpc, observation)
}

func (gateway *Gateway) upstreamFailure(err error, session *gatewaySession, permit *admissionPermit) (result gatewayResult) {
	var failure *SQLRPCError
	defer func() { result.rpc = failure }()
	if !errors.As(err, &failure) || failure.Ambiguous {
		return gateway.unknown(session, permit)
	}
	worker, found := sqlWorkerFailure(failure)
	if !found {
		if failure.evidence.Rejected() {
			return gatewayFailure(502, "upstream_rejected")
		}
		return gatewayFailure(503, "upstream_unavailable")
	}
	// The worker text is the operator's evidence; the caller only sees the
	// classified code and message.
	log.Printf("SQL worker failure: code=%s status=%d executed=%t rejected=%t worker_error=%s", worker.code, worker.status, worker.executed, worker.rejected, workerFailureLog(failure.workerFailure))
	return gatewayResult{status: worker.status, code: worker.code, failure: &worker, dispatched: true, session: session}
}

func upstreamSessionID(identifier string) bool {
	if len(identifier) != 64 {
		return false
	}
	for _, character := range identifier {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
