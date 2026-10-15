package platform

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	controller "peri-loom/controller"
)

func (gateway *Gateway) open(ctx context.Context, principal Principal, database controller.DatabaseID, requestID string, permit *admissionPermit, quota Quotas) (result gatewayResult) {
	if gateway.authorize(principal, database) != nil {
		return gatewayFailure(403, "forbidden")
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
	operationID, err := registerOperation(gateway.config.State, database, requestID, "gateway", "open", "open_session", worker.ID, "", map[string]any{"command": "open_session", "database": database})
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
		writerState, err = gateway.config.Upstream.(WriterSQLUpstream).CallWriter(ctx, worker, requestID, map[string]any{"command": "open_session", "database": database}, &reply)
	} else {
		ctx = bindSQLOpenObservation(ctx, gateway.config.OpenObserver, requestID)
		err = gateway.config.Upstream.Call(ctx, worker, requestID, map[string]any{"command": "open_session", "database": database}, &reply)
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
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	if session.closed {
		return gatewayFailure(403, "invalid_session")
	}
	if session.poisoned {
		return gateway.isolatedSessionFailure(session.database)
	}
	if principal.Database != nil {
		_, binding, err := gateway.route(session.database)
		if err != nil {
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
		worker, binding, err := gateway.route(session.database)
		if err != nil {
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
	}
	stage := "tool_rpc"
	if action == "close" {
		stage = "close_session"
	} else if action == "execute" || action == "query" || action == "statement" || action == "describe" || action == "cursor_open" {
		command["sql"] = request.SQL
	}
	operationID, err := registerOperation(gateway.config.State, session.database, requestID, "gateway", action, stage, session.worker.ID, session.upstream, command)
	if err != nil {
		return operationAdmissionFailure(err)
	}
	defer func() {
		result.operationID, result.operationStage, result.operationDatabase = operationID, stage, session.database
		result.operationPending = true
	}()
	if action == "close" {
		command["command"] = "close_session"
		var reply struct {
			Closed bool `json:"closed"`
		}
		err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply)
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
		if err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes || !validCursorOpenResult(reply) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true}
	}
	if action == "cursor_fetch" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply); err != nil {
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
		if err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Closed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
	}
	if action == "describe" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply); err != nil {
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
	if action == "statement" {
		args := request.Args
		if args == nil {
			args = []sqlArgument{}
		}
		command["args"] = args
		namedArgs := request.NamedArgs
		if namedArgs == nil {
			namedArgs = []namedSQLArgument{}
		}
		command["named_args"] = namedArgs
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply); err != nil {
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
		err := gateway.callSession(ctx, session, currentBinding, requestID, command, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Executed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"executed": true}, dispatched: true}
	}
	var reply json.RawMessage
	err = gateway.callSession(ctx, session, currentBinding, requestID, command, &reply)
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
	}
	return gatewayResult{status: 502, code: "outcome_unknown", dispatched: true, session: session}
}

func (gateway *Gateway) upstreamFailure(err error, session *gatewaySession, permit *admissionPermit) (result gatewayResult) {
	var failure *SQLRPCError
	defer func() { result.rpc = failure }()
	if errors.As(err, &failure) && failure.ExecutionCompleted && !failure.Ambiguous {
		return gatewayResult{status: 400, code: "sql_error", dispatched: true}
	}
	if errors.As(err, &failure) && !failure.Ambiguous {
		if failure.Rejected {
			return gatewayFailure(502, "upstream_rejected")
		}
		return gatewayFailure(503, "upstream_unavailable")
	}
	return gateway.unknown(session, permit)
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
