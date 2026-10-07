package platform

import (
	"context"
	"encoding/json"
	"errors"

	controller "peri-loom/controller"
)

func (gateway *Gateway) open(ctx context.Context, principal Principal, database controller.DatabaseID, requestID string, permit *admissionPermit) gatewayResult {
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
	var reply struct {
		Session  string `json:"session"`
		Readonly *bool  `json:"readonly"`
	}
	err = gateway.config.Upstream.Call(ctx, worker, requestID, map[string]any{"command": "open_session", "database": database}, &reply)
	if err != nil {
		return gateway.upstreamFailure(err, nil, permit)
	}
	if !upstreamSessionID(reply.Session) || reply.Readonly == nil || *reply.Readonly != (binding.Role == "read_only") {
		return gateway.unknown(nil, permit)
	}
	identifier, err := controller.RandomID()
	if err != nil {
		return gateway.unknown(nil, permit)
	}
	session := &gatewaySession{public: identifier, upstream: reply.Session, worker: worker, binding: binding, database: database, project: principal.Project, subject: principal.Subject}
	session.mu.Lock()
	gateway.mu.Lock()
	if _, exists := gateway.sessions[identifier]; exists {
		gateway.mu.Unlock()
		session.mu.Unlock()
		return gateway.unknown(nil, permit)
	}
	gateway.sessions[identifier] = session
	gateway.mu.Unlock()
	permit.keepSession = true
	return gatewayResult{value: map[string]any{"session": identifier, "readonly": *reply.Readonly}, dispatched: true, session: session, finish: session.mu.Unlock}
}

func (gateway *Gateway) dispatch(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool) gatewayResult {
	gateway.mu.Lock()
	session := gateway.sessions[request.Session]
	gateway.mu.Unlock()
	if session == nil || session.subject != principal.Subject || session.project != principal.Project {
		return gatewayFailure(403, "invalid_session")
	}
	if !session.mu.TryLock() {
		return gatewayFailure(409, "session_busy")
	}
	result := gateway.dispatchLocked(ctx, principal, request, requestID, action, quota, permit, ndjson, session)
	result.session = session
	result.finish = session.mu.Unlock
	return result
}

func (gateway *Gateway) dispatchLocked(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool, session *gatewaySession) gatewayResult {
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	if session.closed {
		return gatewayFailure(403, "invalid_session")
	}
	if session.poisoned {
		return gatewayFailure(502, "outcome_unknown")
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
		if principal.Role == "read_only" && binding.Role != "read_only" {
			return gatewayFailure(403, "readonly_placement_required")
		}
	}
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	command := map[string]any{"command": action, "session": session.upstream}
	if action == "close" {
		command["command"] = "close_session"
		var reply struct {
			Closed bool `json:"closed"`
		}
		err := gateway.config.Upstream.Call(ctx, session.worker, requestID, command, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Closed {
			return gateway.unknown(session, permit)
		}
		gateway.removeSession(session)
		return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
	}
	command["sql"] = request.SQL
	if action == "execute" {
		var reply struct {
			Executed bool `json:"executed"`
		}
		err := gateway.config.Upstream.Call(ctx, session.worker, requestID, command, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Executed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"executed": true}, dispatched: true}
	}
	var reply json.RawMessage
	err := gateway.config.Upstream.Call(ctx, session.worker, requestID, command, &reply)
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

func (gateway *Gateway) upstreamFailure(err error, session *gatewaySession, permit *admissionPermit) gatewayResult {
	var failure *SQLRPCError
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
