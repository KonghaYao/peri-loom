package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

func (session *gatewaySession) acceptWriterState(state *SessionWriterState, binding controller.Binding) bool {
	if state == nil || state.Session != session.upstream || state.Binding.WorkerID != session.worker.ID || !sameGatewayBinding(state.Binding, binding) || session.writerKnown && state.Sequence <= session.writerSequence {
		return false
	}
	session.writerKnown = true
	session.writerOwner = state.Owner
	session.writerSequence = state.Sequence
	return true
}

func (gateway *Gateway) writerDispatchGuard(principal Principal, session *gatewaySession, action string) gatewayResult {
	if gateway.authorize(principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	if session.closed {
		return gatewayFailure(403, "invalid_session")
	}
	if session.poisoned {
		return gateway.isolatedSessionFailure(session.database)
	}
	if action != "close" && !session.writerProtocol {
		return gatewayFailure(503, "writer_profile_unavailable")
	}
	if action == "close" && principal.Database == nil {
		return gatewayResult{}
	}
	worker, binding, err := gateway.route(session.database)
	if err != nil {
		return gatewayFailure(409, "placement_unavailable")
	}
	if !principalBindingValid(principal, binding) {
		return gatewayFailure(403, "token_binding_mismatch")
	}
	if action != "close" && (session.moved || !sameGatewayBinding(session.binding, binding) || session.worker.Address != worker.Address) {
		session.moved = true
		return gatewayFailure(409, "session_moved")
	}
	return gatewayResult{}
}

// callSession sends the frozen command of one dispatch. It takes bytes, not a
// mutable command, so the body that reaches the worker is the body the durable
// record was settled against.
func (gateway *Gateway) callSession(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID string, command frozenCommand, output any) error {
	// Deferred: every refusal below still attributes its worker time to the worker segment.
	defer requestPhaseSpan(ctx, requestPhaseWorker)()
	return gateway.sendSession(ctx, session, binding, requestID, command, output)
}

// sendSession is the dispatch itself, without a phase of its own: the caller
// names the phase, so a worker call that is not the client's statement - the
// read-only classification - is attributed to its own segment instead of being
// folded into the worker segment of the statement it classifies.
func (gateway *Gateway) sendSession(ctx context.Context, session *gatewaySession, binding controller.Binding, requestID string, command frozenCommand, output any) error {
	if !session.writerProtocol {
		return gateway.config.Upstream.Call(ctx, session.worker, requestID, command, output)
	}
	state, err := gateway.config.Upstream.(WriterSQLUpstream).CallWriter(ctx, session.worker, requestID, command, output)
	var failure *SQLRPCError
	settled := err == nil || errors.As(err, &failure) && failure.evidence.Executed() && !failure.Ambiguous
	if !settled {
		return err
	}
	if !session.acceptWriterState(state, binding) || command.operationName() == "close_session" && state.Owner {
		return &SQLRPCError{Ambiguous: true}
	}
	return err
}
