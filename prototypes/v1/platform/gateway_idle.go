package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

var errHranaSessionBusy = errors.New("Hrana session is busy")
var errHranaSessionUnknown = errors.New("Hrana session has an unresolved SQL outcome")

func (gateway *Gateway) expireHranaSession(ctx context.Context, identifier string) error {
	gateway.mu.Lock()
	session := gateway.sessions[identifier]
	gateway.mu.Unlock()
	if session == nil {
		return nil
	}
	if !session.mu.TryLock() {
		return errHranaSessionBusy
	}
	defer session.mu.Unlock()
	if session.closed {
		return nil
	}
	if session.poisoned {
		return errHranaSessionUnknown
	}
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		return err
	}
	if _, blocked := isolatedOperation(state, session.database, true); blocked {
		return ErrDatabaseQuarantined
	}
	project, exists := state.Projects[session.project]
	if !exists {
		return errors.New("Hrana session project unavailable")
	}
	permit, ok := gateway.admission.reserve(session.project, project.Quotas, false, true)
	if !ok {
		return errHranaSessionBusy
	}
	defer permit.finish()
	requestID, err := controller.RandomID()
	if err != nil {
		return err
	}
	if err = gateway.closeTrackedSession(ctx, session, requestID, permit); err != nil {
		var failure *SQLRPCError
		if !errors.As(err, &failure) || failure.Ambiguous {
			session.poisoned = true
			permit.retain()
		}
		return err
	}
	gateway.removeSession(session)
	return nil
}
