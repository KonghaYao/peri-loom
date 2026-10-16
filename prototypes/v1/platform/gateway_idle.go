package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

var errHranaSessionBusy = errors.New("Hrana session is busy")
var errHranaSessionUnknown = errors.New("Hrana session has an unresolved SQL outcome")
var errSessionProjectUnavailable = errors.New("session project unavailable")

// sessionCleanupQuota resolves the quota and quarantine state one session
// expiry needs. Expiry runs on a one-second tick, so it visits the committed
// registry under its own lock instead of cloning it: a full clone holds the
// registry lock that in-flight read requests need before they touch the worker.
func sessionCleanupQuota(store StateStore, session *gatewaySession) (Quotas, error) {
	var quota Quotas
	err := readOperationState(store, func(state State) error {
		if _, blocked := isolatedOperation(state, session.database, true); blocked {
			return ErrDatabaseQuarantined
		}
		project, exists := state.Projects[session.project]
		if !exists {
			return errSessionProjectUnavailable
		}
		quota = project.Quotas
		return nil
	})
	if err != nil {
		return Quotas{}, err
	}
	return quota, nil
}

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
	quota, err := sessionCleanupQuota(gateway.config.State, session)
	if err != nil {
		return err
	}
	permit, ok := gateway.admission.reserve(session.project, quota, false, true)
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
