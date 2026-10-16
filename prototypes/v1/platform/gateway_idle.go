package platform

import (
	"context"
	"errors"
	"time"
)

var errHranaSessionBusy = errors.New("Hrana session is busy")
var errSessionProjectUnavailable = errors.New("session project unavailable")

// sessionCleanupQuota resolves the quota one session expiry needs. Expiry runs
// on a one-second tick, so it visits the committed registry under its own lock
// instead of cloning it: a full clone holds the registry lock that in-flight
// read requests need before they touch the worker.
//
// Isolation is deliberately not consulted here. It refuses new work on the
// namespace, but the caller is retiring a session that is already open, and that
// close is the only path that can take the session and its quota to an accounted
// end: refusing it would leave the platform holding a session it can never
// release.
func sessionCleanupQuota(store StateStore, session *gatewaySession) (Quotas, error) {
	var quota Quotas
	err := readOperationState(store, func(state State) error {
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
	now := time.Now()
	if err := sessionCleanupGate(session, now); err != nil {
		return err
	}
	return gateway.retireSession(ctx, session, now)
}
