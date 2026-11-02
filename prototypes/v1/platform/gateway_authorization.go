package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

// authorizationScope is the state snapshot one HTTP request decides authorization
// against. Authorization is the first lock a request queues for, and one request checks
// it two to four times — once in the protocol adapter, then again before the worker call
// and after routing — so the snapshot is read once and reused while the request only does
// in-process work. Any check that follows an external wait invalidates the scope first, so
// a revocation that wait could have observed is still honoured.
type authorizationScope struct {
	principal Principal
	database  controller.DatabaseID
	state     State
	valid     bool
}

type authorizationScopeKey struct{}

func withAuthorizationScope(ctx context.Context, scope *authorizationScope) context.Context {
	return context.WithValue(ctx, authorizationScopeKey{}, scope)
}

func authorizationScopeFrom(ctx context.Context) *authorizationScope {
	scope, _ := ctx.Value(authorizationScopeKey{}).(*authorizationScope)
	return scope
}

// invalidateAuthorizationScope drops the cached snapshot: the next check reads the state
// again. Callers place it after a wait that could outlast a revocation.
func invalidateAuthorizationScope(ctx context.Context) {
	if scope := authorizationScopeFrom(ctx); scope != nil {
		scope.valid = false
	}
}

// authorizationView returns the snapshot for this (principal, database), reading it only
// when the scope has none. The scope identity is the whole principal: a request that
// swapped its project for a session's project must not reuse the earlier view.
func (gateway *Gateway) authorizationView(ctx context.Context, principal Principal, database controller.DatabaseID) (State, error) {
	scope := authorizationScopeFrom(ctx)
	if scope != nil && scope.valid && scope.principal == principal && scope.database == database {
		return scope.state, nil
	}
	state, err := readAuthorizationState(gateway.config.State, principal.KeyID, principal.Project, database)
	if err != nil {
		return State{}, err
	}
	if scope != nil {
		scope.principal, scope.database, scope.state, scope.valid = principal, database, state, true
	}
	return state, nil
}

// authorizeIn answers the same question as authorize, from the request's snapshot.
func (gateway *Gateway) authorizeIn(ctx context.Context, principal Principal, database controller.DatabaseID) error {
	_, err := gateway.authorizeQuotaIn(ctx, principal, database)
	return err
}

// authorizeQuotaIn answers the same question as authorizeQuota, from the request's
// snapshot. Only the state read is skipped; every check and every refusal is unchanged.
func (gateway *Gateway) authorizeQuotaIn(ctx context.Context, principal Principal, database controller.DatabaseID) (Quotas, error) {
	if err := authorizationTargetValid(principal, database); err != nil {
		return Quotas{}, err
	}
	state, err := gateway.authorizationView(ctx, principal, database)
	if err != nil {
		return Quotas{}, errors.New("state unavailable")
	}
	return gateway.authorizeQuotaFrom(state, principal, database)
}
