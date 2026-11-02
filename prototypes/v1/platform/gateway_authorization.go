package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

// authorizationScope is the state view one HTTP request decides authorization against.
// Authorization is the first lock a request queues for, and one request checks it several
// times — once in the protocol adapter, then again before the worker call and after
// routing — while also reading the same state for its admission checks. The view is
// therefore read once and reused while the request only does in-process work. Any check
// that follows an external wait invalidates the scope first, so a revocation that wait
// could have observed is still honoured.
type authorizationScope struct {
	keyID    string
	project  string
	database controller.DatabaseID
	// scoped records that the view was read for database; a project-only view cannot
	// answer a question about a database.
	scoped bool
	state  State
	valid  bool
}

// covers reports whether the held view answers this question. A database-scoped view also
// answers the project-only question, because it carries the project entry that read
// returns.
func (scope *authorizationScope) covers(keyID, project string, database *controller.DatabaseID) bool {
	if !scope.valid || scope.keyID != keyID || scope.project != project {
		return false
	}
	return database == nil || scope.scoped && scope.database == *database
}

type authorizationScopeKey struct{}

func withAuthorizationScope(ctx context.Context, scope *authorizationScope) context.Context {
	return context.WithValue(ctx, authorizationScopeKey{}, scope)
}

func authorizationScopeFrom(ctx context.Context) *authorizationScope {
	scope, _ := ctx.Value(authorizationScopeKey{}).(*authorizationScope)
	return scope
}

// invalidateAuthorizationScope drops the cached view: the next read goes to the state
// again. Callers place it after a wait that could outlast a revocation.
func invalidateAuthorizationScope(ctx context.Context) {
	if scope := authorizationScopeFrom(ctx); scope != nil {
		scope.valid = false
	}
}

// requestStateView returns the narrow view for this identity, reading it only when the
// request holds none that covers it.
func (gateway *Gateway) requestStateView(ctx context.Context, keyID, projectID string, database *controller.DatabaseID) (State, error) {
	if scope := authorizationScopeFrom(ctx); scope != nil && scope.covers(keyID, projectID, database) {
		return scope.state, nil
	}
	state, err := readRequestState(gateway.config.State, keyID, projectID, database)
	if err != nil {
		return State{}, err
	}
	if scope := authorizationScopeFrom(ctx); scope != nil {
		scope.keyID, scope.project, scope.state, scope.valid = keyID, projectID, state, true
		scope.scoped = database != nil
		if scope.scoped {
			scope.database = *database
		}
	}
	return state, nil
}

// authorizationView is requestStateView for a question that always names a database.
func (gateway *Gateway) authorizationView(ctx context.Context, principal Principal, database controller.DatabaseID) (State, error) {
	return gateway.requestStateView(ctx, principal.KeyID, principal.Project, &database)
}

// authorizeIn answers the same question as authorize, from the request's view.
func (gateway *Gateway) authorizeIn(ctx context.Context, principal Principal, database controller.DatabaseID) error {
	_, err := gateway.authorizeQuotaIn(ctx, principal, database)
	return err
}

// authorizeQuotaIn answers the same question as authorizeQuota, from the request's view.
// Only the state read is skipped; every check and every refusal is unchanged.
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
