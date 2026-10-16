package platform

import (
	"context"
	"encoding/json"
	"sync"

	controller "peri-loom/controller"
)

// A statement the worker prepared as read-only cannot commit. Its dispatch has
// nothing to reconcile after a crash, so it needs no durable intent: the single
// terminal record written after the call is the acknowledgement, which is the
// path beginDispatchOperation already has for non-committing actions.
//
// The verdict is the engine's own answer to describe - the same field the Hrana
// adapter reads to choose a read cursor, and the same predicate the worker
// enforces on a read cursor - and never a Go-side reading of the SQL text. Every
// other outcome (a refused call, an unusable reply, a missing or non-boolean
// flag) keeps the statement committing, so the optimization can be lost but
// never guessed.
const committingStatementCapacity = 2048

// committingStatementKey identifies a statement by the worker that prepared it
// and the exact bytes it prepared. The verdict is a property of the statement on
// one engine and of nothing else - no session, tenant or database takes part.
type committingStatementKey struct {
	worker string
	digest string
}

// committingStatementCache remembers the statements a worker proved committing,
// and remembers nothing else. Remembering a verdict is the one thing a cache can
// do here without a proof that it stays valid: a remembered committing verdict
// only costs the intent its statement was going to write anyway, while the
// read-only verdict - the one that skips the durable intent - is never
// remembered and is always re-proven for the request that relies on it. A stale
// entry can therefore never be the reason an intent is missing.
//
// Two generations bound the memory without an eviction scan: a full generation is
// dropped as a whole, so an entry stays for at least the generation that follows
// the one it was recorded in.
type committingStatementCache struct {
	mutex    sync.Mutex
	current  map[committingStatementKey]struct{}
	previous map[committingStatementKey]struct{}
}

func (cache *committingStatementCache) remembered(key committingStatementKey) bool {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if _, found := cache.current[key]; found {
		return true
	}
	_, found := cache.previous[key]
	return found
}

func (cache *committingStatementCache) remember(key committingStatementKey) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if cache.current == nil {
		cache.current = make(map[committingStatementKey]struct{}, committingStatementCapacity)
	}
	if len(cache.current) >= committingStatementCapacity {
		cache.previous, cache.current = cache.current, make(map[committingStatementKey]struct{}, committingStatementCapacity)
	}
	cache.current[key] = struct{}{}
}

// readonlyStatement reports whether the worker proved that this statement cannot
// commit. Only the read-only verdict - the one that removes the durable intent -
// is asked for and answered afresh; a statement the worker already proved
// committing takes the committing path without asking again, which is where the
// statement's durable intent was going anyway. A classification that cannot be
// obtained answers false: the caller keeps the durable intent.
func (gateway *Gateway) readonlyStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, sql string) bool {
	if session == nil || sql == "" {
		return false
	}
	key := committingStatementKey{worker: session.worker.ID, digest: digestHex([]byte(sql))}
	if gateway.committingStatements.remembered(key) {
		return false
	}
	readonly, known := gateway.classifyStatement(ctx, session, binding, sql)
	if !known {
		// Not logged: losing the classification only means paying the intent this
		// request, and the dispatch that follows reports the same worker trouble
		// through its own failure path.
		return false
	}
	if !readonly {
		gateway.committingStatements.remember(key)
	}
	return readonly
}

// classifyStatement asks the worker that is about to execute the statement to
// prepare it, and reads the engine's read-only verdict out of that reply. The
// probe is a describe, so it cannot commit and carries no operation record.
func (gateway *Gateway) classifyStatement(ctx context.Context, session *gatewaySession, binding controller.Binding, sql string) (bool, bool) {
	probeID, err := controller.RandomID()
	if err != nil {
		return false, false
	}
	command, err := freezeCommand(map[string]any{"command": "describe", "session": session.upstream, "sql": sql})
	if err != nil {
		return false, false
	}
	var reply json.RawMessage
	leaveClassify := requestPhaseSpan(ctx, requestPhaseClassify)
	err = gateway.sendSession(ctx, session, binding, probeID, command, &reply)
	leaveClassify()
	if err != nil || !validDescribeResult(reply) {
		return false, false
	}
	var description struct {
		Readonly *bool `json:"is_readonly"`
	}
	if json.Unmarshal(reply, &description) != nil || description.Readonly == nil {
		return false, false
	}
	return *description.Readonly, true
}
