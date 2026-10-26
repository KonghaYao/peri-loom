package platform

import (
	"context"
	"errors"
	"fmt"
	"testing"

	controller "peri-loom/controller"
)

const cachedWriteSQL = "INSERT INTO records(id) VALUES(?)"

func cachedWriteRequest(session, requestID string, id int) map[string]any {
	return map[string]any{
		"session": session, "request_id": requestID, "sql": cachedWriteSQL,
		"args": []any{map[string]any{"type": "integer", "value": fmt.Sprint(id)}},
	}
}

func cachedWriteReply() map[string]any {
	return map[string]any{
		"cols": []any{}, "rows": []any{}, "affected_row_count": 1,
		"last_insert_rowid": 7, "autocommit": true,
	}
}

// A hit removes only the guarded RPC. The durable intent must still bind the
// exact command, including the changed parameter value, before either write.
func TestGuardedStatementHotWriteKeepsIntentAndUsesOneRPC(test *testing.T) {
	fixture := newGatewayFixture(test)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	requestID := ""
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "guarded_readonly_statement":
			return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
		case "statement":
			commands := transmittedCommands(fixture.upstream, "statement")
			digest := commandDigest(commands[len(commands)-1])
			state, err := store.Snapshot()
			if err != nil {
				return nil, err
			}
			for _, record := range state.Operations {
				if record.RequestID == requestID && record.State == "in_flight" && record.Digest == digest {
					return cachedWriteReply(), nil
				}
			}
			return nil, errors.New("cached statement dispatched before matching durable intent")
		}
		return nil, nil
	})
	before := store.commitsObserved()
	var replies []string
	for id := 1; id <= 2; id++ {
		requestID = fmt.Sprintf("hot-write-%d", id)
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, requestID, id))
		if response.Code != 200 {
			test.Fatalf("write %d failed: %d %s", id, response.Code, response.Body.String())
		}
		replies = append(replies, replyResult(test, response))
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 2 {
		test.Fatalf("hot writes used guarded=%d statement=%d, want 1 and 2",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"))
	}
	if got := store.commitsObserved() - before; got != 4 {
		test.Fatalf("two writes used %d operation commits, want two intent/terminal pairs", got)
	}
	if replies[0] != replies[1] {
		test.Fatalf("cache hit changed the result: %q != %q", replies[0], replies[1])
	}
}

// A context-only refusal does not classify the SQL. The same SELECT in another
// session on the worker must keep its one-RPC, no-intent read path.
func TestGuardedStatementContextRefusalDoesNotPolluteOtherSessions(test *testing.T) {
	for _, reason := range []string{"transaction_context", "connection_control"} {
		test.Run(reason, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			store := newGatedStateStore(fixture.state)
			fixture.gateway.config.State = store
			first := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			second := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			refused := false
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "guarded_readonly_statement" {
					if !refused {
						refused = true
						return map[string]any{"requires_intent": true, "reason": reason}, nil
					}
					return guardedReadonlyReply([][]any{{7}}), nil
				}
				return nil, nil
			})
			firstReply := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
				"session": first, "sql": "SELECT 7", "request_id": "context-first",
			})
			if firstReply.Code != 200 {
				test.Fatalf("context statement failed: %d %s", firstReply.Code, firstReply.Body.String())
			}
			before := store.commitsObserved()
			secondReply := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
				"session": second, "sql": "SELECT 7", "request_id": "context-second",
			})
			if secondReply.Code != 200 || store.commitsObserved() != before {
				test.Fatalf("context refusal contaminated a read: commits=%d response=%d %s",
					store.commitsObserved()-before, secondReply.Code, secondReply.Body.String())
			}
			if fixture.upstream.count("guarded_readonly_statement") != 2 || fixture.upstream.count("statement") != 1 {
				test.Fatalf("context SQL was cached: guarded=%d statement=%d",
					fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"))
			}
		})
	}
}

func warmStatementCache(test *testing.T) (*gatewayFixture, *gatedStateStore, string) {
	test.Helper()
	fixture := newGatewayFixture(test)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "guarded_readonly_statement" {
			return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
		}
		return nil, nil
	})
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, "cache-warm", 1))
	if response.Code != 200 {
		test.Fatalf("cache warm failed: %d %s", response.Code, response.Body.String())
	}
	return fixture, store, session
}

func TestGuardedStatementCachedWriteIntentFailureNeverDispatches(test *testing.T) {
	fixture, store, session := warmStatementCache(test)
	store.refuseCommit(1)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, "cache-intent-failed", 2))
	if response.Code != 503 || gatewayCode(test, response) != "operation_registration_unavailable" {
		test.Fatalf("failed cached intent answered %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 1 {
		test.Fatal("failed intent still dispatched a cached statement")
	}
}

func TestGuardedStatementCachedWriteUnknownNeverReplays(test *testing.T) {
	fixture, _, session := warmStatementCache(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, "cache-unknown", 2))
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
		test.Fatalf("ambiguous cached write answered %d %s", response.Code, response.Body.String())
	}
	records := operationRecordsOf(test, fixture.state, "cache-unknown")
	if len(records) != 1 || records[0].State != "quarantined" || !records[0].MayHaveCommitted {
		test.Fatalf("cached write lost unknown evidence: %+v", records)
	}
	repeated := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, "cache-unknown", 2))
	if repeated.Code == 200 || fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 2 {
		test.Fatalf("unknown cached write replayed: guarded=%d statement=%d response=%d",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"), repeated.Code)
	}
}

// Eviction may add a guarded call again but cannot remove the intent or alter
// the SQL result. This also exercises the bounded cache on the statement path.
func TestGuardedStatementEvictedWriteReturnsToGuardedPath(test *testing.T) {
	fixture, _, session := warmStatementCache(test)
	for index := 0; index < committingStatementCapacity*2; index++ {
		fixture.gateway.committingStatements.remember(committingStatementKey{worker: "other", digest: fmt.Sprint(index)})
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", cachedWriteRequest(session, "cache-evicted", 2))
	if response.Code != 200 || fixture.upstream.count("guarded_readonly_statement") != 2 || fixture.upstream.count("statement") != 2 {
		test.Fatalf("evicted statement failed: guarded=%d statement=%d response=%d %s",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"), response.Code, response.Body.String())
	}
}
