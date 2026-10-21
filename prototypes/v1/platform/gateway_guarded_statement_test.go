package platform

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func guardedReadonlyReply(rows [][]any) map[string]any {
	return map[string]any{
		"cols":               []any{map[string]any{"name": "answer", "decltype": "INTEGER"}},
		"rows":               rows,
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"autocommit":         true,
	}
}

// A statement the worker proves read-only on the same prepared statement that
// executes it needs one RPC and no durable intent.
func TestGuardedStatementReadOnlySkipsIntentAndSecondDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] != "guarded_readonly_statement" {
			return nil, nil
		}
		return guardedReadonlyReply([][]any{{"42"}}), nil
	})
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	before := operationCommits(test, store)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
		"session":    session,
		"sql":        "SELECT ? AS answer",
		"args":       []any{map[string]any{"type": "integer", "value": "42"}},
		"request_id": "guarded-read",
	})
	if response.Code != 200 {
		test.Fatalf("read-only statement failed: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("describe") != 0 {
		test.Fatalf("read-only statement was not one guarded call: guarded=%d statement=%d describe=%d",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"), fixture.upstream.count("describe"))
	}
	if grew := operationCommits(test, store) - before; grew != 0 {
		test.Fatalf("a proven read-only statement still wrote %d operation commits", grew)
	}
	if records := operationRecordsOf(test, fixture.state, "guarded-read"); len(records) != 0 {
		test.Fatalf("a proven read-only statement kept %d operation records", len(records))
	}
	if outcomes := fixture.audit.outcomesOf("statement"); len(outcomes) != 1 || outcomes[0] != "ok" {
		test.Fatalf("the read-only statement lost its durable audit row: %v", outcomes)
	}
	if !strings.Contains(response.Body.String(), `"42"`) {
		test.Fatalf("the guarded result did not reach the client: %s", response.Body.String())
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] != "guarded_readonly_statement" {
			continue
		}
		if call.command["want_rows"] != true {
			test.Fatal("the guarded call lost the row projection")
		}
		args, _ := call.command["args"].([]any)
		if len(args) != 1 {
			test.Fatalf("the guarded call lost its bound arguments: %v", call.command["args"])
		}
	}
}

// A statement the engine classifies as writing is refused before it steps. Only
// then does the platform persist the intent, and the committing dispatch follows.
func TestGuardedStatementRequiresIntentPersistsIntentBeforeStatement(test *testing.T) {
	fixture := newGatewayFixture(test)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "guarded_readonly_statement":
			return map[string]any{"requires_intent": true}, nil
		case "statement":
			state, err := store.Snapshot()
			if err != nil {
				return nil, err
			}
			for _, record := range state.Operations {
				if record.RequestID == "guarded-write" {
					return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 1, "last_insert_rowid": nil, "autocommit": true}, nil
				}
			}
			return nil, errors.New("statement dispatched before its durable intent")
		}
		return nil, nil
	})
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	before := operationCommits(test, store)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
		"session":    session,
		"sql":        "INSERT INTO records(id) VALUES(?)",
		"args":       []any{map[string]any{"type": "integer", "value": "1"}},
		"request_id": "guarded-write",
	})
	if response.Code != 200 {
		test.Fatalf("write statement failed: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 1 {
		test.Fatalf("write path dispatched guarded=%d statement=%d",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"))
	}
	if grew := operationCommits(test, store) - before; grew != 2 {
		test.Fatalf("write path cost %d operation commits, want intent plus terminal", grew)
	}
}

// A lost acknowledgement is not the worker's proof that the statement was
// refused. It stays unknown, writes its operation record, and is never replayed
// as a committing statement.
func TestGuardedStatementTimeoutNeverFallsBackToStatement(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "guarded_readonly_statement" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
		"session":    session,
		"sql":        "SELECT 1",
		"request_id": "guarded-timeout",
	})
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
		test.Fatalf("ambiguous guarded call answered %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 0 {
		test.Fatalf("ambiguous guarded call fell back to the statement: guarded=%d statement=%d",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"))
	}
	if records := operationRecordsOf(test, fixture.state, "guarded-timeout"); len(records) != 1 {
		test.Fatalf("ambiguous guarded call kept %d operation records, want one", len(records))
	}
	repeated := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
		"session":    session,
		"sql":        "SELECT 1",
		"request_id": "guarded-timeout",
	})
	if repeated.Code == 200 {
		test.Fatalf("an isolated session answered a retry: %d %s", repeated.Code, repeated.Body.String())
	}
	if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != 0 {
		test.Fatalf("a retry replayed the ambiguous statement: guarded=%d statement=%d",
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("statement"))
	}
}

// The requires_intent flag is the only reply that moves work to the committing
// path. Anything else is an unusable classification and stays unknown.
func TestGuardedStatementMalformedReplyIsUnknownAndNeverReplayed(test *testing.T) {
	for name, reply := range map[string]any{
		"flag-not-bool":   map[string]any{"requires_intent": "true"},
		"flag-with-extra": map[string]any{"requires_intent": true, "autocommit": true},
		"statement-shape": map[string]any{"cols": []any{}, "rows": []any{}},
	} {
		test.Run(name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "guarded_readonly_statement" {
					return reply, nil
				}
				return nil, nil
			})
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
				"session":    session,
				"sql":        "SELECT 1",
				"request_id": "guarded-malformed",
			})
			if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
				test.Fatalf("malformed guarded reply answered %d %s", response.Code, response.Body.String())
			}
			if fixture.upstream.count("statement") != 0 {
				test.Fatal("a malformed guarded reply fell back to the statement")
			}
			if records := operationRecordsOf(test, fixture.state, "guarded-malformed"); len(records) != 1 {
				test.Fatalf("malformed guarded reply kept %d operation records, want one", len(records))
			}
		})
	}
}

// A settled worker failure is a definite answer: it dispatches nothing further
// and leaves no ambiguous operation behind.
func TestGuardedStatementSettledFailureDoesNotDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "guarded_readonly_statement" {
			return nil, sqlWorkerFailureFromReply(true, false, "syntax error near SELECT")
		}
		return nil, nil
	})
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{
		"session":    session,
		"sql":        "SELEC 1",
		"request_id": "guarded-settled",
	})
	if response.Code != 400 || gatewayCode(test, response) != "sql_error" {
		test.Fatalf("settled guarded failure answered %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("statement") != 0 {
		test.Fatal("a settled guarded failure dispatched the statement")
	}
	if records := operationRecordsOf(test, fixture.state, "guarded-settled"); len(records) != 0 {
		test.Fatalf("a settled guarded failure kept %d operation records", len(records))
	}
}

// The writer protocol validates the guarded command with the same strict shape
// as a statement, including the optional row projection.
func TestGuardedStatementWireProfile(test *testing.T) {
	command := map[string]any{
		"command":    "guarded_readonly_statement",
		"session":    strings.Repeat("a", 64),
		"sql":        "SELECT ?",
		"args":       []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}},
		"named_args": []namedSQLArgument{},
		"want_rows":  true,
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		test.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || !writerCommandValid("guarded_readonly_statement", encoded, fields) {
		test.Fatalf("the writer protocol rejects the guarded command: %s", encoded)
	}
	command["unknown"] = true
	encoded, _ = json.Marshal(command)
	fields = nil
	if json.Unmarshal(encoded, &fields) == nil && writerCommandValid("guarded_readonly_statement", encoded, fields) {
		test.Fatal("the writer protocol accepted an unknown guarded field")
	}
}
