package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestTerminalSQLFailureAllowsExplicitRollbackWithoutReplay(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var body map[string]string
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&raw); err != nil {
			test.Error(err)
		}
		body = make(map[string]string)
		for _, key := range []string{"request_id", "session", "sql"} {
			if value, exists := raw[key]; exists {
				var decoded string
				_ = json.Unmarshal(value, &decoded)
				body[key] = decoded
			}
		}
		if request.URL.Path == "/v1/sql/open" {
			writeReply(writer, body["request_id"], map[string]any{"session": testSession, "readonly": false})
			return
		}
		if body["session"] != testSession {
			test.Error("session silently changed")
		}
		if body["sql"] == "invalid SQL" {
			json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": body["request_id"], "ok": false, "result": nil, "error": map[string]any{"code": "sql_error", "execution_completed": true, "may_have_committed": true}})
			return
		}
		if body["sql"] == "ROLLBACK" {
			writeReply(writer, body["request_id"], map[string]bool{"executed": true})
			return
		}
		if body["sql"] == "SELECT 1" {
			writeReply(writer, body["request_id"], [][]int{{1}})
			return
		}
		test.Error("unexpected replay or SQL")
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testCredential, Options{})
	if err != nil {
		test.Fatal(err)
	}
	session, err := client.Open(context.Background(), testDatabase())
	if err != nil {
		test.Fatal(err)
	}
	err = session.Execute(context.Background(), "invalid SQL", "terminal")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "sql_error" || !failure.ExecutionCompleted || !failure.MayHaveCommitted || failure.OutcomeUnknown {
		test.Fatalf("terminal SQL result lost: %v", err)
	}
	if err := session.Execute(context.Background(), "ROLLBACK", "explicit-rollback"); err != nil {
		test.Fatal(err)
	}
	if _, err := session.Query(context.Background(), "SELECT 1", "explicit-reuse"); err != nil {
		test.Fatal(err)
	}
	if calls.Load() != 4 {
		test.Fatal("implicit replay or reconnection")
	}
}

// Every Worker SQL failure class must reach the caller as its own code, and only
// proven evidence may turn a failure into a definite answer.
func TestTerminalSQLFailureClassesStayDistinguishable(test *testing.T) {
	for _, scenario := range []struct {
		name      string
		failure   map[string]any
		code      string
		message   string
		unknown   bool
		completed bool
		committed bool
	}{
		{"settled deadline", map[string]any{"code": "sql_timeout", "message": "deadline budget", "execution_completed": true, "may_have_committed": true}, "sql_timeout", "deadline budget", false, true, true},
		{"settled compute budget", map[string]any{"code": "sql_budget_exceeded", "message": "step budget", "execution_completed": true, "may_have_committed": true}, "sql_budget_exceeded", "step budget", false, true, true},
		{"rejected statement", map[string]any{"code": "sql_error", "message": "could not be prepared", "execution_completed": false, "may_have_committed": false}, "sql_error", "could not be prepared", false, false, false},
		{"admission refusal", map[string]any{"code": "sql_unavailable", "message": "queue full", "execution_completed": false, "may_have_committed": false}, "sql_unavailable", "queue full", false, false, false},
		{"missing evidence", map[string]any{"code": "sql_timeout", "message": "deadline budget"}, "outcome_unknown", "deadline budget", true, false, false},
		{"settled without commit risk", map[string]any{"code": "sql_error", "execution_completed": true, "may_have_committed": false}, "sql_error", "", false, true, false},
		// The platform never publishes this pairing, so it proves nothing about
		// execution; the commit warning it did publish still reaches the caller.
		{"contradictory evidence", map[string]any{"code": "sql_error", "execution_completed": false, "may_have_committed": true}, "outcome_unknown", "", true, false, true},
		{"partial evidence", map[string]any{"code": "sql_error", "execution_completed": true}, "outcome_unknown", "", true, false, false},
		{"unknown code", map[string]any{"code": "invented_code", "execution_completed": true, "may_have_committed": true}, "remote_error", "", false, true, true},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var raw map[string]json.RawMessage
				if err := json.NewDecoder(request.Body).Decode(&raw); err != nil {
					test.Error(err)
				}
				var identifier, sql string
				_ = json.Unmarshal(raw["request_id"], &identifier)
				_ = json.Unmarshal(raw["sql"], &sql)
				if request.URL.Path == "/v1/sql/open" {
					writeReply(writer, identifier, map[string]any{"session": testSession, "readonly": false})
					return
				}
				if sql == "ROLLBACK" {
					writeReply(writer, identifier, map[string]bool{"executed": true})
					return
				}
				json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": false, "result": nil, "error": scenario.failure})
			}))
			defer server.Close()
			client, err := NewClient(server.URL, testCredential, Options{})
			if err != nil {
				test.Fatal(err)
			}
			session, err := client.Open(context.Background(), testDatabase())
			if err != nil {
				test.Fatal(err)
			}
			_, err = session.Query(context.Background(), "SELECT 1", "failure-class")
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != scenario.code || failure.Message != scenario.message ||
				failure.OutcomeUnknown != scenario.unknown || failure.ExecutionCompleted != scenario.completed || failure.MayHaveCommitted != scenario.committed {
				test.Fatalf("failure class lost: %v", err)
			}
		})
	}
}
