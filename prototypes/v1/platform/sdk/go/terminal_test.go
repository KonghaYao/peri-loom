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
