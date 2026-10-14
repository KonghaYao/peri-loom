package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

func TestSQLUpstreamTerminalNegativeStrictFlag(test *testing.T) {
	for _, scenario := range []string{"completed", "old-negative", "false", "null", "string", "number", "success", "success-false", "success-null", "identity", "version", "nonnull-result", "null-error", "extra"} {
		test.Run(scenario, func(test *testing.T) {
			secret := sqlTestToken(test)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				var envelope struct {
					RequestID string `json:"request_id"`
				}
				_ = json.NewDecoder(request.Body).Decode(&envelope)
				reply := map[string]any{"version": 1, "request_id": envelope.RequestID, "ok": false, "result": nil, "error": secret, "execution_completed": true}
				switch scenario {
				case "old-negative":
					delete(reply, "execution_completed")
				case "false":
					reply["execution_completed"] = false
				case "null":
					reply["execution_completed"] = nil
				case "string":
					reply["execution_completed"] = "true"
				case "number":
					reply["execution_completed"] = 1
				case "success":
					reply["ok"], reply["error"] = true, nil
				case "success-false":
					reply["ok"], reply["error"], reply["execution_completed"] = true, nil, false
				case "success-null":
					reply["ok"], reply["error"], reply["execution_completed"] = true, nil, nil
				case "identity":
					reply["request_id"] = "different-request"
				case "version":
					reply["version"] = 2
				case "nonnull-result":
					reply["result"] = map[string]any{"executed": true}
				case "null-error":
					reply["error"] = nil
				case "extra":
					reply["unexpected"] = true
				}
				_ = json.NewEncoder(writer).Encode(reply)
			}))
			defer server.Close()
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": secret})
			if err != nil {
				test.Fatal(err)
			}
			err = upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "terminal-1", map[string]any{"command": "execute", "sql": "INSERT INTO t VALUES (1)"}, nil)
			failure := sqlTestFailure(test, err)
			terminal := scenario == "completed"
			if failure.ExecutionCompleted != terminal || failure.Ambiguous == terminal || failure.Rejected || calls.Load() != 1 {
				test.Fatal("SQL completion classification or no-retry invariant violated")
			}
			if strings.Contains(fmt.Sprintf("%+v", err), secret) {
				test.Fatal("SQL worker error exposed sensitive text")
			}
		})
	}
}

func TestGatewayTerminalConstraintThenExplicitRollback(test *testing.T) {
	fixture := newGatewayFixture(test)
	var failures, rollbacks, queries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil {
			test.Error("invalid SQL request")
			return
		}
		switch envelope.Command["command"] {
		case "open_session":
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"session": strings.Repeat("a", 64), "readonly": false}))
		case "execute":
			if envelope.Command["session"] != strings.Repeat("a", 64) {
				test.Error("terminal SQL error rebound native session")
			}
			if envelope.Command["sql"] == "ROLLBACK" {
				rollbacks.Add(1)
				_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"executed": true}))
				return
			}
			failures.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": envelope.RequestID, "ok": false, "result": nil, "error": "private constraint details", "execution_completed": true})
		case "close_session":
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"closed": true}))
		case "query":
			if envelope.Command["session"] != strings.Repeat("a", 64) {
				test.Error("query rebound native session after explicit rollback")
			}
			queries.Add(1)
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, [][]any{{1}}))
		default:
			test.Error("unexpected SQL dispatch")
		}
	}))
	defer server.Close()
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		worker := metadata.Workers["worker-tenant-a"]
		worker.Address = server.URL
		metadata.Workers[worker.ID] = worker
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	upstream, err := NewSQLUpstream(map[string]string{"worker-tenant-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	fixture.gateway.config.Upstream = upstream
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	for attempt := 0; attempt < 4; attempt++ {
		requestID := fmt.Sprintf("constraint-%d", attempt)
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (1)", "request_id": requestID})
		var reply struct {
			RequestID string `json:"request_id"`
			OK        bool   `json:"ok"`
			Error     struct {
				Code               string `json:"code"`
				ExecutionCompleted bool   `json:"execution_completed"`
				MayHaveCommitted   bool   `json:"may_have_committed"`
			} `json:"error"`
		}
		if json.Unmarshal(response.Body.Bytes(), &reply) != nil || response.Code != 400 || reply.OK || reply.RequestID != requestID || reply.Error.Code != "sql_error" || !reply.Error.ExecutionCompleted || !reply.Error.MayHaveCommitted || strings.Contains(response.Body.String(), "private constraint") {
			test.Fatal("terminal SQL error misrepresented completion or disclosed worker details")
		}
		metrics := fixture.gateway.Metrics()
		if metrics["active_permits"] != 0 || metrics["unknown_held_permits"] != 0 || failures.Load() != int32(attempt+1) || rollbacks.Load() != 0 {
			test.Fatal("terminal constraint leaked quota, retried SQL, or implicitly rolled back")
		}
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "ROLLBACK", "request_id": "explicit-rollback"})
	if response.Code != 200 || failures.Load() != 4 || rollbacks.Load() != 1 {
		test.Fatal("same native session did not permit explicit rollback")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "query-after-rollback"})
	if response.Code != 200 || queries.Load() != 1 || fixture.gateway.Metrics()["active_permits"] != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
		test.Fatal("session query after explicit rollback failed or leaked permits")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 || fixture.gateway.Close(context.Background()) != nil {
		test.Fatal("terminal SQL error prevented acknowledged session cleanup")
	}
}
