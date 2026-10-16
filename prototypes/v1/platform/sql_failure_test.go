package platform

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// sqlFailureEvidence is the upstream text observed for every failure class the
// platform answers, together with the contract the caller must get. The texts are
// verbatim worker replies (see .peri/plans/error-mapping-report.md).
var sqlFailureEvidence = []struct {
	name     string
	path     string
	text     string
	status   int
	code     string
	executed bool
	rejected bool
}{
	{"duration budget settled", "/v1/sql/query", "SQL execution settled: writer admission deadline: query duration budget exceeded", 504, "sql_timeout", true, false},
	{"duration budget rejected", "/v1/sql/execute", "SQL execution not entered: writer admission deadline: query duration budget exceeded", 504, "sql_timeout", false, true},
	{"compute budget", "/v1/sql/query", "SQL execution settled: VM step budget exceeded", 400, "sql_budget_exceeded", true, false},
	{"result row budget", "/v1/sql/query", "SQL execution settled: result row budget exceeded", 413, "result_limit", true, false},
	{"result byte budget", "/v1/sql/query", "SQL execution settled: result byte budget exceeded", 413, "result_limit", true, false},
	{"statement byte budget", "/v1/sql/execute", "SQL execution not entered: SQL byte budget exceeded", 413, "sql_limit", false, true},
	{"constraint", "/v1/sql/execute", "SQL execution settled: UNIQUE constraint failed: u.a", 400, "sql_error", true, false},
	{"parse error", "/v1/sql/execute", "SQL execution not entered: prepare remote query: Parse error: no such table: missing", 400, "sql_error", false, true},
	{"writer conflict", "/v1/sql/execute", "SQL execution not entered: writer admission conflict", 409, "sql_conflict", false, true},
	{"admission queue full", "/v1/sql/execute", "SQL execution not entered: writer admission queue full", 503, "sql_unavailable", false, true},
	{"cold load over budget", "/v1/sql/execute", "SQL execution not entered: database load budget exceeded (PERI_WORKER_LOAD_MS)", 503, "sql_unavailable", false, true},
	{"ownership fence", "/v1/sql/execute", "SQL execution not entered: local ownership revoked or detached", 503, "sql_unavailable", false, true},
	{"read only", "/v1/sql/execute", "SQL execution settled: attempt to write a readonly database", 403, "readonly_session", true, false},
	{"unclassified settled", "/v1/sql/execute", "SQL execution settled: private constraint details", 400, "sql_error", true, false},
	{"unclassified rejected", "/v1/sql/execute", "SQL execution not entered: private constraint details", 502, "upstream_rejected", false, true},
}

// TestSQLFailureClassification pins one client contract per upstream class. A
// mapping that collapses any of them back into one opaque code fails here.
func TestSQLFailureClassification(test *testing.T) {
	codes := map[string]bool{}
	for _, row := range sqlFailureEvidence {
		test.Run(row.name, func(test *testing.T) {
			failure, found := sqlWorkerFailure(testRPCError(row.executed, row.rejected, SQLRPCError{workerFailure: row.text}))
			if !found || failure.status != row.status || failure.code != row.code || failure.executed != row.executed || failure.rejected != row.rejected {
				test.Fatalf("classification changed: %+v", failure)
			}
			if failure.message == "" || strings.Contains(failure.message, row.text) || strings.Contains(failure.message, "private constraint details") {
				test.Fatalf("unsafe client message: %q", failure.message)
			}
			codes[failure.code] = true
		})
	}
	if len(codes) < 7 {
		test.Fatalf("distinct failure codes collapsed: %v", codes)
	}
}

// TestSQLFailureRequiresProvenEvidence keeps a reply that proves nothing about
// execution from becoming a definite answer.
func TestSQLFailureRequiresProvenEvidence(test *testing.T) {
	for _, reply := range []*SQLRPCError{
		nil,
		{Ambiguous: true, workerFailure: "SQL execution settled: query duration budget exceeded"},
		{workerFailure: "SQL execution settled: query duration budget exceeded"},
		testRPCError(true, true, SQLRPCError{workerFailure: "SQL execution settled: query duration budget exceeded"}),
	} {
		if _, found := sqlWorkerFailure(reply); found {
			test.Fatalf("unproven reply became definite: %+v", reply)
		}
	}
	settled := gatewayResult{code: "sql_timeout", rpc: testRPCError(true, false, SQLRPCError{})}
	rejected := gatewayResult{code: "sql_error", rpc: testRPCError(false, true, SQLRPCError{})}
	ambiguous := gatewayResult{code: "outcome_unknown", rpc: &SQLRPCError{Ambiguous: true}}
	for _, scenario := range []struct {
		result gatewayResult
		want   bool
	}{{gatewayResult{code: ""}, true}, {gatewayResult{code: "result_limit"}, true}, {settled, true}, {rejected, false}, {ambiguous, false}, {gatewayResult{code: "sql_limit"}, false}} {
		if sqlExecutedOutcome(scenario.result) != scenario.want {
			test.Fatalf("execution evidence misread for %s", scenario.result.code)
		}
	}
}

// TestGatewaySQLFailureMapping runs every failure class through the gateway so the
// caller contract, the evidence fields and the log stay aligned.
func TestGatewaySQLFailureMapping(test *testing.T) {
	for _, row := range sqlFailureEvidence {
		test.Run(row.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			secret := "private-store-key-9f2c"
			var logged bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logged)
			test.Cleanup(func() { log.SetOutput(previous) })
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
				case "close_session":
					_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"closed": true}))
				case "describe":
					_ = json.NewEncoder(writer).Encode(sqlTestDescribeReply(envelope.RequestID))
				case "execute", "query":
					reply := map[string]any{"version": 1, "request_id": envelope.RequestID, "ok": false, "result": nil, "error": row.text + " s3://bucket/objects/" + secret}
					if row.executed || row.rejected {
						reply["execution_completed"] = row.executed
					}
					_ = json.NewEncoder(writer).Encode(reply)
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
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, row.path, map[string]any{"session": session, "sql": "SELECT 1", "request_id": "failure-" + strings.ReplaceAll(row.name, " ", "-")})
			var reply struct {
				OK    bool `json:"ok"`
				Error struct {
					Code               string `json:"code"`
					Message            string `json:"message"`
					ExecutionCompleted *bool  `json:"execution_completed"`
					MayHaveCommitted   *bool  `json:"may_have_committed"`
				} `json:"error"`
			}
			if json.Unmarshal(response.Body.Bytes(), &reply) != nil || reply.OK || response.Code != row.status || reply.Error.Code != row.code {
				test.Fatalf("gateway answered %d %s for %s: %s", response.Code, reply.Error.Code, row.name, response.Body.String())
			}
			// The platform only proves an uncommitted statement for describe and
			// read cursors, so a settled query keeps the commit warning.
			if reply.Error.Message == "" || reply.Error.ExecutionCompleted == nil || reply.Error.MayHaveCommitted == nil ||
				*reply.Error.ExecutionCompleted != row.executed || *reply.Error.MayHaveCommitted != row.executed {
				test.Fatalf("gateway evidence mismatch for %s: %s", row.name, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "private constraint") || strings.Contains(response.Body.String(), secret) || strings.Contains(response.Body.String(), row.text) {
				test.Fatal("gateway response disclosed upstream text")
			}
			if !strings.Contains(logged.String(), row.text+" s3://bucket/objects/"+secret) {
				test.Fatal("gateway log dropped the upstream failure detail")
			}
		})
	}
}

// TestGatewaySQLFailureLeavesSuccessAndUnknownAlone pins the paths the mapping
// change must not touch: a successful statement keeps its shape, and a reply with
// no execution evidence stays an unknown outcome on a poisoned session.
func TestGatewaySQLFailureLeavesSuccessAndUnknownAlone(test *testing.T) {
	fixture := newGatewayFixture(test)
	var unproven bool
	var calls int
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
		case "close_session":
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"closed": true}))
		case "describe":
			_ = json.NewEncoder(writer).Encode(sqlTestDescribeReply(envelope.RequestID))
		case "query":
			calls++
			if !unproven {
				_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, [][]any{{1}}))
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": envelope.RequestID, "ok": false, "result": nil, "error": "SQL execution settled: private constraint details"})
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
	success := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "success-1"})
	if success.Code != 200 || gatewayCode(test, success) != "" || strings.Contains(success.Body.String(), "message") {
		test.Fatalf("success path changed: %d %s", success.Code, success.Body.String())
	}
	unproven = true
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "unproven-1"})
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" || !strings.Contains(response.Body.String(), `"possible_commit":true`) || strings.Contains(response.Body.String(), "private constraint") || calls != 2 {
		test.Fatalf("unproven outcome changed: %d %s", response.Code, response.Body.String())
	}
	// The poisoned session must be answered from the durable record, never by
	// dispatching the statement again.
	rejected := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "poisoned-1"})
	if rejected.Code != 502 || gatewayCode(test, rejected) != "outcome_unknown" || !strings.Contains(rejected.Body.String(), `"operation_id"`) || calls != 2 {
		test.Fatalf("unknown outcome did not isolate the session: %d %s", rejected.Code, rejected.Body.String())
	}
}
