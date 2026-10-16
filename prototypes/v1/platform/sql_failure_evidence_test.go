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

// sqlEvidenceRouting points the fixture's worker at a stub server and returns the
// upstream the gateway must use for the exchange.
func sqlEvidenceRouting(test *testing.T, fixture *gatewayFixture, server *httptest.Server) {
	test.Helper()
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
}

// TestGatewayStatusFailureClaimsNoExecution pins where the execution evidence may
// come from. The worker answers every SQL reply with HTTP 200 and everything else
// with a bare error body, so a non-200 answer proves that the request reached the
// worker and nothing about the statement. The status body below deliberately
// repeats a negative SQL reply: a gateway that reads the flags off the wire
// instead of off the worker's decoded evidence is caught here, and a future exit
// that answers a 4xx after dispatch cannot reintroduce a "proven rejected" claim.
func TestGatewayStatusFailureClaimsNoExecution(test *testing.T) {
	for _, status := range []int{302, 400, 401, 403, 404, 409, 429, 500, 503} {
		test.Run(fmt.Sprint(status), func(test *testing.T) {
			fixture := newGatewayFixture(test)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					RequestID string         `json:"request_id"`
					Command   map[string]any `json:"command"`
				}
				if json.NewDecoder(request.Body).Decode(&envelope) != nil {
					test.Error("invalid SQL request")
					return
				}
				if envelope.Command["command"] == "open_session" {
					_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"session": strings.Repeat("a", 64), "readonly": false}))
					return
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(status)
				_, _ = writer.Write([]byte(`{"version":1,"request_id":"` + envelope.RequestID + `","ok":false,"result":null,"error":"SQL execution settled: private constraint details","execution_completed":false,"may_have_committed":false}`))
			}))
			defer server.Close()
			sqlEvidenceRouting(test, fixture, server)
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "status-" + fmt.Sprint(status)})
			var reply struct {
				Error map[string]json.RawMessage `json:"error"`
			}
			if json.Unmarshal(response.Body.Bytes(), &reply) != nil {
				test.Fatalf("invalid gateway reply: %s", response.Body.String())
			}
			if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
				test.Fatalf("status %d claimed a definite outcome: %d %s", status, response.Code, response.Body.String())
			}
			for _, field := range []string{"execution_completed", "may_have_committed"} {
				if _, found := reply.Error[field]; found {
					test.Fatalf("status %d declared execution state: %s", status, response.Body.String())
				}
			}
			if string(reply.Error["possible_commit"]) != "true" {
				test.Fatalf("status %d hid the possible commit: %s", status, response.Body.String())
			}
		})
	}
}

// TestGatewayResultLimitDeclaresCommitRisk covers the platform's own result
// budget, which is enforced after the statement ran. A settled statement must
// carry the commit warning a worker budget failure carries: every client reads an
// omitted may_have_committed as "did not commit", which would tell a caller that a
// write it cannot account for is safe to replay. The verdict follows
// nonCommittingOperation, so a describe keeps its proven "cannot commit".
func TestGatewayResultLimitDeclaresCommitRisk(test *testing.T) {
	payload := strings.Repeat("x", 512)
	scenarios := []struct {
		name       string
		path       string
		command    string
		reply      any
		committing bool
	}{
		{"query", "/v1/sql/query", "query", [][]any{{payload}}, true},
		{"statement", "/v1/sql/statement", "statement", map[string]any{"cols": []any{map[string]any{"name": "payload", "decltype": "TEXT"}}, "rows": [][]any{{payload}}, "affected_row_count": 1, "last_insert_rowid": nil, "autocommit": true}, true},
		{"describe", "/v1/sql/describe", "describe", map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": payload, "decltype": "TEXT"}}, "is_explain": false, "is_readonly": true}, false},
	}
	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 64 })
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == scenario.command {
					return scenario.reply, nil
				}
				return nil, nil
			})
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, scenario.path, map[string]any{"session": session, "sql": "SELECT 1", "request_id": "limit-" + scenario.name})
			var reply struct {
				Error struct {
					Code               string `json:"code"`
					ExecutionCompleted *bool  `json:"execution_completed"`
					MayHaveCommitted   *bool  `json:"may_have_committed"`
				} `json:"error"`
			}
			if json.Unmarshal(response.Body.Bytes(), &reply) != nil || response.Code != 413 || reply.Error.Code != "result_limit" {
				test.Fatalf("result budget not enforced for %s: %d %s", scenario.name, response.Code, response.Body.String())
			}
			if reply.Error.ExecutionCompleted == nil || !*reply.Error.ExecutionCompleted {
				test.Fatalf("result limit dropped the settled verdict for %s: %s", scenario.name, response.Body.String())
			}
			if reply.Error.MayHaveCommitted == nil || *reply.Error.MayHaveCommitted != scenario.committing {
				test.Fatalf("result limit declared the wrong commit risk for %s: %s", scenario.name, response.Body.String())
			}
		})
	}
}

// TestGatewayAuditFailureKeepsCommitRisk covers the degraded reply an audit
// failure produces for a dispatched statement. The reply reports the audit
// failure, so the SQL outcome it replaced has to stay visible in the evidence
// fields: dropping the commit warning here would tell a caller that a write it
// cannot account for is safe to replay.
func TestGatewayAuditFailureKeepsCommitRisk(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.audit.mu.Lock()
	fixture.audit.failAction = "execute"
	fixture.audit.mu.Unlock()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO records VALUES(1)", "request_id": "audit-settled"})
	var reply struct {
		Error struct {
			Code               string `json:"code"`
			ExecutionCompleted *bool  `json:"execution_completed"`
			MayHaveCommitted   *bool  `json:"may_have_committed"`
		} `json:"error"`
	}
	if json.Unmarshal(response.Body.Bytes(), &reply) != nil || response.Code != 503 || reply.Error.Code != "audit_unavailable" {
		test.Fatalf("audit failure not reported: %d %s", response.Code, response.Body.String())
	}
	if reply.Error.ExecutionCompleted == nil || !*reply.Error.ExecutionCompleted || reply.Error.MayHaveCommitted == nil || !*reply.Error.MayHaveCommitted {
		test.Fatalf("audit failure dropped the commit risk: %s", response.Body.String())
	}
}

// TestSQLOpenObservationCoversTheFailureVocabulary keeps the observation's
// closed vocabulary aligned with the codes the platform answers a caller with. A
// class the whitelist does not know is observed without saying which failure it
// was, which is the blind spot that hid readonly_session, result_limit and
// sql_limit.
func TestSQLOpenObservationCoversTheFailureVocabulary(test *testing.T) {
	classes := map[string]bool{}
	for _, row := range sqlFailureEvidence {
		failure, found := sqlWorkerFailure(testRPCError(row.executed, row.rejected, SQLRPCError{workerFailure: row.text}))
		if !found {
			test.Fatalf("unclassified evidence: %s", row.name)
		}
		classes[failure.code] = true
	}
	for code := range classes {
		observer := NewSQLOpenObserver()
		observeSQLOpen(bindSQLOpenObservation(context.Background(), observer, "vocabulary"), "vocabulary", sqlOpenGateway, sqlOpenUpstreamResult, nil, code)
		record, received := observer.TryReceive()
		if !received || record.Classification != code {
			test.Fatalf("observer dropped the %s classification: %+v", code, record)
		}
	}
	if len(classes) < 7 {
		test.Fatalf("failure vocabulary collapsed: %v", classes)
	}
}

// TestSQLUpstreamStatusEvidenceIsUniform drives the transport directly, so the
// rule is pinned one layer below the gateway too: the wire answer of the worker
// never states what happened to a statement, and the caller keeps the ambiguous
// verdict that leaves the outcome unknown.
func TestSQLUpstreamStatusEvidenceIsUniform(test *testing.T) {
	for _, status := range []int{302, 400, 401, 403, 404, 409, 429, 500, 503} {
		test.Run(fmt.Sprint(status), func(test *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(status)
				_, _ = writer.Write([]byte(`{"code":"upstream_rejected","execution_completed":false,"may_have_committed":false}`))
			}))
			defer server.Close()
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
			if err != nil {
				test.Fatal(err)
			}
			failure := sqlTestFailure(test, upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "status-1", map[string]any{"command": "query"}, nil))
			if failure.StatusCode != status || !failure.Ambiguous || failure.evidence.Proven() || calls.Load() != 1 {
				test.Fatalf("status %d produced execution evidence or a retry: %+v calls=%d", status, failure, calls.Load())
			}
		})
	}
}
