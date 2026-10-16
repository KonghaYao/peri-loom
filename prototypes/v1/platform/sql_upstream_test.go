package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func sqlTestToken(test *testing.T) string {
	test.Helper()
	token, err := controller.RandomID()
	if err != nil {
		test.Fatal(err)
	}
	return token
}
func sqlTestFailure(test *testing.T, err error) *SQLRPCError {
	test.Helper()
	var failure *SQLRPCError
	if !errors.As(err, &failure) {
		test.Fatalf("expected SQLRPCError, got %T", err)
	}
	return failure
}
func sqlTestEnvelope(requestID string, result any) map[string]any {
	return map[string]any{"version": 1, "request_id": requestID, "ok": true, "result": result, "error": nil}
}

// sqlTestDescribeReply is the worker's answer to the read-only classification
// probe the gateway sends before a dispatched statement. These fixtures pin the
// committing path of a dispatch, so the verdict they answer is "not read-only":
// the statement and its operation records stay exactly what they were before the
// probe existed.
func sqlTestDescribeReply(requestID string) map[string]any {
	return sqlTestEnvelope(requestID, map[string]any{"params": []any{}, "cols": []any{}, "is_explain": false, "is_readonly": false})
}

// testDescribeVerdict models the worker's read-only answer for a fixture: a
// statement that reads is read-only, every other statement is not. The platform
// never reads the statement text; this is the worker stand-in that does.
func testDescribeVerdict(sql string) map[string]any {
	return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT")}
}

func TestSQLUpstreamActualWireAndNoControlCapability(test *testing.T) {
	token := sqlTestToken(test)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope struct {
			Version   uint32         `json:"version"`
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != 1 || request.URL.Path != "/sql" || request.Method != "POST" || request.Header.Get("Authorization") != "Bearer "+token {
			test.Error("wrong Rust SQL request envelope or SQL credential")
		}
		if envelope.Command["command"] != "query" || envelope.Command["sql"] != "UPDATE t SET value=1 RETURNING value" {
			test.Error("query may-write SQL modified")
		}
		_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, [][]any{{1}}))
	}))
	defer server.Close()
	tokens := map[string]string{"worker-a": token}
	upstream, err := NewSQLUpstream(tokens)
	if err != nil {
		test.Fatal(err)
	}
	tokens["worker-a"] = sqlTestToken(test)
	worker := controller.Worker{ID: "worker-a", Address: server.URL}
	var rows [][]int
	if err := upstream.Call(context.Background(), worker, "sql-1", map[string]any{"command": "query", "session": strings.Repeat("a", 64), "sql": "UPDATE t SET value=1 RETURNING value"}, &rows); err != nil || len(rows) != 1 || rows[0][0] != 1 {
		test.Fatal("genuine SQL wire failed")
	}
	for _, command := range []string{"ownership", "grant", "shutdown"} {
		if failure := sqlTestFailure(test, upstream.Call(context.Background(), worker, "deny-1", map[string]any{"command": command}, nil)); failure.Ambiguous {
			test.Fatal("local privilege rejection ambiguous")
		}
	}
	if calls.Load() != 1 {
		test.Fatal("control operation reached SQL wire")
	}
}

func TestSQLUpstreamStrictResponsesAndNull(test *testing.T) {
	for _, scenario := range []string{"null", "version", "identity", "extra", "missing", "precondition", "duplicate", "nested-duplicate", "malformed", "rejected", "output"} {
		test.Run(scenario, func(test *testing.T) {
			secret := sqlTestToken(test)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					RequestID string `json:"request_id"`
				}
				_ = json.NewDecoder(request.Body).Decode(&envelope)
				reply := sqlTestEnvelope(envelope.RequestID, nil)
				switch scenario {
				case "version":
					reply["version"] = 2
				case "identity":
					reply["request_id"] = "wrong"
				case "extra":
					reply["unknown"] = secret
				case "missing":
					delete(reply, "error")
				case "precondition":
					reply["precondition_failed"] = false
				case "rejected":
					reply["ok"] = false
					reply["error"] = secret
				case "output":
					reply["result"] = map[string]any{"wrong": true}
				}
				body, _ := json.Marshal(reply)
				switch scenario {
				case "duplicate":
					body = []byte(fmt.Sprintf(`{"version":1,"version":1,"request_id":%q,"ok":true,"result":null,"error":null}`, envelope.RequestID))
				case "nested-duplicate":
					body = []byte(fmt.Sprintf(`{"version":1,"request_id":%q,"ok":true,"result":{"x":1,"x":2},"error":null}`, envelope.RequestID))
				case "malformed":
					body = []byte(`{`)
				}
				_, _ = writer.Write(body)
			}))
			defer server.Close()
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": secret})
			if err != nil {
				test.Fatal(err)
			}
			var output struct {
				Value int `json:"value"`
			}
			err = upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "strict-1", map[string]any{"command": "execute"}, &output)
			if scenario == "null" {
				if err != nil {
					test.Fatal("valid null rejected")
				}
				return
			}
			failure := sqlTestFailure(test, err)
			if !failure.Ambiguous || failure.evidence.Rejected() || strings.Contains(fmt.Sprintf("%+v", err), secret) {
				test.Fatal("unsafe response classification or credential leak")
			}
		})
	}
}

func TestSQLUpstreamLostCommitReplyNeverRetries(test *testing.T) {
	var applied atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		applied.Add(1)
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			test.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	err = upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "commit-1", map[string]any{"command": "execute", "session": strings.Repeat("a", 64), "sql": "COMMIT"}, nil)
	if !sqlTestFailure(test, err).Ambiguous || applied.Load() != 1 {
		test.Fatal("lost mutation reply retried or claimed rejection")
	}
}

func TestSQLUpstreamRedirectUnauthorizedAndBounds(test *testing.T) {
	for _, status := range []int{302, 403, 500} {
		test.Run(fmt.Sprint(status), func(test *testing.T) {
			var redirected atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Location", target.URL)
				writer.WriteHeader(status)
			}))
			defer server.Close()
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
			if err != nil {
				test.Fatal(err)
			}
			failure := sqlTestFailure(test, upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "status-1", map[string]any{"command": "query"}, nil))
			if redirected.Load() != 0 || failure.StatusCode != status || failure.evidence.Rejected() || !failure.Ambiguous {
				test.Fatal("HTTP outcome or redirect boundary incorrect")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", fmt.Sprint(sqlWireLimit+1))
		writer.WriteHeader(200)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	if !sqlTestFailure(test, upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "bound-1", map[string]any{"command": "query"}, nil)).Ambiguous {
		test.Fatal("oversized response accepted")
	}
}

func TestSQLUpstreamEndpointCredentialsAndCanceledContext(test *testing.T) {
	for _, endpoint := range []string{"http://localhost", "https://127.0.0.1", "http://127.0.0.1/sql", "http://user@127.0.0.1", "http://127.0.0.1?q=secret", "http://127.0.0.1#"} {
		if _, err := sqlEndpoint(endpoint); err == nil {
			test.Fatal("unsafe endpoint accepted")
		}
	}
	token := sqlTestToken(test)
	for _, tokens := range []map[string]string{{"worker": "short"}, {"worker-a": token, "worker-b": token}, {"bad worker": token}} {
		if _, err := NewSQLUpstream(tokens); err == nil {
			test.Fatal("invalid credentials accepted")
		}
	}
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": token})
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = upstream.Call(ctx, controller.Worker{ID: "worker-a", Address: "http://127.0.0.1:1"}, "cancel-1", map[string]any{"command": "query"}, nil)
	if failure := sqlTestFailure(test, err); failure.Ambiguous || !errors.Is(err, context.Canceled) {
		test.Fatal("canceled request dispatched")
	}
}

func TestGatewayActualSQLWireLostCommitReply(test *testing.T) {
	fixture := newGatewayFixture(test)
	var applied atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		_ = json.NewDecoder(request.Body).Decode(&envelope)
		if envelope.Command["command"] == "open_session" {
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"session": strings.Repeat("a", 64), "readonly": false}))
			return
		}
		if envelope.Command["command"] == "describe" {
			_ = json.NewEncoder(writer).Encode(sqlTestDescribeReply(envelope.RequestID))
			return
		}
		if envelope.Command["command"] != "execute" {
			test.Error("poisoned native session dispatched again")
			return
		}
		applied.Add(1)
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			test.Error(err)
			return
		}
		_ = connection.Close()
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
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT", "request_id": "wire-commit"})
	if gatewayCode(test, response) != "outcome_unknown" || applied.Load() != 1 {
		test.Fatal("gateway actual HTTP lost reply not failclosed")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "outcome_unknown" || applied.Load() != 1 {
		test.Fatal("poisoned wire COMMIT was replayed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if fixture.gateway.Close(ctx) == nil {
		test.Fatal("unknown HTTP side effect fake-cleaned")
	}
}
