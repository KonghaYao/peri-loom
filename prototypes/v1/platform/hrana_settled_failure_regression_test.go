package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	controller "peri-loom/controller"
)

// settledDescribeFailure points the fixture at a worker whose describe fails
// after execution with the deadline cause. The platform answers that with a
// settled, non-committing failure: describe only prepares a statement, so
// sqlExecutionVerdict writes execution_completed=true with
// may_have_committed=false.
func settledDescribeFailure(test *testing.T) *gatewayFixture {
	test.Helper()
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			return nil, testRPCError(true, false, SQLRPCError{workerFailure: "SQL execution settled: writer admission deadline: query duration budget exceeded"})
		}
		return nil, nil
	})
	return fixture
}

// TestHranaDescribeSettledFailureKeepsThePipeline pins the behaviour that
// regressed when the adapter re-derived "did the platform prove this?" from the
// two evidence fields as "completed == committed": a describe that settled and
// was proven unable to commit is a proved outcome, so the pipeline must report a
// step error and continue, exactly as the Go, TypeScript, browser and console
// clients read the same reply.
func TestHranaDescribeSettledFailureKeepsThePipeline(test *testing.T) {
	fixture := settledDescribeFailure(test)
	body := `{"requests":[{"type":"describe","sql":"SELECT 1"},{"type":"close"}]}`
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"code":"sql_timeout"`) {
		test.Fatalf("settled describe failure aborted the pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("close_session") != 1 {
		test.Fatalf("pipeline stopped after a proved failure: %s", response.Body.String())
	}
}

// TestHranaDescribeSettledFailureReachesTheRestEndpoint keeps the client
// contract on the same reply: /v1/sql/describe answers the settled failure with
// its code and its evidence, so a caller can tell a proved outcome from an
// unresolved one.
func TestHranaDescribeSettledFailureReachesTheRestEndpoint(test *testing.T) {
	fixture := settledDescribeFailure(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "describe-settled"})
	if response.Code != 504 || gatewayCode(test, response) != "sql_timeout" {
		test.Fatalf("describe reply lost its code: %d %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if !strings.Contains(body, `"execution_completed":true`) || !strings.Contains(body, `"may_have_committed":false`) {
		test.Fatalf("describe reply lost its proof: %s", body)
	}
}

// TestHranaWebSocketDescribeSettledFailureKeepsTheConnection is the same
// scenario over the WebSocket adapter, where a wrong answer does not abort a
// pipeline but drops the whole connection.
func TestHranaWebSocketDescribeSettledFailureKeepsTheConnection(test *testing.T) {
	fixture := settledDescribeFailure(test)
	server := httptest.NewServer(newHranaHTTP(fixture.gateway))
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{"hrana3", "hrana2"}, HandshakeTimeout: 5 * time.Second}
	connection, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/db/tenant-a/db-a/main/", nil)
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	exchange := func(message map[string]any) map[string]any {
		test.Helper()
		if err := connection.WriteJSON(message); err != nil {
			test.Fatal(err)
		}
		var reply map[string]any
		if err := connection.ReadJSON(&reply); err != nil {
			test.Fatalf("a proved failure closed the WebSocket: %v", err)
		}
		return reply
	}
	if reply := exchange(map[string]any{"type": "hello", "jwt": fixture.tokenA}); reply["type"] != "hello_ok" {
		test.Fatalf("hello failed: %v", reply)
	}
	if reply := exchange(map[string]any{"type": "request", "request_id": 0, "request": map[string]any{"type": "open_stream", "stream_id": 1}}); reply["type"] != "response_ok" {
		test.Fatalf("open failed: %v", reply)
	}
	// Receiving any well-formed reply proves the connection survived: the adapter
	// answers a proved failure with a step error and keeps the WebSocket, and
	// answers an unproven one by dropping it.
	reply := exchange(map[string]any{"type": "request", "request_id": 1, "request": map[string]any{"type": "describe", "stream_id": 1, "sql": "SELECT 1"}})
	if reply["type"] != "response_error" || reply["error"].(map[string]any)["code"] != "sql_timeout" {
		test.Fatalf("settled describe failure was not a step error: %v", reply)
	}
	if fixture.upstream.count("close_session") != 0 {
		test.Fatal("a proved failure tore down the session")
	}
}
