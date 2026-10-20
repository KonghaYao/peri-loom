package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	controller "peri-loom/controller"
)

const hranaThreadsWorkerFailure = "SQL execution not entered: prepare remote query: Parse error: no such table: threads"
const hranaPreparedFailureMessage = "statement could not be prepared (SQL syntax, schema or statement shape)"

// hranaThreadsFailureFixture answers a prepared statement the way the worker
// does for an empty database: the statement and its describe probe are both
// rejected before execution.
func hranaThreadsFailureFixture(test *testing.T) *gatewayFixture {
	test.Helper()
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "statement", "describe":
			return nil, testRPCError(false, true, SQLRPCError{workerFailure: hranaThreadsWorkerFailure})
		}
		return nil, nil
	})
	return fixture
}

func hranaAssertPreparedFailure(test *testing.T, body string, code, message any) {
	test.Helper()
	if code != "sql_error" {
		test.Fatalf("step error code = %v, want sql_error: %s", code, body)
	}
	if message != hranaPreparedFailureMessage {
		test.Fatalf("step error message = %v, want the platform classification: %s", message, body)
	}
	if strings.Contains(body, "no such table") {
		test.Fatalf("worker text leaked to the client: %s", body)
	}
}

func TestHranaPipelineForwardsPlatformFailureMessage(t *testing.T) {
	fixture := hranaThreadsFailureFixture(t)
	handler := newHranaHTTP(fixture.gateway)
	body := `{"requests":[{"type":"execute","stmt":{"sql":"SELECT threads","want_rows":true}},{"type":"close"}]}`
	for _, version := range []string{"v2", "v3"} {
		version := version
		t.Run(version, func(t *testing.T) {
			path := "/db/tenant-a/db-a/main/" + version + "/pipeline"
			response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", body)
			if response.Code != http.StatusOK {
				t.Fatalf("pipeline returned %d: %s", response.Code, response.Body.String())
			}
			document := hranaV3JSONDocument(t, response)
			results, ok := document["results"].([]any)
			if !ok || len(results) == 0 {
				t.Fatalf("pipeline lost the step result: %v", document)
			}
			problem, ok := results[0].(map[string]any)["error"].(map[string]any)
			if !ok {
				t.Fatalf("pipeline lost the step error: %v", results[0])
			}
			hranaAssertPreparedFailure(t, response.Body.String(), problem["code"], problem["message"])
		})
	}
}

func TestHranaV3JSONCursorForwardsPlatformFailureMessage(t *testing.T) {
	fixture := hranaThreadsFailureFixture(t)
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT threads","want_rows":true}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != http.StatusOK {
		t.Fatalf("cursor returned %d: %s", response.Code, response.Body.String())
	}
	var stepError map[string]any
	for _, line := range strings.Split(strings.TrimSpace(response.Body.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("cursor line is not JSON: %v: %s", err, line)
		}
		if entry["type"] == "step_error" {
			stepError = entry
		}
	}
	if stepError == nil {
		t.Fatalf("cursor lost the step error: %s", response.Body.String())
	}
	problem, ok := stepError["error"].(map[string]any)
	if !ok {
		t.Fatalf("cursor step error has no failure object: %v", stepError)
	}
	hranaAssertPreparedFailure(t, response.Body.String(), problem["code"], problem["message"])
}

func TestHranaUnknownDescribeFailureKeepsPlatformMessage(t *testing.T) {
	body := []byte(`{"ok":false,"result":null,"error":{"code":"outcome_unknown","message":"platform-side outcome is unknown","execution_proven":false,"retry_allowed":false}}`)
	reply := hranaGatewayReply(http.StatusBadGateway, body, "/v1/sql/describe")
	if !reply.unknown || reply.failure == nil {
		t.Fatalf("fixture did not produce an unknown platform failure: %+v", reply)
	}
	fatal := hranaFatalDescribeReply(reply)
	if fatal.status != http.StatusBadGateway || !fatal.unknown || fatal.failure != reply.failure {
		t.Fatalf("fatal describe reply lost the platform failure: %+v", fatal)
	}
	fields := hranaFailureFields(fatal, true)
	if fields["code"] != "outcome_unknown" || fields["message"] != "platform-side outcome is unknown" || fields["outcome_unknown"] != true {
		t.Fatalf("pipeline failure repeated the code instead of the platform message: %v", fields)
	}
}

func TestHranaWebSocketCursorStepErrorForwardsPlatformMessage(t *testing.T) {
	fixture := hranaThreadsFailureFixture(t)
	server := httptest.NewServer(newHranaHTTP(fixture.gateway))
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{"hrana3"}, HandshakeTimeout: 5 * time.Second}
	connection, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/db/tenant-a/db-a/main/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	exchange := func(message map[string]any) map[string]any {
		t.Helper()
		if err := connection.WriteJSON(message); err != nil {
			t.Fatal(err)
		}
		var reply map[string]any
		if err := connection.ReadJSON(&reply); err != nil {
			t.Fatal(err)
		}
		return reply
	}
	if reply := exchange(map[string]any{"type": "hello", "jwt": fixture.tokenA}); reply["type"] != "hello_ok" {
		t.Fatalf("hello failed: %v", reply)
	}
	if reply := exchange(map[string]any{"type": "request", "request_id": 0, "request": map[string]any{"type": "open_stream", "stream_id": 1}}); reply["type"] != "response_ok" {
		t.Fatalf("stream open failed: %v", reply)
	}
	opened := exchange(map[string]any{"type": "request", "request_id": 1, "request": map[string]any{
		"type": "open_cursor", "stream_id": 1, "cursor_id": 7,
		"batch": map[string]any{"steps": []any{map[string]any{"stmt": map[string]any{"sql": "SELECT threads", "want_rows": true}}}},
	}})
	if opened["type"] != "response_ok" {
		t.Fatalf("cursor open failed: %v", opened)
	}
	fetched := exchange(map[string]any{"type": "request", "request_id": 2, "request": map[string]any{"type": "fetch_cursor", "cursor_id": 7, "max_count": 100}})
	response, ok := fetched["response"].(map[string]any)
	if fetched["type"] != "response_ok" || !ok {
		t.Fatalf("cursor fetch failed: %v", fetched)
	}
	entries, ok := response["entries"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("cursor fetch lost the step error: %v", response)
	}
	stepError, ok := entries[0].(map[string]any)
	if !ok || stepError["type"] != "step_error" {
		t.Fatalf("cursor entry is not a step error: %v", entries[0])
	}
	problem, ok := stepError["error"].(map[string]any)
	if !ok {
		t.Fatalf("cursor step error has no failure object: %v", stepError)
	}
	encoded, err := json.Marshal(fetched)
	if err != nil {
		t.Fatal(err)
	}
	hranaAssertPreparedFailure(t, string(encoded), problem["code"], problem["message"])
}
