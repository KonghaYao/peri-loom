package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

const hranaV3JSONPath = "/db/tenant-a/db-a/main/v3"

func hranaV3JSONCall(handler http.Handler, token, path, contentType, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func hranaV3JSONDocument(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatalf("invalid JSON response: %v: %s", err, response.Body.String())
	}
	return document
}

func TestHranaV3JSONPipelineReadBindAndV2Compatibility(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "session_autocommit" {
			return map[string]any{"autocommit": true}, nil
		}
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{map[string]any{"name": "answer", "decltype": "INTEGER"}}, "rows": [][]any{{int64(42)}}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway)
	body := `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT ? AS answer","args":[{"type":"integer","value":"42"}],"named_args":[],"want_rows":true}},{"type":"get_autocommit"},{"type":"close"}]}`
	for _, version := range []string{"v3", "v2"} {
		t.Run(version, func(t *testing.T) {
			path := strings.Replace(hranaV3JSONPath, "/v3", "/"+version, 1) + "/pipeline"
			response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", body)
			if response.Code != http.StatusOK {
				t.Fatalf("pipeline returned %d: %s", response.Code, response.Body.String())
			}
			result := hranaV3JSONDocument(t, response)
			entries, ok := result["results"].([]any)
			if !ok || len(entries) != 3 {
				t.Fatalf("expected one result per request: %v", result)
			}
			execute, ok := entries[0].(map[string]any)["response"].(map[string]any)
			if !ok || execute["type"] != "execute" {
				t.Fatalf("missing execute envelope: %v", entries[0])
			}
			statement, ok := execute["result"].(map[string]any)
			if !ok || statement["rows"] == nil || statement["cols"] == nil {
				t.Fatalf("missing statement result: %v", execute)
			}
			rows := statement["rows"].([]any)
			if len(rows) != 1 || len(rows[0].([]any)) != 1 {
				t.Fatalf("expected one integer row: %v", rows)
			}
			value, ok := rows[0].([]any)[0].(map[string]any)
			if !ok || value["type"] != "integer" || value["value"] != "42" {
				t.Fatalf("integer value is not a decimal string: %v", rows[0])
			}
			if entries[1].(map[string]any)["response"].(map[string]any)["is_autocommit"] != true || entries[2].(map[string]any)["response"].(map[string]any)["type"] != "close" {
				t.Fatalf("autocommit/close results missing: %v", entries)
			}
			if result["baton"] != nil {
				t.Fatalf("closed stream retained baton: %v", result["baton"])
			}
		})
	}
	if fixture.upstream.count("session_autocommit") != 2 || fixture.upstream.count("statement") != 2 || fixture.upstream.count("query") != 0 {
		t.Fatalf("unexpected native execution: autocommit=%d statements=%d queries=%d", fixture.upstream.count("session_autocommit"), fixture.upstream.count("statement"), fixture.upstream.count("query"))
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] == "statement" {
			if call.command["sql"] != "SELECT ? AS answer" {
				t.Fatalf("bound SQL was rewritten: %v", call.command)
			}
			args, ok := call.command["args"].([]any)
			if !ok || len(args) != 1 || args[0].(map[string]any)["value"] != "42" {
				t.Fatalf("binding lost: %v", call.command)
			}
		}
	}
}

func TestHranaV3JSONBatonAndAuthorizationIsolation(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway)
	path := hranaV3JSONPath + "/pipeline"
	read := `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}}]}`
	for _, token := range []string{"", "bogus", fixture.tokenB, fixture.viewer} {
		response := hranaV3JSONCall(handler, token, path, "application/json", read)
		if response.Code != http.StatusUnauthorized && response.Code != http.StatusForbidden {
			t.Fatalf("unauthorized SQL accepted: %d %s", response.Code, response.Body.String())
		}
	}
	if fixture.upstream.count("statement") != 0 {
		t.Fatal("unauthorized SQL reached the worker")
	}
	opened := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", read)
	if opened.Code != http.StatusOK {
		t.Fatalf("open failed: %d %s", opened.Code, opened.Body.String())
	}
	baton, ok := hranaV3JSONDocument(t, opened)["baton"].(string)
	if !ok || baton == "" {
		t.Fatalf("missing reusable baton: %s", opened.Body.String())
	}
	continued, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 2", "want_rows": true}}, map[string]any{"type": "close"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []struct{ token, path string }{{fixture.tokenB, "/db/tenant-b/db-b/main/v3/pipeline"}, {fixture.tokenA, "/db/tenant-a/db-a/other/v3/pipeline"}} {
		response := hranaV3JSONCall(handler, attempt.token, attempt.path, "application/json", string(continued))
		if response.Code == http.StatusOK {
			t.Fatalf("baton crossed identity/database boundary: %s", response.Body.String())
		}
	}
	if fixture.upstream.count("statement") != 1 {
		t.Fatal("denied baton executed SQL")
	}
	response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", string(continued))
	if response.Code != http.StatusOK || hranaV3JSONDocument(t, response)["baton"] != nil {
		t.Fatalf("baton could not be closed: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("statement") != 2 {
		t.Fatal("valid continuation did not execute exactly once")
	}
}

func TestHranaV3JSONCursorNDJSONAndBaton(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "answer", "decltype": "INTEGER"}}, "is_readonly": true, "is_explain": false}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": []any{map[string]any{"name": "answer", "decltype": "INTEGER"}}, "rows": [][]any{{int64(42)}}, "done": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway)
	path := hranaV3JSONPath + "/cursor"
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT ? AS answer","args":[{"type":"integer","value":"42"}],"named_args":[],"want_rows":true}}]}}`
	for _, token := range []string{"", fixture.tokenB} {
		denied := hranaV3JSONCall(handler, token, path, "application/json", body)
		if denied.Code != http.StatusUnauthorized && denied.Code != http.StatusForbidden {
			t.Fatalf("unauthorized cursor accepted: %d %s", denied.Code, denied.Body.String())
		}
	}
	if fixture.upstream.count("open_read_cursor_first") != 0 {
		t.Fatal("unauthorized cursor opened upstream")
	}
	response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", body)
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("cursor did not return NDJSON: %d %s %s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	lines := strings.Split(strings.TrimSpace(response.Body.String()), "\n")
	if len(lines) != 4 && len(lines) != 5 {
		t.Fatalf("expected header, step_begin, row, step_end and optional replication_index: %q", lines)
	}
	entries := make([]map[string]any, len(lines))
	for index, line := range lines {
		if err := json.Unmarshal([]byte(line), &entries[index]); err != nil {
			t.Fatalf("cursor line %d is not JSON: %v", index, err)
		}
	}
	baton, ok := entries[0]["baton"].(string)
	if !ok || baton == "" || entries[1]["type"] != "step_begin" || entries[1]["step"] != float64(0) || entries[2]["type"] != "row" || entries[3]["type"] != "step_end" {
		t.Fatalf("cursor entries incompatible with Rust decoder: %v", entries)
	}
	cols, ok := entries[1]["cols"].([]any)
	if !ok || len(cols) != 1 || cols[0].(map[string]any)["name"] != "answer" {
		t.Fatalf("missing step columns: %v", entries[1])
	}
	row, ok := entries[2]["row"].([]any)
	if !ok || len(row) != 1 {
		t.Fatalf("invalid cursor row: %v", entries[2])
	}
	value, ok := row[0].(map[string]any)
	if !ok || value["type"] != "integer" || value["value"] != "42" || entries[3]["affected_row_count"] == nil {
		t.Fatalf("invalid cursor value/end: %v", entries)
	}
	if len(entries) == 5 && entries[4]["type"] != "replication_index" {
		t.Fatalf("unknown cursor trailer: %v", entries[4])
	}
	closeBody, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{map[string]any{"type": "close"}}})
	if err != nil {
		t.Fatal(err)
	}
	closed := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", string(closeBody))
	if closed.Code != http.StatusOK || hranaV3JSONDocument(t, closed)["baton"] != nil {
		t.Fatalf("cursor baton cannot be closed via pipeline: %d %s", closed.Code, closed.Body.String())
	}
	if fixture.upstream.count("open_read_cursor_first") != 1 || fixture.upstream.count("open_read_cursor") != 0 || fixture.upstream.count("fetch_read_cursor") != 0 || fixture.upstream.count("statement") != 0 {
		t.Fatal("cursor bypassed native read cursor or executed twice")
	}
}

func TestHranaV3JSONLargeResultUsesNativePages(t *testing.T) {
	fixture := newGatewayFixture(t)
	const total = 10001
	produced := 0
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": true}, nil
		case "open_read_cursor_first", "fetch_read_cursor":
			rows := make([][]any, 0, 256)
			for len(rows) < 256 && produced < total {
				produced++
				rows = append(rows, []any{produced})
			}
			page := map[string]any{"rows": rows, "done": produced == total}
			if command["command"] == "open_read_cursor_first" {
				page["cols"] = []any{map[string]any{"name": "id", "decltype": "INTEGER"}}
			}
			return page, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		case "autocommit":
			return map[string]any{"is_autocommit": true}, nil
		}
		return nil, nil
	})
	request := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT id FROM records","args":[],"named_args":[],"want_rows":true}},{"condition":{"type":"is_autocommit"},"stmt":{"sql":"SELECT 1","want_rows":false}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", request)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"row"`) != total || fixture.upstream.count("fetch_read_cursor") != 39 || fixture.upstream.count("open_read_cursor_first") != 1 {
		t.Fatalf("v3 JSON cursor did not page %d rows: status=%d rows=%d fetches=%d starts=%d", total, response.Code, strings.Count(response.Body.String(), `"type":"row"`), fixture.upstream.count("fetch_read_cursor"), fixture.upstream.count("open_read_cursor_first"))
	}
}

func TestHranaV3JSONMalformedAndUnknownOutcomeNeverRetry(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway)
	for _, attempt := range []struct{ path, contentType, body string }{
		{hranaV3JSONPath + "/pipeline", "application/json", `{"baton":null,"requests":[`},
		{hranaV3JSONPath + "/pipeline", "application/json", `{"requests":[]}`},
		{hranaV3JSONPath + "/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]} {}`},
		{hranaV3JSONPath + "/pipeline", "text/plain", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`},
		{hranaV3JSONPath + "/cursor", "application/json", `{"batch":{"steps":[]}}`},
		{hranaV3JSONPath + "/cursor", "application/json", `{"batch":{"steps":[{"stmt":{"sql":"SELECT 1"}}]`},
		{hranaV3JSONPath + "/pipeline?retry=1", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`},
	} {
		response := hranaV3JSONCall(handler, fixture.tokenA, attempt.path, attempt.contentType, attempt.body)
		if response.Code == http.StatusOK {
			t.Fatalf("malformed request accepted: %s: %s", attempt.path, response.Body.String())
		}
	}
	if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 {
		t.Fatal("malformed request reached SQL worker")
	}
	invalidBinding := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT ?","args":[{"type":"integer","value":"not-an-integer"}],"want_rows":true}},{"type":"close"}]}`)
	if invalidBinding.Code != http.StatusOK {
		t.Fatalf("statement failure not returned as a pipeline result: %d %s", invalidBinding.Code, invalidBinding.Body.String())
	}
	results, ok := hranaV3JSONDocument(t, invalidBinding)["results"].([]any)
	if !ok || len(results) != 2 || results[0].(map[string]any)["type"] != "error" {
		t.Fatalf("expected per-step error result: %v", results)
	}
	if problem, ok := results[0].(map[string]any)["error"].(map[string]any); !ok || problem["message"] == "" {
		t.Fatalf("error has no protocol message: %v", results[0])
	}
	if fixture.upstream.count("statement") != 0 {
		t.Fatal("invalid binding executed on worker")
	}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return nil, errors.New("simulated uncertain native outcome")
		}
		return nil, nil
	})
	response := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`)
	if response.Code == http.StatusOK || fixture.upstream.count("statement") != 1 {
		t.Fatalf("unknown outcome was retried or hidden: status=%d calls=%d body=%s", response.Code, fixture.upstream.count("statement"), response.Body.String())
	}
}

func TestHranaQueryMetricsJSONPipeline(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		reply   map[string]any
		metrics bool
	}{
		{
			name: "new worker exposes counters",
			reply: map[string]any{
				"cols": []any{}, "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true,
				"rows_read": uint64(8625), "rows_written": uint64(0), "query_duration_ms": 955.25,
			},
			metrics: true,
		},
		{
			name:  "legacy worker keeps its shape",
			reply: map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			reply := scenario.reply
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "statement" {
					return reply, nil
				}
				return nil, nil
			})
			handler := newHranaHTTP(fixture.gateway)
			body := `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"close"}]}`
			for _, version := range []string{"v3", "v2"} {
				t.Run(version, func(t *testing.T) {
					path := strings.Replace(hranaV3JSONPath, "/v3", "/"+version, 1) + "/pipeline"
					response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", body)
					if response.Code != http.StatusOK {
						t.Fatalf("pipeline returned %d: %s", response.Code, response.Body.String())
					}
					document := hranaV3JSONDocument(t, response)
					entries, ok := document["results"].([]any)
					if !ok || len(entries) == 0 {
						t.Fatalf("missing pipeline results: %v", document)
					}
					execute, ok := entries[0].(map[string]any)["response"].(map[string]any)
					if !ok || execute["type"] != "execute" {
						t.Fatalf("missing execute envelope: %v", entries[0])
					}
					statement, ok := execute["result"].(map[string]any)
					if !ok {
						t.Fatalf("missing statement result: %v", execute)
					}
					read, hasRead := statement["rows_read"]
					written, hasWritten := statement["rows_written"]
					duration, hasDuration := statement["query_duration_ms"]
					if !scenario.metrics {
						if hasRead || hasWritten || hasDuration || len(statement) != 4 {
							t.Fatalf("legacy statement result changed shape: %v", statement)
						}
						return
					}
					if !hasRead || read != float64(8625) || !hasWritten || written != float64(0) || !hasDuration || duration != 955.25 || len(statement) != 7 {
						t.Fatalf("execution counters missing or wrong: %v", statement)
					}
				})
			}
		})
	}
}

// A worker reply that names a counter but sends JSON null must not decay into
// the legacy five-field shape: the public validator rejects null, so publishing
// the legacy shape would hand a customer a result the gateway itself refuses.
func TestHranaStatementMetricsRejectJSONNull(t *testing.T) {
	for _, transport := range []string{"json", "protobuf"} {
		t.Run(transport, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "statement" {
					return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true,
						"rows_read": nil, "rows_written": uint64(0), "query_duration_ms": 1.5}, nil
				}
				return nil, nil
			})
			handler := newHranaHTTP(fixture.gateway)
			var response *httptest.ResponseRecorder
			if transport == "json" {
				response = hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"close"}]}`)
			} else {
				response = testHranaV3Call(t, handler.(*hranaHTTP), fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", hranaProtoBytes(2, testHranaV3Statement()))
			}
			if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "outcome_unknown") {
				t.Fatalf("null metric was accepted: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHranaNativeStatementMetricsShapes(t *testing.T) {
	for _, scenario := range []struct {
		name             string
		read, written    json.RawMessage
		duration         json.RawMessage
		accepted, legacy bool
	}{
		{"absent", nil, nil, nil, true, true},
		{"present", json.RawMessage("8625"), json.RawMessage("0"), json.RawMessage("955.25"), true, false},
		{"null rows_read", json.RawMessage("null"), json.RawMessage("0"), json.RawMessage("1.5"), false, false},
		{"null rows_written", json.RawMessage("1"), json.RawMessage("null"), json.RawMessage("1.5"), false, false},
		{"null query_duration_ms", json.RawMessage("1"), json.RawMessage("0"), json.RawMessage("null"), false, false},
		{"partial", json.RawMessage("1"), nil, json.RawMessage("1.5"), false, false},
		{"exponent rows_read", json.RawMessage("1e3"), json.RawMessage("0"), json.RawMessage("1.5"), false, false},
		{"negative query_duration_ms", json.RawMessage("1"), json.RawMessage("0"), json.RawMessage("-0.5"), false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			metrics, ok := hranaNativeStatementMetrics(scenario.read, scenario.written, scenario.duration)
			if ok != scenario.accepted || (ok && (metrics == nil) != scenario.legacy) {
				t.Fatalf("metrics=%v ok=%v, want accepted=%v legacy=%v", metrics, ok, scenario.accepted, scenario.legacy)
			}
			if ok && !scenario.legacy {
				if metrics["rows_read"] != uint64(8625) || metrics["rows_written"] != uint64(0) || metrics["query_duration_ms"] != 955.25 {
					t.Fatalf("metrics = %v", metrics)
				}
			}
		})
	}
}

func TestHranaNativeStatementNullCountersStayDistinguishable(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"rows_read":null,"rows_written":0,"query_duration_ms":1.5}`))
	decoder.UseNumber()
	var native hranaNativeStatement
	if err := decoder.Decode(&native); err != nil {
		t.Fatal(err)
	}
	if native.RowsRead == nil || string(native.RowsRead) != "null" {
		t.Fatalf("null rows_read decoded as %q; it must stay distinguishable from an absent key", native.RowsRead)
	}
	if _, ok := hranaNativeStatementMetrics(native.RowsRead, native.RowsWritten, native.QueryDurationMS); ok {
		t.Fatal("null rows_read was accepted as a counter")
	}
	if native.RowsWritten == nil || string(native.RowsWritten) != "0" {
		t.Fatalf("rows_written decoded as %q", native.RowsWritten)
	}
}
