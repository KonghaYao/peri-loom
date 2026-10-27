package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func firstCursorColumns() []any {
	return []any{map[string]any{"name": "id", "decltype": "INTEGER"}}
}

func TestCursorFirstWireRejectsPartialOldAndUnboundedReplies(t *testing.T) {
	for _, operation := range []string{"open_read_cursor_first", "open_write_cursor_first"} {
		for _, limit := range []int{0, 1, 256, 257} {
			command := map[string]any{"command": operation, "session": strings.Repeat("a", 64), "sql": "SELECT ?", "args": []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}}, "named_args": []namedSQLArgument{}, "max_rows": limit}
			encoded, _ := json.Marshal(command)
			fields := map[string]json.RawMessage{}
			_ = json.Unmarshal(encoded, &fields)
			if got := writerCommandValid(operation, encoded, fields); got != (limit >= 1 && limit <= 256) {
				t.Fatalf("%s limit=%d valid=%t", operation, limit, got)
			}
			delete(fields, "max_rows")
			if writerCommandValid(operation, encoded, fields) {
				t.Fatal("missing limit accepted")
			}
		}
	}
	for _, raw := range []string{
		`{"cols":[]}`,
		`{"cols":[],"rows":[],"done":true}`,
		`{"cols":[],"rows":[],"done":true,"affected_row_count":null,"last_insert_rowid":null}`,
		`{"cols":[],"rows":[],"done":false,"affected_row_count":null,"last_insert_rowid":null}`,
		`{"cols":[],"rows":[[1]],"done":true,"affected_row_count":1,"last_insert_rowid":1}`,
		`{"cols":[],"rows":[],"done":false,"done":true,"affected_row_count":1,"last_insert_rowid":1}`,
		`{"cols":[],"rows":[],"done":true,"affected_row_count":1,"last_insert_rowid":1,"extra":true}`,
	} {
		if validCursorFirstResponse(json.RawMessage(raw), 256, true) {
			t.Fatalf("invalid first page accepted: %s", raw)
		}
	}
	if !validCursorFirstResponse(json.RawMessage(`{"cols":[],"rows":[],"done":true,"affected_row_count":1,"last_insert_rowid":1}`), 256, true) {
		t.Fatal("zero-column write rejected")
	}
}

func TestCursorFirstCannotBeEnabledByPublicRequest(t *testing.T) {
	fixture := newGatewayFixture(t)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	body, _ := json.Marshal(map[string]any{"session": session, "sql": "SELECT 1", "args": []any{}, "named_args": []any{}, "max_rows": 256})
	request := httptest.NewRequest(http.MethodPost, "/v1/sql/cursor/open", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("X-Peri-Cursor-Start", "true")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 400 || fixture.upstream.count("open_read_cursor_first") != 0 {
		t.Fatalf("public request enabled execution: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaJSONCursorFirstSDKShapeUsesNativeAutocommitAndFrozenWriteIntent(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy_eight_rpcs", true: "snapshot_six_rpcs"}[snapshot], func(t *testing.T) {
			fixture := newGatewayFixture(t)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "describe":
					write := strings.HasPrefix(command["sql"].(string), "INSERT")
					cols := firstCursorColumns()
					if write {
						cols = []any{}
					}
					return map[string]any{"params": []any{}, "cols": cols, "is_readonly": !write, "is_explain": false}, nil
				case "open_write_cursor_first":
					state, err := fixture.state.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					fixture.upstream.mu.Lock()
					sent := append([]byte(nil), fixture.upstream.calls[len(fixture.upstream.calls)-1].encoded...)
					fixture.upstream.mu.Unlock()
					found := false
					for _, record := range state.Operations {
						if record.Action == "cursor_start" && record.State == "in_flight" && record.Digest == commandDigest(sent) {
							found = true
						}
					}
					if !found || command["max_rows"] != float64(256) {
						t.Fatal("write reached worker without complete frozen intent")
					}
					page := map[string]any{"cols": []any{}, "rows": []any{}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}
					if snapshot {
						page["autocommit"] = true
					}
					return page, nil
				case "open_read_cursor_first":
					page := map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true}
					if snapshot {
						page["autocommit"] = true
					}
					return page, nil
				case "session_autocommit":
					return map[string]any{"autocommit": true}, nil
				case "guarded_readonly_statement":
					return map[string]any{"cols": firstCursorColumns(), "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}, nil
				}
				return nil, nil
			})
			handler := newHranaHTTP(fixture.gateway)
			// Same shape as @tursodatabase/serverless 1.4.1: want_rows is true for
			// run/get alike, followed by its conditional autocommit SQL probe.
			body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(?)","args":[{"type":"integer","value":"1"}],"named_args":[],"want_rows":true}},{"stmt":{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":false},"condition":{"type":"is_autocommit"}}]}}`
			first := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
			if first.Code != 200 || strings.Count(first.Body.String(), `"type":"step_end"`) != 2 {
				t.Fatalf("write/probe failed: %d %s", first.Code, first.Body.String())
			}
			var header map[string]any
			_ = json.Unmarshal([]byte(strings.Split(first.Body.String(), "\n")[0]), &header)
			body = strings.Replace(body, `"baton":null`, `"baton":"`+header["baton"].(string)+`"`, 1)
			body = strings.Replace(body, "INSERT INTO records VALUES(?)", "SELECT id FROM records WHERE id=?", 1)
			second := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
			if second.Code != 200 || strings.Count(second.Body.String(), `"type":"row"`) != 1 || strings.Count(second.Body.String(), `"type":"step_end"`) != 2 {
				t.Fatalf("read/probe failed: %d %s", second.Code, second.Body.String())
			}
			autocommitCalls := 2
			if snapshot {
				autocommitCalls = 0
			}
			for command, want := range map[string]int{"describe": 2, "open_write_cursor_first": 1, "open_read_cursor_first": 1, "session_autocommit": autocommitCalls, "guarded_readonly_statement": 2, "open_write_cursor": 0, "open_read_cursor": 0, "fetch_write_cursor": 0, "fetch_read_cursor": 0, "close_write_cursor": 0, "close_read_cursor": 0} {
				if got := fixture.upstream.count(command); got != want {
					t.Errorf("%s count=%d want=%d", command, got, want)
				}
			}
			record := latestCursorOperation(t, fixture, "cursor_start")
			if record.CursorNativeOutcome != cursorOutcomeCommitted || record.State != "terminal" {
				t.Fatalf("first page commit not durable: %+v", record.OperationStatus)
			}
		})
	}
}

func TestHranaJSONCursorFirstUnknownNeverFallsBackAndKeepsOperation(t *testing.T) {
	for _, scenario := range []string{"lost", "old-worker", "missing-count"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "describe":
					return map[string]any{"params": []any{}, "cols": firstCursorColumns(), "is_readonly": false, "is_explain": false}, nil
				case "open_write_cursor_first":
					if scenario == "lost" {
						return nil, &SQLRPCError{Ambiguous: true}
					}
					if scenario == "old-worker" {
						return map[string]any{"cols": firstCursorColumns()}, nil
					}
					return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true, "affected_row_count": nil, "last_insert_rowid": 1}, nil
				}
				return nil, nil
			})
			body := `{"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(1) RETURNING id","want_rows":true}},{"stmt":{"sql":"INSERT INTO records VALUES(2)","want_rows":true}}]}}`
			response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
			if response.Code != 502 {
				t.Fatalf("unknown status=%d body=%s", response.Code, response.Body.String())
			}
			document := hranaV3JSONDocument(t, response)
			failure := document["error"].(map[string]any)
			record := assertCursorIsolated(t, fixture)
			if failure["operation_id"] != record.OperationID || failure["possible_commit"] != true || failure["retry_allowed"] != false {
				t.Fatalf("lost durable evidence: %v", failure)
			}
			for command, want := range map[string]int{"open_write_cursor_first": 1, "describe": 1, "open_write_cursor": 0, "fetch_write_cursor": 0} {
				if fixture.upstream.count(command) != want {
					t.Fatalf("replayed or continued %s", command)
				}
			}
		})
	}
}

func TestHranaJSONCursorFirstDoneDisconnectRemainsCommitted(t *testing.T) {
	fixture := newGatewayFixture(t)
	cursorWriteHook(fixture, func() (any, error) {
		return map[string]any{"rows": [][]any{{1}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}, nil
	}, func() (any, error) { t.Fatal("completed first page must not be aborted"); return nil, nil })
	handler := newHranaHTTP(fixture.gateway)
	body := `{"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(1) RETURNING id","want_rows":true}}]}}`
	request := httptest.NewRequest(http.MethodPost, hranaV3JSONPath+"/cursor", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := &failingCursorWriter{ResponseRecorder: httptest.NewRecorder(), maxWrites: 0}
	handler.ServeHTTP(response, request)
	record := latestCursorOperation(t, fixture, "cursor_start")
	if record.CursorNativeOutcome != cursorOutcomeCommitted || record.State != "terminal" || fixture.upstream.count("close_write_cursor") != 0 {
		t.Fatalf("lost committed first page: %+v", record.OperationStatus)
	}
	assertCursorNotIsolated(t, fixture)
}

func TestHranaJSONCursorFirstSettledStepFailureStopsBatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	cursorWriteHook(fixture, func() (any, error) { return nil, testRPCError(true, false, SQLRPCError{}) }, func() (any, error) { return map[string]any{"closed": true, "abort_proven": true}, nil })
	body := `{"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(1) RETURNING id","want_rows":true}},{"stmt":{"sql":"INSERT INTO records VALUES(2)","want_rows":true}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"type":"error"`) || !strings.Contains(response.Body.String(), `"code":"sql_error"`) || !strings.Contains(response.Body.String(), `"execution_completed":true`) || !strings.Contains(response.Body.String(), `"outcome_unknown":false`) || fixture.upstream.count("describe") != 1 || fixture.upstream.count("open_write_cursor_first") != 1 {
		t.Fatalf("stepped error continued batch: %d %s", response.Code, response.Body.String())
	}
	assertCursorNotIsolated(t, fixture)
}

func TestHranaJSONCursorFirstPrepareRejectionContinuesBatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	starts := 0
	cursorWriteHook(fixture, func() (any, error) {
		starts++
		if starts == 1 {
			return nil, testRPCError(false, true, SQLRPCError{workerFailure: "SQL execution not entered: SQL parameter count mismatch"})
		}
		return map[string]any{"rows": [][]any{{2}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 2}, nil
	}, func() (any, error) { return map[string]any{"closed": true, "abort_proven": true}, nil })
	body := `{"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(?) RETURNING id","want_rows":true}},{"stmt":{"sql":"INSERT INTO records VALUES(2) RETURNING id","want_rows":true}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != 200 || starts != 2 || strings.Count(response.Body.String(), `"type":"step_error"`) != 1 || strings.Count(response.Body.String(), `"type":"step_end"`) != 1 {
		t.Fatalf("prepare rejection changed continuation: %d %s", response.Code, response.Body.String())
	}
	assertCursorNotIsolated(t, fixture)
}
