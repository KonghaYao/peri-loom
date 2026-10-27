package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func cursorSnapshotStep(sql string, rows bool, condition any) map[string]any {
	step := map[string]any{"stmt": map[string]any{"sql": sql, "want_rows": rows}}
	if condition != nil {
		step["condition"] = condition
	}
	return step
}

func cursorSnapshotBody(baton any, steps ...map[string]any) string {
	body, _ := json.Marshal(map[string]any{"baton": baton, "batch": map[string]any{"steps": steps}})
	return string(body)
}

func cursorSnapshotDescription() map[string]any {
	return map[string]any{"params": []any{}, "cols": firstCursorColumns(), "is_readonly": true, "is_explain": false}
}

func cursorSnapshotStatement() map[string]any {
	return map[string]any{"cols": firstCursorColumns(), "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}
}

func TestCursorFirstAutocommitOptionalButNeverCoerced(t *testing.T) {
	for _, sample := range []struct {
		suffix string
		done   bool
		valid  bool
	}{
		{"", true, true}, {`,"autocommit":true`, true, true}, {`,"autocommit":false`, true, true},
		{`,"autocommit":null`, true, false}, {`,"autocommit":"false"`, true, false}, {`,"autocommit":0`, true, false},
		{`,"autocommit":true,"autocommit":false`, true, false},
		{"", false, true}, {`,"autocommit":null`, false, true}, {`,"autocommit":false`, false, false}, {`,"autocommit":true`, false, false},
	} {
		done := "false"
		if sample.done {
			done = "true"
		}
		raw := `{"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[[1]],"done":` + done + sample.suffix + `}`
		if validCursorFirstResponse(json.RawMessage(raw), 256, false) != sample.valid {
			t.Fatalf("snapshot shape accepted incorrectly: %s", raw)
		}
	}
}

func TestHranaCursorSnapshotUsesNativeTrueFalseAndNestedCondition(t *testing.T) {
	for _, snapshot := range []bool{true, false} {
		for _, invert := range []bool{false, true} {
			name := map[bool]string{false: "transaction", true: "autocommit"}[snapshot] + map[bool]string{false: "_plain", true: "_not"}[invert]
			t.Run(name, func(t *testing.T) {
				fixture := newGatewayFixture(t)
				fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
					switch command["command"] {
					case "describe":
						return cursorSnapshotDescription(), nil
					case "open_read_cursor_first":
						return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true, "autocommit": snapshot}, nil
					case "session_autocommit":
						t.Error("completed cursor unnecessarily probed connection state")
						return map[string]any{"autocommit": !snapshot}, nil
					case "guarded_readonly_statement":
						return cursorSnapshotStatement(), nil
					}
					return nil, nil
				})
				condition := map[string]any{"type": "is_autocommit"}
				if invert {
					condition = map[string]any{"type": "not", "cond": condition}
				}
				body := cursorSnapshotBody(nil, cursorSnapshotStep("SELECT 1", true, nil), cursorSnapshotStep("SELECT 2", false, condition))
				response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
				expected := 0
				if snapshot != invert {
					expected = 1
				}
				if response.Code != 200 || fixture.upstream.count("guarded_readonly_statement") != expected || fixture.upstream.count("session_autocommit") != 0 {
					t.Fatalf("native bool changed condition: %d %s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestHranaCursorSnapshotIsConsumedBeforeAnyInterveningStep(t *testing.T) {
	for _, scenario := range []string{"statement", "prepare_error", "skipped_condition", "multipage", "legacy"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "describe":
					return cursorSnapshotDescription(), nil
				case "open_read_cursor_first":
					page := map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true, "autocommit": true}
					if scenario == "multipage" {
						page["done"], page["autocommit"] = false, nil
					}
					if scenario == "legacy" {
						delete(page, "autocommit")
					}
					return page, nil
				case "fetch_read_cursor":
					return map[string]any{"rows": []any{}, "done": true}, nil
				case "session_autocommit":
					return map[string]any{"autocommit": false}, nil
				case "guarded_readonly_statement":
					if scenario == "prepare_error" {
						return nil, testRPCError(false, true, SQLRPCError{workerFailure: "SQL execution not entered: SQL parameter count mismatch"})
					}
					return map[string]any{"requires_intent": true, "reason": "connection_control"}, nil
				case "statement":
					result := cursorSnapshotStatement()
					result["autocommit"] = false
					return result, nil
				}
				return nil, nil
			})
			steps := []map[string]any{cursorSnapshotStep("SELECT 1", true, nil)}
			switch scenario {
			case "statement":
				steps = append(steps, cursorSnapshotStep("BEGIN", false, nil))
			case "prepare_error":
				steps = append(steps, cursorSnapshotStep("SELECT ?", false, nil))
			case "skipped_condition":
				steps = append(steps, cursorSnapshotStep("SELECT skipped", false, map[string]any{"type": "not", "cond": map[string]any{"type": "is_autocommit"}}))
			}
			steps = append(steps, cursorSnapshotStep("SELECT must_not_run", true, map[string]any{"type": "is_autocommit"}))
			response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", cursorSnapshotBody(nil, steps...))
			if response.Code != 200 || fixture.upstream.count("session_autocommit") != 1 || fixture.upstream.count("open_read_cursor_first") != 1 {
				t.Fatalf("stale %s snapshot bypassed native state: %d %s", scenario, response.Code, response.Body.String())
			}
			if scenario == "multipage" && fixture.upstream.count("fetch_read_cursor") != 1 {
				t.Fatal("multi-page result did not finish through original fetch")
			}
		})
	}
}

func TestHranaCursorSnapshotDoesNotCrossHTTPBatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return cursorSnapshotDescription(), nil
		case "open_read_cursor_first":
			return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true, "autocommit": true}, nil
		case "session_autocommit":
			return map[string]any{"autocommit": false}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway)
	first := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", cursorSnapshotBody(nil, cursorSnapshotStep("SELECT 1", true, nil)))
	if first.Code != 200 {
		t.Fatal(first.Body.String())
	}
	var header map[string]any
	if json.Unmarshal([]byte(strings.Split(first.Body.String(), "\n")[0]), &header) != nil {
		t.Fatal("missing baton header")
	}
	second := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", cursorSnapshotBody(header["baton"], cursorSnapshotStep("SELECT must_not_run", true, map[string]any{"type": "is_autocommit"})))
	if second.Code != 200 || fixture.upstream.count("session_autocommit") != 1 || fixture.upstream.count("open_read_cursor_first") != 1 {
		t.Fatalf("snapshot survived HTTP boundary: %d %s", second.Code, second.Body.String())
	}
}

func TestHranaCursorInvalidSnapshotIsUnknownWithoutProbeOrReplay(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{}, "is_readonly": false, "is_explain": false}, nil
		case "open_write_cursor_first":
			return map[string]any{"cols": []any{}, "rows": []any{}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1, "autocommit": "true"}, nil
		}
		return nil, nil
	})
	body := cursorSnapshotBody(nil, cursorSnapshotStep("INSERT INTO records VALUES(1)", true, nil), cursorSnapshotStep("SELECT must_not_run", true, map[string]any{"type": "is_autocommit"}))
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != http.StatusBadGateway || fixture.upstream.count("open_write_cursor_first") != 1 || fixture.upstream.count("session_autocommit") != 0 || fixture.upstream.count("statement") != 0 {
		t.Fatalf("invalid snapshot was retried or queried: %d %s", response.Code, response.Body.String())
	}
	assertCursorIsolated(t, fixture)
}
