package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func TestHranaHighLevelExecutePagesOnlyProvenReadonlyResult(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		readonly bool
		failPage bool
		discard  bool
	}{
		{name: "read_large_result", readonly: true},
		{name: "read_large_result_without_returned_rows", readonly: true, discard: true},
		{name: "read_fetch_error_closes", readonly: true, failPage: true},
		{name: "write_returning_preflight_before_execution", readonly: false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 64 * 1024 })
			next := 0
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "statement":
					return nil, testRPCError(true, false, SQLRPCError{})
				case "describe":
					return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": scenario.readonly}, nil
				case "open_read_cursor", "open_write_cursor":
					return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
				case "fetch_read_cursor", "fetch_write_cursor":
					if scenario.failPage {
						return nil, testRPCError(true, false, SQLRPCError{})
					}
					rows := make([][]any, 0, 256)
					for len(rows) < 256 && next < 300 {
						next++
						rows = append(rows, []any{next})
					}
					if !scenario.readonly {
						if next == 300 {
							return map[string]any{"rows": rows, "done": true, "affected_row_count": 300, "last_insert_rowid": 300}, nil
						}
						return map[string]any{"rows": rows, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
					}
					return map[string]any{"rows": rows, "done": next == 300}, nil
				case "close_read_cursor", "close_write_cursor":
					return map[string]bool{"closed": true}, nil
				}
				return nil, nil
			})
			sql := "SELECT id FROM records"
			if !scenario.readonly {
				sql = "INSERT INTO records VALUES(1) RETURNING id"
			}
			body, err := json.Marshal(map[string]any{"requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": sql, "want_rows": !scenario.discard}}, map[string]any{"type": "close"}}})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(string(body)))
			request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
			// A read statement is classified twice: the gateway classifies the statement
			// it is about to dispatch, and the pipeline classifies the same statement
			// again to decide that it may be paged by a read cursor. A write statement
			// is only preflighted by the pipeline, which then opens a write cursor.
			describes := fixture.upstream.count("describe")
			if scenario.readonly {
				if fixture.upstream.count("statement") != 1 || describes != 2 {
					t.Fatalf("failed SQL execution replayed or not classified: %d %s", response.Code, response.Body.String())
				}
			} else if fixture.upstream.count("statement") != 0 || describes != 1 {
				t.Fatalf("failed SQL execution replayed or not classified: %d %s", response.Code, response.Body.String())
			}
			if !scenario.readonly {
				if fixture.upstream.count("open_read_cursor") != 0 || fixture.upstream.count("open_write_cursor") != 1 || fixture.upstream.count("fetch_write_cursor") != 2 || !strings.Contains(response.Body.String(), `"affected_row_count":300`) || !strings.Contains(response.Body.String(), `"last_insert_rowid":"300"`) {
					t.Fatalf("writable RETURNING lacked preflight or native metadata: %d %s", response.Code, response.Body.String())
				}
				return
			}
			if fixture.upstream.count("open_read_cursor") != 1 {
				t.Fatalf("known read error did not use native pages: %d %s", response.Code, response.Body.String())
			}
			if scenario.failPage {
				if fixture.upstream.count("close_read_cursor") != 1 || response.Code == http.StatusOK && !strings.Contains(response.Body.String(), `"code":"sql_error"`) {
					t.Fatalf("failed fetch left cursor open: %d %s", response.Code, response.Body.String())
				}
				return
			}
			var result struct {
				Results []struct {
					Response struct {
						Result struct {
							Rows []any `json:"rows"`
						} `json:"result"`
					} `json:"response"`
				} `json:"results"`
			}
			expectedRows := 300
			if scenario.discard {
				expectedRows = 0
			}
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Results) == 0 || len(result.Results[0].Response.Result.Rows) != expectedRows || fixture.upstream.count("fetch_read_cursor") != 2 || fixture.upstream.count("close_read_cursor") != 0 {
				t.Fatalf("high-level result did not aggregate bounded pages: %d fetch=%d body=%s", response.Code, fixture.upstream.count("fetch_read_cursor"), response.Body.String())
			}
		})
	}
}
