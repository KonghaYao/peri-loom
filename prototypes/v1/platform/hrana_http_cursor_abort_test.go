package platform

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	controller "peri-loom/controller"
)

func TestHranaHTTPWriteCursorInFlightAbortKeepsIsolation(t *testing.T) {
	fixture := newGatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetches := 0
	fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": false}, nil
		case "open_write_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_write_cursor":
			fetches++
			if fetches == 1 {
				return map[string]any{"rows": [][]any{{1}}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
			}
			// 模拟请求已派发但取消丢失回执，不能把它解释为 worker 已 rollback。
			cancel()
			return nil, &SQLRPCError{Ambiguous: true, cause: ctx.Err()}
		case "close_write_cursor":
			t.Error("未决写入后不能越过隔离强制关闭 cursor")
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "INSERT INTO records VALUES(1) RETURNING id"), hranaProtoNumber(5, 1)...)
	body := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if fetches != 2 || response.Code != 200 {
		t.Fatalf("未覆盖已开始流式输出的在途取消: fetches=%d status=%d", fetches, response.Code)
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	record, isolated := isolatedOperation(state, fixture.first, true)
	if !isolated || record.Action != "cursor_fetch" || record.State != "quarantined" || record.BusinessOutcome != "unknown" || !record.MayHaveCommitted || record.CursorNativeOutcome != cursorOutcomeUnknown {
		t.Fatalf("在途取消没有保留未决状态: isolated=%t action=%s state=%s outcome=%s possible_commit=%t native=%s", isolated, record.Action, record.State, record.BusinessOutcome, record.MayHaveCommitted, record.CursorNativeOutcome)
	}
	if outcomes := fixture.audit.outcomesOf("cursor_fetch"); len(outcomes) != 2 || outcomes[0] != "ok" || outcomes[1] != "outcome_unknown" {
		t.Fatalf("在途取消的审计不完整: %v", outcomes)
	}
	if len(handler.streams) != 0 {
		t.Fatal("未决写入留下了可复用 baton")
	}
	probe := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}`)
	document := hranaV3JSONDocument(t, probe)
	if probe.Code != 409 || document["error"].(map[string]any)["code"] != "database_isolated" || fetches != 2 {
		t.Fatalf("隔离证据被覆盖或 SQL 被重试: %d %s", probe.Code, probe.Body.String())
	}
	evidence := document["error"].(map[string]any)
	if evidence["operation_id"] != record.OperationID || evidence["possible_commit"] != true || evidence["retry_allowed"] != false {
		t.Fatalf("隔离响应没有指向未决 operation: %v", evidence)
	}
}
