package platform

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func TestCursorWireRejectsUnboundedOrMalformedPages(t *testing.T) {
	session := strings.Repeat("a", 64)
	for _, command := range []map[string]any{
		{"command": "open_read_cursor", "session": session, "sql": "SELECT ?", "args": []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}}, "named_args": []namedSQLArgument{}},
		{"command": "fetch_read_cursor", "session": session, "max_rows": 256},
		{"command": "close_read_cursor", "session": session},
		{"command": "open_write_cursor", "session": session, "sql": "INSERT INTO rows VALUES(?) RETURNING id", "args": []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}}, "named_args": []namedSQLArgument{}},
		{"command": "fetch_write_cursor", "session": session, "max_rows": 256},
		{"command": "close_write_cursor", "session": session},
	} {
		encoded, _ := json.Marshal(command)
		fields := map[string]json.RawMessage{}
		if json.Unmarshal(encoded, &fields) != nil || !writerCommandValid(command["command"].(string), encoded, fields) {
			t.Fatalf("cursor command rejected: %s", encoded)
		}
	}
	for _, count := range []int{0, 257, -1} {
		command := map[string]any{"command": "fetch_read_cursor", "session": session, "max_rows": count}
		encoded, _ := json.Marshal(command)
		fields := map[string]json.RawMessage{}
		_ = json.Unmarshal(encoded, &fields)
		if writerCommandValid("fetch_read_cursor", encoded, fields) {
			t.Fatalf("unbounded cursor fetch accepted: %d", count)
		}
	}
	for _, raw := range []string{`{"rows":null,"done":true}`, `{"rows":[[1],[2]],"done":false}`, `{"rows":[],"done":"true"}`, `{"rows":[],"done":true,"extra":1}`} {
		if validCursorFetchResult(json.RawMessage(raw), 1) {
			t.Fatalf("invalid cursor page accepted: %s", raw)
		}
	}
	if !validCursorFetchResult(json.RawMessage(`{"rows":[[1]],"done":false}`), 1) {
		t.Fatal("bounded cursor page rejected")
	}
	for _, raw := range []string{
		`{"rows":null,"done":true,"affected_row_count":1,"last_insert_rowid":1}`,
		`{"rows":[[1],[2]],"done":true,"affected_row_count":1,"last_insert_rowid":1}`,
		`{"rows":[],"done":true,"affected_row_count":null,"last_insert_rowid":null}`,
		`{"rows":[],"done":false,"affected_row_count":0,"last_insert_rowid":null}`,
		`{"rows":[],"done":true,"affected_row_count":-1,"last_insert_rowid":null}`,
		`{"rows":[],"done":true,"affected_row_count":0,"last_insert_rowid":null,"unexpected":1}`,
	} {
		if validCursorFetchResponse(json.RawMessage(raw), 1, true) {
			t.Fatalf("invalid write cursor page accepted: %s", raw)
		}
	}
	for _, raw := range []string{
		`{"rows":[[1]],"done":false,"affected_row_count":null,"last_insert_rowid":null}`,
		`{"rows":[],"done":true,"affected_row_count":12000,"last_insert_rowid":12000}`,
	} {
		if !validCursorFetchResponse(json.RawMessage(raw), 1, true) {
			t.Fatalf("valid write cursor page rejected: %s", raw)
		}
	}
}

func TestGatewayWriteCursorRoutesBoundedNativePagesAndRejectsCrossProjectAccess(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "open_write_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_write_cursor":
			return map[string]any{"rows": [][]any{{1}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}, nil
		case "close_write_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	open := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/cursor/open", map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(1) RETURNING id", "args": []any{}, "named_args": []any{}, "write_cursor": true})
	if open.Code != 200 || fixture.upstream.count("open_write_cursor") != 1 {
		t.Fatalf("write cursor open failed: %d %s", open.Code, open.Body.String())
	}
	denied := gatewayHTTP(t, fixture.gateway, fixture.tokenB, "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 1, "write_cursor": true})
	if denied.Code != 403 || fixture.upstream.count("fetch_write_cursor") != 0 {
		t.Fatalf("cross-project cursor fetch dispatched: %d %s", denied.Code, denied.Body.String())
	}
	fetch := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 1, "write_cursor": true})
	if fetch.Code != 200 || fixture.upstream.count("fetch_write_cursor") != 1 {
		t.Fatalf("write cursor fetch failed: %d %s", fetch.Code, fetch.Body.String())
	}
	close := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/cursor/close", map[string]any{"session": session, "write_cursor": true})
	if close.Code != 200 || fixture.upstream.count("close_write_cursor") != 1 {
		t.Fatalf("write cursor close failed: %d %s", close.Code, close.Body.String())
	}
}
