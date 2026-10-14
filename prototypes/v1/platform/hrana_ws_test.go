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

func TestHranaWebSocketScopesAuthenticationAndClosesNativeStreamOnDisconnect(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	server := httptest.NewServer(handler)
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{"hrana3", "hrana2"}, HandshakeTimeout: 5 * time.Second}
	address := "ws" + strings.TrimPrefix(server.URL, "http") + "/db/tenant-a/db-a/main/"
	connection, _, err := dialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Subprotocol() != "hrana3" {
		t.Fatalf("negotiated %q", connection.Subprotocol())
	}
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
		t.Fatalf("open failed: %v", reply)
	}
	if reply := exchange(map[string]any{"type": "request", "request_id": 1, "request": map[string]any{"type": "execute", "stream_id": 1, "stmt": map[string]any{"sql": "SELECT 1 AS message", "want_rows": true}}}); reply["type"] != "response_ok" {
		t.Fatalf("execute failed: %v", reply)
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("statement") != 1 {
		t.Fatal("WebSocket SQL did not dispatch once")
	}
	if err := RevokeAPIKey(fixture.state, fixture.keyA.ID); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for fixture.upstream.count("close_session") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatal("disconnect leaked native session")
	}
	other, _, err := dialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.WriteJSON(map[string]any{"type": "hello", "jwt": fixture.tokenB}); err != nil {
		t.Fatal(err)
	}
	var refused map[string]any
	if err := other.ReadJSON(&refused); err != nil || refused["type"] != "hello_error" || fixture.upstream.count("open_session") != 1 {
		t.Fatalf("cross-project hello not denied: %v %v", refused, err)
	}
	missing, _, err := dialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Close()
	if err := missing.WriteJSON(map[string]any{"type": "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := missing.ReadJSON(&refused); err != nil || refused["type"] != "hello_error" || fixture.upstream.count("open_session") != 1 {
		t.Fatalf("missing credential was not denied: %v %v", refused, err)
	}
}

func TestHranaWebSocketProtobufNegotiatesAndDispatchesNativeSQL(t *testing.T) {
	fixture := newGatewayFixture(t)
	server := httptest.NewServer(newHranaHTTP(fixture.gateway))
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{"hrana3-protobuf", "hrana3"}, HandshakeTimeout: 5 * time.Second}
	connection, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/db/tenant-a/db-a/main/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Subprotocol() != "hrana3-protobuf" {
		t.Fatalf("protobuf was not preferred: %q", connection.Subprotocol())
	}
	exchange := func(data []byte) []hranaProtoField {
		t.Helper()
		if err := connection.WriteMessage(websocket.BinaryMessage, data); err != nil {
			t.Fatal(err)
		}
		kind, output, err := connection.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage {
			t.Fatalf("invalid protobuf frame: %d %v", kind, err)
		}
		fields, err := hranaProtoRead(output)
		if err != nil || len(fields) != 1 {
			t.Fatalf("invalid protobuf message: %v %v", fields, err)
		}
		return fields
	}
	if result := exchange(hranaProtoBytes(1, hranaProtoString(1, fixture.tokenA))); result[0].number != 1 {
		t.Fatalf("hello was not accepted: %v", result)
	}
	open := hranaProtoNumber(1, 0)
	open = append(open, hranaProtoBytes(2, hranaProtoNumber(1, 1))...)
	if result := exchange(hranaProtoBytes(2, open)); result[0].number != 3 {
		t.Fatalf("stream was not opened: %v", result)
	}
	statement := hranaProtoString(1, "SELECT 1 AS message")
	statement = append(statement, hranaProtoNumber(5, 1)...)
	execute := append(hranaProtoNumber(1, 2), hranaProtoBytes(4, append(hranaProtoNumber(1, 1), hranaProtoBytes(2, statement)...))...)
	if result := exchange(hranaProtoBytes(2, execute)); result[0].number != 3 || fixture.upstream.count("statement") != 1 {
		t.Fatalf("protobuf execute failed: %v", result)
	}
}

func TestHranaGatewayShutdownClosesWebSocketBeforeNativeSessions(t *testing.T) {
	t.Setenv("PERI_HRANA_EXPERIMENTAL", "1")
	fixture := newGatewayFixture(t)
	server := httptest.NewServer(newPublicMux(fixture.gateway, http.NotFoundHandler()))
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{"hrana2"}, HandshakeTimeout: 5 * time.Second}
	connection, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/db/tenant-a/db-a/main/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(map[string]any{"type": "hello", "jwt": fixture.tokenA}); err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := connection.ReadJSON(&response); err != nil || response["type"] != "hello_ok" {
		t.Fatalf("gateway hello failed: %v %v", response, err)
	}
	if err := connection.WriteJSON(map[string]any{"type": "request", "request_id": 1, "request": map[string]any{"type": "open_stream", "stream_id": 0}}); err != nil {
		t.Fatal(err)
	}
	if err := connection.ReadJSON(&response); err != nil || response["type"] != "response_ok" {
		t.Fatalf("stream not opened: %v %v", response, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := fixture.gateway.Close(ctx); err != nil {
		t.Fatalf("gateway did not drain WS session: %v", err)
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatal("native session survived gateway shutdown")
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("WebSocket remained open after gateway shutdown")
	}
}

func TestHranaWebSocketCursorFetchIsBoundedAndCloseCancelsRemainingRows(t *testing.T) {
	entries := make([]map[string]any, 1001)
	for index := range entries {
		entries[index] = map[string]any{"type": "row", "row": []any{index}}
	}
	cursors := map[int]*hranaWSCursor{7: {streamID: 0, entries: entries}}
	handler := &hranaHTTP{}
	first, failure, fatal := handler.wsRequest(nil, "", controller.DatabaseID{}, "hrana3", nil, nil, cursors, hranaWSRequest{Type: "fetch_cursor", CursorID: 7, MaxCount: 5000})
	if failure != "" || fatal || first["done"] != false || len(first["entries"].([]map[string]any)) != 1000 || len(cursors[7].entries) != 1 {
		t.Fatalf("cursor fetch was not bounded: %v %q %v", first, failure, fatal)
	}
	closed, failure, fatal := handler.wsRequest(nil, "", controller.DatabaseID{}, "hrana3", nil, nil, cursors, hranaWSRequest{Type: "close_cursor", CursorID: 7})
	if failure != "" || fatal || closed["type"] != "close_cursor" || len(cursors) != 0 {
		t.Fatalf("close did not discard buffered rows: %v %q %v", closed, failure, fatal)
	}
}
