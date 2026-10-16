package platform

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func testHranaV3Call(t *testing.T, handler *hranaHTTP, token, path string, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	method := http.MethodPost
	if payload == nil {
		method = http.MethodGet
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/x-protobuf")
	}
	response := httptest.NewRecorder()
	if !serveHranaV3(response, request, handler) {
		t.Fatal("v3 request not routed")
	}
	return response
}

func testHranaV3Statement() []byte {
	stmt := hranaProtoString(1, "SELECT ? AS message")
	stmt = hranaProtoAppend(stmt, 3, hranaProtoString(4, "hello"))
	stmt = append(stmt, hranaProtoNumber(5, 1)...)
	return hranaProtoBytes(2, hranaProtoBytes(1, stmt))
}

type failingCursorWriter struct {
	*httptest.ResponseRecorder
	writes    int
	maxWrites int
}

func (writer *failingCursorWriter) Write(data []byte) (int, error) {
	writer.writes++
	if writer.writes > writer.maxWrites {
		return 0, io.ErrClosedPipe
	}
	return writer.ResponseRecorder.Write(data)
}

func TestHranaV3HTTPReadCursorStreamsPagesAndCancelsOnDisconnect(t *testing.T) {
	fixture := newGatewayFixture(t)
	var next int
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": true}, nil
		case "open_read_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_read_cursor":
			rows := make([][]any, 0, 256)
			for len(rows) < 256 && next < 600 {
				next++
				rows = append(rows, []any{next})
			}
			return map[string]any{"rows": rows, "done": next == 600}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "SELECT id FROM records"), hranaProtoNumber(5, 1)...)
	requestBody := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	path := "/db/tenant-a/db-a/main/v3-protobuf/cursor"
	response := testHranaV3Call(t, handler, fixture.tokenA, path, requestBody)
	if response.Code != 200 || fixture.upstream.count("fetch_read_cursor") != 3 || fixture.upstream.count("close_read_cursor") != 0 {
		t.Fatalf("cursor did not read bounded native pages: status=%d fetches=%d body=%q", response.Code, fixture.upstream.count("fetch_read_cursor"), response.Body.String())
	}
	encoded := response.Body.Bytes()
	frames := 0
	for len(encoded) > 0 {
		length, bytesRead := binary.Uvarint(encoded)
		if bytesRead < 1 || uint64(len(encoded)-bytesRead) < length {
			t.Fatal("truncated cursor frame")
		}
		encoded = encoded[bytesRead+int(length):]
		frames++
	}
	if frames != 603 {
		t.Fatalf("expected header, begin, 600 rows, end; got %d", frames)
	}
	if denied := testHranaV3Call(t, handler, fixture.tokenB, path, requestBody); denied.Code != 403 {
		t.Fatalf("cross-project cursor returned %d", denied.Code)
	}

	partial := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	next = 0
	writer := &failingCursorWriter{ResponseRecorder: partial, maxWrites: 2}
	if !serveHranaV3(writer, request, handler) {
		t.Fatal("cursor disconnect was not routed")
	}
	if fixture.upstream.count("close_read_cursor") != 1 || fixture.upstream.count("close_session") == 0 {
		t.Fatal("disconnected cursor was not closed with the native session")
	}
}

func TestHranaV3HTTPWriteCursorStreamsNativePagesAndRowid(t *testing.T) {
	fixture := newGatewayFixture(t)
	var next int
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": false}, nil
		case "open_write_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_write_cursor":
			rows := make([][]any, 0, 256)
			for len(rows) < 256 && next < 600 {
				next++
				rows = append(rows, []any{next})
			}
			if next == 600 {
				return map[string]any{"rows": rows, "done": true, "affected_row_count": 600, "last_insert_rowid": 600}, nil
			}
			return map[string]any{"rows": rows, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
		case "close_write_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "INSERT INTO records(id) VALUES(1) RETURNING id"), hranaProtoNumber(5, 1)...)
	body := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/cursor", body)
	if response.Code != 200 || fixture.upstream.count("open_write_cursor") != 1 || fixture.upstream.count("fetch_write_cursor") != 3 || fixture.upstream.count("statement") != 0 {
		t.Fatalf("write cursor did not stream native pages: status=%d body=%q", response.Code, response.Body.String())
	}
	encoded := response.Body.Bytes()
	frames := 0
	for len(encoded) > 0 {
		length, bytesRead := binary.Uvarint(encoded)
		if bytesRead < 1 || uint64(len(encoded)-bytesRead) < length {
			t.Fatal("truncated write cursor frame")
		}
		payload := encoded[bytesRead : bytesRead+int(length)]
		if uint64(len(encoded)-bytesRead) == length {
			entry, err := hranaProtoRead(payload)
			if err != nil || len(entry) != 1 || entry[0].number != 2 {
				t.Fatalf("invalid final write cursor frame: %v %v", entry, err)
			}
			metadata, err := hranaProtoRead(entry[0].bytes)
			if err != nil || len(metadata) != 2 || metadata[0].integer != 600 || metadata[1].integer != 600 {
				t.Fatalf("write cursor rowid/count mismatch: %v %v", metadata, err)
			}
		}
		encoded = encoded[bytesRead+int(length):]
		frames++
	}
	if frames != 603 {
		t.Fatalf("expected header, begin, 600 rows, end; got %d", frames)
	}
	next = 0
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	partial := &failingCursorWriter{ResponseRecorder: httptest.NewRecorder(), maxWrites: 2}
	if !serveHranaV3(partial, request, handler) || fixture.upstream.count("close_write_cursor") != 1 || fixture.upstream.count("close_session") == 0 {
		t.Fatalf("disconnected write cursor cleanup: closes=%d sessions=%d", fixture.upstream.count("close_write_cursor"), fixture.upstream.count("close_session"))
	}
}

func TestHranaV3DescribeProtobufKeepsMetadataNative(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			return map[string]any{"params": []any{map[string]any{"name": ":lookup"}}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": true}, nil
		}
		return nil, nil
	}
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := hranaProtoBytes(2, hranaProtoBytes(5, hranaProtoString(1, "SELECT id WHERE id=:lookup")))
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != 200 || fixture.upstream.count("describe") != 1 || fixture.upstream.count("statement") != 0 {
		t.Fatalf("native protobuf describe did not prepare only: %d %s", response.Code, response.Body.String())
	}
	fields, err := hranaProtoRead(response.Body.Bytes())
	if err != nil || len(fields) != 2 || fields[1].number != 3 {
		t.Fatalf("invalid describe response envelope: %v %v", fields, err)
	}
	stream, err := hranaProtoRead(fields[1].bytes)
	if err != nil || len(stream) != 1 || stream[0].number != 1 {
		t.Fatalf("invalid stream envelope: %v %v", stream, err)
	}
	message, err := hranaProtoRead(stream[0].bytes)
	if err != nil || len(message) != 1 || message[0].number != 5 {
		t.Fatalf("invalid describe envelope: %v %v", message, err)
	}
}

func TestHranaV3TypedPipelineAndHandshake(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	base := "/db/tenant-a/db-a/main/v3-protobuf"
	if response := testHranaV3Call(t, handler, "", base, nil); response.Code != 200 {
		t.Fatalf("v3 probe: %d", response.Code)
	}
	request := hranaProtoBytes(2, testHranaV3Statement())
	if response := testHranaV3Call(t, handler, fixture.tokenB, base+"/pipeline", request); response.Code != 403 {
		t.Fatalf("cross tenant: %d", response.Code)
	}
	response := testHranaV3Call(t, handler, fixture.tokenA, base+"/pipeline", append(request, hranaProtoBytes(2, hranaProtoBytes(1, nil))...))
	if response.Code != 200 || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("protobuf pipeline: %d %s", response.Code, response.Body.String())
	}
	fields, err := hranaProtoRead(response.Body.Bytes())
	if err != nil || len(fields) != 2 || fields[0].number != 3 || fields[1].number != 3 {
		t.Fatalf("pipeline envelope: %v %v", fields, err)
	}
	if fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatal("native typed query or close missing")
	}
	for _, operation := range fixture.upstream.calls {
		if operation.command["command"] != "statement" {
			continue
		}
		args := operation.command["args"].([]any)
		if len(args) != 1 || args[0].(map[string]any)["type"] != "text" || args[0].(map[string]any)["value"] != "hello" {
			t.Fatalf("typed args lost: %v", args)
		}
	}
}

func TestHranaV3ProtobufCombinedConditions(t *testing.T) {
	first := hranaProtoBytes(1, hranaProtoNumber(1, 0))
	second := hranaProtoBytes(1, hranaProtoBytes(3, hranaProtoNumber(2, 0)))
	for _, field := range []int{4, 5} {
		condition, err := hranaProtoCondition(hranaProtoBytes(field, append(first, second...)), 0)
		if err != nil {
			t.Fatalf("nested condition type %d: %v", field, err)
		}
		kind := "and"
		if field == 5 {
			kind = "or"
		}
		if condition["type"] != kind || len(condition["conds"].([]any)) != 2 {
			t.Fatalf("condition type %d lost: %v", field, condition)
		}
	}
	oversized := hranaProtoBytes(4, bytes.Repeat(first, 65))
	if _, err := hranaProtoCondition(oversized, 0); err == nil {
		t.Fatal("unbounded condition list accepted")
	}
}

func TestHranaV3ProtobufGetAutocommitUsesNativeSession(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "session_autocommit" {
			return map[string]any{"autocommit": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := hranaProtoBytes(2, hranaProtoBytes(8, nil))
	request = append(request, hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != 200 || fixture.upstream.count("session_autocommit") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("native autocommit request: %d %s", response.Code, response.Body.String())
	}
	fields, err := hranaProtoRead(response.Body.Bytes())
	if err != nil || len(fields) != 2 {
		t.Fatalf("autocommit protobuf envelope: %v %v", fields, err)
	}
	streamResult, err := hranaProtoRead(fields[0].bytes)
	if err != nil || len(streamResult) != 1 || streamResult[0].number != 1 {
		t.Fatalf("autocommit stream result: %v %v", streamResult, err)
	}
	streamResponse, err := hranaProtoRead(streamResult[0].bytes)
	if err != nil || len(streamResponse) != 1 || streamResponse[0].number != 8 {
		t.Fatalf("autocommit response: %v %v", streamResponse, err)
	}
	flag, err := hranaProtoRead(streamResponse[0].bytes)
	if err != nil || len(flag) != 1 || flag[0].number != 1 || flag[0].integer != 1 {
		t.Fatalf("native autocommit flag: %v %v", flag, err)
	}
}

func TestHranaV3CursorFramesAndUnknown(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	base := "/db/tenant-a/db-a/main/v3-protobuf/cursor"
	stmt := hranaProtoString(1, "SELECT ? AS message")
	stmt = hranaProtoAppend(stmt, 3, hranaProtoString(4, "hello"))
	step := hranaProtoBytes(2, stmt)
	batch := hranaProtoBytes(1, step)
	response := testHranaV3Call(t, handler, fixture.tokenA, base, hranaProtoBytes(2, batch))
	if response.Code != 200 {
		t.Fatalf("cursor status: %d %s", response.Code, response.Body.String())
	}
	buffer := response.Body.Bytes()
	var frames [][]hranaProtoField
	for len(buffer) > 0 {
		length, size := binary.Uvarint(buffer)
		if size <= 0 || uint64(len(buffer[size:])) < length {
			t.Fatal("invalid cursor frame length")
		}
		fields, err := hranaProtoRead(buffer[size : size+int(length)])
		if err != nil {
			t.Fatalf("cursor frame %d %x: %v", len(frames), buffer[size:size+int(length)], err)
		}
		frames = append(frames, fields)
		buffer = buffer[size+int(length):]
	}
	if len(frames) != 4 || frames[0][0].number != 1 || frames[1][0].number != 1 || frames[2][0].number != 4 || frames[3][0].number != 2 {
		t.Fatalf("cursor frame sequence: %v", frames)
	}
	begin, err := hranaProtoRead(frames[1][0].bytes)
	if err != nil || len(begin) != 2 || begin[0].number != 1 || begin[1].number != 2 {
		t.Fatalf("cursor column metadata: %v %v", begin, err)
	}
	if fixture.upstream.count("statement") != 1 {
		t.Fatal("cursor did not execute real statement")
	}

	unknownFixture := newGatewayFixture(t)
	unknownFixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{map[string]any{"name": "message"}}, "rows": [][]any{{1, 2}}, "affected_row_count": 0, "last_insert_rowid": nil}, nil
		}
		return nil, nil
	})
	unknownHandler := newHranaHTTP(unknownFixture.gateway).(*hranaHTTP)
	result := testHranaV3Call(t, unknownHandler, unknownFixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", hranaProtoBytes(2, testHranaV3Statement()))
	if result.Code != 502 || !strings.Contains(result.Body.String(), "unknown") || unknownFixture.upstream.count("statement") != 1 {
		t.Fatalf("unknown was hidden or retried: %d %s", result.Code, result.Body.String())
	}
}

func TestHranaV3MalformedAndExactInteger(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	path := "/db/tenant-a/db-a/main/v3-protobuf/pipeline"
	for _, data := range [][]byte{{0xff}, hranaProtoBytes(2, hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(3, []byte{0xff}))))} {
		if response := testHranaV3Call(t, handler, fixture.tokenA, path, data); response.Code != 400 {
			t.Fatalf("malformed payload accepted: %d", response.Code)
		}
	}
	if fixture.upstream.count("open_session") != 0 {
		t.Fatal("invalid payload dispatched native SQL")
	}
	integer := int64(9223372036854775807)
	encoded := hranaProtoNumber(2, uint64(integer<<1)^uint64(integer>>63))
	value, err := hranaProtoValue(encoded)
	if err != nil || value["value"] != "9223372036854775807" {
		t.Fatalf("bigint lost: %v %v", value, err)
	}
	response, err := hranaProtoStatementResult(map[string]any{"cols": []any{map[string]any{"name": "big", "decltype": nil}}, "rows": []any{[]any{map[string]any{"type": "integer", "value": "9223372036854775807"}}}, "affected_row_count": json.Number("0"), "last_insert_rowid": nil})
	if err != nil || len(response) == 0 {
		t.Fatalf("bigint reply unavailable: %v", err)
	}
}
