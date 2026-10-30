package platform

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
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

func hranaProtoTestStatement() []byte {
	statement := hranaProtoString(1, "SELECT ? AS message")
	statement = hranaProtoAppend(statement, 3, hranaProtoString(4, "hello"))
	return append(statement, hranaProtoNumber(5, 1)...)
}

func testHranaV3Statement() []byte {
	return hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoTestStatement()))
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

// A single execute may only exceed the fast path budget when the tenant allows
// more than the gateway-wide 8 MiB ceiling. The reply must be refused as the
// pipeline limit failure, not published as a success.
func TestHranaV3ProtoSingleExecuteRejectsOversizedResult(t *testing.T) {
	fixture := newGatewayFixture(t)
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 12 * 1024 * 1024 })
	value := strings.Repeat("x", 9*1024*1024)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{map[string]any{"name": "payload", "decltype": "TEXT"}}, "rows": [][]any{{value}}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := append(hranaProtoBytes(2, testHranaV3Statement()), hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	// The plain-text body proves the native single-execute path answered: the
	// JSON compatibility path would return its own JSON error envelope.
	if response.Code != http.StatusRequestEntityTooLarge || response.Body.String() != "pipeline_result_limit\n" {
		t.Fatalf("oversized single execute returned %d: %q", response.Code, response.Body.String())
	}
	if fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") == 0 {
		t.Fatalf("oversized single execute did not run once and retire its session: statements=%d closes=%d", fixture.upstream.count("statement"), fixture.upstream.count("close_session"))
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

// hranaOutputValue converts every native worker cell before the protobuf
// encoder sees it, and REAL cells become float64. The encoder must accept that
// shape; a json.Number-only branch truncated the cursor stream after the 200
// response was already committed.
func TestHranaProtoResultValueFloatAcceptsOutputValue(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		input json.Number
		want  float64
		wire  []byte
	}{
		{"fraction", json.Number("1.5"), 1.5, []byte{0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
		{"exponent", json.Number("1e3"), 1000, []byte{0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x8f, 0x40}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			output, ok := hranaOutputValue(scenario.input)
			if !ok {
				t.Fatalf("hranaOutputValue(%q) rejected", scenario.input)
			}
			value, ok := output.(map[string]any)["value"].(float64)
			if !ok || value != scenario.want {
				t.Fatalf("hranaOutputValue(%q) = %#v, want float64 %v", scenario.input, output, scenario.want)
			}
			encoded, err := hranaProtoResultValue(output)
			if err != nil {
				t.Fatalf("encode float64 %v: %v", scenario.want, err)
			}
			// Golden bytes, independent of hranaProtoRead: field 3 fixed64.
			if !bytes.Equal(encoded, scenario.wire) {
				t.Fatalf("wire = %x, want %x", encoded, scenario.wire)
			}
		})
	}
}

func TestHranaProtoResultValueRoundTripsEveryOutputType(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		input any
		field int
		wire  int
		check func(t *testing.T, value map[string]any)
	}{
		{"null", nil, 1, 2, func(t *testing.T, value map[string]any) {
			if value["type"] != "null" {
				t.Fatalf("null decoded as %v", value)
			}
		}},
		{"integer", json.Number("42"), 2, 0, func(t *testing.T, value map[string]any) {
			if value["type"] != "integer" || value["value"] != "42" {
				t.Fatalf("integer decoded as %v", value)
			}
		}},
		{"float", json.Number("1.5"), 3, 1, func(t *testing.T, value map[string]any) {
			if value["type"] != "float" || value["value"] != 1.5 {
				t.Fatalf("float decoded as %v", value)
			}
		}},
		{"text", "hello", 4, 2, func(t *testing.T, value map[string]any) {
			if value["type"] != "text" || value["value"] != "hello" {
				t.Fatalf("text decoded as %v", value)
			}
		}},
		{"blob", []any{json.Number("0"), json.Number("255")}, 5, 2, func(t *testing.T, value map[string]any) {
			if value["type"] != "blob" || value["base64"] != "AP8=" {
				t.Fatalf("blob decoded as %v", value)
			}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			output, ok := hranaOutputValue(scenario.input)
			if !ok {
				t.Fatalf("hranaOutputValue(%#v) rejected", scenario.input)
			}
			encoded, err := hranaProtoResultValue(output)
			if err != nil {
				t.Fatalf("encode %#v: %v", output, err)
			}
			fields, err := hranaProtoRead(encoded)
			if err != nil || len(fields) != 1 || fields[0].number != scenario.field || fields[0].wire != scenario.wire {
				t.Fatalf("wire fields = %v, want #%d/%d", fields, scenario.field, scenario.wire)
			}
			decoded, err := hranaProtoValue(encoded)
			if err != nil {
				t.Fatalf("decode %s: %v", scenario.name, err)
			}
			scenario.check(t, decoded)
		})
	}
}

func TestHranaProtoResultValueRejectsInvalidFloat(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		value any
	}{
		{"nan", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"overflowing number", json.Number("1e999")},
		{"string", "1.5"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			encoded, err := hranaProtoResultValue(map[string]any{"type": "float", "value": scenario.value})
			if err == nil || encoded != nil {
				t.Fatalf("invalid float %s encoded as %x", scenario.name, encoded)
			}
		})
	}
}

func TestHranaProtoStatementResultMetrics(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": json.Number("0"), "last_insert_rowid": nil}
	}
	fieldsByNumber := func(t *testing.T, encoded []byte) map[int]hranaProtoField {
		t.Helper()
		fields, err := hranaProtoRead(encoded)
		if err != nil {
			t.Fatal(err)
		}
		byNumber := make(map[int]hranaProtoField, len(fields))
		for _, field := range fields {
			byNumber[field.number] = field
		}
		return byNumber
	}
	checkMetrics := func(t *testing.T, fields map[int]hranaProtoField) {
		t.Helper()
		read, exists := fields[6]
		if !exists || read.wire != 0 || read.integer != 8625 {
			t.Fatalf("rows_read field: %+v", read)
		}
		written, exists := fields[7]
		if !exists || written.wire != 0 || written.integer != 0 {
			t.Fatalf("rows_written field: %+v", written)
		}
		duration, exists := fields[8]
		if !exists || duration.wire != 1 || len(duration.bytes) != 8 {
			t.Fatalf("query_duration_ms field: %+v", duration)
		}
		if restored := math.Float64frombits(binary.LittleEndian.Uint64(duration.bytes)); math.Abs(restored-955.25) > 1e-9 {
			t.Fatalf("query_duration_ms = %v, want 955.25", restored)
		}
		if _, exists := fields[5]; exists {
			t.Fatal("replication_index was written")
		}
	}

	typed := base()
	typed["rows_read"] = uint64(8625)
	typed["rows_written"] = uint64(0)
	typed["query_duration_ms"] = 955.25
	encoded, err := hranaProtoStatementResult(typed)
	if err != nil {
		t.Fatal(err)
	}
	// Official wire bytes, independent of the package's own decoder: field 6
	// varint 8625, field 7 varint 0, then field 8 fixed64 955.25. Field 5 must
	// not appear. A shared bug between encoder and decoder cannot pass this.
	golden := []byte{
		0x30, 0xb1, 0x43, // field 6 (rows_read), varint 8625
		0x38, 0x00, // field 7 (rows_written), varint 0
		0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0xda, 0x8d, 0x40, // field 8, little-endian IEEE-754 955.25
	}
	if !bytes.HasSuffix(encoded, golden) {
		t.Fatalf("wire bytes = %x, want suffix %x", encoded, golden)
	}
	decoded, err := hranaProtoRead(encoded)
	if err != nil {
		t.Fatal(err)
	}
	sequence := make([]int, 0, len(decoded))
	for _, field := range decoded {
		sequence = append(sequence, field.number)
	}
	if len(sequence) != 4 || sequence[0] != 3 || sequence[1] != 6 || sequence[2] != 7 || sequence[3] != 8 {
		t.Fatalf("statement result field sequence = %v, want [3 6 7 8]", sequence)
	}
	checkMetrics(t, fieldsByNumber(t, encoded))

	number := base()
	number["rows_read"] = json.Number("8625")
	number["rows_written"] = json.Number("0")
	number["query_duration_ms"] = json.Number("955.25")
	encoded, err = hranaProtoStatementResult(number)
	if err != nil {
		t.Fatal(err)
	}
	checkMetrics(t, fieldsByNumber(t, encoded))

	encoded, err = hranaProtoStatementResult(base())
	if err != nil {
		t.Fatal(err)
	}
	fields := fieldsByNumber(t, encoded)
	for _, number := range []int{5, 6, 7, 8} {
		if _, exists := fields[number]; exists {
			t.Fatalf("legacy statement result wrote field %d", number)
		}
	}
}

func TestHranaProtoStatementResultMetricsRejected(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": json.Number("0"), "last_insert_rowid": nil}
	}
	for _, scenario := range []struct {
		name   string
		metric string
		value  any
	}{
		{"partial metrics", "rows_read", uint64(1)},
		{"nil rows_read", "rows_read", nil},
		{"nil rows_written", "rows_written", nil},
		{"nil query_duration_ms", "query_duration_ms", nil},
		{"fractional rows_read", "rows_read", 1.5},
		{"negative rows_read", "rows_read", -1},
		{"float64 beyond uint64", "rows_read", math.Ldexp(1, 64)},
		{"string rows_written", "rows_written", "1"},
		{"bool query_duration_ms", "query_duration_ms", true},
		{"string query_duration_ms", "query_duration_ms", "955.25"},
		{"negative query_duration_ms", "query_duration_ms", -0.5},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			reply := base()
			reply[scenario.metric] = scenario.value
			if _, err := hranaProtoStatementResult(reply); err == nil {
				t.Fatalf("%s accepted a malformed metric", scenario.metric)
			}
		})
	}
}

// The bounded encoder must refuse as soon as the bytes it has written no longer
// fit, instead of encoding the whole result and checking afterwards.
func TestHranaProtoStatementResultStopsAtResultBudget(t *testing.T) {
	rows := make([]any, 2000)
	for index := range rows {
		rows[index] = []any{map[string]any{"type": "text", "value": strings.Repeat("x", 1024)}}
	}
	reply := map[string]any{
		"cols":               []any{map[string]any{"name": "body", "decltype": "TEXT"}},
		"rows":               rows,
		"affected_row_count": json.Number("0"),
		"last_insert_rowid":  nil,
	}
	full, err := hranaProtoStatementResult(reply)
	if err != nil {
		t.Fatal(err)
	}
	const budget = 64 * 1024
	partial, err := hranaProtoStatementResultWithin(reply, budget)
	if !errors.Is(err, errHranaProtoResultLimit) {
		t.Fatalf("oversized result returned %v, want the result limit sentinel", err)
	}
	if len(partial) > budget {
		t.Fatalf("partial encoding wrote %d bytes past a %d byte budget", len(partial), budget)
	}
	if len(partial) >= len(full)/4 {
		t.Fatalf("result was fully encoded before the limit: partial=%d full=%d", len(partial), len(full))
	}
}

// An unencodable row sits past the budget. Reaching that row would mean the
// whole result was encoded; the sentinel proves the loop stopped first.
func TestHranaProtoPipelineResponseStopsAtResultBudget(t *testing.T) {
	response := map[string]any{"results": []any{map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": map[string]any{
		"cols": []any{},
		"rows": []any{
			[]any{map[string]any{"type": "text", "value": strings.Repeat("x", 4096)}},
			[]any{struct{}{}},
		},
		"affected_row_count": json.Number("0"),
		"last_insert_rowid":  nil,
	}}}}}
	if _, err := hranaProtoPipelineResponseWithin(response, 1024); !errors.Is(err, errHranaProtoResultLimit) {
		t.Fatalf("pipeline response encoded every row instead of stopping at the budget: %v", err)
	}
}

func hranaProtoTestStreamResponse(t *testing.T, entry []byte) hranaProtoField {
	t.Helper()
	stream, err := hranaProtoRead(entry)
	if err != nil || len(stream) != 1 || stream[0].number != 1 || stream[0].wire != 2 {
		t.Fatalf("invalid pipeline stream result: %v %v", stream, err)
	}
	response, err := hranaProtoRead(stream[0].bytes)
	if err != nil || len(response) != 1 || response[0].wire != 2 {
		t.Fatalf("invalid pipeline stream response: %v %v", response, err)
	}
	return response[0]
}

func hranaProtoTestPipelineEntry(t *testing.T, fields []hranaProtoField) []byte {
	t.Helper()
	for _, field := range fields {
		if field.number == 3 && field.wire == 2 {
			return field.bytes
		}
	}
	t.Fatalf("pipeline reply has no result: %v", fields)
	return nil
}

func hranaProtoTestPipelineStatement(t *testing.T, entry []byte) []hranaProtoField {
	t.Helper()
	response := hranaProtoTestStreamResponse(t, entry)
	if response.number != 2 {
		t.Fatalf("pipeline response is not execute: %+v", response)
	}
	wrapper, err := hranaProtoRead(response.bytes)
	if err != nil || len(wrapper) != 1 || wrapper[0].number != 1 || wrapper[0].wire != 2 {
		t.Fatalf("invalid execute response: %v %v", wrapper, err)
	}
	statement, err := hranaProtoRead(wrapper[0].bytes)
	if err != nil {
		t.Fatalf("invalid statement result: %v", err)
	}
	return statement
}

// hranaProtoTestStatementRow returns the decoded row message of an execute
// result. A row is a repeated Value field 1, so each item wraps the oneof.
func hranaProtoTestStatementRow(t *testing.T, statement []hranaProtoField, index int) []hranaProtoField {
	t.Helper()
	var rows []hranaProtoField
	for _, field := range statement {
		if field.number == 2 && field.wire == 2 {
			rows = append(rows, field)
		}
	}
	if index >= len(rows) {
		t.Fatalf("statement has %d rows, want at least %d", len(rows), index+1)
	}
	row, err := hranaProtoRead(rows[index].bytes)
	if err != nil || len(row) != 1 || row[0].number != 1 || row[0].wire != 2 {
		t.Fatalf("invalid statement row %d: %v %v", index, row, err)
	}
	return row
}

func hranaProtoTestPipelineBatch(t *testing.T, entry []byte) [][]hranaProtoField {
	t.Helper()
	response := hranaProtoTestStreamResponse(t, entry)
	if response.number != 3 {
		t.Fatalf("pipeline response is not batch: %+v", response)
	}
	wrapper, err := hranaProtoRead(response.bytes)
	if err != nil || len(wrapper) != 1 || wrapper[0].number != 1 || wrapper[0].wire != 2 {
		t.Fatalf("invalid batch response: %v %v", wrapper, err)
	}
	steps, err := hranaProtoRead(wrapper[0].bytes)
	if err != nil || len(steps) == 0 {
		t.Fatalf("invalid batch steps: %v %v", steps, err)
	}
	results := make([][]hranaProtoField, 0, len(steps))
	for index, step := range steps {
		if step.number != 1 || step.wire != 2 {
			t.Fatalf("batch step %d is not a result: %+v", index, step)
		}
		items, err := hranaProtoRead(step.bytes)
		if err != nil {
			t.Fatalf("invalid batch step %d: %v", index, err)
		}
		var encoded []byte
		for _, item := range items {
			if item.number == 2 && item.wire == 2 {
				encoded = item.bytes
			}
		}
		if encoded == nil {
			t.Fatalf("batch step %d has no statement result", index)
		}
		statement, err := hranaProtoRead(encoded)
		if err != nil {
			t.Fatalf("invalid batch step %d statement result: %v", index, err)
		}
		results = append(results, statement)
	}
	return results
}

func hranaProtoTestExpectCounters(t *testing.T, statement []hranaProtoField) {
	t.Helper()
	byNumber := make(map[int]hranaProtoField, len(statement))
	for _, field := range statement {
		byNumber[field.number] = field
	}
	if read, exists := byNumber[6]; !exists || read.wire != 0 || read.integer != 8625 {
		t.Fatalf("rows_read field: %+v", statement)
	}
	if written, exists := byNumber[7]; !exists || written.wire != 0 || written.integer != 0 {
		t.Fatalf("rows_written field: %+v", statement)
	}
	duration, exists := byNumber[8]
	if !exists || duration.wire != 1 || len(duration.bytes) != 8 {
		t.Fatalf("query_duration_ms field: %+v", statement)
	}
	if restored := math.Float64frombits(binary.LittleEndian.Uint64(duration.bytes)); restored != 955.25 {
		t.Fatalf("query_duration_ms = %v, want 955.25", restored)
	}
	if _, exists := byNumber[5]; exists {
		t.Fatalf("replication_index was written: %+v", statement)
	}
}

func TestHranaV3HTTPPipelineEncodesExecutionCounters(t *testing.T) {
	hook := func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true,
				"rows_read": uint64(8625), "rows_written": uint64(0), "query_duration_ms": 955.25}, nil
		}
		return nil, nil
	}
	const path = "/db/tenant-a/db-a/main/v3-protobuf/pipeline"
	t.Run("execute", func(t *testing.T) {
		fixture := newGatewayFixture(t)
		fixture.upstream.setHook(hook)
		handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
		response := testHranaV3Call(t, handler, fixture.tokenA, path, hranaProtoBytes(2, testHranaV3Statement()))
		if response.Code != http.StatusOK {
			t.Fatalf("pipeline returned %d: %s", response.Code, response.Body.String())
		}
		fields, err := hranaProtoRead(response.Body.Bytes())
		if err != nil || len(fields) == 0 {
			t.Fatalf("pipeline envelope: %v %v", fields, err)
		}
		hranaProtoTestExpectCounters(t, hranaProtoTestPipelineStatement(t, hranaProtoTestPipelineEntry(t, fields)))
	})
	t.Run("batch", func(t *testing.T) {
		fixture := newGatewayFixture(t)
		fixture.upstream.setHook(hook)
		handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
		batch := hranaProtoBytes(1, hranaProtoBytes(2, hranaProtoTestStatement()))
		batch = append(batch, hranaProtoBytes(1, hranaProtoBytes(2, hranaProtoTestStatement()))...)
		response := testHranaV3Call(t, handler, fixture.tokenA, path, hranaProtoBytes(2, hranaProtoBytes(3, hranaProtoBytes(1, batch))))
		if response.Code != http.StatusOK || fixture.upstream.count("statement") != 2 {
			t.Fatalf("batch pipeline returned %d, statements=%d: %s", response.Code, fixture.upstream.count("statement"), response.Body.String())
		}
		fields, err := hranaProtoRead(response.Body.Bytes())
		if err != nil || len(fields) == 0 {
			t.Fatalf("pipeline envelope: %v %v", fields, err)
		}
		results := hranaProtoTestPipelineBatch(t, hranaProtoTestPipelineEntry(t, fields))
		if len(results) != 2 {
			t.Fatalf("batch step results = %d, want 2", len(results))
		}
		for index, statement := range results {
			if len(statement) == 0 {
				t.Fatalf("batch step %d returned an empty statement result", index)
			}
			hranaProtoTestExpectCounters(t, statement)
		}
	})
}

// The fake worker answers with a bare JSON number, so the gateway decodes it
// into json.Number and hranaOutputValue turns it into float64 before the
// protobuf encoder runs. A Hrana-wrapped fixture would skip that real path.
func TestHranaV3HTTPPipelineEncodesRealValue(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{
				"cols":               []any{map[string]any{"name": "id", "decltype": "REAL"}},
				"rows":               [][]any{{json.Number("1.5")}},
				"affected_row_count": 0,
				"last_insert_rowid":  nil,
				"autocommit":         true,
			}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", hranaProtoBytes(2, testHranaV3Statement()))
	if response.Code != http.StatusOK || fixture.upstream.count("statement") != 1 {
		t.Fatalf("pipeline returned %d, statements=%d: %s", response.Code, fixture.upstream.count("statement"), response.Body.String())
	}
	fields, err := hranaProtoRead(response.Body.Bytes())
	if err != nil || len(fields) == 0 {
		t.Fatalf("pipeline envelope: %v %v", fields, err)
	}
	statement := hranaProtoTestPipelineStatement(t, hranaProtoTestPipelineEntry(t, fields))
	row := hranaProtoTestStatementRow(t, statement, 0)
	value, err := hranaProtoRead(row[0].bytes)
	if err != nil || len(value) != 1 || value[0].number != 3 || value[0].wire != 1 || len(value[0].bytes) != 8 {
		t.Fatalf("REAL value field: %v %v", value, err)
	}
	if restored := math.Float64frombits(binary.LittleEndian.Uint64(value[0].bytes)); restored != 1.5 {
		t.Fatalf("REAL value = %v, want 1.5", restored)
	}
}

func TestHranaV3HTTPReadCursorEncodesRealValue(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "REAL"}}, "is_explain": false, "is_readonly": true}, nil
		case "open_read_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "REAL"}}}, nil
		case "fetch_read_cursor":
			return map[string]any{"rows": [][]any{{json.Number("1.5")}}, "done": true}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "SELECT id FROM records"), hranaProtoNumber(5, 1)...)
	requestBody := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/cursor", requestBody)
	if response.Code != http.StatusOK {
		t.Fatalf("cursor returned %d: %s", response.Code, response.Body.String())
	}
	var frames [][]hranaProtoField
	encoded := response.Body.Bytes()
	for len(encoded) > 0 {
		length, size := binary.Uvarint(encoded)
		if size <= 0 || uint64(len(encoded[size:])) < length {
			t.Fatal("truncated cursor frame")
		}
		frame, err := hranaProtoRead(encoded[size : size+int(length)])
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame)
		encoded = encoded[size+int(length):]
	}
	// A truncated stream stops after step_begin; the step_end frame proves the
	// handler kept the cursor contract instead of closing on the value error.
	if len(frames) != 4 {
		t.Fatalf("cursor frames = %d, want header, step_begin, row, step_end", len(frames))
	}
	for index, want := range []int{1, 1, 4, 2} {
		if len(frames[index]) == 0 || frames[index][0].number != want {
			t.Fatalf("cursor frame %d = %v, want field %d", index, frames[index], want)
		}
	}
	row, err := hranaProtoRead(frames[2][0].bytes)
	if err != nil || len(row) != 1 || row[0].number != 1 || row[0].wire != 2 {
		t.Fatalf("cursor row: %v %v", row, err)
	}
	value, err := hranaProtoRead(row[0].bytes)
	if err != nil || len(value) != 1 || value[0].number != 3 || value[0].wire != 1 || len(value[0].bytes) != 8 {
		t.Fatalf("cursor REAL value field: %v %v", value, err)
	}
	if restored := math.Float64frombits(binary.LittleEndian.Uint64(value[0].bytes)); restored != 1.5 {
		t.Fatalf("cursor REAL value = %v, want 1.5", restored)
	}
}
