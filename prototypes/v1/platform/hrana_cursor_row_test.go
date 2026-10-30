package platform

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// hranaLegacyProtoCursorRowFrame is the pre-optimization cursor row encoder:
// hranaOutputValue -> hranaProtoResultValue -> hranaProtoAppend -> writeFrame.
// It stays the byte-for-byte oracle for the direct encoder.
func hranaLegacyProtoCursorRowFrame(row []any) ([]byte, bool) {
	var encoded []byte
	for _, cell := range row {
		value, valid := hranaOutputValue(cell)
		if !valid {
			return nil, false
		}
		result, err := hranaProtoResultValue(value)
		if err != nil {
			return nil, false
		}
		encoded = hranaProtoAppend(encoded, 1, result)
	}
	payload := hranaProtoBytes(4, encoded)
	return append(hranaProtoVar(uint64(len(payload))), payload...), true
}

// hranaLegacyJSONCursorRow is the oracle for the direct NDJSON row writer.
func hranaLegacyJSONCursorRow(row []any) ([]byte, bool) {
	values := make([]any, 0, len(row))
	for _, cell := range row {
		value, valid := hranaOutputValue(cell)
		if !valid {
			return nil, false
		}
		values = append(values, value)
	}
	encoded, err := json.Marshal(map[string]any{"type": "row", "row": values})
	if err != nil {
		return nil, false
	}
	return append(encoded, '\n'), true
}

func hranaCursorRowFixture() [][]any {
	longText := strings.Repeat("长 text ", 400)
	longBlob := make([]any, 0, 400)
	for index := 0; index < 400; index++ {
		longBlob = append(longBlob, json.Number(fmt.Sprintf("%d", index%256)))
	}
	return [][]any{
		nil,
		{},
		{nil},
		{"hello"},
		{""},
		{"中文 😀 \u2028\u2029 é"},
		{"quote\" backslash\\ <html> & \n\t\r\b\f\x00\x1f\x7f"},
		{longText},
		{string([]byte{0xff, 0xfe})},
		{json.Number("0")},
		{json.Number("42")},
		{json.Number("-42")},
		{json.Number("127")},
		{json.Number("128")},
		{json.Number("-128")},
		{json.Number("-129")},
		{json.Number("9223372036854775807")},
		{json.Number("-9223372036854775808")},
		{json.Number("1.5")},
		{json.Number("-1.5")},
		{json.Number("0.0")},
		{json.Number("-0.0")},
		{json.Number("1e3")},
		{json.Number("1e-7")},
		{json.Number("1e21")},
		{json.Number("1.7976931348623157e308")},
		{json.Number("5e-324")},
		{[]any{}},
		{[]any{json.Number("0")}},
		{[]any{json.Number("255")}},
		{[]any{json.Number("0"), json.Number("1"), json.Number("254"), json.Number("255")}},
		{longBlob},
		{nil, json.Number("1"), "two", json.Number("2.5"), []any{json.Number("3")}},
	}
}

func TestHranaCursorRowEncodingMatchesLegacy(t *testing.T) {
	for index, row := range hranaCursorRowFixture() {
		t.Run(fmt.Sprintf("row-%d", index), func(t *testing.T) {
			legacyProto, protoValid := hranaLegacyProtoCursorRowFrame(row)
			var frame hranaProtoCursorRowFrame
			frame.begin()
			var protoErr error
			for _, cell := range row {
				if protoErr = frame.appendCell(cell); protoErr != nil {
					break
				}
			}
			if protoValid != (protoErr == nil) {
				t.Fatalf("protobuf validity = %t, direct encoder error = %v", protoValid, protoErr)
			}
			if protoValid && !bytes.Equal(frame.frame(), legacyProto) {
				t.Fatalf("protobuf row bytes differ\nlegacy: %x\nactual: %x", legacyProto, frame.frame())
			}

			legacyJSON, jsonValid := hranaLegacyJSONCursorRow(row)
			var jsonRow hranaJSONCursorRow
			jsonRow.begin()
			jsonErr := jsonRow.appendCells(row)
			if jsonValid != (jsonErr == nil) {
				t.Fatalf("JSON validity = %t, direct encoder error = %v", jsonValid, jsonErr)
			}
			if jsonValid && !bytes.Equal(jsonRow.bytes(), legacyJSON) {
				t.Fatalf("JSON row bytes differ\nlegacy: %s\nactual: %s", legacyJSON, jsonRow.bytes())
			}
		})
	}
}

func TestHranaCursorRowEncodingRejectsInvalidCells(t *testing.T) {
	for index, row := range [][]any{
		{json.Number("abc")},
		{json.Number("1e999")},
		{json.Number("NaN")},
		{json.Number("9223372036854775808")},
		{[]any{json.Number("256")}},
		{[]any{json.Number("-1")}},
		{[]any{json.Number("1.0")}},
		{[]any{"x"}},
		{float64(1.5)},
		{int(1)},
		{true},
		{map[string]any{}},
	} {
		t.Run(fmt.Sprintf("row-%d", index), func(t *testing.T) {
			if _, valid := hranaLegacyProtoCursorRowFrame(row); valid {
				t.Fatalf("legacy protobuf accepted invalid row %#v", row)
			}
			var frame hranaProtoCursorRowFrame
			frame.begin()
			protoErr := error(nil)
			for _, cell := range row {
				if protoErr = frame.appendCell(cell); protoErr != nil {
					break
				}
			}
			if protoErr == nil {
				t.Fatalf("direct protobuf encoder accepted invalid row %#v", row)
			}

			// Some cells are only invalid for protobuf: hranaOutputValue keeps
			// an integer as its original text, so NDJSON accepts values that
			// ParseInt rejects. The oracle decides which shape applies.
			legacyJSON, valid := hranaLegacyJSONCursorRow(row)
			var jsonRow hranaJSONCursorRow
			jsonRow.begin()
			jsonErr := jsonRow.appendCells(row)
			if valid != (jsonErr == nil) {
				t.Fatalf("JSON validity = %t, direct encoder error = %v", valid, jsonErr)
			}
			if valid && !bytes.Equal(jsonRow.bytes(), legacyJSON) {
				t.Fatalf("JSON row bytes differ\nlegacy: %s\nactual: %s", legacyJSON, jsonRow.bytes())
			}
		})
	}

	for _, number := range []string{"abc", "NaN", "9223372036854775808", "+5", "007"} {
		row := []any{json.Number(number)}
		legacy, valid := hranaLegacyJSONCursorRow(row)
		if !valid {
			t.Fatalf("legacy JSON rejected integer text %q", number)
		}
		var jsonRow hranaJSONCursorRow
		jsonRow.begin()
		if err := jsonRow.appendCells(row); err != nil {
			t.Fatalf("direct JSON encoder rejected integer text %q: %v", number, err)
		}
		if !bytes.Equal(jsonRow.bytes(), legacy) {
			t.Fatalf("JSON integer text %q differs\nlegacy: %s\nactual: %s", number, legacy, jsonRow.bytes())
		}
	}

	var frame hranaProtoCursorRowFrame
	frame.begin()
	if frame.appendFloat(math.NaN()) == nil {
		t.Fatal("direct protobuf encoder accepted NaN")
	}
	frame.begin()
	if frame.appendFloat(math.Inf(-1)) == nil {
		t.Fatal("direct protobuf encoder accepted an infinity")
	}
}

func TestHranaCursorRowWireGolden(t *testing.T) {
	row := []any{nil, "hi", json.Number("1"), json.Number("1.5"), []any{json.Number("0"), json.Number("255")}}
	var frame hranaProtoCursorRowFrame
	frame.begin()
	for _, cell := range row {
		if err := frame.appendCell(cell); err != nil {
			t.Fatal(err)
		}
	}
	wantProto := []byte{
		0x21, 0x22, 0x1f,
		0x0a, 0x02, 0x0a, 0x00,
		0x0a, 0x04, 0x22, 0x02, 'h', 'i',
		0x0a, 0x02, 0x10, 0x02,
		0x0a, 0x09, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f,
		0x0a, 0x04, 0x2a, 0x02, 0x00, 0xff,
	}
	if !bytes.Equal(frame.frame(), wantProto) {
		t.Fatalf("protobuf golden mismatch\nwant: %x\n got: %x", wantProto, frame.frame())
	}

	var jsonRow hranaJSONCursorRow
	jsonRow.begin()
	if err := jsonRow.appendCells(row); err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"row":[{"type":"null"},{"type":"text","value":"hi"},{"type":"integer","value":"1"},{"type":"float","value":1.5},{"base64":"AP8=","type":"blob"}],"type":"row"}` + "\n"
	if string(jsonRow.bytes()) != wantJSON {
		t.Fatalf("JSON golden mismatch\nwant: %s\n got: %s", wantJSON, jsonRow.bytes())
	}
}

func TestHranaCursorRowEncoderReuse(t *testing.T) {
	var frame hranaProtoCursorRowFrame
	var jsonRow hranaJSONCursorRow
	for index, row := range hranaCursorRowFixture() {
		legacyProto, valid := hranaLegacyProtoCursorRowFrame(row)
		if !valid {
			continue
		}
		frame.begin()
		encoderValid := true
		for _, cell := range row {
			if frame.appendCell(cell) != nil {
				encoderValid = false
				break
			}
		}
		if !encoderValid || !bytes.Equal(frame.frame(), legacyProto) {
			t.Fatalf("protobuf reuse diverged at row %d", index)
		}
		legacyJSON, valid := hranaLegacyJSONCursorRow(row)
		if !valid {
			continue
		}
		jsonRow.begin()
		if jsonRow.appendCells(row) != nil || !bytes.Equal(jsonRow.bytes(), legacyJSON) {
			t.Fatalf("JSON reuse diverged at row %d", index)
		}
	}
}

func TestHranaV3CursorRowEncodingEndToEnd(t *testing.T) {
	rows := [][]any{
		{nil, "hello", json.Number("42"), json.Number("1.5"), []any{json.Number("0"), json.Number("255")}},
		{"quote\" <&> \n中文", json.Number("-7"), json.Number("9223372036854775807"), json.Number("-1.5"), []any{}},
	}
	cols := []any{
		map[string]any{"name": "a", "decltype": "TEXT"},
		map[string]any{"name": "b", "decltype": "TEXT"},
		map[string]any{"name": "c", "decltype": "INTEGER"},
		map[string]any{"name": "d", "decltype": "REAL"},
		map[string]any{"name": "e", "decltype": "BLOB"},
	}
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": cols, "is_explain": false, "is_readonly": true}, nil
		case "open_read_cursor":
			return map[string]any{"cols": cols}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": cols, "rows": rows, "done": true}, nil
		case "fetch_read_cursor":
			return map[string]any{"rows": rows, "done": true}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)

	statement := append(hranaProtoString(1, "SELECT a, b, c, d, e FROM records"), hranaProtoNumber(5, 1)...)
	requestBody := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/cursor", requestBody)
	if response.Code != http.StatusOK {
		t.Fatalf("protobuf cursor returned %d: %s", response.Code, response.Body.String())
	}
	var rowFrames [][]byte
	encoded := response.Body.Bytes()
	for len(encoded) > 0 {
		length, size := binary.Uvarint(encoded)
		if size <= 0 || uint64(len(encoded)-size) < length {
			t.Fatal("truncated cursor frame")
		}
		frame := encoded[:size+int(length)]
		fields, err := hranaProtoRead(frame[size:])
		if err != nil {
			t.Fatalf("cursor frame %x: %v", frame, err)
		}
		if len(fields) == 1 && fields[0].number == 4 {
			rowFrames = append(rowFrames, frame)
		}
		encoded = encoded[size+int(length):]
	}
	if len(rowFrames) != len(rows) {
		t.Fatalf("row frames = %d, want %d", len(rowFrames), len(rows))
	}
	for index, row := range rows {
		legacy, valid := hranaLegacyProtoCursorRowFrame(row)
		if !valid {
			t.Fatalf("legacy encoder rejected row %d", index)
		}
		if !bytes.Equal(rowFrames[index], legacy) {
			t.Fatalf("protobuf row %d differs\nwant: %x\n got: %x", index, legacy, rowFrames[index])
		}
	}

	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT a, b, c, d, e FROM records","args":[],"named_args":[],"want_rows":true}}]}}`
	jsonResponse := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if jsonResponse.Code != http.StatusOK {
		t.Fatalf("JSON cursor returned %d: %s", jsonResponse.Code, jsonResponse.Body.String())
	}
	lines := bytes.Split(bytes.TrimSuffix(jsonResponse.Body.Bytes(), []byte("\n")), []byte("\n"))
	if len(lines) != len(rows)+3 {
		t.Fatalf("JSON cursor lines = %d, want %d: %s", len(lines), len(rows)+3, jsonResponse.Body.String())
	}
	for index, row := range rows {
		legacy, valid := hranaLegacyJSONCursorRow(row)
		if !valid {
			t.Fatalf("legacy JSON encoder rejected row %d", index)
		}
		if !bytes.Equal(lines[index+2], bytes.TrimSuffix(legacy, []byte("\n"))) {
			t.Fatalf("JSON row %d differs\nwant: %s\n got: %s", index, legacy, lines[index+2])
		}
	}
}
