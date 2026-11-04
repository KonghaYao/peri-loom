package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// Cursor rows are collected into a page buffer before one write. This pins
// that the buffered stream still carries the exact row bytes in step order.
func TestHranaV3JSONCursorPageBufferKeepsRowBytes(t *testing.T) {
	fixture := newGatewayFixture(t)
	rows := [][]any{{json.Number("7"), "alpha"}, {json.Number("8"), nil}}
	columns := []any{
		map[string]any{"name": "id", "decltype": "INTEGER"},
		map[string]any{"name": "label", "decltype": "TEXT"},
	}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": columns, "is_readonly": true, "is_explain": false}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": columns, "rows": rows, "done": true}, nil
		}
		return nil, nil
	})
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT id, label FROM records","args":[],"named_args":[],"want_rows":true}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != http.StatusOK {
		t.Fatalf("cursor failed: %d %s", response.Code, response.Body.String())
	}
	var expected bytes.Buffer
	var rowEncoder hranaJSONCursorRow
	for _, cells := range rows {
		rowEncoder.begin()
		if err := rowEncoder.appendCells(cells); err != nil {
			t.Fatalf("row encoder rejected %v: %v", cells, err)
		}
		expected.Write(rowEncoder.bytes())
	}
	stream := response.Body.Bytes()
	offset := bytes.Index(stream, expected.Bytes())
	if offset < 0 {
		t.Fatalf("buffered rows are not contiguous in the stream: %q", stream)
	}
	before, after := string(stream[:offset]), string(stream[offset+expected.Len():])
	beforeLines := strings.Split(strings.TrimSuffix(before, "\n"), "\n")
	afterLines := strings.Split(after, "\n")
	if len(beforeLines) != 2 || !strings.Contains(beforeLines[0], `"baton"`) || !strings.Contains(beforeLines[1], `"type":"step_begin"`) {
		t.Fatalf("rows arrived before their step_begin frame: %q", before)
	}
	if len(afterLines) < 1 || !strings.Contains(afterLines[0], `"type":"step_end"`) {
		t.Fatalf("rows arrived after their step_end frame: %q", after)
	}
}
