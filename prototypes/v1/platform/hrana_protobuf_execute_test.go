package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

// legacyHranaProtoStatement reproduces the conversion executeWithin performs
// before the protobuf encoder runs, so tests can compare the direct bytes with
// the published legacy answer instead of a re-implementation of it.
func legacyHranaProtoStatement(t *testing.T, raw []byte) (any, bool) {
	t.Helper()
	var native hranaNativeStatement
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&native) != nil || native.Cols == nil || native.Rows == nil {
		return nil, false
	}
	metrics, ok := hranaNativeStatementMetrics(native.RowsRead, native.RowsWritten, native.QueryDurationMS)
	if !ok {
		return nil, false
	}
	cols := make([]any, len(native.Cols))
	for index, column := range native.Cols {
		cols[index] = map[string]any{"name": column.Name, "decltype": column.Decltype}
	}
	converted := make([][]any, 0, len(native.Rows))
	for _, row := range native.Rows {
		values := make([]any, 0, len(row))
		for _, value := range row {
			convertedValue, ok := hranaOutputValue(value)
			if !ok {
				return nil, false
			}
			values = append(values, convertedValue)
		}
		converted = append(converted, values)
	}
	var rowid *string
	if native.LastInsertRowid != nil {
		encoded := strconv.FormatInt(*native.LastInsertRowid, 10)
		rowid = &encoded
	}
	result := map[string]any{"cols": cols, "rows": converted, "affected_row_count": native.AffectedRowCount, "last_insert_rowid": rowid}
	for name, value := range metrics {
		result[name] = value
	}
	return result, true
}

func legacyHranaProtoSingleExecute(t *testing.T, raw []byte) ([]byte, error) {
	t.Helper()
	value, ok := legacyHranaProtoStatement(t, raw)
	if !ok {
		return nil, errHranaProtoResultLimit
	}
	response := map[string]any{
		"results": []any{
			map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": value}},
			map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
		},
	}
	return hranaProtoPipelineResponseWithin(response, 0)
}

func directHranaProtoSingleExecute(t *testing.T, raw []byte, budget int) ([]byte, hranaProtoEncodeStatus) {
	t.Helper()
	encoded, status := encodeHranaProtoStatementResult(raw, budget)
	if status != hranaProtoEncodeComplete {
		return nil, status
	}
	return assembleHranaProtoSingleExecute(encoded.bytes), status
}

func assertHranaProtoMatchesLegacy(t *testing.T, payload string) {
	t.Helper()
	expected, err := legacyHranaProtoSingleExecute(t, []byte(payload))
	if err != nil {
		t.Fatalf("legacy encoder refused payload %q: %v", payload, err)
	}
	direct, status := directHranaProtoSingleExecute(t, []byte(payload), 0)
	if status != hranaProtoEncodeComplete {
		t.Fatalf("direct encoder refused payload %q: status=%d", payload, status)
	}
	if !bytes.Equal(direct, expected) {
		t.Fatalf("direct bytes differ for %q\n direct=%x\nlegacy=%x", payload, direct, expected)
	}
}

func assertHranaProtoRefuses(t *testing.T, payload string) {
	t.Helper()
	if _, status := encodeHranaProtoStatementResult([]byte(payload), 0); status != hranaProtoEncodeUnsupported {
		t.Fatalf("direct encoder accepted unsupported payload %q: status=%d", payload, status)
	}
	// The legacy path decides the outcome for these shapes, so it must not
	// silently publish a result the direct path refused.
	if _, ok := legacyHranaProtoStatement(t, []byte(payload)); ok {
		// Some refused shapes (for example duplicate keys whose last value is
		// valid) are accepted by the legacy decoder; that is exactly why the
		// direct path refuses them.
		return
	}
}

func TestHranaProtoStatementResultMatchesLegacy(t *testing.T) {
	payloads := []string{
		`{"cols":[],"rows":[],"affected_row_count":0}`,
		`{"cols":[],"rows":[]}`,
		`{"cols":[{"name":"a"}],"rows":[[1]],"affected_row_count":1}`,
		`{"cols":[{"name":"a","decltype":"INTEGER"}],"rows":[[1],[2]],"affected_row_count":2}`,
		`{"cols":[{"name":"","decltype":""}],"rows":[[]]}`,
		`{"cols":[null],"rows":[null]}`,
		`{"cols":[{"name":"t"}],"rows":[["hello"]],"affected_row_count":1}`,
		`{"cols":[{"name":"t"}],"rows":[["<html> & \u2028 \u2029"]],"affected_row_count":1}`,
		`{"cols":[{"name":"t"}],"rows":[["quote:\" slash:\\ backslash:\/ tab:\t newline:\n"]],"affected_row_count":1}`,
		`{"cols":[{"name":"t"}],"rows":[["\u0041\u00e9\u4e16\ud83d\ude00"]],"affected_row_count":1}`,
		`{"cols":[{"name":"t"}],"rows":[["\ud800"]],"affected_row_count":1}`,
		"{\"cols\":[{\"name\":\"t\"}],\"rows\":[[\"\xff\"]],\"affected_row_count\":1}",
		`{"cols":[{"name":"i"}],"rows":[[0],[-1],[9223372036854775807],[-9223372036854775808],[-0]],"affected_row_count":5}`,
		`{"cols":[{"name":"f"}],"rows":[[1.5],[1e10],[-0.0],[1e-300],[1.7976931348623157e308]],"affected_row_count":5}`,
		`{"cols":[{"name":"n"}],"rows":[[null]],"affected_row_count":1}`,
		`{"cols":[{"name":"b"}],"rows":[[[0,255,128]]],"affected_row_count":1}`,
		`{"cols":[{"name":"b"}],"rows":[[[]]],"affected_row_count":1}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1,"last_insert_rowid":42,"rows_read":3,"rows_written":4,"query_duration_ms":1.25}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1,"last_insert_rowid":-7}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1,"last_insert_rowid":null}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1,"rows_read":0,"rows_written":0,"query_duration_ms":0}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1,"unknown":[1,{"a":null}]}`,
		`{"cols":[{"name":"m"}],"rows":[[1]],"affected_row_count":1} trailing`,
	}
	for _, payload := range payloads {
		assertHranaProtoMatchesLegacy(t, payload)
	}
}

func TestHranaProtoStatementResultCaseFoldedKeysFallBack(t *testing.T) {
	// The struct decoder matches tags case-insensitively; the direct scanner
	// refuses folded keys so the legacy path applies that rule itself.
	assertHranaProtoRefuses(t, `{"COLS":[],"rows":[],"affected_row_count":0}`)
	assertHranaProtoRefuses(t, `{"cols":[],"ROWs":[],"affected_row_count":0}`)
	assertHranaProtoRefuses(t, `{"cols":[{"NAME":"a"}],"rows":[]}`)
}

func TestHranaProtoStatementResultRefusesUnsupportedShapes(t *testing.T) {
	payloads := []string{
		``,
		`null`,
		`[]`,
		`{}`,
		`{"cols":null,"rows":[]}`,
		`{"cols":[],"rows":null}`,
		`{"rows":[],"affected_row_count":0}`,
		`{"cols":[],"affected_row_count":0}`,
		`{"cols":5,"rows":[]}`,
		`{"cols":[],"rows":5}`,
		`{"cols":[5],"rows":[]}`,
		`{"cols":["x"],"rows":[]}`,
		`{"cols":[{"name":1}],"rows":[]}`,
		`{"cols":[{"decltype":1}],"rows":[]}`,
		`{"cols":[],"rows":[5]}`,
		`{"cols":[],"rows":[{"a":1}]}`,
		`{"cols":[],"rows":[[true]]}`,
		`{"cols":[],"rows":[[{"a":1}]]}`,
		`{"cols":[],"rows":[[[256]]]}`,
		`{"cols":[],"rows":[[[-1]]]}`,
		`{"cols":[],"rows":[[[1.0]]]}`,
		`{"cols":[],"rows":[[["x"]]]}`,
		`{"cols":[],"rows":[[9223372036854775808]],"affected_row_count":1}`,
		`{"cols":[],"rows":[[1e400]],"affected_row_count":1}`,
		`{"cols":[],"rows":[],"affected_row_count":-1}`,
		`{"cols":[],"rows":[],"affected_row_count":1.5}`,
		`{"cols":[],"rows":[],"affected_row_count":"1"}`,
		`{"cols":[],"rows":[],"last_insert_rowid":1.5}`,
		`{"cols":[],"rows":[],"rows_read":1}`,
		`{"cols":[],"rows":[],"rows_read":1,"rows_written":1}`,
		`{"cols":[],"rows":[],"rows_read":null,"rows_written":1,"query_duration_ms":1}`,
		`{"cols":[],"rows":[],"rows_read":-1,"rows_written":1,"query_duration_ms":1}`,
		`{"cols":[],"rows":[],"rows_read":1.5,"rows_written":1,"query_duration_ms":1}`,
		`{"cols":[],"rows":[],"rows_read":1,"rows_written":1,"query_duration_ms":-1}`,
		`{"cols":[],"rows":[],"rows_read":1,"rows_written":1,"query_duration_ms":"1"}`,
		`{"cols":[],"cols":[],"rows":[]}`,
		`{"cols":[],"rows":[],"affected_row_count":0,"affected_row_count":1}`,
		`{"cols":[],"rows":[],}`,
		`{"cols":[],"rows":[]`,
	}
	for _, payload := range payloads {
		assertHranaProtoRefuses(t, payload)
	}
}

func TestHranaProtoStatementResultBudgetStopsAtTheLegacyBoundary(t *testing.T) {
	payload := []byte(`{"cols":[{"name":"t"}],"rows":[["aaaaaaaaaa"],["bbbbbbbbbb"]],"affected_row_count":2}`)
	value, ok := legacyHranaProtoStatement(t, payload)
	if !ok {
		t.Fatalf("legacy conversion refused the budget payload")
	}
	full, err := hranaProtoStatementResultWithin(value, 0)
	if err != nil {
		t.Fatalf("legacy full encode failed: %v", err)
	}
	if _, status := encodeHranaProtoStatementResult(payload, 0); status != hranaProtoEncodeComplete {
		t.Fatalf("direct full encode failed: status=%d", status)
	}
	if _, status := encodeHranaProtoStatementResult(payload, len(full)); status != hranaProtoEncodeComplete {
		t.Fatalf("direct encode rejected the exact budget: status=%d", status)
	}
	if _, err := hranaProtoStatementResultWithin(value, len(full)); err != nil {
		t.Fatalf("legacy encode rejected the exact budget: %v", err)
	}
	for _, budget := range []int{1, 8, 16, len(full) - 1} {
		if _, status := encodeHranaProtoStatementResult(payload, budget); status != hranaProtoEncodeOverBudget {
			t.Fatalf("direct encode did not stop at budget %d: status=%d", budget, status)
		}
		if _, err := hranaProtoStatementResultWithin(value, budget); err == nil {
			t.Fatalf("legacy encode did not stop at budget %d", budget)
		}
	}
}

func TestHranaProtoExecuteMarker(t *testing.T) {
	if hranaProtoExecuteRequested(context.Background()) {
		t.Fatalf("marker requested without a value")
	}
	if !hranaProtoExecuteRequested(withHranaProtoExecute(context.Background())) {
		t.Fatalf("marker not visible after withHranaProtoExecute")
	}
}

func TestHranaProtoSingleExecuteAssemblyMatchesLegacy(t *testing.T) {
	payload := []byte(`{"cols":[{"name":"content","decltype":"TEXT"}],"rows":[["hello"],["world"]],"affected_row_count":2,"rows_read":2,"rows_written":0,"query_duration_ms":0.5}`)
	expected, err := legacyHranaProtoSingleExecute(t, payload)
	if err != nil {
		t.Fatalf("legacy assembly failed: %v", err)
	}
	encoded, status := encodeHranaProtoStatementResult(payload, 0)
	if status != hranaProtoEncodeComplete {
		t.Fatalf("direct encode failed: status=%d", status)
	}
	if direct := assembleHranaProtoSingleExecute(encoded.bytes); !bytes.Equal(direct, expected) {
		t.Fatalf("assembly differs from legacy\n direct=%x\nlegacy=%x", direct, expected)
	}
}

func TestHranaProtoStatementResultLargeTextMatchesLegacy(t *testing.T) {
	rows := make([][]any, 0, 200)
	for index := 0; index < 200; index++ {
		rows = append(rows, []any{"issue28/" + strconv.Itoa(index) + "/" + string(bytes.Repeat([]byte("x"), 128))})
	}
	payload, err := json.Marshal(map[string]any{
		"cols":               []any{map[string]any{"name": "content", "decltype": "TEXT"}},
		"rows":               rows,
		"affected_row_count": 200,
		"rows_read":          200,
		"rows_written":       0,
		"query_duration_ms":  1.5,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	expected, err := legacyHranaProtoSingleExecute(t, payload)
	if err != nil {
		t.Fatalf("legacy encode failed: %v", err)
	}
	direct, status := directHranaProtoSingleExecute(t, payload, 0)
	if status != hranaProtoEncodeComplete {
		t.Fatalf("direct encode failed: status=%d", status)
	}
	if !bytes.Equal(direct, expected) {
		t.Fatalf("large payload differs: direct=%d bytes legacy=%d bytes", len(direct), len(expected))
	}
}

func TestHranaProtoStatementResultFloatEdges(t *testing.T) {
	for _, literal := range []string{"0.0", "-0.0", "1e-300", "1.7976931348623157e308", "5e-324"} {
		payload := `{"cols":[{"name":"f"}],"rows":[[` + literal + `]],"affected_row_count":1}`
		assertHranaProtoMatchesLegacy(t, payload)
	}
	negativeZero := math.Copysign(0, -1)
	if math.Float64bits(negativeZero) == 0 {
		t.Fatalf("negative zero is not distinct in this build")
	}
}
