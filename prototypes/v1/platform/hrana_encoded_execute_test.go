package platform

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
)

// legacyEncodeHranaExecuteEntry is the adapter path R49 replaces: decode the
// worker result into generic values, convert every cell, then let encoding/json
// encode the entry. The differential tests compare the single-pass encoder
// against exactly these bytes, so the pipeline may choose either path without
// the reply changing.
func legacyEncodeHranaExecuteEntry(result []byte) ([]byte, string) {
	var native hranaNativeStatement
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.UseNumber()
	if decoder.Decode(&native) != nil || native.Cols == nil || native.Rows == nil {
		return nil, "invalid_native_statement"
	}
	metrics, ok := hranaNativeStatementMetrics(native.RowsRead, native.RowsWritten, native.QueryDurationMS)
	if !ok {
		return nil, "invalid_native_statement"
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
				return nil, "invalid_native_value"
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
	value := map[string]any{"cols": cols, "rows": converted, "affected_row_count": native.AffectedRowCount, "last_insert_rowid": rowid}
	for name, metric := range metrics {
		value[name] = metric
	}
	entry, err := json.Marshal(map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": value}})
	if err != nil {
		return nil, "marshal"
	}
	return entry, ""
}

// encodedExecuteEntryCase declares one statement result. encodable marks the
// shapes the single-pass encoder must handle itself; every other case has to
// fall back, and code is the classification the legacy path gives it (empty
// when the legacy path accepts the result and only the encoder is conservative).
type encodedExecuteEntryCase struct {
	name      string
	result    string
	encodable bool
	code      string
}

func encodedExecuteEntryCases() []encodedExecuteEntryCase {
	return []encodedExecuteEntryCase{
		{name: "empty-shape", result: `{"cols":[],"rows":[]}`, encodable: true},
		{name: "empty-object", result: `{}`, code: "invalid_native_statement"},
		{name: "rows-null", result: `{"cols":[],"rows":null}`, code: "invalid_native_statement"},
		{name: "cols-null", result: `{"cols":null,"rows":[]}`, code: "invalid_native_statement"},
		{name: "rows-missing", result: `{"cols":[]}`, code: "invalid_native_statement"},
		{name: "not-an-object", result: `[]`, code: "invalid_native_statement"},
		{name: "row-null", result: `{"cols":[],"rows":[null]}`, encodable: true},
		{name: "column-null", result: `{"cols":[null],"rows":[]}`, encodable: true},
		{name: "column-empty-object", result: `{"cols":[{}],"rows":[]}`, encodable: true},
		{name: "column-null-fields", result: `{"cols":[{"name":null,"decltype":null}],"rows":[[]]}`, encodable: true},
		{
			name:      "text-escapes",
			result:    `{"cols":[],"rows":[["quote\" backslash\\ slash/ ctrl\u0000\u001f bell\u0007 short\b\f\n\r\t del\u007f <>& \u2028 \u2029"]]}`,
			encodable: true,
		},
		{name: "text-utf8", result: `{"cols":[],"rows":[["中文 😀 ünïcode"]]}`, encodable: true},
		{name: "text-invalid-utf8", result: "{\"cols\":[],\"rows\":[[\"broken \xff byte\"]]}", encodable: true},
		{name: "text-surrogates", result: `{"cols":[],"rows":[["\ud83d\ude00 \ud800 \u0041"]]}`, encodable: true},
		{name: "integers", result: `{"cols":[],"rows":[[-0],[9223372036854775807],[-9223372036854775808],[9223372036854775808],[123456789012345678901234]]}`, encodable: true},
		{name: "floats", result: `{"cols":[],"rows":[[1.0],[1e3],[-0.0],[1e-7],[1e21],[1.2345678901234567],[0.1]]}`, encodable: true},
		{name: "float-overflow", result: `{"cols":[],"rows":[[1e999]]}`, code: "invalid_native_value"},
		{name: "blobs", result: `{"cols":[],"rows":[[[],[0,255],[1,2,3]]]}`, encodable: true},
		{name: "blob-out-of-range", result: `{"cols":[],"rows":[[[256]]]}`, code: "invalid_native_value"},
		{name: "blob-fraction", result: `{"cols":[],"rows":[[[1.5]]]}`, code: "invalid_native_value"},
		{name: "blob-string", result: `{"cols":[],"rows":[[["x"]]]}`, code: "invalid_native_value"},
		{name: "cell-boolean", result: `{"cols":[],"rows":[[true]]}`, code: "invalid_native_value"},
		{name: "cell-object", result: `{"cols":[],"rows":[[{"a":1}]]}`, code: "invalid_native_value"},
		{name: "rowid-null", result: `{"cols":[],"rows":[],"last_insert_rowid":null}`, encodable: true},
		{name: "rowid-negative-zero", result: `{"cols":[],"rows":[],"last_insert_rowid":-0}`, encodable: true},
		{name: "rowid-value", result: `{"cols":[],"rows":[],"last_insert_rowid":-42}`, encodable: true},
		{name: "rowid-fraction", result: `{"cols":[],"rows":[],"last_insert_rowid":1.5}`, code: "invalid_native_statement"},
		{name: "affected-missing", result: `{"cols":[],"rows":[],"last_insert_rowid":null}`, encodable: true},
		{name: "affected-null", result: `{"cols":[],"rows":[],"affected_row_count":null}`, encodable: true},
		{name: "affected-negative", result: `{"cols":[],"rows":[],"affected_row_count":-1}`, encodable: true},
		{name: "affected-fraction", result: `{"cols":[],"rows":[],"affected_row_count":1.5}`, code: "invalid_native_statement"},
		{
			name:      "metrics",
			result:    `{"cols":[],"rows":[],"rows_read":1000,"rows_written":0,"query_duration_ms":0.25}`,
			encodable: true,
		},
		{name: "metrics-integer-duration", result: `{"cols":[],"rows":[],"rows_read":0,"rows_written":0,"query_duration_ms":3}`, encodable: true},
		{name: "metrics-negative-duration", result: `{"cols":[],"rows":[],"rows_read":0,"rows_written":0,"query_duration_ms":-0.5}`, code: "invalid_native_statement"},
		{name: "metrics-read-null", result: `{"cols":[],"rows":[],"rows_read":null,"rows_written":0,"query_duration_ms":1}`, code: "invalid_native_statement"},
		{name: "metrics-partial", result: `{"cols":[],"rows":[],"rows_read":0}`, code: "invalid_native_statement"},
		{name: "metrics-fraction-read", result: `{"cols":[],"rows":[],"rows_read":1.5,"rows_written":0,"query_duration_ms":1}`, code: "invalid_native_statement"},
		{name: "metrics-read-overflow", result: `{"cols":[],"rows":[],"rows_read":18446744073709551616,"rows_written":0,"query_duration_ms":1}`, code: "invalid_native_statement"},
		{name: "metrics-duration-overflow", result: `{"cols":[],"rows":[],"rows_read":0,"rows_written":0,"query_duration_ms":1e999}`, code: "invalid_native_statement"},
		{
			name:      "duplicate-cols-key",
			result:    `{"cols":[{"name":"first","decltype":null}],"cols":[{"name":"second","decltype":"TEXT"}],"rows":[[]]}`,
			encodable: true,
		},
		{name: "duplicate-rows-key", result: `{"cols":[],"rows":[[1]],"rows":[]}`, encodable: true},
		{name: "duplicate-affected-key", result: `{"cols":[],"rows":[],"affected_row_count":1,"affected_row_count":2}`, encodable: true},
		{name: "folded-key", result: `{"COLS":[],"rows":[]}`, code: ""},
		{name: "unknown-key", result: `{"cols":[],"rows":[],"autocommit":true}`, encodable: true},
		{name: "escaped-key", result: `{"col\u0073":[],"rows":[]}`, encodable: true},
		{name: "trailing-content", result: `{"cols":[],"rows":[]} trailing`, encodable: true},
		{name: "column-non-string-name", result: `{"cols":[{"name":5,"decltype":null}],"rows":[]}`, code: "invalid_native_statement"},
		{name: "column-folded-key", result: `{"cols":[{"Name":"x","decltype":null}],"rows":[]}`, code: ""},
		{name: "column-duplicate-name", result: `{"cols":[{"name":"first","name":"second","decltype":null}],"rows":[[]]}`, encodable: true},
		{name: "column-not-an-object", result: `{"cols":[[]],"rows":[]}`, code: "invalid_native_statement"},
		{name: "rows-object", result: `{"cols":[],"rows":{}}`, code: "invalid_native_statement"},
		{name: "row-scalar", result: `{"cols":[],"rows":[7]}`, code: "invalid_native_statement"},
		{name: "spaced", result: "{ \"cols\" : [ ] ,\n \"rows\" : [ [ 1 ] ] }", encodable: true},
	}
}

func TestEncodedHranaExecuteEntryMatchesTheDecodedPath(t *testing.T) {
	for _, testCase := range encodedExecuteEntryCases() {
		t.Run(testCase.name, func(t *testing.T) {
			entry, encoded := encodeHranaExecuteEntry(json.RawMessage(testCase.result))
			legacy, legacyCode := legacyEncodeHranaExecuteEntry([]byte(testCase.result))
			if legacyCode != testCase.code {
				t.Fatalf("legacy classification %q, want %q", legacyCode, testCase.code)
			}
			if !testCase.encodable {
				if encoded {
					t.Fatalf("encoded a result the legacy path must decide: %s", entry)
				}
				return
			}
			if !encoded {
				t.Fatalf("refused a reproducible result (legacy classification %q)", legacyCode)
			}
			if legacyCode != "" {
				t.Fatalf("legacy path failed: %s", legacyCode)
			}
			if !bytes.Equal([]byte(entry), legacy) {
				t.Fatalf("entry bytes differ\n new: %s\n old: %s", entry, legacy)
			}
		})
	}
}

// TestEncodedHranaExecuteEntryMatchesTheR1Fixture runs the differential on the
// 1,000-row result the R1 benchmark reads. The fixture lives outside the
// repository, so the test skips when it is not present.
func TestEncodedHranaExecuteEntryMatchesTheR1Fixture(t *testing.T) {
	result, err := os.ReadFile("/tmp/peri-perf/r48-result-1000.json")
	if err != nil {
		t.Skipf("R1 fixture unavailable: %v", err)
	}
	entry, encoded := encodeHranaExecuteEntry(json.RawMessage(result))
	if !encoded {
		t.Fatal("the R1 result was refused")
	}
	legacy, code := legacyEncodeHranaExecuteEntry(result)
	if code != "" {
		t.Fatalf("legacy path failed: %s", code)
	}
	if !bytes.Equal([]byte(entry), legacy) {
		t.Fatalf("entry bytes differ: new %d bytes, old %d bytes", len(entry), len(legacy))
	}
}

// TestHranaEncodedEntrySurvivesReencoding pins the property the failure paths
// depend on: json.Marshal of a Marshaler compacts and HTML-escapes the output,
// which must leave an already encoded entry unchanged.
func TestHranaEncodedEntrySurvivesReencoding(t *testing.T) {
	entry, encoded := encodeHranaExecuteEntry(json.RawMessage(`{"cols":[],"rows":[["<&> \u2028 quote\" backslash\\"]],"affected_row_count":0,"last_insert_rowid":null}`))
	if !encoded {
		t.Fatal("refused a reproducible result")
	}
	wrapped, err := json.Marshal(map[string]any{"entry": entry})
	if err != nil {
		t.Fatal(err)
	}
	if expected := `{"entry":` + string(entry) + `}`; string(wrapped) != expected {
		t.Fatalf("re-encoding changed the entry\n got: %s\nwant: %s", wrapped, expected)
	}
}

func TestAssembleHranaResponseMatchesTheStructMarshal(t *testing.T) {
	entries := [][]byte{
		[]byte(`{"response":{"type":"close"},"type":"ok"}`),
		[]byte(`{"response":{"result":{"affected_row_count":0},"type":"execute"},"type":"ok"}`),
		[]byte(`{"error":{"code":"invalid_native_value","message":"invalid_native_value"},"type":"error"}`),
	}
	batons := []*string{nil, stringPointer(""), stringPointer("baton-1"), stringPointer("baton-\u2028-<&>\"")}
	for _, baton := range batons {
		for count := 0; count <= len(entries); count++ {
			results := make([]any, count)
			for index := 0; index < count; index++ {
				results[index] = json.RawMessage(entries[index])
			}
			expected, err := json.Marshal(hranaResponse{Baton: baton, Results: results})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := assembleHranaResponse(baton, entries[:count])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("baton %v count %d: %s != %s", baton, count, actual, expected)
			}
		}
	}
}

func stringPointer(value string) *string {
	return &value
}

// TestHranaExecuteWithinEncodesThePipelineEntry exercises the wiring: only a
// request that carries both the fused answer and the pipeline's entry gate may
// return the encoded entry, and the bytes must match the decoded path.
func TestHranaExecuteWithinEncodesThePipelineEntry(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	opened := runPipelineAuditRequest(t, handler, fixture, []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 42", "want_rows": true}},
	})
	if opened.Code != http.StatusOK {
		t.Fatalf("open pipeline: %d %s", opened.Code, opened.Body.String())
	}
	var envelope struct {
		Baton string `json:"baton"`
	}
	if err := json.Unmarshal(opened.Body.Bytes(), &envelope); err != nil || envelope.Baton == "" {
		t.Fatalf("baton: %q %v from %s", envelope.Baton, err, opened.Body.String())
	}
	handler.mu.Lock()
	stream := handler.streams[envelope.Baton]
	handler.mu.Unlock()
	if stream == nil {
		t.Fatalf("stream %q is not registered", envelope.Baton)
	}
	stream.mu.Lock()
	session := stream.session
	stream.mu.Unlock()
	if session == "" {
		t.Fatal("the open pipeline retired its session")
	}
	published, err := json.Marshal(guardedBatchReadResult(42))
	if err != nil {
		t.Fatal(err)
	}
	statement := hranaStatement{SQL: "SELECT 42", WantRows: true}
	marker := fusedStatementResult{
		Session:     session,
		SQL:         statement.SQL,
		Reply:       published,
		Answer:      fusedStatementAnswer{kind: fusedStatementOK, result: published},
		AnswerValid: true,
	}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request = request.WithContext(withFusedStatementResult(request.Context(), marker))
	request = request.WithContext(withHranaExecuteEntry(request.Context()))
	value, reply := handler.executeWithin(request, fixture.tokenA, session, statement, 0)
	if reply.code != "" {
		t.Fatalf("execute: %d %s", reply.status, reply.code)
	}
	entry, ok := value.(hranaEncodedEntry)
	if !ok {
		t.Fatalf("executeWithin returned %T, want an encoded entry", value)
	}
	legacy, code := legacyEncodeHranaExecuteEntry(published)
	if code != "" {
		t.Fatalf("legacy path failed: %s", code)
	}
	if !bytes.Equal([]byte(entry), legacy) {
		t.Fatalf("entry bytes differ\n new: %s\n old: %s", entry, legacy)
	}

	// The protobuf adapter consumes the same fused answer without the gate, so
	// it must keep receiving the decoded value.
	plain := httptest.NewRequest(http.MethodPost, "/", nil)
	plain = plain.WithContext(withFusedStatementResult(plain.Context(), marker))
	decoded, reply := handler.executeWithin(plain, fixture.tokenA, session, statement, 0)
	if reply.code != "" {
		t.Fatalf("execute without the gate: %d %s", reply.status, reply.code)
	}
	if _, ok := decoded.(hranaEncodedEntry); ok {
		t.Fatal("the encoded entry escaped to a caller that did not ask for it")
	}
	if _, ok := decoded.(map[string]any); !ok {
		t.Fatalf("decoded value is %T, want the result map", decoded)
	}
}

// TestHranaPipelinePublishesTheEncodedEntryBytes compares a whole fused
// pipeline reply with the bytes the decoded path produces, so the response
// assembly is checked byte for byte.
func TestHranaPipelinePublishesTheEncodedEntryBytes(t *testing.T) {
	fixture := newGatewayFixture(t)
	result := map[string]any{
		"cols":               []any{map[string]any{"name": "content", "decltype": "TEXT"}},
		"rows":               [][]any{{"alpha <&> beta"}, {nil}},
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"autocommit":         true,
		"rows_read":          2,
		"rows_written":       0,
		"query_duration_ms":  0.25,
	}
	fixture.upstream.setHook(fusedOpenHook(func(map[string]any) (any, error) {
		return map[string]any{
			"session":   fusedOpenSession,
			"readonly":  false,
			"statement": map[string]any{"status": "ok", "result": result},
			"close":     map[string]any{"status": "ok"},
		}, nil
	}))
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK {
		t.Fatalf("pipeline: %d %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Baton json.RawMessage `json:"baton"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || len(envelope.Baton) == 0 {
		t.Fatalf("baton: %v from %s", err, response.Body.String())
	}
	published, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	entry, code := legacyEncodeHranaExecuteEntry(published)
	if code != "" {
		t.Fatalf("legacy path failed: %s", code)
	}
	expected := `{"baton":` + string(envelope.Baton) + `,"results":[` + string(entry) + `,{"response":{"type":"close"},"type":"ok"}]}` + "\n"
	if response.Body.String() != expected {
		t.Fatalf("pipeline body differs\n got: %s\nwant: %s", response.Body.String(), expected)
	}
}

// TestHranaPipelineKeepsTheLegacyClassificationForUnsupportedCells checks the
// fallback wiring: a cell the single-pass encoder refuses must reach the client
// with the classification the decoded path always gave it, not a new local code.
func TestHranaPipelineKeepsTheLegacyClassificationForUnsupportedCells(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(func(map[string]any) (any, error) {
		return map[string]any{
			"session":  fusedOpenSession,
			"readonly": false,
			"statement": map[string]any{"status": "ok", "result": map[string]any{
				"cols":               []any{map[string]any{"name": "flag", "decltype": "BOOL"}},
				"rows":               [][]any{{true}},
				"affected_row_count": 0,
				"last_insert_rowid":  nil,
				"autocommit":         true,
			}},
			"close": map[string]any{"status": "ok"},
		}, nil
	}))
	rejected := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if rejected.Code != http.StatusBadGateway || !bytes.Contains(rejected.Body.Bytes(), []byte(`"invalid_native_value"`)) {
		t.Fatalf("unsupported cell: %d %s", rejected.Code, rejected.Body.String())
	}
}
