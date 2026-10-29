package platform

import (
	"context"
	"encoding/json"
	"testing"

	controller "peri-loom/controller"
)

func TestGatewayStatementReturnsNativeMetadataAndForwardsTypedArguments(t *testing.T) {
	fixture := newGatewayFixture(t)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	args := []any{map[string]any{"type": "text", "value": "hello"}}
	response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT ? AS message", "args": args})
	if response.Code != 200 {
		t.Fatalf("native statement: %d %s", response.Code, response.Body.String())
	}
	var result struct {
		Result struct {
			Columns []struct {
				Name string `json:"name"`
			} `json:"cols"`
			Rows             [][]string `json:"rows"`
			AffectedRowCount int64      `json:"affected_row_count"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Result.Columns) != 1 || result.Result.Columns[0].Name != "message" || len(result.Result.Rows) != 1 || result.Result.Rows[0][0] != "hello" {
		t.Fatalf("statement metadata missing: %s (%v)", response.Body.String(), err)
	}
	if fixture.upstream.count("statement") != 1 {
		t.Fatal("statement not routed to native worker")
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOperations(state, nil); err != nil {
		t.Fatalf("statement operation not durable across restart: %v", err)
	}
}

func TestGatewayStatementMalformedNativeResultIsUnknownAndNeverReplayed(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": nil}}, "rows": [][]any{{1, 2}}, "affected_row_count": 0, "last_insert_rowid": nil}, nil
		}
		return nil, nil
	}
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	request := map[string]any{"session": session, "sql": "SELECT id FROM proof", "args": []any{}}
	first := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", request)
	if first.Code != 502 || gatewayCode(t, first) != "outcome_unknown" || fixture.upstream.count("statement") != 1 {
		t.Fatalf("malformed native ACK not quarantined: %d %s", first.Code, first.Body.String())
	}
	second := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", request)
	if second.Code == 200 || fixture.upstream.count("statement") != 1 {
		t.Fatalf("uncertain statement replayed: %d %s", second.Code, second.Body.String())
	}
}

func TestGatewayStatementRejectsMalformedArgsBeforeDispatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	for _, args := range []any{
		[]any{map[string]any{"type": "integer", "value": "1); DROP TABLE proof"}},
		[]any{map[string]any{"type": "text", "value": "safe", "extra": true}},
		[]any{map[string]any{"type": "blob", "base64": "%%%"}},
	} {
		response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT ?", "args": args})
		if response.Code != 400 || fixture.upstream.count("statement") != 0 {
			t.Fatalf("malformed bind dispatched: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestGatewayStatementEmptyBlobPreservesRequiredBase64Field(t *testing.T) {
	fixture := newGatewayFixture(t)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT ?", "args": []any{map[string]any{"type": "blob", "base64": ""}}})
	if response.Code != 200 {
		t.Fatalf("empty blob rejected: %d %s", response.Code, response.Body.String())
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] == "statement" {
			args := call.command["args"].([]any)
			if _, present := args[0].(map[string]any)["base64"]; !present {
				t.Fatal("native blob argument lost required empty base64")
			}
		}
	}
}

func TestStatementResultValidator(t *testing.T) {
	const legacy = `"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true`
	withMetrics := func(rowsRead, rowsWritten, duration string) string {
		return `{` + legacy + `,"rows_read":` + rowsRead + `,"rows_written":` + rowsWritten + `,"query_duration_ms":` + duration + `}`
	}
	for _, scenario := range []struct {
		name  string
		body  string
		valid bool
	}{
		{"legacy five fields", `{` + legacy + `}`, true},
		{"eight fields with metrics", withMetrics("8625", "0", "955.25"), true},
		{"six fields", `{` + legacy + `,"rows_read":1}`, false},
		{"seven fields", `{` + legacy + `,"rows_read":1,"rows_written":0}`, false},
		{"nine fields", `{` + legacy + `,"rows_read":1,"rows_written":0,"query_duration_ms":1,"extra":null}`, false},
		{"eight fields without query_duration_ms", `{` + legacy + `,"rows_read":1,"rows_written":0,"extra":1}`, false},
		{"null rows_read", withMetrics("null", "0", "1.5"), false},
		{"null rows_written", withMetrics("1", "null", "1.5"), false},
		{"null query_duration_ms", withMetrics("1", "0", "null"), false},
		{"negative rows_read", withMetrics("-1", "0", "1.5"), false},
		{"negative rows_written", withMetrics("1", "-1", "1.5"), false},
		{"fractional rows_read", withMetrics("1.5", "0", "1.5"), false},
		{"exponent rows_read", withMetrics("1e3", "0", "1.5"), false},
		{"overflowing rows_read", withMetrics("18446744073709551616", "0", "1.5"), false},
		{"quoted rows_written", withMetrics("1", `"0"`, "1.5"), false},
		{"negative query_duration_ms", withMetrics("1", "0", "-0.5"), false},
		{"negative zero query_duration_ms", withMetrics("1", "0", "-0"), true}, // -0 compares equal to zero, so the current rule accepts it
		{"overflowing query_duration_ms", withMetrics("1", "0", "1e400"), false},
		{"quoted query_duration_ms", withMetrics("1", "0", `"1.5"`), false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if got := validStatementResult(json.RawMessage(scenario.body)); got != scenario.valid {
				t.Fatalf("validStatementResult(%s) = %v, want %v", scenario.body, got, scenario.valid)
			}
		})
	}
}
