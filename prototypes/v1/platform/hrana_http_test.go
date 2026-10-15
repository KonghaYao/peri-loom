package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestHranaArgumentsRequireNativeValueShapes(t *testing.T) {
	if !validSQLArguments([]sqlArgument{{Type: "text", Value: json.RawMessage(`"x'); DELETE FROM accounts; --"`)}}) {
		t.Fatal("bound text was rejected")
	}
	if validSQLArguments([]sqlArgument{{Type: "integer", Value: json.RawMessage(`"NaN"`)}}) {
		t.Fatal("invalid integer was accepted")
	}
}

func TestHranaSequenceExecutesOnceThroughTrackedNativeSession(t *testing.T) {
	fixture := newGatewayFixture(t)
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"sequence","sql":"CREATE TABLE sample(id INTEGER); INSERT INTO sample VALUES(1)"},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"type":"sequence"`) || fixture.upstream.count("execute") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("multi-statement SQL was not tracked as one native execute: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaUsesNativeColumnsAndBindingWithoutRewritingSQL(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{map[string]any{"name": "actual_column", "decltype": "TEXT"}}, "rows": [][]any{{"it's bound"}}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}, nil
		}
		return nil, nil
	}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT ? FROM records","args":[{"type":"text","value":"it's bound"}],"want_rows":true}},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"name":"actual_column"`) {
		t.Fatalf("native column metadata missing: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("statement") != 1 || fixture.upstream.count("query") != 0 {
		t.Fatal("Hrana did not use a single native statement")
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] != "statement" {
			continue
		}
		if call.command["sql"] != "SELECT ? FROM records" {
			t.Fatal("statement was rewritten instead of binding")
		}
		args, valid := call.command["args"].([]any)
		if !valid || len(args) != 1 || args[0].(map[string]any)["value"] != "it's bound" {
			t.Fatal("native binding argument missing")
		}
	}
}

func TestHranaDescribeUsesPrepareOnlyNativeCommand(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			return map[string]any{"params": []any{map[string]any{"name": ":lookup"}}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": true}, nil
		}
		return nil, nil
	}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"describe","sql":"SELECT id WHERE id=:lookup"},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"is_readonly":true`) || fixture.upstream.count("describe") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("execute") != 0 {
		t.Fatalf("describe must prepare without execution: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaAuthenticationAndNativeWrite(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 1, "last_insert_rowid": int64(9223372036854775807), "autocommit": true}, nil
		}
		return nil, nil
	}
	handler := newHranaHTTP(fixture.gateway)
	path := "/db/tenant-a/db-a/main/v2/pipeline"
	call := func(credential, target, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+credential)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	write := `{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO proof VALUES(1)"}},{"type":"close"}]}`
	if response := call("bogus", path, write); response.Code != 403 {
		t.Fatalf("invalid credential: %d", response.Code)
	}
	if response := call(fixture.tokenB, path, write); response.Code != 403 {
		t.Fatalf("cross-project credential: %d", response.Code)
	}
	response := call(fixture.tokenA, path, write)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"last_insert_rowid":"9223372036854775807"`) || fixture.upstream.count("statement") != 1 {
		t.Fatalf("native write metadata missing: %d %s", response.Code, response.Body.String())
	}
	if response := call(fixture.tokenA, "/db/tenant-a/db-a/main/v3/pipeline", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}`); response.Code != 200 || fixture.upstream.count("statement") != 2 {
		t.Fatalf("v3 JSON pipeline: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaFullStreamCapacityRejectsBeforeOpeningSQL(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	for index := range maxHranaStreams {
		handler.streams[fmt.Sprint(index)] = &hranaStream{}
	}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1 AS one"}}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("unbounded stream admission: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_session") != 0 {
		t.Fatal("SQL opened despite exhausted stream capacity")
	}
}

func TestHranaPipelineTotalStepsRejectsBeforeOpeningSQL(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway)
	step := `{"stmt":{"sql":"SELECT 1","want_rows":true}}`
	steps := strings.TrimSuffix(strings.Repeat(step+",", 33), ",")
	body := `{"requests":[{"type":"batch","batch":{"steps":[` + steps + `]}},{"type":"batch","batch":{"steps":[` + steps + `]}}]}`
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("aggregate step limit: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 {
		t.Fatal("over-budget pipeline dispatched SQL")
	}
}

func TestHranaRouteRequiresExplicitExperimentalOptIn(t *testing.T) {
	fixture := newGatewayFixture(t)
	path := "/db/tenant-a/db-a/main/v2/pipeline"
	requestBody := `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1 AS one, 2 AS two"}},{"type":"close"}]}`
	call := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(requestBody))
		request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		newPublicMux(fixture.gateway, http.NotFoundHandler()).ServeHTTP(response, request)
		return response
	}
	t.Setenv("PERI_HRANA_EXPERIMENTAL", "")
	if response := call(); response.Code != 404 {
		t.Fatalf("default route exposed: %d", response.Code)
	}
	probe := func() int {
		request := httptest.NewRequest(http.MethodGet, "/db/tenant-a/db-a/main/v3-protobuf", nil)
		response := httptest.NewRecorder()
		newPublicMux(fixture.gateway, http.NotFoundHandler()).ServeHTTP(response, request)
		return response.Code
	}
	if code := probe(); code != 404 {
		t.Fatalf("default v3 probe exposed: %d", code)
	}
	t.Setenv("PERI_HRANA_EXPERIMENTAL", "true")
	if response := call(); response.Code != 404 {
		t.Fatalf("non-exact opt-in exposed route: %d", response.Code)
	}
	if code := probe(); code != 404 {
		t.Fatalf("non-exact opt-in exposed v3 probe: %d", code)
	}
	if fixture.upstream.count("open_session") != 0 {
		t.Fatal("disabled route dispatched SQL")
	}
	t.Setenv("PERI_HRANA_EXPERIMENTAL", "1")
	if code := probe(); code != 200 {
		t.Fatalf("explicit v3 probe unavailable: %d", code)
	}
	if response := call(); response.Code != 200 || !strings.Contains(response.Body.String(), `"type":"execute"`) {
		t.Fatalf("explicit opt-in unavailable: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatal("unexpected experimental operations")
	}
}

func TestHranaConnectionDiscoveryFollowsLiveRoute(t *testing.T) {
	fixture := newGatewayFixture(t)
	call := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "https://example.test/v1/platform/capabilities", nil)
		response := httptest.NewRecorder()
		newPublicMux(fixture.gateway, http.NotFoundHandler()).ServeHTTP(response, request)
		return response
	}
	for _, setting := range []struct {
		flag    string
		enabled bool
	}{
		{flag: "", enabled: false},
		{flag: "true", enabled: false},
		{flag: "1", enabled: true},
	} {
		t.Setenv("PERI_HRANA_EXPERIMENTAL", setting.flag)
		response := call()
		var capabilities struct {
			Version      int    `json:"version"`
			HranaHTTP    bool   `json:"hrana_http"`
			HranaWS      bool   `json:"hrana_ws"`
			PublicOrigin string `json:"public_origin"`
		}
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &capabilities) != nil || capabilities.Version != 1 || capabilities.HranaHTTP != setting.enabled || capabilities.HranaWS != setting.enabled || capabilities.PublicOrigin != "https://example.test" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("connection discovery flag=%q: %d %s", setting.flag, response.Code, response.Body.String())
		}
	}
	if fixture.upstream.count("open_session") != 0 {
		t.Fatal("capability discovery dispatched SQL")
	}
}

func TestHranaBatonScopedAndClosed(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway)
	call := func(credential, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+credential)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	first := call(fixture.tokenA, `{"requests":[{"type":"execute","stmt":{"sql":"SELECT id AS id, title AS title FROM records"}}]}`)
	if first.Code != 200 {
		t.Fatalf("read: %d %s", first.Code, first.Body.String())
	}
	var reply struct {
		Baton string `json:"baton"`
	}
	if json.Unmarshal(first.Body.Bytes(), &reply) != nil || reply.Baton == "" {
		t.Fatal("baton missing")
	}
	body, _ := json.Marshal(map[string]any{"baton": reply.Baton, "requests": []map[string]any{{"type": "close"}}})
	if other := call(fixture.tokenB, string(body)); other.Code != 403 {
		t.Fatalf("cross-tenant baton: %d", other.Code)
	}
	if closed := call(fixture.tokenA, string(body)); closed.Code != 200 || !strings.Contains(closed.Body.String(), `"baton":null`) {
		t.Fatalf("close: %d %s", closed.Code, closed.Body.String())
	}
	if replay := call(fixture.tokenA, string(body)); replay.Code != 403 {
		t.Fatalf("closed baton replay: %d", replay.Code)
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatal("unexpected native operations")
	}
}

func TestHranaIdleBatonExpiresAndClosesNativeSessionAfterKeyRevocation(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	reply := httptest.NewRecorder()
	handler.ServeHTTP(reply, request)
	var result struct {
		Baton string `json:"baton"`
	}
	if reply.Code != 200 || json.Unmarshal(reply.Body.Bytes(), &result) != nil || result.Baton == "" {
		t.Fatalf("idle stream not opened: %d %s", reply.Code, reply.Body.String())
	}
	if err := RevokeAPIKey(fixture.state, fixture.keyA.ID); err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	stream := handler.streams[result.Baton]
	handler.mu.Unlock()
	stream.mu.Lock()
	stream.expiresAt = time.Now().Add(-time.Second)
	stream.timer.Reset(time.Millisecond)
	stream.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for fixture.upstream.count("close_session") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	handler.mu.Lock()
	_, stillOpen := handler.streams[result.Baton]
	handler.mu.Unlock()
	if stillOpen || fixture.upstream.count("close_session") != 1 || fixture.upstream.count("statement") != 1 {
		t.Fatalf("revoked idle stream leaked: open=%v closes=%d", stillOpen, fixture.upstream.count("close_session"))
	}
}

func TestHranaConditionalBatchCombinesAndOrWithoutExecutingSkippedSQL(t *testing.T) {
	fixture := newGatewayFixture(t)
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"SELECT 1"}},{"condition":{"type":"and","conds":[{"type":"ok","step":0},{"type":"not","cond":{"type":"error","step":0}}]},"stmt":{"sql":"SELECT 2"}},{"condition":{"type":"or","conds":[{"type":"error","step":0},{"type":"ok","step":1}]},"stmt":{"sql":"SELECT 3"}},{"condition":{"type":"or","conds":[{"type":"error","step":0},{"type":"error","step":1}]},"stmt":{"sql":"SELECT 4"}}]}},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || fixture.upstream.count("statement") != 3 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("nested conditional batch not evaluated correctly: %d %s", response.Code, response.Body.String())
	}
	var result struct {
		Results []struct {
			Response struct {
				Result struct {
					Steps []any `json:"step_results"`
				} `json:"result"`
			} `json:"response"`
		} `json:"results"`
	}
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Results) == 0 || len(result.Results[0].Response.Result.Steps) != 4 || result.Results[0].Response.Result.Steps[3] != nil {
		t.Fatalf("skipped statement reported executed: %s", response.Body.String())
	}
}

func TestHranaAutocommitConditionUsesNativeState(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" && command["sql"] == "SELECT 1 AS state_probe" {
			return map[string]any{"cols": []any{map[string]any{"name": "state_probe", "decltype": nil}}, "rows": [][]any{{1}}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": false}, nil
		}
		return nil, nil
	})
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"batch","batch":{"steps":[{"condition":{"type":"is_autocommit"},"stmt":{"sql":"SELECT must_not_run"}}]}},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != 200 || fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") != 1 || !strings.Contains(response.Body.String(), `"step_results":[null]`) {
		t.Fatalf("autocommit condition did not use native state: %d %s", response.Code, response.Body.String())
	}
}
