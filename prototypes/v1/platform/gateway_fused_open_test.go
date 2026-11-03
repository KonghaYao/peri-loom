package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

const fusedOpenSession = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// fusedOpenHook answers the private capability and fused-open commands. fused
// decides the statement half of the open, so a test can model a read, a write
// refusal, a SQL error or an unknown outcome without a second code path.
func fusedOpenHook(fused func(map[string]any) (any, error)) func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 1}, nil
		case "open_guarded_statement":
			if fused != nil {
				return fused(command)
			}
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "ok", "result": guardedBatchReadResult(42)},
			}, nil
		case "open_session":
			return map[string]any{"session": fusedOpenSession, "readonly": false}, nil
		case "guarded_readonly_statement":
			return guardedBatchReadResult(9), nil
		case "statement":
			return guardedBatchWriteResult(), nil
		case "guarded_readonly_batch":
			raw, _ := command["statements"].([]any)
			items := make([]any, len(raw))
			for index := range raw {
				items[index] = map[string]any{"index": index, "status": "ok", "result": guardedBatchReadResult(index + 1)}
			}
			return map[string]any{"items": items, "next": len(raw)}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	}
}

func fusedOpenPipelineRequests() []any {
	return []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 42", "want_rows": true}},
		map[string]any{"type": "close"},
	}
}

func TestHranaPipelineFusesTheImplicitOpenWithTheFirstRead(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"execute"`) || !strings.Contains(response.Body.String(), "42") {
		t.Fatalf("fused pipeline: %d %s", response.Code, response.Body.String())
	}
	if opens := fixture.upstream.count("open_guarded_statement"); opens != 1 {
		t.Fatalf("fused opens: %d", opens)
	}
	if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("guarded_readonly_statement") != 0 || fixture.upstream.count("guarded_readonly_batch") != 0 {
		t.Fatal("the fused statement was dispatched a second time")
	}
	if closes := fixture.upstream.count("close_session"); closes != 1 {
		t.Fatalf("closes: %d", closes)
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] != "open_guarded_statement" {
			continue
		}
		if call.command["sql"] != "SELECT 42" || call.command["want_rows"] != true {
			t.Fatalf("fused command lost the statement: %v", call.command)
		}
	}
	// The open and the statement still record their own facts.
	if len(fixture.audit.outcomesOf("open")) != 1 || len(fixture.audit.outcomesOf("statement")) != 1 {
		t.Fatalf("audit outcomes: open=%v statement=%v", fixture.audit.outcomesOf("open"), fixture.audit.outcomesOf("statement"))
	}
}

func TestHranaPipelineFusedWriteTakesTheCommittingPath(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(func(map[string]any) (any, error) {
		return map[string]any{
			"session":   fusedOpenSession,
			"readonly":  false,
			"statement": map[string]any{"status": "requires_intent", "reason": "engine_writable"},
		}, nil
	}))
	requests := []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records(id) VALUES(1)", "want_rows": false}},
		map[string]any{"type": "close"},
	}
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, requests)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"execute"`) {
		t.Fatalf("committing pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("statement") != 1 || fixture.upstream.count("open_session") != 0 {
		t.Fatalf("fused=%d statement=%d open=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("statement"), fixture.upstream.count("open_session"))
	}
	// The committing dispatch happens only after the durable intent exists.
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	intent := false
	for _, record := range state.Operations {
		if record.Action == "statement" && record.State == "terminal" && record.ExecutionProven {
			intent = true
		}
	}
	if !intent {
		t.Fatalf("fused write left no settled statement record: %+v", state.Operations)
	}
}

func TestHranaPipelineFusedStatementErrorKeepsTheSession(t *testing.T) {
	fixture := newGatewayFixture(t)
	// A sql_error makes the Hrana adapter re-probe the statement with describe
	// before it answers; the probe fails the same way the statement did, so the
	// original fused failure survives.
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 1}, nil
		case "open_guarded_statement":
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "error", "error": "no such table: missing", "execution_completed": false},
			}, nil
		case "describe":
			return nil, sqlWorkerFailureFromReply(true, false, "no such table: missing")
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"error"`) {
		t.Fatalf("fused statement error: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("a failed fused statement was dispatched again")
	}
	if closes := fixture.upstream.count("close_session"); closes != 1 {
		t.Fatalf("a failed fused statement did not retire its session: %d", closes)
	}
}

func TestHranaFusedOpenUnknownDoesNotReplayOrIsolate(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(func(map[string]any) (any, error) {
		return nil, &SQLRPCError{Ambiguous: true}
	}))
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "outcome_unknown") {
		t.Fatalf("fused unknown: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("an unknown fused open was replayed")
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	opened := 0
	for _, record := range state.Operations {
		if record.Action != "open" {
			continue
		}
		opened++
		if record.State != "terminal" || record.MayHaveCommitted {
			t.Fatalf("fused unknown open was not classified as non-committing: %+v", record)
		}
	}
	if opened != 1 {
		t.Fatalf("open records: %d", opened)
	}
	// The namespace is still usable: an ordinary open does not meet isolation.
	if response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first}); response.Code != http.StatusOK {
		t.Fatalf("fused unknown isolated the namespace: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaFusedOpenFallsBackWhenTheWorkerRefusesTheCommand(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 1}, nil
		case "open_guarded_statement":
			return nil, &SQLRPCError{StatusCode: http.StatusBadRequest, unsupportedCommand: true}
		case "open_session":
			return map[string]any{"session": fusedOpenSession, "readonly": false}, nil
		case "guarded_readonly_statement":
			return guardedBatchReadResult(7), nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	handler := newHranaHTTP(fixture.gateway)
	response := runPipelineAuditRequest(t, handler, fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"execute"`) {
		t.Fatalf("fallback pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("guarded_readonly_statement") != 1 {
		t.Fatalf("fallback counts: fused=%d open=%d statement=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("open_session"), fixture.upstream.count("guarded_readonly_statement"))
	}
	// The refusal downgrades the capability for this worker: the next implicit
	// open goes straight to the plain command instead of paying a failed probe.
	response = runPipelineAuditRequest(t, handler, fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK {
		t.Fatalf("second pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("open_session") != 2 {
		t.Fatalf("negative capability cache did not hold: fused=%d open=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("open_session"))
	}
}

func TestHranaFusedEntryIsNotRepeatedInTheGuardedBatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	requests := []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 1", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 2", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 3", "want_rows": true}},
		map[string]any{"type": "close"},
	}
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, requests)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"execute"`) != 3 {
		t.Fatalf("three executes: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("guarded_readonly_batch") != 1 || fixture.upstream.count("statement") != 0 {
		t.Fatalf("planning counts: fused=%d batch=%d statement=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("guarded_readonly_batch"), fixture.upstream.count("statement"))
	}
	for _, call := range fixture.upstream.calls {
		if call.command["command"] != "guarded_readonly_batch" {
			continue
		}
		raw, _ := call.command["statements"].([]any)
		if len(raw) != 2 {
			t.Fatalf("block did not start after the fused entry: %v", raw)
		}
		for index, want := range []string{"SELECT 2", "SELECT 3"} {
			statement, _ := raw[index].(map[string]any)
			if statement["sql"] != want {
				t.Fatalf("block entry %d: %v", index, statement)
			}
		}
	}
}

func TestHranaProtoSingleExecuteUsesTheFusedOpen(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := append(hranaProtoBytes(2, testHranaV3Statement()), hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("protobuf fused pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("open_session") != 0 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("protobuf fused counts: fused=%d statement=%d open=%d close=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("statement"), fixture.upstream.count("open_session"), fixture.upstream.count("close_session"))
	}
}

func TestHranaFusedOpenSkipsAStatementOverTheProjectSQLBudget(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxSQLBytes = 8 })
	requests := []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 123456789", "want_rows": true}},
		map[string]any{"type": "close"},
	}
	runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, requests)
	if fixture.upstream.count("open_guarded_statement") != 0 || fixture.upstream.count("open_session") != 1 {
		t.Fatal("a statement over the project SQL budget was fused")
	}
	if fixture.upstream.count("statement") != 0 || fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("a statement over the project SQL budget reached the worker")
	}
}

func TestHranaFusedOpenKeepsTheResultBudget(t *testing.T) {
	fixture := newGatewayFixture(t)
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 512 })
	large := guardedBatchReadResult(strings.Repeat("x", 512))
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 1}, nil
		case "open_guarded_statement":
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "ok", "result": large},
			}, nil
		case "describe":
			// The adapter re-probes a result_limit with describe before it
			// decides whether a cursor could serve the read. The probe fails the
			// same way a missing table would, so the budget answer survives.
			return nil, sqlWorkerFailureFromReply(true, false, "no such table: missing")
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "result_limit") {
		t.Fatalf("fused result budget: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("a statement over the result budget was dispatched again")
	}
	if closes := fixture.upstream.count("close_session"); closes != 1 {
		t.Fatalf("result budget did not retire the session: %d", closes)
	}
}

func TestFusedOpenCommandValidation(t *testing.T) {
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	frozen, err := freezeCommand(fusedOpenCommand(database, fusedOpenStatement{
		SQL:       "SELECT ?",
		Args:      []sqlArgument{{Type: "text", Value: json.RawMessage(`"x"`)}},
		NamedArgs: []namedSQLArgument{},
		WantRows:  true,
	}))
	if err != nil || !fusedOpenCommandValid("open_guarded_statement", frozen.bytes()) {
		t.Fatalf("the platform's own fused command was refused: %v", err)
	}
	if fusedOpenCommandValid("open_session", frozen.bytes()) {
		t.Fatal("a fused body was accepted as another command")
	}
	invalid := map[string]string{
		"missing rows":     `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main"},"sql":"SELECT 1","args":[],"named_args":[]}`,
		"empty sql":        `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main"},"sql":"","args":[],"named_args":[],"want_rows":true}`,
		"unknown field":    `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main"},"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true,"extra":1}`,
		"bad database":     `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"","database":"db-a"},"branch":"main"},"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true}`,
		"bad argument":     `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main"},"sql":"SELECT ?","args":[{"type":"integer","value":"NaN"}],"named_args":[],"want_rows":true}`,
		"empty named name": `{"command":"open_guarded_statement","database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main"},"sql":"SELECT :x","args":[],"named_args":[{"name":"","value":{"type":"text","value":"x"}}],"want_rows":true}`,
	}
	for name, body := range invalid {
		if fusedOpenCommandValid("open_guarded_statement", []byte(body)) {
			t.Fatalf("%s: malformed fused command was accepted", name)
		}
	}
}

func TestHranaOpenWithoutAFusionCandidateKeepsThePublicShape(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedOpenHook(nil))
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"describe","sql":"SELECT 1"},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if fixture.upstream.count("open_guarded_statement") != 0 || fixture.upstream.count("open_session") != 1 {
		t.Fatal("a non-execute first entry was fused")
	}
}
