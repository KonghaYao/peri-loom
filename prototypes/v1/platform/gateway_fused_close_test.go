package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// TestFusedCloseAnswerShapes pins the only two payloads that count as a close
// answer and rejects every partial shape: an unreadable close is not evidence,
// so the caller must fall back to the ordinary close entry.
func TestFusedCloseAnswerShapes(t *testing.T) {
	if closed, valid := parseFusedCloseAnswer(json.RawMessage(`{"status":"ok"}`)); !closed || !valid {
		t.Fatalf("ok close: closed=%t valid=%t", closed, valid)
	}
	if closed, valid := parseFusedCloseAnswer(json.RawMessage(`{"status":"error","error":"quarantined"}`)); closed || !valid {
		t.Fatalf("error close: closed=%t valid=%t", closed, valid)
	}
	for _, body := range []string{
		`null`,
		`{}`,
		`{"status":"ok","error":"x"}`,
		`{"status":"ok","extra":1}`,
		`{"status":"error"}`,
		`{"status":"error","error":""}`,
		`{"status":"skipped"}`,
		`{"status":"ok"}{"status":"ok"}`,
	} {
		if closed, valid := parseFusedCloseAnswer(json.RawMessage(body)); closed || valid {
			t.Fatalf("%s: malformed close answer accepted (closed=%t valid=%t)", body, closed, valid)
		}
	}
	completed, incomplete := true, false
	if !fusedCloseEligible(fusedStatementAnswer{kind: fusedStatementOK}) {
		t.Fatal("a successful statement cannot carry a close")
	}
	if fusedCloseEligible(fusedStatementAnswer{kind: fusedStatementError}) ||
		!fusedCloseEligible(fusedStatementAnswer{kind: fusedStatementError, executionCompleted: &incomplete}) ||
		!fusedCloseEligible(fusedStatementAnswer{kind: fusedStatementError, executionCompleted: &completed}) {
		t.Fatal("close eligibility does not follow settled statement evidence")
	}
	if fusedCloseEligible(fusedStatementAnswer{kind: fusedStatementRequiresIntent}) {
		t.Fatal("a refused write is allowed to carry a close")
	}
}

// fusedCloseHook answers a worker that advertises the fused-open close level.
// closeReply decides the close half of the open; nil answers a successful close
// whenever the caller asked for one.
func fusedCloseHook(closeReply func(map[string]any) any) func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			result := map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "ok", "result": guardedBatchReadResult(42)},
			}
			if command["close_after_statement"] == true {
				if closeReply != nil {
					result["close"] = closeReply(command)
				} else {
					result["close"] = map[string]any{"status": "ok"}
				}
			}
			return result, nil
		case "open_session":
			return map[string]any{"session": fusedOpenSession, "readonly": false}, nil
		case "guarded_readonly_statement":
			return guardedBatchReadResult(9), nil
		case "guarded_readonly_batch":
			raw, _ := command["statements"].([]any)
			items := make([]any, len(raw))
			for index := range raw {
				items[index] = map[string]any{"index": index, "status": "ok", "result": guardedBatchReadResult(index + 1)}
			}
			return map[string]any{"items": items, "next": len(raw)}, nil
		case "statement":
			return guardedBatchWriteResult(), nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	}
}

func fusedOpenCommandCalls(fixture *gatewayFixture) []map[string]any {
	fixture.upstream.mu.Lock()
	defer fixture.upstream.mu.Unlock()
	var calls []map[string]any
	for _, call := range fixture.upstream.calls {
		if call.command["command"] == "open_guarded_statement" {
			calls = append(calls, call.command)
		}
	}
	return calls
}

func TestHranaPipelineFusedCloseSkipsTheWorkerClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedCloseHook(nil))
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"ok"`) != 2 {
		t.Fatalf("fused close pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 {
		t.Fatalf("fused opens: %d", fixture.upstream.count("open_guarded_statement"))
	}
	if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 ||
		fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("the fused entry was dispatched a second time")
	}
	// The worker already closed the session: the close entry must not name it
	// again, because the command would answer "unknown worker session".
	if closes := fixture.upstream.count("close_session"); closes != 0 {
		t.Fatalf("a fused close still called close_session: %d", closes)
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if command["close_after_statement"] != true {
			t.Fatalf("fused open did not ask for the close: %v", command)
		}
	}
	// The platform's own facts are unchanged: open, statement and close records
	// all exist even though the worker round trips were fused.
	if len(fixture.audit.outcomesOf("open")) != 1 || len(fixture.audit.outcomesOf("statement")) != 1 || len(fixture.audit.outcomesOf("close")) != 1 {
		t.Fatalf("audit outcomes: open=%v statement=%v close=%v",
			fixture.audit.outcomesOf("open"), fixture.audit.outcomesOf("statement"), fixture.audit.outcomesOf("close"))
	}
	fixture.gateway.mu.Lock()
	registered := len(fixture.gateway.sessions)
	fixture.gateway.mu.Unlock()
	if registered != 0 {
		t.Fatalf("a fused close left %d sessions registered", registered)
	}
	fixture.gateway.deferred.wait()
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	opens, statements, closes := 0, 0, 0
	for _, record := range state.Operations {
		switch record.Action {
		case "open":
			opens++
			if record.CleanupOutcome != "completed" {
				t.Fatalf("the close did not discharge the open: %+v", record.OperationStatus)
			}
		case "statement":
			statements++
		case "close":
			closes++
			if record.State != "terminal" {
				t.Fatalf("close record is not settled: %+v", record.OperationStatus)
			}
		}
	}
	// A proven read-only statement has no sidecar record of its own; its audit
	// row above is the only durable trace. The open and the close both keep one.
	if opens != 1 || statements != 0 || closes != 1 {
		t.Fatalf("durable records: opens=%d statements=%d closes=%d", opens, statements, closes)
	}
}

func TestHranaPipelineFusedCloseFallsBackWhenTheWorkerReportsACloseError(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedCloseHook(func(map[string]any) any {
		return map[string]any{"status": "error", "error": "session cleanup quarantined"}
	}))
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"ok"`) != 2 {
		t.Fatalf("close fallback pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("fused=%d close=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("close_session"))
	}
	// A close the worker could not prove must not be recorded as the session's
	// retirement; the ordinary close entry is the one that finishes it.
	fixture.gateway.mu.Lock()
	registered := len(fixture.gateway.sessions)
	fixture.gateway.mu.Unlock()
	if registered != 0 {
		t.Fatalf("the fallback close left %d sessions registered", registered)
	}
	if len(fixture.audit.outcomesOf("close")) != 1 {
		t.Fatalf("close outcomes: %v", fixture.audit.outcomesOf("close"))
	}
}

func TestHranaPipelineFusedCloseLeavesARefusedWriteSession(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			if command["close_after_statement"] != true {
				return nil, errors.New("the adapter did not ask for the fused close")
			}
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "requires_intent", "reason": "engine_writable"},
			}, nil
		case "statement":
			return guardedBatchWriteResult(), nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	requests := []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records(id) VALUES(1)", "want_rows": false}},
		map[string]any{"type": "close"},
	}
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, requests)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"ok"`) != 2 {
		t.Fatalf("fused write pipeline: %d %s", response.Code, response.Body.String())
	}
	// The worker keeps a session it may still write on and close normally.
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("fused=%d statement=%d close=%d",
			fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("statement"), fixture.upstream.count("close_session"))
	}
}

func TestHranaPipelineFusedCloseRequiresTheSecondCapabilityLevel(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			// An R42 worker: it fuses the statement and nothing else.
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 1}, nil
		case "open_guarded_statement":
			if _, exists := command["close_after_statement"]; exists {
				return nil, errors.New("a level-1 worker was asked to close")
			}
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "ok", "result": guardedBatchReadResult(42)},
			}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"ok"`) != 2 {
		t.Fatalf("level-1 pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("fused=%d close=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("close_session"))
	}
}

func TestHranaPipelineFusedCloseOnlyForTheExecuteCloseShape(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedCloseHook(nil))
	requests := []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 1", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 2", "want_rows": true}},
		map[string]any{"type": "close"},
	}
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, requests)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"type":"execute"`) != 2 {
		t.Fatalf("two-execute pipeline: %d %s", response.Code, response.Body.String())
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if _, exists := command["close_after_statement"]; exists {
			t.Fatalf("a later entry needs the session: %v", command)
		}
	}
	if closes := fixture.upstream.count("close_session"); closes != 1 {
		t.Fatalf("the ordinary close ran %d times", closes)
	}
}

func TestHranaProtoSingleExecuteFusesTheClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(fusedCloseHook(nil))
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := append(hranaProtoBytes(2, testHranaV3Statement()), hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("protobuf fused close: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("close_session") != 0 {
		t.Fatalf("fused=%d close=%d", fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("close_session"))
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if command["close_after_statement"] != true {
			t.Fatalf("protobuf open did not ask for the close: %v", command)
		}
	}
}

func TestHranaFusedCloseDoesNotReProbeTheRetiredSession(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			return map[string]any{
				"session":   fusedOpenSession,
				"readonly":  false,
				"statement": map[string]any{"status": "error", "error": "no such table: missing", "execution_completed": false},
				"close":     map[string]any{"status": "ok"},
			}, nil
		case "describe":
			return nil, errors.New("the adapter re-probed a retired session")
		case "close_session":
			return nil, errors.New("the adapter closed a session the worker already retired")
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"error"`) ||
		!strings.Contains(response.Body.String(), `"code":"sql_error"`) {
		t.Fatalf("fused statement error: %d %s", response.Code, response.Body.String())
	}
	// A settled statement error is answer data: the describe fallback would
	// report an unknown session instead of the error the statement produced.
	if fixture.upstream.count("describe") != 0 || fixture.upstream.count("statement") != 0 || fixture.upstream.count("close_session") != 0 {
		t.Fatalf("describe=%d statement=%d close=%d",
			fixture.upstream.count("describe"), fixture.upstream.count("statement"), fixture.upstream.count("close_session"))
	}
}

func TestHranaFusedCloseFallsBackWhenTheWorkerRefusesTheCommand(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			// A stale positive cache in front of an older binary: the whole body
			// is refused, so neither the statement nor the close ran.
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
		t.Fatalf("stale capability fallback: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("open_session") != 1 ||
		fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("fused=%d open=%d statement=%d close=%d",
			fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("open_session"),
			fixture.upstream.count("guarded_readonly_statement"), fixture.upstream.count("close_session"))
	}
	// The refusal downgrades the capability: the next open is plain and pays no
	// second refused fused request.
	response = runPipelineAuditRequest(t, handler, fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK {
		t.Fatalf("second pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("open_session") != 2 {
		t.Fatalf("negative capability cache did not hold: fused=%d open=%d",
			fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("open_session"))
	}
}
