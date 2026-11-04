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
//
// The gateway keeps this machinery, but the Hrana adapters no longer ask the
// open to retire the session: a statement that overruns its result budget may
// still need the session for the cursor fallback. These shapes stay pinned so a
// future request cannot silently accept a partial close as proof.
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

// TestHranaPipelineExecuteCloseKeepsTheWorkerClose pins the contract that
// replaced the fused close: even when the worker advertises the fused-close
// level, the implicit open fuses only the statement and the close entry still
// reaches the worker. The platform's own open/statement/close records are
// unchanged, so the lifecycle fusion and its audit trail stay separate.
func TestHranaPipelineExecuteCloseKeepsTheWorkerClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			// Level 2 is what a fused-close capable worker advertises. The adapter
			// must not ask for the close even though the worker would accept it.
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			if _, exists := command["close_after_statement"]; exists {
				return nil, errors.New("the adapter fused the close into the open")
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
		t.Fatalf("execute+close pipeline: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 {
		t.Fatalf("fused opens: %d", fixture.upstream.count("open_guarded_statement"))
	}
	if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("statement") != 0 ||
		fixture.upstream.count("guarded_readonly_statement") != 0 {
		t.Fatal("the fused entry was dispatched a second time")
	}
	// The close entry must reach the worker: only a fused close would have
	// retired the session inside the open.
	if closes := fixture.upstream.count("close_session"); closes != 1 {
		t.Fatalf("the close entry did not reach the worker: %d", closes)
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if _, exists := command["close_after_statement"]; exists {
			t.Fatalf("a fused open asked the worker to retire the session: %v", command)
		}
	}
	// The platform's own facts are unchanged: open, statement and close records
	// all exist even though the open and the statement were fused.
	if len(fixture.audit.outcomesOf("open")) != 1 || len(fixture.audit.outcomesOf("statement")) != 1 || len(fixture.audit.outcomesOf("close")) != 1 {
		t.Fatalf("audit outcomes: open=%v statement=%v close=%v",
			fixture.audit.outcomesOf("open"), fixture.audit.outcomesOf("statement"), fixture.audit.outcomesOf("close"))
	}
	fixture.gateway.mu.Lock()
	registered := len(fixture.gateway.sessions)
	fixture.gateway.mu.Unlock()
	if registered != 0 {
		t.Fatalf("the close left %d sessions registered", registered)
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

// fusedBudgetWorker models a worker that advertises the fused-close level and
// answers an implicit open with a statement result over the tenant budget. Its
// describe and cursor pages answer the read-only fallback. The hook refuses an
// open that asks the worker to retire the session: that fusion is what removed
// the session the fallback needs.
type fusedBudgetWorker struct {
	large   map[string]any
	fetched int
}

func (worker *fusedBudgetWorker) hook(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
	switch command["command"] {
	case "capabilities":
		return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
	case "open_guarded_statement":
		if _, exists := command["close_after_statement"]; exists {
			return nil, errors.New("the adapter retired the session before the cursor fallback")
		}
		return map[string]any{
			"session":   fusedOpenSession,
			"readonly":  false,
			"statement": map[string]any{"status": "ok", "result": worker.large},
		}, nil
	case "describe":
		return map[string]any{
			"params":      []any{},
			"cols":        []any{map[string]any{"name": "body", "decltype": "TEXT"}},
			"is_explain":  false,
			"is_readonly": true,
		}, nil
	case "open_read_cursor":
		return map[string]any{"cols": []any{map[string]any{"name": "body", "decltype": "TEXT"}}}, nil
	case "fetch_read_cursor":
		worker.fetched++
		return map[string]any{"rows": [][]any{{"cursor-row"}}, "done": true}, nil
	case "close_session":
		return map[string]bool{"closed": true}, nil
	}
	return nil, errors.New("unknown fixture action")
}

// TestHranaPipelineExecuteCloseKeepsTheSessionForTheCursorFallback covers the
// R43 regression that made the official client fail a 12,000-row HTTP execute:
// the fused open ran the statement, the fused close retired the session, and a
// statement over the result budget was then published as result_limit because
// the describe/cursor fallback had no session left to address.
//
// The fixture needs one over-budget fused answer and one successful describe,
// so the body stays small; it does not reproduce the row count. The assertion
// is on the path, not on the volume: the fallback must run and the close entry
// must still reach the worker.
func TestHranaPipelineExecuteCloseKeepsTheSessionForTheCursorFallback(t *testing.T) {
	fixture := newGatewayFixture(t)
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 512 })
	worker := &fusedBudgetWorker{large: guardedBatchReadResult(strings.Repeat("x", 512))}
	fixture.upstream.setHook(worker.hook)
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"rows":[["cursor-row"]]`) {
		t.Fatalf("the fused result budget did not use the cursor fallback: %d %s", response.Code, body)
	}
	if strings.Contains(body, `"code":"result_limit"`) {
		t.Fatalf("the pipeline published result_limit instead of the cursor rows: %s", body)
	}
	if fixture.upstream.count("describe") != 1 || fixture.upstream.count("open_read_cursor") != 1 || worker.fetched != 1 {
		t.Fatalf("fallback calls: describe=%d open=%d fetches=%d",
			fixture.upstream.count("describe"), fixture.upstream.count("open_read_cursor"), worker.fetched)
	}
	// The fallback needs the session the fused close would have retired, and the
	// close entry that follows the statement still retires it normally.
	if fixture.upstream.count("close_session") != 1 {
		t.Fatalf("the close entry did not reach the worker: %d", fixture.upstream.count("close_session"))
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if _, exists := command["close_after_statement"]; exists {
			t.Fatalf("the open retired the session before the fallback: %v", command)
		}
	}
}

// TestHranaProtoSingleExecuteKeepsTheSessionForTheCursorFallback applies the
// same path to the protobuf single-execute fast path used by the HTTP clients.
func TestHranaProtoSingleExecuteKeepsTheSessionForTheCursorFallback(t *testing.T) {
	fixture := newGatewayFixture(t)
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 512 })
	worker := &fusedBudgetWorker{large: guardedBatchReadResult(strings.Repeat("x", 512))}
	fixture.upstream.setHook(worker.hook)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := append(hranaProtoBytes(2, testHranaV3Statement()), hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("protobuf fused result budget did not use the cursor fallback: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("describe") != 1 || fixture.upstream.count("open_read_cursor") != 1 || worker.fetched != 1 {
		t.Fatalf("fallback calls: describe=%d open=%d fetches=%d",
			fixture.upstream.count("describe"), fixture.upstream.count("open_read_cursor"), worker.fetched)
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatalf("the close entry did not reach the worker: %d", fixture.upstream.count("close_session"))
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if _, exists := command["close_after_statement"]; exists {
			t.Fatalf("the protobuf open retired the session before the fallback: %v", command)
		}
	}
}

// TestHranaProtoSingleExecuteKeepsTheWorkerClose applies the same contract to
// the protobuf fast path: it still fuses the implicit open with the first
// statement, but the close is a separate worker call.
func TestHranaProtoSingleExecuteKeepsTheWorkerClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
		case "open_guarded_statement":
			if _, exists := command["close_after_statement"]; exists {
				return nil, errors.New("the protobuf adapter fused the close into the open")
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
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := append(hranaProtoBytes(2, testHranaV3Statement()), hranaProtoBytes(2, hranaProtoBytes(1, nil))...)
	response := testHranaV3Call(t, handler, fixture.tokenA, "/db/tenant-a/db-a/main/v3-protobuf/pipeline", request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("protobuf execute+close: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_guarded_statement") != 1 || fixture.upstream.count("close_session") != 1 {
		t.Fatalf("fused=%d close=%d",
			fixture.upstream.count("open_guarded_statement"), fixture.upstream.count("close_session"))
	}
	for _, command := range fusedOpenCommandCalls(fixture) {
		if _, exists := command["close_after_statement"]; exists {
			t.Fatalf("protobuf open asked the worker to retire the session: %v", command)
		}
	}
}
