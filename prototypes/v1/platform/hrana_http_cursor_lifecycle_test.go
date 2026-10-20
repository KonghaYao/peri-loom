package platform

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	controller "peri-loom/controller"
)

func hranaHTTPCursorStatement(sql string) []byte {
	statement := append(hranaProtoString(1, sql), hranaProtoNumber(5, 1)...)
	return hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
}

func hranaHTTPCursorBaton(test *testing.T, body []byte) string {
	test.Helper()
	length, read := binary.Uvarint(body)
	if read < 1 || uint64(len(body)-read) < length {
		test.Fatal("cursor response has no baton frame")
	}
	fields, err := hranaProtoRead(body[read : read+int(length)])
	if err != nil || len(fields) != 1 || fields[0].number != 1 {
		test.Fatalf("invalid baton frame: %v %v", fields, err)
	}
	return string(fields[0].bytes)
}

func hranaHTTPCursorCall(handler http.Handler, token string, ctx context.Context, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func latestCursorOperation(test *testing.T, fixture *gatewayFixture, action string) OperationRecord {
	test.Helper()
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var selected OperationRecord
	found := false
	for _, record := range state.Operations {
		if record.Database == fixture.first && record.Action == action && (!found || record.RegistrationSequence > selected.RegistrationSequence) {
			selected, found = record, true
		}
	}
	if !found {
		test.Fatalf("no %s operation record", action)
	}
	return selected
}

func assertCursorNotIsolated(test *testing.T, fixture *gatewayFixture) {
	test.Helper()
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if record, isolated := isolatedOperation(state, fixture.first, true); isolated {
		test.Fatalf("proven cursor outcome still isolated: action=%s state=%s outcome=%s", record.Action, record.State, record.CursorNativeOutcome)
	}
}

func assertCursorIsolated(test *testing.T, fixture *gatewayFixture) OperationStatus {
	test.Helper()
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	record, isolated := isolatedOperation(state, fixture.first, true)
	if !isolated || !record.MayHaveCommitted || record.CursorNativeOutcome != cursorOutcomeUnknown {
		test.Fatalf("unproven cursor did not stay isolated: isolated=%t outcome=%s possible_commit=%t", isolated, record.CursorNativeOutcome, record.MayHaveCommitted)
	}
	return record
}

func cursorWriteHook(fixture *gatewayFixture, fetch func() (any, error), close func() (any, error)) {
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": false}, nil
		case "open_write_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_write_cursor":
			return fetch()
		case "close_write_cursor":
			return close()
		}
		return nil, nil
	})
}

func TestHranaHTTPWriteCursorPrepareDisconnectClosesWithoutWrite(t *testing.T) {
	fixture := newGatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			cancel()
			return nil, &SQLRPCError{Ambiguous: true, cause: context.Canceled}
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	response := hranaHTTPCursorCall(handler, fixture.tokenA, ctx, hranaHTTPCursorStatement("INSERT INTO records VALUES(1) RETURNING id"))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("prepare disconnect answered %d: %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_write_cursor") != 0 || fixture.upstream.count("fetch_write_cursor") != 0 || fixture.upstream.count("close_write_cursor") != 0 {
		t.Fatalf("prepare disconnect opened a write cursor: open=%d fetch=%d close=%d", fixture.upstream.count("open_write_cursor"), fixture.upstream.count("fetch_write_cursor"), fixture.upstream.count("close_write_cursor"))
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatalf("aborted session was not retired: %d", fixture.upstream.count("close_session"))
	}
	assertCursorNotIsolated(t, fixture)
	if len(handler.streams) != 0 {
		t.Fatal("aborted prepare left a reusable baton")
	}
	if outcomes := fixture.audit.outcomesOf("describe"); len(outcomes) != 1 || outcomes[0] != "outcome_unknown" {
		t.Fatalf("prepare disconnect audit missing: %v", outcomes)
	}
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	if probe := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"}); probe.Code != http.StatusOK {
		t.Fatalf("prepare disconnect left write access unusable: %d %s", probe.Code, probe.Body.String())
	}
}

func TestHranaHTTPWriteCursorAbortBeforeFirstRowProvesRollback(t *testing.T) {
	fixture := newGatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetches := 0
	cursorWriteHook(fixture, func() (any, error) {
		fetches++
		cancel()
		return map[string]any{"rows": []any{}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
	}, func() (any, error) {
		return map[string]any{"closed": true, "abort_proven": true}, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	response := hranaHTTPCursorCall(handler, fixture.tokenA, ctx, hranaHTTPCursorStatement("INSERT INTO records VALUES(1) RETURNING id"))
	if fetches != 1 || fixture.upstream.count("close_write_cursor") != 1 {
		t.Fatalf("abort before first row was not cleaned once: fetch=%d close=%d", fetches, fixture.upstream.count("close_write_cursor"))
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatalf("aborted session was not retired: %d", fixture.upstream.count("close_session"))
	}
	assertCursorNotIsolated(t, fixture)
	record := latestCursorOperation(t, fixture, "cursor_close")
	if record.State != "terminal" || record.CursorNativeOutcome != cursorOutcomeRolledBack || !record.CursorClientStopped || !record.CursorDispatchStopped || record.MayHaveCommitted || record.BusinessOutcome != "not_executed" {
		t.Fatalf("abort proof not recorded: state=%s native=%s client=%t dispatch=%t possible_commit=%t business=%s", record.State, record.CursorNativeOutcome, record.CursorClientStopped, record.CursorDispatchStopped, record.MayHaveCommitted, record.BusinessOutcome)
	}
	if outcomes := fixture.audit.outcomesOf("cursor_close"); len(outcomes) != 1 || outcomes[0] != "ok" {
		t.Fatalf("cursor close audit missing: %v", outcomes)
	}
	baton := hranaHTTPCursorBaton(t, response.Body.Bytes())
	reuse := hranaHTTPCursorCall(handler, fixture.tokenA, context.Background(), append(hranaProtoString(1, baton), hranaHTTPCursorStatement("INSERT INTO records VALUES(2) RETURNING id")...))
	if reuse.Code != http.StatusForbidden || fetches != 1 || fixture.upstream.count("open_session") != 1 {
		t.Fatalf("aborted baton was reused: status=%d fetch=%d sessions=%d", reuse.Code, fetches, fixture.upstream.count("open_session"))
	}
}

func TestHranaHTTPWriteCursorPartialRowsDisconnectProvesRollback(t *testing.T) {
	fixture := newGatewayFixture(t)
	fetches := 0
	cursorWriteHook(fixture, func() (any, error) {
		fetches++
		return map[string]any{"rows": [][]any{{1}, {2}, {3}}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
	}, func() (any, error) {
		return map[string]any{"closed": true, "abort_proven": true}, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	// Let the baton, step_begin and the first two rows reach the client, then
	// disconnect on the third row so the abort happens after a partial response.
	writer := &failingCursorWriter{ResponseRecorder: httptest.NewRecorder(), maxWrites: 4}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(hranaHTTPCursorStatement("INSERT INTO records VALUES(1) RETURNING id")))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	if !serveHranaV3(writer, request, handler) {
		t.Fatal("partial cursor disconnect was not routed")
	}
	if fetches != 1 || fixture.upstream.count("close_write_cursor") != 1 {
		t.Fatalf("partial disconnect dispatched again: fetch=%d close=%d", fetches, fixture.upstream.count("close_write_cursor"))
	}
	assertCursorNotIsolated(t, fixture)
	record := latestCursorOperation(t, fixture, "cursor_close")
	if record.CursorNativeOutcome != cursorOutcomeRolledBack || !record.CursorClientStopped || !record.CursorDispatchStopped {
		t.Fatalf("partial disconnect did not record rollback: native=%s client=%t dispatch=%t", record.CursorNativeOutcome, record.CursorClientStopped, record.CursorDispatchStopped)
	}
}

func TestHranaHTTPWriteCursorDisconnectAfterDonePageProvesCommit(t *testing.T) {
	fixture := newGatewayFixture(t)
	fetches := 0
	cursorWriteHook(fixture, func() (any, error) {
		fetches++
		return map[string]any{"rows": [][]any{{1}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}, nil
	}, func() (any, error) {
		// The worker has no cursor left after the done page; this is not an abort proof.
		return map[string]any{"closed": true, "abort_proven": false}, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	writer := &failingCursorWriter{ResponseRecorder: httptest.NewRecorder(), maxWrites: 2}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(hranaHTTPCursorStatement("INSERT INTO records VALUES(1) RETURNING id")))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	if !serveHranaV3(writer, request, handler) {
		t.Fatal("post-commit disconnect was not routed")
	}
	if fetches != 1 || fixture.upstream.count("close_write_cursor") != 1 {
		t.Fatalf("post-commit disconnect dispatched again: fetch=%d close=%d", fetches, fixture.upstream.count("close_write_cursor"))
	}
	assertCursorNotIsolated(t, fixture)
	fetchRecord := latestCursorOperation(t, fixture, "cursor_fetch")
	if fetchRecord.CursorNativeOutcome != cursorOutcomeCommitted || !fetchRecord.ExecutionProven || fetchRecord.BusinessOutcome != "completed" {
		t.Fatalf("delivered done page was not a commit proof: native=%s proven=%t business=%s", fetchRecord.CursorNativeOutcome, fetchRecord.ExecutionProven, fetchRecord.BusinessOutcome)
	}
	closeRecord := latestCursorOperation(t, fixture, "cursor_close")
	if closeRecord.CursorNativeOutcome != cursorOutcomeCommitted || closeRecord.State != "terminal" || closeRecord.MayHaveCommitted {
		t.Fatalf("close overwrote the commit proof: native=%s state=%s possible_commit=%t", closeRecord.CursorNativeOutcome, closeRecord.State, closeRecord.MayHaveCommitted)
	}
	if outcomes := fixture.audit.outcomesOf("cursor_close"); len(outcomes) != 1 || outcomes[0] != "ok" {
		t.Fatalf("post-commit close audit missing: %v", outcomes)
	}
}

func TestHranaHTTPWriteCursorCloseWithoutAbortProofStaysIsolated(t *testing.T) {
	fixture := newGatewayFixture(t)
	fetches := 0
	cursorWriteHook(fixture, func() (any, error) {
		fetches++
		return map[string]any{"rows": [][]any{{1}}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
	}, func() (any, error) {
		// A successful close without the native abort proof may hide a lost done
		// page, so the platform must not read it as a rollback.
		return map[string]any{"closed": true}, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	writer := &failingCursorWriter{ResponseRecorder: httptest.NewRecorder(), maxWrites: 2}
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v3-protobuf/cursor", bytes.NewReader(hranaHTTPCursorStatement("INSERT INTO records VALUES(1) RETURNING id")))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/x-protobuf")
	if !serveHranaV3(writer, request, handler) {
		t.Fatal("unproven cursor close was not routed")
	}
	if fetches != 1 {
		t.Fatalf("unproven close retried the fetch: %d", fetches)
	}
	isolated := assertCursorIsolated(t, fixture)
	closeRecord := latestCursorOperation(t, fixture, "cursor_close")
	if closeRecord.State != "quarantined" || closeRecord.CursorNativeOutcome != cursorOutcomeUnknown || !closeRecord.MayHaveCommitted || !closeRecord.CursorClientStopped || !closeRecord.CursorDispatchStopped {
		t.Fatalf("unproven close did not stay unknown: state=%s native=%s possible_commit=%t client=%t dispatch=%t", closeRecord.State, closeRecord.CursorNativeOutcome, closeRecord.MayHaveCommitted, closeRecord.CursorClientStopped, closeRecord.CursorDispatchStopped)
	}
	if closeRecord.OperationID != isolated.OperationID {
		t.Fatalf("isolation names a different operation: %s != %s", closeRecord.OperationID, isolated.OperationID)
	}
	if outcomes := fixture.audit.outcomesOf("cursor_close"); len(outcomes) != 1 || outcomes[0] != "outcome_unknown" {
		t.Fatalf("unproven close audit missing: %v", outcomes)
	}
	probe := hranaV3JSONCall(handler, fixture.tokenA, hranaV3JSONPath+"/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}`)
	document := hranaV3JSONDocument(t, probe)
	if probe.Code != http.StatusConflict || document["error"].(map[string]any)["code"] != "database_isolated" {
		t.Fatalf("unproven write was not isolated: %d %s", probe.Code, probe.Body.String())
	}
	evidence := document["error"].(map[string]any)
	if evidence["operation_id"] != closeRecord.OperationID || evidence["possible_commit"] != true || evidence["retry_allowed"] != false {
		t.Fatalf("isolation evidence is incomplete: %v", evidence)
	}
}

func TestHranaV3JSONWriteCursorAbortProvesRollback(t *testing.T) {
	fixture := newGatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetches := 0
	cursorWriteHook(fixture, func() (any, error) {
		fetches++
		cancel()
		return map[string]any{"rows": []any{}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
	}, func() (any, error) {
		return map[string]any{"closed": true, "abort_proven": true}, nil
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"INSERT INTO records VALUES(1) RETURNING id","args":[],"named_args":[],"want_rows":true}}]}}`
	request := httptest.NewRequest(http.MethodPost, hranaV3JSONPath+"/cursor", bytes.NewBufferString(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if fetches != 1 || fixture.upstream.count("close_write_cursor") != 1 {
		t.Fatalf("JSON abort was not cleaned once: fetch=%d close=%d", fetches, fixture.upstream.count("close_write_cursor"))
	}
	assertCursorNotIsolated(t, fixture)
	record := latestCursorOperation(t, fixture, "cursor_close")
	if record.CursorNativeOutcome != cursorOutcomeRolledBack || !record.CursorClientStopped || !record.CursorDispatchStopped || record.State != "terminal" {
		t.Fatalf("JSON abort proof not recorded: native=%s client=%t dispatch=%t state=%s", record.CursorNativeOutcome, record.CursorClientStopped, record.CursorDispatchStopped, record.State)
	}
	if len(handler.streams) != 0 {
		t.Fatal("JSON abort left a reusable baton")
	}
}

func TestHranaHTTPCursorLifecycleFieldsRejectUnboundStopReport(t *testing.T) {
	encoded, _ := json.Marshal(map[string]any{"session": "00000000000000000000000000000000", "write_cursor": false, "cursor_client_stopped": true, "cursor_dispatch_stopped": true})
	_, valid := decodeGatewayRequest(context.Background(), encoded, "cursor_close")
	if valid {
		t.Fatal("client stop report was accepted for a read cursor")
	}
}
