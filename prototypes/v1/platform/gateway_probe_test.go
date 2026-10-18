package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// A platform probe answers a protocol question with a dispatch that cannot
// commit, so it leaves no operation record and no audit row; the client cursor
// request that needed the answer still records and audits its own steps.
func TestHranaReadonlyProbeLeavesNoRecordOrAuditRow(test *testing.T) {
	store, gateway, upstream, audit, credential := securityGatewayFixture(test)
	handler := newHranaHTTP(gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "SELECT id FROM records"), hranaProtoNumber(5, 1)...)
	requestBody := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	path := "/db/tenant_a/database/main/v3-protobuf/cursor"
	before := sidecarRevision(store)
	response := testHranaV3Call(test, handler, credential, path, requestBody)
	if response.Code != 200 {
		test.Fatalf("cursor request failed: %d %s", response.Code, response.Body.String())
	}
	if upstream.count("describe") != 1 {
		test.Fatal("readonly probe did not reach the worker")
	}
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Action == "describe" {
			test.Fatalf("readonly probe left %+v", record.OperationStatus)
		}
	}
	for _, action := range audit.actions() {
		if action == "describe" {
			test.Fatal("readonly probe wrote an audit row")
		}
	}
	// The client action pays only for its session open: that open is a committing
	// operation with an intent and a settlement. The cursor open and fetch are
	// proven non-committing, so on a known outcome they write no sidecar record.
	if grew := sidecarRevision(store) - before; grew != 2 {
		test.Fatalf("client cursor request cost %d operation sidecar revisions, want 2", grew)
	}
	var opened struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	response = gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase()})
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &opened) != nil || !publicSessionID(opened.Result.Session) {
		test.Fatalf("open failed: %d %s", response.Code, response.Body.String())
	}
	describeBefore, auditBefore := sidecarRevision(store), len(audit.actions())
	response = gatewayHTTP(test, gateway, credential, "/v1/sql/describe", map[string]any{"session": opened.Result.Session, "sql": "SELECT id FROM records"})
	if response.Code != 200 {
		test.Fatalf("direct describe failed: %d %s", response.Code, response.Body.String())
	}
	if grew := sidecarRevision(store) - describeBefore; grew != 0 {
		test.Fatalf("direct describe cost %d operation sidecar revisions, want 0", grew)
	}
	if actions := audit.actions(); len(actions) != auditBefore+1 || actions[len(actions)-1] != "describe" {
		test.Fatalf("direct describe audited %v", actions[auditBefore:])
	}
	var described struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(response.Body.Bytes(), &described) != nil || len(described.Result) == 0 {
		test.Fatalf("direct describe returned no description: %s", response.Body.String())
	}
}

// A probe that fails becomes the client's error, so it keeps its audit row and
// still leaves no operation record.
func TestHranaFailedProbeKeepsAuditRow(test *testing.T) {
	store, gateway, upstream, audit, credential := securityGatewayFixture(test)
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			return nil, errors.New("worker unavailable")
		}
		return nil, nil
	})
	handler := newHranaHTTP(gateway).(*hranaHTTP)
	statement := append(hranaProtoString(1, "SELECT id FROM records"), hranaProtoNumber(5, 1)...)
	requestBody := hranaProtoBytes(2, hranaProtoBytes(1, hranaProtoBytes(2, statement)))
	before := sidecarRevision(store)
	testHranaV3Call(test, handler, credential, "/db/tenant_a/database/main/v3-protobuf/cursor", requestBody)
	if upstream.count("describe") != 1 {
		test.Fatal("failing probe did not reach the worker")
	}
	actions := audit.actions()
	audited := false
	for _, action := range actions {
		if action == "describe" {
			audited = true
		}
	}
	if !audited {
		test.Fatalf("failed probe left no audit row: %v", actions)
	}
	// The refused request retires its session on its own goroutine, so wait for
	// that retirement to leave the registry before the accounting below.
	retired := time.Now().Add(time.Second)
	for {
		gateway.mu.Lock()
		registered := len(gateway.sessions)
		gateway.mu.Unlock()
		if registered == 0 || time.Now().After(retired) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Action == "describe" {
			test.Fatalf("failed probe left %+v", record.OperationStatus)
		}
		if record.Action == "cursor_open" || record.Action == "cursor_fetch" {
			test.Fatalf("refused cursor dispatched: %+v", record.OperationStatus)
		}
	}
	// The refused request pays for the session it opened and for the retirement
	// of that session, which the platform still owes: a failed probe makes the
	// session's outcome unknown, and the close_session below is the only path
	// that returns the session and its quota to an accounted end. The cursor
	// itself never dispatched, so no cursor operation is recorded above.
	if grew := sidecarRevision(store) - before; grew != 4 {
		test.Fatalf("refused cursor request cost %d operation sidecar revisions, want 4", grew)
	}
}

// The autocommit probe answers a protocol condition with the session's own
// connection state, so it runs no SQL and leaves no operation record and no
// audit row. The client request that asked still records and audits its own
// steps.
func TestHranaAutocommitProbeCostsNoSQLOrRecord(test *testing.T) {
	store, gateway, upstream, audit, credential := securityGatewayFixture(test)
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "session_autocommit":
			return map[string]any{"autocommit": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	before := sidecarRevision(store)
	request := httptest.NewRequest(http.MethodPost, "/db/tenant_a/database/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"batch","batch":{"steps":[{"condition":{"type":"is_autocommit"},"stmt":{"sql":"SELECT must_not_run"}}]}},{"type":"close"}]}`))
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(gateway).ServeHTTP(response, request)
	if response.Code != 200 || upstream.count("session_autocommit") != 1 || upstream.count("statement") != 0 {
		test.Fatalf("autocommit probe did not replace the SQL probe: %d %s", response.Code, response.Body.String())
	}
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Action == "session_state" {
			test.Fatalf("autocommit probe left %+v", record.OperationStatus)
		}
	}
	for _, action := range audit.actions() {
		if action == "session_state" {
			test.Fatal("autocommit probe wrote an audit row")
		}
	}
	// Only the client's own session open and close remain: each is a
	// registration plus a settlement, and the probe and the skipped step add
	// nothing.
	if grew := sidecarRevision(store) - before; grew != 4 {
		test.Fatalf("autocommit probe cost %d operation sidecar revisions, want 4", grew)
	}
}
