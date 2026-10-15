package platform

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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
	// The client action still pays for its session open, cursor open and fetch:
	// the session open is a committing operation, the cursor steps are not.
	if grew := sidecarRevision(store) - before; grew != 4 {
		test.Fatalf("client cursor request cost %d operation sidecar revisions, want 4", grew)
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
	if grew := sidecarRevision(store) - describeBefore; grew != 1 {
		test.Fatalf("direct describe cost %d operation sidecar revisions, want 1", grew)
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
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Action == "describe" {
			test.Fatalf("failed probe left %+v", record.OperationStatus)
		}
	}
	// Only the session open remains: the refused cursor never dispatched.
	if grew := sidecarRevision(store) - before; grew != 2 {
		test.Fatalf("refused cursor request cost %d operation sidecar revisions, want 2", grew)
	}
}
