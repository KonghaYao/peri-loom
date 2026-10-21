package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type forkTestState struct {
	mu        sync.Mutex
	state     State
	failStage string
}

func (store *forkTestState) Snapshot() (State, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return cloneState(store.state)
}

func (store *forkTestState) Update(update func(*State) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	candidate, err := cloneState(store.state)
	if err != nil {
		return err
	}
	if err := update(&candidate); err != nil {
		return err
	}
	if store.failStage != "" {
		for _, record := range candidate.Provisionings {
			if record.Stage == store.failStage {
				return errors.New("injected state update failure")
			}
		}
	}
	store.state = candidate
	return nil
}

func (*forkTestState) Close() error { return nil }

func (store *forkTestState) setFailStage(stage string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failStage = stage
}

// forkTestTransport applies maintenance commands and delegates every control
// command to the restart fixture, which already models worker acquisition.
type forkTestTransport struct {
	*restartTransport
	mu                 sync.Mutex
	maintenance        int
	reject             bool
	ambiguous          bool
	ambiguousMutations bool
	applyOnAmbiguous   bool
	missingSnapshot    string
	receipts           map[string]string
	branches           map[string]bool
}

func newForkTestTransport(metadata controller.Metadata) *forkTestTransport {
	return &forkTestTransport{restartTransport: newRestartTransport(metadata), receipts: map[string]string{}, branches: map[string]bool{}}
}

func (transport *forkTestTransport) Authority(ctx context.Context, identifier string, input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var action struct {
		Command   string          `json:"command"`
		RequestID string          `json:"request_id"`
		Operation json.RawMessage `json:"operation"`
	}
	if err := json.Unmarshal(encoded, &action); err != nil {
		return err
	}
	if action.Command != "maintenance" {
		return transport.restartTransport.Authority(ctx, identifier, input, output)
	}
	transport.mu.Lock()
	transport.maintenance++
	reject, ambiguous := transport.reject, transport.ambiguous
	ambiguousMutations, applyOnAmbiguous := transport.ambiguousMutations, transport.applyOnAmbiguous
	missingSnapshot := transport.missingSnapshot
	payload := string(action.Operation)
	prior, replayed := transport.receipts[action.RequestID]
	transport.mu.Unlock()
	if replayed && prior != payload {
		return &controller.RPCError{Message: "request id reused with another command", Rejected: true}
	}
	var operation struct {
		Operation string `json:"operation"`
		Snapshot  string `json:"snapshot"`
		Name      string `json:"name"`
	}
	if err := json.Unmarshal(action.Operation, &operation); err != nil {
		return err
	}
	if operation.Operation == "refs" {
		transport.mu.Lock()
		if transport.ambiguous {
			transport.mu.Unlock()
			return &controller.RPCError{Message: "authority acknowledgement lost", Ambiguous: true}
		}
		branches := make(map[string]any, len(transport.branches))
		for name := range transport.branches {
			branches[name] = map[string]any{}
		}
		transport.mu.Unlock()
		return decodeTestJSON(map[string]any{"revision": 1, "branches": branches, "snapshots": map[string]any{}}, output)
	}
	if missingSnapshot != "" && operation.Snapshot == missingSnapshot {
		return &controller.RPCError{Message: "snapshot missing", Rejected: true}
	}
	if reject {
		return &controller.RPCError{Message: "authority rejected the operation", Rejected: true}
	}
	if ambiguous {
		return &controller.RPCError{Message: "authority acknowledgement lost", Ambiguous: true}
	}
	if ambiguousMutations {
		// A domain error before the receipt is normally indistinguishable from
		// a lost acknowledgement on the wire; applyOnAmbiguous models the other
		// case where the mutation committed but only the reply was lost.
		if applyOnAmbiguous {
			transport.mu.Lock()
			transport.receipts[action.RequestID] = payload
			if operation.Name != "" {
				transport.branches[operation.Name] = true
			}
			transport.mu.Unlock()
		}
		return &controller.RPCError{Message: "authority acknowledgement lost", Ambiguous: true}
	}
	transport.mu.Lock()
	transport.receipts[action.RequestID] = payload
	if operation.Name != "" {
		transport.branches[operation.Name] = true
	}
	transport.mu.Unlock()
	return decodeTestJSON(map[string]any{"request_id": action.RequestID}, output)
}

func (transport *forkTestTransport) maintenanceCalls() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.maintenance
}

func (transport *forkTestTransport) setFailure(reject, ambiguous bool) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.reject, transport.ambiguous = reject, ambiguous
}

func (transport *forkTestTransport) setMissingSnapshot(name string) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.missingSnapshot = name
}

func (transport *forkTestTransport) setAmbiguousMutations(ambiguous, apply bool) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.ambiguousMutations, transport.applyOnAmbiguous = ambiguous, apply
}

type forkFixture struct {
	test       *testing.T
	state      *forkTestState
	placements *gatewayTestPlacements
	transport  *forkTestTransport
	management *Management
	parent     controller.DatabaseID
	token      string
}

func newForkFixture(test *testing.T) *forkFixture {
	test.Helper()
	parent := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	state := &forkTestState{state: State{
		Version:       1,
		Organizations: map[string]Organization{"org": {ID: "org", Name: "tests"}},
		Projects: map[string]Project{"project-a": {
			ID: "project-a", Organization: "org", Tenant: "tenant-a",
			Quotas: Quotas{MaxDatabases: 2, MaxSessions: 8, MaxConcurrent: 2, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096},
		}},
		Databases: map[string]Database{parent.Key(): {ID: parent, Project: "project-a", State: "active", RootIncarnation: "root-a", BranchIncarnation: 1}},
		APIKeys:   map[string]APIKey{},
		Keys:      map[string]KeyDescriptor{},
	}}
	metadata := controller.Metadata{Version: 1, Workers: map[string]controller.Worker{}, Placements: map[string]controller.Placement{}}
	worker := controller.Worker{ID: "worker-a", Address: "http://127.0.0.1:1234", Healthy: true, Capacity: 8, Load: 0, LastHeartbeatMS: time.Now().UnixMilli()}
	metadata.Workers[worker.ID] = worker
	placements := &gatewayTestPlacements{metadata: metadata}
	transport := newForkTestTransport(metadata)
	control, err := controller.NewController(placements, transport, controller.ReconcileConfig{LeaseTTL: 30 * time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Second})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(state, make([]byte, 32))
	if err != nil {
		test.Fatal(err)
	}
	_, token, err := NewAPIKey(state, "developer", "project-a", "developer", false)
	if err != nil {
		test.Fatal(err)
	}
	management := &Management{config: ManagementConfig{State: state, Placements: placements, Controller: control, Identity: identity, Authority: transport, Audit: &gatewayTestAudit{}}}
	return &forkFixture{test: test, state: state, placements: placements, transport: transport, management: management, parent: parent, token: token}
}

type forkReply struct {
	RequestID string `json:"request_id"`
	OK        bool   `json:"ok"`
	Result    any    `json:"result"`
	Error     struct {
		Code string `json:"code"`
	} `json:"error"`
}

func (fixture *forkFixture) call(path string, payload map[string]any) (int, forkReply) {
	fixture.test.Helper()
	input := map[string]any{"database": fixture.parent}
	for key, value := range payload {
		input[key] = value
	}
	body, err := json.Marshal(input)
	if err != nil {
		fixture.test.Fatal(err)
	}
	request := httptest.NewRequest("POST", path, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.token)
	response := httptest.NewRecorder()
	fixture.management.ServeHTTP(response, request)
	var reply forkReply
	if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
		fixture.test.Fatalf("decode reply: %v body=%s", err, response.Body.String())
	}
	return response.Code, reply
}

func (fixture *forkFixture) view() State {
	fixture.test.Helper()
	state, err := fixture.state.Snapshot()
	if err != nil {
		fixture.test.Fatal(err)
	}
	return state
}

func (fixture *forkFixture) child(name string) controller.DatabaseID {
	child := fixture.parent
	child.Branch = name
	return child
}

func TestManagementForkDeterministicRejectionLeavesNoState(test *testing.T) {
	for _, scenario := range []struct {
		name    string
		path    string
		payload map[string]any
	}{
		{"invalid_role", "/v1/databases/branch", map[string]any{"request_id": "fork-invalid-role", "name": "child", "role": "owner"}},
		{"missing_snapshot", "/v1/databases/clone", map[string]any{"request_id": "fork-missing-snapshot", "name": "child"}},
		{"malformed_snapshot", "/v1/databases/clone", map[string]any{"request_id": "fork-malformed-snapshot", "name": "child", "snapshot": "bad/snapshot"}},
		{"invalid_name", "/v1/databases/branch", map[string]any{"request_id": "fork-invalid-name", "name": "bad name"}},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newForkFixture(test)
			status, reply := fixture.call(scenario.path, scenario.payload)
			if status != 409 || reply.Error.Code != "operation_incomplete" {
				test.Fatalf("invalid fork was not rejected: %d %+v", status, reply)
			}
			after := fixture.view()
			if len(after.Databases) != 1 || len(after.Provisionings) != 0 {
				test.Fatalf("invalid fork left state: databases=%d provisionings=%d", len(after.Databases), len(after.Provisionings))
			}
			if fixture.transport.maintenanceCalls() != 0 {
				test.Fatalf("invalid fork reached the authority %d times", fixture.transport.maintenanceCalls())
			}
			placement, err := fixture.placements.Snapshot()
			if err != nil || len(placement.Placements) != 0 {
				test.Fatalf("invalid fork left a placement: %+v %v", placement.Placements, err)
			}
			// The name and quota are still free for a new request id.
			status, reply = fixture.call("/v1/databases/branch", map[string]any{"request_id": "fork-valid-after-" + scenario.name, "name": "child", "role": "writer"})
			if status != 200 || !reply.OK {
				test.Fatalf("released name could not be reused: %d %+v", status, reply)
			}
		})
	}
}

func TestManagementForkRejectsSameRequestIDWithDifferentPayload(test *testing.T) {
	fixture := newForkFixture(test)
	payload := map[string]any{"request_id": "fork-one", "name": "child", "role": "writer"}
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("initial fork failed: %d %+v", status, reply)
	}
	if status, reply := fixture.call("/v1/databases/branch", map[string]any{"request_id": "fork-one", "name": "other", "role": "writer"}); status != 409 || reply.Error.Code != "operation_incomplete" {
		test.Fatalf("different payload reused the request id: %d %+v", status, reply)
	}
	after := fixture.view()
	if _, exists := after.Databases[fixture.child("other").Key()]; exists {
		test.Fatal("different payload reserved a second descriptor")
	}
	if fixture.transport.maintenanceCalls() != 1 {
		test.Fatalf("different payload reached the authority: %d", fixture.transport.maintenanceCalls())
	}
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("same payload retry did not converge: %d %+v", status, reply)
	}
	if fixture.transport.maintenanceCalls() != 1 {
		test.Fatalf("active retry replayed the authority command: %d", fixture.transport.maintenanceCalls())
	}
	after = fixture.view()
	record := after.Provisionings["fork-one"]
	if record.Stage != provisioningActive {
		test.Fatalf("same payload retry did not converge to active: %+v", record)
	}
}

func TestManagementForkAuthorityRejectionReleasesReservation(test *testing.T) {
	fixture := newForkFixture(test)
	fixture.transport.setFailure(true, false)
	payload := map[string]any{"request_id": "fork-rejected", "name": "child", "role": "writer"}
	if status, _ := fixture.call("/v1/databases/branch", payload); status != 409 {
		test.Fatalf("authority rejection returned %d", status)
	}
	after := fixture.view()
	if _, exists := after.Databases[fixture.child("child").Key()]; exists {
		test.Fatal("definitely rejected fork kept its descriptor")
	}
	record := after.Provisionings["fork-rejected"]
	if record.Stage != provisioningFailed || record.LastError != provisioningErrorAuthorityRejected {
		test.Fatalf("rejected fork record is not explicit: %+v", record)
	}
	if status, reply := fixture.call("/v1/databases/operations", map[string]any{"request_id": "fork-status", "operation_id": "fork-rejected"}); status != 200 {
		test.Fatalf("provisioning status query failed: %d", status)
	} else {
		encoded, _ := json.Marshal(reply.Result)
		if !strings.Contains(string(encoded), `"stage":"failed"`) || !strings.Contains(string(encoded), `"retry_allowed":true`) {
			test.Fatalf("provisioning status did not expose the failed stage: %s", encoded)
		}
	}
	// The same request id with a different payload is still refused by the
	// durable digest even though the descriptor was released.
	if status, _ := fixture.call("/v1/databases/branch", map[string]any{"request_id": "fork-rejected", "name": "other", "role": "writer"}); status != 409 {
		test.Fatalf("released failure accepted a different payload: %d", status)
	}
	fixture.transport.setFailure(false, false)
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("same-id retry did not recover: %d %+v", status, reply)
	}
	after = fixture.view()
	if after.Provisionings["fork-rejected"].Stage != provisioningActive {
		test.Fatalf("same-id retry did not converge to active: %+v", after.Provisionings["fork-rejected"])
	}
}

func TestManagementForkMissingSnapshotReleasesReservation(test *testing.T) {
	fixture := newForkFixture(test)
	fixture.transport.setMissingSnapshot("gone")
	payload := map[string]any{"request_id": "fork-missing-target", "name": "child", "role": "writer", "snapshot": "gone"}
	if status, _ := fixture.call("/v1/databases/clone", payload); status != 409 {
		test.Fatalf("missing snapshot returned %d", status)
	}
	after := fixture.view()
	if _, exists := after.Databases[fixture.child("child").Key()]; exists {
		test.Fatal("missing snapshot kept its descriptor")
	}
	record := after.Provisionings["fork-missing-target"]
	if record.Stage != provisioningFailed || record.LastError != provisioningErrorAuthorityRejected {
		test.Fatalf("missing snapshot record is not explicit: %+v", record)
	}
	// The descriptor and its quota slot were released for a new request id.
	fixture.transport.setMissingSnapshot("")
	if status, reply := fixture.call("/v1/databases/clone", map[string]any{"request_id": "fork-present-target", "name": "child", "role": "writer", "snapshot": "present"}); status != 200 || !reply.OK {
		test.Fatalf("released clone identity could not be reused: %d %+v", status, reply)
	}
}

func TestManagementForkUncertainAuthorityKeepsReservationForRetry(test *testing.T) {
	fixture := newForkFixture(test)
	fixture.transport.setFailure(false, true)
	payload := map[string]any{"request_id": "fork-uncertain", "name": "child", "role": "writer"}
	if status, _ := fixture.call("/v1/databases/branch", payload); status != 409 {
		test.Fatalf("ambiguous authority failure returned %d", status)
	}
	after := fixture.view()
	if _, exists := after.Databases[fixture.child("child").Key()]; !exists {
		test.Fatal("possible commit lost its descriptor and quota")
	}
	record := after.Provisionings["fork-uncertain"]
	if record.Stage != provisioningUncertain || record.LastError != provisioningErrorAuthorityUncertain {
		test.Fatalf("ambiguous failure is not explicit: %+v", record)
	}
	_, reply := fixture.call("/v1/databases/operations", map[string]any{"request_id": "fork-query", "operation_id": "fork-uncertain"})
	encoded, _ := json.Marshal(reply.Result)
	if !strings.Contains(string(encoded), `"stage":"uncertain"`) {
		test.Fatalf("uncertain stage is not queryable: %s", encoded)
	}
	fixture.transport.setFailure(false, false)
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("same-id retry did not continue: %d %+v", status, reply)
	}
	// The failed call, its catalog probe (also unavailable), and the retry.
	if fixture.transport.maintenanceCalls() != 3 {
		test.Fatalf("unexpected authority calls: %d", fixture.transport.maintenanceCalls())
	}
	if after = fixture.view(); after.Provisionings["fork-uncertain"].Stage != provisioningActive {
		test.Fatalf("uncertain retry did not converge to active: %+v", after.Provisionings["fork-uncertain"])
	}
}

func TestManagementForkResolvesAmbiguousFailureFromCatalog(test *testing.T) {
	fixture := newForkFixture(test)
	fixture.transport.setAmbiguousMutations(true, false)
	payload := map[string]any{"request_id": "fork-catalog-absent", "name": "child", "role": "writer"}
	if status, _ := fixture.call("/v1/databases/branch", payload); status != 409 {
		test.Fatalf("ambiguous authority failure returned %d", status)
	}
	after := fixture.view()
	if _, exists := after.Databases[fixture.child("child").Key()]; exists {
		test.Fatal("proven-unapplied fork kept its descriptor")
	}
	record := after.Provisionings["fork-catalog-absent"]
	if record.Stage != provisioningFailed || record.LastError != provisioningErrorAuthorityRejected {
		test.Fatalf("catalog resolution was not recorded: %+v", record)
	}
	// One failed mutation and one successful catalog read.
	if fixture.transport.maintenanceCalls() != 2 {
		test.Fatalf("catalog probe did not run exactly once: %d", fixture.transport.maintenanceCalls())
	}
	fixture.transport.setAmbiguousMutations(false, false)
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("same-id retry after proof did not recover: %d %+v", status, reply)
	}
}

func TestManagementForkLostAcknowledgementContinuesFromCatalog(test *testing.T) {
	fixture := newForkFixture(test)
	fixture.transport.setAmbiguousMutations(true, true)
	payload := map[string]any{"request_id": "fork-lost-ack", "name": "child", "role": "writer"}
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("lost acknowledgement did not converge: %d %+v", status, reply)
	}
	// The catalog read replaces a replay of the possibly committed mutation.
	if fixture.transport.maintenanceCalls() != 2 {
		test.Fatalf("workflow replayed the mutation instead of trusting the catalog: %d", fixture.transport.maintenanceCalls())
	}
	after := fixture.view()
	if after.Provisionings["fork-lost-ack"].Stage != provisioningActive {
		test.Fatalf("lost acknowledgement did not reach active: %+v", after.Provisionings["fork-lost-ack"])
	}
	if _, exists := after.Databases[fixture.child("child").Key()]; !exists {
		test.Fatal("lost acknowledgement lost its descriptor")
	}
}

func TestManagementForkStateUpdateBoundaryIsRecoverable(test *testing.T) {
	for _, scenario := range []struct {
		name           string
		failStage      string
		stage          string
		authority      int
		retryAuthority int
	}{
		{"reservation_fails", provisioningReserved, "", 0, 1},
		{"authority_stage_write_fails", provisioningAuthorityApplied, provisioningReserved, 1, 2},
		{"placement_stage_write_fails", provisioningPlacementReady, provisioningAuthorityApplied, 1, 1},
		{"active_stage_write_fails", provisioningActive, provisioningPlacementReady, 1, 1},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newForkFixture(test)
			payload := map[string]any{"request_id": "fork-stage", "name": "child", "role": "writer"}
			if scenario.failStage == provisioningReserved {
				// The reservation itself must fail before the authority is called.
				fixture.state.setFailStage(provisioningReserved)
				if status, _ := fixture.call("/v1/databases/branch", payload); status != 409 {
					test.Fatalf("injected reservation failure returned %d", status)
				}
				after := fixture.view()
				if len(after.Databases) != 1 || len(after.Provisionings) != 0 || fixture.transport.maintenanceCalls() != 0 {
					test.Fatalf("failed reservation left effects: databases=%d provisionings=%d authority=%d", len(after.Databases), len(after.Provisionings), fixture.transport.maintenanceCalls())
				}
			} else {
				fixture.state.setFailStage(scenario.failStage)
				if status, _ := fixture.call("/v1/databases/branch", payload); status != 409 {
					test.Fatalf("injected stage failure returned %d", status)
				}
				after := fixture.view()
				record := after.Provisionings["fork-stage"]
				if record.Stage != scenario.stage {
					test.Fatalf("failed stage write is not explainable: %+v", record)
				}
				if _, exists := after.Databases[fixture.child("child").Key()]; !exists {
					test.Fatal("partially applied fork lost its descriptor")
				}
				if fixture.transport.maintenanceCalls() != scenario.authority {
					test.Fatalf("unexpected authority calls: %d", fixture.transport.maintenanceCalls())
				}
			}
			fixture.state.setFailStage("")
			if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
				test.Fatalf("retry after stage failure did not converge: %d %+v", status, reply)
			}
			after := fixture.view()
			if after.Provisionings["fork-stage"].Stage != provisioningActive {
				test.Fatalf("retry did not reach active: %+v", after.Provisionings["fork-stage"])
			}
			if fixture.transport.maintenanceCalls() != scenario.retryAuthority {
				test.Fatalf("retry did not reconcile the durable stage: %d", fixture.transport.maintenanceCalls())
			}
		})
	}
}

func TestManagementForkAdoptsLegacyActiveDescriptorWithoutReplay(test *testing.T) {
	fixture := newForkFixture(test)
	payload := map[string]any{"request_id": "fork-legacy", "name": "child", "role": "writer"}
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("fork failed: %d %+v", status, reply)
	}
	if err := fixture.state.Update(func(state *State) error {
		delete(state.Provisionings, "fork-legacy")
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if status, reply := fixture.call("/v1/databases/branch", payload); status != 200 || !reply.OK {
		test.Fatalf("legacy active descriptor did not converge: %d %+v", status, reply)
	}
	if fixture.transport.maintenanceCalls() != 1 {
		test.Fatalf("legacy adoption replayed the authority command: %d", fixture.transport.maintenanceCalls())
	}
	if after := fixture.view(); after.Provisionings["fork-legacy"].Stage != provisioningActive {
		test.Fatalf("legacy adoption did not record a terminal stage: %+v", after.Provisionings["fork-legacy"])
	}
}

func TestProvisioningRecordValidationRejectsTampering(test *testing.T) {
	fixture := newForkFixture(test)
	if status, reply := fixture.call("/v1/databases/branch", map[string]any{"request_id": "fork-valid", "name": "child", "role": "writer"}); status != 200 || !reply.OK {
		test.Fatalf("fork failed: %d %+v", status, reply)
	}
	state := fixture.view()
	if err := validateState(state, nil); err != nil {
		test.Fatalf("committed provisioning record does not validate: %v", err)
	}
	tampered, err := cloneState(state)
	if err != nil {
		test.Fatal(err)
	}
	record := tampered.Provisionings["fork-valid"]
	record.Stage = "guessed_rollback"
	tampered.Provisionings["fork-valid"] = record
	if err := validateState(tampered, &state); err == nil {
		test.Fatal("invalid provisioning stage was accepted")
	}
	tampered, err = cloneState(state)
	if err != nil {
		test.Fatal(err)
	}
	record = tampered.Provisionings["fork-valid"]
	record.Digest = "provisioning_v1:" + strings.Repeat("0", 64)
	tampered.Provisionings["fork-valid"] = record
	if err := validateState(tampered, &state); err == nil {
		test.Fatal("provisioning digest rewrite was accepted")
	}
	progress, err := cloneState(state)
	if err != nil {
		test.Fatal(err)
	}
	record = progress.Provisionings["fork-valid"]
	record.Stage = provisioningPlacementReady
	progress.Provisionings["fork-valid"] = record
	if err := validateState(progress, &state); err != nil {
		test.Fatalf("stage progress was rejected: %v", err)
	}
}
