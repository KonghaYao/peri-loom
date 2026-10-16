package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func operationDurableStore(test *testing.T) StateStore {
	test.Helper()
	store := securityState(test)
	if err := store.Update(func(state *State) error {
		database := securityDatabase()
		state.Databases[database.Key()] = Database{ID: database, Project: "alpha", State: "active"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	return store
}

func TestOperationDurableReloadAndCrashIsolation(test *testing.T) {
	for _, stage := range []string{"open_session", "tool_rpc", "close_session"} {
		test.Run(stage, func(test *testing.T) {
			store := operationDurableStore(test)
			database := securityDatabase()
			if err := beginOperation(store, database, "crash", "management", "checkpoint", "open_session", "worker-a", "", map[string]string{"command": "checkpoint"}); err != nil {
				test.Fatal(err)
			}
			if err := updateOperation(store, database, "crash", func(record *OperationRecord) {
				record.Stage = stage
				if stage != "open_session" {
					record.Session = strings.Repeat("a", 64)
					record.CleanupOutcome = "pending"
				}
				if stage == "close_session" {
					record.BusinessOutcome = "completed"
				}
			}); err != nil {
				test.Fatal(err)
			}
			directory := filepath.Dir(store.(*stateStore).disk.path)
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			reloaded, err := OpenStateStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			defer reloaded.Close()
			state, err := reloaded.Snapshot()
			record := state.Operations[operationKey(database, "crash")]
			if err != nil || record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed || record.ExecutionCompleted {
				test.Fatalf("crash lost isolation or invented ACK: %+v %v", record.OperationStatus, err)
			}
			if stage == "close_session" && (record.BusinessOutcome != "completed" || record.CleanupOutcome != "unknown") {
				test.Fatal("cleanup uncertainty erased acknowledged business")
			}
			if err := beginOperation(reloaded, database, "retry", "management", "checkpoint", "open_session", "worker-a", "", nil); !errors.Is(err, ErrDatabaseQuarantined) {
				test.Fatal("restart admitted a retry")
			}
			if err := reloaded.Update(func(state *State) error { delete(state.Operations, operationKey(database, "crash")); return nil }); err == nil {
				test.Fatal("quarantine clear accepted")
			}
		})
	}
}

func TestOperationAcknowledgedOpenWithoutCloseProofReloadsIsolated(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "known_open", "gateway", "open", "open_session", "worker-a", strings.Repeat("a", 64), nil); err != nil {
		test.Fatal(err)
	}
	if err := updateOperation(store, database, "known_open", func(record *OperationRecord) {
		record.State, record.BusinessOutcome, record.CleanupOutcome, record.ExecutionProven = "terminal", "completed", "pending", true
	}); err != nil {
		test.Fatal(err)
	}
	directory := filepath.Dir(store.(*stateStore).disk.path)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reloaded, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reloaded.Close()
	state, err := reloaded.Snapshot()
	record := state.Operations[operationKey(database, "known_open")]
	if err != nil || record.State != "quarantined" || record.BusinessOutcome != "completed" || record.CleanupOutcome != "unknown" || !record.ExecutionProven {
		test.Fatal("Open ACK incorrectly implied session Close proof after crash")
	}
}

func operationHTTP(test *testing.T, management *Management, token, path string, input any) *httptest.ResponseRecorder {
	test.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	management.ServeHTTP(response, request)
	return response
}

func TestOperationHTTPFlagsAuthorizationDecisionAndNoLeaks(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	management.config.Identity, management.config.Audit = fixture.identity, fixture.audit
	_, adminToken, err := NewAPIKey(fixture.state, "operator", "project-a", "admin", false)
	if err != nil {
		test.Fatal(err)
	}
	privateCause := errors.New("private-session SQL-text credentials " + adminToken)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "checkpoint" {
			// A reply that proves nothing is the only one the platform can produce
			// on this path, so the operator surface must report both directions as
			// unproven instead of echoing a state the worker never sent.
			return nil, &SQLRPCError{Ambiguous: true, cause: privateCause}
		}
		return nil, nil
	})
	response := operationHTTP(test, management, adminToken, "/v1/databases/checkpoint", managementRequest{RequestID: "correlated", Database: fixture.first})
	var reply struct {
		RequestID string `json:"request_id"`
		Error     struct {
			Code               string `json:"code"`
			OperationID        string `json:"operation_id"`
			Stage              string `json:"stage"`
			Ambiguous          bool   `json:"ambiguous"`
			Rejected           bool   `json:"rejected"`
			ExecutionCompleted bool   `json:"execution_completed"`
			ExecutionProven    bool   `json:"execution_proven"`
			RetryAllowed       bool   `json:"retry_allowed"`
		} `json:"error"`
	}
	if json.Unmarshal(response.Body.Bytes(), &reply) != nil || response.Code != 502 || reply.RequestID != "correlated" || reply.Error.OperationID == "" || reply.Error.OperationID == "correlated" || reply.Error.Code != "outcome_unknown" || reply.Error.Stage != "tool_rpc" || !reply.Error.Ambiguous || reply.Error.Rejected || reply.Error.ExecutionCompleted || reply.Error.ExecutionProven || reply.Error.RetryAllowed {
		test.Fatalf("incorrect unknown response: %s", response.Body.String())
	}
	for _, token := range []string{fixture.tokenB, fixture.viewer, fixture.tokenA} {
		denied := operationHTTP(test, management, token, "/v1/databases/operations", managementRequest{RequestID: "denied", Database: fixture.first})
		if denied.Code == 200 || strings.Contains(denied.Body.String(), "correlated") {
			test.Fatal("unauthorized incident disclosed")
		}
	}
	for _, path := range []string{"/v1/databases/operations", "/v1/databases/status", "/v1/databases/schema", "/v1/databases/diagnostics"} {
		read := operationHTTP(test, management, adminToken, path, managementRequest{RequestID: "read", Database: fixture.first})
		if read.Code != 200 || !strings.Contains(read.Body.String(), "correlated") {
			test.Fatalf("safe incident read failed: %s", read.Body.String())
		}
		for _, forbidden := range []string{"private-session", "SQL-text", adminToken, strings.Repeat("a", 64), "\"session\"", "\"digest\""} {
			if strings.Contains(read.Body.String(), forbidden) || strings.Contains(response.Body.String(), forbidden) {
				test.Fatalf("private field leaked: %s", forbidden)
			}
		}
	}
	decision := managementRequest{RequestID: "operator_decision", Database: fixture.first, OperationID: reply.Error.OperationID, Decision: "retain_isolation_new_database_required"}
	for attempt := 0; attempt < 2; attempt++ {
		if response := operationHTTP(test, management, adminToken, "/v1/databases/operations/decision", decision); response.Code != 200 {
			test.Fatalf("retain decision rejected: %s", response.Body.String())
		}
	}
	for _, path := range []string{"/v1/databases/operations/clear", "/v1/databases/checkpoint", "/v1/databases/resume", "/v1/databases/delete", "/v1/databases/snapshot"} {
		if response := operationHTTP(test, management, adminToken, path, managementRequest{RequestID: "reject", Database: fixture.first}); response.Code == 200 {
			test.Fatal("isolated mutation or clear accepted")
		}
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 || fixture.upstream.count("checkpoint") != 1 {
		test.Fatal("safe read or decision touched native sessions")
	}
}

type operationFailStore struct {
	StateStore
	updates int
	failAt  int
}

func (store *operationFailStore) Update(change func(*State) error) error {
	store.updates++
	if store.updates == store.failAt {
		return ErrStateUncertain
	}
	return store.StateStore.Update(change)
}

func TestOperationPersistenceFailureRefusesDispatchAndBlindClose(test *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4} {
		test.Run(string(rune('0'+failAt)), func(test *testing.T) {
			management, fixture, _, _ := nativeDiagnosticManagement(test)
			management.config.State = &operationFailStore{StateStore: fixture.state, failAt: failAt}
			_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "durability_failure", Database: fixture.first}, "/v1/databases/checkpoint")
			if err == nil || failAt == 1 && fixture.upstream.count("open_session") != 0 || failAt <= 2 && fixture.upstream.count("checkpoint") != 0 || failAt <= 3 && fixture.upstream.count("close_session") != 0 {
				test.Fatal("durability failure dispatched without registration or blindly closed")
			}
		})
	}
}

func TestOperationCorrelationOnlyAndCapacityRefuseBeforeDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	request := map[string]any{"request_id": "same_id", "database": fixture.first}
	first := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", request)
	if first.Code != 200 {
		test.Fatal(first.Body.String())
	}
	second := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", request)
	if second.Code != 200 || fixture.upstream.count("open_session") != 2 {
		test.Fatal("request correlation silently became SQL deduplication")
	}
	if err := fixture.state.Update(func(state *State) error {
		state.Operations = make(map[string]OperationRecord, operationCapacity)
		for index := 0; index < operationCapacity; index++ {
			state.Operations[string(rune(index))] = OperationRecord{OperationStatus: OperationStatus{State: "terminal"}}
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	full := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"request_id": "full", "database": fixture.first})
	if full.Code != 503 || fixture.upstream.count("open_session") != 2 {
		test.Fatal("saturated durable capacity dispatched")
	}
}

func TestOperationSameCorrelationDifferentSQLDoesNotDeduplicate(test *testing.T) {
	fixture := newGatewayFixture(test)
	opened := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"request_id": "reused", "database": fixture.first})
	var reply struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if opened.Code != 200 || json.Unmarshal(opened.Body.Bytes(), &reply) != nil {
		test.Fatal("fresh session failed")
	}
	for _, sql := range []string{"CREATE TABLE independent(id INTEGER)", "INSERT INTO independent VALUES(1)"} {
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"request_id": "reused", "session": reply.Result.Session, "sql": sql})
		if response.Code != 200 {
			test.Fatal("correlation ID incorrectly deduplicated different SQL")
		}
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	operations := map[string]bool{}
	digests := map[string]bool{}
	for _, record := range state.Operations {
		if record.RequestID == "reused" && record.Action == "execute" {
			operations[record.OperationID], digests[record.Digest] = true, true
		}
	}
	if len(operations) != 2 || len(digests) != 2 || fixture.upstream.count("execute") != 2 || fixture.upstream.count("close_session") != 0 {
		test.Fatal("independent SQL operations collapsed or auto-closed")
	}
}

func TestOperationCloseACKDurablyTerminatesOpenResource(test *testing.T) {
	fixture := newGatewayFixture(test)
	opened := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	var reply struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if opened.Code != 200 || json.Unmarshal(opened.Body.Bytes(), &reply) != nil {
		test.Fatal("fresh session failed")
	}
	if closed := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]string{"session": reply.Result.Session}); closed.Code != 200 {
		test.Fatal("explicit healthy Close failed")
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Action == "open" && (record.CleanupOutcome != "completed" || operationNeedsRecovery(record)) {
			test.Fatal("Close ACK did not terminate durable native resource")
		}
	}
}

func TestOperationCleanupUnknownKeepsBusinessACK(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "checkpoint" {
			return map[string]bool{"checkpointed": true}, nil
		}
		if command["command"] == "close_session" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "cleanup", Database: fixture.first}, "/v1/databases/checkpoint")
	state, _ := fixture.state.Snapshot()
	var record OperationRecord
	for _, candidate := range state.Operations {
		if candidate.RequestID == "cleanup" {
			record = candidate
		}
	}
	if err == nil || record.BusinessOutcome != "completed" || record.CleanupOutcome != "unknown" || record.State != "quarantined" {
		test.Fatalf("lost business ACK or cleanup uncertainty: %+v", record.OperationStatus)
	}
	var output bytes.Buffer
	response := httptest.NewRecorder()
	managementUnknownReply(response, "cleanup", err)
	output.Write(response.Body.Bytes())
	if !strings.Contains(output.String(), `"business_outcome":"completed"`) || !strings.Contains(output.String(), `"cleanup_outcome":"unknown"`) {
		test.Fatal("HTTP suggested rerunning acknowledged Checkpoint")
	}
}

func TestOperationDurableCapacityAndTerminalReplay(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "completed", "gateway", "execute", "tool_rpc", "worker-a", strings.Repeat("a", 64), map[string]string{"sql": "INSERT"}); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "completed", nil, true); err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "completed", "gateway", "execute", "tool_rpc", "worker-a", strings.Repeat("a", 64), map[string]string{"sql": "DIFFERENT"}); !errors.Is(err, ErrOperationExists) {
		test.Fatal("different digest reused an operation ID")
	}
	if err := store.Update(func(state *State) error {
		template := state.Operations[operationKey(database, "completed")]
		for index := 0; index < operationCapacity; index++ {
			record := template
			record.OperationID = fmt.Sprintf("unresolved_%d", index)
			record.State, record.BusinessOutcome, record.ExecutionProven, record.RegistrationSequence = "in_flight", "not_proven", false, 0
			state.Operations[operationKey(database, record.OperationID)] = record
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "overflow", "gateway", "execute", "tool_rpc", "worker-a", strings.Repeat("a", 64), nil); !errors.Is(err, ErrOperationCapacity) {
		test.Fatal("durable capacity overflow admitted")
	}
}

type operationStopVerifier struct{ failure error }

func (verifier operationStopVerifier) VerifyOwnedWorkerStopped(context.Context, string) error {
	return verifier.failure
}

func TestOperationRetirementNeedsOwnedStopProofAndNeverClearsIsolation(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	management.config.Identity = fixture.identity
	_, token, err := NewAPIKey(fixture.state, "operator", "project-a", "admin", false)
	if err != nil {
		test.Fatal(err)
	}
	principal, err := fixture.identity.Authenticate(token)
	if err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(fixture.state, fixture.first, "lost", "gateway", "open", "open_session", "worker-tenant-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.state, fixture.first, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	resources, err := NewGateway(fixture.gateway.config)
	if err != nil {
		test.Fatal(err)
	}
	management.config.Resources = resources
	decision := managementRequest{RequestID: "decision", Database: fixture.first, OperationID: "lost", Decision: "retain_isolation_new_database_required"}
	if _, err := management.operations(principal, decision, "/v1/databases/operations/decision"); err != nil {
		test.Fatal(err)
	}
	input := managementRequest{RequestID: "retire", Database: fixture.first}
	if _, err := management.retireLocal(context.Background(), principal, input); err == nil {
		test.Fatal("missing proof retired local resources")
	}
	management.config.StopProof = operationStopVerifier{failure: errors.New("worker still alive")}
	if _, err := management.retireLocal(context.Background(), principal, input); err == nil {
		test.Fatal("negative proof retired resources")
	}
	if resources.Metrics()["unknown_held_permits"] != 1 {
		test.Fatal("unproven stop recycled unknown permit")
	}
	management.config.StopProof = operationStopVerifier{}
	if _, err := management.retireLocal(context.Background(), principal, input); err != nil {
		test.Fatal(err)
	}
	if resources.Metrics()["unknown_held_permits"] != 0 {
		test.Fatal("proven stop did not retire local permit")
	}
	state, _ := fixture.state.Snapshot()
	record := state.Operations[operationKey(fixture.first, "lost")]
	if !record.LocalResourcesRetired || record.State != "quarantined" || record.BusinessOutcome != "unknown" {
		test.Fatal("local retirement falsely proved business or cleared isolation")
	}
	reloaded, err := NewGateway(fixture.gateway.config)
	if err != nil {
		test.Fatal(err)
	}
	if reloaded.Metrics()["unknown_held_permits"] != 0 {
		test.Fatal("retired resources reconstructed after reload")
	}
	if err := beginOperation(fixture.state, fixture.first, "reopen", "gateway", "open", "open_session", "worker-tenant-a", "", nil); !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatal("retirement reopened namespace")
	}
}

func TestOperationGrantAcquireDrainGateAfterUnknown(test *testing.T) {
	fixture := newGatewayFixture(test)
	if err := beginOperation(fixture.state, fixture.first, "lost", "gateway", "open", "open_session", "worker-tenant-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.state, fixture.first, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	transport := &isolatedControlTransport{store: fixture.state}
	intent, _ := json.Marshal(map[string]any{"database": fixture.first})
	grant := map[string]any{"command": "grant", "request": map[string]any{"intent": json.RawMessage(intent)}}
	if err := transport.permitted(grant, false); !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatal("isolated namespace granted")
	}
	for _, command := range []string{"drain_database", "revoke_database", "acquire_database"} {
		action := map[string]any{"command": command, "database": fixture.first, "lease": map[string]any{"binding": map[string]any{"database": fixture.first}}}
		if err := transport.permitted(action, true); !errors.Is(err, ErrDatabaseQuarantined) {
			test.Fatal("unknown namespace drained, revoked, or acquired")
		}
	}
	for _, command := range []string{"get_worker_state", "renew_lease"} {
		if err := transport.permitted(map[string]string{"command": command}, true); err != nil {
			test.Fatal("safe observation or resource preservation denied")
		}
	}
}
