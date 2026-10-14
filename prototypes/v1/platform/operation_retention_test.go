package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type retentionTestStore struct {
	mu    sync.Mutex
	state State
}

func (store *retentionTestStore) Snapshot() (State, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state := store.state
	state.Operations = maps.Clone(state.Operations)
	return state, nil
}

func (store *retentionTestStore) Update(change func(*State) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	candidate := store.state
	candidate.Operations = maps.Clone(store.state.Operations)
	if err := change(&candidate); err != nil {
		return err
	}
	if err := validateOperations(candidate, &store.state); err != nil {
		return err
	}
	store.state = candidate
	return nil
}

func (*retentionTestStore) Close() error { return nil }

func TestOperationKnownTerminalRetentionIsBounded(test *testing.T) {
	fixture := newGatewayFixture(test)
	for index := 0; index < 258; index++ {
		identifier := fmt.Sprintf("known_%04d", index)
		if err := beginOperation(fixture.state, fixture.first, identifier, "gateway", "execute", "tool_rpc", "worker-tenant-a", "", nil); err != nil {
			test.Fatal(err)
		}
		if err := finishOperation(fixture.state, fixture.first, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
	}
	state, err := fixture.state.Snapshot()
	if err != nil || len(state.Operations) != 256 {
		test.Fatal("safe terminal history grows without a bounded retention policy")
	}
	if _, exists := state.Operations[operationKey(fixture.first, "known_0000")]; exists {
		test.Fatal("old known terminal was not evicted")
	}
	if _, exists := state.Operations[operationKey(fixture.first, "known_0257")]; !exists {
		test.Fatal("new known terminal was randomly evicted")
	}
}

func TestOperationKnownRPCsProgressBeyondLifetimeCapacity(test *testing.T) {
	fixture := newGatewayFixture(test)
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	store := &retentionTestStore{state: state}
	fixture.gateway.config.State = store
	opened := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first, "request_id": "correlation"})
	var openReply struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if opened.Code != 200 || json.Unmarshal(opened.Body.Bytes(), &openReply) != nil {
		test.Fatal("session unavailable")
	}
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[openReply.Result.Session]
	fixture.gateway.mu.Unlock()
	session.writerKnown, session.writerOwner = true, true
	seen := map[string]bool{}
	firstOperation := ""
	baselineSize := 0
	total := operationCapacity + 256
	for index := 0; index < total; index++ {
		fixture.gateway.admission.mu.Lock()
		fixture.gateway.admission.projects["project-a"].tokens = 1000
		fixture.gateway.admission.mu.Unlock()
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session.public, "request_id": "correlation", "sql": fmt.Sprintf("INSERT INTO retained_ledger VALUES(%d)", index)})
		if response.Code != 200 {
			test.Fatalf("known RPC stopped at %d: %s", index, response.Body.String())
		}
		current, err := store.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		for _, record := range current.Operations {
			if record.RegistrationSequence == current.OperationSequence {
				if seen[record.OperationID] {
					test.Fatal("server operation ID reused or correlation deduplicated")
				}
				seen[record.OperationID] = true
				if index == 0 {
					firstOperation = record.OperationID
				}
			}
		}
		if index >= operationKnownRetention && index%256 == 0 {
			encoded, err := json.Marshal(current)
			if err != nil {
				test.Fatal(err)
			}
			if baselineSize == 0 {
				baselineSize = len(encoded)
			}
			if len(encoded) > baselineSize+2048 {
				test.Fatal("operation JSON grows with lifetime RPC count")
			}
		}
	}
	state, _ = store.Snapshot()
	known, unresolved := operationCounts(state)
	if known != operationKnownRetention || unresolved != 1 || state.OperationSequence != uint64(total+1) || state.OperationEvictions != uint64(total-operationKnownRetention) || len(seen) != total {
		test.Fatal("retention count, sequence or cumulative eviction count incorrect")
	}
	if fixture.upstream.count("execute") != total || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 || !session.writerOwner || session.closed {
		test.Fatal("retention replayed SQL or closed/evicted a known-completed owner session")
	}
	management := &Management{config: ManagementConfig{State: store, Identity: &nativeDiagnosticIdentity{}}}
	result, err := management.operations(Principal{}, managementRequest{Database: fixture.first, OperationID: firstOperation}, "/v1/databases/operations")
	encoded, _ := json.Marshal(result)
	if err != nil || !strings.Contains(string(encoded), `"operations":[]`) || !strings.Contains(string(encoded), `"history_complete":false`) || !strings.Contains(string(encoded), `"missing_record_proves_not_executed":false`) || !strings.Contains(string(encoded), `"missing_record_allows_replay":false`) || !strings.Contains(string(encoded), `"retry_allowed":false`) || strings.Contains(string(encoded), `"registration_sequence":`) {
		test.Fatal("missing history implies non-execution or replay, or leaked private ordering")
	}
}

func TestOperationKnownRetentionOrderAndCountersSurviveReload(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	for index := 0; index < operationKnownRetention+2; index++ {
		identifier := fmt.Sprintf("known_%04d", 9999-index)
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
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
	state, _ := reloaded.Snapshot()
	if len(state.Operations) != operationKnownRetention || state.OperationSequence != 258 || state.OperationEvictions != 2 {
		test.Fatal("reload lost bounded counters")
	}
	for _, identifier := range []string{"known_9999", "known_9998"} {
		if _, exists := state.Operations[operationKey(database, identifier)]; exists {
			test.Fatal("retention used random ID order rather than durable registration order")
		}
	}
	if err := beginOperation(reloaded, database, "newest", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(reloaded, database, "newest", nil, true); err != nil {
		test.Fatal(err)
	}
	state, _ = reloaded.Snapshot()
	if state.OperationSequence != 259 || state.OperationEvictions != 3 {
		test.Fatal("registration order reset after restart")
	}
	if _, exists := state.Operations[operationKey(database, "newest")]; !exists {
		test.Fatal("new known record evicted instead of oldest")
	}
}

func TestOperationFullUnresolvedCapacityRefusesBeforeDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	state, _ := fixture.state.Snapshot()
	store := &retentionTestStore{state: state}
	fixture.gateway.config.State = store
	identifier, err := registerOperation(store, fixture.second, "protected", "gateway", "open", "open_session", "worker-tenant-b", "", nil)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		template := state.Operations[operationKey(fixture.second, identifier)]
		for index := 1; index < operationCapacity; index++ {
			record := template
			record.OperationID, record.RegistrationSequence = fmt.Sprintf("unresolved_%04d", index), 0
			state.Operations[operationKey(fixture.second, record.OperationID)] = record
		}
		for index := 0; index < operationKnownRetention; index++ {
			record := template
			record.OperationID, record.RegistrationSequence, record.State, record.Action, record.Stage, record.BusinessOutcome, record.ExecutionProven = fmt.Sprintf("known_%04d", index), 0, "terminal", "execute", "tool_rpc", "completed", true
			state.Operations[operationKey(fixture.second, record.OperationID)] = record
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != 503 || fixture.upstream.count("open_session") != 0 {
		test.Fatal("full unresolved capacity dispatched native mutation")
	}
	state, _ = store.Snapshot()
	if known, unresolved := operationCounts(state); known != operationKnownRetention || unresolved != operationCapacity || state.OperationSequence != 1 {
		test.Fatal("rejected dispatch consumed a sequence or dropped unresolved records")
	}
}

func TestOperationRetentionNeverDeletesProtectedHistory(test *testing.T) {
	for _, category := range []string{"in_flight", "quarantined", "open_pending", "retirement_pending", "retired_quarantine", "ambiguous_terminal"} {
		test.Run(category, func(test *testing.T) {
			store := operationDurableStore(test)
			database := securityDatabase()
			if err := beginOperation(store, database, "protected", "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
				test.Fatal(err)
			}
			if err := store.Update(func(state *State) error {
				record := state.Operations[operationKey(database, "protected")]
				switch category {
				case "quarantined", "retirement_pending", "retired_quarantine":
					record.State, record.BusinessOutcome, record.Ambiguous = "quarantined", "unknown", true
				case "open_pending":
					record.State, record.BusinessOutcome, record.CleanupOutcome, record.ExecutionProven = "terminal", "completed", "pending", true
				case "ambiguous_terminal":
					record.State, record.BusinessOutcome, record.ExecutionProven, record.Ambiguous, record.ExecutionCompleted = "terminal", "completed", true, true, true
				}
				if category == "retirement_pending" || category == "retired_quarantine" {
					record.Decision, record.DecisionRequestID, record.RetirementPending = "retain_isolation_new_database_required", "decision", true
				}
				state.Operations[operationKey(database, "protected")] = record
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			if category == "retired_quarantine" {
				if err := store.Update(func(state *State) error {
					record := state.Operations[operationKey(database, "protected")]
					record.LocalResourcesRetired, record.RetirementRequestID, record.RetirementPending = true, "retired", false
					state.Operations[operationKey(database, "protected")] = record
					return nil
				}); err != nil {
					test.Fatal(err)
				}
			}
			if err := store.Update(func(state *State) error { delete(state.Operations, operationKey(database, "protected")); return nil }); err == nil {
				test.Fatal("protected history deleted")
			}
			if err := store.Update(func(state *State) error {
				if err := removeKnownDatabaseOperations(state, database); err != nil {
					return err
				}
				delete(state.Databases, database.Key())
				return nil
			}); err == nil {
				test.Fatal("database deletion cleared protected history")
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
			state, _ := reloaded.Snapshot()
			if _, exists := state.Operations[operationKey(database, "protected")]; !exists {
				test.Fatal("reload deleted protected history")
			}
			fixture := newGatewayFixture(test)
			config := fixture.gateway.config
			config.State = reloaded
			resources, err := NewGateway(config)
			if err != nil {
				test.Fatal(err)
			}
			if category != "retired_quarantine" && category != "ambiguous_terminal" && resources.Metrics()["unknown_held_permits"] != 1 {
				test.Fatal("reload lost protected resource admission")
			}
			if category == "retired_quarantine" && resources.Metrics()["unknown_held_permits"] != 0 {
				test.Fatal("retired tombstone rebuilt resource quota")
			}
			if category != "ambiguous_terminal" {
				if err := beginOperation(reloaded, database, "new_id", "gateway", "open", "open_session", "worker-a", "", nil); !errors.Is(err, ErrDatabaseQuarantined) {
					test.Fatal("new ID bypassed protected quarantine after restart")
				}
			}
		})
	}
}

func TestOperationKnownCloseAllowsDatabaseDeletionWithoutHistoryProof(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	upstreamSession := strings.Repeat("a", 64)
	if err := beginOperation(store, database, "opened", "gateway", "open", "open_session", "worker-a", upstreamSession, nil); err != nil {
		test.Fatal(err)
	}
	if err := updateOperation(store, database, "opened", func(record *OperationRecord) {
		record.State, record.BusinessOutcome, record.CleanupOutcome, record.ExecutionProven = "terminal", "completed", "pending", true
	}); err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "closed", "gateway", "close", "close_session", "worker-a", upstreamSession, nil); err != nil {
		test.Fatal(err)
	}
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	config := fixture.gateway.config
	config.State = store
	resources, err := NewGateway(config)
	if err != nil {
		test.Fatal(err)
	}
	result := gatewayResult{session: &gatewaySession{upstream: upstreamSession, database: database}, dispatched: true}
	resources.settleOperation(database, "closed", "close_session", &result, nil)
	state, err := store.Snapshot()
	if err != nil || !operationKnownTerminal(state.Operations[operationKey(database, "opened")]) || !operationKnownTerminal(state.Operations[operationKey(database, "closed")]) {
		test.Fatal("Close ACK did not settle safe resource history")
	}
	management.config.State = store
	if err := management.setDatabaseState(database, "deleted"); err != nil {
		test.Fatal(err)
	}
	state, _ = store.Snapshot()
	if len(state.Operations) != 0 || state.Databases[database.Key()].State != "deleted" || state.OperationEvictions != 2 {
		test.Fatal("normal deletion kept safe history or removed database identity tombstone")
	}
	value, err := management.operations(Principal{}, managementRequest{Database: database, OperationID: "opened"}, "/v1/databases/operations")
	encoded, _ := json.Marshal(value)
	if err != nil || !strings.Contains(string(encoded), `"retry_allowed":false`) || !strings.Contains(string(encoded), `"history_complete":false`) {
		test.Fatal("deleted history implied non-execution or replay")
	}
	management.config.Controller = nil
	if err := beginOperation(store, database, "lost", "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if _, err := management.deleteDatabase(context.Background(), Principal{}, managementRequest{Database: database, RequestID: "delete_unknown"}); !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatal("unknown deletion reached controller dispatch")
	}
}

func TestOperationLegacyRetentionMigrationPreservesUnknownAndOpen(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "legacy_template", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "legacy_template", nil, true); err != nil {
		test.Fatal(err)
	}
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	template := state.Operations[operationKey(database, "legacy_template")]
	before := state
	state.Operations = map[string]OperationRecord{}
	state.OperationSequence = 0
	for index := 0; index < operationKnownRetention+20; index++ {
		record := template
		record.OperationID, record.RegistrationSequence = fmt.Sprintf("legacy_%04d", index), 0
		state.Operations[operationKey(database, record.OperationID)] = record
	}
	unknown := template
	unknown.OperationID, unknown.RegistrationSequence, unknown.State, unknown.BusinessOutcome, unknown.ExecutionProven, unknown.Ambiguous = "lost", 0, "quarantined", "unknown", false, true
	state.Operations[operationKey(database, unknown.OperationID)] = unknown
	opened := template
	opened.OperationID, opened.Action, opened.RegistrationSequence, opened.CleanupOutcome, opened.Session = "old_open", "open", 0, "pending", strings.Repeat("b", 64)
	state.Operations[operationKey(database, opened.OperationID)] = opened
	state.Revision++
	disk := store.(*stateStore).disk
	directory := filepath.Dir(disk.path)
	if validateState(state, &before) == nil {
		test.Fatal("legacy fixture unexpectedly accepted as a current mutation")
	}
	if err := validateState(state, nil); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		test.Fatal(err)
	}
	checksum := digestHex(payload)
	envelope, err := json.Marshal(diskEnvelope{Version: 1, Revision: state.Revision, Checksum: checksum, Payload: payload})
	if err != nil {
		test.Fatal(err)
	}
	floor, err := json.Marshal(diskFloor{Version: 1, Revision: state.Revision, Checksum: checksum})
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicPrivate(disk.path+".floor", floor); err != nil {
		test.Fatal(err)
	}
	if err := atomicPrivate(disk.path, envelope); err != nil {
		test.Fatal(err)
	}
	if _, err := os.Lstat(disk.path + ".pending"); !os.IsNotExist(err) {
		test.Fatal("standalone legacy snapshot unexpectedly has a new commit intent")
	}
	reloaded, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reloaded.Close()
	state, _ = reloaded.Snapshot()
	known, unresolved := operationCounts(state)
	if known != operationKnownRetention || unresolved != 2 || state.OperationEvictions != 20 {
		test.Fatal("legacy migration lost bounded history or protected records")
	}
	if _, exists := state.Operations[operationKey(database, "legacy_0000")]; exists {
		test.Fatal("legacy order not deterministic")
	}
	if state.Operations[operationKey(database, "old_open")].State != "quarantined" || state.Operations[operationKey(database, "lost")].State != "quarantined" {
		test.Fatal("migration retroactively proved unknown or Open cleanup")
	}
	_, err = registerOperation(reloaded, database, "new_correlation", "gateway", "execute", "tool_rpc", "worker-a", "", nil)
	if !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatal("migration reopened isolated namespace")
	}
}

func TestOperationKnownTerminalEligibilityRequiresProof(test *testing.T) {
	base := OperationRecord{OperationStatus: OperationStatus{State: "terminal", Source: "gateway", Action: "execute", BusinessOutcome: "completed", CleanupOutcome: "not_applicable", ExecutionProven: true}}
	if !operationKnownTerminal(base) {
		test.Fatal("known completed record not eligible")
	}
	for _, category := range []string{"no_proof", "ambiguous", "cleanup_unknown", "cleanup_pending", "business_unknown", "business_not_proven"} {
		record := base
		switch category {
		case "no_proof":
			record.ExecutionProven = false
		case "ambiguous":
			record.Ambiguous, record.ExecutionCompleted = true, true
		case "cleanup_unknown":
			record.CleanupOutcome = "unknown"
		case "cleanup_pending":
			record.CleanupOutcome = "pending"
		case "business_unknown":
			record.BusinessOutcome = "unknown"
		case "business_not_proven":
			record.BusinessOutcome = "not_proven"
		}
		if operationKnownTerminal(record) {
			test.Fatalf("unsafe terminal eligible: %s", category)
		}
	}
	base.Action, base.BusinessOutcome, base.CleanupOutcome = "close", "not_applicable", "completed"
	if !operationKnownTerminal(base) {
		test.Fatal("proven Close ACK cannot be retained forever")
	}
	base.BusinessOutcome = "not_executed"
	if !operationKnownTerminal(base) {
		test.Fatal("known not-executed terminal not eligible")
	}
}
