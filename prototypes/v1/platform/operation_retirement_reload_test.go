package platform

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperationRetirementIncludesAcknowledgedOpenOnReload(test *testing.T) {
	for _, failCommit := range []bool{false, true} {
		test.Run(map[bool]string{false: "released", true: "commit_failure"}[failCommit], func(test *testing.T) {
			store := operationDurableStore(test)
			database, session := securityDatabase(), strings.Repeat("a", 64)
			if err := beginOperation(store, database, "acked_open", "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
				test.Fatal(err)
			}
			if err := updateOperation(store, database, "acked_open", func(record *OperationRecord) {
				record.State, record.BusinessOutcome, record.ExecutionProven, record.CleanupOutcome, record.Session = "terminal", "completed", true, "pending", session
			}); err != nil {
				test.Fatal(err)
			}
			if err := beginOperation(store, database, "lost_close", "gateway", "close", "close_session", "worker-a", session, nil); err != nil {
				test.Fatal(err)
			}
			if err := finishOperation(store, database, "lost_close", &SQLRPCError{Ambiguous: true}, false); err != nil {
				test.Fatal(err)
			}
			if err := store.Update(func(state *State) error {
				record := state.Operations[operationKey(database, "lost_close")]
				record.Decision, record.DecisionRequestID = "retain_isolation_new_database_required", "decision"
				state.Operations[operationKey(database, "lost_close")] = record
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			management, fixture, _, _ := nativeDiagnosticManagement(test)
			config := fixture.gateway.config
			config.State = store
			resources, err := NewGateway(config)
			if err != nil {
				test.Fatal(err)
			}
			management.config.State, management.config.Resources, management.config.StopProof = store, resources, operationStopVerifier{}
			if failCommit {
				management.config.State = retirementFailStore{store}
				resources.config.State = management.config.State
			}
			_, err = management.retireLocal(context.Background(), Principal{}, managementRequest{Database: database, RequestID: "retire"})
			if (err != nil) != failCommit {
				test.Fatalf("unexpected retirement result: %v", err)
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
			config.State = reloaded
			restarted, err := NewGateway(config)
			if err != nil {
				test.Fatal(err)
			}
			state, err := reloaded.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			opened := state.Operations[operationKey(database, "acked_open")]
			if opened.BusinessOutcome != "completed" || !opened.ExecutionProven || opened.Ambiguous || opened.CleanupOutcome != "unknown" || opened.State != "quarantined" || opened.LocalResourcesRetired == failCommit || opened.RetirementPending != failCommit {
				test.Fatalf("Open ACK or retirement proof corrupted: %+v", opened.OperationStatus)
			}
			held := restarted.Metrics()["unknown_held_permits"]
			if !failCommit && held != 0 || failCommit && held != 2 {
				test.Fatalf("incorrect durable quota reconstruction: %v", held)
			}
			if _, blocked := isolatedOperation(state, database, true); !blocked {
				test.Fatal("retired namespace unsealed")
			}
			if knownDatabaseDeletionPermitted(state, database) == nil {
				test.Fatal("retired records became deletable")
			}
		})
	}
}
