package platform

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type retirementFailStore struct{ StateStore }

func (store retirementFailStore) Update(change func(*State) error) error {
	return store.StateStore.Update(func(state *State) error {
		if err := change(state); err != nil {
			return err
		}
		for _, record := range state.Operations {
			if record.LocalResourcesRetired {
				return errors.New("retirement commit unavailable")
			}
		}
		return nil
	})
}

func retirementInterleavingFixture(test *testing.T) (*Management, *gatewayFixture) {
	test.Helper()
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	if err := beginOperation(fixture.state, fixture.first, "lost", "gateway", "open", "open_session", "worker-tenant-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.state, fixture.first, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if err := fixture.state.Update(func(state *State) error {
		record := state.Operations[operationKey(fixture.first, "lost")]
		record.Decision, record.DecisionRequestID = "retain_isolation_new_database_required", "decision"
		state.Operations[operationKey(fixture.first, "lost")] = record
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	resources, err := NewGateway(fixture.gateway.config)
	if err != nil {
		test.Fatal(err)
	}
	management.config.Resources, management.config.StopProof = resources, operationStopVerifier{}
	return management, fixture
}

func TestOperationRetirementFailureKeepsDurableAdmission(test *testing.T) {
	for _, scenario := range []string{"locked_session", "canceled", "commit_failure"} {
		test.Run(scenario, func(test *testing.T) {
			management, fixture := retirementInterleavingFixture(test)
			resources := management.config.Resources
			ctx := context.Background()
			if scenario == "locked_session" {
				session := &gatewaySession{public: "held", database: fixture.first, worker: controller.Worker{ID: "worker-tenant-a"}, project: "project-a"}
				session.mu.Lock()
				defer session.mu.Unlock()
				resources.sessions[session.public] = session
			}
			if scenario == "canceled" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if scenario == "commit_failure" {
				management.config.State = retirementFailStore{fixture.state}
				resources.config.State = management.config.State
			}
			if _, err := management.retireLocal(ctx, Principal{}, managementRequest{Database: fixture.first, RequestID: "retire"}); err == nil {
				test.Fatal("failed release marked successful")
			}
			state, _ := fixture.state.Snapshot()
			record := state.Operations[operationKey(fixture.first, "lost")]
			if record.LocalResourcesRetired || !record.RetirementPending || resources.Metrics()["unknown_held_permits"] != 1 {
				test.Fatal("failed retirement released durable or memory admission")
			}
			reloaded, err := NewGateway(fixture.gateway.config)
			if err != nil || reloaded.Metrics()["unknown_held_permits"] != 1 {
				test.Fatal("restart falsely skipped failed retirement")
			}
		})
	}
}

func TestOperationPendingRetirementCrashReloadRetainsAdmission(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "lost", "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		record := state.Operations[operationKey(database, "lost")]
		record.Decision, record.DecisionRequestID, record.RetirementPending = "retain_isolation_new_database_required", "decision", true
		state.Operations[operationKey(database, "lost")] = record
		return nil
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
	fixture := newGatewayFixture(test)
	config := fixture.gateway.config
	config.State = reloaded
	resources, err := NewGateway(config)
	if err != nil || resources.Metrics()["unknown_held_permits"] != 1 {
		test.Fatal("pending retirement crash erased conservative quota")
	}
	state, err := reloaded.Snapshot()
	if err != nil || state.Operations[operationKey(database, "lost")].LocalResourcesRetired {
		test.Fatal("pending retirement became fake terminal release")
	}
}

func TestOperationLateHoldAfterRetirementCannotLeakPermit(test *testing.T) {
	management, fixture := retirementInterleavingFixture(test)
	management.config.Resources = fixture.gateway
	state, _ := fixture.state.Snapshot()
	permit, reason := fixture.gateway.admission.reserve("project-a", state.Projects["project-a"].Quotas, true, false)
	if reason != rejectionNone {
		test.Fatal("permit unavailable")
	}
	permit.retain()
	if _, err := management.retireLocal(context.Background(), Principal{}, managementRequest{Database: fixture.first, RequestID: "retire"}); err != nil {
		test.Fatal(err)
	}
	fixture.gateway.holdOperation(fixture.first, "lost", permit)
	permit.finish()
	if fixture.gateway.Metrics()["unknown_held_permits"] != 0 || len(fixture.gateway.operationHolds) != 0 {
		test.Fatal("late hold leaked retired admission")
	}
	fixture.gateway.admission.mu.Lock()
	defer fixture.gateway.admission.mu.Unlock()
	project := fixture.gateway.admission.projects["project-a"]
	if project.active != 0 || project.sessions != 0 || project.unknown != 0 {
		test.Fatal("late deferred finish double released permit")
	}
}

type snapshotPauseStore struct {
	StateStore
	entered chan struct{}
	resume  chan struct{}
}

func (store *snapshotPauseStore) Snapshot() (State, error) {
	state, err := store.StateStore.Snapshot()
	if store.entered != nil {
		close(store.entered)
		<-store.resume
	}
	return state, err
}

type authorityPauseTransport struct {
	controller.ControlTransport
	entered chan struct{}
	resume  chan struct{}
	calls   int
}

func (transport *authorityPauseTransport) Authority(context.Context, string, any, any) error {
	transport.calls++
	close(transport.entered)
	<-transport.resume
	return nil
}

func operationAwait(test *testing.T, signal <-chan struct{}) {
	test.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		test.Fatal("deterministic interleaving deadline exceeded")
	}
}

func TestOperationGrantCheckAndDispatchShareIsolationFence(test *testing.T) {
	fixture := newGatewayFixture(test)
	store := &snapshotPauseStore{StateStore: fixture.state, entered: make(chan struct{}), resume: make(chan struct{})}
	native := &authorityPauseTransport{entered: make(chan struct{}), resume: make(chan struct{})}
	transport := &isolatedControlTransport{store: store, ControlTransport: native}
	intent, _ := json.Marshal(map[string]any{"database": fixture.first})
	grant := map[string]any{"command": "grant", "request": map[string]any{"intent": json.RawMessage(intent)}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := transport.Authority(context.Background(), "grant", grant, nil); err != nil {
			test.Error(err)
		}
	}()
	operationAwait(test, store.entered)
	digest := sha256.Sum256([]byte(namespaceKey(fixture.first.Namespace)))
	fence := &operationNamespaceFences[int(digest[0])%len(operationNamespaceFences)]
	if fence.TryLock() {
		fence.Unlock()
		test.Fatal("check-to-send window lacks namespace fence")
	}
	registered := make(chan struct{})
	go func() {
		defer close(registered)
		if err := beginOperation(fixture.state, fixture.first, "lost", "gateway", "open", "open_session", "worker-tenant-a", "", nil); err != nil {
			test.Error(err)
		}
	}()
	close(store.resume)
	operationAwait(test, native.entered)
	if fence.TryLock() {
		fence.Unlock()
		test.Fatal("isolation fence released before actual transport completed")
	}
	close(native.resume)
	operationAwait(test, done)
	operationAwait(test, registered)
	store.entered = nil
	if err := finishOperation(fixture.state, fixture.first, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if err := transport.Authority(context.Background(), "after_unknown", grant, nil); !errors.Is(err, ErrDatabaseQuarantined) || native.calls != 1 {
		test.Fatal("quarantined grant dispatched")
	}
}
