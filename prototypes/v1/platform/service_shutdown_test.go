package platform

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// closeBlockingAudit stands in for an audit tail that never finishes, which is
// the shape an unbounded writers.Wait() takes when its file stops making
// progress.
type closeBlockingAudit struct {
	release chan struct{}
}

func (*closeBlockingAudit) Record(AuditEvent) error { return nil }
func (audit *closeBlockingAudit) Close() error {
	<-audit.release
	return nil
}

// Every teardown step runs under one deadline: the process must never wait for
// one of them forever, and a step that did not finish must be reported instead
// of being counted as a clean stop.
func TestPlatformTeardownReportsUncleanStopWithinBudget(test *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ran := false
	started := time.Now()
	err := runServiceTeardown(newShutdownBudget(50*time.Millisecond), []stopStep{
		{name: "blocking", run: func(context.Context) error { <-release; return nil }},
		{name: "after", run: func(context.Context) error { ran = true; return nil }},
	})
	if !errors.Is(err, ErrShutdownIncomplete) || !strings.Contains(err.Error(), "blocking") {
		test.Fatalf("teardown did not report the step that ran out of budget: %v", err)
	}
	if !strings.Contains(err.Error(), "after not attempted") {
		test.Fatalf("teardown did not report the step it never gave a budget to: %v", err)
	}
	if ran {
		test.Fatal("a step ran without any budget left")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		test.Fatalf("teardown waited %v for a step it had abandoned", elapsed)
	}
	// The later step cannot buy a budget of its own: the first step spent the
	// one deadline the whole stop shares.
	shared := make(chan struct{})
	err = runServiceTeardown(newShutdownBudget(50*time.Millisecond), []stopStep{
		{name: "first", run: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		{name: "second", run: func(context.Context) error { close(shared); return nil }},
	})
	if !strings.Contains(err.Error(), "second not attempted") {
		test.Fatalf("a later step received a fresh budget: %v", err)
	}
	select {
	case <-shared:
		test.Fatal("a later step ran after the deadline")
	default:
	}
	// Steps that finish inside the budget stay a clean stop.
	quick := newShutdownBudget(time.Second)
	if err := runServiceTeardown(quick, []stopStep{{name: "quick", run: func(context.Context) error { return nil }}}); err != nil {
		test.Fatalf("bounded teardown reported an unclean stop: %v", err)
	}
}

func TestServiceAuditTailIsBoundedByTheShutdownBudget(test *testing.T) {
	audit := &closeBlockingAudit{release: make(chan struct{})}
	defer close(audit.release)
	started := time.Now()
	err := closeAuditWithin(newShutdownBudget(50*time.Millisecond), audit)
	if !errors.Is(err, ErrShutdownIncomplete) || !strings.Contains(err.Error(), "audit tail") {
		test.Fatalf("audit tail did not report an unclean stop: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		test.Fatalf("audit tail waited %v for an unbounded close", elapsed)
	}
}

// The drain loops while an in-flight handler holds a session lock, so its
// deadline has to be the one that ends it: a stop that waits here is a process
// the orchestrator has to kill, which turns a clean stop into a dirty one.
func TestGatewayShutdownDrainIsBoundedByItsDeadline(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	session := idleSession(fixture, identifier, test)
	session.mu.Lock()
	defer session.mu.Unlock()
	started := time.Now()
	err := fixture.gateway.Close(newShutdownBudget(50 * time.Millisecond).begin())
	if err == nil {
		test.Fatal("drain reported a clean stop with a session it never retired")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		test.Fatalf("drain waited %v for a session it could not lock", elapsed)
	}
	if fixture.upstream.count("close_session") != 0 {
		test.Fatal("drain dispatched a close under a session lock it could not take")
	}
}

// The budget has to stay inside the cap the launcher will stop the process with,
// and the cap has to stay inside the grace the orchestrator enforces around the
// stop. A process still draining when either deadline expires is killed
// mid-teardown, so a budget above the default is only accepted next to a declared
// cap that stays strictly above it, and a cap that reaches the declared grace
// refuses to start instead of reporting a stop the kill can preempt.
func TestServiceShutdownTimeoutStaysBelowTheDeclaredStageCap(test *testing.T) {
	cases := []struct {
		name   string
		budget string
		cap    string
		grace  string
		want   time.Duration
		refuse bool
	}{
		{name: "default", want: defaultServiceShutdownTimeout},
		{name: "lowered", budget: "4000", want: 4 * time.Second},
		{name: "lowered beside a cap", budget: "4000", cap: "12000", want: 4 * time.Second},
		{name: "default beside a larger cap", cap: "12000", want: defaultServiceShutdownTimeout},
		{name: "raised beside a larger cap", budget: "20000", cap: "45000", want: 20 * time.Second},
		{name: "raised without a cap", budget: "20000", refuse: true},
		{name: "raised to the cap", budget: "20000", cap: "20000", refuse: true},
		{name: "raised past the cap", budget: "20000", cap: "15000", refuse: true},
		{name: "cap below the default", cap: "5000", refuse: true},
		{name: "cap at the default", cap: "10000", refuse: true},
		{name: "unusable budget", budget: "999", refuse: true},
		{name: "unbounded budget", budget: "60001", refuse: true},
		{name: "unparsable budget", budget: "10s", refuse: true},
		{name: "unusable cap", budget: "4000", cap: "999", refuse: true},
		{name: "unbounded cap", budget: "4000", cap: "300001", refuse: true},
		{name: "unparsable cap", budget: "4000", cap: "fifteen", refuse: true},
		{name: "budget and cap inside a declared grace", budget: "20000", cap: "30000", grace: "45000", want: 20 * time.Second},
		{name: "default budget inside a declared grace", grace: "45000", want: defaultServiceShutdownTimeout},
		{name: "cap at the declared grace", budget: "20000", cap: "45000", grace: "45000", refuse: true},
		{name: "cap past the declared grace", budget: "45000", cap: "45001", grace: "45000", refuse: true},
		{name: "cap past a grace above the module bound", budget: "60000", cap: "300000", grace: "45000", refuse: true},
		{name: "default budget the declared grace does not cover", grace: "5000", refuse: true},
		{name: "lowered budget inside a declared grace", budget: "4000", grace: "5000", want: 4 * time.Second},
		{name: "lowered budget at the declared grace", budget: "5000", grace: "5000", refuse: true},
		{name: "unusable outer grace", budget: "4000", grace: "999", refuse: true},
		{name: "unparsable outer grace", grace: "45s", refuse: true},
	}
	for _, testCase := range cases {
		test.Run(testCase.name, func(test *testing.T) {
			test.Setenv("PERI_SHUTDOWN_TIMEOUT_MS", testCase.budget)
			test.Setenv("PERI_SHUTDOWN_STAGE_CAP_MS", testCase.cap)
			test.Setenv("PERI_SHUTDOWN_OUTER_GRACE_MS", testCase.grace)
			timeout, err := serviceShutdownTimeout()
			if testCase.refuse {
				if err == nil {
					test.Fatalf("accepted budget %q beside cap %q and grace %q as %v", testCase.budget, testCase.cap, testCase.grace, timeout)
				}
				return
			}
			if err != nil || timeout != testCase.want {
				test.Fatalf("budget %q beside cap %q and grace %q resolved to %v (%v), want %v", testCase.budget, testCase.cap, testCase.grace, timeout, err, testCase.want)
			}
		})
	}
}

// The shutdown checkpoint is what makes payload.operations the durable truth a
// launcher reads, and its marker is what separates that final view from the
// stale snapshot any earlier registry commit leaves behind.
func TestOperationCheckpointMaterializesJoinedView(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant_a", Database: "db"}, Branch: "main"}
	session := strings.Repeat("a", 64)
	if err := store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "test"}
		state.Projects["alpha"] = Project{ID: "alpha", Organization: "org", Tenant: "tenant_a"}
		// An active database without a sealed key descriptor: this test is about
		// the operation payload, not about key binding.
		state.Databases[database.Key()] = Database{ID: database, Project: "alpha", State: "provisioning"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	// One known terminal cleanup and one unresolved operation stay in the
	// sidecar only: no registry commit carries them yet.
	if err := beginOperation(store, database, "cleanup-1", "gateway", "close", "close_session", "worker-a", session, map[string]any{"command": "close_session", "session": session}); err != nil {
		test.Fatal(err)
	}
	if err := updateOperation(store, database, "cleanup-1", func(record *OperationRecord) {
		record.State, record.BusinessOutcome, record.CleanupOutcome, record.ExecutionProven = "terminal", "not_applicable", "completed", true
	}); err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "execute-2", "gateway", "execute", "tool_rpc", "worker-a", session, map[string]any{"command": "execute", "session": session, "sql": "INSERT INTO records VALUES (1)"}); err != nil {
		test.Fatal(err)
	}
	if err := updateOperation(store, database, "execute-2", func(record *OperationRecord) {
		record.State, record.BusinessOutcome, record.MayHaveCommitted, record.Ambiguous = "quarantined", "unknown", true, true
	}); err != nil {
		test.Fatal(err)
	}
	joined, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if len(joined.Operations) != 2 {
		test.Fatalf("joined view lost an operation: %d", len(joined.Operations))
	}
	stale := durableStatePayload(directory, test)
	if !reflect.DeepEqual(stale.Operations, joined.Operations) && len(stale.Operations) != 0 {
		test.Fatalf("registry payload unexpectedly carries the sidecar view: %d", len(stale.Operations))
	}
	if operationsCheckpointMaterialized(stale) {
		test.Fatal("a payload that trails the sidecar claimed to be a final view")
	}
	if err := CheckpointOperations(store); err != nil {
		test.Fatal(err)
	}
	final := durableStatePayload(directory, test)
	if !operationsCheckpointMaterialized(final) || final.OperationsCheckpoint != final.Revision {
		test.Fatalf("checkpoint did not name its own revision: %+v", final)
	}
	if !reflect.DeepEqual(final.Operations, joined.Operations) {
		test.Fatalf("checkpoint did not materialize the joined view: %d vs %d", len(final.Operations), len(joined.Operations))
	}
	frozen, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(final.Operations, frozen.Operations) {
		test.Fatal("checkpoint payload and store view disagree")
	}
	// Nothing may write after the checkpoint: a later operation would silently
	// take back the final view the payload claims.
	if err := store.Update(func(state *State) error { return nil }); !errors.Is(err, ErrStateFrozen) {
		test.Fatalf("registry accepted a write after the checkpoint: %v", err)
	}
	if err := updateOperationRecords(store, nil); !errors.Is(err, ErrStateFrozen) {
		test.Fatalf("operations accepted a write after the checkpoint: %v", err)
	}
	if err := CheckpointOperations(store); err == nil {
		test.Fatal("checkpoint was recorded twice")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	// Recovery joins the sidecar into exactly the payload the checkpoint wrote,
	// and any registry commit of the next run leaves the marker behind again.
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(recovered.Operations, final.Operations) {
		test.Fatalf("recovery did not join the checkpointed view")
	}
	if err := reopened.Update(func(state *State) error { return nil }); err != nil {
		test.Fatal(err)
	}
	next := durableStatePayload(directory, test)
	if operationsCheckpointMaterialized(next) || next.OperationsCheckpoint != final.OperationsCheckpoint {
		test.Fatalf("a later commit still claimed to be the final view: %+v", next)
	}
}

// A checkpoint that cannot commit leaves no final view and no clean claim: the
// launcher has to be able to see the stop as unclean.
func TestOperationCheckpointFailureLeavesNoFinalView(test *testing.T) {
	store, _, _, _, _ := securityGatewayFixture(test)
	durable, ok := store.(*stateStore)
	if !ok {
		test.Fatalf("fixture did not use the durable store: %T", store)
	}
	durable.disk.poisoned = true
	err := runServiceTeardown(newShutdownBudget(time.Second), []stopStep{
		{name: "operation checkpoint", run: func(context.Context) error { return CheckpointOperations(store) }},
	})
	if err == nil || !strings.Contains(err.Error(), "operation checkpoint") {
		test.Fatalf("an uncommittable checkpoint was reported as a clean stop: %v", err)
	}
	if !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("checkpoint failure lost its durable cause: %v", err)
	}
}

func TestOperationCheckpointValidation(test *testing.T) {
	store := securityState(test)
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	ahead := state
	ahead.Revision, ahead.OperationsCheckpoint = 4, 5
	if err := validateState(ahead, nil); err == nil || !strings.Contains(err.Error(), "operation checkpoint") {
		test.Fatalf("state accepted a checkpoint ahead of its revision: %v", err)
	}
	previous := state
	previous.Revision, previous.OperationsCheckpoint = 4, 4
	next := previous
	next.Revision = 5
	if err := validateState(next, &previous); err != nil {
		test.Fatalf("state refused a checkpoint left behind by a later commit: %v", err)
	}
	rewound := next
	rewound.OperationsCheckpoint = 3
	if err := validateState(rewound, &previous); err == nil || !strings.Contains(err.Error(), "operation checkpoint") {
		test.Fatalf("state accepted a rewound checkpoint: %v", err)
	}
}

// durableStatePayload reads the payload a launcher reads: state.json on its own,
// without the operation sidecar.
func durableStatePayload(directory string, test *testing.T) State {
	test.Helper()
	var envelope diskEnvelope
	if err := readDurableEnvelope(directory+"/state.json", &envelope); err != nil {
		test.Fatal(err)
	}
	var state State
	if err := strictDecode(envelope.Payload, &state); err != nil {
		test.Fatal(err)
	}
	return state
}
