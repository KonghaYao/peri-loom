package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func s02BoolPointer(value bool) *bool { return &value }

func s02MarkWorkerUnloaded(test *testing.T, fixture *reconcileFixture, placement Placement) {
	test.Helper()
	actual, exists := fixture.fake.workers[placement.DesiredWorker][placement.Database.Key()]
	if !exists {
		test.Fatalf("worker %s never observed %s", placement.DesiredWorker, placement.Database.Key())
	}
	actual.Resident = s02BoolPointer(false)
	fixture.fake.workers[placement.DesiredWorker][placement.Database.Key()] = actual
}

// s02IdleDatabase places one database, then reports the exactly observed state a
// worker leaves behind after its idle sweep: ownership context still present,
// no resident instance.
func s02IdleDatabase(test *testing.T, fixture *reconcileFixture, name string) Placement {
	test.Helper()
	database := reconcileDatabase(name)
	placement, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	s02MarkWorkerUnloaded(test, fixture, placement)
	return placement
}

func TestIdleReleaseRevokesLeaseAndLeavesRenewalSchedule(test *testing.T) {
	fixture := newReconcileFixture(test)
	placed := s02IdleDatabase(test, fixture, "idle-release")
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	placement, err := fixture.controller.Status(placed.Database)
	if err != nil {
		test.Fatal(err)
	}
	if placement.Phase != "idle" || placement.SuspendReason != SuspendReasonIdle || placement.WakePolicy != WakePolicyAuto || placement.DesiredState != "active" {
		test.Fatalf("idle release did not settle: %+v", placement)
	}
	if fixture.fake.calls["revoke_database"] != 1 || fixture.fake.calls["revoke"] != 1 {
		test.Fatalf("scope or lease was not released: calls=%v", fixture.fake.calls)
	}
	// The next pass must not renew or re-grant an idle database.
	before := map[string]int{}
	for _, command := range []string{"grant", "renew", "revoke", "revoke_database"} {
		before[command] = fixture.fake.calls[command]
	}
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	for command, count := range before {
		if fixture.fake.calls[command] != count {
			test.Fatalf("idle database stayed in the active schedule: %s %d -> %d", command, count, fixture.fake.calls[command])
		}
	}
	// Idle placements leave the pass view but stay readable.
	metadata, err := readReconcileView(fixture.store)
	if err != nil {
		test.Fatal(err)
	}
	if _, scheduled := metadata.Placements[placement.Database.Key()]; scheduled {
		test.Fatal("idle placement was still scheduled for reconcile")
	}
}

func TestIdleMatrixLeavesHotDatabaseRenewing(test *testing.T) {
	fixture := newReconcileFixture(test)
	var idle []Placement
	for _, name := range []string{"idle-a", "idle-b", "idle-c", "idle-d", "idle-e", "idle-f"} {
		idle = append(idle, s02IdleDatabase(test, fixture, name))
	}
	hotDatabase := reconcileDatabase("hot")
	hot, err := fixture.controller.Place(context.Background(), hotDatabase, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	authority := fixture.fake.authority[hotDatabase.Key()]
	authority.RemainingTTLMS = 100
	fixture.fake.authority[hotDatabase.Key()] = authority
	grants := fixture.fake.calls["grant"]
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	for _, placed := range idle {
		placement, statusErr := fixture.controller.Status(placed.Database)
		if statusErr != nil {
			test.Fatal(statusErr)
		}
		if placement.Phase != "idle" {
			test.Fatalf("idle database %s remained %s", placed.Database.Key(), placement.Phase)
		}
	}
	current, err := fixture.controller.Status(hot.Database)
	if err != nil {
		test.Fatal(err)
	}
	if current.Phase != "active" || current.DesiredWorker != hot.DesiredWorker {
		test.Fatalf("hot database was disturbed: %+v", current)
	}
	if fixture.fake.calls["renew"] != 1 || fixture.fake.calls["renew_lease"] != 1 {
		test.Fatalf("hot database did not renew exactly once: calls=%v", fixture.fake.calls)
	}
	if fixture.fake.calls["grant"] != grants {
		test.Fatalf("idle databases allocated new grants: calls=%v", fixture.fake.calls)
	}
}

// s02BlockingAcquire holds the exact RPC that performs the cold load, so a test
// can observe callers merging before the activation finishes.
type s02BlockingAcquire struct {
	*reconcileFake
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func s02NewBlockingAcquire(fake *reconcileFake) *s02BlockingAcquire {
	return &s02BlockingAcquire{reconcileFake: fake, entered: make(chan struct{}), release: make(chan struct{})}
}

func (transport *s02BlockingAcquire) Worker(ctx context.Context, worker Worker, requestID string, input, output any) error {
	var action workerAction
	if err := reconcileDecode(input, &action); err != nil {
		return err
	}
	if action.Command == "acquire_database" {
		transport.once.Do(func() { close(transport.entered) })
		select {
		case <-transport.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return transport.reconcileFake.Worker(ctx, worker, requestID, input, output)
}

func TestConcurrentWakesShareOneActivation(test *testing.T) {
	fixture := newReconcileFixture(test)
	placed := s02IdleDatabase(test, fixture, "wake-single-flight")
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	transport := s02NewBlockingAcquire(fixture.fake)
	control, err := NewController(fixture.store, transport, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	grants := fixture.fake.calls["grant"]
	acquires := fixture.fake.calls["acquire_database"]
	const waiters = 100
	type outcome struct {
		status ActivationStatus
		err    error
	}
	results := make(chan outcome, waiters)
	var group sync.WaitGroup
	for range waiters {
		group.Add(1)
		go func() {
			defer group.Done()
			status, err := control.Wake(context.Background(), placed.Database)
			results <- outcome{status, err}
		}()
	}
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		test.Fatal("activation never reached the worker acquire")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		control.wakeMutex.Lock()
		call := control.wakes[placed.Database.Key()]
		control.wakeMutex.Unlock()
		if call != nil {
			call.mutex.Lock()
			joined := call.waiters
			call.mutex.Unlock()
			if joined == waiters {
				break
			}
		}
		if time.Now().After(deadline) {
			test.Fatalf("waiters did not merge onto one activation: %d", waiters)
		}
		time.Sleep(time.Millisecond)
	}
	close(transport.release)
	group.Wait()
	close(results)
	operationID := ""
	for result := range results {
		if result.err != nil || result.status.State != ActivationActive {
			test.Fatalf("wake failed: %+v %v", result.status, result.err)
		}
		if operationID == "" {
			operationID = result.status.OperationID
		}
		if result.status.OperationID != operationID {
			test.Fatalf("waiters observed different activation identities: %s != %s", result.status.OperationID, operationID)
		}
	}
	if fixture.fake.calls["grant"] != grants+1 || fixture.fake.calls["acquire_database"] != acquires+1 {
		test.Fatalf("concurrent wakes started more than one activation: calls=%v", fixture.fake.calls)
	}
	placement, err := control.Status(placed.Database)
	if err != nil {
		test.Fatal(err)
	}
	if placement.Phase != "active" || placement.SuspendReason != "" || placement.WakePolicy != "" {
		test.Fatalf("wake did not settle active: %+v", placement)
	}
}

func TestCancelledWaitKeepsActivationAndEpoch(test *testing.T) {
	fixture := newReconcileFixture(test)
	placed := s02IdleDatabase(test, fixture, "wake-cancel")
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	transport := s02NewBlockingAcquire(fixture.fake)
	control, err := NewController(fixture.store, transport, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	grants := fixture.fake.calls["grant"]
	ctx, cancel := context.WithCancel(context.Background())
	status, wakeErr := func() (ActivationStatus, error) {
		go func() {
			<-transport.entered
			cancel()
		}()
		return control.Wake(ctx, placed.Database)
	}()
	if !errors.Is(wakeErr, context.Canceled) || status.OperationID == "" || status.State != ActivationWaking {
		test.Fatalf("cancelled waiter did not keep a loading identity: %+v %v", status, wakeErr)
	}
	close(transport.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		placement, statusErr := control.Status(placed.Database)
		if statusErr != nil {
			test.Fatal(statusErr)
		}
		if placement.Phase == "active" {
			break
		}
		if time.Now().After(deadline) {
			test.Fatalf("activation did not finish after the waiter left: %+v", placement)
		}
		time.Sleep(5 * time.Millisecond)
	}
	active, err := control.Wake(context.Background(), placed.Database)
	if err != nil || active.State != ActivationActive {
		test.Fatalf("retry after cancellation failed: %+v %v", active, err)
	}
	if fixture.fake.calls["grant"] != grants+1 {
		test.Fatalf("cancelled wait repeated the activation: calls=%v", fixture.fake.calls)
	}
}

func TestResumeRacingWakeMintsOneWriterGrant(test *testing.T) {
	fixture := newReconcileFixture(test)
	placed := s02IdleDatabase(test, fixture, "wake-resume-race")
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	transport := s02NewBlockingAcquire(fixture.fake)
	control, err := NewController(fixture.store, transport, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	grants := fixture.fake.calls["grant"]
	acquires := fixture.fake.calls["acquire_database"]
	wakeDone := make(chan error, 1)
	go func() {
		_, wakeErr := control.Wake(context.Background(), placed.Database)
		wakeDone <- wakeErr
	}()
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		test.Fatal("wake never reached acquire")
	}
	resumeDone := make(chan error, 1)
	go func() {
		_, resumeErr := control.Resume(context.Background(), placed.Database)
		resumeDone <- resumeErr
	}()
	close(transport.release)
	if err := <-wakeDone; err != nil {
		test.Fatal(err)
	}
	if err := <-resumeDone; err != nil {
		test.Fatal(err)
	}
	placement, err := control.Status(placed.Database)
	if err != nil {
		test.Fatal(err)
	}
	if placement.Phase != "active" {
		test.Fatalf("wake and resume did not settle active: %+v", placement)
	}
	if fixture.fake.calls["grant"] != grants+1 || fixture.fake.calls["acquire_database"] != acquires+1 {
		test.Fatalf("manager resume and auto wake created competing writers: calls=%v", fixture.fake.calls)
	}
}

func TestAdministratorSuspensionIsNotWokenByRequests(test *testing.T) {
	fixture := newReconcileFixture(test)
	placed := s02IdleDatabase(test, fixture, "admin-suspend")
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	if err := fixture.controller.Suspend(context.Background(), placed.Database); err != nil {
		test.Fatal(err)
	}
	placement, err := fixture.controller.Status(placed.Database)
	if err != nil {
		test.Fatal(err)
	}
	if placement.Phase != "suspended" || placement.SuspendReason != SuspendReasonAdmin || placement.WakePolicy != WakePolicyManual {
		test.Fatalf("administrator suspension was not durable: %+v", placement)
	}
	grants := fixture.fake.calls["grant"]
	if _, err := fixture.controller.Wake(context.Background(), placed.Database); !errors.Is(err, ErrActivationRefused) {
		test.Fatalf("request woke an administrator suspension: %v", err)
	}
	if fixture.fake.calls["grant"] != grants {
		test.Fatalf("administrator suspension allocated a grant: calls=%v", fixture.fake.calls)
	}
}
