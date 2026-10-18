package controller

import (
	"context"
	"errors"
	"testing"
	"time"
)

type delayedHeartbeat struct {
	ControlTransport
	entered chan struct{}
}

func (transport delayedHeartbeat) Worker(ctx context.Context, worker Worker, requestID string, input, output any) error {
	if worker.ID == "worker-a" {
		close(transport.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return transport.ControlTransport.Worker(ctx, worker, requestID, input, output)
}

func TestPollWorkersSlowFirstWorkerDoesNotDelayHealthyPeer(test *testing.T) {
	fixture := newReconcileFixture(test)
	transport := delayedHeartbeat{ControlTransport: fixture.fake, entered: make(chan struct{})}
	control, err := NewController(fixture.store, transport, ReconcileConfig{LeaseTTL: time.Second, HeartbeatTimeout: 2 * time.Second, Interval: time.Millisecond})
	if err != nil {
		test.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- control.pollWorkers(context.Background()) }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		test.Fatal("slow worker heartbeat was not started")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		metadata, err := fixture.store.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		if metadata.Workers["worker-b"].Healthy {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("healthy worker was held behind slow worker")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-finished:
		test.Fatal("slow worker probe ended before its deadline")
	default:
	}
	select {
	case err := <-finished:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("slow heartbeat exceeded per-worker deadline")
	}
	metadata, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if metadata.Workers["worker-a"].Healthy || !metadata.Workers["worker-b"].Healthy {
		test.Fatalf("incorrect independent heartbeat state: %+v", metadata.Workers)
	}
}

func TestPollWorkersUsesNonSamplingHeartbeat(test *testing.T) {
	fixture := newReconcileFixture(test)
	if err := fixture.controller.pollWorkers(context.Background()); err != nil {
		test.Fatal(err)
	}
	if fixture.fake.calls["get_worker_heartbeat"] != 2 || fixture.fake.calls["get_worker_state"] != 0 {
		test.Fatalf("controller sampled expensive worker status for heartbeat: %+v", fixture.fake.calls)
	}
	metadata, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, worker := range metadata.Workers {
		if !worker.Healthy {
			test.Fatal("heartbeat incorrectly marked worker unhealthy")
		}
	}
}

func TestRefreshWorkersProbesOnlyRequestedActiveWorker(test *testing.T) {
	fixture := newReconcileFixture(test)
	before, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if err := fixture.controller.RefreshWorkers(context.Background(), "worker-a"); err != nil {
		test.Fatal(err)
	}
	after, err := fixture.store.Snapshot()
	if err != nil || after.Revision != before.Revision || !after.Workers["worker-a"].Healthy || after.Workers["worker-b"].Healthy || fixture.fake.calls["get_worker_heartbeat"] != 1 {
		test.Fatal("startup probe touched unrelated Worker or persisted pure heartbeat")
	}
}

func TestReconcileLostGrantACKReplaysExactDurableReceiptAfterRestart(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("grant-ack")
	fixture.fake.lost["grant"] = 1
	placement, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err == nil || placement.Phase != "grant_pending" {
		test.Fatalf("lost ACK must retain pending grant: %+v %v", placement, err)
	}
	before, err := fixture.controller.Status(database)
	if err != nil {
		test.Fatal(err)
	}
	fixture.restart(test)
	fixture.fake.trace = nil
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, err := fixture.controller.Status(database)
	if err != nil {
		test.Fatal(err)
	}
	if after.Phase != "active" || after.Epoch != before.Epoch || after.LeaseID != before.LeaseID || len(fixture.fake.receipts) != 1 || fixture.fake.calls["grant"] != 2 {
		test.Fatalf("receipt replay duplicated grant: %+v", after)
	}
	firstAuthority := ""
	for _, operation := range fixture.fake.trace {
		if operation == "grant" || operation == "get_database_state" || operation == "challenge" {
			firstAuthority = operation
			break
		}
	}
	if firstAuthority != "grant" {
		test.Fatalf("readback preceded exact pending receipt replay: %v", fixture.fake.trace)
	}
}

func TestReconcileWorkerAcquireLostACKDoesNotAcquireTwice(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("acquire-ack")
	fixture.fake.lost["acquire_database"] = 1
	placement, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err == nil || placement.Phase != "acquire_pending" {
		test.Fatalf("acquire ACK loss not retained: %+v %v", placement, err)
	}
	fixture.restart(test)
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	placement, err = fixture.controller.Status(database)
	if err != nil || placement.Phase != "active" || fixture.fake.calls["acquire_database"] != 1 {
		test.Fatalf("duplicate worker acquire: %+v %v", placement, err)
	}
}

func TestReconcileEpochIsGlobalAndExceedsAuthorityFloor(test *testing.T) {
	fixture := newReconcileFixture(test)
	first := reconcileDatabase("floor")
	fixture.fake.authority[first.Key()] = Actual{Database: first, Epoch: 80, Mode: "revoked"}
	placement, err := fixture.controller.Place(context.Background(), first, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	if placement.Epoch != 81 {
		test.Fatalf("authority floor not exceeded: %d", placement.Epoch)
	}
	fixture.restart(test)
	second, err := fixture.controller.Place(context.Background(), reconcileDatabase("another"), "local", "read_only")
	if err != nil {
		test.Fatal(err)
	}
	if second.Epoch != 82 || second.Grant.Binding.Role != "read_only" {
		test.Fatalf("epoch or role lost across restart: %+v", second)
	}
}

func TestReconcileAutomaticFailoverFencesBeforeNewAcquireAndOldWorkerRejoin(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("failover")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	fixture.fake.offline[before.DesiredWorker] = true
	fixture.fake.trace = nil
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, err := fixture.controller.Status(database)
	if err != nil {
		test.Fatal(err)
	}
	if after.Epoch <= before.Epoch || after.DesiredWorker == before.DesiredWorker || after.Phase != "active" {
		test.Fatalf("failover did not converge: %+v", after)
	}
	grantIndex, acquireIndex := -1, -1
	for index, operation := range fixture.fake.trace {
		if operation == "grant" {
			grantIndex = index
		}
		if operation == after.DesiredWorker+":acquire_database" {
			acquireIndex = index
		}
	}
	if grantIndex < 0 || acquireIndex <= grantIndex {
		test.Fatalf("acquire preceded root fence: %v", fixture.fake.trace)
	}
	fixture.fake.offline[before.DesiredWorker] = false
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	old := fixture.fake.workers[before.DesiredWorker][database.Key()]
	root := fixture.fake.authority[database.Key()]
	if old.Binding == nil || root.Binding == nil || old.Binding.Epoch >= root.Binding.Epoch {
		test.Fatal("revived old worker was not fenced")
	}
	current, _ := fixture.controller.Status(database)
	if current.Epoch != after.Epoch || current.DesiredWorker != after.DesiredWorker {
		test.Fatal("old worker rejoin stole placement")
	}
}

func TestReconcileGracefulMigrationWaitsForNativeDetach(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("drain")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	fixture.fake.busy[before.DesiredWorker] = true
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = fixture.controller.Migrate(ctx, database, "worker-b")
	if err == nil {
		test.Fatal("active native transaction did not block migration")
	}
	pending, err := fixture.controller.Status(database)
	if err != nil {
		test.Fatal(err)
	}
	if pending.Phase != "migrate_drain" || fixture.fake.authority[database.Key()].Epoch != before.Epoch || fixture.fake.calls["grant"] != 1 {
		test.Fatalf("granted before old native detach: %+v", pending)
	}
	fixture.restart(test)
	fixture.fake.busy[before.DesiredWorker] = false
	old := fixture.fake.workers[before.DesiredWorker][database.Key()]
	old.Mode = "detached"
	old.RemainingTTLMS = 0
	fixture.fake.workers[before.DesiredWorker][database.Key()] = old
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, _ := fixture.controller.Status(database)
	if after.Phase != "active" || after.Epoch <= before.Epoch || after.DesiredWorker != "worker-b" {
		test.Fatalf("detached migration failed: %+v", after)
	}
}

func TestReconcileSuspendReceiptAndResumeAllocateNewEpoch(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("suspend")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	fixture.fake.lost["revoke"] = 1
	if err := fixture.controller.Suspend(context.Background(), database); err == nil {
		test.Fatal("lost revoke ACK ignored")
	}
	pending, _ := fixture.controller.Status(database)
	if pending.Phase != "revoke_pending" {
		test.Fatalf("pending revoke missing: %+v", pending)
	}
	fixture.restart(test)
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	suspended, _ := fixture.controller.Status(database)
	if suspended.Phase != "suspended" || suspended.DesiredState != "suspended" {
		test.Fatal("suspension did not settle")
	}
	after, err := fixture.controller.Resume(context.Background(), database)
	if err != nil || after.Epoch <= before.Epoch || after.Phase != "active" {
		test.Fatalf("resume failed: %+v %v", after, err)
	}
}

func TestReconcileRenewalReplaysAuthorityAndObservesWorkerBeforeDuplicateSequence(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("renew")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	actual := fixture.fake.authority[database.Key()]
	actual.RemainingTTLMS = 400
	fixture.fake.authority[database.Key()] = actual
	fixture.fake.lost["renew"] = 1
	if err := fixture.controller.Reconcile(context.Background()); err == nil {
		test.Fatal("lost renewal ACK ignored")
	}
	fixture.restart(test)
	fixture.fake.lost["renew_lease"] = 1
	if err := fixture.controller.Reconcile(context.Background()); err == nil {
		test.Fatal("lost worker renewal ACK ignored")
	}
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, _ := fixture.controller.Status(database)
	if after.Epoch != before.Epoch || after.Grant.Binding.RenewalSequence != 1 || after.Phase != "active" || fixture.fake.calls["renew"] != 2 || fixture.fake.calls["renew_lease"] != 1 {
		test.Fatalf("renewal duplicated or failed: %+v calls=%v", after, fixture.fake.calls)
	}
}

type renewalStateFailure struct {
	ControlTransport
}

func (transport renewalStateFailure) Authority(ctx context.Context, identifier string, input, output any) error {
	var action authorityAction
	if err := reconcileDecode(input, &action); err != nil {
		return err
	}
	if action.Command == "get_database_state" {
		return errors.New("authority state unavailable")
	}
	return transport.ControlTransport.Authority(ctx, identifier, input, output)
}

func TestReconcileExpiredPendingRenewalPlansFreshGrant(test *testing.T) {
	for _, receipt := range []bool{false, true} {
		name := "rejected"
		if receipt {
			name = "expired-receipt"
		}
		test.Run(name, func(test *testing.T) {
			fixture := newReconcileFixture(test)
			database := reconcileDatabase(name)
			before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
			if err != nil {
				test.Fatal(err)
			}
			actual := fixture.fake.authority[database.Key()]
			actual.RemainingTTLMS = 400
			fixture.fake.authority[database.Key()] = actual
			fixture.fake.lost["renew"] = 1
			if err := fixture.controller.Reconcile(context.Background()); err == nil {
				test.Fatal("renewal ACK loss ignored")
			}
			pending, err := fixture.controller.Status(database)
			if err != nil || pending.Phase != "renew_pending" {
				test.Fatalf("renewal not durable: %+v %v", pending, err)
			}
			if receipt {
				grant := fixture.fake.receipts[pending.PendingID]
				grant.RemainingTTLMS = 0
				fixture.fake.receipts[pending.PendingID] = grant
			} else {
				delete(fixture.fake.receipts, pending.PendingID)
				delete(fixture.fake.payloads, pending.PendingID)
			}
			fixture.fake.authority[database.Key()] = Actual{Database: database, Mode: "revoked", Epoch: before.Epoch}
			fixture.restart(test)
			fixture.fake.trace = nil
			if err := fixture.controller.Reconcile(context.Background()); err != nil {
				test.Fatal(err)
			}
			after, err := fixture.controller.Status(database)
			if err != nil || after.Phase != "active" || after.Epoch <= before.Epoch || after.LeaseID == before.LeaseID || after.Grant.Binding.GrantID == pending.PendingID {
				test.Fatalf("fresh grant not confirmed: %+v %v", after, err)
			}
			trace := fixture.fake.trace
			if len(trace) < 5 || trace[2] != "renew" || trace[3] != "get_database_state" || trace[4] != "get_database_state" {
				test.Fatalf("pending replay and verified state must precede replanning: %v", fixture.fake.trace)
			}
		})
	}
}

func TestReconcileExpiredPendingGrantRequiresProvenRevocation(test *testing.T) {
	for _, mode := range []string{"revoked", "unknown"} {
		test.Run(mode, func(test *testing.T) {
			fixture := newReconcileFixture(test)
			database := reconcileDatabase("expired-grant-" + mode)
			fixture.fake.lost["grant"] = 1
			if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err == nil {
				test.Fatal("lost grant acknowledgment ignored")
			}
			pending, err := fixture.controller.Status(database)
			if err != nil || pending.Phase != "grant_pending" {
				test.Fatalf("grant intent was not durable: %+v %v", pending, err)
			}
			grant := fixture.fake.receipts[pending.PendingID]
			grant.RemainingTTLMS = 0
			fixture.fake.receipts[pending.PendingID] = grant
			fixture.fake.authority[database.Key()] = Actual{Database: database, Mode: mode, Epoch: pending.Epoch}
			fixture.restart(test)
			err = fixture.controller.Reconcile(context.Background())
			result, statusErr := fixture.controller.Status(database)
			if mode == "unknown" {
				if err == nil || statusErr != nil || result.Phase != "grant_pending" || result.PendingID != pending.PendingID || result.Epoch != pending.Epoch {
					test.Fatalf("unproven grant expiry progressed: %+v %v", result, err)
				}
				return
			}
			if err != nil || statusErr != nil || result.Phase != "active" || result.Epoch <= pending.Epoch || result.PendingID != "" {
				test.Fatalf("expired grant did not recover: %+v %v", result, err)
			}
		})
	}
}

func TestReconcilePendingRenewalUnknownStateNeverGrants(test *testing.T) {
	for _, mode := range []string{"unknown", "state-error", "live"} {
		test.Run(mode, func(test *testing.T) {
			fixture := newReconcileFixture(test)
			database := reconcileDatabase(mode)
			_, err := fixture.controller.Place(context.Background(), database, "local", "writer")
			if err != nil {
				test.Fatal(err)
			}
			actual := fixture.fake.authority[database.Key()]
			actual.RemainingTTLMS = 400
			fixture.fake.authority[database.Key()] = actual
			fixture.fake.lost["renew"] = 1
			if err := fixture.controller.Reconcile(context.Background()); err == nil {
				test.Fatal("renewal ACK loss ignored")
			}
			pending, _ := fixture.controller.Status(database)
			delete(fixture.fake.receipts, pending.PendingID)
			delete(fixture.fake.payloads, pending.PendingID)
			actual.Binding = nil
			actual.RemainingTTLMS = 0
			actual.Mode = mode
			if mode == "live" {
				actual.Mode = "writer"
				binding := pending.Grant.Binding
				binding.RenewalSequence++
				actual.Binding = &binding
				actual.RemainingTTLMS = 400
			}
			fixture.fake.authority[database.Key()] = actual
			fixture.restart(test)
			if mode == "state-error" {
				fixture.controller.transport = renewalStateFailure{fixture.fake}
			}
			if err := fixture.controller.Reconcile(context.Background()); err == nil {
				test.Fatal("unproven expiry allowed renewal to progress")
			}
			after, err := fixture.controller.Status(database)
			if err != nil || after.Phase != "renew_pending" || after.Epoch != pending.Epoch || after.PendingID != pending.PendingID || string(after.PendingCommand) != string(pending.PendingCommand) || fixture.fake.calls["grant"] != 1 {
				test.Fatalf("unknown authority outcome changed pending renewal: %+v %v", after, err)
			}
		})
	}
}

func TestReconcileExpiredPendingRenewalKeepsFencedGrantPending(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("renew-fenced-grant")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	actual := fixture.fake.authority[database.Key()]
	actual.RemainingTTLMS = 400
	fixture.fake.authority[database.Key()] = actual
	fixture.fake.lost["renew"] = 1
	if err := fixture.controller.Reconcile(context.Background()); err == nil {
		test.Fatal("renewal ACK loss ignored")
	}
	pending, _ := fixture.controller.Status(database)
	delete(fixture.fake.receipts, pending.PendingID)
	delete(fixture.fake.payloads, pending.PendingID)
	fixture.fake.authority[database.Key()] = Actual{Database: database, Mode: "revoked", Epoch: before.Epoch}
	fixture.fake.rejectGrant = true
	if err := fixture.controller.Reconcile(context.Background()); err == nil {
		test.Fatal("grant CAS rejection ignored")
	}
	after, err := fixture.controller.Status(database)
	if err != nil || after.Phase != "grant_pending" || after.Epoch <= before.Epoch || after.PendingID == pending.PendingID || fixture.fake.calls["acquire_database"] != 1 {
		test.Fatalf("rejected new grant bypassed fencing: %+v %v", after, err)
	}
	fixture.restart(test)
	fixture.fake.rejectGrant = false
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	settled, err := fixture.controller.Status(database)
	if err != nil || settled.Phase != "active" || settled.Epoch != after.Epoch || settled.PendingID != "" {
		test.Fatalf("durable grant did not recover: %+v %v", settled, err)
	}
}

func TestReconcileRestartDoesNotTrustPersistedLeaseTTL(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("expired")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	fixture.restart(test)
	actual := fixture.fake.authority[database.Key()]
	actual.RemainingTTLMS = 0
	actual.Mode = "revoked"
	fixture.fake.authority[database.Key()] = actual
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, _ := fixture.controller.Status(database)
	if after.Epoch <= before.Epoch || after.Phase != "active" {
		test.Fatalf("persisted TTL resurrected old epoch: %+v", after)
	}
}

func TestReconcilePreconditionFailurePreservesPendingRequestAndEpoch(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("precondition")
	fixture.fake.rejectGrant = true
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err == nil {
		test.Fatal("grant precondition failure ignored")
	}
	if err := fixture.controller.Reconcile(context.Background()); err == nil {
		test.Fatal("pending precondition retry unexpectedly succeeded")
	}
	after, _ := fixture.controller.Status(database)
	if after.Epoch != before.Epoch || after.PendingID != before.PendingID || string(after.PendingCommand) != string(before.PendingCommand) || after.Phase != "grant_pending" || fixture.fake.calls["acquire_database"] != 0 {
		test.Fatal("precondition failure abandoned exact pending request")
	}
	fixture.fake.rejectGrant = false
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
}

func TestReconcileContextAndCapacityPreconditions(test *testing.T) {
	fixture := newReconcileFixture(test)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.controller.Place(ctx, reconcileDatabase("cancelled"), "local", "writer"); !errors.Is(err, context.Canceled) {
		test.Fatalf("cancelled operation dispatched: %v", err)
	}
	if _, err := fixture.controller.Place(context.Background(), reconcileDatabase("region"), "unavailable", "writer"); !errors.Is(err, ErrNoEligibleWorker) {
		test.Fatalf("region capacity ignored: %v", err)
	}
	if _, err := fixture.controller.Place(context.Background(), reconcileDatabase("role"), "local", "admin"); err == nil {
		test.Fatal("unsupported role accepted")
	}
}

// A restarted controller reacquires every active placement in one pass. When
// that work outlives the heartbeat window, the later placements used to see a
// stale observation and either migrate a healthy placement or fail with
// ErrNoEligibleWorker. The pass must re-probe the desired worker once instead.
func TestReconcileRefreshesHeartbeatThatAgesOutDuringThePass(test *testing.T) {
	fixture := newReconcileFixture(test)
	fixture.config.HeartbeatTimeout = 250 * time.Millisecond
	controller, err := NewController(fixture.store, fixture.fake, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	first, second := reconcileDatabase("first"), reconcileDatabase("second")
	if _, err := controller.Place(context.Background(), first, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	if _, err := controller.Place(context.Background(), second, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	before, err := controller.Status(second)
	if err != nil {
		test.Fatal(err)
	}
	firstStatus, err := controller.Status(first)
	if err != nil {
		test.Fatal(err)
	}
	if firstStatus.DesiredWorker == before.DesiredWorker {
		test.Fatalf("fixture did not spread placements: %+v %+v", firstStatus, before)
	}
	// A worker restart drops its local instances while the authority keeps the
	// grants; reacquiring the first database holds the pass past the window.
	fixture.fake.workers = make(map[string]map[string]Actual)
	fixture.fake.delay[first.Key()] = 500 * time.Millisecond
	acquired := fixture.fake.calls["acquire_database"]
	if err := controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, err := controller.Status(second)
	if err != nil {
		test.Fatal(err)
	}
	if after.Phase != "active" || after.DesiredWorker != before.DesiredWorker {
		test.Fatalf("stale pass observation moved a healthy placement: %+v", after)
	}
	if fixture.fake.calls["acquire_database"]-acquired != 2 {
		test.Fatalf("restart did not reacquire both placements: %+v", fixture.fake.calls)
	}
}

func TestReconcileWithinRetriesTransientAuthorityFailure(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("retry")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	fixture.fake.authorityReject = 2
	if err := fixture.controller.ReconcileWithin(context.Background(), time.Now().Add(2*time.Second)); err != nil {
		test.Fatal(err)
	}
	if fixture.fake.calls["get_database_state"] < 3 {
		test.Fatalf("transient failure was not retried: %+v", fixture.fake.calls)
	}
}

func TestReconcileWithinStopsAtDeadline(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("deadline")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	fixture.fake.authorityReject = 100
	started := time.Now()
	err := fixture.controller.ReconcileWithin(context.Background(), started.Add(200*time.Millisecond))
	if err == nil {
		test.Fatal("deadline expiration returned success")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		test.Fatalf("retry loop ignored its deadline: %s", elapsed)
	}
	if attempts := fixture.fake.calls["get_database_state"]; attempts < 2 || attempts > 10 {
		test.Fatalf("retry attempts were not bounded by the deadline: %d", attempts)
	}
}
