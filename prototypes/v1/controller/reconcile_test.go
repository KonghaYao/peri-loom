package controller

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
