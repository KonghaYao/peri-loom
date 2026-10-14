package controller

import (
	"path/filepath"
	"testing"
)

func TestP5VerifySuspendSurvivesMetadataRestartAndResumeUsesFreshActualFence(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	directory := filepath.Join(cluster.root, "controller-state")
	store := p5VerifyStore(test, directory)
	cluster.register(store)
	control := p5VerifyController(test, store, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("suspend-resume")
	placed, err := control.Place(ctx, database, "local", "writer")
	if err != nil || placed.Grant == nil {
		test.Fatalf("initial placement failed: %v", err)
	}
	session := cluster.open(placed.DesiredWorker, database)
	cluster.execute(placed.DesiredWorker, session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(6)")
	if !cluster.sql(placed.DesiredWorker, map[string]any{"command": "close_session", "session": session}).OK {
		test.Fatal("pre-suspend native session cleanup failed")
	}
	if err := control.Suspend(ctx, database); err != nil {
		test.Fatalf("real suspend failed: %v", err)
	}
	suspended, err := control.Status(database)
	actual := p5VerifyActual(test, cluster, database)
	if err != nil || suspended.Phase != "suspended" || suspended.DesiredState != "suspended" || actual.Epoch != placed.Epoch || actual.Mode != "revoked" || actual.RemainingTTLMS != 0 {
		test.Fatal("suspend did not settle desired state plus actual Rust revocation")
	}
	if cluster.sql(placed.DesiredWorker, map[string]any{"command": "open_session", "database": database}).OK {
		test.Fatal("suspended old owner accepted SQL ingress")
	}
	oldWorker := cluster.workers[placed.DesiredWorker]
	rejected, status, err := p5VerifyRPC(cluster.authority.address, "/", oldWorker.authorityToken, map[string]any{"command": "validate_lease", "binding": placed.Grant.Binding, "role": "writer"})
	if err != nil || status != 200 || rejected.OK {
		test.Fatal("suspend left original authority write capability valid")
	}
	if err := control.Suspend(ctx, database); err != nil {
		test.Fatal("settled suspend was not idempotent")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened := p5VerifyStore(test, directory)
	restarted := p5VerifyController(test, reopened, cluster.transport)
	if err := restarted.Reconcile(ctx); err != nil {
		test.Fatal(err)
	}
	loaded, err := restarted.Status(database)
	if err != nil || loaded.Phase != "suspended" || loaded.Epoch != placed.Epoch || p5VerifyActual(test, cluster, database).Mode != "revoked" {
		test.Fatal("metadata load/reconciliation resumed a suspended database")
	}
	if _, err := restarted.Place(ctx, database, "local", "writer"); err == nil {
		test.Fatal("Place bypassed explicit Resume of suspended database")
	}
	resumed, err := restarted.Resume(ctx, database)
	if err != nil || resumed.Phase != "active" || resumed.Epoch <= placed.Epoch || resumed.Grant == nil || resumed.Grant.Binding.LeaseID == placed.Grant.Binding.LeaseID {
		test.Fatalf("resume did not obtain fresh actual fencing grant: %v", err)
	}
	recovered := cluster.open(resumed.DesiredWorker, database)
	p5VerifyRequireRows(test, cluster.rows(resumed.DesiredWorker, recovered, "SELECT id FROM records"), "[[6]]")
	p5VerifyRequireRows(test, cluster.rows(resumed.DesiredWorker, recovered, "PRAGMA integrity_check"), `[["ok"]]`)
	cluster.execute(resumed.DesiredWorker, recovered, "INSERT INTO records VALUES(7)")
	test.Logf("actual suspend revoked epoch%d and survived durable metadata reopen; only Resume obtained epoch%d and recovered ACK6/integrity", placed.Epoch, resumed.Epoch)
}
