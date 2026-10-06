package controller

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func p5VerifyActual(test *testing.T, cluster *p5VerifyCluster, database DatabaseID) Actual {
	test.Helper()
	ctx, cancel := p5VerifyContext()
	defer cancel()
	var actual Actual
	if err := cluster.transport.Authority(ctx, "inspect-real-ownership", map[string]any{"command": "get_database_state", "database": database}, &actual); err != nil {
		test.Fatal(err)
	}
	return actual
}

func TestP5VerifyGracefulMigrationCannotACKActiveTransactionThenAdvancesAfterCleanup(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	store := p5VerifyStore(test, filepath.Join(cluster.root, "controller-state"))
	cluster.register(store)
	control := p5VerifyController(test, store, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("graceful-active-txn")
	placed, err := control.Place(ctx, database, "local", "writer")
	if err != nil || placed.DesiredWorker != "verify-a" || placed.Grant == nil {
		test.Fatalf("initial real placement failed: %v", err)
	}
	session := cluster.open("verify-a", database)
	cluster.execute("verify-a", session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(7); BEGIN IMMEDIATE; INSERT INTO records VALUES(999)")
	premature, err := control.Migrate(ctx, database, "verify-b")
	if err == nil {
		test.Fatal("migration acknowledged while old transaction remained active")
	}
	actual := p5VerifyActual(test, cluster, database)
	if premature.Epoch != placed.Epoch || actual.Epoch != placed.Epoch || actual.Binding == nil || actual.Binding.WorkerID != "verify-a" {
		test.Fatal("failed drain advanced actual fencing epoch prematurely")
	}
	if cluster.sql("verify-b", map[string]any{"command": "open_session", "database": database}).OK {
		test.Fatal("replacement SQL became available before drain settled")
	}
	cluster.execute("verify-a", session, "ROLLBACK")
	if !cluster.sql("verify-a", map[string]any{"command": "close_session", "session": session}).OK {
		test.Fatal("old transaction session cleanup failed")
	}
	migrated, err := control.Migrate(ctx, database, "verify-b")
	if err != nil || migrated.Phase != "active" || migrated.DesiredWorker != "verify-b" || migrated.Epoch <= placed.Epoch {
		test.Fatalf("cleaned migration did not advance: phase=%s epoch=%d err=%v", migrated.Phase, migrated.Epoch, err)
	}
	recovered := cluster.open("verify-b", database)
	p5VerifyRequireRows(test, cluster.rows("verify-b", recovered, "SELECT id FROM records ORDER BY id"), "[[7]]")
	p5VerifyRequireRows(test, cluster.rows("verify-b", recovered, "PRAGMA integrity_check"), `[["ok"]]`)
	cluster.execute("verify-b", recovered, "INSERT INTO records VALUES(8)")
	test.Logf("actual active native transaction migration refused at epoch%d; ROLLBACK+close allowed graceful epoch%d and ACK7 recovery", placed.Epoch, migrated.Epoch)
}

func TestP5VerifyDurableMetadataRestartDoesNotResurrectExpiredLeaseAndAllocatesHigherEpoch(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	directory := filepath.Join(cluster.root, "controller-state")
	store := p5VerifyStore(test, directory)
	cluster.register(store)
	control := p5VerifyController(test, store, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("restart-expired")
	placed, err := control.Place(ctx, database, "local", "writer")
	if err != nil || placed.Grant == nil {
		test.Fatalf("initial placement failed: %v", err)
	}
	session := cluster.open(placed.DesiredWorker, database)
	cluster.execute(placed.DesiredWorker, session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(5)")
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	time.Sleep(3200 * time.Millisecond)
	if cluster.sql(placed.DesiredWorker, map[string]any{"command": "query", "session": session, "sql": "SELECT id FROM records"}).OK {
		test.Fatal("expired cached SQL continued serving without controller renewal")
	}
	closeReply := cluster.sql(placed.DesiredWorker, map[string]any{"command": "close_session", "session": session})
	var resources struct {
		Resources struct {
			ActiveSessions int `json:"active_sessions"`
		} `json:"resources"`
	}
	deadline := time.Now().Add(time.Second)
	for {
		var report json.RawMessage
		if err := cluster.transport.Worker(ctx, cluster.workers[placed.DesiredWorker].worker, "expired-resource-state", map[string]string{"command": "get_worker_state"}, &report); err != nil || json.Unmarshal(report, &resources) != nil {
			test.Fatalf("expired worker resources unavailable: %v", err)
		}
		if resources.Resources.ActiveSessions == 0 {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("expired native sessions retained live capacity")
		}
		time.Sleep(20 * time.Millisecond)
	}
	test.Logf("expiry watchdog/explicit close released all sessions; close_returned_ok=%t", closeReply.OK)
	reopened := p5VerifyStore(test, directory)
	restarted := p5VerifyController(test, reopened, cluster.transport)
	loaded, err := reopened.Snapshot()
	if err != nil || loaded.Epoch != before.Epoch || loaded.Revision != before.Revision {
		test.Fatal("durable metadata was reset or allocated an epoch during load")
	}
	actual := p5VerifyActual(test, cluster, database)
	if actual.RemainingTTLMS != 0 || actual.Mode == "writer" {
		test.Fatal("loading desired metadata resurrected expired actual lease")
	}
	if err := restarted.Reconcile(ctx); err != nil {
		test.Fatalf("fresh reconcile failed: %v", err)
	}
	fresh, err := restarted.Status(database)
	if err != nil || fresh.Phase != "active" || fresh.Epoch <= placed.Epoch || fresh.Grant == nil || fresh.Grant.Binding.LeaseID == placed.Grant.Binding.LeaseID {
		test.Fatal("restart did not allocate a new monotonic fencing lease")
	}
	newSession := cluster.open(fresh.DesiredWorker, database)
	p5VerifyRequireRows(test, cluster.rows(fresh.DesiredWorker, newSession, "SELECT id FROM records"), "[[5]]")
	second, err := restarted.Place(ctx, p5VerifyDatabase("restart-second"), "local", "writer")
	if err != nil || second.Epoch <= fresh.Epoch {
		test.Fatalf("global epoch allocation after restart was not monotonic: %v", err)
	}
	serialized, err := json.Marshal(loaded)
	if err != nil || len(serialized) == 0 {
		test.Fatal("durable loaded metadata missing")
	}
	for _, token := range cluster.tokens {
		if strings.Contains(string(serialized), token) {
			test.Fatal("loaded durable metadata contains an RPC credential")
		}
	}
	test.Logf("durable metadata reopened unchanged; actual expired lease remained expired; reconciliation/newDB epochs %d<%d<%d", placed.Epoch, fresh.Epoch, second.Epoch)
}
