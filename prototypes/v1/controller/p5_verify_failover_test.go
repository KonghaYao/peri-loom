package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func p5VerifyController(test *testing.T, store MetadataStore, transport ControlTransport) *Controller {
	test.Helper()
	control, err := NewController(store, transport, ReconcileConfig{LeaseTTL: 3 * time.Second, HeartbeatTimeout: 500 * time.Millisecond, Interval: 100 * time.Millisecond})
	if err != nil {
		test.Fatal(err)
	}
	return control
}

func p5VerifyStore(test *testing.T, directory string) MetadataStore {
	test.Helper()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = store.Close() })
	return store
}

func p5VerifyRun(test *testing.T, control *Controller) context.CancelFunc {
	test.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- control.Run(ctx) }()
	test.Cleanup(func() {
		cancel()
		select {
		case err := <-finished:
			if err != nil && !errors.Is(err, context.Canceled) {
				test.Errorf("automatic controller exited unexpectedly: %v", err)
			}
		case <-time.After(8 * time.Second):
			test.Error("automatic controller did not stop")
		}
	})
	return cancel
}

func p5VerifyWaitPlacement(test *testing.T, control *Controller, database DatabaseID, ready func(Placement) bool) Placement {
	test.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last Placement
	var err error
	for time.Now().Before(deadline) {
		last, err = control.Status(database)
		if err == nil && ready(last) {
			return last
		}
		time.Sleep(30 * time.Millisecond)
	}
	test.Fatalf("placement did not converge: phase=%s worker=%s epoch=%d err=%v", last.Phase, last.DesiredWorker, last.Epoch, err)
	return Placement{}
}

func p5VerifyRequireRows(test *testing.T, actual json.RawMessage, expected string) {
	test.Helper()
	var decoded any
	if json.Unmarshal(actual, &decoded) != nil {
		test.Fatal("invalid SQL rows")
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		test.Fatal(err)
	}
	if string(canonical) != expected {
		test.Fatalf("SQL rows mismatch: got %s want %s", canonical, expected)
	}
}

func TestP5VerifyAutomaticActualKillFailoverPreservesACKAndRejectsRevivedOldGrant(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	store := p5VerifyStore(test, filepath.Join(cluster.root, "controller-state"))
	cluster.register(store)
	control := p5VerifyController(test, store, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("automatic-kill")
	placed, err := control.Place(ctx, database, "local", "writer")
	if err != nil || placed.Phase != "active" || placed.DesiredWorker != "verify-a" || placed.Grant == nil {
		test.Fatalf("initial real placement failed: phase=%s worker=%s err=%v", placed.Phase, placed.DesiredWorker, err)
	}
	p5VerifyRun(test, control)
	session := cluster.open("verify-a", database)
	cluster.execute("verify-a", session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(1)")
	for _, operation := range []string{"checkpoint", "compact"} {
		if !cluster.sql("verify-a", map[string]any{"command": operation, "session": session}).OK {
			test.Fatalf("initial %s refused", operation)
		}
	}
	cluster.execute("verify-a", session, "INSERT INTO records VALUES(2)")
	p5VerifyRequireRows(test, cluster.rows("verify-a", session, "SELECT id FROM records ORDER BY id"), "[[1],[2]]")
	cluster.execute("verify-a", session, "BEGIN IMMEDIATE; INSERT INTO records VALUES(999)")
	old := *placed.Grant
	workerA := cluster.workers["verify-a"]
	workerA.process.kill()
	var exit *exec.ExitError
	if !errors.As(workerA.process.wait(), &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		test.Fatal("worker A did not actually terminate by SIGKILL")
	}
	if err := os.RemoveAll(workerA.cache); err != nil {
		test.Fatal(err)
	}
	if _, err := os.Stat(workerA.cache); !os.IsNotExist(err) {
		test.Fatal("old worker whole cache still exists")
	}
	failedOver := p5VerifyWaitPlacement(test, control, database, func(actual Placement) bool {
		return actual.Phase == "active" && actual.DesiredWorker == "verify-b" && actual.Epoch > placed.Epoch
	})
	registry, err := store.Snapshot()
	if err != nil || registry.Workers["verify-a"].Healthy || !registry.Workers["verify-b"].Healthy {
		test.Fatal("automatic RPC failure detection did not update durable worker health")
	}
	recovered := cluster.open("verify-b", database)
	p5VerifyRequireRows(test, cluster.rows("verify-b", recovered, "SELECT id FROM records ORDER BY id"), "[[1],[2]]")
	p5VerifyRequireRows(test, cluster.rows("verify-b", recovered, "PRAGMA integrity_check"), `[["ok"]]`)
	cluster.execute("verify-b", recovered, "INSERT INTO records VALUES(3)")
	cluster.startWorker("verify-a")
	var result json.RawMessage
	err = cluster.transport.Worker(ctx, workerA.worker, "revived-old-grant", map[string]any{"command": "acquire_database", "lease": old, "role": "writer"}, &result)
	var rejection *RPCError
	if !errors.As(err, &rejection) || rejection.Message != "RPC operation rejected" {
		test.Fatalf("revived A did not return a parsed negative ownership response: %v", err)
	}
	var revived Actual
	if err := cluster.transport.Worker(ctx, workerA.worker, "revived-state", map[string]any{"command": "get_database_state", "database": database}, &revived); err != nil || revived.Mode == "writer" {
		test.Fatalf("revived old owner serving writer: mode=%s err=%v", revived.Mode, err)
	}
	if cluster.sql("verify-a", map[string]any{"command": "open_session", "database": database}).OK {
		test.Fatal("revived old owner acquired a SQL session")
	}
	p5VerifyRequireRows(test, cluster.rows("verify-b", recovered, "SELECT id FROM records ORDER BY id"), "[[1],[2],[3]]")
	test.Logf("actual SIGKILL9/cache deletion; automatic Run advanced epoch %d->%d and recovered ACK rows, excluded dirty999; revived A old grant denied", placed.Epoch, failedOver.Epoch)
}
