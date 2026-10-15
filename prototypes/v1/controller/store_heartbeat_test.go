package controller

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerHeartbeatDoesNotRewriteDurableMetadataAndRequiresFreshProbeAfterRestart(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	worker := Worker{ID: "worker", Address: "http://127.0.0.1:9001", Region: "local", Capacity: 4}
	if err := RegisterWorker(store, worker); err != nil {
		test.Fatal(err)
	}
	path := filepath.Join(directory, "metadata.json")
	before, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	initial, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if err := HeartbeatWorker(store, worker.ID, 2, 100); err != nil {
		test.Fatal(err)
	}
	live, err := store.Snapshot()
	if err != nil || live.Revision != initial.Revision || !live.Workers[worker.ID].Healthy || live.Workers[worker.ID].Load != 2 {
		test.Fatalf("live heartbeat not visible without revision change: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("pure heartbeat rewrote durable metadata")
	}
	if err := SetWorkerDraining(store, worker.ID, true); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil || !state.Workers[worker.ID].Draining || state.Workers[worker.ID].Healthy {
		test.Fatalf("restart trusted old heartbeat or lost durable drain intent: %v", err)
	}
	if err := SetWorkerDraining(reopened, worker.ID, false); err != nil {
		test.Fatal(err)
	}
	state, err = reopened.Snapshot()
	if err != nil || state.Workers[worker.ID].Healthy || state.Workers[worker.ID].Draining {
		test.Fatal("control update revived stale worker health")
	}
	if err := HeartbeatWorker(reopened, worker.ID, 1, 101); err != nil {
		test.Fatal(err)
	}
	state, err = reopened.Snapshot()
	if err != nil || !state.Workers[worker.ID].Healthy || state.Workers[worker.ID].Load != 1 {
		test.Fatal("fresh heartbeat failed to restore worker eligibility")
	}
}

func TestWorkerHeartbeatIgnoresStaleEndpointAndOutOfOrderProbe(test *testing.T) {
	store, err := OpenStore(test.TempDir())
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	worker := Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 4}
	if err := RegisterWorker(store, worker); err != nil {
		test.Fatal(err)
	}
	if err := store.(*diskStore).observeWorkerHeartbeat(worker.ID, worker.Address, true, 2, 200); err != nil {
		test.Fatal(err)
	}
	if err := store.(*diskStore).observeWorkerHeartbeat(worker.ID, worker.Address, false, 0, 199); err != nil {
		test.Fatal(err)
	}
	if err := store.(*diskStore).observeWorkerHeartbeat(worker.ID, "http://127.0.0.1:9002", false, 0, 201); err != nil {
		test.Fatal(err)
	}
	if err := store.(*diskStore).observeWorkerHeartbeat(worker.ID, worker.Address, false, 0, 210); err != nil {
		test.Fatal(err)
	}
	if err := SetWorkerDraining(store, worker.ID, true); err != nil {
		test.Fatal(err)
	}
	if err := store.(*diskStore).observeWorkerHeartbeat(worker.ID, worker.Address, true, 3, 205); err != nil {
		test.Fatal(err)
	}
	state, err := store.Snapshot()
	if err != nil || state.Workers[worker.ID].Healthy || state.Workers[worker.ID].Load != 2 {
		test.Fatal("stale or foreign probe replaced latest worker observation")
	}
}
