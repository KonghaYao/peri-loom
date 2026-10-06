package controller

import (
	"errors"
	"math"
	"testing"
)

func registryWorker(id string, load, capacity int) Worker {
	return Worker{ID: id, Address: "http://127.0.0.1:9001", Region: "west", Capacity: capacity, Load: load, Healthy: true, LastHeartbeatMS: 100}
}

func TestRegistryRequiresHeartbeatAndPreservesRestartState(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	worker := registryWorker("worker", 1, 3)
	if err := RegisterWorker(store, worker); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SelectWorker(metadata, "west", "", 100, 1000); !errors.Is(err, ErrNoEligibleWorker) {
		t.Fatalf("configuration faked heartbeat: %v", err)
	}
	if err := HeartbeatWorker(store, "worker", 2, 200); err != nil {
		t.Fatal(err)
	}
	if err := SetWorkerDraining(store, "worker", true); err != nil {
		t.Fatal(err)
	}
	worker.LastHeartbeatMS = 0
	worker.Healthy = false
	worker.Load = 0
	worker.Capacity = 4
	if err := RegisterWorker(store, worker); err != nil {
		t.Fatal(err)
	}
	metadata, err = store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	actual := metadata.Workers["worker"]
	if !actual.Draining || !actual.Healthy || actual.Load != 2 || actual.LastHeartbeatMS != 200 || actual.Capacity != 4 {
		t.Fatalf("restart reset durable state: %+v", actual)
	}
	if err := HeartbeatWorker(store, "worker", 1, 199); err == nil {
		t.Fatal("heartbeat clock rollback accepted")
	}
	if err := HeartbeatWorker(store, "worker", 1, 201); err != nil {
		t.Fatal(err)
	}
	metadata, _ = store.Snapshot()
	if !metadata.Workers["worker"].Draining {
		t.Fatal("heartbeat undrained worker")
	}
	worker.Address = "http://127.0.0.1:9002"
	if err := RegisterWorker(store, worker); err == nil {
		t.Fatal("worker endpoint rebound")
	}
}

func TestRegistryRejectsUnsafeEndpointAndInvalidHeartbeats(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, address := range []string{"http://localhost:9001", "http://example.com:9001", "http://192.168.0.1:9001", "http://user:password@127.0.0.1:9001", "https://127.0.0.1:9001", "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:9001/private", "http://127.0.0.1:9001?token=secret", "http://[::1%25zone]:9001"} {
		worker := registryWorker("worker", 0, 2)
		worker.Address = address
		if err := RegisterWorker(store, worker); err == nil {
			t.Fatal("unsafe worker endpoint accepted")
		}
	}
	worker := registryWorker("worker", 0, 2)
	worker.Address = "http://[::1]:9001"
	if err := RegisterWorker(store, worker); err != nil {
		t.Fatal(err)
	}
	if err := HeartbeatWorker(store, "missing", 0, 100); err == nil {
		t.Fatal("unknown worker heartbeat")
	}
	if err := HeartbeatWorker(store, "worker", -1, 100); err == nil {
		t.Fatal("negative load")
	}
	if err := SetWorkerDraining(store, "missing", true); err == nil {
		t.Fatal("unknown worker drain")
	}
	metadata, _ := store.Snapshot()
	if metadata.Revision != 1 {
		t.Fatalf("invalid calls persisted: %d", metadata.Revision)
	}
}

func TestSelectWorkerHealthCapacityRegionAndDeterministicLeastUtilization(t *testing.T) {
	metadata := Metadata{Workers: map[string]Worker{
		"alpha": registryWorker("alpha", 1, 2),
		"beta":  registryWorker("beta", 1, 4),
		"gamma": registryWorker("gamma", 2, 8),
	}}
	for iteration := 0; iteration < 100; iteration++ {
		worker, err := SelectWorker(metadata, "west", "", 150, 100)
		if err != nil || worker.ID != "beta" {
			t.Fatalf("least load/stable tie: %s %v", worker.ID, err)
		}
	}
	worker, err := SelectWorker(metadata, "west", "beta", 150, 100)
	if err != nil || worker.ID != "gamma" {
		t.Fatalf("exclude: %s %v", worker.ID, err)
	}
	for _, change := range []func(*Worker){
		func(worker *Worker) { worker.Healthy = false },
		func(worker *Worker) { worker.Draining = true },
		func(worker *Worker) { worker.Load = worker.Capacity },
		func(worker *Worker) { worker.LastHeartbeatMS = 50 },
		func(worker *Worker) { worker.LastHeartbeatMS = 151 },
		func(worker *Worker) { worker.Region = "east" },
	} {
		for id, worker := range metadata.Workers {
			change(&worker)
			metadata.Workers[id] = worker
		}
		if _, err := SelectWorker(metadata, "west", "", 150, 100); !errors.Is(err, ErrNoEligibleWorker) {
			t.Fatalf("ineligible worker selected: %v", err)
		}
		metadata.Workers = map[string]Worker{"alpha": registryWorker("alpha", 1, 2)}
	}
	if _, err := SelectWorker(metadata, "", "", 150, 0); err == nil {
		t.Fatal("zero timeout accepted")
	}
}

func TestSelectWorkerUtilizationComparisonDoesNotOverflow(t *testing.T) {
	metadata := Metadata{Workers: map[string]Worker{
		"alpha": registryWorker("alpha", math.MaxInt-2, math.MaxInt),
		"beta":  registryWorker("beta", math.MaxInt-3, math.MaxInt),
	}}
	worker, err := SelectWorker(metadata, "", "", 100, 100)
	if err != nil || worker.ID != "beta" {
		t.Fatalf("overflowing comparison: %s %v", worker.ID, err)
	}
}
