package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type narrowViewProbe struct {
	MetadataStore
	snapshots int
}

func (probe *narrowViewProbe) Snapshot() (Metadata, error) {
	probe.snapshots++
	return Metadata{}, errors.New("full metadata snapshot on narrow read")
}

func (probe *narrowViewProbe) RoutingView(database DatabaseID) (Placement, Worker, bool, error) {
	return probe.MetadataStore.(interface {
		RoutingView(DatabaseID) (Placement, Worker, bool, error)
	}).RoutingView(database)
}

func (probe *narrowViewProbe) workerView(identifier string) (Worker, bool, error) {
	return probe.MetadataStore.(*diskStore).workerView(identifier)
}

func (probe *narrowViewProbe) workersView() (map[string]Worker, error) {
	return probe.MetadataStore.(*diskStore).workersView()
}

func (probe *narrowViewProbe) reconcileView() (Metadata, error) {
	return probe.MetadataStore.(*diskStore).reconcileView()
}

func TestStoreRoutingAndWorkerViewsAvoidFullSnapshotAndDetachMutableFields(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("narrow-view")
	placed, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil || placed.Grant == nil {
		test.Fatalf("placement fixture unavailable: %v", err)
	}
	probe := &narrowViewProbe{MetadataStore: fixture.store}
	placement, worker, found, err := ReadRouting(probe, database)
	if err != nil || !found || placement.Database != database || worker.ID != placement.DesiredWorker || probe.snapshots != 0 {
		test.Fatalf("routing cloned the catalog or returned inconsistent records: %v", err)
	}
	originalGrant := placement.Grant.Binding.GrantID
	placement.Grant.Binding.GrantID = "tampered"
	placement.PendingCommand = append(placement.PendingCommand, 'x')
	worker.Address = "http://changed.invalid"
	workers, err := readWorkers(probe)
	if err != nil || workers[placed.DesiredWorker].Address == worker.Address || probe.snapshots != 0 {
		test.Fatal("worker view aliases stored metadata or cloned full metadata")
	}
	workers[placed.DesiredWorker] = Worker{}
	current, _, found, err := ReadRouting(probe, database)
	if err != nil || !found || current.Grant.Binding.GrantID != originalGrant || probe.snapshots != 0 {
		test.Fatal("routing view mutation changed durable state")
	}
	selected, found, err := readWorker(probe, placed.DesiredWorker)
	if err != nil || !found || selected.ID != placed.DesiredWorker || probe.snapshots != 0 {
		test.Fatal("single worker read cloned metadata")
	}
}

func TestStoreReconcileViewTracksActionablePlacementsAcrossCommitAndRestart(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if err := RegisterWorker(store, Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 8}); err != nil {
		test.Fatal(err)
	}
	active := reconcileDatabase("active-view")
	pending := reconcileDatabase("pending-view")
	if err := store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 1
		for index := range 128 {
			database := reconcileDatabase(fmt.Sprintf("suspended-%d", index))
			metadata.Placements[database.Key()] = Placement{Database: database, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "suspended", Phase: "suspended", Role: "writer"}
		}
		metadata.Placements[active.Key()] = Placement{Database: active, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "active", Phase: "active", Role: "writer", Grant: &Grant{Binding: Binding{Database: active, Epoch: 1}}}
		metadata.Placements[pending.Key()] = Placement{Database: pending, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "active", Phase: "migrate_drain", Role: "writer"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	check := func(wantActive, wantPending bool) {
		test.Helper()
		probe := &narrowViewProbe{MetadataStore: store}
		view, err := readReconcileView(probe)
		if err != nil || probe.snapshots != 0 || len(view.Placements) != boolCount(wantActive, wantPending) {
			test.Fatalf("reconcile view scanned catalog or retained suspended placements: %v", err)
		}
		_, hasActive := view.Placements[active.Key()]
		_, hasPending := view.Placements[pending.Key()]
		if hasActive != wantActive || hasPending != wantPending {
			test.Fatal("actionable placement index is stale")
		}
		if hasActive {
			placement := view.Placements[active.Key()]
			placement.Grant.Binding.GrantID = "tampered"
			current, _, found, err := ReadRouting(store, active)
			if err != nil || !found || current.Grant.Binding.GrantID == "tampered" {
				test.Fatal("reconcile view aliases durable grant")
			}
		}
	}
	check(true, true)
	if err := store.Update(func(metadata *Metadata) error {
		placement := metadata.Placements[active.Key()]
		placement.Phase, placement.DesiredState = "suspended", "suspended"
		metadata.Placements[active.Key()] = placement
		delete(metadata.Placements, pending.Key())
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	check(false, false)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	store, err = OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	check(false, false)
	if err := store.Update(func(metadata *Metadata) error {
		placement := metadata.Placements[active.Key()]
		placement.Phase, placement.DesiredState = "active", "active"
		metadata.Placements[active.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	check(true, false)
}

func TestReconcileUsesActionableViewWithoutFullCatalogSnapshot(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("cold-catalog")
	if err := fixture.store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 1
		metadata.Placements[database.Key()] = Placement{Database: database, DesiredWorker: "worker-a", Epoch: 1, TTLMS: 1000, DesiredState: "suspended", Phase: "suspended", Role: "writer"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	probe := &narrowViewProbe{MetadataStore: fixture.store}
	fixture.controller.store = probe
	if err := fixture.controller.Reconcile(context.Background()); err != nil || probe.snapshots != 0 {
		test.Fatalf("cold placement forced a full catalog snapshot: %v", err)
	}
	if _, err := fixture.controller.Status(database); err != nil || probe.snapshots != 0 {
		test.Fatalf("status forced a full catalog snapshot: %v", err)
	}
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func BenchmarkReconcileViewWithColdCatalog(bench *testing.B) {
	store, err := OpenStore(bench.TempDir())
	if err != nil {
		bench.Fatal(err)
	}
	defer store.Close()
	if err := RegisterWorker(store, Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 32}); err != nil {
		bench.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 1
		for index := range 10_000 {
			database := reconcileDatabase(fmt.Sprintf("cold-%d", index))
			metadata.Placements[database.Key()] = Placement{Database: database, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "suspended", Phase: "suspended", Role: "writer"}
		}
		for index := range 32 {
			database := reconcileDatabase(fmt.Sprintf("hot-%d", index))
			metadata.Placements[database.Key()] = Placement{Database: database, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "active", Phase: "active", Role: "writer"}
		}
		return nil
	}); err != nil {
		bench.Fatal(err)
	}
	bench.Run("full_snapshot", func(bench *testing.B) {
		bench.ReportAllocs()
		for range bench.N {
			if _, err := store.Snapshot(); err != nil {
				bench.Fatal(err)
			}
		}
	})
	bench.Run("actionable_view", func(bench *testing.B) {
		bench.ReportAllocs()
		for range bench.N {
			if _, err := readReconcileView(store); err != nil {
				bench.Fatal(err)
			}
		}
	})
}
