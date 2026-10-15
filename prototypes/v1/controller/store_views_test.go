package controller

import (
	"context"
	"errors"
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
