package controller

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func TestReconcileUnacknowledgedWorkerFailureCanFenceAndPlaceReplacement(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("acquire-failover")
	fixture.fake.lost["acquire_database"] = 1
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err == nil || before.Phase != "acquire_pending" {
		test.Fatal("unacknowledged worker acquire missing")
	}
	fixture.fake.offline[before.DesiredWorker] = true
	if err := fixture.controller.Reconcile(context.Background()); err != nil {
		test.Fatal(err)
	}
	after, _ := fixture.controller.Status(database)
	if after.Phase != "active" || after.Epoch <= before.Epoch || after.DesiredWorker == before.DesiredWorker {
		test.Fatalf("pending worker failover stalled: %+v", after)
	}
}

func TestReconcilePlaceAfterRestartValidatesActualLeaseBeforeReturningActive(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("place-expired")
	before, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	fixture.restart(test)
	actual := fixture.fake.authority[database.Key()]
	actual.RemainingTTLMS = 0
	actual.Mode = "revoked"
	fixture.fake.authority[database.Key()] = actual
	after, err := fixture.controller.Place(context.Background(), database, "local", "writer")
	if err != nil || after.Epoch <= before.Epoch {
		test.Fatalf("Place trusted stored TTL: %+v %v", after, err)
	}
}

func TestReconcileRejectsUnknownPersistedProtocolFieldsBeforeRPC(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("unknown-payload")
	fixture.fake.lost["grant"] = 1
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err == nil {
		test.Fatal("lost grant acknowledgement ignored")
	}
	if err := fixture.store.Update(func(metadata *Metadata) error {
		placement := metadata.Placements[database.Key()]
		var command map[string]any
		if err := json.Unmarshal(placement.PendingCommand, &command); err != nil {
			return err
		}
		command["unexpected_host_path"] = "/tmp/untrusted"
		bytes, err := json.Marshal(command)
		if err != nil {
			return err
		}
		placement.PendingCommand = bytes
		metadata.Placements[database.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	before := fixture.fake.calls["grant"]
	if _, err := NewController(fixture.store, fixture.fake, fixture.config); err == nil {
		test.Fatal("unknown durable protocol fields accepted")
	}
	if fixture.fake.calls["grant"] != before {
		test.Fatal("invalid metadata dispatched a mutator")
	}
}

type reconcileUnavailableStore struct {
	MetadataStore
	failing bool
}

func (store *reconcileUnavailableStore) Snapshot() (Metadata, error) {
	if store.failing {
		return Metadata{}, errors.New("metadata unavailable")
	}
	return store.MetadataStore.Snapshot()
}

func TestReconcilePlaceDoesNotTreatUnavailableMetadataAsMissingDatabase(test *testing.T) {
	fixture := newReconcileFixture(test)
	store := &reconcileUnavailableStore{MetadataStore: fixture.store}
	control, err := NewController(store, fixture.fake, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	store.failing = true
	if _, err := control.Place(context.Background(), reconcileDatabase("unavailable"), "local", "writer"); err == nil || errors.Is(err, ErrPlacementMissing) {
		test.Fatalf("metadata failure treated as missing: %v", err)
	}
	if len(fixture.fake.trace) != 0 {
		test.Fatal("unavailable metadata caused RPC")
	}
}

func TestReconcileEpochOverflowFailsBeforeChallengeOrGrant(test *testing.T) {
	fixture := newReconcileFixture(test)
	if err := fixture.store.Update(func(metadata *Metadata) error { metadata.Epoch = math.MaxUint64; return nil }); err != nil {
		test.Fatal(err)
	}
	if _, err := fixture.controller.Place(context.Background(), reconcileDatabase("overflow"), "local", "writer"); err == nil {
		test.Fatal("epoch wrapped")
	}
	if fixture.fake.calls["challenge"] != 0 || fixture.fake.calls["grant"] != 0 {
		test.Fatal("overflow dispatched mutator")
	}
}

func TestReconcileRunStopsAndDoesNotHoldMetadataMutexAcrossTransport(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("run")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := fixture.controller.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		test.Fatalf("Run cancellation failed: %v", err)
	}
	if _, err := fixture.store.Snapshot(); err != nil {
		test.Fatal("metadata lock remained held")
	}
}
