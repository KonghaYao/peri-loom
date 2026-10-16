package platform

import (
	"errors"
	"testing"
	"time"
)

// The checkpoint writes the payload a reader may trust without the sidecar, so it must join
// the barrier first: an append the queue has applied but not yet made durable is not part of
// the view the log holds, and freezing it would name a record that may still be refused.
func TestOperationCheckpointJoinsTheBarrierBeforeFreezing(test *testing.T) {
	store, directory := commitQueueStore(test)
	defer store.Close()
	release := gatePop(test, store)
	database := securityDatabase()

	committed := make(chan error, 1)
	go func() { committed <- operationSettled(store, database, "checkpoint_barrier") }()
	waitOrdered(test, store, 1)
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- CheckpointOperations(store) }()
	select {
	case err := <-checkpoint:
		test.Fatalf("a checkpoint returned before the barrier: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-committed; err != nil {
		test.Fatal(err)
	}
	select {
	case err := <-checkpoint:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		test.Fatal("the checkpoint never returned after the barrier")
	}
	final := durableStatePayload(directory, test)
	if !operationsCheckpointMaterialized(final) {
		test.Fatalf("the checkpoint did not materialize its own revision: %+v", final.OperationsCheckpoint)
	}
	if _, found := final.Operations[operationKey(database, "checkpoint_barrier")]; !found {
		test.Fatal("the checkpoint payload lost the append whose barrier returned")
	}
}

// Freezing the store is a refusal, not a barrier: the write path must hand the view lock
// back on that path too, or every later reader and the checkpoint itself waits forever.
func TestOperationCheckpointRefusalReleasesTheViewLock(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	database := securityDatabase()
	if err := operationSettled(store, database, "before_freeze"); err != nil {
		test.Fatal(err)
	}
	if err := CheckpointOperations(store); err != nil {
		test.Fatal(err)
	}
	if err := operationSettled(store, database, "after_freeze"); !errors.Is(err, ErrStateFrozen) {
		test.Fatalf("operations accepted a write after the checkpoint: %v", err)
	}
	frozen := make(chan error, 1)
	go func() { frozen <- CheckpointOperations(store) }()
	select {
	case err := <-frozen:
		if err == nil {
			test.Fatal("checkpoint was recorded twice")
		}
	case <-time.After(5 * time.Second):
		test.Fatal("the refused checkpoint never returned: the view lock was not released")
	}
	view := make(chan error, 1)
	go func() { view <- store.readOperations(func(State) error { return nil }) }()
	select {
	case err := <-view:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		test.Fatal("a read after the refusal never returned: the view lock was not released")
	}
}
