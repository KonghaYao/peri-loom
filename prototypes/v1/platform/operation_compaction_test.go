package platform

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func primeOperationCompaction(store *stateStore) {
	store.operations.mutex.Lock()
	// Exercise the production threshold without charging every lock regression
	// test 255 independent filesystem barriers first.
	store.operations.appends = operationJournalCompactRecords - 1
	store.operations.mutex.Unlock()
}

func holdOperationCompaction(test *testing.T, store *stateStore, phase string, failure error) (<-chan struct{}, func()) {
	test.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releasedOnce sync.Once
	store.operations.testCompactionGate = func(current string) error {
		if current == phase {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return failure
		}
		return nil
	}
	unblock := func() { releasedOnce.Do(func() { close(release) }) }
	test.Cleanup(func() {
		unblock()
		store.Close()
	})
	return entered, unblock
}

func awaitCompactionResult(test *testing.T, result <-chan error) error {
	test.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		test.Fatal("operation compaction did not finish")
		return nil
	}
}

func TestOperationCompactionDoesNotHoldRegistryReaders(test *testing.T) {
	store, directory := commitQueueStore(test)
	primeOperationCompaction(store)
	entered, release := holdOperationCompaction(test, store, "before_snapshot", nil)
	database := securityDatabase()
	first := make(chan error, 1)
	go func() { first <- operationSettled(store, database, "compacting") }()
	operationAwait(test, entered)
	read := make(chan error, 1)
	go func() {
		if _, _, err := store.credentialByDigest("absent"); err != nil {
			read <- err
			return
		}
		if _, err := store.authorizationState("absent", "project", database); err != nil {
			read <- err
			return
		}
		if _, err := store.gatewayRequestState("project", &database); err != nil {
			read <- err
			return
		}
		if err := store.readOperations(func(state State) error {
			record, found := state.Operations[operationKey(database, "compacting")]
			if !found || record.State != "terminal" {
				return errors.New("reader lost the already durable operation")
			}
			return nil
		}); err != nil {
			read <- err
			return
		}
		store.operationStats()
		_, err := store.Snapshot()
		read <- err
	}()
	if err := awaitCompactionResult(test, read); err != nil {
		test.Fatal(err)
	}
	next := make(chan error, 1)
	go func() { next <- operationSettled(store, database, "after_compaction") }()
	select {
	case err := <-first:
		test.Fatalf("triggering request skipped its compaction barrier: %v", err)
	case err := <-next:
		test.Fatalf("new mutation crossed the compaction gate: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if operationUnconfirmedCount(store) != 0 || len(operationViewSnapshot(store)) != 1 {
		test.Fatal("a waiting mutation exposed an optimistic edit during compaction")
	}
	release()
	for _, result := range []<-chan error{first, next} {
		if err := awaitCompactionResult(test, result); err != nil {
			test.Fatal(err)
		}
	}
	stats := store.operations.stats()
	if stats["operation_journal_compactions_total"] != 1 || stats["operation_journal_compaction_failures_total"] != 0 {
		test.Fatalf("unexpected compaction result counters: %v", stats)
	}
	for _, name := range []string{"operation_journal_compaction_max_ns", "operation_journal_compaction_snapshot_ns_total", "operation_journal_compaction_sync_ns_total", "operation_journal_compaction_truncate_ns_total"} {
		if stats[name] == 0 {
			test.Fatalf("compaction phase not observable: %s", name)
		}
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := journalReopen(test, directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if len(reopened.state.Operations) != 2 {
		test.Fatal("snapshot and following append did not survive reopen")
	}
}

func TestOperationCompactionFailureGatesWritersAndCheckpoint(test *testing.T) {
	for _, phase := range []string{"before_snapshot", "after_snapshot", "before_truncate"} {
		test.Run(phase, func(test *testing.T) {
			store, directory := commitQueueStore(test)
			primeOperationCompaction(store)
			injected := errors.New("injected compaction failure")
			entered, release := holdOperationCompaction(test, store, phase, injected)
			trigger := make(chan error, 1)
			go func() { trigger <- operationSettled(store, securityDatabase(), "durable") }()
			operationAwait(test, entered)
			mutation, checkpoint, registry := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			var registryApplied atomic.Bool
			go func() { mutation <- operationSettled(store, securityDatabase(), "refused") }()
			go func() { checkpoint <- CheckpointOperations(store) }()
			go func() {
				registry <- store.Update(func(*State) error { registryApplied.Store(true); return nil })
			}()
			select {
			case err := <-mutation:
				test.Fatalf("mutation crossed unresolved compaction: %v", err)
			case err := <-checkpoint:
				test.Fatalf("checkpoint crossed unresolved compaction: %v", err)
			case err := <-registry:
				test.Fatalf("registry commit crossed unresolved compaction: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			release()
			if err := awaitCompactionResult(test, trigger); !errors.Is(err, injected) {
				test.Fatalf("trigger did not report its compaction failure: %v", err)
			}
			for _, result := range []<-chan error{mutation, checkpoint, registry} {
				if err := awaitCompactionResult(test, result); !errors.Is(err, ErrStateUncertain) {
					test.Fatalf("waiter did not fail closed: %v", err)
				}
			}
			if registryApplied.Load() || operationsCheckpointMaterialized(durableStatePayload(directory, test)) {
				test.Fatal("compaction failure was bypassed by a registry or shutdown commit")
			}
			if store.operations.stats()["operation_journal_compaction_failures_total"] != 1 {
				test.Fatal("compaction failure was not counted")
			}
			if err := store.Close(); !errors.Is(err, injected) {
				test.Fatalf("close lost the compaction failure: %v", err)
			}
			reopened, err := journalReopen(test, directory)
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			if len(reopened.state.Operations) != 1 || reopened.state.Operations[operationKey(securityDatabase(), "durable")].State != "terminal" {
				test.Fatal("interrupted compaction lost or changed the durable operation")
			}
		})
	}
}

func TestOperationCompactionCloseJoinsAndRefusesNewMutations(test *testing.T) {
	store, _ := commitQueueStore(test)
	primeOperationCompaction(store)
	entered, release := holdOperationCompaction(test, store, "before_snapshot", nil)
	trigger := make(chan error, 1)
	go func() { trigger <- operationSettled(store, securityDatabase(), "durable") }()
	operationAwait(test, entered)
	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		unlock := store.lockState()
		closing := store.maintenanceClosing
		unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("close did not stop compaction admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		test.Fatalf("close did not join compaction: %v", err)
	default:
	}
	next := make(chan error, 1)
	go func() { next <- operationSettled(store, securityDatabase(), "after_close") }()
	release()
	for _, result := range []<-chan error{trigger, closed} {
		if err := awaitCompactionResult(test, result); err != nil {
			test.Fatal(err)
		}
	}
	if err := awaitCompactionResult(test, next); !errors.Is(err, ErrStateClosed) {
		test.Fatalf("closing store accepted a waiting mutation: %v", err)
	}
}

func TestOperationCompactionAlreadyCommittedWaitersShareOneSnapshot(test *testing.T) {
	store, _ := commitQueueStore(test)
	primeOperationCompaction(store)
	entered, release := holdOperationCompaction(test, store, "before_snapshot", nil)
	pop := make(chan struct{})
	var popOnce sync.Once
	releaseAppends := func() { popOnce.Do(func() { close(pop) }) }
	store.operations.testPopGate = func() { <-pop }
	test.Cleanup(releaseAppends)
	results := make([]chan error, 3)
	for index := range results {
		results[index] = make(chan error, 1)
		go func() {
			results[index] <- operationSettled(store, securityDatabase(), fmt.Sprintf("coalesced_%d", index))
		}()
	}
	waitOrdered(test, store, 3)
	releaseAppends()
	operationAwait(test, entered)
	if operationUnconfirmedCount(store) != 0 || len(operationViewSnapshot(store)) != 3 {
		test.Fatal("compaction did not capture the whole confirmed batch")
	}
	release()
	for _, result := range results {
		if err := awaitCompactionResult(test, result); err != nil {
			test.Fatal(err)
		}
	}
	if store.operations.stats()["operation_journal_compactions_total"] != 1 {
		test.Fatal("already committed waiters compacted the same prefix twice")
	}
}
