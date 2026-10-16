package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// commitQueueStore opens a durable store and returns its directory so a test can reopen it
// the way a restarted process would.
func commitQueueStore(test *testing.T) (*stateStore, string) {
	test.Helper()
	store := operationDurableStore(test).(*stateStore)
	return store, filepath.Dir(store.disk.path)
}

// operationSettled orders one durable settled record through the store's own commit path. It
// skips the namespace fence on purpose: production serializes same-namespace commits with
// that fence, and the queue only merges appends that actually overlap. What this helper
// exercises is the queue every fence-wrapped caller funnels into. The record is settled and
// known-terminal, so recovery leaves it exactly as it was written.
func operationSettled(store *stateStore, database controller.DatabaseID, identifier string) error {
	digest, err := freezeCommandValue(nil)
	if err != nil {
		return err
	}
	return store.updateOperations(func(edit *operationEdit) error {
		if edit.has(operationKey(database, identifier)) {
			return ErrOperationExists
		}
		record := seedOperationRecord(edit, database, identifier, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", digest.digest())
		record.State, record.BusinessOutcome, record.ExecutionProven, record.CleanupOutcome = "terminal", "completed", true, "not_applicable"
		edit.put(operationKey(database, identifier), record)
		return nil
	})
}

// gatePop holds the committer before it chooses a batch, so every append submitted while it
// is held belongs to one batch. gateWrite holds it inside a batch, so exactly that batch is
// in flight and later appends stay queued. Neither gate holds a submitter: the queue keeps
// accepting appends while either is held.
func gatePop(test *testing.T, store *stateStore) func() {
	test.Helper()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	store.operations.testPopGate = func() {
		once.Do(func() { entered <- struct{}{} })
		<-release
	}
	test.Cleanup(func() { store.operations.testPopGate = nil })
	return func() {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			test.Fatal("the committer never reached its barrier")
		}
		close(release)
	}
}

func gateWrite(test *testing.T, store *stateStore) func() {
	test.Helper()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	store.operations.testWriteGate = func() {
		once.Do(func() { entered <- struct{}{} })
		<-release
	}
	test.Cleanup(func() { store.operations.testWriteGate = nil })
	return func() {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			test.Fatal("the committer never reached its barrier")
		}
		close(release)
	}
}

// operationOrderedTotal counts the appends that were ordered, which stays readable while a
// barrier is outstanding: it is the same counter an operator reads.
func operationOrderedTotal(store *stateStore) uint64 {
	return store.operations.stats()["operation_journal_ordered_total"]
}

func operationQueuedDepth(store *stateStore) uint64 {
	return store.operations.stats()["operation_journal_queued"]
}

func operationUnconfirmedCount(store *stateStore) int {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	return len(store.unconfirmed)
}

// operationViewSnapshot copies the view under the view lock, so a test never reads the map
// while a writer could be touching it.
func operationViewSnapshot(store *stateStore) map[string]OperationRecord {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	return maps.Clone(store.state.Operations)
}

func waitOrdered(test *testing.T, store *stateStore, count uint64) {
	test.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for operationOrderedTotal(store) < count {
		if time.Now().After(deadline) {
			test.Fatalf("ordered %d appends, want %d", operationOrderedTotal(store), count)
		}
		time.Sleep(time.Millisecond)
	}
}

// A batch pays one barrier for the appends already waiting, and that barrier leaves the chain
// head at the last append it covered.
func TestOperationCommitQueueCoalescesWaitingAppends(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	journal := store.operations
	database := securityDatabase()
	release := gatePop(test, store)
	start := map[string]uint64{"syncs": journal.syncs, "appends": journal.appendsTotal, "coalesced": journal.coalesced, "waits": journal.waitNS}
	const appends = 3

	var group sync.WaitGroup
	failures := make([]error, appends)
	for index := range failures {
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = operationSettled(store, database, fmt.Sprintf("coalesced_%d", index))
		}()
	}
	waitOrdered(test, store, appends)
	release()
	group.Wait()
	for index, failure := range failures {
		if failure != nil {
			test.Fatalf("append %d failed: %v", index, failure)
		}
	}
	if syncs := journal.syncs - start["syncs"]; syncs != 1 {
		test.Fatalf("%d waiting appends paid %d barriers, want 1", appends, syncs)
	}
	if count := journal.appendsTotal - start["appends"]; count != appends {
		test.Fatalf("appends = %d, want %d", count, appends)
	}
	if coalesced := journal.coalesced - start["coalesced"]; coalesced != appends-1 {
		test.Fatalf("coalesced = %d, want %d", coalesced, appends-1)
	}
	if journal.waitNS == start["waits"] {
		test.Fatal("coalesced waiters recorded no wait time")
	}
	if journal.batchMaximum < appends || journal.queueMaximum < appends {
		test.Fatalf("batch/queue maxima = %d/%d, want at least %d", journal.batchMaximum, journal.queueMaximum, appends)
	}
	if journal.revision != appends {
		test.Fatalf("chain head = %d, want %d", journal.revision, appends)
	}
	view := operationViewSnapshot(store)
	for index := range failures {
		record, found := view[operationKey(database, fmt.Sprintf("coalesced_%d", index))]
		if !found || record.State != "terminal" {
			test.Fatalf("coalesced append %d is not in the view: %+v", index, record.OperationStatus)
		}
	}
}

// A failed barrier is reported to every waiter in its batch, and none of their edits may
// survive as though the append had been confirmed.
func TestOperationCommitQueueFailureFansOutToBatchWaiters(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	journal := store.operations
	// A read-only descriptor fails the write deterministically. It is installed before any
	// submit, so no goroutine observes the swap.
	readonly, err := os.Open(journal.path)
	if err != nil {
		test.Fatal(err)
	}
	journal.file = readonly
	release := gatePop(test, store)
	database := securityDatabase()
	before := operationViewSnapshot(store)
	const appends = 3

	var group sync.WaitGroup
	failures := make([]error, appends)
	for index := range failures {
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = operationSettled(store, database, fmt.Sprintf("failed_%d", index))
		}()
	}
	waitOrdered(test, store, appends)
	release()
	group.Wait()
	first := failures[0]
	if !errors.Is(first, ErrStateUncertain) {
		test.Fatalf("failed batch reported %v", first)
	}
	for index, failure := range failures {
		if failure != first {
			test.Fatalf("waiter %d saw %v, want the batch's own failure", index, failure)
		}
	}
	if !store.disk.poisoned {
		test.Fatal("a failed barrier left the store available")
	}
	if !maps.Equal(before, operationViewSnapshot(store)) {
		test.Fatal("a failed barrier left unconfirmed records in the view")
	}
	if operationUnconfirmedCount(store) != 0 {
		test.Fatal("a failed barrier left unconfirmed edits behind")
	}
	if err := operationSettled(store, database, "after_failure"); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("poisoned store accepted a later append: %v", err)
	}
}

// The queue is bounded: a stalled barrier refuses new appends instead of growing, and that
// refusal poisons nothing and leaves no edit behind.
func TestOperationCommitQueueBoundedRefusal(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	journal := store.operations
	release := gatePop(test, store)
	database := securityDatabase()
	before := operationViewSnapshot(store)
	attempts := operationJournalQueueMaximum + 8

	var group sync.WaitGroup
	failures := make([]error, attempts)
	for index := range failures {
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = operationSettled(store, database, fmt.Sprintf("bounded_%04d", index))
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	for operationQueuedDepth(store) < operationJournalQueueMaximum {
		if time.Now().After(deadline) {
			test.Fatalf("queue depth %d never reached the limit %d while %d appends were waiting", operationQueuedDepth(store), operationJournalQueueMaximum, attempts)
		}
		time.Sleep(time.Millisecond)
	}
	if journal.queueMaximum > operationJournalQueueMaximum {
		test.Fatalf("queue depth %d exceeded the limit %d", journal.queueMaximum, operationJournalQueueMaximum)
	}
	if journal.stats()["operation_journal_queued"] > operationJournalQueueMaximum {
		test.Fatal("the queue grew past its limit")
	}
	if operationUnconfirmedCount(store) > operationJournalQueueMaximum+1 {
		test.Fatal("the queue kept edits that were never accepted")
	}
	if store.disk.poisoned {
		test.Fatal("a full queue poisoned the store")
	}
	release()
	group.Wait()
	accepted, refused := 0, 0
	for _, failure := range failures {
		switch {
		case failure == nil:
			accepted++
		case errors.Is(failure, ErrOperationQueueFull):
			refused++
		default:
			test.Fatalf("a bounded queue reported %v", failure)
		}
	}
	if refused == 0 {
		test.Fatal("a full queue accepted every append")
	}
	if accepted != attempts-refused {
		test.Fatalf("accepted = %d, want %d of %d attempts", accepted, attempts-refused, attempts)
	}
	view := operationViewSnapshot(store)
	for key, record := range view {
		if _, existed := before[key]; existed {
			continue
		}
		if record.State != "terminal" {
			test.Fatalf("accepted append %s is not a settled record", key)
		}
	}
	present := 0
	for index := range failures {
		if _, found := view[operationKey(database, fmt.Sprintf("bounded_%04d", index))]; found {
			present++
		}
	}
	// Settled records are retained up to the known-terminal limit, so a burst larger than
	// that keeps exactly the most recent window.
	expected := min(accepted, operationKnownRetention)
	if present != expected {
		test.Fatalf("view holds %d accepted appends, want %d", present, expected)
	}
}

// A read that answers with the operations view settles the ordered appends first, so it can
// never report a record whose barrier has not returned. A read that does not look at
// operations is not blocked by that barrier: this is the boundary the queue moved.
func TestOperationNarrowReadIsNotBlockedByASlowBarrier(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	release := gatePop(test, store)
	database := securityDatabase()

	committed := make(chan error, 1)
	go func() { committed <- operationSettled(store, database, "slow_barrier") }()
	waitOrdered(test, store, 1)

	// The unconfirmed edit is in the view, so a narrow read must neither report it nor wait
	// for the barrier.
	narrow := make(chan error, 1)
	go func() {
		state, err := readGatewayRequestState(store, "alpha", nil)
		if err == nil && len(state.Operations) != 0 {
			err = errors.New("narrow read carried a pending operation")
		}
		narrow <- err
	}()
	select {
	case err := <-narrow:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("a narrow read queued behind the barrier")
	}

	// A read that answers with operations must not return while the barrier is outstanding.
	settled := make(chan error, 1)
	go func() {
		settled <- store.readOperations(func(state State) error {
			if _, found := state.Operations[operationKey(database, "slow_barrier")]; !found {
				return errors.New("settled read lost the committed append")
			}
			return nil
		})
	}()
	select {
	case err := <-settled:
		test.Fatalf("an operations read returned before the barrier: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-committed; err != nil {
		test.Fatal(err)
	}
	if err := <-settled; err != nil {
		test.Fatal(err)
	}
	if store.operations.syncs == 0 {
		test.Fatal("the observed barrier was never paid")
	}
}

// Closing stops the queue: appends that were never written fail, the barrier already in
// flight still reports its own result, and only that one is durable afterwards.
func TestOperationCommitQueueCloseDrainsOrderedAppends(test *testing.T) {
	store, directory := commitQueueStore(test)
	release := gateWrite(test, store)
	database := securityDatabase()

	written := make(chan error, 1)
	go func() { written <- operationSettled(store, database, "in_batch") }()
	waitOrdered(test, store, 1)
	deadline := time.Now().Add(10 * time.Second)
	for operationQueuedDepth(store) != 0 {
		if time.Now().After(deadline) {
			test.Fatal("the first append never entered its batch")
		}
		time.Sleep(time.Millisecond)
	}
	queued := make(chan error, 1)
	go func() { queued <- operationSettled(store, database, "queued") }()
	for operationQueuedDepth(store) == 0 {
		if time.Now().After(deadline) {
			test.Fatal("the second append never queued")
		}
		time.Sleep(time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	select {
	case err := <-closed:
		test.Fatalf("close returned before the barrier finished: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-written; err != nil {
		test.Fatalf("the appended batch lost its own result: %v", err)
	}
	if err := <-queued; !errors.Is(err, ErrStateClosed) {
		test.Fatalf("a queued append survived close: %v", err)
	}
	if err := <-closed; err != nil {
		test.Fatalf("close reported %v", err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if _, found := state.Operations[operationKey(database, "in_batch")]; !found {
		test.Fatal("the append whose barrier returned was not durable")
	}
	if _, found := state.Operations[operationKey(database, "queued")]; found {
		test.Fatal("an append that was never written became durable")
	}
}

// A batch whose tail never landed recovers as its verified prefix, and the chain continues
// from that prefix instead of leaving a gap behind it.
func TestOperationCommitQueueRecoversTornBatchTail(test *testing.T) {
	store, directory := commitQueueStore(test)
	journal := store.operations
	database := securityDatabase()
	for _, identifier := range []string{"one", "two", "three"} {
		if err := operationSettled(store, database, identifier); err != nil {
			test.Fatal(err)
		}
	}
	before := operationViewSnapshot(store)
	if journal.revision != 3 {
		test.Fatalf("chain head = %d, want 3", journal.revision)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	prefix := journalBytes(test, store)
	file, err := os.OpenFile(journal.path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := file.Write([]byte(`{"payload":{"version":1,"revision":4,"base_revision`)); err != nil {
		test.Fatal(err)
	}
	if err := file.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if !maps.Equal(before, state.Operations) {
		test.Fatal("a torn batch tail dropped or invented records")
	}
	survivor := reopened.(*stateStore)
	if survivor.operations.revision != 3 {
		test.Fatalf("recovered head = %d, want the verified prefix 3", survivor.operations.revision)
	}
	if journalBytes(test, survivor) != prefix {
		test.Fatal("recovery kept the unverifiable tail")
	}
	if err := operationSettled(survivor, database, "after_torn"); err != nil {
		test.Fatal(err)
	}
	if survivor.operations.revision != 4 {
		test.Fatal("the chain did not continue after the torn tail")
	}
}

// Concurrent appends through the production entry point keep both orders: the order on disk
// and the view a reader sees once every append is confirmed. The namespace fence serializes
// same-namespace commits here, so this asserts order and durability rather than coalescing.
func TestOperationCommitQueueKeepsConcurrentOrder(test *testing.T) {
	store, directory := commitQueueStore(test)
	journal := store.operations
	database := securityDatabase()
	const appends = 24
	var group sync.WaitGroup
	identifiers := make([]string, appends)
	failures := make([]error, appends)
	for index := range failures {
		identifiers[index] = fmt.Sprintf("ordered_%02d", index)
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = beginOperation(store, database, identifiers[index], "gateway", "execute", "tool_rpc", "worker-a", "", nil)
		}()
	}
	group.Wait()
	for index, failure := range failures {
		if failure != nil {
			test.Fatalf("append %d failed: %v", index, failure)
		}
	}
	// The registration is one append and its settlement is another; a settled record is what
	// recovery preserves, so the test settles every registration before comparing views.
	for index := range identifiers {
		if err := finishOperation(store, database, identifiers[index], nil, true); err != nil {
			test.Fatalf("settling append %d failed: %v", index, err)
		}
	}
	if journal.revision != 2*appends {
		test.Fatalf("chain head = %d, want %d", journal.revision, 2*appends)
	}
	view := operationViewSnapshot(store)
	sequence := store.state.OperationSequence
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if !maps.Equal(view, state.Operations) || state.OperationSequence != sequence {
		test.Fatal("recovery disagreed with the confirmed view")
	}
	sequences := map[uint64]bool{}
	for _, identifier := range identifiers {
		record, found := state.Operations[operationKey(database, identifier)]
		if !found {
			test.Fatalf("recovery lost %s", identifier)
		}
		if sequences[record.RegistrationSequence] {
			test.Fatalf("registration sequence %d was reused", record.RegistrationSequence)
		}
		sequences[record.RegistrationSequence] = true
	}
	if len(sequences) != appends {
		test.Fatalf("distinct registration sequences = %d, want %d", len(sequences), appends)
	}
	for sequence := uint64(1); sequence <= appends; sequence++ {
		if !sequences[sequence] {
			test.Fatalf("registration sequence %d is missing", sequence)
		}
	}
}

// A snapshot carries operations, so it must wait for the barrier and then report the record
// exactly once: it is the path every management reader and the gateway startup use.
func TestOperationSnapshotWaitsForItsAppend(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	release := gatePop(test, store)
	database := securityDatabase()

	committed := make(chan error, 1)
	go func() { committed <- operationSettled(store, database, "snapshot_barrier") }()
	waitOrdered(test, store, 1)
	snapshot := make(chan State, 1)
	failures := make(chan error, 1)
	go func() {
		state, err := store.Snapshot()
		if err != nil {
			failures <- err
			return
		}
		snapshot <- state
	}()
	select {
	case state := <-snapshot:
		test.Fatalf("a snapshot returned before the barrier, reporting %d operations", len(state.Operations))
	case err := <-failures:
		test.Fatalf("a snapshot failed before the barrier: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-committed; err != nil {
		test.Fatal(err)
	}
	select {
	case state := <-snapshot:
		if _, found := state.Operations[operationKey(database, "snapshot_barrier")]; !found {
			test.Fatal("the settled snapshot lost the appended record")
		}
	case err := <-failures:
		test.Fatal(err)
	case <-time.After(5 * time.Second):
		test.Fatal("the snapshot never returned after the barrier")
	}
}

// The registry payload is the base the log's next deltas bind to, so an append whose barrier
// failed must make the registry commit fail instead of being recorded as acknowledged.
func TestOperationRegistryCommitRefusesAnUnconfirmedAppend(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	database := securityDatabase()
	readonly, err := os.Open(store.operations.path)
	if err != nil {
		test.Fatal(err)
	}
	store.operations.file = readonly
	release := gatePop(test, store)

	committed := make(chan error, 1)
	go func() { committed <- operationSettled(store, database, "unconfirmed") }()
	waitOrdered(test, store, 1)
	registry := make(chan error, 1)
	go func() { registry <- store.Update(func(*State) error { return nil }) }()
	select {
	case err := <-registry:
		test.Fatalf("the registry commit returned before the barrier: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-committed; !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("the failed append reported %v", err)
	}
	if err := <-registry; !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("the registry commit reported %v, want the append's failure", err)
	}
	store.disk.mutex.Lock()
	payload := append([]byte(nil), store.disk.payload...)
	store.disk.mutex.Unlock()
	var persisted State
	if err := json.Unmarshal(payload, &persisted); err != nil {
		test.Fatal(err)
	}
	if _, found := persisted.Operations[operationKey(database, "unconfirmed")]; found {
		test.Fatal("the registry payload recorded an append whose barrier failed")
	}
}

// A failed append is rolled back; an append whose barrier returned is not, because its bytes
// are durable and the view must not contradict the log it has already acknowledged.
func TestOperationSettleRollsBackOnlyFailedAppends(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	database := securityDatabase()
	confirmedKey := operationKey(database, "confirmed")
	failedKey := operationKey(database, "failed")

	closed := make(chan struct{})
	close(closed)
	unlock := store.lockState()
	store.operationsIndex = newOperationIndex(store.state.Operations)
	for _, entry := range []struct {
		identifier string
		failure    error
	}{
		{identifier: "confirmed"},
		{identifier: "failed", failure: ErrStateUncertain},
	} {
		edit := newOperationEdit(&store.state, store.operationsIndex)
		record := seedOperationRecord(edit, database, entry.identifier, entry.identifier, "gateway", "execute", "tool_rpc", "worker-a", "", "")
		record.State, record.BusinessOutcome, record.ExecutionProven, record.CleanupOutcome = "terminal", "completed", true, "not_applicable"
		edit.put(operationKey(database, entry.identifier), record)
		store.unconfirmed = append(store.unconfirmed, operationPendingEdit{
			commit: &operationCommit{settled: closed, err: entry.failure},
			revert: edit.revert,
		})
	}
	sequence, evictions := store.state.OperationSequence, store.state.OperationEvictions
	failure := store.settleOperations()
	if !errors.Is(failure, ErrStateUncertain) {
		test.Fatalf("settling reported %v", failure)
	}
	if _, found := store.state.Operations[confirmedKey]; !found {
		test.Fatal("a confirmed append was rolled back out of the view")
	}
	if _, found := store.state.Operations[failedKey]; found {
		test.Fatal("a failed append stayed in the view")
	}
	if store.state.OperationSequence == sequence && store.state.OperationEvictions != evictions {
		test.Fatal("settlement moved the retention counters inconsistently")
	}
	if !store.disk.poisoned {
		test.Fatal("an uncertain append did not poison the store")
	}
	if len(store.unconfirmed) != 0 {
		test.Fatal("settlement left unconfirmed edits behind")
	}
	unlock()
}

// The batch bound is enforced, not just declared: a burst larger than one batch is split, all
// of it is durable, and no batch exceeds the limit.
func TestOperationCommitQueueKeepsBatchBounded(test *testing.T) {
	store, directory := commitQueueStore(test)
	journal := store.operations
	database := securityDatabase()
	release := gatePop(test, store)
	start := map[string]uint64{"syncs": journal.syncs, "appends": journal.appendsTotal}
	const appends = operationJournalBatchMaximum + 8

	var group sync.WaitGroup
	failures := make([]error, appends)
	for index := range failures {
		group.Add(1)
		go func() {
			defer group.Done()
			failures[index] = operationSettled(store, database, fmt.Sprintf("batched_%02d", index))
		}()
	}
	waitOrdered(test, store, appends)
	release()
	group.Wait()
	for index, failure := range failures {
		if failure != nil {
			test.Fatalf("append %d failed: %v", index, failure)
		}
	}
	if journal.batchMaximum > operationJournalBatchMaximum {
		test.Fatalf("a batch held %d appends, limit %d", journal.batchMaximum, operationJournalBatchMaximum)
	}
	if syncs := journal.syncs - start["syncs"]; syncs != 2 {
		test.Fatalf("%d waiting appends paid %d barriers, want 2 batches", appends, syncs)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	// Settled records are retained up to the known-terminal limit, which this burst exceeds.
	retained := min(appends, operationKnownRetention)
	if len(state.Operations) != retained {
		test.Fatalf("recovery held %d records, want %d", len(state.Operations), retained)
	}
}

// The namespace fence is not relaxed: two commits for one namespace cannot even be ordered
// while the first is waiting, so they never share a barrier.
func TestOperationNamespaceFenceStillSerializesCommits(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	journal := store.operations
	database := securityDatabase()
	release := gateWrite(test, store)
	start := map[string]uint64{"syncs": journal.syncs}

	first := make(chan error, 1)
	go func() {
		first <- beginOperation(store, database, "fenced_first", "gateway", "execute", "tool_rpc", "worker-a", "", nil)
	}()
	waitOrdered(test, store, 1)
	second := make(chan error, 1)
	go func() {
		second <- beginOperation(store, database, "fenced_second", "gateway", "execute", "tool_rpc", "worker-a", "", nil)
	}()
	time.Sleep(200 * time.Millisecond)
	if ordered := operationOrderedTotal(store); ordered != 1 {
		test.Fatalf("a second same-namespace append was ordered behind the fence: %d", ordered)
	}
	release()
	if err := <-first; err != nil {
		test.Fatal(err)
	}
	if err := <-second; err != nil {
		test.Fatal(err)
	}
	if syncs := journal.syncs - start["syncs"]; syncs != 2 {
		test.Fatalf("same-namespace appends shared a barrier: %d", syncs)
	}
}
