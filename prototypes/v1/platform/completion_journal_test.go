package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func unifiedStore(test *testing.T) (*stateStore, *fileAudit, string) {
	test.Helper()
	directory := test.TempDir()
	state, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	store := state.(*stateStore)
	auditPath := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenCompletionAudit(store, auditPath, true)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	test.Cleanup(func() { _ = audit.Close(); _ = store.Close() })
	return store, audit, directory
}

func TestUnifiedCompletionAuditRecoveryAndCompaction(test *testing.T) {
	store, audit, directory := unifiedStore(test)
	const count = 600
	for index := 0; index < count; index++ {
		if err := audit.Record(completionAuditEvent(fmt.Sprintf("unified%04d", index))); err != nil {
			test.Fatal(err)
		}
	}
	store.maintenance.Wait()
	if store.operations.auditHead.Sequence != count {
		test.Fatal("audit tail not current")
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	recovered := reopened.(*stateStore)
	if recovered.operations.auditHead.Sequence != count {
		test.Fatal("recovery lost archived audit")
	}
	opened, err := OpenCompletionAudit(reopened, recovered.operations.completion.AuditPath, false)
	if err != nil {
		test.Fatal(err)
	}
	defer opened.Close()
	if err := opened.Record(completionAuditEvent("afterrestart")); err != nil {
		test.Fatal(err)
	}
	if recovered.operations.auditHead.Sequence != count+1 {
		test.Fatal("restart did not resume chain")
	}
}

func unifiedGateway(test *testing.T) (*Gateway, *gatewayTestUpstream, string, *stateStore, *fileAudit, string) {
	test.Helper()
	original, gateway, upstream, _, credential := securityGatewayFixture(test)
	store, audit, directory := unifiedStore(test)
	source, err := original.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		state.Organizations, state.Projects, state.Databases = source.Organizations, source.Projects, source.Databases
		state.APIKeys, state.Keys = source.APIKeys, source.Keys
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	gateway.config.State, gateway.config.Audit = store, audit
	return gateway, upstream, credential, store, audit, directory
}

func unifiedOpenSession(test *testing.T, gateway *Gateway, credential string) string {
	test.Helper()
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase()})
	if response.Code != 200 {
		test.Fatalf("open: %d %s", response.Code, response.Body.String())
	}
	var result struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		test.Fatal(err)
	}
	return result.Result.Session
}

func TestUnifiedCompletionGatewayUsesOneBarrier(test *testing.T) {
	gateway, _, credential, store, audit, _ := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	before := store.operations.stats()["operation_journal_syncs_total"]
	sequence := store.operations.auditHead.Sequence
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (1)"})
	if response.Code != 200 {
		test.Fatalf("%d %s", response.Code, response.Body.String())
	}
	after := store.operations.stats()["operation_journal_syncs_total"]
	if after-before != 2 {
		test.Fatalf("intent + unified completion should sync twice, got %d", after-before)
	}
	if store.operations.auditHead.Sequence != sequence+1 || audit.syncCount.Load() != 0 {
		test.Fatal("completion wrote a second audit barrier")
	}
	entries := store.operations.records
	last := entries[len(entries)-1].Payload
	if last.Audit == nil || len(last.Set) != 1 {
		test.Fatal("terminal and audit were not in one envelope")
	}
	for _, record := range last.Set {
		if record.State != "terminal" || record.BusinessOutcome != "completed" {
			test.Fatalf("terminal not visible before reply: %+v", record)
		}
	}
	response = gatewayHTTP(test, gateway, credential, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 {
		test.Fatalf("close: %s", response.Body.String())
	}
}

// Closing sessions of one database must not queue behind each other: each
// settlement queues its record under the namespace fence and pays the barrier
// only after the fence is dropped. The second close here has to reach the queue
// while the first one is still inside its barrier.
func TestUnifiedCompletionDropsNamespaceFenceBeforeBarrier(test *testing.T) {
	gateway, _, credential, store, audit, _ := unifiedGateway(test)
	const closes = 2
	sessions := make([]string, closes)
	for index := range sessions {
		sessions[index] = unifiedOpenSession(test, gateway, credential)
	}
	ordered := operationOrderedTotal(store)
	release := gatePop(test, store)
	var once sync.Once
	open := func() { once.Do(release) }
	defer open()
	codes := make([]int, closes)
	bodies := make([]string, closes)
	var group sync.WaitGroup
	for index := range sessions {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			response := gatewayHTTP(test, gateway, credential, "/v1/sql/close", map[string]any{"session": sessions[index]})
			codes[index], bodies[index] = response.Code, response.Body.String()
		}(index)
	}
	// The first close is parked inside its barrier; the second one is only able
	// to admit itself if the fence was dropped before that barrier.
	waitOrdered(test, store, ordered+1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		audit.queueMutex.Lock()
		admitted := audit.admitted[AuditClassCompletion]
		audit.queueMutex.Unlock()
		if admitted >= closes {
			break
		}
		if time.Now().After(deadline) {
			test.Fatalf("only %d of %d closes were admitted while one barrier was held", admitted, closes)
		}
		time.Sleep(time.Millisecond)
	}
	open()
	group.Wait()
	for index, code := range codes {
		if code != 200 {
			test.Fatalf("close %d: %d %s", index, code, bodies[index])
		}
	}
}

func TestUnifiedCompletionMixedAuditChain(test *testing.T) {
	store, audit, directory := unifiedStore(test)
	var group sync.WaitGroup
	failures := make(chan error, 64)
	for index := 0; index < 64; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			failures <- audit.Record(completionAuditEvent(fmt.Sprintf("mixed%03d", index)))
		}(index)
	}
	group.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			test.Fatal(err)
		}
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*stateStore).operations.auditHead.Sequence != 64 {
		test.Fatal("concurrent audit forked or lost records")
	}
}

func TestUnifiedCompletionArchiveTamperRefused(test *testing.T) {
	store, audit, directory := unifiedStore(test)
	for index := 0; index < 270; index++ {
		if err := audit.Record(completionAuditEvent(fmt.Sprintf("archive%04d", index))); err != nil {
			test.Fatal(err)
		}
	}
	store.maintenance.Wait()
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(directory, "operations", "completion.*.*"))
	if err != nil || len(paths) == 0 {
		test.Fatal("archive missing")
	}
	if err := os.Remove(paths[0]); err != nil {
		test.Fatal(err)
	}
	if recovered, err := OpenStateStore(directory); err == nil {
		recovered.Close()
		test.Fatal("missing audit archive accepted")
	}
}

func TestUnifiedCompletionMigrationRequiresCleanState(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	if audit, err := OpenCompletionAudit(store, path, true); err == nil {
		audit.Close()
		test.Fatal("unclean v1 upgrade accepted")
	}
	if err := CheckpointOperations(store); err != nil {
		test.Fatal(err)
	}
	directory := filepath.Dir(store.disk.path)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	state, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer state.Close()
	audit, err := OpenCompletionAudit(state, path, true)
	if err != nil {
		test.Fatal(err)
	}
	defer audit.Close()
	snapshot, err := state.Snapshot()
	if err != nil || snapshot.OperationStorageVersion != 2 || snapshot.CompletionGenesis == "" {
		test.Fatal("v2 identity absent")
	}
	old := snapshot
	old.OperationStorageVersion = 1
	if err := validateState(old, &snapshot); err == nil {
		test.Fatal("downgrade accepted")
	}
}

func TestUnifiedCompletionCapacityReservesAdmittedCompletion(test *testing.T) {
	store, audit, _ := unifiedStore(test)
	held, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatal(err)
	}
	store.operations.mutex.Lock()
	store.operations.retainedBytes = completionRetentionSoftBytes
	store.operations.mutex.Unlock()
	if extra, err := audit.ReserveCompletion(); !errors.Is(err, ErrAuditOverloaded) {
		if extra != nil {
			extra.Release()
		}
		test.Fatal("full log admitted new request")
	}
	if err := held.Record(completionAuditEvent("alreadyadmitted")); err != nil {
		test.Fatal(err)
	}
	if err := audit.Record(completionAuditEvent("newrequest")); !errors.Is(err, ErrOperationQueueFull) {
		test.Fatalf("unreserved event consumed completion reserve: %v", err)
	}
}

// The envelope adds a hash and JSON punctuation beyond the mutation limit.
// A writer-valid boundary payload used to become a reader-invalid line. The
// synthetic map key isolates codec framing from operation domain validation.
func TestUnifiedCompletionRecordEnvelopeBoundary(test *testing.T) {
	mutation := journalMutation{Version: completionJournalVersion, Revision: 1, Set: map[string]OperationRecord{"": {}}}
	base, err := json.Marshal(mutation)
	if err != nil {
		test.Fatal(err)
	}
	mutation.Set = map[string]OperationRecord{strings.Repeat("x", operationJournalMaximumRecordBytes-len(base)): {}}
	body, err := json.Marshal(mutation)
	if err != nil || len(body) != operationJournalMaximumRecordBytes {
		test.Fatalf("boundary fixture invalid: %d %v", len(body), err)
	}
	line, err := json.Marshal(journalEntry{Payload: mutation, Hash: digestHex(body)})
	if err != nil || len(line) <= operationJournalMaximumRecordBytes || len(line) >= completionRecordReservationBytes {
		test.Fatal("envelope did not straddle the former reader limit")
	}
	journal := &operationJournal{completion: &completionGenesis{Version: completionJournalVersion}}
	entries, consumed := journal.parse(append(line, '\n'))
	if len(entries) != 1 || consumed != int64(len(line)+1) {
		test.Fatal("writer-valid completion envelope refused at recovery boundary")
	}
	mutation.Set = map[string]OperationRecord{strings.Repeat("x", operationJournalMaximumRecordBytes-len(base)+1): {}}
	body, _ = json.Marshal(mutation)
	line, _ = json.Marshal(journalEntry{Payload: mutation, Hash: digestHex(body)})
	entries, consumed = journal.parse(append(line, '\n'))
	if len(entries) != 0 || consumed != 0 {
		test.Fatal("oversized payload accepted because its envelope still fits")
	}
}

// The unified log's maintenance pass archives, snapshots and truncates outside the view
// lock, so a reader never queues behind those barriers. Before this split the whole pass ran
// under the lock and a single reader could wait for a five-figure-microsecond file barrier.
func TestUnifiedCompletionMaintenanceDoesNotHoldTheViewLock(test *testing.T) {
	store, _, _ := unifiedStore(test)
	database := securityDatabase()
	if err := store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "test"}
		state.Projects["alpha"] = Project{ID: "alpha", Organization: "org", Tenant: "tenant_a"}
		state.Databases[database.Key()] = Database{ID: database, Project: "alpha", State: "active"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	primeOperationCompaction(store)
	entered, release := holdOperationCompaction(test, store, "after_snapshot", nil)
	settled := make(chan error, 1)
	go func() { settled <- operationSettled(store, database, "maintaining") }()
	operationAwait(test, entered)
	read := make(chan error, 1)
	go func() {
		read <- store.readOperations(func(state State) error {
			if _, found := state.Operations[operationKey(database, "maintaining")]; !found {
				return errors.New("reader lost the already durable operation")
			}
			return nil
		})
	}()
	select {
	case err := <-read:
		if err != nil {
			release()
			test.Fatalf("reader failed during maintenance: %v", err)
		}
	case <-time.After(2 * time.Second):
		release()
		test.Fatal("a reader waited for the maintenance pass to finish its snapshot")
	}
	release()
	if err := <-settled; err != nil {
		test.Fatalf("append behind the maintenance pass failed: %v", err)
	}
}

// The drainer submits every member of a batch before waiting for its barrier, so
// the members share one view lock. Resolving that lock once per record made the
// audit side queue behind request traffic for the length of a whole batch.
func TestUnifiedCompletionBatchSubmitsUnderOneViewLock(test *testing.T) {
	store, _, _ := unifiedStore(test)
	const count = 8
	entries := make([]operationAuditEntry, count)
	for index := range entries {
		event := completionAuditEvent(fmt.Sprintf("batch%02d", index))
		entries[index] = operationAuditEntry{event: &event, reserved: true}
	}
	locks := 0
	store.testOperationLock = func() { locks++ }
	waits, failures := store.submitOperationsAuditBatch(entries)
	if locks != 1 {
		test.Fatalf("batch submissions entered the view lock %d times", locks)
	}
	for index, failure := range failures {
		if failure != nil {
			test.Fatalf("entry %d refused: %v", index, failure)
		}
	}
	for index, wait := range waits {
		if wait == nil {
			test.Fatalf("entry %d has no barrier", index)
		}
		if err := wait(); err != nil {
			test.Fatalf("entry %d barrier failed: %v", index, err)
		}
	}
	if store.operations.auditHead.Sequence != count {
		test.Fatalf("audit head=%d after %d entries", store.operations.auditHead.Sequence, count)
	}
}
