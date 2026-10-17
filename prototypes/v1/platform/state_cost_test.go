package platform

import (
	"fmt"
	"testing"
)

// The acceptance for local operation mutations is read from these counters: a commit
// charges one barrier, takes the shared lock twice (once for its edit and once to settle the
// ordered append, with the barrier itself outside the lock), and does no full-set work until
// the view is replaced wholesale.
func TestStateOperationCostIsObservable(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	defer store.Close()
	database := securityDatabase()
	commit := func(identifier string) {
		test.Helper()
		if err := beginOperation(store, database, identifier, "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
	}
	counter := func(name string) uint64 { return store.operationStats()[name].(uint64) }
	waits, commits, appends, syncs, rebuilds := counter("state_lock_waits_total"), counter("operation_commits_total"), counter("operation_journal_appends_total"), counter("operation_journal_syncs_total"), counter("operation_index_rebuilds_total")
	for index := 0; index < 4; index++ {
		commit(fmt.Sprintf("cost_%04d", index))
	}
	if counter("operation_commits_total")-commits != 8 || counter("operation_journal_appends_total")-appends != 8 {
		test.Fatalf("commits = %d, appends = %d, want 8 each", counter("operation_commits_total")-commits, counter("operation_journal_appends_total")-appends)
	}
	if counter("operation_journal_syncs_total")-syncs != 8 {
		test.Fatalf("syncs = %d, want one barrier per commit", counter("operation_journal_syncs_total")-syncs)
	}
	// The barrier is split into its separable costs so a slow batch can be attributed
	// without timing the request path. Every phase must have been exercised by commits
	// that actually reached the file.
	for _, name := range []string{
		"operation_journal_encode_ns_total",
		"operation_journal_verify_ns_total",
		"operation_journal_assemble_ns_total",
		"operation_journal_write_ns_total",
		"operation_journal_sync_ns_total",
		"operation_journal_witness_ns_total",
	} {
		if counter(name) == 0 {
			test.Fatalf("%s = 0 after durable commits", name)
		}
	}
	// Two acquisitions per commit: one applies the edit, the other settles the ordered
	// append. Neither of them covers the barrier, so what this records is lock contention
	// rather than flush latency.
	if counter("state_lock_waits_total")-waits != 16 {
		test.Fatalf("lock waits = %d, want two per commit (edit and settle)", counter("state_lock_waits_total")-waits)
	}
	if counter("operation_index_rebuilds_total")-rebuilds != 1 {
		test.Fatalf("index rebuilds = %d, want the first commit only", counter("operation_index_rebuilds_total")-rebuilds)
	}
	retained := uint64(len(store.state.Operations))
	// A registry commit replaces the whole view, so the next commit rebuilds the index
	// once; every commit after that must not pay for the retained set again.
	if err := store.Update(func(*State) error { return nil }); err != nil {
		test.Fatal(err)
	}
	commit("cost_after_registry")
	if rebuilt := counter("operation_index_rebuilds_total"); rebuilt-rebuilds != 2 || counter("operation_index_entries_total") != retained {
		test.Fatalf("rebuild after registry commit = %d/%d, want %d/%d", rebuilt-rebuilds, counter("operation_index_entries_total"), 2, retained)
	}
	commit("cost_settled")
	if rebuilt := counter("operation_index_rebuilds_total"); rebuilt-rebuilds != 2 {
		test.Fatalf("steady commit rebuilt the index: %d", rebuilt-rebuilds)
	}
	exposed := stateOperationCost(store)
	if exposed == nil || exposed["operation_commits_total"] != counter("operation_commits_total") {
		test.Fatalf("gateway metric projection = %v", exposed)
	}
}
