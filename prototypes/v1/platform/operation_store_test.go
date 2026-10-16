package platform

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func journalBytes(test *testing.T, store *stateStore) int64 {
	test.Helper()
	info, err := os.Stat(store.operations.path)
	if err != nil {
		test.Fatal(err)
	}
	return info.Size()
}

// Enabled explicitly so host disk load never leaks into the regular regression.
func TestWritePathCost(test *testing.T) {
	if os.Getenv("PERI_WRITE_COST") != "1" {
		test.Skip("PERI_WRITE_COST=1")
	}
	for _, retained := range []int{0, 256} {
		test.Run(fmt.Sprint(retained), func(test *testing.T) {
			service := operationDurableStore(test)
			defer service.Close()
			store := service.(*stateStore)
			database := securityDatabase()
			// Build the retained set in memory, then persist it through the real path.
			seed := &retentionTestStore{state: store.state}
			for index := 0; index < retained; index++ {
				identifier := fmt.Sprintf("seed_%04d", index)
				if err := beginOperation(seed, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
					test.Fatal(err)
				}
				if err := finishOperation(seed, database, identifier, nil, true); err != nil {
					test.Fatal(err)
				}
			}
			if err := store.updateOperations(func(state *State) error {
				state.Operations = seed.state.Operations
				state.OperationSequence = seed.state.OperationSequence
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			start := journalBytes(test, store)
			var register, settle time.Duration
			for index := 0; index < 12; index++ {
				identifier := fmt.Sprintf("measure_%04d", index)
				opened := time.Now()
				if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
					test.Fatal(err)
				}
				register += time.Since(opened)
				opened = time.Now()
				if err := finishOperation(store, database, identifier, nil, true); err != nil {
					test.Fatal(err)
				}
				settle += time.Since(opened)
			}
			appended := journalBytes(test, store) - start
			registry := time.Now()
			for index := 0; index < 12; index++ {
				if err := store.Update(func(*State) error { return nil }); err != nil {
					test.Fatal(err)
				}
			}
			test.Logf("retained=%d register_mean_ms=%.3f settle_mean_ms=%.3f registry_mean_ms=%.3f journal_bytes_per_request=%.0f journal_bytes=%d", retained, float64(register)/12e6, float64(settle)/12e6, float64(time.Since(registry))/12e6, float64(appended)/12, journalBytes(test, store))
		})
	}
}

// A registry commit already persists the complete operation view, so it must not
// append anything to the operation log, and reopening must not rewrite it either.
func TestOperationLogNotRewrittenByRegistryCommit(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	directory := filepath.Dir(store.disk.path)
	database := securityDatabase()
	if err := beginOperation(store, database, "first", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "first", nil, true); err != nil {
		test.Fatal(err)
	}
	before, err := os.ReadFile(store.operations.path)
	if err != nil {
		test.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := store.Update(func(*State) error { return nil }); err != nil {
			test.Fatal(err)
		}
	}
	after, err := os.ReadFile(store.operations.path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("registry commit appended to the operation log")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	opened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	reopened := opened.(*stateStore)
	defer reopened.Close()
	after, err = os.ReadFile(reopened.operations.path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("reopen rewrote the operation log")
	}
	if reopened.state.Operations[operationKey(database, "first")].State != "terminal" {
		test.Fatal("registry view did not survive the log that trails it")
	}
}

// A delta recorded against an older registry revision is already contained in the
// newer payload. Recovery must not replay it, and the next mutation has to bind the
// current revision instead of continuing the stale chain.
func TestOperationLogStaleDeltaDoesNotOverrideRegistry(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	directory := filepath.Dir(store.disk.path)
	database := securityDatabase()
	if err := beginOperation(store, database, "settled", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "settled", nil, true); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		state.Organizations["second"] = Organization{ID: "second", Name: "second"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	stale := store.state.Revision
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	opened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	reopened := opened.(*stateStore)
	defer reopened.Close()
	if reopened.state.Organizations["second"].ID != "second" || reopened.state.Operations[operationKey(database, "settled")].State != "terminal" {
		test.Fatal("newer registry did not restore the operation view")
	}
	if reopened.operations.revision == 0 {
		test.Fatal("operation log lost its chain head")
	}
	if err := beginOperation(reopened, database, "lost", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	head := reopened.operations.records[len(reopened.operations.records)-1].Payload
	if head.BaseRevision != stale || head.BaseRevision == 0 {
		test.Fatal("new mutation did not bind the current registry revision")
	}
	if err := reopened.Close(); err != nil {
		test.Fatal(err)
	}
	recovered, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer recovered.Close()
	state, err := recovered.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	record := state.Operations[operationKey(database, "lost")]
	if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
		test.Fatal("stale chain replay lost Unknown")
	}
}
