package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func journalFixture(test *testing.T) (*stateStore, string) {
	test.Helper()
	store := operationDurableStore(test).(*stateStore)
	return store, store.operations.path
}

func journalCommit(test *testing.T, store *stateStore, identifier string) {
	test.Helper()
	database := securityDatabase()
	if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, identifier, nil, true); err != nil {
		test.Fatal(err)
	}
}

func journalLines(test *testing.T, path string) [][]byte {
	test.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if int64(len(encoded)) < operationJournalWitnessBytes {
		test.Fatal("journal lost its witness line")
	}
	lines := [][]byte{}
	for _, line := range bytes.Split(encoded[operationJournalWitnessBytes:], []byte{'\n'}) {
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}

// journalWriteState reproduces a crash artifact: the witness moves in place and the
// records after it are whatever survived.
func journalWriteState(test *testing.T, path string, revision uint64, digest string, lines [][]byte) {
	test.Helper()
	encoded := append([]byte{}, journalWitnessLine(revision, digest)...)
	for _, line := range lines {
		encoded = append(encoded, line...)
		encoded = append(encoded, '\n')
	}
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		test.Fatal(err)
	}
}

// journalWriteTornState writes the durable prefix plus a final record that never
// received its terminating newline, which is what an interrupted append leaves.
func journalWriteTornState(test *testing.T, path string, revision uint64, digest string, lines [][]byte, torn []byte) {
	test.Helper()
	encoded := append([]byte{}, journalWitnessLine(revision, digest)...)
	for _, line := range lines {
		encoded = append(encoded, line...)
		encoded = append(encoded, '\n')
	}
	encoded = append(encoded, torn...)
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		test.Fatal(err)
	}
}

// journalWriteLines keeps the witness current for the last surviving record, which
// is the state a crash leaves when an append returned and its witness landed.
func journalWriteLines(test *testing.T, path string, lines [][]byte) {
	test.Helper()
	if len(lines) == 0 {
		journalWriteState(test, path, 0, "", nil)
		return
	}
	var entry journalEntry
	if err := strictDecode(lines[len(lines)-1], &entry); err != nil {
		test.Fatal(err)
	}
	journalWriteState(test, path, entry.Payload.Revision, entry.Hash, lines)
}

func journalReopen(test *testing.T, directory string) (*stateStore, error) {
	test.Helper()
	opened, err := OpenStateStore(directory)
	if err != nil {
		return nil, err
	}
	return opened.(*stateStore), nil
}

// One mutation is one appended delta, so the durable cost of a commit does not grow
// with the bounded set it did not change.
func TestOperationJournalAppendsOnlyTheDelta(test *testing.T) {
	store, path := journalFixture(test)
	database := securityDatabase()
	for index := 0; index < operationKnownRetention; index++ {
		identifier := fmt.Sprintf("seed_%04d", index)
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
	}
	before, err := os.Stat(path)
	if err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "single", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		test.Fatal(err)
	}
	appended := after.Size() - before.Size()
	if appended <= 0 || appended > 4096 {
		test.Fatalf("one mutation appended %d bytes for a %d record set", appended, len(store.state.Operations))
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := journalReopen(test, filepath.Dir(store.disk.path))
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil || len(state.Operations) != operationKnownRetention+1 {
		test.Fatalf("replay lost the retained set: %d %v", len(state.Operations), err)
	}
}

// The unverifiable tail is an append whose fsync never returned, so recovery drops
// it instead of refusing to start, and it never invents the missing record.
func TestOperationJournalTornTailIsDroppedNotTrusted(test *testing.T) {
	store, path := journalFixture(test)
	journalCommit(test, store, "one")
	journalCommit(test, store, "two")
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	lines := journalLines(test, path)
	if len(lines) != 4 {
		test.Fatalf("unexpected mutation count %d", len(lines))
	}
	var settled journalEntry
	if err := strictDecode(lines[2], &settled); err != nil {
		test.Fatal(err)
	}
	// The witness describes the last append whose fsync returned; the torn record is
	// the one that never did, so it has no terminating newline either.
	journalWriteTornState(test, path, settled.Payload.Revision, settled.Hash, lines[:3], lines[3][:len(lines[3])/2])
	directory := filepath.Dir(store.disk.path)
	reopened, err := journalReopen(test, directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	// The truncated record was the settlement of an intent that is still durable, so
	// recovery has to keep the operation quarantined instead of forgetting it.
	record := state.Operations[operationKey(securityDatabase(), "two")]
	if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
		test.Fatalf("torn settlement was not isolated: %+v", record.OperationStatus)
	}
	if reopened.operations.revision != 3 {
		test.Fatalf("torn tail was not truncated: head %d", reopened.operations.revision)
	}
	// The truncated record was never acknowledged, so the chain continues from the
	// verified prefix instead of leaving a gap.
	if err := reopened.operations.appendOperations(reopened.state, operationDelta{}); err != nil {
		test.Fatal(err)
	}
	if reopened.operations.revision != 4 {
		test.Fatalf("chain did not continue after the truncation: %d", reopened.operations.revision)
	}
}

// A complete record that fails its own chain check is not a torn append: the
// witness may point at it, and dropping it would erase the evidence an operator
// needs. Opening must refuse and leave every byte in place.
func TestOperationJournalLegacyCompleteCorruptTailIsRejectedWithoutRewriting(test *testing.T) {
	store, path := journalFixture(test)
	journalCommit(test, store, "one")
	journalCommit(test, store, "two")
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	lines := journalLines(test, path)
	if len(lines) != 4 {
		test.Fatalf("unexpected mutation count %d", len(lines))
	}
	var last journalEntry
	if err := strictDecode(lines[3], &last); err != nil {
		test.Fatal(err)
	}
	last.Payload.Sequence++
	corrupted, err := json.Marshal(last)
	if err != nil {
		test.Fatal(err)
	}
	rewritten := append([][]byte{}, lines[:3]...)
	rewritten = append(rewritten, corrupted)
	journalWriteLines(test, path, rewritten)
	before, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	reopened, err := journalReopen(test, filepath.Dir(store.disk.path))
	if err == nil {
		reopened.Close()
		test.Fatal("complete corrupt operation record was accepted")
	}
	if !strings.Contains(err.Error(), "corrupt complete operation record") {
		test.Fatalf("unexpected refusal: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		test.Fatal("refused journal was rewritten")
	}
}

// A torn append is dropped only when the surviving chain still proves the
// witness. When the witness names the torn record itself, the missing bytes are
// a rollback, not a recoverable append: refuse and keep the accident bytes.
func TestOperationJournalWitnessedTornTailIsRejectedWithoutRewriting(test *testing.T) {
	store, path := journalFixture(test)
	journalCommit(test, store, "one")
	journalCommit(test, store, "two")
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	lines := journalLines(test, path)
	if len(lines) != 4 {
		test.Fatalf("unexpected mutation count %d", len(lines))
	}
	var last journalEntry
	if err := strictDecode(lines[3], &last); err != nil {
		test.Fatal(err)
	}
	journalWriteTornState(test, path, last.Payload.Revision, last.Hash, lines[:3], lines[3][:len(lines[3])/2])
	before, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	reopened, err := journalReopen(test, filepath.Dir(store.disk.path))
	if err == nil {
		reopened.Close()
		test.Fatal("witnessed torn tail was accepted")
	}
	if !strings.Contains(err.Error(), "operation journal rollback detected") {
		test.Fatalf("unexpected refusal: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		test.Fatal("witnessed torn tail was truncated before refusal")
	}
}

// A chain that lost, duplicated or reordered a mutation is refused: the witness
// proves the prefix is too short, and a rewritten prefix is not the recorded chain.
func TestOperationJournalRefusesGapDuplicateAndReorder(test *testing.T) {
	for _, scenario := range []string{"gap", "duplicate", "reorder"} {
		test.Run(scenario, func(test *testing.T) {
			store, path := journalFixture(test)
			for _, identifier := range []string{"one", "two", "three"} {
				journalCommit(test, store, identifier)
			}
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			lines := journalLines(test, path)
			switch scenario {
			case "gap":
				lines = append(lines[:2], lines[3:]...)
			case "duplicate":
				lines = append(lines[:2], append([][]byte{lines[1]}, lines[2:]...)...)
			case "reorder":
				lines[1], lines[2] = lines[2], lines[1]
			}
			journalWriteLines(test, path, lines)
			if reopened, err := journalReopen(test, filepath.Dir(path)); err == nil {
				reopened.Close()
				test.Fatal("corrupted operation chain was accepted")
			}
		})
	}
}

// The witness is the rollback bound. It may lag the chain it describes, but a chain
// that cannot cover it means an acknowledged mutation disappeared.
func TestOperationJournalWitnessRefusesShorterOrRewrittenChain(test *testing.T) {
	for _, scenario := range []string{"missing_chain", "rewritten_hash", "torn_witness"} {
		test.Run(scenario, func(test *testing.T) {
			store, path := journalFixture(test)
			journalCommit(test, store, "one")
			journalCommit(test, store, "two")
			head := store.operations.revision
			digest := store.operations.digest
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			lines := journalLines(test, path)
			switch scenario {
			case "missing_chain":
				journalWriteState(test, path, head, digest, lines[:len(lines)-1])
			case "rewritten_hash":
				journalWriteState(test, path, head, strings.Repeat("a", 64), lines[:len(lines)-1])
			case "torn_witness":
				// A torn witness keeps its width but loses its checksum, which has to
				// count as "no witness" instead of bricking the store.
				encoded := append([]byte{}, journalWitnessLine(head, digest)...)
				copy(encoded[len(encoded)-10:len(encoded)-2], "00000000")
				for _, line := range lines[:len(lines)-1] {
					encoded = append(encoded, line...)
					encoded = append(encoded, '\n')
				}
				if err := os.WriteFile(path, encoded, 0600); err != nil {
					test.Fatal(err)
				}
			}
			reopened, err := journalReopen(test, filepath.Dir(store.disk.path))
			if scenario == "torn_witness" {
				// A torn witness proves nothing, so the verified prefix is the truth
				// and the platform stays available.
				if err != nil {
					test.Fatal(err)
				}
				defer reopened.Close()
				if reopened.operations.revision != head-1 {
					test.Fatalf("torn witness did not fall back to the verified prefix: %d", reopened.operations.revision)
				}
				return
			}
			if err == nil {
				reopened.Close()
				test.Fatal("witness was not enforced")
			}
		})
	}
}

type compactionFixture struct {
	directory    string
	path         string
	snapshotPath string
	preTruncate  [][]byte
	compacted    uint64
	compactedSum string
	head         uint64
	digest       string
}

// Compaction is the periodic materialization. Every crash point around it must keep
// reconstructing the same view: the snapshot is durable before the log shrinks, and
// a snapshot that is not provably part of the chain is refused instead of trusted.
func compactionSetup(test *testing.T) compactionFixture {
	test.Helper()
	store, path := journalFixture(test)
	journalCommit(test, store, "one")
	journalCommit(test, store, "two")
	fixture := compactionFixture{directory: filepath.Dir(store.disk.path), path: path, snapshotPath: store.operations.snapshotPath(), preTruncate: journalLines(test, path)}
	if err := store.operations.compact(store.state); err != nil {
		test.Fatal(err)
	}
	fixture.compacted, fixture.compactedSum = store.operations.revision, store.operations.digest
	journalCommit(test, store, "three")
	fixture.head, fixture.digest = store.operations.revision, store.operations.digest
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	return fixture
}

func (fixture compactionFixture) rewriteSnapshot(test *testing.T, change func(*operationSnapshot)) {
	test.Helper()
	encoded, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		test.Fatal(err)
	}
	var snapshot operationSnapshot
	if err := strictDecode(encoded, &snapshot); err != nil {
		test.Fatal(err)
	}
	change(&snapshot)
	rewritten, _ := json.Marshal(snapshot)
	if err := atomicPrivate(fixture.snapshotPath, rewritten); err != nil {
		test.Fatal(err)
	}
}

func TestOperationJournalCompactionCrashPrefixes(test *testing.T) {
	for _, scenario := range []string{"snapshot_before_truncate", "truncated_after_compaction", "records_lost_after_witness", "unbound_snapshot", "snapshot_digest_mismatch", "snapshot_removed"} {
		test.Run(scenario, func(test *testing.T) {
			fixture := compactionSetup(test)
			refused := false
			switch scenario {
			case "snapshot_before_truncate":
				// Snapshot durable, log not shrunk yet, nothing appended after it.
				journalWriteState(test, fixture.path, fixture.compacted, fixture.compactedSum, fixture.preTruncate)
			case "truncated_after_compaction":
				journalWriteState(test, fixture.path, fixture.compacted, fixture.compactedSum, nil)
			case "records_lost_after_witness":
				journalWriteState(test, fixture.path, fixture.head, fixture.digest, nil)
				refused = true
			case "unbound_snapshot":
				journalWriteState(test, fixture.path, fixture.head, fixture.digest, nil)
				fixture.rewriteSnapshot(test, func(snapshot *operationSnapshot) {
					snapshot.Revision, snapshot.Digest = fixture.head+4, strings.Repeat("b", 64)
				})
				refused = true
			case "snapshot_digest_mismatch":
				journalWriteState(test, fixture.path, fixture.head, fixture.digest, nil)
				fixture.rewriteSnapshot(test, func(snapshot *operationSnapshot) { snapshot.Digest = strings.Repeat("c", 64) })
				refused = true
			case "snapshot_removed":
				// Once the log was shrunk at the snapshot, the snapshot is what binds
				// the surviving records; losing it must fail closed.
				if err := os.Remove(fixture.snapshotPath); err != nil {
					test.Fatal(err)
				}
				refused = true
			}
			reopened, err := journalReopen(test, fixture.directory)
			if refused {
				if err == nil {
					reopened.Close()
					test.Fatal("tampered compaction state was accepted")
				}
				return
			}
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			state, err := reopened.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			for _, identifier := range []string{"one", "two"} {
				if state.Operations[operationKey(securityDatabase(), identifier)].State != "terminal" {
					test.Fatalf("%s lost across compaction crash %s", identifier, scenario)
				}
			}
			if len(state.Operations) != 2 {
				test.Fatalf("compaction crash %s recovered %d records, want 2", scenario, len(state.Operations))
			}
		})
	}
}

// An intent that was durable before the crash must still quarantine after the log
// was compacted, so compaction never downgrades Unknown to a missing record.
func TestOperationJournalCompactionPreservesUnknown(test *testing.T) {
	store, _ := journalFixture(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "lost", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := store.operations.compact(store.state); err != nil {
		test.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		journalCommit(test, store, fmt.Sprintf("later_%d", index))
	}
	directory := filepath.Dir(store.disk.path)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := journalReopen(test, directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	record := state.Operations[operationKey(database, "lost")]
	if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
		test.Fatal("compaction lost the durable intent")
	}
	if err := beginOperation(reopened, database, "retry", "gateway", "execute", "tool_rpc", "worker-a", "", nil); !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatal("restart admitted a retry after compaction")
	}
}

// The log is appended through a held descriptor, so a replaced path has to poison
// instead of appending into an orphaned inode.
func TestOperationJournalReplacedPathPoisons(test *testing.T) {
	store, path := journalFixture(test)
	journalCommit(test, store, "one")
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(path, journalWitnessLine(0, ""), 0600); err != nil {
		test.Fatal(err)
	}
	if err := journalCommitErr(store, "two"); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("replaced journal path was accepted: %v", err)
	}
}

func TestOperationJournalFailurePoisonsAndCloseIsIdempotent(test *testing.T) {
	store, _ := journalFixture(test)
	journalCommit(test, store, "one")
	if err := store.operations.file.Close(); err != nil {
		test.Fatal(err)
	}
	if err := store.operations.appendOperations(store.state, operationDelta{}); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("failed append did not poison: %v", err)
	}
	if err := store.operations.available(); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("poisoned journal stayed available")
	}
	if err := store.operations.close(); err == nil {
		test.Fatal("close hid the failed append descriptor")
	}
	if err := store.operations.close(); err != nil {
		test.Fatal(err)
	}
}

func journalCommitErr(store *stateStore, identifier string) error {
	database := securityDatabase()
	if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		return err
	}
	return finishOperation(store, database, identifier, nil, true)
}
