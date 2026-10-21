package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func init() {
	if value := os.Getenv("PERI_STORE_TEST_COMPACT_ENTRIES"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			journalCompactEntries = parsed
		}
	}
	if point := os.Getenv("PERI_STORE_CRASH_POINT"); point != "" {
		storeCommitHook = func(current string) {
			if current == point {
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			}
		}
	}
}

func TestStoreJournalCompactionMetricsAndBoundedReplay(test *testing.T) {
	previousEntries, previousBytes := journalCompactEntries, journalCompactBytes
	journalCompactEntries, journalCompactBytes = 4, 1<<30
	defer func() { journalCompactEntries, journalCompactBytes = previousEntries, previousBytes }()

	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	for revision := uint64(1); revision <= 10; revision++ {
		if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
			test.Fatal(err)
		}
	}
	metrics := store.(*diskStore).Metrics()
	if metrics.Revision != 10 || metrics.SnapshotRevision != 8 || metrics.Compactions != 2 || metrics.JournalEntries != 2 {
		test.Fatalf("unexpected commit pipeline metrics: %+v", metrics)
	}
	if metrics.JournalBytes <= 0 || metrics.ReplayEntries != 0 || metrics.LastCompactionMS < 0 {
		test.Fatalf("missing journal accounting: %+v", metrics)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	metrics = reopened.(*diskStore).Metrics()
	if metrics.Revision != 10 || metrics.SnapshotRevision != 8 || metrics.ReplayEntries != 2 || metrics.JournalEntries != 2 {
		test.Fatalf("bounded replay did not stop at the snapshot: %+v", metrics)
	}
	actual, err := reopened.Snapshot()
	if err != nil || actual.Revision != 10 || actual.Epoch != 10 {
		test.Fatalf("replayed state mismatch: %+v %v", actual, err)
	}
	if exported, ok := ReadStoreMetrics(reopened); !ok || exported.Revision != 10 {
		test.Fatalf("store metrics are not externally readable: %+v %v", exported, ok)
	}
}

func TestStoreJournalCommitSizeScalesWithDeltaNotCatalog(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	if err := store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 1
		for index := 0; index < 2000; index++ {
			database := DatabaseID{Namespace: Namespace{Tenant: "tenant", Database: fmt.Sprintf("db-%d", index)}, Branch: "main"}
			metadata.Placements[database.Key()] = Placement{
				Database: database, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000,
				DesiredState: "suspended", Phase: "suspended", Role: "writer",
			}
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	journalPath := filepath.Join(directory, metadataJournalName)
	before, err := os.Stat(journalPath)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 2; return nil }); err != nil {
		test.Fatal(err)
	}
	after, err := os.Stat(journalPath)
	if err != nil {
		test.Fatal(err)
	}
	delta := after.Size() - before.Size()
	test.Logf("journal after 2000 placements=%d bytes, subsequent one-placement commit=%d bytes", before.Size(), delta)
	if delta <= 0 || delta > before.Size()/10 || delta > 4096 {
		test.Fatalf("small commit wrote %d bytes after a %d byte journal", delta, before.Size())
	}
}

func TestStoreJournalTornTailAndCorruption(test *testing.T) {
	setup := func(test *testing.T) string {
		test.Helper()
		directory := test.TempDir()
		store, err := OpenStore(directory)
		if err != nil {
			test.Fatal(err)
		}
		for epoch := uint64(1); epoch <= 2; epoch++ {
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
				test.Fatal(err)
			}
		}
		if err := store.Close(); err != nil {
			test.Fatal(err)
		}
		return directory
	}
	journalPath := func(directory string) string { return filepath.Join(directory, metadataJournalName) }

	test.Run("torn-tail", func(test *testing.T) {
		directory := setup(test)
		path := journalPath(directory)
		before, err := os.Stat(path)
		if err != nil {
			test.Fatal(err)
		}
		if err := appendJournalBytes(path, []byte(`{"version":1,"revision":3,`)); err != nil {
			test.Fatal(err)
		}
		reopened, err := OpenStore(directory)
		if err != nil {
			test.Fatal(err)
		}
		actual, err := reopened.Snapshot()
		if err != nil || actual.Revision != 2 || actual.Epoch != 2 {
			test.Fatalf("torn tail changed state: %+v %v", actual, err)
		}
		if metrics := reopened.(*diskStore).Metrics(); metrics.JournalBytes != before.Size() {
			test.Fatalf("torn tail was not truncated: %+v", metrics)
		}
		if err := reopened.Close(); err != nil {
			test.Fatal(err)
		}
		after, err := os.Stat(path)
		if err != nil || after.Size() != before.Size() {
			test.Fatalf("journal tail not truncated on disk: %v %v", after, err)
		}
	})

	test.Run("corrupt-tail", func(test *testing.T) {
		directory := setup(test)
		if err := appendJournalBytes(journalPath(directory), []byte(`{"version":1,"revision":3,"prev":"broken","checksum":"deadbeef","delta":{}}`+"\n")); err != nil {
			test.Fatal(err)
		}
		if _, err := OpenStore(directory); err == nil {
			test.Fatal("corrupt complete tail record was accepted")
		}
	})

	test.Run("corrupt-record", func(test *testing.T) {
		directory := setup(test)
		path := journalPath(directory)
		encoded, err := os.ReadFile(path)
		if err != nil {
			test.Fatal(err)
		}
		lines := bytes.SplitAfter(encoded, []byte{'\n'})
		var record journalRecord
		if err := json.Unmarshal(lines[0], &record); err != nil {
			test.Fatal(err)
		}
		record.Checksum = strings.Repeat("0", len(record.Checksum))
		corrupted, err := json.Marshal(record)
		if err != nil {
			test.Fatal(err)
		}
		rewritten := append(append(corrupted, '\n'), bytes.Join(lines[1:], nil)...)
		if err := os.WriteFile(path, rewritten, 0600); err != nil {
			test.Fatal(err)
		}
		if _, err := OpenStore(directory); err == nil {
			test.Fatal("corrupt committed record was accepted")
		}
	})

	test.Run("unsupported-record", func(test *testing.T) {
		directory := setup(test)
		path := journalPath(directory)
		encoded, err := os.ReadFile(path)
		if err != nil {
			test.Fatal(err)
		}
		lines := bytes.SplitAfter(encoded, []byte{'\n'})
		var record journalRecord
		if err := json.Unmarshal(lines[0], &record); err != nil {
			test.Fatal(err)
		}
		record.Version = journalRecordVersion + 1
		corrupted, err := json.Marshal(record)
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(path, append(append(corrupted, '\n'), bytes.Join(lines[1:], nil)...), 0600); err != nil {
			test.Fatal(err)
		}
		if _, err := OpenStore(directory); err == nil {
			test.Fatal("unsupported journal record version was accepted")
		}
	})
}

func TestStoreJournalCrashPoints(test *testing.T) {
	cases := []struct {
		point string
		want  uint64
	}{
		{"journal.append.before", 0},
		{"journal.append.after", 1},
		{"journal.fsync.after", 1},
		{"index.before", 1},
		{"index.after", 1},
	}
	for _, crash := range cases {
		test.Run(crash.point, func(test *testing.T) {
			directory := test.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			runCrashChild(test, directory, crash.point)
			reopened, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			actual, err := reopened.Snapshot()
			if err != nil || actual.Revision != crash.want {
				test.Fatalf("crash at %s produced revision %d: %+v %v", crash.point, crash.want, actual, err)
			}
			if crash.want == 1 && actual.Epoch != 1 {
				test.Fatalf("crash at %s lost the durable epoch: %+v", crash.point, actual)
			}
			if err := reopened.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
				test.Fatal(err)
			}
			actual, err = reopened.Snapshot()
			if err != nil || actual.Revision != crash.want+1 || actual.Epoch != crash.want+1 {
				test.Fatalf("commit after crash at %s duplicated or skipped a revision: %+v %v", crash.point, actual, err)
			}
		})
	}
}

func TestStoreJournalCompactionCrashPoints(test *testing.T) {
	cases := []struct {
		point          string
		snapshotBefore uint64
	}{
		{"compact.snapshot.before", 0},
		{"compact.snapshot.after", 1},
		{"compact.truncate.before", 1},
		{"compact.truncate.after", 1},
	}
	for _, crash := range cases {
		test.Run(crash.point, func(test *testing.T) {
			directory := test.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			runCrashChild(test, directory, crash.point, "PERI_STORE_TEST_COMPACT_ENTRIES=1")
			reopened, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			actual, err := reopened.Snapshot()
			if err != nil || actual.Revision != 1 || actual.Epoch != 1 {
				test.Fatalf("compaction crash at %s produced %+v %v", crash.point, actual, err)
			}
			metrics := reopened.(*diskStore).Metrics()
			if metrics.SnapshotRevision != crash.snapshotBefore {
				test.Fatalf("unexpected durable snapshot at %s: %+v", crash.point, metrics)
			}
			if err := reopened.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
				test.Fatal(err)
			}
			actual, err = reopened.Snapshot()
			if err != nil || actual.Revision != 2 || actual.Epoch != 2 {
				test.Fatalf("commit after compaction crash at %s was not exact: %+v %v", crash.point, actual, err)
			}
		})
	}
}

func TestStoreJournalRecoversStaleSnapshotPrefix(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	for epoch := uint64(1); epoch <= 2; epoch++ {
		if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
			test.Fatal(err)
		}
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	anchor := store.(*diskStore).journalTail
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	encoded, _, err := encodeSnapshot(snapshot, anchor)
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicFile(directory, metadataSnapshotName, encoded); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	actual, err := reopened.Snapshot()
	if err != nil || actual.Revision != 2 || actual.Epoch != 2 {
		test.Fatalf("stale compaction prefix was replayed twice: %+v %v", actual, err)
	}
	if err := reopened.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
		test.Fatal(err)
	}
	actual, err = reopened.Snapshot()
	if err != nil || actual.Revision != 3 || actual.Epoch != 3 {
		test.Fatalf("append after stale prefix was not exact: %+v %v", actual, err)
	}
}

func TestStoreJournalRejectsSnapshotAheadOfJournal(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 1; return nil }); err != nil {
		test.Fatal(err)
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	snapshot.Revision = 2
	snapshot.Epoch = 2
	encoded, _, err := encodeSnapshot(snapshot, strings.Repeat("a", 64))
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicFile(directory, metadataSnapshotName, encoded); err != nil {
		test.Fatal(err)
	}
	if _, err := OpenStore(directory); err == nil {
		test.Fatal("snapshot ahead of the journal was accepted")
	}
}

func TestStoreJournalConcurrentUpdatesSerialize(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	var group sync.WaitGroup
	failures := make(chan error, 24)
	for worker := 0; worker < 24; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for commit := 0; commit < 5; commit++ {
				if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		test.Fatal(err)
	}
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 120 || actual.Epoch != 120 {
		test.Fatalf("concurrent updates lost revisions: %+v %v", actual, err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Revision--; return nil }); err == nil {
		test.Fatal("stale caller revision was accepted")
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Revision++; return nil }); err == nil {
		test.Fatal("caller-owned revision was accepted")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	actual, err = reopened.Snapshot()
	if err != nil || actual.Revision != 120 || actual.Epoch != 120 {
		test.Fatalf("concurrent journal replay mismatch: %+v %v", actual, err)
	}
}

func TestStoreJournalReplayLimitIsEnforced(test *testing.T) {
	path := filepath.Join(test.TempDir(), metadataJournalName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		test.Fatal(err)
	}
	previous := ""
	for revision := 1; revision <= journalMaxEntries+1; revision++ {
		record, checksum, err := encodeJournalRecord(uint64(revision), previous, metadataDelta{})
		if err != nil {
			file.Close()
			test.Fatal(err)
		}
		if _, err := file.Write(record); err != nil {
			file.Close()
			test.Fatal(err)
		}
		previous = checksum
	}
	if err := file.Close(); err != nil {
		test.Fatal(err)
	}
	if _, _, _, err := readJournalFile(path); err == nil {
		test.Fatal("unbounded journal replay was accepted")
	}
}

func TestStoreUpdateAtRevisionRejectsStaleCommit(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	revisioned, ok := store.(RevisionedStore)
	if !ok {
		test.Fatal("store does not expose conditional commits")
	}
	if err := revisioned.UpdateAtRevision(0, func(metadata *Metadata) error { metadata.Epoch = 1; return nil }); err != nil {
		test.Fatal(err)
	}
	if err := revisioned.UpdateAtRevision(0, func(metadata *Metadata) error { metadata.Epoch = 2; return nil }); !errors.Is(err, ErrStoreRevisionMismatch) {
		test.Fatalf("stale revision was accepted: %v", err)
	}
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 1 || actual.Epoch != 1 {
		test.Fatalf("stale conditional commit mutated state: %+v %v", actual, err)
	}
	if err := revisioned.UpdateAtRevision(1, func(metadata *Metadata) error { metadata.Epoch = 2; return nil }); err != nil {
		test.Fatal(err)
	}
	actual, err = store.Snapshot()
	if err != nil || actual.Revision != 2 || actual.Epoch != 2 {
		test.Fatalf("conditional revisions were not enforced: %+v %v", actual, err)
	}
}

func TestStoreCrashWriter(test *testing.T) {
	directory := os.Getenv("PERI_STORE_CRASH_DIRECTORY")
	if directory == "" {
		test.Skip("subprocess helper")
	}
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
		test.Fatal(err)
	}
	test.Fatal("configured crash point was not reached")
}

func runCrashChild(test *testing.T, directory, point string, extra ...string) {
	test.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreCrashWriter$")
	command.Env = append(os.Environ(), "PERI_STORE_CRASH_DIRECTORY="+directory, "PERI_STORE_CRASH_POINT="+point)
	command.Env = append(command.Env, extra...)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		test.Fatalf("crash child did not terminate at %s: %s %v", point, output, err)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		test.Fatalf("crash child did not die at %s: %s %v", point, output, err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		test.Fatalf("crash child exited without SIGKILL at %s: %s %v", point, output, err)
	}
}
