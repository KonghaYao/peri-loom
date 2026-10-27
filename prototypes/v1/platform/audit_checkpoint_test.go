package platform

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func blockAuditCheckpoint(test *testing.T, audit *fileAudit) (<-chan struct{}, func()) {
	test.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	audit.testCheckpointGate = func() {
		enterOnce.Do(func() { close(entered) })
		<-gate
	}
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	test.Cleanup(release)
	return entered, release
}

func fillAuditCheckpoint(test *testing.T, audit *fileAudit, entered <-chan struct{}) {
	test.Helper()
	for range auditBatchMaximum {
		if err := audit.Record(auditFixtureEvent()); err != nil {
			test.Fatal(err)
		}
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		test.Fatal("acknowledged bound did not start head maintenance")
	}
}

func readAuditLineCount(test *testing.T, path string) int {
	test.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	return bytes.Count(data, []byte{'\n'})
}

func TestAuditCheckpointAcknowledgesOnlySyncedBatchAndBlocksNext(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	entered, release := blockAuditCheckpoint(test, audit)
	fillAuditCheckpoint(test, audit, entered)
	// All 32 calls already returned while the checkpoint is frozen, and their
	// bytes are in the synced append file. The next record must not pass it.
	if count := readAuditLineCount(test, audit.file.Name()); count != auditBatchMaximum || audit.syncCount.Load() != auditBatchMaximum {
		test.Fatal("acknowledged records did not each reach their append barrier")
	}
	next := make(chan error, 1)
	go func() { next <- audit.Record(auditFixtureEvent()) }()
	waitAuditStatus(test, audit, "next record queued behind maintenance", func(status map[string]any) bool { return status["queue_count"] == 1 })
	select {
	case err := <-next:
		test.Fatalf("next record escaped maintenance: %v", err)
	default:
	}
	if count := readAuditLineCount(test, audit.file.Name()); count != auditBatchMaximum {
		test.Fatal("next audit append exceeded the existing witness bound")
	}
	release()
	if err := <-next; err != nil {
		test.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	if audit.headCheckpointCount.Load() != 2 || audit.head.revision != auditBatchMaximum+1 {
		test.Fatal("unexpected checkpoint count or shutdown head")
	}
	reopened, err := OpenAudit(audit.file.Name())
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != auditBatchMaximum+1 {
		test.Fatal("an acknowledged append disappeared on reopen")
	}
}

func TestAuditCheckpointCloseJoinsMaintenanceAndRefusesNewRecords(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	entered, release := blockAuditCheckpoint(test, audit)
	fillAuditCheckpoint(test, audit, entered)
	closed := make(chan error, 1)
	go func() { closed <- audit.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		audit.queueMutex.Lock()
		closing := audit.closing
		audit.queueMutex.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("Close did not stop admission")
		}
		time.Sleep(time.Millisecond)
	}
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateClosed) {
		test.Fatalf("new record accepted during Close: %v", err)
	}
	select {
	case err := <-closed:
		test.Fatalf("Close did not join maintenance: %v", err)
	default:
	}
	release()
	if err := <-closed; err != nil {
		test.Fatal(err)
	}
	if audit.headCheckpointCount.Load() != 1 {
		test.Fatal("Close duplicated an already completed head checkpoint")
	}
	reopened, err := OpenAudit(audit.file.Name())
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != auditBatchMaximum {
		test.Fatal("maintenance shutdown lost acknowledged records")
	}
}

func TestAuditCheckpointHeadAndFloorFailuresRemainFailClosed(test *testing.T) {
	for _, suffix := range []string{".head", ".head.floor"} {
		test.Run(suffix, func(test *testing.T) {
			audit := openAuditAdmissionFixture(test)
			entered, release := blockAuditCheckpoint(test, audit)
			fillAuditCheckpoint(test, audit, entered)
			path := audit.file.Name() + suffix
			backup := filepath.Join(filepath.Dir(path), "checkpoint-before-fault")
			if err := os.Rename(path, backup); err != nil {
				test.Fatal(err)
			}
			// A directory at the destination forces a real atomic rename failure.
			// For the floor case, the new head has already reached its fsync.
			if err := os.Mkdir(path, 0700); err != nil {
				test.Fatal(err)
			}
			release()
			if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateUncertain) {
				test.Fatalf("head maintenance failure did not refuse next append: %v", err)
			}
			if err := audit.Close(); !errors.Is(err, ErrStateUncertain) {
				test.Fatalf("Close hid maintenance failure: %v", err)
			}
			if count := readAuditLineCount(test, audit.file.Name()); count != auditBatchMaximum || audit.headCheckpointFailures.Load() == 0 {
				test.Fatal("failed head maintenance changed the acknowledged append prefix")
			}
			if suffix == ".head.floor" {
				var head diskEnvelope
				if err := readPrivate(audit.file.Name()+".head", &head); err != nil || head.Revision != auditBatchMaximum {
					test.Fatal("floor fault did not occur after durable head publication")
				}
			}
			if err := os.Remove(path); err != nil {
				test.Fatal(err)
			}
			if err := os.Rename(backup, path); err != nil {
				test.Fatal(err)
			}
			reopened, err := OpenAudit(audit.file.Name())
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			if reopened.(*fileAudit).state.Sequence != auditBatchMaximum {
				test.Fatal("existing head/floor recovery rejected the synced prefix")
			}
		})
	}
}

func TestAuditCheckpointDoesNotTouchCompletionJournal(test *testing.T) {
	// The unified path owns its own append and witness. No legacy head exists
	// here, so touching it would fail immediately instead of silently passing.
	audit := &fileAudit{completionStore: &stateStore{}, testCheckpointGate: func() { test.Fatal("legacy maintenance entered the unified path") }}
	audit.checkpointAfterBatch()
	if audit.headCheckpointCount.Load() != 0 {
		test.Fatal("unified journal triggered legacy head maintenance")
	}
}
