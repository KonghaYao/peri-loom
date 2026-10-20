package platform

// Q04 fault matrix. The completion envelope is a design at this point: production
// still writes the operation journal and the audit file separately. These tests pin
// the semantics the envelope has to preserve, inject the crash points the design
// names, and model the envelope's two-view replay without changing any durable
// format.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// completionCopyDirectory takes a byte-for-byte copy of an idle private state
// directory. Opening the copy is the test's stand-in for a process restart: the
// original locks stay on the original inodes and no writer is running.
func completionCopyDirectory(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			// A private directory must be 0700 including the copy root; t.TempDir
			// hands out a 0755 directory.
			if err := os.MkdirAll(target, 0700); err != nil {
				return err
			}
			return os.Chmod(target, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected non-regular private entry %s", path)
		}
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, encoded, info.Mode().Perm())
	})
}

func completionReopenState(test *testing.T, directory string) *stateStore {
	test.Helper()
	opened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatalf("restart refused the crash artifact: %v", err)
	}
	test.Cleanup(func() { _ = opened.Close() })
	return opened.(*stateStore)
}

func completionOperation(test *testing.T, store *stateStore, identifier string) OperationRecord {
	test.Helper()
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	record, found := state.Operations[operationKey(securityDatabase(), identifier)]
	if !found {
		test.Fatalf("operation %s disappeared across recovery", identifier)
	}
	return record
}

func completionSeedIntent(test *testing.T, identifier string) (*stateStore, string) {
	test.Helper()
	store := operationDurableStore(test).(*stateStore)
	if err := beginOperation(store, securityDatabase(), identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	return store, filepath.Dir(store.disk.path)
}

// TestCompletionCrashMatrixOperationJournal injects the completion-side crash
// points into the real operation journal. The synthetic artifacts use the same
// helper the journal recovery tests use (a witness that lags the records), so the
// crash state is the byte state recovery is specified to accept.
func TestCompletionCrashMatrixOperationJournal(test *testing.T) {
	database := securityDatabase()

	test.Run("intent_durable_before_dispatch", func(test *testing.T) {
		_, directory := completionSeedIntent(test, "q04a")
		copyDirectory := test.TempDir()
		if err := completionCopyDirectory(directory, copyDirectory); err != nil {
			test.Fatal(err)
		}
		reopened := completionReopenState(test, copyDirectory)
		record := completionOperation(test, reopened, "q04a")
		if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
			test.Fatalf("unresolved intent was not isolated as Unknown: %+v", record.OperationStatus)
		}
	})

	test.Run("completion_write_never_started", func(test *testing.T) {
		store, directory := completionSeedIntent(test, "q04b")
		store.operations.mutex.Lock()
		closeErr := store.operations.file.Close()
		store.operations.mutex.Unlock()
		if closeErr != nil {
			test.Fatal(closeErr)
		}
		if err := finishOperation(store, database, "q04b", nil, true); err == nil {
			test.Fatal("completion without a writable journal reported success")
		}
		copyDirectory := test.TempDir()
		if err := completionCopyDirectory(directory, copyDirectory); err != nil {
			test.Fatal(err)
		}
		_ = store.Close()
		reopened := completionReopenState(test, copyDirectory)
		record := completionOperation(test, reopened, "q04b")
		if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
			test.Fatalf("failed completion claimed a definite outcome: %+v", record.OperationStatus)
		}
		if record.BusinessOutcome == "not_executed" {
			test.Fatal("a dispatched operation was reported as not executed")
		}
	})

	// completedJournal writes one intent and one terminal record, closes the
	// journal cleanly, and returns the two record lines plus the directory.
	completedJournal := func(test *testing.T, identifier string) (string, string, [][]byte, journalEntry) {
		test.Helper()
		store, directory := completionSeedIntent(test, identifier)
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
		path := store.operations.path
		lines := journalLines(test, path)
		if len(lines) != 2 {
			test.Fatalf("completion fixture wrote %d records, want 2", len(lines))
		}
		var intent journalEntry
		if err := strictDecode(lines[0], &intent); err != nil {
			test.Fatal(err)
		}
		if err := store.Close(); err != nil {
			test.Fatal(err)
		}
		return path, directory, lines, intent
	}

	test.Run("completion_written_before_fsync_survives", func(test *testing.T) {
		path, directory, lines, intent := completedJournal(test, "q04c")
		// The records reached the file; the witness still names the intent because
		// the barrier (and the witness that follows it) never returned.
		journalWriteState(test, path, intent.Payload.Revision, intent.Hash, lines)
		reopened := completionReopenState(test, directory)
		record := completionOperation(test, reopened, "q04c")
		if record.State != "terminal" || record.BusinessOutcome != "completed" || !record.ExecutionProven {
			test.Fatalf("surviving completion record was not readable as success: %+v", record.OperationStatus)
		}
	})

	test.Run("completion_written_before_fsync_absent", func(test *testing.T) {
		path, directory, lines, intent := completedJournal(test, "q04d")
		torn := [][]byte{lines[0], lines[1][:len(lines[1])/2]}
		journalWriteState(test, path, intent.Payload.Revision, intent.Hash, torn)
		reopened := completionReopenState(test, directory)
		record := completionOperation(test, reopened, "q04d")
		if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
			test.Fatalf("torn completion was not isolated: %+v", record.OperationStatus)
		}
		if record.BusinessOutcome == "not_executed" {
			test.Fatal("a torn completion was reported as not executed")
		}
		if reopened.operations.revision != intent.Payload.Revision {
			test.Fatalf("torn tail was not truncated to the verified prefix: %d", reopened.operations.revision)
		}
	})

	test.Run("completion_fsynced_before_ack", func(test *testing.T) {
		store, directory := completionSeedIntent(test, "q04e")
		if err := finishOperation(store, database, "q04e", nil, true); err != nil {
			test.Fatal(err)
		}
		copyDirectory := test.TempDir()
		if err := completionCopyDirectory(directory, copyDirectory); err != nil {
			test.Fatal(err)
		}
		reopened := completionReopenState(test, copyDirectory)
		record := completionOperation(test, reopened, "q04e")
		if record.State != "terminal" || record.BusinessOutcome != "completed" || !record.ExecutionProven || record.MayHaveCommitted {
			test.Fatalf("durable completion was not readable as success: %+v", record.OperationStatus)
		}
		// The client lost the reply, so the operation query resolves the request;
		// replaying it under the same identity must be refused.
		if err := beginOperation(reopened, database, "q04e", "gateway", "execute", "tool_rpc", "worker-a", "", nil); !errors.Is(err, ErrOperationExists) {
			test.Fatalf("acknowledged completion accepted a blind replay: %v", err)
		}
		if err := beginOperation(reopened, database, "q04f", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatalf("known-terminal completion blocked new work: %v", err)
		}
	})
}

func completionAuditEvent(identifier string) AuditEvent {
	return AuditEvent{RequestID: identifier, Subject: "alice", Project: "alpha", Database: securityDatabase().Key(), Action: "/v1/sql/execute", Outcome: "ok", TimestampMS: time.Now().UnixMilli()}
}

func completionOpenAudit(test *testing.T) (*fileAudit, string) {
	test.Helper()
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = service.Close() })
	return service.(*fileAudit), path
}

// completionAuditChain reopens the audit and recomputes the chain itself: the
// reopen proves the production verifier accepts it, the walk proves the sequence,
// previous link and record hash are the ones the envelope has to reproduce.
func completionAuditChain(test *testing.T, path string) (uint64, int) {
	test.Helper()
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatalf("audit chain refused the crash artifact: %v", err)
	}
	sequence := service.(*fileAudit).state.Sequence
	if err := service.Close(); err != nil {
		test.Fatalf("closing the recovered audit: %v", err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	previous := strings.Repeat("0", 64)
	expected := uint64(0)
	count := 0
	for _, line := range bytes.Split(bytes.TrimSuffix(encoded, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record auditRecord
		if err := json.Unmarshal(line, &record); err != nil {
			test.Fatalf("audit record %d is not decodable: %v", count, err)
		}
		payload, err := json.Marshal(record.Payload)
		if err != nil {
			test.Fatal(err)
		}
		if record.Payload.Sequence != expected+1 || record.Payload.Previous != previous || digestHex(payload) != record.Hash {
			test.Fatalf("audit chain broke at record %d", count)
		}
		previous, expected = record.Hash, record.Payload.Sequence
		count++
	}
	if expected != sequence {
		test.Fatalf("verified chain ends at %d but the reopened audit reports %d", expected, sequence)
	}
	return sequence, count
}

func completionCopyPath(test *testing.T, source, base string) string {
	test.Helper()
	destination := test.TempDir()
	if err := completionCopyDirectory(source, destination); err != nil {
		test.Fatal(err)
	}
	return filepath.Join(destination, base)
}

// TestCompletionCrashMatrixAudit injects the audit-side crash points. The audit
// file is still the authority in production, so a torn append is expected to fail
// closed until the envelope makes the audit a repairable view.
func TestCompletionCrashMatrixAudit(test *testing.T) {
	test.Run("audit_write_never_started", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		directory := filepath.Dir(path)
		audit.mutex.Lock()
		closeErr := audit.file.Close()
		audit.mutex.Unlock()
		if closeErr != nil {
			test.Fatal(closeErr)
		}
		if err := audit.Record(completionAuditEvent("q04audita")); err == nil {
			test.Fatal("audit with no writable file reported success")
		}
		copyPath := completionCopyPath(test, directory, filepath.Base(path))
		if sequence, records := completionAuditChain(test, copyPath); sequence != 0 || records != 0 {
			test.Fatalf("failed audit append invented a record: sequence=%d records=%d", sequence, records)
		}
	})

	test.Run("audit_written_before_fsync", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		directory := filepath.Dir(path)
		copyDirectory := test.TempDir()
		var copyErr error
		audit.testSyncGate = func() {
			// The appended bytes are in the file and the barrier has not returned,
			// which is the crash state a process kill leaves behind.
			copyErr = completionCopyDirectory(directory, copyDirectory)
			_ = audit.file.Close()
		}
		recordErr := audit.Record(completionAuditEvent("q04auditb"))
		audit.testSyncGate = nil
		if recordErr == nil {
			test.Fatal("audit append with a failed barrier reported success")
		}
		if copyErr != nil {
			test.Fatal(copyErr)
		}
		copyPath := filepath.Join(copyDirectory, filepath.Base(path))
		if sequence, records := completionAuditChain(test, copyPath); sequence != 1 || records != 1 {
			test.Fatalf("surviving audit append was lost or duplicated: sequence=%d records=%d", sequence, records)
		}
	})

	test.Run("audit_fsynced_before_head", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		for index := 0; index < 3; index++ {
			if err := audit.Record(completionAuditEvent(fmt.Sprintf("q04auditc%d", index))); err != nil {
				test.Fatal(err)
			}
		}
		if audit.head.revision != 0 {
			test.Fatalf("fixture advanced the head early: %d", audit.head.revision)
		}
		copyPath := completionCopyPath(test, filepath.Dir(path), filepath.Base(path))
		if sequence, records := completionAuditChain(test, copyPath); sequence != 3 || records != 3 {
			test.Fatalf("head lag lost or duplicated acknowledged records: sequence=%d records=%d", sequence, records)
		}
	})

	test.Run("audit_fsynced_after_head_materialization", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		if err := audit.Record(completionAuditEvent("q04auditd")); err != nil {
			test.Fatal(err)
		}
		audit.mutex.Lock()
		headErr := audit.head.commitAuditBatch(audit.state.Sequence, audit.state)
		audit.mutex.Unlock()
		if headErr != nil {
			test.Fatal(headErr)
		}
		copyPath := completionCopyPath(test, filepath.Dir(path), filepath.Base(path))
		if sequence, records := completionAuditChain(test, copyPath); sequence != 1 || records != 1 {
			test.Fatalf("materialized head did not cover its record: sequence=%d records=%d", sequence, records)
		}
	})

	test.Run("torn_audit_tail_is_refused", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		if err := audit.Record(completionAuditEvent("q04audite")); err != nil {
			test.Fatal(err)
		}
		copyPath := completionCopyPath(test, filepath.Dir(path), filepath.Base(path))
		encoded, err := os.ReadFile(copyPath)
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(copyPath, encoded[:len(encoded)-1], 0600); err != nil {
			test.Fatal(err)
		}
		if reopened, err := OpenAudit(copyPath); err == nil {
			reopened.Close()
			test.Fatal("torn audit append was accepted")
		}
	})

	test.Run("rewritten_audit_record_is_refused", func(test *testing.T) {
		audit, path := completionOpenAudit(test)
		if err := audit.Record(completionAuditEvent("q04auditf")); err != nil {
			test.Fatal(err)
		}
		copyPath := completionCopyPath(test, filepath.Dir(path), filepath.Base(path))
		encoded, err := os.ReadFile(copyPath)
		if err != nil {
			test.Fatal(err)
		}
		tampered := bytes.Replace(encoded, []byte(`"outcome":"ok"`), []byte(`"outcome":"no"`), 1)
		if err := os.WriteFile(copyPath, tampered, 0600); err != nil {
			test.Fatal(err)
		}
		if reopened, err := OpenAudit(copyPath); err == nil {
			reopened.Close()
			test.Fatal("rewritten audit record was accepted")
		}
	})
}

// The model below is the design's reference reducer, not production code. One
// envelope carries an operation result and, when the request is auditable, the
// audit event. Both views are pure functions of the verified envelope prefix.
type completionEnvelopePayload struct {
	Version   uint32      `json:"version"`
	Sequence  uint64      `json:"sequence"`
	Previous  string      `json:"previous"`
	Operation string      `json:"operation"`
	Audit     *AuditEvent `json:"audit,omitempty"`
}

type completionEnvelopeRecord struct {
	Payload completionEnvelopePayload `json:"payload"`
	Seal    string                    `json:"seal"`
}

var completionGenesisSeal = strings.Repeat("0", 64)

func completionEnvelope(previous completionEnvelopeRecord, sequence uint64, operation string, event *AuditEvent) completionEnvelopeRecord {
	previousSeal := previous.Seal
	if previousSeal == "" {
		previousSeal = completionGenesisSeal
	}
	payload := completionEnvelopePayload{Version: 2, Sequence: sequence, Previous: previousSeal, Operation: operation, Audit: event}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return completionEnvelopeRecord{Payload: payload, Seal: digestHex(encoded)}
}

func completionReduce(initial map[string]string, records []completionEnvelopeRecord) (map[string]string, []auditRecord, error) {
	operations := make(map[string]string, len(initial))
	for key, value := range initial {
		operations[key] = value
	}
	previousSeal := completionGenesisSeal
	auditSequence := uint64(0)
	auditPrevious := strings.Repeat("0", 64)
	audits := []auditRecord{}
	for index, record := range records {
		if record.Payload.Version != 2 || record.Payload.Sequence != uint64(index+1) || record.Payload.Previous != previousSeal {
			return nil, nil, fmt.Errorf("envelope chain broke at %d", index)
		}
		payload, err := json.Marshal(record.Payload)
		if err != nil {
			return nil, nil, err
		}
		if digestHex(payload) != record.Seal {
			return nil, nil, fmt.Errorf("envelope seal broke at %d", index)
		}
		if record.Payload.Operation != "" {
			operations["op"] = record.Payload.Operation
		}
		if record.Payload.Audit != nil {
			auditPayloadValue := auditPayload{Version: 1, Sequence: auditSequence + 1, Previous: auditPrevious, Event: *record.Payload.Audit}
			encoded, err := json.Marshal(auditPayloadValue)
			if err != nil {
				return nil, nil, err
			}
			audits = append(audits, auditRecord{Payload: auditPayloadValue, Hash: digestHex(encoded)})
			auditPrevious, auditSequence = digestHex(encoded), auditSequence+1
		}
		previousSeal = record.Seal
	}
	return operations, audits, nil
}

// TestCompletionEnvelopeModelCrashPoints runs the design's crash matrix against
// the reference reducer. It is the executable form of the design's claim: an ACK
// exists only after the seal is fsynced, and the two views can never disagree
// about which envelopes are in the verified prefix.
func TestCompletionEnvelopeModelCrashPoints(test *testing.T) {
	event := completionAuditEvent("q04model")
	terminal := completionEnvelope(completionEnvelopeRecord{}, 1, "completed", &event)
	intentOnly := completionEnvelope(completionEnvelopeRecord{}, 1, "in_flight", nil)
	cases := []struct {
		name         string
		records      []completionEnvelopeRecord
		acknowledged bool
		wantAudit    int
	}{
		{name: "before_write", records: nil, acknowledged: false, wantAudit: 0},
		{name: "after_write_before_fsync_present", records: []completionEnvelopeRecord{terminal}, acknowledged: false, wantAudit: 1},
		{name: "after_write_before_fsync_lost", records: nil, acknowledged: false, wantAudit: 0},
		{name: "after_fsync_before_ack", records: []completionEnvelopeRecord{terminal}, acknowledged: false, wantAudit: 1},
		{name: "after_fsync_and_ack", records: []completionEnvelopeRecord{terminal}, acknowledged: true, wantAudit: 1},
	}
	for _, scenario := range cases {
		test.Run(scenario.name, func(test *testing.T) {
			operations, audits, err := completionReduce(map[string]string{"op": "in_flight"}, scenario.records)
			if err != nil {
				test.Fatal(err)
			}
			if scenario.acknowledged && len(scenario.records) == 0 {
				test.Fatal("model acknowledged without a durable envelope")
			}
			if scenario.acknowledged && operations["op"] != "completed" {
				test.Fatalf("acknowledged completion reduced to %q", operations["op"])
			}
			if len(audits) != scenario.wantAudit {
				test.Fatalf("audit view holds %d records, want %d", len(audits), scenario.wantAudit)
			}
			if !scenario.acknowledged && len(scenario.records) == 0 && operations["op"] != "in_flight" {
				test.Fatalf("missing completion invented the state %q", operations["op"])
			}
			if operations["op"] == "not_executed" {
				test.Fatal("an unacknowledged dispatch was reduced as not executed")
			}
		})
	}

	// The audit sequence is a projection: envelopes without an audit event do not
	// consume it, and the records before and after still form one chain.
	intent := completionEnvelope(completionEnvelopeRecord{}, 1, "in_flight", nil)
	completed := completionEnvelope(intent, 2, "completed", &event)
	operations, audits, err := completionReduce(map[string]string{"op": "in_flight"}, []completionEnvelopeRecord{intent, completed})
	if err != nil {
		test.Fatal(err)
	}
	if operations["op"] != "completed" || len(audits) != 1 || audits[0].Payload.Sequence != 1 || audits[0].Payload.Previous != strings.Repeat("0", 64) {
		test.Fatalf("audit projection diverged from envelope order: %+v %+v", operations, audits)
	}
	if intentOnly.Seal == completed.Seal {
		test.Fatal("distinct envelopes share a seal")
	}
}
