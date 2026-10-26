package platform

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// These tests exercise the integrated v2 implementation, including its real
// format. The older completion_envelope_fault_test retains the v1/model matrix.
func TestUnifiedCompletionFailureAfterSQLKeepsUnknown(test *testing.T) {
	gateway, upstream, credential, store, audit, directory := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	sequence := store.operations.auditHead.Sequence
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "execute" {
			if err := store.operations.file.Close(); err != nil {
				return nil, err
			}
			return map[string]bool{"executed": true}, nil
		}
		return nil, nil
	})
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (1)"})
	if response.Code != 502 || !strings.Contains(response.Body.String(), "outcome_unknown") {
		test.Fatalf("failed completion invented a definite outcome: %d %s", response.Code, response.Body.String())
	}
	if upstream.count("execute") != 1 || store.operations.auditHead.Sequence != sequence {
		test.Fatal("failed completion replayed SQL or advanced the confirmed audit tail")
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("failed completion left the authority available: %v", err)
	}
	copyDirectory := test.TempDir()
	if err := completionCopyDirectory(directory, copyDirectory); err != nil {
		test.Fatal(err)
	}
	_ = audit.Close()
	_ = store.Close()
	reopened := completionReopenState(test, copyDirectory)
	snapshot, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var unresolved int
	for _, record := range snapshot.Operations {
		if record.Action != "execute" {
			continue
		}
		unresolved++
		if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed || record.BusinessOutcome == "not_executed" {
			test.Fatalf("restart failed to isolate the durable intent: %+v", record.OperationStatus)
		}
	}
	if unresolved != 1 || reopened.operations.auditHead.Sequence != sequence {
		test.Fatal("recovery lost intent or fabricated a completion audit")
	}
}

func TestUnifiedCompletionDurableEnvelopeBeforeLostReply(test *testing.T) {
	gateway, _, credential, store, _, directory := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (1)"})
	if response.Code != 200 {
		test.Fatalf("execute: %d %s", response.Code, response.Body.String())
	}
	// Discard the response as if the client connection died after the durable
	// envelope. A fresh process must recover both facts from the same bytes.
	copyDirectory := test.TempDir()
	if err := completionCopyDirectory(directory, copyDirectory); err != nil {
		test.Fatal(err)
	}
	reopened := completionReopenState(test, copyDirectory)
	snapshot, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var completed int
	for _, record := range snapshot.Operations {
		if record.Action == "execute" && record.State == "terminal" && record.ExecutionProven && record.BusinessOutcome == "completed" {
			completed++
		}
	}
	if completed != 1 || reopened.operations.auditHead != store.operations.auditHead {
		test.Fatal("lost response recovery split the terminal from its audit")
	}
}

func TestUnifiedCompletionSyncFailureRetainsPermit(test *testing.T) {
	gateway, upstream, credential, store, _, directory := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	sequence := store.operations.auditHead.Sequence
	closed := make(chan error, 1)
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "execute" {
			store.operations.testSyncGate = func() { closed <- store.operations.file.Close() }
			return map[string]bool{"executed": true}, nil
		}
		return nil, nil
	})
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (1)"})
	if response.Code != 502 || !strings.Contains(response.Body.String(), "outcome_unknown") {
		test.Fatalf("failed Sync invented success: %d %s", response.Code, response.Body.String())
	}
	if err := <-closed; err != nil {
		test.Fatal(err)
	}
	gateway.operationMu.Lock()
	held := len(gateway.operationHolds)
	gateway.operationMu.Unlock()
	if held != 1 || store.operations.auditHead.Sequence != sequence {
		test.Fatal("failed Sync released the retained permit or published audit")
	}
	_ = gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES (2)"})
	if upstream.count("execute") != 1 {
		test.Fatal("uncertain authority admitted another SQL dispatch")
	}
	copyDirectory := test.TempDir()
	if err := completionCopyDirectory(directory, copyDirectory); err != nil {
		test.Fatal(err)
	}
	// This fault leaves the full envelope in the crash artifact. Recovery can
	// prove both terminal and audit from those surviving bytes even though the
	// interrupted process could only return Unknown to its caller.
	reopened := completionReopenState(test, copyDirectory)
	if reopened.operations.auditHead.Sequence != sequence+1 {
		test.Fatal("surviving complete envelope did not replay atomically")
	}
}

func TestUnifiedCompletionDeferredUnknownUsesReservedCapacity(test *testing.T) {
	gateway, upstream, credential, store, audit, directory := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	before := store.operations.stats()["operation_journal_syncs_total"]
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			store.operations.mutex.Lock()
			store.operations.retainedBytes = completionRetentionSoftBytes
			store.operations.mutex.Unlock()
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 502 || !strings.Contains(response.Body.String(), "outcome_unknown") {
		test.Fatalf("describe: %d %s", response.Code, response.Body.String())
	}
	after := store.operations.stats()["operation_journal_syncs_total"]
	if after-before != 1 {
		test.Fatalf("read Unknown must fuse quarantine and audit: %d barriers", after-before)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened := completionReopenState(test, directory)
	snapshot, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	_, isolated := isolatedOperation(snapshot, securityDatabase(), false)
	var described *OperationRecord
	for _, record := range snapshot.Operations {
		if record.Action == "describe" {
			copy := record
			described = &copy
		}
	}
	if !isolated || described == nil || described.State != "quarantined" || described.RetryAllowed {
		test.Fatalf("reserved read completion lost durable isolation: %+v", described)
	}
	last := reopened.operations.records[len(reopened.operations.records)-1].Payload
	if last.Audit == nil || last.Audit.Payload.Event.Outcome != "outcome_unknown" || len(last.Set) != 1 {
		test.Fatal("deferred Unknown was not an atomic completion envelope")
	}
}

func TestUnifiedCompletionStateCloseHonorsShutdownBudget(test *testing.T) {
	store, _, _ := unifiedStore(test)
	store.maintenance.Add(1)
	budget := newShutdownBudget(30 * time.Millisecond)
	defer budget.stop()
	closed := make(chan error, 1)
	started := time.Now()
	err := runServiceTeardown(budget, []stopStep{{name: "state close", run: func(context.Context) error {
		err := store.Close()
		closed <- err
		return err
	}}})
	store.maintenance.Done()
	if !errors.Is(err, ErrShutdownIncomplete) || time.Since(started) > time.Second {
		test.Fatalf("maintenance blocked the bounded service tail: %v", err)
	}
	if err := <-closed; err != nil {
		test.Fatal(err)
	}
}

func TestUnifiedCompletionInterruptedCompactionRecovers(test *testing.T) {
	for _, point := range []string{"archive_before_snapshot", "snapshot_before_truncate"} {
		test.Run(point, func(test *testing.T) {
			store, audit, directory := unifiedStore(test)
			for index := 0; index < 7; index++ {
				if err := audit.Record(completionAuditEvent(fmt.Sprintf("rotate%02d", index))); err != nil {
					test.Fatal(err)
				}
			}
			prior, err := os.ReadFile(store.operations.path)
			if err != nil {
				test.Fatal(err)
			}
			if point == "archive_before_snapshot" {
				store.operations.write.Lock()
				store.operations.mutex.Lock()
				err = store.operations.archiveCompletionPrefix()
				store.operations.mutex.Unlock()
				store.operations.write.Unlock()
			} else {
				err = store.operations.compactConfirmed(store.state)
			}
			if err != nil {
				test.Fatal(err)
			}
			copyDirectory := test.TempDir()
			if err := completionCopyDirectory(directory, copyDirectory); err != nil {
				test.Fatal(err)
			}
			if point == "snapshot_before_truncate" {
				if err := os.WriteFile(filepath.Join(copyDirectory, "operations", "journal"), prior, 0600); err != nil {
					test.Fatal(err)
				}
			}
			reopened := completionReopenState(test, copyDirectory)
			if reopened.operations.auditHead.Sequence != 7 || len(reopened.operations.records) != 0 {
				test.Fatal("recovery did not finish the proved pending rotation")
			}
			event := completionAuditEvent("afterrotation")
			if err := reopened.updateOperationsAudit(nil, &event, false); err != nil {
				test.Fatal(err)
			}
			if err := reopened.operations.compactConfirmed(reopened.state); err != nil {
				test.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				test.Fatal(err)
			}
			again := completionReopenState(test, copyDirectory)
			if again.operations.auditHead.Sequence != 8 {
				test.Fatal("second compaction duplicated an archived prefix")
			}
		})
	}
}

func TestUnifiedCompletionCorruptAndPartialTails(test *testing.T) {
	for _, point := range []string{"complete_corrupt", "partial_only", "complete_then_partial"} {
		test.Run(point, func(test *testing.T) {
			store, audit, directory := unifiedStore(test)
			if err := audit.Record(completionAuditEvent("tail")); err != nil {
				test.Fatal(err)
			}
			copyDirectory := test.TempDir()
			if err := completionCopyDirectory(directory, copyDirectory); err != nil {
				test.Fatal(err)
			}
			path := filepath.Join(copyDirectory, "operations", "journal")
			original, err := os.ReadFile(path)
			if err != nil {
				test.Fatal(err)
			}
			tail := []byte("{\"payload\":")
			if point != "partial_only" {
				tail = []byte("{\"payload\":\"corrupt\"}\n")
			}
			if point == "complete_then_partial" {
				tail = append(tail, []byte("{\"payload\":")...)
			}
			corrupt := append(append([]byte(nil), original...), tail...)
			if err := os.WriteFile(path, corrupt, 0600); err != nil {
				test.Fatal(err)
			}
			opened, err := OpenStateStore(copyDirectory)
			if point == "partial_only" {
				if err != nil {
					test.Fatal(err)
				}
				defer opened.Close()
				if opened.(*stateStore).operations.auditHead != store.operations.auditHead {
					test.Fatal("partial append changed confirmed audit")
				}
			} else {
				if err == nil {
					opened.Close()
					test.Fatal("complete corrupt record accepted")
				}
				after, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(after, corrupt) {
					test.Fatal("failed verification modified forensic evidence")
				}
			}
		})
	}
}

func TestUnifiedCompletionAuditOutlivesOperationRetention(test *testing.T) {
	_, _, _, store, audit, directory := unifiedGateway(test)
	for index := 0; index < 300; index++ {
		identifier := fmt.Sprintf("retained%04d", index)
		if err := beginOperation(store, securityDatabase(), identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		held, err := audit.ReserveCompletion()
		if err != nil {
			test.Fatal(err)
		}
		err = held.(*auditReservation).RecordOperation(completionAuditEvent(identifier), func(edit *operationEdit) error {
			key := operationKey(securityDatabase(), identifier)
			record, _ := edit.record(key)
			record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
			edit.put(key, record)
			return nil
		})
		if err != nil {
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
	reopened := completionReopenState(test, directory)
	snapshot, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if snapshot.OperationEvictions == 0 || len(snapshot.Operations) > 256 || reopened.operations.auditHead.Sequence != 300 {
		test.Fatal("operation retention evicted authoritative audit")
	}
}
