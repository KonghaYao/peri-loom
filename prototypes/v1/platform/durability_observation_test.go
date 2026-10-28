package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func collectDurabilityEvents(observer *durabilityObserver) []durabilityEvent {
	var events []durabilityEvent
	for {
		select {
		case event := <-observer.queue:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestDurabilityObservationJournalBatchBarrierAndFailure(test *testing.T) {
	for _, fail := range []bool{false, true} {
		test.Run(fmt.Sprint(fail), func(test *testing.T) {
			store, _ := commitQueueStore(test)
			defer store.Close()
			observer := newDurabilityObserver(64)
			store.operations.observation.Store(observer)
			releasePop := gatePop(test, store)
			synced, releaseSync := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(releaseSync) }) }
			// Fatal invokes defers before Cleanup; release the gate before Close joins it.
			defer unblock()
			test.Cleanup(unblock)
			store.operations.testSyncGate = func() {
				close(synced)
				<-releaseSync
				if fail {
					// Fail the actual Sync after the write, not an observation callback.
					_ = store.operations.file.Close()
				}
			}
			results := make(chan error, 3)
			for index := range 3 {
				go func() { results <- operationSettled(store, securityDatabase(), fmt.Sprintf("observed_%d", index)) }()
			}
			waitOrdered(test, store, 3)
			releasePop()
			operationAwait(test, synced)
			select {
			case err := <-results:
				test.Fatalf("caller returned before its sync: %v", err)
			default:
			}
			prepares := collectDurabilityEvents(observer)
			if len(prepares) != 3 {
				unblock()
				test.Fatalf("missing pre-barrier enqueue evidence: %+v", prepares)
			}
			for _, event := range prepares {
				if event.Kind != "journal_prepare" || event.Outcome != "enqueued" {
					unblock()
					test.Fatalf("unfinished batch emitted final events: %+v", prepares)
				}
			}
			unblock()
			for range 3 {
				err := awaitCompactionResult(test, results)
				if fail != errors.Is(err, ErrStateUncertain) || (!fail && err != nil) {
					test.Fatalf("failure fanout changed: %v", err)
				}
			}
			events := collectDurabilityEvents(observer)
			var batch *durabilityEvent
			waits := make(map[uint64]durabilityEvent)
			for index := range events {
				event := &events[index]
				if event.Outcome != durabilityOutcome(map[bool]error{true: ErrStateUncertain}[fail]) {
					test.Fatalf("lost batch outcome: %+v", event)
				}
				switch event.Kind {
				case "journal_batch":
					if batch != nil {
						test.Fatal("one physical batch recorded twice")
					}
					batch = event
				case "journal_member_wait":
					waits[event.ID] = *event
				default:
					test.Fatalf("unexpected event: %s", event.Kind)
				}
			}
			if batch == nil || batch.Count != 3 || len(batch.Members) != 3 || len(waits) != 3 || batch.Timings.SyncNS == 0 {
				test.Fatalf("batch/member coverage mismatch: %+v", events)
			}
			for _, member := range batch.Members {
				wait := waits[member.ID]
				if wait.BatchID != batch.ID || wait.Timings.QueueNS == 0 || wait.Timings.WaitNS == 0 || wait.Timings.SyncNS != 0 || member.RequestID == "" {
					test.Fatalf("batch cost duplicated or membership lost: %+v", wait)
				}
			}
		})
	}
}

func TestDurabilityObservationDurableBytesUnchanged(test *testing.T) {
	var journalBytes, snapshotBytes, auditBytes []byte
	witnesses := make(map[string][]byte)
	event := auditFixtureEvent()
	event.TimestampMS = 42
	for _, enabled := range []bool{false, true} {
		store, _ := commitQueueStore(test)
		observer := newDurabilityObserver(64)
		if enabled {
			store.operations.observation.Store(observer)
		}
		if err := operationSettled(store, securityDatabase(), "same_record"); err != nil {
			test.Fatal(err)
		}
		raw, err := os.ReadFile(store.operations.path)
		if err != nil {
			test.Fatal(err)
		}
		if enabled && !bytes.Equal(raw, journalBytes) {
			test.Fatal("observer changed journal bytes or witness")
		}
		journalBytes = raw
		if err := store.operations.compactConfirmed(store.state); err != nil {
			test.Fatal(err)
		}
		raw, err = os.ReadFile(store.operations.snapshotPath())
		if err != nil {
			test.Fatal(err)
		}
		if enabled && !bytes.Equal(raw, snapshotBytes) {
			test.Fatal("observer changed snapshot bytes")
		}
		snapshotBytes = raw
		if err := store.Close(); err != nil {
			test.Fatal(err)
		}
		audit := openAuditAdmissionFixture(test)
		if enabled {
			audit.observation.Store(observer)
		}
		if err := audit.Record(event); err != nil {
			test.Fatal(err)
		}
		if err := audit.Close(); err != nil {
			test.Fatal(err)
		}
		raw, err = os.ReadFile(audit.file.Name())
		if err != nil {
			test.Fatal(err)
		}
		if enabled && !bytes.Equal(raw, auditBytes) {
			test.Fatal("observer changed audit encoding")
		}
		auditBytes = raw
		for _, suffix := range []string{".head", ".head.floor"} {
			raw, err := os.ReadFile(audit.file.Name() + suffix)
			if err != nil {
				test.Fatal(err)
			}
			if enabled && !bytes.Equal(raw, witnesses[suffix]) {
				test.Fatalf("observer changed audit %s bytes", suffix)
			}
			witnesses[suffix] = raw
		}
		if !enabled && len(collectDurabilityEvents(observer)) != 0 {
			test.Fatal("disabled observer received events")
		}
	}
}

func TestDurabilityObservationAuditCheckpointAndClose(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	observer := newDurabilityObserver(256)
	audit.observation.Store(observer)
	entered, release := blockAuditCheckpoint(test, audit)
	fillAuditCheckpoint(test, audit, entered)
	result := make(chan error, 1)
	go func() { result <- audit.Record(auditFixtureEvent()) }()
	waitAuditStatus(test, audit, "observed checkpoint waiter", func(status map[string]any) bool { return status["queue_count"] == 1 })
	closed := make(chan error, 1)
	go func() { closed <- audit.Close() }()
	select {
	case err := <-closed:
		test.Fatalf("Close escaped checkpoint: %v", err)
	case err := <-result:
		test.Fatalf("queued record escaped checkpoint: %v", err)
	default:
	}
	release()
	if err := awaitCompactionResult(test, result); err != nil {
		test.Fatal(err)
	}
	if err := awaitCompactionResult(test, closed); err != nil {
		test.Fatal(err)
	}
	events := collectDurabilityEvents(observer)
	var checkpoint uint64
	var linked bool
	var batches, waits, checkpoints int
	for _, event := range events {
		switch event.Kind {
		case "audit_checkpoint":
			checkpoints++
			if event.Revision == auditBatchMaximum {
				if event.Mode != "post_batch" {
					test.Fatal("checkpoint trigger lost")
				}
				checkpoint = event.ID
			} else if event.Mode != "shutdown" {
				test.Fatal("shutdown checkpoint trigger lost")
			}
		case "audit_batch":
			batches++
			if event.Count == 1 && len(event.Members) == 1 && event.Members[0].Sequence == auditBatchMaximum+1 {
				linked = event.PreviousMaintenance == checkpoint && checkpoint != 0
			}
		case "audit_member_wait":
			waits++
		}
	}
	if !linked || batches != auditBatchMaximum+1 || waits != batches || checkpoints != 2 {
		test.Fatalf("checkpoint/member/close evidence incomplete: linked=%v batches=%d waits=%d checkpoints=%d", linked, batches, waits, checkpoints)
	}
}

func TestDurabilityObservationAuditBarrierFailure(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	observer := newDurabilityObserver(32)
	audit.observation.Store(observer)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	test.Cleanup(unblock)
	audit.testSyncGate = func() { close(entered); <-release; _ = audit.file.Close() }
	result := make(chan error, 1)
	go func() { result <- audit.Record(auditFixtureEvent()) }()
	operationAwait(test, entered)
	select {
	case err := <-result:
		test.Fatalf("audit acknowledged before sync: %v", err)
	default:
	}
	unblock()
	if err := awaitCompactionResult(test, result); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("audit barrier failure lost: %v", err)
	}
	events := collectDurabilityEvents(observer)
	if len(events) != 2 {
		test.Fatalf("want batch and member events: %+v", events)
	}
	for _, event := range events {
		if event.Outcome != "error" {
			test.Fatalf("failed barrier observed as success: %+v", event)
		}
	}
}

func TestDurabilityObservationAuditSharedBatch(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	observer := newDurabilityObserver(32)
	audit.observation.Store(observer)
	entered, release := make(chan struct{}), make(chan struct{})
	var gateOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	test.Cleanup(unblock)
	audit.testSyncGate = func() { gateOnce.Do(func() { close(entered); <-release }) }
	results := make(chan error, 4)
	go func() { results <- audit.Record(auditFixtureEvent()) }()
	operationAwait(test, entered)
	for range 3 {
		go func() { results <- audit.Record(auditFixtureEvent()) }()
	}
	waitAuditStatus(test, audit, "three batched waiters", func(status map[string]any) bool { return status["queue_count"] == 3 })
	unblock()
	for range 4 {
		if err := awaitCompactionResult(test, results); err != nil {
			test.Fatal(err)
		}
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	var shared *durabilityEvent
	waits := make(map[uint64]durabilityEvent)
	events := collectDurabilityEvents(observer)
	for index := range events {
		event := &events[index]
		if event.Kind == "audit_batch" && event.Count == 3 {
			shared = event
		}
		if event.Kind == "audit_member_wait" {
			waits[event.ID] = *event
		}
	}
	if shared == nil || len(shared.Members) != 3 || audit.syncCount.Load() != 2 {
		test.Fatalf("concurrent append batch lost: %+v", events)
	}
	for _, member := range shared.Members {
		wait := waits[member.ID]
		if wait.BatchID != shared.ID || wait.Timings.SyncNS != 0 || wait.Timings.WaitNS == 0 {
			test.Fatal("audit batch cost repeated on members")
		}
	}
}

func TestDurabilityObservationCompactionBoundaries(test *testing.T) {
	for _, fail := range []bool{false, true} {
		test.Run(fmt.Sprint(fail), func(test *testing.T) {
			store, _ := commitQueueStore(test)
			defer store.Close()
			observer := newDurabilityObserver(32)
			store.operations.observation.Store(observer)
			primeOperationCompaction(store)
			if fail {
				store.operations.testCompactionGate = func(phase string) error {
					if phase == "after_snapshot" {
						return errors.New("injected")
					}
					return nil
				}
			}
			err := operationSettled(store, securityDatabase(), "compact_observed")
			if (err != nil) != fail {
				test.Fatalf("compaction result changed: %v", err)
			}
			var found bool
			for _, event := range collectDurabilityEvents(observer) {
				if event.Kind != "journal_compaction" {
					continue
				}
				found = true
				if event.Owner != "background" || event.Revision != 1 || event.Timings.CaptureNS == 0 || event.Timings.EncodeNS == 0 || event.Timings.SnapshotNS == 0 || (event.Outcome == "error") != fail {
					test.Fatalf("compaction phases missing: %+v", event)
				}
				if fail && (event.Timings.SyncNS != 0 || event.Timings.TruncateNS != 0) {
					test.Fatal("failed compaction fabricated later boundaries")
				}
				if !fail && (event.Timings.SyncNS == 0 || event.Timings.TruncateNS == 0) {
					test.Fatal("successful compaction missing later boundaries")
				}
			}
			if !found {
				test.Fatal("compaction event missing")
			}
		})
	}
}

type durabilityTestOutput struct {
	bytes.Buffer
	failure bool
}

func (output *durabilityTestOutput) Write(data []byte) (int, error) {
	if output.failure {
		return 0, io.ErrClosedPipe
	}
	return output.Buffer.Write(data)
}
func (output *durabilityTestOutput) Close() error { return nil }

func TestDurabilityObservationSinkBoundedAndIncomplete(test *testing.T) {
	for _, mode := range []string{"complete", "overflow", "cap", "failure"} {
		test.Run(mode, func(test *testing.T) {
			observer := newDurabilityObserver(2)
			output := &durabilityTestOutput{failure: mode == "failure"}
			limit := int64(durabilityObservationBytes)
			if mode == "cap" {
				limit = 2300
			}
			sink := &serviceDurabilityObservation{observer: observer, stream: "test", limit: limit, stopping: make(chan struct{}), done: make(chan struct{})}
			count := 2
			if mode == "overflow" {
				count = 3
			}
			for range count {
				observer.record(durabilityEvent{Kind: "journal_batch", Owner: "background", Outcome: "ok"})
			}
			go sink.consume(output)
			ok := sink.stop()
			if mode == "failure" {
				if ok {
					test.Fatal("sink hid write failure")
				}
				return
			}
			var footer map[string]any
			for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
				var record map[string]any
				if err := json.Unmarshal(line, &record); err != nil {
					test.Fatal(err)
				}
				if record["kind"] == "footer" {
					footer = record
				}
			}
			if footer == nil || footer["coverage_complete"] != false || footer["stream_complete"] != (mode == "complete") {
				test.Fatalf("invalid completeness footer: %s", output.Bytes())
			}
			if int64(output.Len()) > limit {
				test.Fatal("sink exceeded byte cap")
			}
			if !sink.stop() {
				test.Fatal("successful close not idempotent")
			}
		})
	}
}

func TestDurabilityObservationDefaultOffAndExistingFile(test *testing.T) {
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	for _, disabled := range []string{"", "0"} {
		sink, err := startServiceDurabilityObservation(directory, disabled)
		if err != nil || sink != nil {
			test.Fatal("default-off created observer")
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		test.Fatal("default-off performed sink I/O")
	}
	sink, err := startServiceDurabilityObservation(directory, "1")
	if err != nil {
		test.Fatal(err)
	}
	sink.observer.record(durabilityEvent{Kind: "audit_checkpoint", Owner: "background", Outcome: "ok"})
	if !sink.stop() {
		test.Fatal("sink did not finish")
	}
	path := filepath.Join(directory, durabilityObservationFile)
	before, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if second, err := startServiceDurabilityObservation(directory, "1"); err == nil || second != nil {
		test.Fatal("existing diagnostic file overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("old diagnostic evidence changed")
	}
}

func TestDurabilityObservationPrivacyAndOwners(test *testing.T) {
	secret := "SELECT secret key FROM hidden"
	member := durabilityRequestMember(1, 1, secret)
	if member.Owner != "background" || member.RequestID != "" {
		test.Fatal("invalid request identifier escaped")
	}
	for _, request := range []string{"plk1_private", "pldb1.private"} {
		if durabilityRequestMember(1, 1, request).RequestID != "" {
			test.Fatal("credential-shaped identifier escaped")
		}
	}
	for _, count := range []int{0, 2} {
		delta := operationDelta{Set: make(map[string]OperationRecord)}
		for index := range count {
			delta.Set[fmt.Sprint(index)] = OperationRecord{}
		}
		member := durabilityOperationMember(2, 1, delta, nil)
		if member.RequestID != "" || (count == 2 && member.Owner != "multiple") {
			test.Fatal("ambiguous ownership guessed")
		}
	}
	event := auditFixtureEvent()
	event.Subject = secret
	member = durabilityOperationMember(3, 2, operationDelta{}, &event)
	encoded, err := json.Marshal(durabilityEvent{Members: []durabilityMember{member}})
	if err != nil || strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "alice") {
		test.Fatal("payload leaked into observer")
	}
	if member.RequestID != event.RequestID {
		test.Fatal("valid request ID lost")
	}
}

func TestDurabilityObservationFailedSinkDoesNotFailJournal(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	observer := newDurabilityObserver(2)
	sink := &serviceDurabilityObservation{observer: observer, stream: "test", limit: durabilityObservationBytes, stopping: make(chan struct{}), done: make(chan struct{})}
	go sink.consume(&durabilityTestOutput{failure: true})
	store.operations.observation.Store(observer)
	for index := range 8 {
		if err := operationSettled(store, securityDatabase(), fmt.Sprintf("sink_failure_%d", index)); err != nil {
			test.Fatalf("diagnostic failure affected durable operation: %v", err)
		}
	}
	if sink.stop() {
		test.Fatal("failed sink not marked incomplete")
	}
	if store.operations.revision != 8 || store.disk.poisoned {
		test.Fatal("sink failure changed durable state")
	}
}

func TestDurabilityObservationStopTimeoutIsDiagnosticOnly(test *testing.T) {
	observer := newDurabilityObserver(1)
	sink := &serviceDurabilityObservation{observer: observer, stream: "test", stopping: make(chan struct{}), done: make(chan struct{})}
	started := time.Now()
	if sink.stop() {
		test.Fatal("unfinished sink marked complete")
	}
	if time.Since(started) > time.Second {
		test.Fatal("diagnostic close blocked service indefinitely")
	}
	observer.record(durabilityEvent{})
	_, dropped := observer.counts()
	if dropped != 1 {
		test.Fatal("late event was not counted as loss")
	}
}

func TestDurabilityObservationUnjoinedProducerIsIncomplete(test *testing.T) {
	observer := newDurabilityObserver(4)
	output := &durabilityTestOutput{}
	sink := &serviceDurabilityObservation{observer: observer, stream: "test", limit: durabilityObservationBytes, stopping: make(chan struct{}), done: make(chan struct{})}
	go sink.consume(output)
	budget := newShutdownBudget(10 * time.Millisecond)
	defer budget.stop()
	release, finished := make(chan struct{}), make(chan struct{})
	result := runServiceTeardown(budget, []stopStep{{name: "gated producer", run: func(context.Context) error {
		defer close(finished)
		<-release
		observer.record(durabilityEvent{Kind: "audit_checkpoint", Owner: "background"})
		return nil
	}}})
	if !errors.Is(result, ErrShutdownIncomplete) {
		test.Fatalf("expected original shutdown failure: %v", result)
	}
	sink.serviceResult(result)
	if sink.stop() {
		test.Fatal("unjoined producer marked complete")
	}
	if !bytes.Contains(output.Bytes(), []byte(`"producers_joined":false`)) || !bytes.Contains(output.Bytes(), []byte(`"stream_complete":false`)) {
		test.Fatalf("footer hid producer still running: %s", output.Bytes())
	}
	close(release)
	operationAwait(test, finished)
	_, dropped := observer.counts()
	if dropped != 1 {
		test.Fatal("late producer event not counted as a loss")
	}
}
