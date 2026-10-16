package platform

import (
	"bytes"
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

func auditBatchFixture(count int) ([]*auditRequest, []byte, auditHead) {
	batch := make([]*auditRequest, count)
	head := auditHead{Version: 1, Hash: strings.Repeat("0", 64)}
	var lines []byte
	for index := range batch {
		event := auditFixtureEvent()
		// 相同关联 ID 不是去重键；每次调用都必须留下独立记录。
		batch[index] = &auditRequest{event: event, done: make(chan error, 1)}
		payload := auditPayload{Version: 1, Sequence: head.Sequence + 1, Previous: head.Hash, Event: event}
		encoded, _ := json.Marshal(payload)
		head.Sequence++
		head.Hash = digestHex(encoded)
		line, _ := json.Marshal(auditRecord{Payload: payload, Hash: head.Hash})
		lines = append(lines, append(line, '\n')...)
	}
	return batch, lines, head
}

func TestAuditBatchCommitAndValidation(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	defer audit.Close()
	for _, count := range []int{0, auditBatchMaximum + 1} {
		batch, _, _ := auditBatchFixture(count)
		if err := audit.recordBatch(batch); err == nil {
			test.Fatal("invalid batch size accepted")
		}
	}
	batch, _, _ := auditBatchFixture(2)
	batch[1].event.Action = "invalid SQL"
	if err := audit.recordBatch(batch); err == nil || audit.head.revision != 0 || audit.size != 0 {
		test.Fatal("invalid event partially committed")
	}
	batch, lines, head := auditBatchFixture(auditBatchMaximum)
	if err := audit.recordBatch(batch); err != nil {
		test.Fatal(err)
	}
	if audit.state != head {
		test.Fatal("batch lost a record")
	}
	if audit.head.revision != 0 {
		test.Fatal("witness advanced for a batch that stayed inside the bound")
	}
	if err := audit.head.commitAuditBatch(head.Sequence, head); err != nil {
		test.Fatal(err)
	}
	if err := audit.head.commitAuditBatch(head.Sequence, head); err == nil {
		test.Fatal("duplicate revision accepted")
	}
	if err := audit.head.commitAuditBatch(head.Sequence-1, head); err == nil {
		test.Fatal("reordered revision accepted")
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	// A clean shutdown leaves the witness current, so the next open proves every
	// acknowledged record is still present.
	if audit.head.revision != head.Sequence {
		test.Fatal("clean shutdown did not checkpoint the witness")
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, lines) {
		test.Fatal("batch order or chain changed")
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state != head {
		test.Fatal("batch not durable")
	}
}

func TestAuditBatchCrashPrefixes(test *testing.T) {
	for _, scenario := range []string{"old", "head_only", "floor_only", "partial", "complete_floor_behind", "complete", "gap_33", "chain_ahead", "reordered", "duplicate", "truncated"} {
		test.Run(scenario, func(test *testing.T) {
			path := filepath.Join(test.TempDir(), "audit.jsonl")
			service, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			if err := service.Close(); err != nil {
				test.Fatal(err)
			}
			count := auditBatchMaximum
			if scenario == "gap_33" {
				count++
			}
			_, lines, head := auditBatchFixture(count)
			payload, _ := json.Marshal(head)
			envelope := diskEnvelope{Version: 1, Revision: head.Sequence, Checksum: digestHex(payload), Payload: payload}
			if scenario != "old" && scenario != "floor_only" && scenario != "chain_ahead" {
				durableRecoveryWriteJSON(test, path+".head", envelope)
			}
			if scenario == "complete" || scenario == "floor_only" {
				durableRecoveryWriteJSON(test, path+".head.floor", diskFloor{Version: 1, Revision: head.Sequence, Checksum: envelope.Checksum})
			}
			switch scenario {
			case "old", "head_only", "floor_only":
				lines = nil
			case "partial":
				lines = lines[:bytes.LastIndexByte(lines[:len(lines)-1], '\n')+1]
			case "truncated":
				lines = lines[:len(lines)-1]
			case "reordered", "duplicate":
				parts := bytes.SplitAfter(lines, []byte{'\n'})
				if scenario == "reordered" {
					parts[0], parts[1] = parts[1], parts[0]
				} else {
					parts[1] = parts[0]
				}
				lines = bytes.Join(parts, nil)
			}
			if err := os.WriteFile(path, lines, 0600); err != nil {
				test.Fatal(err)
			}
			reopened, err := OpenAudit(path)
			// A chain that leads its checkpoint is the platform's own successful
			// append before the witness caught up, so it must be accepted rather
			// than refused as rollback.
			accepted := scenario == "old" || scenario == "complete" || scenario == "complete_floor_behind" || scenario == "chain_ahead"
			if err == nil {
				defer reopened.Close()
			}
			if (err == nil) != accepted {
				test.Fatalf("accepted=%v err=%v", accepted, err)
			}
			if accepted && scenario != "old" && scenario != "chain_ahead" {
				var floor diskFloor
				if err := readPrivate(path+".head.floor", &floor); err != nil || floor.Revision != head.Sequence || floor.Checksum != envelope.Checksum {
					test.Fatal("floor not advanced to complete batch")
				}
			}
			if accepted && scenario == "chain_ahead" && reopened.(*fileAudit).state != head {
				test.Fatal("chain ahead of the witness was not recovered")
			}
		})
	}
}

// A failed append must leave no record behind and must never be reconstructed from
// the witness: the checkpoint only ever describes already durable records.
func TestAuditBatchAppendFailurePoisonsAllLaterCalls(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	defer audit.Close()
	if err := audit.file.Close(); err != nil {
		test.Fatal(err)
	}
	batch, _, _ := auditBatchFixture(auditBatchMaximum)
	if err := audit.recordBatch(batch); !errors.Is(err, ErrStateUncertain) {
		test.Fatal(err)
	}
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("failed batch allowed another append")
	}
	if err := audit.Close(); err == nil {
		test.Fatal("close hid the already closed append descriptor")
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != 0 {
		test.Fatal("failed batch was accepted as a durable record")
	}
}

func TestAuditBatchConcurrentCloseDrainsAcceptedCalls(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	const count = auditBatchMaximum*2 + 1
	results := make(chan error, count)
	audit.mutex.Lock()
	// 先固定一个正在等待写锁的请求，再让其余请求入队。
	go func() { results <- audit.Record(auditFixtureEvent()) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		audit.queueMutex.Lock()
		started := audit.running && len(audit.pending) == 0
		audit.queueMutex.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			audit.mutex.Unlock()
			test.Fatal("writer did not start")
		}
		time.Sleep(time.Millisecond)
	}
	for index := 1; index < count; index++ {
		go func() { results <- audit.Record(auditFixtureEvent()) }()
	}
	for {
		audit.queueMutex.Lock()
		pending := len(audit.pending)
		audit.queueMutex.Unlock()
		if pending == count-1 {
			break
		}
		if time.Now().After(deadline) {
			audit.mutex.Unlock()
			test.Fatal("requests did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-results:
		audit.mutex.Unlock()
		test.Fatal("acknowledged before durable write")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- audit.Close() }()
	for {
		audit.queueMutex.Lock()
		closing := audit.closing
		audit.queueMutex.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			audit.mutex.Unlock()
			test.Fatal("close did not stop admission")
		}
		time.Sleep(time.Millisecond)
	}
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateClosed) {
		audit.mutex.Unlock()
		test.Fatal("closing audit accepted another record")
	}
	select {
	case <-closed:
		audit.mutex.Unlock()
		test.Fatal("close returned before accepted writes")
	default:
	}
	audit.mutex.Unlock()
	for index := 0; index < count; index++ {
		if err := <-results; err != nil {
			test.Fatal(err)
		}
	}
	if err := <-closed; err != nil {
		test.Fatal(err)
	}
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateClosed) {
		test.Fatal("record after close accepted")
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != count {
		test.Fatal("close lost accepted requests")
	}
}

func BenchmarkAuditWritePath(bench *testing.B) {
	for _, concurrency := range []int{1, 8, 32} {
		bench.Run(fmt.Sprint(concurrency), func(bench *testing.B) {
			audit, err := OpenAudit(filepath.Join(bench.TempDir(), "audit.jsonl"))
			if err != nil {
				bench.Fatal(err)
			}
			defer audit.Close()
			event := auditFixtureEvent()
			bench.ResetTimer()
			for round := 0; round < bench.N; round++ {
				var writers sync.WaitGroup
				writers.Add(concurrency)
				for worker := 0; worker < concurrency; worker++ {
					go func() {
						defer writers.Done()
						if err := audit.Record(event); err != nil {
							bench.Error(err)
						}
					}()
				}
				writers.Wait()
			}
			bench.ReportMetric(float64(bench.Elapsed().Nanoseconds())/float64(bench.N*concurrency), "ns/record")
		})
	}
}

func TestAuditBatchRotationPreservesWholeChain(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	audit.size = auditSegmentBytes - 1
	batch, _, _ := auditBatchFixture(auditBatchMaximum)
	if err := audit.recordBatch(batch); err != nil {
		test.Fatal(err)
	}
	if audit.start != 2 || audit.state.Sequence != auditBatchMaximum+1 {
		test.Fatal("batch rotation split the chain incorrectly")
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != auditBatchMaximum+1 {
		test.Fatal("batch rotation lost records")
	}
}

func TestAuditBatchRejectsOverflowAndOversizedRevision(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	defer audit.Close()
	_, _, head := auditBatchFixture(auditBatchMaximum + 1)
	if err := audit.head.commitAuditBatch(head.Sequence, head); err == nil || audit.head.poisoned {
		test.Fatal("oversized revision must refuse without writing")
	}
	audit.state.Sequence = ^uint64(0) - 1
	batch, _, _ := auditBatchFixture(2)
	if err := audit.recordBatch(batch); err == nil || audit.head.revision != 0 {
		test.Fatal("sequence overflow wrote an audit head")
	}
}
