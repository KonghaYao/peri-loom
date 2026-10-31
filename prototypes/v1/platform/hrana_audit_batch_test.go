package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// pipelineReadReply is a valid read-only statement result the fixture worker can
// answer for either the guarded statement or a plain statement call.
func pipelineReadReply() map[string]any {
	return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}
}

// pipelineReadHook models a worker that proves SELECT read-only and refuses a
// write before executing it, so the gateway takes the committing path for the
// write while reads stay unregistered.
func pipelineReadHook() func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			sql, _ := command["sql"].(string)
			return testDescribeVerdict(sql), nil
		case "execute":
			return map[string]bool{"executed": true}, nil
		case "guarded_readonly_statement":
			sql, _ := command["sql"].(string)
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
				return pipelineReadReply(), nil
			}
			return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
		case "statement":
			return pipelineReadReply(), nil
		}
		return nil, nil
	}
}

func hranaPipelinePath(database controller.DatabaseID) string {
	return fmt.Sprintf("/db/%s/%s/%s/v2/pipeline", database.Namespace.Tenant, database.Namespace.Database, database.Branch)
}

func hranaPipelineRequest(test *testing.T, ctx context.Context, token string, database controller.DatabaseID, requests []any) *http.Request {
	test.Helper()
	body, err := json.Marshal(map[string]any{"requests": requests})
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, hranaPipelinePath(database), bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	return request
}

func runPipelineAuditRequest(test *testing.T, handler http.Handler, fixture *gatewayFixture, requests []any) *httptest.ResponseRecorder {
	test.Helper()
	request := hranaPipelineRequest(test, context.Background(), fixture.tokenA, fixture.first, requests)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// auditChainEvents walks the durable chain and returns the events in sequence
// order. Any gap, reorder or rewritten record fails the test.
func auditChainEvents(test *testing.T, path string) []AuditEvent {
	test.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	var events []AuditEvent
	previous := strings.Repeat("0", 64)
	var sequence uint64
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record auditRecord
		if err := json.Unmarshal(line, &record); err != nil {
			test.Fatalf("invalid audit line: %v", err)
		}
		if record.Payload.Version != 1 || record.Payload.Sequence != sequence+1 || record.Payload.Previous != previous {
			test.Fatalf("audit chain broke at %d", sequence+1)
		}
		encoded, err := json.Marshal(record.Payload)
		if err != nil || digestHex(encoded) != record.Hash {
			test.Fatalf("audit hash broke at %d", record.Payload.Sequence)
		}
		sequence, previous = record.Payload.Sequence, record.Hash
		events = append(events, record.Payload.Event)
	}
	return events
}

func reservePipelineCompletion(test *testing.T, audit *fileAudit) AuditReservation {
	test.Helper()
	reservation, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatal(err)
	}
	return reservation
}

func addPipelineCompletion(test *testing.T, collector *pipelineAuditCollector, audit *fileAudit, event AuditEvent) {
	test.Helper()
	consumed, err := collector.Add(event, reservePipelineCompletion(test, audit))
	if !consumed || err != nil {
		test.Fatalf("completion was not prepared: consumed=%t err=%v", consumed, err)
	}
}

func auditCompletionStatus(status map[string]any) (reserved, admitted int) {
	return status["reserved_completion_slots"].(int), auditClassStatus(status, "admitted_by_class", "completion")
}

func TestAuditPrepareSubmitPreservesChain(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	test.Cleanup(func() { _ = audit.Close() })

	// A rejected preparation releases its reservation and writes nothing.
	invalid := reservePipelineCompletion(test, audit)
	if _, err := audit.prepareCompletionOperation(AuditEvent{RequestID: "request", Subject: "alice", Project: "alpha", Action: "invalid SQL", Outcome: "ok"}, invalid.(*auditReservation), nil); err == nil {
		test.Fatal("invalid event prepared")
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		test.Fatalf("rejected preparation leaked capacity: reserved=%d admitted=%d", reserved, admitted)
	}

	collector := newPipelineAuditCollector()
	for index := 0; index < pipelineAuditBatchMaximum; index++ {
		addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("chain%02d", index)))
	}
	if pending := collector.Pending(); pending != pipelineAuditBatchMaximum {
		test.Fatalf("pending=%d", pending)
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != pipelineAuditBatchMaximum {
		test.Fatalf("prepared records are not charged to the admitted budget: reserved=%d admitted=%d", reserved, admitted)
	}
	if syncs := audit.syncCount.Load(); syncs != 0 {
		test.Fatalf("preparation paid %d barriers", syncs)
	}
	if err := collector.Flush(); err != nil {
		test.Fatal(err)
	}
	if pending := collector.Pending(); pending != 0 {
		test.Fatalf("flush left %d prepared records", pending)
	}
	if syncs := audit.syncCount.Load(); syncs != 1 {
		test.Fatalf("one batch paid %d barriers, want 1", syncs)
	}
	if _, admitted := auditCompletionStatus(audit.AuditStatus()); admitted != 0 {
		test.Fatalf("durable batch still holds admitted capacity: %d", admitted)
	}
	events := auditChainEvents(test, path)
	if len(events) != pipelineAuditBatchMaximum {
		test.Fatalf("chain has %d records", len(events))
	}
	for index, event := range events {
		if want := fmt.Sprintf("chain%02d", index); event.RequestID != want {
			test.Fatalf("record %d has request %q, want %q", index, event.RequestID, want)
		}
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if sequence := reopened.(*fileAudit).state.Sequence; sequence != pipelineAuditBatchMaximum {
		test.Fatalf("recovered sequence=%d", sequence)
	}
}

func TestPipelineAuditBatchFlushOnCloseAndWrite(test *testing.T) {
	fixture, service, path := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	fixture.upstream.setHook(pipelineReadHook())
	response := runPipelineAuditRequest(test, newHranaHTTP(fixture.gateway), fixture, []any{
		map[string]any{"type": "sequence", "sql": "SELECT * FROM records"},
		map[string]any{"type": "sequence", "sql": "SELECT * FROM records"},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records VALUES(1)"}},
		map[string]any{"type": "sequence", "sql": "SELECT * FROM records"},
		map[string]any{"type": "close"},
	})
	if response.Code != http.StatusOK {
		test.Fatalf("pipeline failed: %d %s", response.Code, response.Body.String())
	}
	events := auditChainEvents(test, path)
	want := []string{"open", "execute", "execute", "statement", "execute", "close"}
	actual := make([]string, len(events))
	for index, event := range events {
		actual[index] = event.Action
	}
	if strings.Join(actual, ",") != strings.Join(want, ",") {
		test.Fatalf("audit order=%v, want %v", actual, want)
	}
	// The two reads before the write shared one barrier; open, the write, the
	// read after the seal and close each kept their own. The write must not share
	// a barrier with a read on either side.
	if syncs := audit.syncCount.Load(); syncs != 5 {
		test.Fatalf("pipeline paid %d barriers, want 5", syncs)
	}
}

func TestPipelineAuditBatchMixedWriteDoesNotMask(test *testing.T) {
	fixture, service, path := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	fixture.upstream.setHook(pipelineReadHook())
	// The fourth barrier is the read behind the sealed write. Closing the file
	// there makes that read's own row fail without touching the durable write.
	var syncs atomic.Int32
	audit.testSyncGate = func() {
		if syncs.Add(1) == 4 {
			_ = audit.file.Close()
		}
	}
	response := runPipelineAuditRequest(test, newHranaHTTP(fixture.gateway), fixture, []any{
		map[string]any{"type": "sequence", "sql": "SELECT * FROM records"},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records VALUES(1)"}},
		map[string]any{"type": "sequence", "sql": "SELECT * FROM records"},
	})
	if response.Code != http.StatusOK {
		test.Fatalf("pipeline did not answer with its step results: %d %s", response.Code, response.Body.String())
	}
	var reply hranaResponse
	if json.Unmarshal(response.Body.Bytes(), &reply) != nil || len(reply.Results) != 3 {
		test.Fatalf("unexpected pipeline body: %s", response.Body.String())
	}
	for index := 0; index < 2; index++ {
		result, _ := reply.Results[index].(map[string]any)
		if result["type"] != "ok" {
			test.Fatalf("step %d lost its settled result: %v", index, reply.Results[index])
		}
	}
	failed, _ := reply.Results[2].(map[string]any)
	detail, _ := failed["error"].(map[string]any)
	if failed["type"] != "error" || detail["code"] != "audit_unavailable" {
		test.Fatalf("failed read did not report audit_unavailable: %v", reply.Results[2])
	}
	if sequence := audit.state.Sequence; sequence != 3 {
		test.Fatalf("durable sequence=%d, want open + read + write", sequence)
	}
	events := auditChainEvents(test, path)
	if len(events) < 3 || events[0].Action != "open" || events[1].Action != "execute" || events[2].Action != "statement" {
		test.Fatalf("audit order changed around the write: %+v", events)
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	completed := 0
	for _, record := range state.Operations {
		if record.Action == "statement" && record.State == "terminal" && record.BusinessOutcome == "completed" {
			completed++
		}
	}
	if completed != 1 {
		test.Fatalf("the executed write lost its terminal record: %+v", state.Operations)
	}
}

func TestPipelineAuditBatchFlushFailureReleasesReservations(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	test.Cleanup(func() { _ = audit.Close() })
	collector := newPipelineAuditCollector()
	for index := 0; index < pipelineAuditBatchMaximum; index++ {
		addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("flush%02d", index)))
	}
	_ = audit.file.Close()
	if err := collector.Flush(); err == nil {
		test.Fatal("flush over a closed audit file succeeded")
	}
	if collector.Active() {
		test.Fatal("failed collector still accepts completions")
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		test.Fatalf("failed flush retained capacity: reserved=%d admitted=%d", reserved, admitted)
	}
	// A refused handoff leaves the reservation with the caller.
	late := reservePipelineCompletion(test, audit)
	if consumed, err := collector.Add(completionAuditEvent("late"), late); consumed || err != nil {
		test.Fatalf("failed collector consumed a completion: consumed=%t err=%v", consumed, err)
	}
	late.Release()
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		test.Fatalf("refused handoff leaked capacity: reserved=%d admitted=%d", reserved, admitted)
	}

	// Abort releases completions prepared but never submitted.
	secondPath := filepath.Join(test.TempDir(), "audit-abort.jsonl")
	service, err = OpenAudit(secondPath)
	if err != nil {
		test.Fatal(err)
	}
	second := service.(*fileAudit)
	test.Cleanup(func() { _ = second.Close() })
	pending := newPipelineAuditCollector()
	for index := 0; index < 3; index++ {
		addPipelineCompletion(test, pending, second, completionAuditEvent(fmt.Sprintf("abort%d", index)))
	}
	pending.Abort()
	if _, admitted := auditCompletionStatus(second.AuditStatus()); admitted != 0 {
		test.Fatalf("abort retained %d admitted completions", admitted)
	}
	if pending.Pending() != 0 || pending.Active() {
		test.Fatal("aborted collector stayed usable")
	}
	if count := readAuditLineCount(test, secondPath); count != 0 {
		test.Fatalf("abort wrote %d audit records", count)
	}
}

func TestPipelineAuditBatchCrashPrefix(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	test.Cleanup(func() { _ = audit.Close() })
	collector := newPipelineAuditCollector()
	for index := 0; index < pipelineAuditBatchMaximum; index++ {
		addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("crash%02d", index)))
	}
	if err := collector.Flush(); err != nil {
		test.Fatal(err)
	}
	// One more completion is prepared and never submitted: a crash must leave it
	// out of the chain instead of reserving a sequence for it.
	addPipelineCompletion(test, collector, audit, completionAuditEvent("unflushed"))
	audit.writers.Wait()
	if err := audit.file.Close(); err != nil {
		test.Fatal(err)
	}
	if err := audit.head.close(); err != nil {
		test.Fatal(err)
	}
	collector.Abort()
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	recovered := reopened.(*fileAudit)
	if sequence := recovered.state.Sequence; sequence != pipelineAuditBatchMaximum {
		test.Fatalf("recovered sequence=%d, want %d", sequence, pipelineAuditBatchMaximum)
	}
	if err := recovered.Record(completionAuditEvent("aftercrash")); err != nil {
		test.Fatal(err)
	}
	events := auditChainEvents(test, path)
	if len(events) != pipelineAuditBatchMaximum+1 || events[len(events)-1].RequestID != "aftercrash" {
		test.Fatalf("chain after crash: %d records", len(events))
	}
}

func TestPipelineAuditBatchBoundedSlots(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	test.Cleanup(func() { _ = audit.Close() })
	collector := newPipelineAuditCollector()
	for index := 0; index < pipelineAuditBatchMaximum; index++ {
		addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("bound%02d", index)))
	}
	if pending := collector.Pending(); pending != pipelineAuditBatchMaximum {
		test.Fatalf("pending=%d, want the documented batch bound", pending)
	}
	if err := collector.Flush(); err != nil {
		test.Fatal(err)
	}
	if pending := collector.Pending(); pending != 0 {
		test.Fatalf("flush left %d prepared records", pending)
	}
	prepared := 0
	for {
		reservation, err := audit.ReserveCompletion()
		if errors.Is(err, ErrAuditOverloaded) {
			break
		}
		if err != nil {
			test.Fatal(err)
		}
		if _, err := collector.Add(completionAuditEvent(fmt.Sprintf("bound%04d", prepared)), reservation); err != nil {
			test.Fatal(err)
		}
		prepared++
		if prepared > auditCompletionMaximum {
			test.Fatal("completion admission never refused")
		}
	}
	if prepared != auditCompletionMaximum {
		test.Fatalf("bounded %d prepared completions, want %d", prepared, auditCompletionMaximum)
	}
	if _, err := audit.ReserveCompletion(); !errors.Is(err, ErrAuditOverloaded) {
		test.Fatalf("completion capacity beyond the bound was admitted: %v", err)
	}
	collector.Abort()
	if _, admitted := auditCompletionStatus(audit.AuditStatus()); admitted != 0 {
		test.Fatalf("abort retained %d admitted completions", admitted)
	}
	late, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatalf("abort did not return capacity: %v", err)
	}
	late.Release()
}

func TestPipelineAuditBatchV2Unified(test *testing.T) {
	store, audit, _ := unifiedStore(test)
	collector := newPipelineAuditCollector()
	before := store.operations.stats()["operation_journal_syncs_total"]
	for index := 0; index < pipelineAuditBatchMaximum; index++ {
		addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("unified%02d", index)))
	}
	if err := collector.Flush(); err != nil {
		test.Fatal(err)
	}
	stats := store.operations.stats()
	if sequence := store.operations.auditHead.Sequence; sequence != pipelineAuditBatchMaximum {
		test.Fatalf("unified journal sequence=%d", sequence)
	}
	if appends := stats["operation_journal_appends_total"]; appends != pipelineAuditBatchMaximum {
		test.Fatalf("unified journal appended %d records", appends)
	}
	if syncs := stats["operation_journal_syncs_total"] - before; syncs == 0 || syncs > pipelineAuditBatchMaximum {
		test.Fatalf("unified batch paid %d barriers", syncs)
	}
	if _, admitted := auditCompletionStatus(audit.AuditStatus()); admitted != 0 {
		test.Fatalf("unified batch retained admitted capacity: %d", admitted)
	}
	// A single completion after the batch still shares the journal path and
	// resumes the chain without a gap.
	if err := audit.Record(completionAuditEvent("unified-late")); err != nil {
		test.Fatal(err)
	}
	if sequence := store.operations.auditHead.Sequence; sequence != pipelineAuditBatchMaximum+1 {
		test.Fatalf("unified journal did not resume the chain: %d", sequence)
	}
}

func TestPipelineAuditBatchTimeoutCancelPanic(test *testing.T) {
	test.Run("timeout", func(test *testing.T) {
		fixture, service, path := gatewayFileAuditFixture(test)
		audit := service.(*fileAudit)
		fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
			switch command["command"] {
			case "describe":
				sql, _ := command["sql"].(string)
				if strings.Contains(sql, "slow") {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return testDescribeVerdict(sql), nil
			case "execute":
				return map[string]bool{"executed": true}, nil
			}
			return nil, nil
		})
		handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
		handler.timeout = 150 * time.Millisecond
		response := runPipelineAuditRequest(test, handler, fixture, []any{
			map[string]any{"type": "sequence", "sql": "SELECT 1"},
			map[string]any{"type": "sequence", "sql": "SELECT slow"},
		})
		if response.Code != http.StatusGatewayTimeout {
			test.Fatalf("timeout pipeline answered %d: %s", response.Code, response.Body.String())
		}
		if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
			test.Fatalf("timeout leaked audit capacity: reserved=%d admitted=%d", reserved, admitted)
		}
		events := auditChainEvents(test, path)
		if len(events) == 0 || events[0].Action != "open" {
			test.Fatalf("timeout lost the open row: %+v", events)
		}
		settled := false
		for _, event := range events {
			if event.Action == "execute" && event.Outcome == "ok" {
				settled = true
			}
		}
		if !settled {
			test.Fatalf("the read completed before the timeout was not audited: %+v", events)
		}
	})
	test.Run("cancel", func(test *testing.T) {
		fixture, service, _ := gatewayFileAuditFixture(test)
		audit := service.(*fileAudit)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
			switch command["command"] {
			case "describe":
				sql, _ := command["sql"].(string)
				if strings.Contains(sql, "cancel") {
					cancel()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return testDescribeVerdict(sql), nil
			case "execute":
				return map[string]bool{"executed": true}, nil
			}
			return nil, nil
		})
		request := hranaPipelineRequest(test, ctx, fixture.tokenA, fixture.first, []any{
			map[string]any{"type": "sequence", "sql": "SELECT 1"},
			map[string]any{"type": "sequence", "sql": "SELECT cancel"},
		})
		response := httptest.NewRecorder()
		newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
		if response.Code != http.StatusGatewayTimeout {
			test.Fatalf("cancelled pipeline answered %d: %s", response.Code, response.Body.String())
		}
		if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
			test.Fatalf("cancel leaked audit capacity: reserved=%d admitted=%d", reserved, admitted)
		}
	})
	test.Run("panic", func(test *testing.T) {
		path := filepath.Join(test.TempDir(), "audit.jsonl")
		service, err := OpenAudit(path)
		if err != nil {
			test.Fatal(err)
		}
		audit := service.(*fileAudit)
		test.Cleanup(func() { _ = audit.Close() })
		collector := newPipelineAuditCollector()
		func() {
			defer func() {
				if recover() == nil {
					test.Fatal("expected panic")
				}
			}()
			defer collector.Abort()
			for index := 0; index < 3; index++ {
				addPipelineCompletion(test, collector, audit, completionAuditEvent(fmt.Sprintf("panic%d", index)))
			}
			panic("pipeline panic")
		}()
		if _, admitted := auditCompletionStatus(audit.AuditStatus()); admitted != 0 {
			test.Fatalf("panic leaked %d admitted completions", admitted)
		}
		if count := readAuditLineCount(test, path); count != 0 {
			test.Fatalf("panic wrote %d audit records", count)
		}
	})
}

// pipelineGateWriter records the first response write so a test can prove the
// pipeline has not answered yet.
type pipelineGateWriter struct {
	mutex  sync.Mutex
	header http.Header
	status int
	body   bytes.Buffer
	wrote  chan struct{}
	once   sync.Once
}

func newPipelineGateWriter() *pipelineGateWriter {
	return &pipelineGateWriter{header: make(http.Header), wrote: make(chan struct{})}
}

func (writer *pipelineGateWriter) Header() http.Header {
	return writer.header
}

func (writer *pipelineGateWriter) WriteHeader(status int) {
	writer.mutex.Lock()
	if writer.status == 0 {
		writer.status = status
	}
	writer.mutex.Unlock()
}

func (writer *pipelineGateWriter) Write(data []byte) (int, error) {
	writer.mutex.Lock()
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	written, err := writer.body.Write(data)
	writer.mutex.Unlock()
	writer.once.Do(func() { close(writer.wrote) })
	return written, err
}

func (writer *pipelineGateWriter) result() (int, string) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return writer.status, writer.body.String()
}

func TestPipelineAuditBatchResponseWaitsForFlush(test *testing.T) {
	fixture, service, path := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	fixture.upstream.setHook(pipelineReadHook())
	var syncs atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	audit.testSyncGate = func() {
		// The first barrier is the internal open; the second is the final flush
		// of the single read-only completion.
		if syncs.Add(1) == 2 {
			enteredOnce.Do(func() { close(entered) })
			<-release
		}
	}
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	test.Cleanup(releaseGate)
	writer := newPipelineGateWriter()
	handler := newHranaHTTP(fixture.gateway)
	request := hranaPipelineRequest(test, context.Background(), fixture.tokenA, fixture.first, []any{
		map[string]any{"type": "sequence", "sql": "SELECT 1"},
	})
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, request)
		close(done)
	}()
	awaitClosed(test, entered, "the final audit flush")
	select {
	case <-writer.wrote:
		status, body := writer.result()
		test.Fatalf("pipeline answered before its completion row was durable: %d %s", status, body)
	case <-time.After(100 * time.Millisecond):
	}
	releaseGate()
	awaitClosed(test, done, "the pipeline response")
	status, body := writer.result()
	if status != http.StatusOK || !strings.Contains(body, `"results"`) {
		test.Fatalf("pipeline answered %d: %s", status, body)
	}
	events := auditChainEvents(test, path)
	if len(events) != 2 || events[1].Action != "execute" || events[1].Outcome != "ok" {
		test.Fatalf("flush did not leave the read row durable: %+v", events)
	}
}
