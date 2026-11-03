package platform

import (
	"encoding/json"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// holdJournalBarrier parks every operation-journal barrier until release is
// called, so a test can observe what a reply does while its record is not
// durable yet.
func holdJournalBarrier(journal *operationJournal) (chan struct{}, func()) {
	entered := make(chan struct{})
	freed := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	journal.testSyncGate = func() {
		enteredOnce.Do(func() { close(entered) })
		<-freed
	}
	return entered, func() {
		releaseOnce.Do(func() { close(freed) })
	}
}

// A non-committing open and close answer before their unified record is durable.
// The record is queued before the reply, so the chain order is unchanged; only
// the barrier moved behind the answer.
func TestUnifiedNonCommittingReplyDoesNotWaitForTheRecord(test *testing.T) {
	gateway, _, credential, store, _, _ := unifiedGateway(test)
	session := ""
	for _, action := range []string{"open", "close"} {
		body := map[string]any{"database": securityDatabase()}
		path := "/v1/sql/open"
		if action == "close" {
			body, path = map[string]any{"session": session}, "/v1/sql/close"
		}
		entered, release := holdJournalBarrier(store.operations)
		reply := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			reply <- gatewayHTTP(test, gateway, credential, path, body)
		}()
		awaitClosed(test, entered, action+" barrier")
		response := awaitReply(test, reply)
		if response.Code != 200 {
			test.Fatalf("%s: %d %s", action, response.Code, response.Body.String())
		}
		release()
		gateway.deferred.wait()
		store.operations.testSyncGate = nil
		if action == "open" {
			var opened struct {
				Result struct {
					Session string `json:"session"`
				} `json:"result"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &opened); err != nil || opened.Result.Session == "" {
				test.Fatalf("open reply lost its session: %s", response.Body.String())
			}
			session = opened.Result.Session
		}
	}
	if sequence := store.operations.auditHead.Sequence; sequence != 2 {
		test.Fatalf("durable chain=%d, want the deferred open and close records", sequence)
	}
}

// A deferred barrier that fails cannot rewrite the reply it belonged to, so the
// process refuses the next request before it dispatches anything.
func TestDeferredRecordFailureRefusesLaterAdmission(test *testing.T) {
	gateway, upstream, credential, store, _, _ := unifiedGateway(test)
	var closed atomic.Bool
	store.operations.testSyncGate = func() {
		if closed.CompareAndSwap(false, true) {
			_ = store.operations.file.Close()
		}
	}
	test.Cleanup(func() { store.operations.testSyncGate = nil })
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase()})
	if response.Code != 200 {
		test.Fatalf("open waited for its record: %d %s", response.Code, response.Body.String())
	}
	gateway.deferred.wait()
	if failure := gateway.deferred.refusal(); failure == nil {
		test.Fatal("failed deferred record left no refusal behind")
	}
	next := gatewayHTTP(test, gateway, credential, "/v1/sql/query", map[string]any{"session": "ignored", "sql": "SELECT 1"})
	if next.Code != 503 || gatewayCode(test, next) != "audit_unavailable" {
		test.Fatalf("admission after a deferred failure answered %d %s", next.Code, next.Body.String())
	}
	if upstream.count("query") != 0 {
		test.Fatal("refused admission still reached the worker")
	}
}

// A deferred completion releases its admission charge when the queue accepts
// it, not when the barrier returns. Without that, one connection's completed
// replies would hold a slot for its next request and high concurrency would
// exhaust the completion budget with nothing wrong on the durable side.
func TestDeferredCompletionDoesNotHoldTheCompletionSlot(test *testing.T) {
	gateway, _, credential, store, audit, _ := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	gateway.deferred.wait()
	entered, release := holdJournalBarrier(store.operations)
	test.Cleanup(release)
	for index := 0; index < 32; index++ {
		response := gatewayHTTP(test, gateway, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT 1"})
		if response.Code != 200 {
			test.Fatalf("describe %d: %d %s", index, response.Code, response.Body.String())
		}
	}
	awaitClosed(test, entered, "a deferred completion barrier")
	held := make([]AuditReservation, 0, completionReservedMaximum)
	for index := 0; index < completionReservedMaximum; index++ {
		reservation, err := audit.ReserveCompletion()
		if err != nil {
			test.Fatalf("reserve %d while 32 deferred records wait: %v", index, err)
		}
		held = append(held, reservation)
	}
	release()
	gateway.deferred.wait()
	for _, reservation := range held {
		reservation.Release()
	}
}

// A saturated deferred queue waits inline instead of refusing: the barrier
// becomes latency again, exactly as it was before the deferral existed.
func TestDeferredQueueSaturationWaitsInline(test *testing.T) {
	gateway, _, credential, store, _, _ := unifiedGateway(test)
	gateway.deferred = newDeferredDurabilityOf(1, 1)
	entered, release := holdJournalBarrier(store.operations)
	test.Cleanup(release)
	first := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase()})
	if first.Code != 200 {
		test.Fatalf("first open: %d %s", first.Code, first.Body.String())
	}
	awaitClosed(test, entered, "the first deferred barrier")
	reply := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reply <- gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase()})
	}()
	select {
	case response := <-reply:
		test.Fatalf("a full deferred queue answered instead of waiting: %d %s", response.Code, response.Body.String())
	case <-time.After(100 * time.Millisecond):
	}
	release()
	response := awaitReply(test, reply)
	if response.Code != 200 {
		test.Fatalf("second open: %d %s", response.Code, response.Body.String())
	}
	gateway.deferred.wait()
	if sequence := store.operations.auditHead.Sequence; sequence != 2 {
		test.Fatalf("durable chain=%d, want both opens", sequence)
	}
}

// A close is the only request that gives a session back. An exhausted completion
// reserve refuses new work, but it must never refuse the close: that would leak
// the worker session and the seat it holds.
func TestCloseRetiresSessionWhenCompletionCapacityIsExhausted(test *testing.T) {
	gateway, upstream, credential, store, audit, _ := unifiedGateway(test)
	session := unifiedOpenSession(test, gateway, credential)
	gateway.deferred.wait()
	held := make([]AuditReservation, 0, completionReservedMaximum)
	for index := 0; index < completionReservedMaximum; index++ {
		reservation, err := audit.ReserveCompletion()
		if err != nil {
			test.Fatalf("reserve %d: %v", index, err)
		}
		held = append(held, reservation)
	}
	defer func() {
		for _, reservation := range held {
			reservation.Release()
		}
	}()
	refused := gatewayHTTP(test, gateway, credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if refused.Code != 503 || gatewayCode(test, refused) != "audit_unavailable" {
		test.Fatalf("exhausted completion capacity did not refuse the read: %d %s", refused.Code, refused.Body.String())
	}
	if upstream.count("query") != 0 {
		test.Fatal("refused read reached the worker")
	}
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 {
		test.Fatalf("close did not retire its session: %d %s", response.Code, response.Body.String())
	}
	if upstream.count("close_session") != 1 {
		test.Fatal("close was refused before it reached the worker")
	}
	gateway.mu.Lock()
	_, registered := gateway.sessions[session]
	gateway.mu.Unlock()
	if registered {
		test.Fatal("close left its session registered")
	}
	gateway.deferred.wait()
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	opens, closes := 0, 0
	for _, record := range state.Operations {
		switch record.Action {
		case "open":
			opens++
			if record.CleanupOutcome != "completed" {
				test.Fatalf("close did not discharge the open: %+v", record.OperationStatus)
			}
		case "close":
			closes++
			if record.State != "terminal" {
				test.Fatalf("close record is not settled: %+v", record.OperationStatus)
			}
		}
	}
	if opens != 1 || closes != 1 {
		test.Fatalf("durable records: opens=%d closes=%d", opens, closes)
	}
}

// The same rule holds when the audit sink is gone, not merely full: the worker
// session is still closed before the failure is reported.
func TestCloseRetiresSessionWhenAuditIsClosed(test *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.gateway.deferred.wait()
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if fixture.upstream.count("close_session") != 1 {
		test.Fatal("close was refused before it reached the worker")
	}
	if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" {
		test.Fatalf("closed audit did not report audit_unavailable: %d %s", response.Code, response.Body.String())
	}
	fixture.gateway.mu.Lock()
	_, registered := fixture.gateway.sessions[session]
	fixture.gateway.mu.Unlock()
	if registered {
		test.Fatal("close left its session registered")
	}
}
