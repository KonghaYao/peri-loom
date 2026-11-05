package platform

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func deferredCloseTestDatabase() controller.DatabaseID {
	return controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
}

func deferredCloseTestSession(counter *atomic.Int64) *gatewaySession {
	return &gatewaySession{database: deferredCloseTestDatabase(), public: fmt.Sprintf("%064x", counter.Add(1))}
}

func enableDeferredSessionClose(test *testing.T, gateway *Gateway) *deferredSessionClose {
	test.Helper()
	if gateway.deferredClose == nil {
		gateway.deferredClose = newDeferredSessionClose()
	}
	return gateway.deferredClose
}

func deferredCloseStatementResult(autocommit bool) map[string]any {
	return map[string]any{
		"cols":               []any{map[string]any{"name": "value", "decltype": "INTEGER"}},
		"rows":               [][]any{{1}},
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"autocommit":         autocommit,
	}
}

// TestDeferredSessionCloseEligibility pins the conservative proof: only a
// session that never committed, never opened a transaction or cursor, and whose
// last statement said autocommit=true may close after its reply.
func TestDeferredSessionCloseEligibility(t *testing.T) {
	tests := []struct {
		name    string
		session *gatewaySession
		want    bool
	}{
		{"never ran sql", &gatewaySession{pristine: true}, true},
		{"read only autocommit", &gatewaySession{pristine: true, statementAutocommitKnown: true, statementAutocommit: true}, true},
		{"autocommit false", &gatewaySession{pristine: true, statementAutocommitKnown: true, statementAutocommit: false}, false},
		{"committing statement", &gatewaySession{pristine: false}, false},
		{"transaction context", &gatewaySession{pristine: false, statementAutocommitKnown: true, statementAutocommit: true}, false},
		{"poisoned by an unknown outcome", &gatewaySession{pristine: true, poisoned: true}, false},
		{"write cursor", &gatewaySession{pristine: false, writeCursorOpen: true}, false},
		{"read cursor still open", &gatewaySession{pristine: true, readCursorOpen: true}, false},
		{"writer protocol", &gatewaySession{pristine: true, writerProtocol: true}, false},
	}
	for _, test := range tests {
		if got := test.session.deferredCloseEligible(); got != test.want {
			t.Fatalf("%s: eligible=%t want %t", test.name, got, test.want)
		}
	}
}

// deferredCloseFixtureWorker answers the gateway with one read-only statement
// whose autocommit flag the test controls. The close gate keeps the worker
// close in flight while a test inspects the platform's own state.
type deferredCloseFixtureWorker struct {
	sessions       atomic.Int64
	autocommit     bool
	requiresIntent bool
	unknown        bool
	closeGate      <-chan struct{}
	closeStarted   chan<- struct{}
	closeDone      *atomic.Bool
}

func (worker *deferredCloseFixtureWorker) hook(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
	switch command["command"] {
	case "open_session":
		return map[string]any{"session": fmt.Sprintf("%064x", worker.sessions.Add(1)), "readonly": false}, nil
	case "guarded_readonly_statement":
		if worker.unknown {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		if worker.requiresIntent {
			return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
		}
		return deferredCloseStatementResult(worker.autocommit), nil
	case "statement":
		return deferredCloseStatementResult(worker.autocommit), nil
	case "execute":
		return map[string]bool{"executed": true}, nil
	case "open_write_cursor":
		return map[string]any{"cols": []any{map[string]any{"name": "value", "decltype": "INTEGER"}}}, nil
	case "close_session":
		if worker.closeStarted != nil {
			worker.closeStarted <- struct{}{}
		}
		if worker.closeGate != nil {
			<-worker.closeGate
		}
		if worker.closeDone != nil {
			worker.closeDone.Store(true)
		}
		return map[string]bool{"closed": true}, nil
	}
	return nil, fmt.Errorf("unexpected worker command %v", command["command"])
}

func deferredCloseCount(status map[string]any, key string) int64 {
	switch value := status[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	default:
		return 0
	}
}

// TestDeferredSessionCloseEligibilityThroughHTTP covers the statements and
// cursor actions that must keep the synchronous close, and the read-only
// autocommit session that may defer it.
func TestDeferredSessionCloseEligibilityThroughHTTP(t *testing.T) {
	tests := []struct {
		name       string
		worker     *deferredCloseFixtureWorker
		action     func(test *testing.T, fixture *gatewayFixture, session string)
		wantClosed int
	}{
		{
			name:   "read only autocommit",
			worker: &deferredCloseFixtureWorker{autocommit: true},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
				if response.Code != http.StatusOK {
					test.Fatalf("statement: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: 1,
		},
		{
			name:   "autocommit false",
			worker: &deferredCloseFixtureWorker{autocommit: false},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "BEGIN"})
				if response.Code != http.StatusOK {
					test.Fatalf("statement: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: 0,
		},
		{
			name:   "committing statement",
			worker: &deferredCloseFixtureWorker{autocommit: false, requiresIntent: true},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "INSERT INTO t VALUES(1)"})
				if response.Code != http.StatusOK {
					test.Fatalf("statement: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: 0,
		},
		{
			name:   "execute without autocommit",
			worker: &deferredCloseFixtureWorker{},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "SELECT 1"})
				if response.Code != http.StatusOK {
					test.Fatalf("execute: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: 0,
		},
		{
			name:   "write cursor",
			worker: &deferredCloseFixtureWorker{},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/cursor/open", map[string]any{"session": session, "sql": "INSERT INTO t VALUES(1)", "write_cursor": true})
				if response.Code != http.StatusOK {
					test.Fatalf("cursor open: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: 0,
		},
		{
			name:   "unknown outcome",
			worker: &deferredCloseFixtureWorker{unknown: true},
			action: func(test *testing.T, fixture *gatewayFixture, session string) {
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
				if response.Code != http.StatusBadGateway {
					test.Fatalf("statement: %d %s", response.Code, response.Body.String())
				}
			},
			wantClosed: -1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			latch := enableDeferredSessionClose(t, fixture.gateway)
			fixture.upstream.setHook(test.worker.hook)
			session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
			test.action(t, fixture, session)
			response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
			if test.wantClosed < 0 {
				if response.Code != http.StatusBadGateway || gatewayCode(t, response) != "outcome_unknown" {
					t.Fatalf("poisoned close: %d %s", response.Code, response.Body.String())
				}
				if fixture.upstream.count("close_session") != 0 {
					t.Fatal("a poisoned session reached the deferred close path")
				}
				return
			}
			if response.Code != http.StatusOK {
				t.Fatalf("close: %d %s", response.Code, response.Body.String())
			}
			status := latch.Status()
			if test.wantClosed == 1 {
				if deferredCloseCount(status, "enqueued_total") != 1 || deferredCloseCount(status, "inline_total") != 0 {
					t.Fatalf("deferred close account: %v", status)
				}
				latch.wait()
			} else {
				if deferredCloseCount(status, "enqueued_total") != 0 || deferredCloseCount(status, "inline_total") != 0 {
					t.Fatalf("ineligible close entered the account: %v", status)
				}
				if fixture.upstream.count("close_session") != 1 {
					t.Fatalf("ineligible close did not wait for the worker: %d", fixture.upstream.count("close_session"))
				}
			}
			if fixture.upstream.count("close_session") != 1 {
				t.Fatalf("close_session calls: %d", fixture.upstream.count("close_session"))
			}
		})
	}
}

// TestDeferredSessionCloseReturnsTheSeatAndKeepsTheCommand pins the facts the
// reply must keep: the public session is gone and its seat is free while the
// worker call is still in flight, and the deferred call is still exactly the
// close_session command with the upstream session.
func TestDeferredSessionCloseReturnsTheSeatAndKeepsTheCommand(t *testing.T) {
	fixture := newGatewayFixture(t)
	latch := enableDeferredSessionClose(t, fixture.gateway)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	worker := &deferredCloseFixtureWorker{autocommit: true, closeGate: release, closeStarted: started}
	fixture.upstream.setHook(worker.hook)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != http.StatusOK {
		t.Fatalf("close: %d %s", response.Code, response.Body.String())
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the deferred worker close never started")
	}
	if seats := fixture.gateway.admission.sessionSeats(); seats != 0 {
		t.Fatalf("the public session still holds %d seats while the worker close is in flight", seats)
	}
	reuse := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
	if reuse.Code != http.StatusForbidden || gatewayCode(t, reuse) != "invalid_session" {
		t.Fatalf("closed session reused: %d %s", reuse.Code, reuse.Body.String())
	}
	close(release)
	latch.wait()
	fixture.upstream.mu.Lock()
	var closeCall map[string]any
	for _, call := range fixture.upstream.calls {
		if call.command["command"] == "close_session" {
			closeCall = call.command
		}
	}
	fixture.upstream.mu.Unlock()
	if closeCall == nil || closeCall["session"] != fmt.Sprintf("%064x", 1) {
		t.Fatalf("deferred close command: %v", closeCall)
	}
}

// TestDeferredSessionCloseFallsBackInlineAtThePerDatabaseBound proves a full
// account turns the worker wait into latency, never a refusal, and never a
// second goroutine per close.
func TestDeferredSessionCloseFallsBackInlineAtThePerDatabaseBound(t *testing.T) {
	fixture := newGatewayFixture(t)
	latch := newDeferredSessionCloseOf(8, 1, 2)
	fixture.gateway.deferredClose = latch
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	worker := &deferredCloseFixtureWorker{autocommit: true, closeGate: release, closeStarted: started}
	fixture.upstream.setHook(worker.hook)
	first := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	second := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	firstResponse := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstResponse <- gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": first})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the deferred close never reached the worker")
	}
	secondResponse := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		secondResponse <- gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": second})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the saturated account did not close inline")
	}
	close(release)
	if response := <-firstResponse; response.Code != http.StatusOK {
		t.Fatalf("deferred close: %d %s", response.Code, response.Body.String())
	}
	if response := <-secondResponse; response.Code != http.StatusOK {
		t.Fatalf("inline close: %d %s", response.Code, response.Body.String())
	}
	latch.wait()
	status := latch.Status()
	if deferredCloseCount(status, "enqueued_total") != 1 || deferredCloseCount(status, "inline_total") != 1 || deferredCloseCount(status, "pending_max") != 1 {
		t.Fatalf("bound account: %v", status)
	}
}

// TestDeferredSessionCloseFlushesBeforeTheCapacityRetry pins the one legal
// retry: the worker proved open_session never ran, so the platform flushes the
// closes this database owes and asks exactly once more.
func TestDeferredSessionCloseFlushesBeforeTheCapacityRetry(t *testing.T) {
	fixture := newGatewayFixture(t)
	latch := enableDeferredSessionClose(t, fixture.gateway)
	closeStarted := make(chan struct{}, 1)
	closeRelease := make(chan struct{})
	capacityRefused := make(chan struct{}, 1)
	var openAttempts atomic.Int64
	var closeDone atomic.Bool
	var closeSeenBeforeRetry atomic.Bool
	var sessions atomic.Int64
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "close_session":
			closeStarted <- struct{}{}
			<-closeRelease
			closeDone.Store(true)
			return map[string]bool{"closed": true}, nil
		case "open_session":
			switch openAttempts.Add(1) {
			case 1:
				// The first session this test opens.
				return map[string]any{"session": fmt.Sprintf("%064x", sessions.Add(1)), "readonly": false}, nil
			case 2:
				// The second open is refused for capacity before any session
				// exists; the platform must flush the pending close and retry.
				capacityRefused <- struct{}{}
				return nil, sqlWorkerFailureFromReply(true, false, "database session capacity exceeded")
			case 3:
				closeSeenBeforeRetry.Store(closeDone.Load())
				return map[string]any{"session": fmt.Sprintf("%064x", sessions.Add(1)), "readonly": false}, nil
			}
			return nil, fmt.Errorf("unexpected open attempt")
		}
		return nil, fmt.Errorf("unexpected worker command %v", command["command"])
	})
	first := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	closeResponse := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": first})
	if closeResponse.Code != http.StatusOK {
		t.Fatalf("close: %d %s", closeResponse.Code, closeResponse.Body.String())
	}
	select {
	case <-closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the deferred close never reached the worker")
	}
	openResponse := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		openResponse <- gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	}()
	select {
	case <-capacityRefused:
	case <-time.After(5 * time.Second):
		t.Fatal("the capacity refusal never reached the open")
	}
	close(closeRelease)
	response := <-openResponse
	if response.Code != http.StatusOK {
		t.Fatalf("open retry: %d %s", response.Code, response.Body.String())
	}
	if openAttempts.Load() != 3 {
		t.Fatalf("open attempts: %d want 3 (first open, refusal, retry)", openAttempts.Load())
	}
	if !closeSeenBeforeRetry.Load() {
		t.Fatal("the retry did not wait for the pending close to finish")
	}
	if deferredCloseCount(latch.Status(), "flushed_total") != 1 {
		t.Fatalf("flush account: %v", latch.Status())
	}
}

func TestDeferredSessionCloseCapacityRetryHappensOnce(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure string
		code    string
		status  int
		flushed int64
	}{
		{name: "capacity refusal", failure: "database session capacity exceeded", status: http.StatusServiceUnavailable, code: "sql_unavailable", flushed: 0},
		{name: "other refusal", failure: "worker saturated", status: http.StatusServiceUnavailable, code: "sql_unavailable", flushed: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			latch := enableDeferredSessionClose(t, fixture.gateway)
			var attempts atomic.Int64
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] != "open_session" {
					return nil, fmt.Errorf("unexpected worker command %v", command["command"])
				}
				attempts.Add(1)
				return nil, sqlWorkerFailureFromReply(true, false, test.failure)
			})
			response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
			if response.Code != test.status || gatewayCode(t, response) != test.code {
				t.Fatalf("open: %d %s", response.Code, response.Body.String())
			}
			want := int64(1)
			if strings.Contains(test.failure, "session capacity") {
				want = 2
			}
			if attempts.Load() != want {
				t.Fatalf("open attempts: %d want %d", attempts.Load(), want)
			}
			if deferredCloseCount(latch.Status(), "flushed_total") != test.flushed {
				t.Fatalf("flush account: %v", latch.Status())
			}
		})
	}
}

// TestDeferredSessionCloseFusedCapacityRetryHappensOnce pins the same one-retry
// rule on the fused open. The worker wraps the fused session creation with
// not_executed evidence, so the capacity refusal proves no session exists and
// the statement never ran: flushing the pending closes and asking once more
// creates a session, it never replays SQL. Any other refusal keeps the ordinary
// answer and is not retried.
func TestDeferredSessionCloseFusedCapacityRetryHappensOnce(t *testing.T) {
	for _, test := range []struct {
		name     string
		failure  string
		status   int
		code     string
		attempts int64
	}{
		{name: "capacity refusal", failure: "database session capacity exceeded", status: http.StatusOK, attempts: 2},
		{name: "other refusal", failure: "worker saturated", status: http.StatusServiceUnavailable, code: "sql_unavailable", attempts: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			latch := enableDeferredSessionClose(t, fixture.gateway)
			var attempts atomic.Int64
			var retriedFused atomic.Bool
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "capabilities":
					return map[string]any{"readonly_batch_protocol": 1, "open_guarded_statement": 2}, nil
				case "open_guarded_statement":
					if attempts.Add(1) == 1 {
						return nil, sqlWorkerFailureFromReply(true, false, test.failure)
					}
					retriedFused.Store(command["sql"] == "SELECT 42")
					return map[string]any{
						"session":   fusedOpenSession,
						"readonly":  false,
						"statement": map[string]any{"status": "ok", "result": guardedBatchReadResult(42)},
					}, nil
				case "close_session":
					return map[string]bool{"closed": true}, nil
				}
				return nil, fmt.Errorf("unexpected worker command %v", command["command"])
			})
			response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
			if response.Code != test.status {
				t.Fatalf("pipeline: %d %s", response.Code, response.Body.String())
			}
			if test.code != "" && gatewayCode(t, response) != test.code {
				t.Fatalf("pipeline code: %s", gatewayCode(t, response))
			}
			if attempts.Load() != test.attempts {
				t.Fatalf("fused open attempts: %d want %d", attempts.Load(), test.attempts)
			}
			if test.attempts == 2 && !retriedFused.Load() {
				t.Fatal("the retry did not resend the fused command")
			}
			if deferredCloseCount(latch.Status(), "flushed_total") != 0 {
				t.Fatalf("flush account: %v", latch.Status())
			}
			latch.wait()
		})
	}
}

func TestSessionCapacityRefusal(t *testing.T) {
	ambiguous := &SQLRPCError{Ambiguous: true, workerFailure: "database session capacity exceeded"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"database capacity", sqlWorkerFailureFromReply(true, false, "database session capacity exceeded"), true},
		{"worker capacity", sqlWorkerFailureFromReply(true, false, "worker session capacity exceeded"), true},
		{"other refusal", sqlWorkerFailureFromReply(true, false, "worker saturated"), false},
		{"ambiguous capacity", ambiguous, false},
		{"unknown text", sqlWorkerFailureFromReply(true, false, "boom"), false},
		{"not a sql failure", fmt.Errorf("boom"), false},
	}
	for _, test := range tests {
		if got := sessionCapacityRefusal(test.err); got != test.want {
			t.Fatalf("%s: %t want %t", test.name, got, test.want)
		}
	}
}

func TestDeferredSessionCloseStopDrainsPending(t *testing.T) {
	latch := newDeferredSessionCloseOf(4, 4, 1)
	var counter atomic.Int64
	session := deferredCloseTestSession(&counter)
	started := make(chan struct{})
	release := make(chan struct{})
	if !latch.enqueue(session, func(context.Context) error {
		close(started)
		<-release
		return nil
	}) {
		t.Fatal("the account refused the only close")
	}
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- latch.stop(context.Background()) }()
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("stop: %v", err)
	}
	if pending := deferredCloseCount(latch.Status(), "pending"); pending != 0 {
		t.Fatalf("stop left %d pending", pending)
	}
}

func TestDeferredSessionCloseStopReportsUnresolved(t *testing.T) {
	latch := newDeferredSessionCloseOf(4, 4, 1)
	var counter atomic.Int64
	session := deferredCloseTestSession(&counter)
	attempted := make(chan struct{})
	if !latch.enqueue(session, func(context.Context) error {
		close(attempted)
		return fmt.Errorf("worker unavailable")
	}) {
		t.Fatal("the account refused the only close")
	}
	<-attempted
	err := latch.stop(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("stop error: %v", err)
	}
	status := latch.Status()
	if deferredCloseCount(status, "pending") != 0 || deferredCloseCount(status, "failed_total") != 1 || deferredCloseCount(status, "unresolved_total") != 1 {
		t.Fatalf("unresolved account: %v", status)
	}
}

// TestDeferredSessionCloseStopAbandonsTheQueueWhenTheBudgetExpires pins the
// stop contract: once the shutdown budget is spent, the queued closes are
// abandoned without each paying another attempt, an attempt already in flight
// is cancelled through the lifetime it took, and the account reports exactly
// what it could not finish.
func TestDeferredSessionCloseStopAbandonsTheQueueWhenTheBudgetExpires(t *testing.T) {
	latch := newDeferredSessionCloseOf(8, 8, 1)
	var counter atomic.Int64
	var queuedAttempts atomic.Int64
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	if !latch.enqueue(deferredCloseTestSession(&counter), func(context.Context) error {
		close(firstStarted)
		<-firstRelease
		return nil
	}) {
		t.Fatal("the account refused the first close")
	}
	<-firstStarted
	for index := 0; index < 3; index++ {
		if !latch.enqueue(deferredCloseTestSession(&counter), func(context.Context) error {
			queuedAttempts.Add(1)
			return nil
		}) {
			t.Fatal("the account refused a queued close")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- latch.stop(ctx) }()
	cancel()
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("stop reported a clean drain after the budget expired")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not honour the shutdown budget")
	}
	close(firstRelease)
	latch.wait()
	if queuedAttempts.Load() != 0 {
		t.Fatalf("queued closes paid %d attempts after the budget expired", queuedAttempts.Load())
	}
	status := latch.Status()
	if deferredCloseCount(status, "pending") != 0 || deferredCloseCount(status, "unresolved_total") != 3 {
		t.Fatalf("budget-exhausted account: %v", status)
	}
}

// TestDeferredSessionCloseKeepsTheCursorFallbackBeforeTheClose is the R43
// regression: a session the cursor fallback still needs may not have its worker
// close issued before the fallback finishes. The deferred close is dispatched
// only by the pipeline's close entry, so the fallback's describe and cursor
// calls must all precede it.
func TestDeferredSessionCloseKeepsTheCursorFallbackBeforeTheClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	latch := enableDeferredSessionClose(t, fixture.gateway)
	gatewayQuota(t, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 512 })
	worker := &fusedBudgetWorker{large: guardedBatchReadResult(strings.Repeat("x", 512))}
	fixture.upstream.setHook(worker.hook)
	response := runPipelineAuditRequest(t, newHranaHTTP(fixture.gateway), fixture, fusedOpenPipelineRequests())
	if response.Code != http.StatusOK {
		t.Fatalf("pipeline: %d %s", response.Code, response.Body.String())
	}
	latch.wait()
	fixture.upstream.mu.Lock()
	order := make([]string, 0, len(fixture.upstream.calls))
	for _, call := range fixture.upstream.calls {
		name, _ := call.command["command"].(string)
		order = append(order, name)
	}
	fixture.upstream.mu.Unlock()
	index := func(name string) int {
		for position, candidate := range order {
			if candidate == name {
				return position
			}
		}
		return -1
	}
	closeAt := index("close_session")
	if closeAt < 0 {
		t.Fatalf("the close never reached the worker: %v", order)
	}
	for _, name := range []string{"describe", "open_read_cursor", "fetch_read_cursor"} {
		if position := index(name); position < 0 || position > closeAt {
			t.Fatalf("%s ran at %d, close at %d: %v", name, position, closeAt, order)
		}
	}
	if deferredCloseCount(latch.Status(), "enqueued_total") != 1 {
		t.Fatalf("the close did not use the deferred path: %v", latch.Status())
	}
}

// TestDeferredSessionCloseDisabledKeepsTheSynchronousClose pins the gateway end
// of the kill switch: with DeferredSessionClose false (what service startup
// derives from PERI_DEFERRED_SESSION_CLOSE=0) there is no account, no goroutine,
// no counter and the worker close still finishes before the reply is written.
func TestDeferredSessionCloseDisabledKeepsTheSynchronousClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	worker := &deferredCloseFixtureWorker{autocommit: true}
	fixture.upstream.setHook(worker.hook)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != http.StatusOK {
		t.Fatalf("close: %d %s", response.Code, response.Body.String())
	}
	if fixture.gateway.deferredClose != nil {
		t.Fatal("the disabled gateway created a deferred close account")
	}
	if fixture.upstream.count("close_session") != 1 {
		t.Fatalf("close_session calls: %d", fixture.upstream.count("close_session"))
	}
	metrics := fixture.gateway.Metrics()
	if _, exists := metrics["deferred_session_close"]; exists {
		t.Fatal("a disabled account published metrics")
	}
}

func TestDeferredSessionCloseFlagParsing(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   string
		unset   bool
		want    bool
		wantErr bool
	}{
		{"unset enables the account", "", true, true, false},
		{"empty value enables the account", "", false, true, false},
		{"explicit 0 keeps the synchronous close", "0", false, false, false},
		{"explicit 1 enables the account", "1", false, true, false},
		{"surrounding whitespace is trimmed", " 1 ", false, true, false},
		{"an illegal value is refused", "yes", false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PERI_DEFERRED_SESSION_CLOSE", test.value)
			if test.unset {
				os.Unsetenv("PERI_DEFERRED_SESSION_CLOSE")
			}
			got, err := deferredSessionCloseEnabled()
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("value %q unset=%t: enabled=%t err=%v", test.value, test.unset, got, err)
			}
		})
	}
}

// TestDeferredSessionCloseRetriesThroughTheCleanupLedger covers the failure
// path: one failed worker close is retried, and the retry uses the same
// sessionCleanup backoff variable as the ordinary cleanup gate.
func TestDeferredSessionCloseRetriesThroughTheCleanupLedger(t *testing.T) {
	previous := sessionCleanupBackoff
	sessionCleanupBackoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { sessionCleanupBackoff = previous })
	latch := newDeferredSessionCloseOf(4, 4, 1)
	var counter atomic.Int64
	session := deferredCloseTestSession(&counter)
	var attempts atomic.Int64
	if !latch.enqueue(session, func(context.Context) error {
		if attempts.Add(1) == 1 {
			return fmt.Errorf("worker unavailable")
		}
		return nil
	}) {
		t.Fatal("the account refused the only close")
	}
	latch.wait()
	status := latch.Status()
	if deferredCloseCount(status, "failed_total") != 1 || deferredCloseCount(status, "retried_total") != 1 || deferredCloseCount(status, "enqueued_total") != 1 {
		t.Fatalf("retry account: %v", status)
	}
	if session.cleanup.attempts != 2 {
		t.Fatalf("cleanup attempts: %d", session.cleanup.attempts)
	}
}

// TestGatewayCloseReportsAnUnresolvedDeferredClose ties the account's final
// state to the shutdown error the operator sees.
func TestGatewayCloseReportsAnUnresolvedDeferredClose(t *testing.T) {
	fixture := newGatewayFixture(t)
	latch := enableDeferredSessionClose(t, fixture.gateway)
	var counter atomic.Int64
	session := deferredCloseTestSession(&counter)
	attempted := make(chan struct{})
	if !latch.enqueue(session, func(context.Context) error {
		close(attempted)
		return fmt.Errorf("worker unavailable")
	}) {
		t.Fatal("the account refused the only close")
	}
	<-attempted
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := fixture.gateway.Close(ctx)
	if err == nil || !strings.Contains(err.Error(), "deferred session close unresolved") {
		t.Fatalf("gateway close: %v", err)
	}
}
