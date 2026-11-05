package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// settleFixtures starts the committer, parks it at its batch gate, and fills the
// bounded journal queue with settled records. Every settlement submitted until the
// returned release runs is refused with ErrOperationQueueFull; the filler goroutines
// themselves finish only after the release, so waitFillers must follow it.
func settleFixtures(test *testing.T, durable *stateStore, database controller.DatabaseID) (release func(), waitQueued func(), waitFillers func()) {
	test.Helper()
	entered := make(chan struct{})
	gate := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	durable.operations.testPopGate = func() {
		enteredOnce.Do(func() { close(entered) })
		<-gate
	}
	test.Cleanup(func() { durable.operations.testPopGate = nil })
	release = func() { releaseOnce.Do(func() { close(gate) }) }
	test.Cleanup(release)

	var fillers sync.WaitGroup
	failures := make([]error, operationJournalQueueMaximum)
	for index := range failures {
		fillers.Add(1)
		go func() {
			defer fillers.Done()
			failures[index] = operationSettled(durable, database, fmt.Sprintf("settle_filler_%04d", index))
		}()
	}
	waitQueued = func() {
		test.Helper()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			test.Fatal("the committer never reached its gate")
		}
		// The queue depth is the fill state; the ordered counter is cumulative, so a
		// caller that already registered a record before the fillers start makes
		// ordered >= operationJournalQueueMaximum true one filler early. Waiting on
		// the depth holds until every filler is queued, which is what makes the next
		// submit meet the full queue.
		deadline := time.Now().Add(10 * time.Second)
		for operationQueuedDepth(durable) < operationJournalQueueMaximum {
			if time.Now().After(deadline) {
				depth := operationQueuedDepth(durable)
				test.Fatalf("operation queue depth = %d, want %d", depth, operationJournalQueueMaximum)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFillers = func() {
		test.Helper()
		fillers.Wait()
		for index, failure := range failures {
			if failure != nil {
				test.Fatalf("filler %d failed once the committer ran: %v", index, failure)
			}
		}
	}
	return release, waitQueued, waitFillers
}

// The intent-settlement path funnels through the same helper: a write's terminal
// record is retried locally while the SQL call it describes is never repeated.
func TestSettlementRetriesAnIntentSettlement(test *testing.T) {
	store, gateway, _, _, _ := securityGatewayFixture(test)
	durable := store.(*stateStore)
	database := securityDatabase()
	if err := operationInFlight(durable, database, "intent_settle"); err != nil {
		test.Fatal(err)
	}
	release, waitQueued, waitFillers := settleFixtures(test, durable, database)
	waitQueued()

	retried := make(chan struct{}, 1)
	gateway.settle.testRetry = func() {
		select {
		case retried <- struct{}{}:
		default:
		}
	}
	test.Cleanup(func() { gateway.settle.testRetry = nil })

	result := gatewayResult{dispatched: true}
	settled := make(chan error, 1)
	go func() {
		settled <- gateway.settleOperationUsing(context.Background(), database, "intent_settle", "tool_rpc", &result, nil, func(change func(*operationEdit) error) (func() error, error) {
			return nil, updateOperationRecords(gateway.config.State, change)
		})
	}()
	select {
	case <-retried:
		release()
	case <-time.After(2 * time.Second):
		release()
		test.Fatal("intent settlement never retried the local queue refusal")
	}
	select {
	case err := <-settled:
		if err != nil {
			test.Fatalf("intent settlement failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		test.Fatal("intent settlement did not finish after the queue was released")
	}
	waitFillers()
	if result.code != "" {
		test.Fatalf("intent settlement answered %q, want the dispatched success", result.code)
	}
	state, err := durable.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	record, found := state.Operations[operationKey(database, "intent_settle")]
	if !found || !operationKnownTerminal(record) {
		test.Fatalf("intent record = %+v, found=%t, want a known terminal", record, found)
	}
	if record.BusinessOutcome != "completed" {
		test.Fatalf("intent business outcome = %q, want completed", record.BusinessOutcome)
	}
}

// A settlement that meets a full operation journal was refused before anything
// was written, so it must retry the local append. Reporting the SQL as an unknown
// outcome instead leaves a permit and a session seat behind that nothing can
// release, because no durable record exists and the SQL already ran.
func TestSettlementRetriesALocalQueueRefusal(test *testing.T) {
	store, gateway, upstream, _, credential := securityGatewayFixture(test)
	durable := store.(*stateStore)
	database := securityDatabase()
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	release, waitQueued, waitFillers := settleFixtures(test, durable, database)
	waitQueued()

	retried := make(chan struct{}, 1)
	gateway.settle.testRetry = func() {
		select {
		case retried <- struct{}{}:
		default:
		}
	}
	test.Cleanup(func() { gateway.settle.testRetry = nil })

	body, err := json.Marshal(map[string]any{"database": database})
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/sql/open", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+credential)
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		gateway.ServeHTTP(recorder, request)
		response <- recorder
	}()

	select {
	case recorder := <-response:
		release()
		test.Fatalf("open answered %d %s without retrying the local queue refusal", recorder.Code, recorder.Body.String())
	case <-retried:
		// The settlement refused its first attempt and is now backing off outside
		// every lock. Releasing the committer lets the retry land.
		release()
	case <-time.After(2 * time.Second):
		// A settlement that is refused for good is rewritten to an unknown outcome
		// whose hold waits for the queued commits, so without a retry the request
		// does not answer until the committer runs.
		release()
		select {
		case recorder := <-response:
			test.Fatalf("open answered %d %s without retrying the local queue refusal", recorder.Code, recorder.Body.String())
		case <-time.After(10 * time.Second):
			test.Fatal("open neither retried its settlement nor answered")
		}
	}

	var recorder *httptest.ResponseRecorder
	select {
	case recorder = <-response:
	case <-time.After(10 * time.Second):
		test.Fatal("open did not finish after the queue was released")
	}
	if recorder.Code != 200 {
		test.Fatalf("open failed after a retried settlement: %d %s", recorder.Code, recorder.Body.String())
	}
	var opened struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &opened); err != nil || !publicSessionID(opened.Result.Session) {
		test.Fatalf("invalid public session: %s", recorder.Body.String())
	}

	closed := gatewayHTTP(test, gateway, credential, "/v1/sql/close", map[string]any{"session": opened.Result.Session})
	if closed.Code != 200 {
		test.Fatalf("close failed after a retried settlement: %d %s", closed.Code, closed.Body.String())
	}
	waitFillers()
	gateway.deferred.wait()
	if _, active, unknown := gateway.admission.status(); active != 0 || unknown != 0 {
		test.Fatalf("settlement retry leaked permits: active=%d unknown=%d", active, unknown)
	}
	if seats := gateway.admission.sessionSeats(); seats != 0 {
		test.Fatalf("settlement retry leaked %d session seats", seats)
	}

	// The retried record must be as durable as one written with room to spare: a
	// restarted process has to recover both ends of the session and find the open
	// discharged, not isolated.
	directory := filepath.Dir(durable.disk.path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gateway.Close(ctx); err != nil {
		test.Fatalf("gateway did not close cleanly: %v", err)
	}
	if err := durable.Close(); err != nil {
		test.Fatalf("store close: %v", err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	openedRecord, closedRecord := false, false
	for _, record := range state.Operations {
		if record.Database.Key() != database.Key() || record.Source != "gateway" {
			continue
		}
		switch record.Action {
		case "open":
			openedRecord = true
			if record.State != "terminal" || record.CleanupOutcome != "completed" {
				test.Fatalf("recovered open is %s/%s, want terminal/completed", record.State, record.CleanupOutcome)
			}
		case "close":
			closedRecord = true
			if record.State != "terminal" || record.CleanupOutcome != "completed" {
				test.Fatalf("recovered close is %s/%s, want terminal/completed", record.State, record.CleanupOutcome)
			}
		}
	}
	if !openedRecord || !closedRecord {
		test.Fatalf("recovered records: open=%t close=%t", openedRecord, closedRecord)
	}
	status := gateway.settle.Status()
	if status["retry_success_total"].(uint64) < 1 {
		test.Fatalf("retry success was not counted: %v", status)
	}
}

// The backoff runs outside every lock: a refused attempt releases the namespace
// fence and the operation state lock before the retry sleeps, so a retry can never
// deadlock against the committer or a completion drainer holding a batch.
func TestSettlementBackoffHoldsNoLock(test *testing.T) {
	fixture := newGatewayFixture(test)
	gateway := fixture.gateway
	database := fixture.first
	var attempts atomic.Uint64
	checked := make(chan error, 1)
	gateway.settle.testRetry = func() {
		// Both acquisitions must succeed while the retry is backing off. If either
		// lock were still held, the snippet would block and the test would time out.
		done := make(chan error, 1)
		go func() {
			unlock := lockOperationNamespace(database.Namespace)
			unlock()
			_, err := gateway.config.State.Snapshot()
			done <- err
		}()
		select {
		case err := <-done:
			select {
			case checked <- err:
			default:
			}
		case <-time.After(2 * time.Second):
		}
	}
	test.Cleanup(func() { gateway.settle.testRetry = nil })

	submit := func(func(*operationEdit) error) (func() error, error) {
		if attempts.Add(1) < 3 {
			return nil, ErrOperationQueueFull
		}
		return func() error { return nil }, nil
	}
	result := gatewayResult{dispatched: true}
	if err := gateway.settleOperationUsingCaller(context.Background(), database, "op-lock", "tool_rpc", &result, nil, submit, nil); err != nil {
		test.Fatalf("a retried settlement failed: %v", err)
	}
	if attempts.Load() != 3 {
		test.Fatalf("submit attempts = %d, want 3", attempts.Load())
	}
	if result.code != "" {
		test.Fatalf("a retried settlement rewrote the result to %q", result.code)
	}
	select {
	case err := <-checked:
		if err != nil {
			test.Fatalf("lock check failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		test.Fatal("the retry never backed off outside the locks")
	}
	status := gateway.settle.Status()
	if status["retry_total"].(uint64) != 2 || status["retry_success_total"].(uint64) != 1 {
		test.Fatalf("retry account = %v", status)
	}
	published, ok := gateway.Metrics()["operation_settle"].(map[string]any)
	if !ok || published["retry_total"].(uint64) != 2 {
		test.Fatalf("/v1/metrics does not publish the settlement account: %v", gateway.Metrics()["operation_settle"])
	}
	test.Logf("operation_settle: %v", published)
}

// A queue that never drains is still an unknown outcome, as it was before the
// retry existed, but it is now bounded, counted and logged instead of silent.
func TestSettlementQueueRefusalExhaustionStaysUnknown(test *testing.T) {
	fixture := newGatewayFixture(test)
	database := fixture.first
	attempts := 0
	submit := func(func(*operationEdit) error) (func() error, error) {
		attempts++
		return nil, ErrOperationQueueFull
	}
	result := gatewayResult{dispatched: true}
	started := time.Now()
	err := fixture.gateway.settleOperationUsingCaller(context.Background(), database, "op-exhaust", "tool_rpc", &result, nil, submit, nil)
	elapsed := time.Since(started)
	if !errors.Is(err, ErrOperationQueueFull) {
		test.Fatalf("exhausted settlement reported %v, want a queue refusal", err)
	}
	if result.code != "outcome_unknown" {
		test.Fatalf("exhausted settlement answered %q, want outcome_unknown", result.code)
	}
	if attempts < 2 {
		test.Fatalf("submit attempts = %d, want a bounded retry", attempts)
	}
	if elapsed < operationSettleRetryBudget*9/10 || elapsed > 2*time.Second {
		test.Fatalf("settlement spent %v retrying, want about %v", elapsed, operationSettleRetryBudget)
	}
	status := fixture.gateway.settle.Status()
	if status["exhausted_total"].(uint64) != 1 {
		test.Fatalf("exhaustion was not counted: %v", status)
	}
	if refused := status["refused_total"].(map[string]uint64); refused["queue_full"] != 1 {
		test.Fatalf("terminal queue refusal = %v, want one queue_full", refused)
	}
}

// A refusal that is not a local queue refusal must not be retried: a closed,
// frozen or poisoned store is a terminal answer.
func TestSettlementDoesNotRetryATerminalRefusal(test *testing.T) {
	fixture := newGatewayFixture(test)
	database := fixture.first
	attempts := 0
	submit := func(func(*operationEdit) error) (func() error, error) {
		attempts++
		return nil, ErrStateClosed
	}
	result := gatewayResult{dispatched: true}
	started := time.Now()
	err := fixture.gateway.settleOperationUsingCaller(context.Background(), database, "op-closed", "tool_rpc", &result, nil, submit, nil)
	if !errors.Is(err, ErrStateClosed) {
		test.Fatalf("terminal refusal reported %v", err)
	}
	if attempts != 1 {
		test.Fatalf("a terminal refusal was retried %d times", attempts)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		test.Fatalf("a terminal refusal waited %v", elapsed)
	}
	status := fixture.gateway.settle.Status()
	if status["retry_total"].(uint64) != 0 {
		test.Fatalf("a terminal refusal was counted as a retry: %v", status)
	}
	if refused := status["refused_total"].(map[string]uint64); refused["state_closed"] != 1 {
		test.Fatalf("terminal refusal = %v, want one state_closed", refused)
	}
}

// A cancelled request stops retrying immediately: the budget belongs to the
// request that asked for the settlement.
func TestSettlementRetryStopsOnContextCancellation(test *testing.T) {
	fixture := newGatewayFixture(test)
	database := fixture.first
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	submit := func(func(*operationEdit) error) (func() error, error) {
		attempts++
		return nil, ErrOperationQueueFull
	}
	result := gatewayResult{dispatched: true}
	started := time.Now()
	err := fixture.gateway.settleOperationUsingCaller(ctx, database, "op-cancel", "tool_rpc", &result, nil, submit, nil)
	if !errors.Is(err, ErrOperationQueueFull) {
		test.Fatalf("cancelled settlement reported %v", err)
	}
	if attempts != 1 {
		test.Fatalf("cancelled settlement made %d attempts, want the initial one only", attempts)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		test.Fatalf("cancelled settlement waited %v", elapsed)
	}
	if status := fixture.gateway.settle.Status(); status["exhausted_total"].(uint64) != 1 {
		test.Fatalf("cancellation was not counted: %v", status)
	}
}
