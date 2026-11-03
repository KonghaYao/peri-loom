package platform

import (
	"encoding/json"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
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
