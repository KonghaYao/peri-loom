package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// gatedStateStore counts the operation commits of a request and can refuse one of
// them. The gateway writes the operation registry through updateOperations when
// the store offers it, so a wrapper sees exactly the durable appends of the
// dispatch path: an intent, then a terminal record.
type gatedStateStore struct {
	inner StateStore
	mutex sync.Mutex
	// commits counts every operation commit this store was asked for.
	commits int
	// fail names the commit to refuse.
	fail int
}

func newGatedStateStore(inner StateStore) *gatedStateStore {
	return &gatedStateStore{inner: inner, fail: -1}
}

func (store *gatedStateStore) Snapshot() (State, error) { return store.inner.Snapshot() }
func (store *gatedStateStore) Update(update func(*State) error) error {
	return store.inner.Update(update)
}
func (*gatedStateStore) Close() error { return nil }

func (store *gatedStateStore) updateOperations(change func(*operationEdit) error) error {
	store.mutex.Lock()
	store.commits++
	commit := store.commits
	failed := store.fail == commit
	store.mutex.Unlock()
	if failed {
		return errors.New("private operation store failure")
	}
	// The wrapped store is a plain StateStore, so this is the generic path of
	// updateOperationRecords, with the same edit, validation and revert semantics.
	return updateOperationRecords(store.inner, change)
}

func (store *gatedStateStore) commitsObserved() int {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.commits
}

func (store *gatedStateStore) refuseCommit(offset int) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.fail = store.commits + offset
}

func operationCommits(test *testing.T, store StateStore) int {
	test.Helper()
	gated, ok := store.(*gatedStateStore)
	if !ok {
		test.Fatal("the fixture store is not the gated one")
	}
	return gated.commitsObserved()
}

// gatedAudit records when the audit row of a request was entered and can hold it,
// so a test sees what the gateway does while that row is not durable yet.
type gatedAudit struct {
	*gatewayTestAudit
	mutex sync.Mutex
	hold  bool
	once  sync.Once
	// entered is closed by the first held row, release lets it continue.
	entered chan struct{}
	release chan struct{}
}

func newGatedAudit() *gatedAudit {
	return &gatedAudit{gatewayTestAudit: &gatewayTestAudit{}, entered: make(chan struct{}), release: make(chan struct{}, 1)}
}

func (audit *gatedAudit) Record(event AuditEvent) error {
	audit.mutex.Lock()
	hold := audit.hold
	audit.mutex.Unlock()
	if hold {
		audit.once.Do(func() { close(audit.entered) })
		<-audit.release
	}
	return audit.gatewayTestAudit.Record(event)
}

func (audit *gatedAudit) holdNextRow() {
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	audit.hold = true
}

func (audit *gatedAudit) unblock() {
	select {
	case audit.release <- struct{}{}:
	default:
	}
}

// outcomesOf returns the outcome of every row recorded for one action, so a test
// reads only the row of the request it is about.
func (audit *gatedAudit) outcomesOf(action string) []string {
	audit.gatewayTestAudit.mu.Lock()
	defer audit.gatewayTestAudit.mu.Unlock()
	var outcomes []string
	for _, event := range audit.gatewayTestAudit.events {
		if event.Action == action {
			outcomes = append(outcomes, event.Outcome)
		}
	}
	return outcomes
}

// replyResult returns the outcome and payload of a reply without its request id,
// so two requests for the same statement can be compared.
func replyResult(test *testing.T, response *httptest.ResponseRecorder) string {
	test.Helper()
	var value struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		test.Fatalf("invalid reply: %s", response.Body.String())
	}
	return fmt.Sprintf("ok=%t result=%s", value.OK, value.Result)
}

func awaitClosed(test *testing.T, channel chan struct{}, what string) {
	test.Helper()
	select {
	case <-channel:
	case <-time.After(5 * time.Second):
		test.Fatalf("timed out waiting for %s", what)
	}
}

func awaitReply(test *testing.T, reply chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	test.Helper()
	select {
	case response := <-reply:
		return response
	case <-time.After(5 * time.Second):
		test.Fatal("the request never answered")
		return nil
	}
}

// An audit row that is not durable yet must not be answered around: the reply
// waits for it, and once it is released the request answers normally.
func TestReadReplyWaitsForTheAuditBarrier(test *testing.T) {
	fixture := newGatewayFixture(test)
	audit := newGatedAudit()
	fixture.gateway.config.Audit = audit
	test.Cleanup(audit.unblock)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	audit.holdNextRow()

	reply := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reply <- gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	}()
	awaitClosed(test, audit.entered, "the audit row of a read")
	select {
	case response := <-reply:
		test.Fatalf("the reply was written while its audit row was not durable: %s", response.Body.String())
	default:
	}
	audit.unblock()
	response := awaitReply(test, reply)
	if response.Code != 200 {
		test.Fatalf("the read did not answer 200 once its row was durable: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 1 {
		test.Fatal("the audit barrier changed how often the statement reached the worker")
	}
}

// A terminal record that cannot be written still degrades the reply to
// outcome_unknown, and the audit row of that request reports that same degraded
// outcome: the row is taken after the terminal record settles.
func TestReadReplyKeepsOutcomeUnknownWhenTheTerminalCommitFails(test *testing.T) {
	fixture := newGatewayFixture(test)
	audit := newGatedAudit()
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State, fixture.gateway.config.Audit = store, audit
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	store.refuseCommit(2)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
		test.Fatalf("an unconfirmed terminal record was answered around: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 1 {
		test.Fatal("the failed terminal record changed what reached the worker")
	}
	if outcomes := audit.outcomesOf("query"); len(outcomes) != 1 || outcomes[0] != "outcome_unknown" {
		test.Fatalf("the audit row of the refused terminal record changed: %v", outcomes)
	}
}

// An audit row that cannot be written still degrades the reply, even when the
// terminal record it follows settled cleanly.
func TestReadReplyKeepsAuditUnavailableWhenTheRowFails(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.audit.failAction = "query"
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" {
		test.Fatalf("an unwritable audit row was answered around: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 1 {
		test.Fatal("the audit failure changed what reached the worker")
	}
}

// readonlyDescribeHook answers a describe with a fixed verdict and counts the
// describes the gateway asked for.
type readonlyDescribeHook struct {
	mutex     sync.Mutex
	describes int
	verdict   func(sql string) (any, error)
}

func (hook *readonlyDescribeHook) install(fixture *gatewayFixture) *readonlyDescribeHook {
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			hook.mutex.Lock()
			hook.describes++
			hook.mutex.Unlock()
			sql, _ := command["sql"].(string)
			return hook.verdict(sql)
		case "query":
			return [][]any{{1, "one"}}, nil
		case "statement":
			return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 1, "last_insert_rowid": nil, "autocommit": true}, nil
		}
		return nil, nil
	})
	return hook
}

func (hook *readonlyDescribeHook) count() int {
	hook.mutex.Lock()
	defer hook.mutex.Unlock()
	return hook.describes
}

func readonlyVerdict(readonly bool) func(string) (any, error) {
	return func(string) (any, error) {
		return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": readonly}, nil
	}
}

// A statement the worker prepared as read-only keeps no durable intent: the
// request pays one operation commit, and the statement still reaches the worker.
func TestReadOnlyStatementKeepsNoIntent(test *testing.T) {
	fixture := newGatewayFixture(test)
	hook := (&readonlyDescribeHook{verdict: readonlyVerdict(true)}).install(fixture)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	opened := operationCommits(test, store)

	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM records", "request_id": "readonly_query"})
	if response.Code != 200 {
		test.Fatalf("read-only query failed: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 1 {
		test.Fatal("the proven read-only statement did not reach the worker")
	}
	if hook.count() != 1 {
		test.Fatalf("the classification did not use exactly one worker describe: %d", hook.count())
	}
	if commits := operationCommits(test, store) - opened; commits != 1 {
		test.Fatalf("a proven read-only statement still wrote an intent: %d commits", commits)
	}
	record := operationByRequestID(test, fixture.state, "readonly_query")
	if record.State != "terminal" || record.BusinessOutcome != "completed" || !record.ExecutionProven {
		test.Fatalf("the proven read-only statement kept no settled terminal record: %+v", record.OperationStatus)
	}
	if record.Digest != commandDigest(transmittedCommand(test, fixture.upstream, "query")) {
		test.Fatal("the terminal record does not cover the transmitted statement")
	}
}

// Every way the classification can fail keeps the committing path, including the
// extra commit that path costs, and none of those failures is remembered as a
// verdict.
func TestUnprovableStatementKeepsItsIntent(test *testing.T) {
	scenarios := []struct {
		name    string
		verdict func(string) (any, error)
	}{
		{"describe-refused", func(string) (any, error) { return nil, errors.New("private describe refusal") }},
		{"describe-malformed", func(string) (any, error) { return map[string]any{"is_readonly": true}, nil }},
		{"describe-flag-absent", func(string) (any, error) {
			return map[string]any{"params": []any{}, "cols": []any{}, "is_explain": false}, nil
		}},
		{"describe-flag-not-a-flag", func(string) (any, error) {
			return map[string]any{"params": []any{}, "cols": []any{}, "is_explain": false, "is_readonly": "true"}, nil
		}},
		{"describe-column-shape-unknown", func(string) (any, error) {
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id"}}, "is_explain": false, "is_readonly": true}, nil
		}},
		{"describe-sql-mismatch", func(sql string) (any, error) {
			if sql == "SELECT id FROM records" {
				return nil, errors.New("private describe refusal for that text")
			}
			return readonlyVerdict(true)(sql)
		}},
	}
	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			hook := (&readonlyDescribeHook{verdict: scenario.verdict}).install(fixture)
			store := newGatedStateStore(fixture.state)
			fixture.gateway.config.State = store
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			opened := operationCommits(test, store)

			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM records", "request_id": "unprovable_query"})
			if response.Code != 200 {
				test.Fatalf("an unprovable statement did not keep its normal reply: %d %s", response.Code, response.Body.String())
			}
			if fixture.upstream.count("query") != 1 {
				test.Fatal("the statement did not reach the worker")
			}
			if commits := operationCommits(test, store) - opened; commits != 2 {
				test.Fatalf("the committing path did not write an intent and a terminal record: %d commits", commits)
			}
			// A classification that could not be obtained is not a verdict: the next
			// statement asks the worker again instead of trusting one.
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM records", "request_id": "unprovable_query_again"})
			if response.Code != 200 || hook.count() != 2 {
				test.Fatalf("a failed classification was cached: describes=%d status=%d", hook.count(), response.Code)
			}
		})
	}
}

// A write statement is classified once per text and keeps the committing path
// even when the verdict is remembered.
func TestWriterStatementKeepsItsIntent(test *testing.T) {
	fixture := newGatewayFixture(test)
	hook := (&readonlyDescribeHook{verdict: readonlyVerdict(false)}).install(fixture)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	opened := operationCommits(test, store)

	for index := 0; index < 2; index++ {
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(?)", "args": []any{map[string]any{"type": "integer", "value": "1"}}, "named_args": []any{}})
		if response.Code != 200 {
			test.Fatalf("write statement failed: %d %s", response.Code, response.Body.String())
		}
	}
	if commits := operationCommits(test, store) - opened; commits != 4 {
		test.Fatalf("a written statement lost its intent: %d commits for two writes", commits)
	}
	if hook.count() != 1 {
		test.Fatalf("a remembered verdict was not reused for a write: %d describes", hook.count())
	}
	if fixture.upstream.count("statement") != 2 {
		test.Fatal("the write statements did not reach the worker")
	}
}

// A read-only verdict is never remembered: the second request for the same text
// asks the worker again and skips its intent again, so the reply, the worker
// traffic and the durable record of the two requests stay identical.
func TestReadOnlyVerdictIsNotRemembered(test *testing.T) {
	fixture := newGatewayFixture(test)
	hook := (&readonlyDescribeHook{verdict: readonlyVerdict(true)}).install(fixture)
	store := newGatedStateStore(fixture.state)
	fixture.gateway.config.State = store
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	opened := operationCommits(test, store)

	first := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM records", "request_id": "cached_first"})
	second := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM records", "request_id": "cached_second"})
	if first.Code != 200 || second.Code != 200 || replyResult(test, first) != replyResult(test, second) {
		test.Fatalf("the second request answered differently: %s vs %s", first.Body.String(), second.Body.String())
	}
	if hook.count() != 2 {
		test.Fatalf("the read-only verdict was remembered instead of re-proven: %d describes", hook.count())
	}
	if commits := operationCommits(test, store) - opened; commits != 2 {
		test.Fatalf("a re-proven read-only statement wrote an intent: %d commits for two reads", commits)
	}
	firstRecord, secondRecord := operationByRequestID(test, fixture.state, "cached_first"), operationByRequestID(test, fixture.state, "cached_second")
	firstRecord.OperationID, firstRecord.RequestID = "", ""
	secondRecord.OperationID, secondRecord.RequestID = "", ""
	// RegistrationSequence is the order of the append, not the record's content.
	firstRecord.RegistrationSequence, secondRecord.RegistrationSequence = 0, 0
	if firstRecord != secondRecord {
		test.Fatalf("the re-proven verdict wrote a different record: %+v vs %+v", firstRecord, secondRecord)
	}
}

// The committing cache is bounded by two generations, remembers every entry it
// was asked to, and a dropped entry only costs one describe again.
func TestCommittingStatementCacheIsBounded(test *testing.T) {
	cache := &committingStatementCache{}
	total := committingStatementCapacity*3 + 7
	for index := 0; index < total; index++ {
		cache.remember(committingStatementKey{worker: "worker", digest: fmt.Sprintf("statement-%d", index)})
	}
	entries := len(cache.current) + len(cache.previous)
	if entries > 2*committingStatementCapacity {
		test.Fatalf("the cache grew past two generations: %d entries", entries)
	}
	if entries < committingStatementCapacity {
		test.Fatalf("the cache dropped a generation too early: %d entries", entries)
	}
	if cache.remembered(committingStatementKey{worker: "worker", digest: "statement-0"}) {
		test.Fatal("the cache kept an entry from a generation that was already dropped")
	}
	last := committingStatementKey{worker: "worker", digest: fmt.Sprintf("statement-%d", total-1)}
	if !cache.remembered(last) {
		test.Fatal("the cache lost the entry it had just stored")
	}
	if cache.remembered(committingStatementKey{worker: "other-worker", digest: last.digest}) {
		test.Fatal("an entry crossed a worker boundary")
	}
}
