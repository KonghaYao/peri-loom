package platform

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// sameNamespaceBranch registers a second branch of the fixture's first database.
// Isolation is namespace-wide, so this branch is the sibling whose cleanup has
// to keep working while the isolated branch refuses every new statement.
func sameNamespaceBranch(fixture *gatewayFixture, test *testing.T) controller.DatabaseID {
	test.Helper()
	sibling := controller.DatabaseID{Namespace: fixture.first.Namespace, Branch: "feature"}
	if err := fixture.state.Update(func(state *State) error {
		state.Databases[sibling.Key()] = Database{ID: sibling, Project: "project-a", State: "active", Encrypted: true, RootIncarnation: "root-a", BranchIncarnation: 1}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		worker := metadata.Workers["worker-tenant-a"]
		binding := controller.Binding{Database: sibling, RootIncarnation: "root-a", AuthorityTerm: 1, BranchIncarnation: 1, Epoch: 1, WorkerID: worker.ID, LeaseID: "lease-1", GrantID: "grant-1", Role: "writer"}
		metadata.Placements[sibling.Key()] = controller.Placement{Database: sibling, DesiredWorker: worker.ID, Epoch: 1, LeaseID: "lease-1", Role: "writer", DesiredState: "active", Phase: "active", Grant: &controller.Grant{Binding: binding, RemainingTTLMS: 10000}}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	return sibling
}

// idleSession ages one session past the expiry deadline the sweeper uses.
func idleSession(fixture *gatewayFixture, identifier string, test *testing.T) *gatewaySession {
	test.Helper()
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	if session == nil {
		test.Fatalf("session %s is not registered", identifier)
	}
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	return session
}

// expireRetryWindow clears the account's next attempt so a test can drive the
// bounded retry without waiting for the backoff it produces.
func expireRetryWindow(session *gatewaySession) {
	session.mu.Lock()
	session.cleanup.nextAttempt = time.Time{}
	session.mu.Unlock()
}

// sweepRetirements drives the expiry sweep the way its own ticker does: the
// session-close lane admits one close of a project at a time, so a second due
// session retires on a later tick instead of in the same pass.
func sweepRetirements(fixture *gatewayFixture, want int, test *testing.T) {
	test.Helper()
	for range 8 {
		_ = fixture.gateway.expireNativeSessions(context.Background(), time.Minute)
		if fixture.upstream.count("close_session") >= want {
			return
		}
	}
	test.Fatalf("expiry retired %d of %d due sessions", fixture.upstream.count("close_session"), want)
}

func projectSessions(fixture *gatewayFixture, project string) int {
	projects, _, _ := fixture.gateway.admission.status()
	admitted, _ := projects[project].(map[string]any)
	count, _ := admitted["sessions"].(int)
	return count
}

// Isolation refuses new SQL on the namespace, and it must not refuse the close
// of a session that is already registered: that close is the platform's own
// retirement, it carries no SQL, and it is the only path that returns the
// session and its quota to an accounted end.
func TestGatewayCleanupProgressesInsideIsolation(test *testing.T) {
	fixture := newGatewayFixture(test)
	sibling := sameNamespaceBranch(fixture, test)
	isolated := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	retirable := gatewayOpen(test, fixture, fixture.tokenA, sibling)
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	test.Cleanup(func() { log.SetOutput(previous) })
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "execute" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": isolated, "sql": "COMMIT", "request_id": "cleanup_unknown"})
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
		test.Fatalf("unproven execute was not quarantined: %d %s", response.Code, response.Body.String())
	}
	// The transition is what the process log has to keep: the 502 lives for one
	// request, and the refusals below are the same isolation seen again.
	if line := logged.String(); !strings.Contains(line, "SQL outcome unknown") || !strings.Contains(line, "request_id=cleanup_unknown") ||
		!strings.Contains(line, "stage=tool_rpc") || !strings.Contains(line, "session="+isolated) || !strings.Contains(line, "database="+fixture.first.Key()) ||
		!strings.Contains(line, "rpc=ambiguous=true,rejected=false,execution_completed=false") || !strings.Contains(line, "observation=none") {
		test.Fatalf("an unknown outcome left no usable log line: %q", line)
	}
	// The isolation now blocks every new statement of the namespace, including
	// the sibling branch's, and every new session.
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": retirable, "sql": "SELECT 1"})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("isolated namespace accepted a new statement: %d %s", response.Code, response.Body.String())
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": retirable, "sql": "INSERT INTO records VALUES (1)"})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("isolated namespace accepted a new write: %d %s", response.Code, response.Body.String())
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": sibling})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("isolated namespace accepted a new session: %d %s", response.Code, response.Body.String())
	}
	// Those refusals keep an audit row each, and none of them is the transition:
	// a retrying client must not be able to grow the process log.
	if entries := strings.Count(logged.String(), "SQL outcome unknown"); entries != 1 {
		test.Fatalf("the isolation's own refusals were logged as new unknowns: %d", entries)
	}
	if sessions := projectSessions(fixture, "project-a"); sessions != 2 {
		test.Fatalf("isolation released or duplicated a session seat: %d", sessions)
	}
	// Both registered sessions are still retirable while the namespace is
	// isolated: the poisoned one because its owner can no longer address it, and
	// the sibling because isolation never blocked the platform's own cleanup.
	isolatedSession, retirableSession := idleSession(fixture, isolated, test), idleSession(fixture, retirable, test)
	sweepRetirements(fixture, 2, test)
	if fixture.upstream.count("close_session") != 2 {
		test.Fatalf("isolation blocked a registered session's cleanup: %d closes", fixture.upstream.count("close_session"))
	}
	fixture.gateway.mu.Lock()
	registered := len(fixture.gateway.sessions)
	fixture.gateway.mu.Unlock()
	if registered != 0 {
		test.Fatalf("retired sessions stayed registered: %d", registered)
	}
	isolatedSession.mu.Lock()
	closed, poisoned := isolatedSession.closed, isolatedSession.poisoned
	isolatedSession.mu.Unlock()
	retirableSession.mu.Lock()
	retirableClosed := retirableSession.closed
	retirableSession.mu.Unlock()
	if !closed || !retirableClosed || !poisoned {
		test.Fatal("retirement did not close the sessions it owned")
	}
	if sessions := projectSessions(fixture, "project-a"); sessions != 0 {
		test.Fatalf("retirement did not return every session seat: %d", sessions)
	}
	// The unknown itself is untouched: the record still isolates the namespace
	// and the retained permit is still held, so a proven close cannot be used to
	// end an isolation that needs the worker-stop proof.
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if record, blocked := isolatedOperation(state, fixture.first, true); !blocked || record.State != "quarantined" || !record.MayHaveCommitted {
		test.Fatalf("retirement rewrote the quarantined record: %+v", record)
	}
	if held := fixture.gateway.Metrics()["unknown_held_permits"]; held != 1 {
		test.Fatalf("retirement released the retained unknown permit: %v", held)
	}
	// The seat is returned once: nothing is registered anymore, so a second
	// sweep cannot release it again.
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil {
		test.Fatal(err)
	}
	if fixture.upstream.count("close_session") != 2 {
		test.Fatalf("retirement replayed a close after the session was gone: %d", fixture.upstream.count("close_session"))
	}
	if sessions := projectSessions(fixture, "project-a"); sessions != 0 {
		test.Fatalf("a second sweep released the session seat twice: %d", sessions)
	}
	// Retiring the sessions does not end the isolation: the namespace still
	// refuses new work until an operator proves the isolated worker stopped.
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": sibling})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("cleanup ended the isolation without a proof: %d %s", response.Code, response.Body.String())
	}
}

// An unproven open has no session to name, and the isolation it creates still
// has to be attributable: the log line carries the database the operation
// belongs to instead of a session that never existed.
func TestUnprovenOpenNamesTheDatabaseItIsolated(test *testing.T) {
	fixture := newGatewayFixture(test)
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	test.Cleanup(func() { log.SetOutput(previous) })
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "open_session" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first, "request_id": "open_unknown"})
	if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
		test.Fatalf("unproven open was not quarantined: %d %s", response.Code, response.Body.String())
	}
	line := logged.String()
	if !strings.Contains(line, "request_id=open_unknown") || !strings.Contains(line, "stage=open_session") ||
		!strings.Contains(line, "database="+fixture.first.Key()) || !strings.Contains(line, "session= worker=") {
		test.Fatalf("an unproven open left no attributable log line: %q", line)
	}
	// The open did reach the worker, so the line keeps the verdict it gave even
	// though there is no session to attribute it to and no failure text to quote.
	if !strings.Contains(line, "rpc=ambiguous=true,rejected=false,execution_completed=false observation=none") {
		test.Fatalf("an unproven open lost the worker's verdict: %q", line)
	}
}

// The worker's verdict is what separates "the statement never entered execution"
// from "it may have committed", and its text is the only account of why the reply
// could not be used. Both belong on the line that outlives the request, including
// when the worker sent no text at all.
func TestUnknownOutcomeLogKeepsTheWorkersVerdict(test *testing.T) {
	cases := []struct {
		name        string
		failure     *SQLRPCError
		wantVerdict string
		wantObserve string
	}{
		{
			name:        "ambiguous reply with worker text",
			failure:     &SQLRPCError{Ambiguous: true, workerFailure: "near \"SELEC\": syntax error"},
			wantVerdict: "ambiguous=true,rejected=false,execution_completed=false",
			wantObserve: "near \"SELEC\": syntax error",
		},
		{
			name:        "bare ambiguous reply",
			failure:     &SQLRPCError{Ambiguous: true},
			wantVerdict: "ambiguous=true,rejected=false,execution_completed=false",
			wantObserve: "none",
		},
		{
			name:        "ambiguous reply beside a transport cause",
			failure:     &SQLRPCError{Ambiguous: true, cause: context.DeadlineExceeded},
			wantVerdict: "ambiguous=true,rejected=false,execution_completed=false",
			wantObserve: "context deadline exceeded",
		},
	}
	for _, testCase := range cases {
		test.Run(testCase.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			var logged bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logged)
			test.Cleanup(func() { log.SetOutput(previous) })
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "execute" {
					return nil, testCase.failure
				}
				return nil, nil
			})
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "SELEC 1"})
			if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
				test.Fatalf("unproven execute was not quarantined: %d %s", response.Code, response.Body.String())
			}
			line := logged.String()
			if !strings.Contains(line, "rpc="+testCase.wantVerdict) || !strings.Contains(line, "observation="+testCase.wantObserve) {
				test.Fatalf("the unknown line lost the worker's verdict: %q", line)
			}
		})
	}
}

// A close that is not proven is retried, because the command is the platform's
// own retirement and replays no SQL. The retry is bounded: it waits for its
// window, stops at the account's deadline, and never replays the SQL whose
// outcome is unknown. Every failed attempt is also logged, because the cleanup
// ledger is a metrics surface an operator may never scrape.
func TestNativeExpiryRetriesAmbiguousCloseWithinBoundedAccount(test *testing.T) {
	fixture := newGatewayFixture(test)
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	test.Cleanup(func() { log.SetOutput(previous) })
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	session := idleSession(fixture, identifier, test)
	ambiguous := true
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" && ambiguous {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err == nil {
		test.Fatal("ambiguous close was reported as proven cleanup")
	}
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 1 {
		test.Fatalf("retirement ran inside its own retry window: %v", err)
	}
	ambiguous = false
	expireRetryWindow(session)
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil {
		test.Fatalf("bounded retry did not retire the session: %v", err)
	}
	if fixture.upstream.count("close_session") != 2 {
		test.Fatalf("retirement did not retry the close once: %d", fixture.upstream.count("close_session"))
	}
	if fixture.upstream.count("execute") != 0 || fixture.upstream.count("query") != 0 {
		test.Fatal("retirement replayed SQL instead of closing the session")
	}
	session.mu.Lock()
	attempts := session.cleanup.attempts
	session.mu.Unlock()
	if !session.closed || attempts != 2 {
		test.Fatalf("retry account or session state is inconsistent: closed=%t attempts=%d", session.closed, attempts)
	}
	fixture.gateway.mu.Lock()
	_, registered := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	if registered {
		test.Fatal("proven close left the session registered")
	}
	if held := fixture.gateway.Metrics()["unknown_held_permits"]; held != 1 {
		test.Fatalf("retry released the retained unknown permit: %v", held)
	}
	// An account that is spent stops attempting and says so instead of looping
	// forever; the session stays visible in the cleanup ledger. The isolated
	// namespace above refuses new sessions, so this one lives in the second
	// project the fixture holds.
	stuck := gatewayOpen(test, fixture, fixture.tokenB, fixture.second)
	stuckSession := idleSession(fixture, stuck, test)
	ambiguous = true
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err == nil {
		test.Fatal("ambiguous close was reported as proven cleanup")
	}
	before := fixture.upstream.count("close_session")
	// The attempt that spends the last unit of the account is the transition the
	// operator sees: it is logged with the session identity and the spent account,
	// and after it only the ledger, an operator and a restart can finish the
	// session.
	stuckSession.mu.Lock()
	stuckSession.cleanup.attempts = sessionCleanupMaxAttempts - 1
	stuckSession.mu.Unlock()
	expireRetryWindow(stuckSession)
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err == nil {
		test.Fatal("the account's last attempt was reported as proven cleanup")
	}
	err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute)
	if !errors.Is(err, errSessionCleanupExhausted) {
		test.Fatalf("spent account did not report its bound: %v", err)
	}
	if fixture.upstream.count("close_session") != before+1 {
		test.Fatal("spent account kept dispatching")
	}
	if entries := strings.Count(logged.String(), "failure=outcome_unknown exhausted=false"); entries < 2 {
		test.Fatalf("a failed retirement attempt left no log line: %q", logged.String())
	}
	if entries := strings.Count(logged.String(), "exhausted=true"); entries != 1 ||
		!strings.Contains(logged.String(), "database="+fixture.second.Key()) || !strings.Contains(logged.String(), "session="+stuck) {
		test.Fatalf("the spent account was not reported with its identity: %q", logged.String())
	}
	ledger := fixture.gateway.Metrics()["cleanup_ledger"].(map[string]any)
	pending := ledger["pending_close"].(map[string]any)
	if pending["count"] != 1 || pending["exhausted"] != 1 || pending["last_failure"] != "outcome_unknown" {
		test.Fatalf("cleanup ledger lost the spent account: %v", pending)
	}
}

// The ledger is the exit condition of the lifecycle batch: what is still owed,
// what has no owner, and what keeps a namespace isolated are visible with a
// count and an age, and they decrease only when the underlying fact does.
func TestCleanupLedgerReportsPendingCloseOrphanAndIsolation(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	idleSession(fixture, identifier, test)
	ambiguous := true
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" && ambiguous {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err == nil {
		test.Fatal("ambiguous close was reported as proven cleanup")
	}
	ledger := fixture.gateway.Metrics()["cleanup_ledger"].(map[string]any)
	pending := ledger["pending_close"].(map[string]any)
	orphan := ledger["orphan_sessions"].(map[string]any)
	isolated := ledger["isolated_records"].(map[string]any)
	if pending["count"] != 1 || orphan["count"] != 1 || isolated["count"] != 1 {
		test.Fatalf("cleanup ledger lost a fact: %v", ledger)
	}
	if pending["last_failure"] != "outcome_unknown" || isolated["state_read"] != "ok" {
		test.Fatalf("cleanup ledger lost the reason or the readable state: %v", ledger)
	}
	for name, account := range map[string]map[string]any{"pending_close": pending, "orphan_sessions": orphan, "isolated_records": isolated} {
		if age, ok := account["oldest_age_ms"].(int64); !ok || age < 0 {
			test.Fatalf("cleanup ledger reported an impossible age for %s: %v", name, account)
		}
	}
	// The session retires on its next window; the quarantine stays until an
	// operator proves the worker stopped, and it stays visible while it does.
	ambiguous = false
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	expireRetryWindow(session)
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil {
		test.Fatal(err)
	}
	ledger = fixture.gateway.Metrics()["cleanup_ledger"].(map[string]any)
	pending = ledger["pending_close"].(map[string]any)
	orphan = ledger["orphan_sessions"].(map[string]any)
	isolated = ledger["isolated_records"].(map[string]any)
	if pending["count"] != 0 || orphan["count"] != 0 {
		test.Fatalf("retired session stayed in the cleanup ledger: %v", ledger)
	}
	if isolated["count"] != 1 {
		test.Fatalf("isolation disappeared without a worker-stop proof: %v", ledger)
	}
}
