package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestNativeIdleSessionCleanupProvesCloseBeforeReturningQuota(test *testing.T) {
	fixture := newGatewayFixture(test)
	if err := fixture.state.Update(func(state *State) error {
		project := state.Projects["project-a"]
		project.Quotas.MaxSessions = 1
		state.Projects[project.ID] = project
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	if response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first}); response.Code != 429 {
		test.Fatal("session quota was not occupied before idle cleanup")
	}
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil {
		test.Fatal(err)
	}
	if fixture.upstream.count("close_session") != 1 {
		test.Fatal("idle session close was not joined")
	}
	fixture.gateway.mu.Lock()
	_, remains := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	if remains {
		test.Fatal("proven closed session remained registered")
	}
	_, _, unknown := fixture.gateway.admission.status()
	if unknown != 0 {
		test.Fatal("proven close held an unknown permit")
	}
	if response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first}); response.Code != 200 {
		test.Fatalf("idle cleanup did not return session quota: %d", response.Code)
	}
}

func TestNativeIdleExpiryLoopRunsAndJoinsBeforeShutdown(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-time.Second)
	session.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fixture.gateway.runNativeSessionExpiryWith(ctx, 50*time.Millisecond, 10*time.Millisecond)
	}()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		fixture.gateway.mu.Lock()
		_, remains := fixture.gateway.sessions[identifier]
		fixture.gateway.mu.Unlock()
		if !remains {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			test.Fatal("background native expiry did not close idle session")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		test.Fatalf("expiry loop did not join on cancellation: %v", err)
	}
	if fixture.upstream.count("close_session") != 1 {
		test.Fatal("expiry loop returned without one proven close")
	}
}

func TestNativeSessionActivityRefreshesIdleDeadline(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": identifier, "sql": "SELECT 1"})
	if response.Code != 200 {
		test.Fatalf("session activity failed: %d", response.Code)
	}
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 0 {
		test.Fatalf("active session expired: %v", err)
	}
}

func TestNativeIdleCleanupAfterKeyRevocationWaitsForIdleSessionLock(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	if err := RevokeAPIKey(fixture.state, fixture.keyA.ID); err != nil {
		session.mu.Unlock()
		test.Fatal(err)
	}
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 0 {
		session.mu.Unlock()
		test.Fatal("busy session was closed while its lock was held")
	}
	session.mu.Unlock()
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 1 {
		test.Fatalf("revoked key prevented trusted idle cleanup: %v", err)
	}
}

func TestSlowNativeIdleCloseDoesNotDelayOtherProject(test *testing.T) {
	fixture := newGatewayFixture(test)
	first := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	second := gatewayOpen(test, fixture, fixture.tokenB, fixture.second)
	fixture.gateway.mu.Lock()
	sessions := []*gatewaySession{fixture.gateway.sessions[first], fixture.gateway.sessions[second]}
	fixture.gateway.mu.Unlock()
	for _, session := range sessions {
		session.mu.Lock()
		session.lastUsed = time.Now().Add(-2 * time.Minute)
		session.mu.Unlock()
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.upstream.setHook(func(_ context.Context, worker controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" {
			if worker.ID == "worker-tenant-a" {
				close(entered)
				<-release
			}
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	finished := make(chan error, 1)
	go func() { finished <- fixture.gateway.expireNativeSessions(context.Background(), time.Minute) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		test.Fatal("slow close did not start")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		fixture.gateway.mu.Lock()
		_, remains := fixture.gateway.sessions[second]
		fixture.gateway.mu.Unlock()
		if !remains {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			test.Fatal("healthy project cleanup waited for slow peer")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	if err := <-finished; err != nil || fixture.upstream.count("close_session") != 2 {
		test.Fatalf("parallel cleanup did not join both closes: %v", err)
	}
}

func TestNativeIdleAmbiguousCloseRetainsSessionAndDoesNotReplayImmediately(test *testing.T) {
	fixture := newGatewayFixture(test)
	identifier := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[identifier]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err == nil {
		test.Fatal("ambiguous close was reported as proven cleanup")
	}
	session.mu.Lock()
	poisoned, closed := session.poisoned, session.closed
	session.mu.Unlock()
	if !poisoned || closed {
		test.Fatal("unknown close released or failed to quarantine session")
	}
	// The retirement command is retried, but not replayed inside its own retry
	// window: the account is what keeps an unreachable worker from turning the
	// sweep into a loop, and the retained permit keeps the unknown outcome.
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 1 {
		test.Fatal("retirement command was replayed inside its retry window")
	}
	_, _, unknown := fixture.gateway.admission.status()
	if unknown == 0 {
		test.Fatal("unknown close lost retained admission")
	}
}

func TestHranaAdapterSessionKeepsItsOwnIdleTimer(test *testing.T) {
	fixture := newGatewayFixture(test)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := httptest.NewRequest("POST", "/db", nil)
	reply := handler.call(request, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	var opened struct {
		Session string `json:"session"`
	}
	if reply.status != 200 || reply.code != "" || json.Unmarshal(reply.result, &opened) != nil || !publicSessionID(opened.Session) {
		test.Fatal("Hrana adapter failed to create a marked session")
	}
	fixture.gateway.mu.Lock()
	session := fixture.gateway.sessions[opened.Session]
	fixture.gateway.mu.Unlock()
	session.mu.Lock()
	marked := session.hrana
	session.lastUsed = time.Now().Add(-2 * time.Minute)
	session.mu.Unlock()
	if !marked {
		test.Fatal("Hrana session lost its private origin marker")
	}
	if err := fixture.gateway.expireNativeSessions(context.Background(), time.Minute); err != nil || fixture.upstream.count("close_session") != 0 {
		test.Fatalf("native sweeper closed Hrana-owned session: %v", err)
	}
}
