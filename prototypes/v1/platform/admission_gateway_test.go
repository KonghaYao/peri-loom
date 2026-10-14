package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func gatewayQuota(test *testing.T, fixture *gatewayFixture, project string, update func(*Quotas)) {
	test.Helper()
	if err := fixture.state.Update(func(state *State) error {
		value := state.Projects[project]
		update(&value.Quotas)
		state.Projects[project] = value
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

func TestGatewaySessionQuotaCloseAndRateLimits(test *testing.T) {
	fixture := newGatewayFixture(test)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSessions = 1 })
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != 429 || fixture.upstream.count("open_session") != 1 {
		test.Fatal("session quota not enforced before upstream")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 {
		test.Fatal("healthy close failed")
	}
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.RequestsPerSecond = 1 })
	fixture.gateway.admission.mu.Lock()
	bucket := fixture.gateway.admission.projects["project-a"]
	bucket.tokens = 0
	bucket.last = time.Now()
	fixture.gateway.admission.mu.Unlock()
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 429 || fixture.upstream.count("query") != 0 {
		test.Fatal("rate limit dispatched query")
	}
	if fixture.gateway.Metrics()["errors_total"].(map[string]uint64)["quota_exceeded"] != 2 {
		test.Fatal("429 accounting incorrect")
	}
}

func TestGatewaySQLBytesBeforeDispatchAndBoundedNDJSON(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSQLBytes = 8 })
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": strings.Repeat("x", 9)})
	if gatewayCode(test, response) != "sql_limit" || fixture.upstream.count("query") != 0 {
		test.Fatal("oversized SQL dispatched")
	}
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSQLBytes = 4096 })
	body, _ := json.Marshal(map[string]any{"session": session, "sql": "SELECT 1", "request_id": "stream-1"})
	request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Accept", "application/x-ndjson")
	response = httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 200 || response.Header().Get("Content-Type") != "application/x-ndjson" {
		test.Fatal("NDJSON not negotiated")
	}
	lines := strings.Split(strings.TrimSpace(response.Body.String()), "\n")
	if len(lines) != 3 {
		test.Fatal("wrong NDJSON row count")
	}
	var complete struct {
		Type      string `json:"type"`
		Rows      int    `json:"rows"`
		RequestID string `json:"request_id"`
	}
	if json.Unmarshal([]byte(lines[2]), &complete) != nil || complete.Type != "complete" || complete.Rows != 2 || complete.RequestID != "stream-1" {
		test.Fatal("stream completion correlation invalid")
	}
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 2 })
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "result_limit" || !strings.Contains(response.Body.String(), `"execution_completed":true`) {
		test.Fatal("post-execution result limit pretended no execution")
	}
	if fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("confirmed terminal result response leaked permit")
	}
}

func TestGatewayTwoTenantProgressAndNoUnboundedSessionQueue(test *testing.T) {
	fixture := newGatewayFixture(test)
	first := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	second := gatewayOpen(test, fixture, fixture.tokenB, fixture.second)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 1 })
	entered, release := make(chan struct{}, 1), make(chan struct{})
	fixture.upstream.setHook(func(ctx context.Context, worker controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "query" && worker.ID == "worker-tenant-a" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, &SQLRPCError{Ambiguous: true}
			}
		}
		return nil, nil
	})
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		finished <- gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": first, "sql": "SELECT 1"})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		test.Fatal("first query not dispatched")
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": first, "sql": "SELECT 2"})
	if response.Code != 429 {
		test.Fatal("tenant concurrent bound did not reject immediately")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenB, "/v1/sql/query", map[string]any{"session": second, "sql": "SELECT 3"})
	if response.Code != 200 {
		test.Fatal("tenant A occupied tenant B lane")
	}
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 2 })
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": first, "sql": "SELECT 4"})
	if gatewayCode(test, response) != "session_busy" {
		test.Fatal("same session enqueued concurrent native dispatch")
	}
	close(release)
	select {
	case response = <-finished:
		if response.Code != 200 {
			test.Fatal("first query failed")
		}
	case <-time.After(time.Second):
		test.Fatal("native request not released")
	}
	if fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("successful response did not release permit")
	}
}

func TestGatewayUnknownCommitNoRetryPoisonAndConservativePermit(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 1 })
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "execute" {
			return nil, errors.New("private unknown committed response " + fixture.tokenA)
		}
		return nil, nil
	})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT", "request_id": "commit-1"})
	if gatewayCode(test, response) != "outcome_unknown" || fixture.upstream.count("execute") != 1 || !strings.Contains(response.Body.String(), `"request_id":"commit-1"`) {
		test.Fatal("unknown COMMIT classification or retry incorrect")
	}
	if strings.Contains(response.Body.String(), fixture.tokenA) {
		test.Fatal("unknown response leaked credential")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 429 || fixture.upstream.count("query") != 0 {
		test.Fatal("unknown permit released or native retried")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if gatewayCode(test, response) != "outcome_unknown" || fixture.upstream.count("close_session") != 0 {
		test.Fatal("close falsely claimed unknown mutation canceled")
	}
	_ = gatewayOpen(test, fixture, fixture.tokenB, fixture.second)
	if fixture.gateway.Metrics()["unknown_held_permits"] != 1 {
		test.Fatal("unknown request quota not retained")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if fixture.gateway.Close(ctx) == nil {
		test.Fatal("unknown lane shutdown pretended terminal proof")
	}
	if fixture.gateway.Metrics()["active_permits"] != 1 {
		test.Fatal("shutdown fake-released active permit")
	}
}

func TestGatewayCloseReleasesHealthyHandlesAndRejectsNewOpen(test *testing.T) {
	fixture := newGatewayFixture(test)
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.gateway.Close(ctx); err != nil {
		test.Fatal(err)
	}
	if fixture.upstream.count("close_session") != 1 || fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("shutdown cleanup accounting incorrect")
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if gatewayCode(test, response) != "gateway_closing" || fixture.upstream.count("open_session") != 1 {
		test.Fatal("closed gateway admitted new SQL session")
	}
}
