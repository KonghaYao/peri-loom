package platform

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type gatewayCorrelationBody struct {
	reads int
}

func (body *gatewayCorrelationBody) Read([]byte) (int, error) {
	body.reads++
	return 0, io.EOF
}

func (*gatewayCorrelationBody) Close() error { return nil }

func gatewayCorrelationID(test *testing.T, response *httptest.ResponseRecorder) string {
	test.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		test.Fatal(err)
	}
	var identifier string
	if err := json.Unmarshal(envelope["request_id"], &identifier); err != nil {
		test.Fatal(err)
	}
	if string(envelope["ok"]) == "false" {
		var failure map[string]json.RawMessage
		if err := json.Unmarshal(envelope["error"], &failure); err != nil {
			test.Fatal(err)
		}
		if _, found := failure["execution_completed"]; found {
			test.Fatal("pre-dispatch denial claimed execution completion")
		}
		if string(envelope["precondition_failed"]) == "true" {
			test.Fatal("gateway denial claimed authority precondition failure")
		}
	}
	return identifier
}

func TestGatewayCorrelationEarlyQuotaWithoutBodyRead(test *testing.T) {
	fixture := newGatewayFixture(test)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSessions = 1 })
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	body := &gatewayCorrelationBody{}
	request := httptest.NewRequest("POST", "/v1/sql/open", body)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("X-Request-ID", "quota-full-42")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 429 || gatewayCode(test, response) != "quota_exceeded" || gatewayCorrelationID(test, response) != "quota-full-42" {
		test.Fatalf("early quota correlation lost: %s", response.Body.String())
	}
	if body.reads != 0 || fixture.upstream.count("open_session") != 1 {
		test.Fatal("early quota read body or dispatched upstream")
	}
}

func TestGatewayCorrelationUnauthenticatedWithoutBodyRead(test *testing.T) {
	fixture := newGatewayFixture(test)
	body := &gatewayCorrelationBody{}
	request := httptest.NewRequest("POST", "/v1/sql/query", body)
	request.Header.Set("X-Request-ID", "unauthenticated-42")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 401 || gatewayCode(test, response) != "unauthenticated" || gatewayCorrelationID(test, response) != "unauthenticated-42" || body.reads != 0 {
		test.Fatalf("unauthenticated correlation lost: %s", response.Body.String())
	}
	if len(fixture.upstream.calls) != 0 {
		test.Fatal("correlation header admitted unauthenticated request")
	}
}

func TestGatewayCorrelationConcurrentQuotaWithoutBodyRead(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 1 })
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	fixture.upstream.setHook(func(ctx context.Context, worker controller.Worker, command map[string]any) (any, error) {
		if command["command"] != "query" {
			return nil, nil
		}
		close(entered)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		finished <- gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "in-flight"})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		test.Fatal("first SQL did not occupy concurrent permit")
	}
	body := &gatewayCorrelationBody{}
	request := httptest.NewRequest("POST", "/v1/sql/query", body)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("X-Request-ID", "concurrent-quota-42")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 429 || gatewayCode(test, response) != "quota_exceeded" || gatewayCorrelationID(test, response) != "concurrent-quota-42" || body.reads != 0 || fixture.upstream.count("query") != 1 {
		test.Fatalf("concurrent quota correlation failed: %s", response.Body.String())
	}
	release <- struct{}{}
	select {
	case response = <-finished:
		if response.Code != 200 {
			test.Fatal("existing admitted SQL was affected by quota denial")
		}
	case <-time.After(time.Second):
		test.Fatal("existing SQL did not finish")
	}
	if fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("completed SQL retained a permit")
	}
}

func TestGatewayCorrelationInvalidHeadersFailClosed(test *testing.T) {
	cases := map[string][]string{
		"duplicate":   {"same-id", "same-id"},
		"conflicting": {"first-id", "second-id"},
		"empty":       {""},
		"space":       {"bad id"},
		"comma":       {"first,second"},
		"control":     {"bad\r\nid"},
		"unicode":     {"请求"},
		"too-long":    {strings.Repeat("a", 129)},
	}
	for name, values := range cases {
		test.Run(name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			body := &gatewayCorrelationBody{}
			request := httptest.NewRequest("POST", "/v1/sql/query", body)
			request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
			request.Header["X-Request-Id"] = values
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			identifier := gatewayCorrelationID(test, response)
			if response.Code != 400 || gatewayCode(test, response) != "invalid_request" || !publicSessionID(identifier) || body.reads != 0 || len(fixture.upstream.calls) != 0 {
				test.Fatalf("malformed correlation admitted: %s", response.Body.String())
			}
			for _, value := range values {
				if identifier == value {
					test.Fatal("invalid correlation reflected")
				}
			}
		})
	}
}

func TestGatewayCorrelationBodyMismatchDoesNotDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	encoded, _ := json.Marshal(map[string]any{"session": session, "sql": "SELECT 1", "request_id": "body-id"})
	request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("X-Request-ID", "header-id")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 400 || gatewayCode(test, response) != "invalid_request" || gatewayCorrelationID(test, response) != "header-id" || fixture.upstream.count("query") != 0 {
		test.Fatalf("mismatched correlation dispatched: %s", response.Body.String())
	}
	if fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("mismatch leaked admission permit")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 2", "request_id": "new-business-op"})
	if response.Code != 200 || fixture.upstream.count("query") != 1 {
		test.Fatal("pre-dispatch mismatch poisoned session")
	}
}

func TestGatewayCorrelationSuccessCompatibility(test *testing.T) {
	for _, mode := range []string{"matching", "header-only", "body-only", "header-limit"} {
		test.Run(mode, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			identifier := "business-operation-42"
			if mode == "header-limit" {
				identifier = strings.Repeat("a", 128)
			}
			body := map[string]any{"session": session, "sql": "SELECT 1"}
			if mode != "header-only" {
				body["request_id"] = identifier
			}
			encoded, _ := json.Marshal(body)
			request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(string(encoded)))
			request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
			if mode != "body-only" {
				request.Header.Set("X-Request-ID", identifier)
			}
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			if response.Code != 200 || gatewayCorrelationID(test, response) != identifier || fixture.upstream.count("query") != 1 {
				test.Fatalf("success correlation lost: %s", response.Body.String())
			}
			fixture.upstream.mu.Lock()
			call := fixture.upstream.calls[len(fixture.upstream.calls)-1]
			fixture.upstream.mu.Unlock()
			if call.requestID != identifier || fixture.gateway.Metrics()["active_permits"] != 0 {
				test.Fatal("request ID not forwarded once or permit leaked")
			}
		})
	}
}

func TestGatewayCorrelationBodyOnlyEarlyDenialUsesFallback(test *testing.T) {
	fixture := newGatewayFixture(test)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSessions = 1 })
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	identifiers := make(map[string]bool)
	for attempt := 0; attempt < 2; attempt++ {
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first, "request_id": "unread-body-id"})
		identifier := gatewayCorrelationID(test, response)
		if response.Code != 429 || !publicSessionID(identifier) || identifier == "unread-body-id" || identifiers[identifier] {
			test.Fatal("headerless early rejection did not use fresh safe fallback")
		}
		identifiers[identifier] = true
	}
	if fixture.upstream.count("open_session") != 1 {
		test.Fatal("early rejection dispatched upstream")
	}
}
