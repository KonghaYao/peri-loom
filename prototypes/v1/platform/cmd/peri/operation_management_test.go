package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestOperationDecisionUsesIndependentRecordID(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if request.URL.Path != "/v1/databases/operations/decision" || json.NewDecoder(request.Body).Decode(&body) != nil || body["request_id"] != "correlation" || body["operation_id"] != "server_record" || body["decision"] != "retain_isolation_new_database_required" {
			test.Error("CLI operation identity or decision lost")
		}
		commandReply(response, "correlation", map[string]any{"operations": []any{}, "recovery": "none"})
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"operations/decision", "--endpoint", server.URL, "--database", "tenant/database/main", "--request-id", "correlation", "--operation-id", "server_record", "--decision", "retain_isolation_new_database_required"}, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
}

// recoveryServer models the durable operation view the recovery steps act on.
// Every POST updates that view, so the CLI's re-read after each step observes
// the same state a real recovery instance would.
type recoveryServer struct {
	mutex     sync.Mutex
	operation string
	calls     []string
	requests  []map[string]any
	decided   bool
	retired   bool
	released  bool
}

func (fixture *recoveryServer) view() []any {
	record := map[string]any{
		"operation_id": fixture.operation, "source": "gateway", "action": "execute",
		"state": "quarantined", "business_outcome": "unknown", "cleanup_outcome": "not_applicable",
		"local_resources_retired": fixture.retired,
	}
	if fixture.decided {
		record["decision"] = "retain_isolation_new_database_required"
	}
	if fixture.released {
		record["release_request_id"] = "release-" + fixture.operation
	}
	return []any{record}
}

func (fixture *recoveryServer) handler(test *testing.T) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			test.Error("recovery request body unavailable")
			return
		}
		fixture.mutex.Lock()
		defer fixture.mutex.Unlock()
		fixture.calls = append(fixture.calls, request.URL.Path)
		fixture.requests = append(fixture.requests, body)
		requestID, _ := body["request_id"].(string)
		switch request.URL.Path {
		case "/v1/databases/operations":
			commandReply(response, requestID, map[string]any{"operations": fixture.view(), "recovery": "isolation_release_or_new_database_required"})
		case "/v1/databases/operations/decision":
			if body["operation_id"] != fixture.operation || body["decision"] != "retain_isolation_new_database_required" {
				test.Error("recovery decision lost its operation identity")
			}
			fixture.decided = true
			commandReply(response, requestID, map[string]any{"operations": fixture.view(), "recovery": "isolation_release_or_new_database_required"})
		case "/v1/databases/operations/retire-local":
			fixture.retired = true
			commandReply(response, requestID, map[string]any{"operations": fixture.view(), "recovery": "isolation_release_or_new_database_required"})
		case "/v1/databases/isolation/release":
			fixture.released = true
			commandReply(response, requestID, map[string]any{"operations": fixture.view(), "recovery": "none"})
		default:
			test.Errorf("unexpected recovery RPC %s", request.URL.Path)
		}
	}
}

func TestRecoverRunsDecisionRetireReleaseWithDeterministicRequestIDs(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	fixture := &recoveryServer{operation: "0123456789abcdef0123456789abcdef"}
	server := httptest.NewServer(fixture.handler(test))
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"recover", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	expected := []string{
		"/v1/databases/operations",
		"/v1/databases/operations/decision",
		"/v1/databases/operations",
		"/v1/databases/operations/retire-local",
		"/v1/databases/operations",
		"/v1/databases/isolation/release",
		"/v1/databases/operations",
	}
	if strings.Join(fixture.calls, ",") != strings.Join(expected, ",") {
		test.Fatalf("recovery order lost: %v", fixture.calls)
	}
	for index, prefix := range map[int]string{1: "decision-", 3: "retire-", 5: "release-"} {
		if fixture.requests[index]["request_id"] != prefix+fixture.operation {
			test.Fatalf("request %d is not deterministic: %v", index, fixture.requests[index]["request_id"])
		}
	}
	if !fixture.decided || !fixture.retired || !fixture.released {
		test.Fatal("recovery did not complete every durable step")
	}
	if lines := strings.Count(strings.TrimSpace(output.String()), "\n") + 1; lines != 3 {
		test.Fatalf("recovery did not print the operation view after each step: %q", output.String())
	}
}

func TestRecoverIsIdempotentWhenEveryIsolationIsReleased(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	fixture := &recoveryServer{operation: "0123456789abcdef0123456789abcdef", decided: true, retired: true, released: true}
	server := httptest.NewServer(fixture.handler(test))
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"recover", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	if len(fixture.calls) != 1 || fixture.calls[0] != "/v1/databases/operations" {
		test.Fatalf("released recovery issued a write: %v", fixture.calls)
	}
	if !strings.Contains(output.String(), `"release_request_id":"release-0123456789abcdef0123456789abcdef"`) {
		test.Fatalf("released recovery did not return the current view: %q", output.String())
	}
}

func TestRecoverStopsAtTheFirstFailedStep(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	fixture := &recoveryServer{operation: "0123456789abcdef0123456789abcdef"}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			test.Error("recovery request body unavailable")
			return
		}
		fixture.mutex.Lock()
		defer fixture.mutex.Unlock()
		fixture.calls = append(fixture.calls, request.URL.Path)
		if request.URL.Path == "/v1/databases/operations/decision" {
			response.WriteHeader(http.StatusConflict)
			json.NewEncoder(response).Encode(map[string]any{"version": 1, "request_id": body["request_id"], "ok": false, "result": nil, "error": map[string]any{"code": "database_isolated"}})
			return
		}
		commandReply(response, body["request_id"].(string), map[string]any{"operations": fixture.view(), "recovery": "isolation_release_or_new_database_required"})
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"recover", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(""), &output); err == nil {
		test.Fatal("recovery continued after a failed step")
	}
	fixture.mutex.Lock()
	defer fixture.mutex.Unlock()
	if len(fixture.calls) != 2 || fixture.calls[1] != "/v1/databases/operations/decision" {
		test.Fatalf("recovery continued past the failure: %v", fixture.calls)
	}
}

func TestRecoverRejectsExplicitRequestID(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		test.Error("recover with --request-id reached the service")
	}))
	defer server.Close()
	var output bytes.Buffer
	err := run([]string{"recover", "--endpoint", server.URL, "--database", "tenant/database/main", "--request-id", "correlation"}, strings.NewReader(""), &output)
	if err == nil || !strings.Contains(err.Error(), "--request-id") {
		test.Fatalf("recover accepted a correlation id: %v", err)
	}
}
