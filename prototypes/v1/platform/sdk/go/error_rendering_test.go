package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestErrorRendersSanitizedMessageWithoutWorkerText(test *testing.T) {
	failure := &Error{Code: "sql_error", Message: "statement violated a SQL constraint", RequestID: "request-1"}
	rendered := failure.Error()
	if !strings.Contains(rendered, "sql_error") || !strings.Contains(rendered, "statement violated a SQL constraint") || !strings.Contains(rendered, "request-1") {
		test.Fatalf("message or correlation missing: %s", rendered)
	}
	local := &Error{Code: "outcome_unknown", RequestID: "local", OutcomeUnknown: true}
	if rendered := local.Error(); strings.Contains(rendered, "operation_id=") || strings.Contains(rendered, "may_have_committed=") || strings.Contains(rendered, "execution_proven=") {
		test.Fatalf("absent evidence rendered as a guess: %s", rendered)
	}
}

func TestOutcomeUnknownRendersOnlyPublishedEvidence(test *testing.T) {
	reply := map[string]any{
		"code":               "outcome_unknown",
		"message":            "SQL upstream request failed",
		"operation_id":       "operation-7",
		"may_have_committed": true,
		"execution_proven":   false,
		"worker_text":        "private worker detail",
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			RequestID string `json:"request_id"`
		}
		json.NewDecoder(request.Body).Decode(&body)
		if request.URL.Path == "/v1/sql/open" {
			writeReply(writer, body.RequestID, map[string]any{"session": testSession, "readonly": false})
			return
		}
		writer.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": body.RequestID, "ok": false, "result": nil, "error": reply})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testCredential, Options{})
	if err != nil {
		test.Fatal(err)
	}
	session, err := client.Open(context.Background(), testDatabase())
	if err != nil {
		test.Fatal(err)
	}
	_, err = session.Query(context.Background(), "SELECT 1", "render-unknown")
	var failure *Error
	if !errors.As(err, &failure) || !failure.OutcomeUnknown {
		test.Fatalf("unknown outcome not typed: %v", err)
	}
	rendered := failure.Error()
	for _, expected := range []string{"SQL upstream request failed", "operation_id=operation-7", "may_have_committed=true", "execution_proven=false"} {
		if !strings.Contains(rendered, expected) {
			test.Fatalf("%q missing from %s", expected, rendered)
		}
	}
	if strings.Contains(rendered, "private worker detail") {
		test.Fatalf("unpublished worker text reached the caller: %s", rendered)
	}
}

func TestOutcomeUnknownKeepsExplicitFalseEvidence(test *testing.T) {
	reply := map[string]any{
		"code":               "outcome_unknown",
		"may_have_committed": false,
		"execution_proven":   false,
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			RequestID string `json:"request_id"`
		}
		json.NewDecoder(request.Body).Decode(&body)
		if request.URL.Path == "/v1/sql/open" {
			writeReply(writer, body.RequestID, map[string]any{"session": testSession, "readonly": false})
			return
		}
		writer.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": body.RequestID, "ok": false, "result": nil, "error": reply})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testCredential, Options{})
	if err != nil {
		test.Fatal(err)
	}
	session, err := client.Open(context.Background(), testDatabase())
	if err != nil {
		test.Fatal(err)
	}
	_, err = session.Query(context.Background(), "SELECT 1", "explicit-false")
	var failure *Error
	if !errors.As(err, &failure) || !failure.OutcomeUnknown {
		test.Fatalf("unknown outcome not typed: %v", err)
	}
	rendered := failure.Error()
	if !strings.Contains(rendered, "may_have_committed=false") || !strings.Contains(rendered, "execution_proven=false") {
		test.Fatalf("published false evidence dropped: %s", rendered)
	}
	if strings.Contains(rendered, "operation_id=") {
		test.Fatalf("absent operation identity rendered: %s", rendered)
	}
}
