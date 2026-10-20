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

func TestCLIRendersSanitizedFailureAndUnknownEvidence(test *testing.T) {
	for _, sample := range []struct {
		name         string
		status       int
		failure      map[string]any
		expected     []string
		expectedPath []string
	}{
		{
			name:   "settled_sql_error",
			status: http.StatusBadRequest,
			failure: map[string]any{
				"code":                "sql_error",
				"message":             "statement violated a SQL constraint",
				"execution_completed": true,
				"may_have_committed":  true,
				"worker_text":         "private worker detail",
			},
			expected:     []string{"sql_error", "statement violated a SQL constraint", "request "},
			expectedPath: []string{"/v1/sql/open", "/v1/sql/query", "/v1/sql/close"},
		},
		{
			name:   "unknown_outcome",
			status: http.StatusBadGateway,
			failure: map[string]any{
				"code":               "outcome_unknown",
				"message":            "SQL upstream request failed",
				"operation_id":       "operation-7",
				"may_have_committed": true,
				"execution_proven":   false,
			},
			expected:     []string{"outcome_unknown", "SQL upstream request failed", "operation_id=operation-7", "may_have_committed=true", "execution_proven=false"},
			expectedPath: []string{"/v1/sql/open", "/v1/sql/query"},
		},
	} {
		test.Run(sample.name, func(test *testing.T) {
			test.Setenv("PERI_API_KEY", commandCredential)
			var mutex sync.Mutex
			var paths []string
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				mutex.Lock()
				defer mutex.Unlock()
				paths = append(paths, request.URL.Path)
				var body map[string]any
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					test.Error("request body unavailable")
					return
				}
				identifier, _ := body["request_id"].(string)
				if request.Header.Get("Authorization") != "Bearer "+commandCredential {
					test.Error("credential absent")
				}
				switch request.URL.Path {
				case "/v1/sql/open":
					commandReply(writer, identifier, map[string]any{"session": "0123456789abcdef0123456789abcdef", "readonly": false})
				case "/v1/sql/query":
					writer.WriteHeader(sample.status)
					json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": false, "result": nil, "error": sample.failure})
				case "/v1/sql/close":
					commandReply(writer, identifier, map[string]bool{"closed": true})
				default:
					test.Error("unexpected RPC")
				}
			}))
			defer server.Close()
			err := run([]string{"sql", "--endpoint", server.URL, "--database", "tenant/database/main", "--query", "SELECT missing FROM absent"}, strings.NewReader(""), &bytes.Buffer{})
			if err == nil {
				test.Fatal("failed SQL accepted")
			}
			var stderr bytes.Buffer
			reportFailure(&stderr, err)
			for _, expected := range sample.expected {
				if !strings.Contains(stderr.String(), expected) {
					test.Fatalf("%q missing from CLI failure: %s", expected, stderr.String())
				}
			}
			if strings.Contains(stderr.String(), "private worker detail") || strings.Contains(stderr.String(), commandCredential) {
				test.Fatalf("unpublished text reached stderr: %s", stderr.String())
			}
			mutex.Lock()
			defer mutex.Unlock()
			if strings.Join(paths, ",") != strings.Join(sample.expectedPath, ",") {
				test.Fatalf("unexpected cleanup: %v", paths)
			}
		})
	}
}
