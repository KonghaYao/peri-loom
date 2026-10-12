package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperationDecisionUsesIndependentRecordID(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if request.URL.Path != "/v1/databases/operations/decision" || json.NewDecoder(request.Body).Decode(&body) != nil || body["request_id"] != "correlation" || body["operation_id"] != "server_record" || body["decision"] != "retain_isolation_new_database_required" {
			test.Error("CLI operation identity or decision lost")
		}
		commandReply(response, "correlation", map[string]any{"operations": []any{}, "recovery": "new_database_or_fixture_required"})
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"operations/decision", "--endpoint", server.URL, "--database", "tenant/database/main", "--request-id", "correlation", "--operation-id", "server_record", "--decision", "retain_isolation_new_database_required"}, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
}
