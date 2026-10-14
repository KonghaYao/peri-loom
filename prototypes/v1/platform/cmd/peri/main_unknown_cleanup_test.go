package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	sdk "peri-loom/platform/sdk/go"
)

type sqlOutputFailure struct {
	failure error
}

func (output sqlOutputFailure) Write([]byte) (int, error) {
	return 0, output.failure
}

func TestSQLCleanupRequiresSuccessfulOperationAndCloseACK(test *testing.T) {
	for _, action := range []string{"query", "execute"} {
		for _, sample := range []struct {
			name            string
			operationReply  string
			closeReply      string
			outputFailure   bool
			expectedCloses  int
			expectedCode    string
			expectedUnknown bool
		}{
			{name: "success", expectedCloses: 1},
			{name: "unknown", operationReply: "outcome_unknown", expectedCode: "outcome_unknown", expectedUnknown: true},
			{name: "bad_response", operationReply: "bad_response", expectedCode: "outcome_unknown", expectedUnknown: true},
			{name: "sql_error", operationReply: "sql_error", expectedCode: "sql_error"},
			{name: "close_missing_ack", closeReply: "missing_ack", expectedCloses: 1, expectedCode: "outcome_unknown", expectedUnknown: true},
			{name: "close_error", closeReply: "operation_incomplete", expectedCloses: 1, expectedCode: "operation_incomplete"},
			{name: "output_error", outputFailure: true},
		} {
			test.Run(action+"/"+sample.name, func(test *testing.T) {
				test.Setenv("PERI_API_KEY", commandCredential)
				var mutex sync.Mutex
				var paths []string
				var closeID string
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					mutex.Lock()
					defer mutex.Unlock()
					paths = append(paths, request.URL.Path)
					var body map[string]any
					if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
						test.Error("request body unavailable")
						return
					}
					requestID, valid := body["request_id"].(string)
					if !valid || request.Header.Get("Authorization") != "Bearer "+commandCredential {
						test.Error("request identity unavailable")
						return
					}
					failureReply := func(code string) {
						writer.WriteHeader(http.StatusConflict)
						json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": requestID, "ok": false, "result": nil, "error": map[string]any{"code": code, "execution_completed": code == "sql_error", "may_have_committed": code == "sql_error"}})
					}
					switch request.URL.Path {
					case "/v1/sql/open":
						commandReply(writer, requestID, map[string]any{"session": "0123456789abcdef0123456789abcdef", "readonly": false})
					case "/v1/sql/query", "/v1/sql/execute":
						if sample.operationReply == "bad_response" {
							io.WriteString(writer, commandCredential)
						} else if sample.operationReply != "" {
							failureReply(sample.operationReply)
						} else if action == "query" {
							commandReply(writer, requestID, [][]any{{1}})
						} else {
							commandReply(writer, requestID, map[string]bool{"executed": true})
						}
					case "/v1/sql/close":
						closeID = requestID
						if sample.closeReply == "operation_incomplete" {
							failureReply(sample.closeReply)
						} else {
							commandReply(writer, requestID, map[string]bool{"closed": sample.closeReply != "missing_ack"})
						}
					default:
						test.Error("unexpected RPC")
					}
				}))
				defer server.Close()
				var output bytes.Buffer
				var destination io.Writer = &output
				encodingError := errors.New("synthetic output failure")
				if sample.outputFailure {
					destination = sqlOutputFailure{failure: encodingError}
				}
				err := run([]string{"sql", "--endpoint", server.URL, "--database", "tenant/database/main", "--" + action, "SELECT 1", "--request-id", "operation-proof"}, strings.NewReader(""), destination)
				mutex.Lock()
				defer mutex.Unlock()
				if len(paths) != 2+sample.expectedCloses || paths[0] != "/v1/sql/open" || paths[1] != "/v1/sql/"+action || (sample.expectedCloses == 1 && paths[2] != "/v1/sql/close") {
					test.Fatalf("unexpected cleanup or retry: %v", paths)
				}
				if sample.outputFailure {
					if err != encodingError {
						test.Fatal("original output error replaced")
					}
				} else if sample.expectedCode != "" {
					var failure *sdk.Error
					if !errors.As(err, &failure) || failure.Code != sample.expectedCode || failure.OutcomeUnknown != sample.expectedUnknown {
						test.Fatal("original operation or close error not preserved")
					}
					expectedID := "operation-proof"
					if sample.expectedCloses == 1 {
						expectedID = closeID
					}
					if failure.RequestID != expectedID {
						test.Fatal("original error request identity replaced")
					}
					if sample.expectedCode == "sql_error" && (!failure.ExecutionCompleted || !failure.MayHaveCommitted) {
						test.Fatal("original terminal error metadata replaced")
					}
				} else if err != nil {
					test.Fatal("successful acknowledged operation rejected")
				}
				if strings.Contains(output.String(), commandCredential) || (err != nil && strings.Contains(err.Error(), commandCredential)) {
					test.Fatal("raw credential exposed")
				}
				if sample.operationReply != "" && output.Len() != 0 {
					test.Fatal("failed operation emitted success output")
				}
				if sample.operationReply == "" && !sample.outputFailure {
					expectedOutput := "[[1]]"
					if action == "execute" {
						expectedOutput = `{"executed":true}`
					}
					if strings.TrimSpace(output.String()) != expectedOutput {
						test.Fatal("successful operation output changed")
					}
				}
			})
		}
	}
}
