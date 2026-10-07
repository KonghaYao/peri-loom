package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	sdk "peri-loom/platform/sdk/go"
)

func TestKnownSessionMovedRequiresTypedNonUnknownError(test *testing.T) {
	for _, sample := range []struct {
		name     string
		failure  error
		expected bool
	}{
		{name: "nil"},
		{name: "typed_moved", failure: &sdk.Error{Code: "session_moved"}, expected: true},
		{name: "moved_but_unknown", failure: &sdk.Error{Code: "session_moved", OutcomeUnknown: true}},
		{name: "unknown", failure: &sdk.Error{Code: "outcome_unknown", OutcomeUnknown: true}},
		{name: "string_moved", failure: errors.New("session_moved")},
		{name: "wrapped_moved", failure: fmt.Errorf("synthetic wrapper: %w", &sdk.Error{Code: "session_moved"}), expected: true},
		{name: "sql_error", failure: &sdk.Error{Code: "sql_error"}},
	} {
		test.Run(sample.name, func(test *testing.T) {
			if knownSessionMoved(sample.failure) != sample.expected {
				test.Fatal("unsafe moved classification")
			}
		})
	}
}

func TestShellOnlyClosesAcknowledgedHealthySession(test *testing.T) {
	openPath, queryPath, executePath, closePath := "/v1/sql/open", "/v1/sql/query", "/v1/sql/execute", "/v1/sql/close"
	query := ":query SELECT 1\n"
	laterInput := ":reconnect\n:query SELECT 2\n:execute COMMIT\n:quit\n"
	for _, sample := range []struct {
		name            string
		input           string
		operationReply  string
		closeReply      string
		reopenFailure   bool
		outputFailure   bool
		paths           []string
		expectedCode    string
		expectedUnknown bool
		errorOrigin     string
	}{
		{name: "query_unknown", input: query + laterInput, operationReply: "outcome_unknown", paths: []string{openPath, queryPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "operation"},
		{name: "query_bad_ack", input: query + laterInput, operationReply: "bad_response", paths: []string{openPath, queryPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "operation"},
		{name: "query_missing_rows_ack", input: query + laterInput, operationReply: "missing_query_ack", paths: []string{openPath, queryPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "operation"},
		{name: "transport_lost_ack", input: query + laterInput, operationReply: "transport", paths: []string{openPath, queryPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "operation"},
		{name: "execute_unknown", input: ":execute INSERT INTO items VALUES(1)\n" + laterInput, operationReply: "outcome_unknown", paths: []string{openPath, executePath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "operation"},
		{name: "known_sql_error", input: query + laterInput, operationReply: "sql_error", paths: []string{openPath, queryPath}, expectedCode: "sql_error", errorOrigin: "operation"},
		{name: "moved_quit", input: query + ":execute COMMIT\n:quit\n", operationReply: "session_moved", paths: []string{openPath, queryPath}},
		{name: "moved_eof", input: query + ":query SELECT 2\n", operationReply: "session_moved", paths: []string{openPath, queryPath}},
		{name: "moved_explicit_reconnect", input: query + ":execute COMMIT\n:query SELECT 2\n:reconnect\n" + query + ":quit\n", operationReply: "session_moved", paths: []string{openPath, queryPath, openPath, queryPath, closePath}},
		{name: "moved_reconnect_open_error", input: query + ":reconnect\n" + query, operationReply: "session_moved", reopenFailure: true, paths: []string{openPath, queryPath, openPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "open"},
		{name: "healthy_quit", input: query + ":quit\n", paths: []string{openPath, queryPath, closePath}},
		{name: "healthy_eof", input: query, paths: []string{openPath, queryPath, closePath}},
		{name: "quit_close_error", input: query + ":quit\n", closeReply: "operation_incomplete", paths: []string{openPath, queryPath, closePath}, expectedCode: "operation_incomplete", errorOrigin: "close"},
		{name: "eof_close_error", input: query, closeReply: "operation_incomplete", paths: []string{openPath, queryPath, closePath}, expectedCode: "operation_incomplete", errorOrigin: "close"},
		{name: "missing_close_ack", input: query, closeReply: "missing_ack", paths: []string{openPath, queryPath, closePath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "close"},
		{name: "healthy_reconnect", input: query + ":reconnect\n" + query + ":quit\n", paths: []string{openPath, queryPath, closePath, openPath, queryPath, closePath}},
		{name: "reconnect_close_error", input: query + ":reconnect\n" + query, closeReply: "operation_incomplete", paths: []string{openPath, queryPath, closePath}, expectedCode: "operation_incomplete", errorOrigin: "close"},
		{name: "reconnect_open_error", input: query + ":reconnect\n" + query, reopenFailure: true, paths: []string{openPath, queryPath, closePath, openPath}, expectedCode: "outcome_unknown", expectedUnknown: true, errorOrigin: "open"},
		{name: "output_error", input: query + laterInput, outputFailure: true, paths: []string{openPath, queryPath}},
	} {
		test.Run(sample.name, func(test *testing.T) {
			test.Setenv("PERI_API_KEY", commandCredential)
			var mutex sync.Mutex
			var paths []string
			var operationID, closeID, openID string
			opens := 0
			operations := 0
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
				case openPath:
					opens++
					openID = requestID
					if sample.reopenFailure && opens > 1 {
						failureReply("outcome_unknown")
					} else {
						commandReply(writer, requestID, map[string]any{"session": "0123456789abcdef0123456789abcdef", "readonly": false})
					}
				case queryPath, executePath:
					operations++
					operationID = requestID
					if sample.operationReply == "transport" {
						connection, _, err := writer.(http.Hijacker).Hijack()
						if err != nil {
							test.Error("mock transport unavailable")
							return
						}
						connection.Close()
					} else if sample.operationReply == "missing_query_ack" {
						commandReply(writer, requestID, nil)
					} else if sample.operationReply == "bad_response" {
						io.WriteString(writer, commandCredential)
					} else if sample.operationReply != "" && (sample.operationReply != "session_moved" || operations == 1) {
						failureReply(sample.operationReply)
					} else if request.URL.Path == queryPath {
						commandReply(writer, requestID, [][]any{{1}})
					} else {
						commandReply(writer, requestID, map[string]bool{"executed": true})
					}
				case closePath:
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
			encodingError := errors.New("synthetic shell output failure")
			if sample.outputFailure {
				destination = sqlOutputFailure{failure: encodingError}
			}
			err := run([]string{"shell", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(sample.input), destination)
			mutex.Lock()
			defer mutex.Unlock()
			if !reflect.DeepEqual(paths, sample.paths) {
				test.Fatalf("unexpected cleanup, reconnect or SQL retry: %v", paths)
			}
			if sample.outputFailure {
				if err != encodingError {
					test.Fatal("original output error replaced")
				}
			} else if sample.expectedCode != "" {
				var failure *sdk.Error
				if !errors.As(err, &failure) || failure.Code != sample.expectedCode || failure.OutcomeUnknown != sample.expectedUnknown {
					test.Fatal("original error replaced")
				}
				expectedID := operationID
				if sample.errorOrigin == "close" {
					expectedID = closeID
				} else if sample.errorOrigin == "open" {
					expectedID = openID
				}
				if failure.RequestID != expectedID {
					test.Fatal("original error request identity replaced")
				}
				if sample.expectedCode == "sql_error" && (!failure.ExecutionCompleted || !failure.MayHaveCommitted) {
					test.Fatal("original terminal error metadata replaced")
				}
			} else if err != nil {
				test.Fatal("healthy shell rejected")
			}
			if strings.Contains(output.String(), commandCredential) || (err != nil && strings.Contains(err.Error(), commandCredential)) {
				test.Fatal("raw credential exposed")
			}
			if sample.operationReply == "session_moved" && !strings.Contains(output.String(), "session_moved") {
				test.Fatal("known fenced state not reported")
			}
			if sample.expectedCode == "" && !sample.outputFailure && sample.operationReply != "session_moved" && !strings.Contains(output.String(), "[[1]]") {
				test.Fatal("successful query output changed")
			}
		})
	}
}
