package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestHeaderCorrelatedEarlyRejectionNeverReplays(test *testing.T) {
	for _, action := range []string{"open", "execute", "query"} {
		for _, status := range []int{401, 429} {
			for _, mismatched := range []bool{false, true} {
				test.Run(action+"-"+strconv.Itoa(status)+"-mismatch-"+strconv.FormatBool(mismatched), func(test *testing.T) {
					var requests atomic.Int32
					code := "quota_exceeded"
					if status == 401 {
						code = "unauthenticated"
					}
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						requests.Add(1)
						var body map[string]any
						if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
							test.Error(err)
						}
						identifier := request.Header.Get("X-Request-ID")
						if !label.MatchString(identifier) || identifier != body["request_id"] {
							test.Error("request header/body correlation lost")
						}
						if action != "open" && request.URL.Path == "/v1/sql/open" {
							writeReply(writer, identifier, map[string]any{"session": testSession, "readonly": false})
							return
						}
						if mismatched {
							identifier = "wrong-response-id"
						}
						writer.WriteHeader(status)
						json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": false, "result": nil, "error": map[string]string{"code": code}})
					}))
					defer server.Close()
					client, err := NewClient(server.URL, testCredential, Options{})
					if err != nil {
						test.Fatal(err)
					}
					session, err := client.Open(context.Background(), testDatabase())
					if action != "open" {
						if err != nil {
							test.Fatal(err)
						}
						if action == "execute" {
							err = session.Execute(context.Background(), "COMMIT", "early-reject")
						} else {
							_, err = session.Query(context.Background(), "INSERT INTO items VALUES(1) RETURNING id", "early-reject")
						}
					}
					var failure *Error
					if !errors.As(err, &failure) {
						test.Fatalf("expected typed rejection: %v", err)
					}
					expectedCode := code
					if mismatched {
						expectedCode = "outcome_unknown"
					}
					if failure.Code != expectedCode || failure.OutcomeUnknown != mismatched || failure.ExecutionCompleted || failure.MayHaveCommitted {
						test.Fatalf("rejection classification changed: %+v", failure)
					}
					if action != "open" && failure.RequestID != "early-reject" {
						test.Fatal("error lost the original request identifier")
					}
					if mismatched && session != nil {
						if err := session.Execute(context.Background(), "COMMIT", "no-replay"); err == nil {
							test.Fatal("uncertain session reused")
						}
					}
					expectedRequests := int32(2)
					if action == "open" {
						expectedRequests = 1
					}
					if requests.Load() != expectedRequests {
						test.Fatal("SDK replayed or restored a rejected request")
					}
				})
			}
		}
	}
}
