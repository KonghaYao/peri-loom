package faultprobe

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type headerTransport func(*http.Request) (*http.Response, error)

func (transport headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func headerReply(identifier string, status int, code string) *http.Response {
	var failure any
	var result any = [][]int{{1}}
	if code != "" {
		failure = map[string]any{"code": code}
		result = nil
	}
	encoded, _ := json.Marshal(map[string]any{"version": 1, "request_id": identifier, "ok": code == "", "result": result, "error": failure})
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(encoded)))}
}

func TestRequestIDHeaderMatchesBodyWithoutNetwork(t *testing.T) {
	for _, path := range []string{"/v1/sql/open", "/v1/sql/query", "/v1/sql/execute", "/v1/sql/close", "/v1/databases/status"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			client := &Client{endpoint: "https://unit.invalid", key: "unit_private_key", prefix: "header", http: &http.Client{}}
			client.http.Transport = headerTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				var body testRequest
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				values := request.Header.Values("X-Request-ID")
				if len(values) != 1 || values[0] == "" || values[0] != body.RequestID {
					t.Fatal("request correlation header/body mismatch")
				}
				if request.Method != "POST" || request.URL.Path != path || request.GetBody != nil || request.Header.Get("Authorization") != "Bearer "+client.key || request.Header.Get("Content-Type") != "application/json" {
					t.Fatal("request policy changed")
				}
				return headerReply(body.RequestID, http.StatusOK, ""), nil
			})
			_, failure := client.call(context.Background(), path, map[string]any{"sql": "SELECT 1"})
			if failure != nil || calls != 1 {
				t.Fatalf("failure=%v calls=%d", failure, calls)
			}
		})
	}
}

func TestHeaderDoesNotRelaxResponseIDOrRetryAdmission(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		name := "correlated_quota"
		if mismatch {
			name = "mismatched_reply"
		}
		t.Run(name, func(t *testing.T) {
			client := &Client{endpoint: "https://unit.invalid", key: "unit_private_key", prefix: "header", http: &http.Client{}}
			calls := 0
			client.http.Transport = headerTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				var body testRequest
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				identifier := request.Header.Get("X-Request-ID")
				if identifier == "" || identifier != body.RequestID {
					t.Fatal("missing correlation")
				}
				if mismatch {
					identifier = "different_server_generated_id"
				}
				return headerReply(identifier, http.StatusTooManyRequests, "quota_exceeded"), nil
			})
			session := &Session{client: client, id: strings.Repeat("a", 32)}
			_, failure := session.SQL(context.Background(), true, "SELECT 1")
			if failure == nil || calls != 1 || failure.Unknown != mismatch || session.poisoned != mismatch {
				t.Fatalf("unexpected admission/unknown handling: failure=%v calls=%d", failure, calls)
			}
			if mismatch {
				if failure.Code != "invalid_envelope" {
					t.Fatal("response ID check weakened")
				}
				_, nextFailure := session.SQL(context.Background(), true, "SELECT 2")
				closeFailure := session.Close(context.Background())
				if nextFailure == nil || !nextFailure.Unknown || closeFailure == nil || !closeFailure.Unknown || calls != 1 {
					t.Fatal("poisoned session dispatched or retried")
				}
			} else if failure.Code != "quota_exceeded" {
				t.Fatal("correlated quota classification changed")
			}
		})
	}
}
