package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func transportToken(test *testing.T) string {
	test.Helper()
	token, err := RandomID()
	if err != nil {
		test.Fatal(err)
	}
	return token
}

func transportInstance(test *testing.T, endpoint string) ControlTransport {
	test.Helper()
	transport, err := NewTransport(endpoint, transportToken(test), map[string]string{"worker-a": transportToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	return transport
}

func transportFailure(test *testing.T, err error) *RPCError {
	test.Helper()
	var failure *RPCError
	if !errors.As(err, &failure) {
		test.Fatalf("expected typed RPCError, got %T", err)
	}
	return failure
}

func transportReply(requestID string, result any, authority bool) map[string]any {
	reply := map[string]any{"version": 1, "request_id": requestID, "ok": true, "result": result, "error": nil}
	if authority {
		reply["precondition_failed"] = false
	}
	return reply
}

func TestTransportActualEnvelopePathsAndPrincipalTokens(test *testing.T) {
	controlToken, workerToken := transportToken(test), transportToken(test)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope struct {
			Version   uint32         `json:"version"`
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		if request.Method != "POST" || request.Header.Get("Content-Type") != "application/json" {
			test.Error("invalid HTTP request metadata")
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil || envelope.Version != 1 {
			test.Error("invalid protocol envelope")
		}
		authority := request.URL.Path == "/"
		expectedToken := controlToken
		if !authority {
			expectedToken = workerToken
			if request.URL.Path != "/control" || envelope.Command["command"] != "ownership" {
				test.Error("invalid worker command wrapping")
			}
			action, valid := envelope.Command["action"].(map[string]any)
			if !valid || action["command"] != "get_worker_state" {
				test.Error("invalid worker ownership action")
			}
		}
		if request.Header.Get("Authorization") != "Bearer "+expectedToken {
			test.Error("wrong principal credential")
		}
		_ = json.NewEncoder(writer).Encode(transportReply(envelope.RequestID, map[string]bool{"accepted": true}, authority))
	}))
	defer server.Close()
	tokens := map[string]string{"worker-a": workerToken}
	transport, err := NewTransport(server.URL, controlToken, tokens)
	if err != nil {
		test.Fatal(err)
	}
	tokens["worker-a"] = transportToken(test)
	var output struct {
		Accepted bool `json:"accepted"`
	}
	if err := transport.Authority(context.Background(), "authority-1", map[string]any{"command": "get_database_state"}, &output); err != nil || !output.Accepted {
		test.Fatalf("authority failed: %v", err)
	}
	if err := transport.Worker(context.Background(), Worker{ID: "worker-a", Address: server.URL}, "worker-1", map[string]any{"command": "get_worker_state"}, &output); err != nil || !output.Accepted {
		test.Fatalf("worker failed: %v", err)
	}
	if calls.Load() != 2 {
		test.Fatal("unexpected RPC count")
	}
}

func TestTransportEndpointAndCredentialValidation(test *testing.T) {
	for _, endpoint := range []string{"https://127.0.0.1", "http://localhost", "http://192.0.2.1", "http://127.1", "http://127.0.0.1/path", "http://127.0.0.1?", "http://127.0.0.1?q=secret", "http://127.0.0.1/#fragment", "http://user@127.0.0.1", "http://127.0.0.1:", "http://127.0.0.1:65536", "http://127.0.0.1/%2f", "http://[::1%25zone]"} {
		test.Run(endpoint, func(test *testing.T) {
			if _, err := NewTransport(endpoint, transportToken(test), nil); err == nil {
				test.Fatal("unsafe endpoint accepted")
			}
		})
	}
	for _, endpoint := range []string{"http://127.0.0.1", "http://127.0.0.1:1234/", "http://[::1]:1234"} {
		if _, err := NewTransport(endpoint, transportToken(test), nil); err != nil {
			test.Fatalf("valid endpoint rejected: %v", err)
		}
	}
	token := transportToken(test)
	for _, tokens := range []map[string]string{{"worker": token}, {"worker": "short"}, {"bad worker": transportToken(test)}, {"worker-a": strings.Repeat("x", 32), "worker-b": strings.Repeat("x", 32)}} {
		if _, err := NewTransport("http://127.0.0.1", token, tokens); err == nil {
			test.Fatal("invalid or duplicate credentials accepted")
		}
	}
	for _, credential := range []string{"short", strings.Repeat("x", 257), strings.Repeat("x", 31) + "\n"} {
		if _, err := NewTransport("http://127.0.0.1", credential, nil); err == nil {
			test.Fatal("invalid control credential accepted")
		}
	}
}

func TestTransportNeverRetriesLostMutationReply(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			test.Error("test connection hijack failed")
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	transport := transportInstance(test, server.URL)
	err := transport.Authority(context.Background(), "mutation-1", map[string]any{"command": "grant", "request": map[string]any{"request_id": "mutation-1"}}, nil)
	if failure := transportFailure(test, err); !failure.Ambiguous || failure.Rejected {
		test.Fatal("lost reply must be ambiguous")
	}
	if calls.Load() != 1 {
		test.Fatal("mutation was retried")
	}
}

func TestTransportRedirectIsNeverFollowed(test *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	defer server.Close()
	err := transportInstance(test, server.URL).Authority(context.Background(), "redirect-1", map[string]any{"command": "shutdown"}, nil)
	if failure := transportFailure(test, err); failure.StatusCode != 302 || !failure.Ambiguous {
		test.Fatal("redirect classification incorrect")
	}
	if redirected.Load() != 0 {
		test.Fatal("redirect followed")
	}
}

func TestTransportUnauthorizedAndBackendErrorsAreRedacted(test *testing.T) {
	for _, status := range []int{401, 403, 500} {
		test.Run(fmt.Sprint(status), func(test *testing.T) {
			secret := transportToken(test)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(status)
				_, _ = io.WriteString(writer, secret)
			}))
			defer server.Close()
			transport, err := NewTransport(server.URL, secret, nil)
			if err != nil {
				test.Fatal(err)
			}
			err = transport.Authority(context.Background(), "reject-1", map[string]any{"command": "shutdown"}, nil)
			failure := transportFailure(test, err)
			if strings.Contains(fmt.Sprintf("%+v", err), secret) || failure.Ambiguous != (status >= 500) || failure.Rejected != (status < 500) {
				test.Fatal("unsafe rejection classification or credential exposure")
			}
		})
	}
}

func TestTransportCancellationBeforeAndAfterDispatch(test *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		entered <- struct{}{}
		select {
		case <-request.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer server.Close()
	transport := transportInstance(test, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := transport.Authority(ctx, "cancel-before", map[string]any{"command": "shutdown"}, nil)
	if failure := transportFailure(test, err); failure.Ambiguous || !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		test.Fatal("pre-dispatch cancellation incorrect")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- transport.Authority(ctx, "cancel-after", map[string]any{"command": "shutdown"}, nil)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		test.Fatal("RPC not dispatched")
	}
	cancel()
	select {
	case err = <-finished:
	case <-time.After(3 * time.Second):
		test.Fatal("cancellation unbounded")
	}
	if failure := transportFailure(test, err); !failure.Ambiguous || !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		test.Fatal("post-dispatch cancellation incorrect")
	}
}

func TestTransportLocalValidationNoDispatch(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls.Add(1) }))
	defer server.Close()
	transport := transportInstance(test, server.URL)
	for _, command := range []any{nil, map[string]any{"command": "grant", "request": map[string]any{"request_id": "wrong"}}, map[string]any{"command": "grant", "request_id": "local-1", "request": map[string]any{}}, map[string]any{"command": "maintenance", "request_id": "wrong"}} {
		if failure := transportFailure(test, transport.Authority(context.Background(), "local-1", command, nil)); failure.Ambiguous {
			test.Fatal("local error is ambiguous")
		}
	}
	for _, output := range []any{42, (*int)(nil)} {
		transportFailure(test, transport.Authority(context.Background(), "local-1", map[string]any{"command": "shutdown"}, output))
	}
	transportFailure(test, transport.Worker(context.Background(), Worker{ID: "unknown", Address: server.URL}, "local-1", nil, nil))
	transportFailure(test, transport.Worker(context.Background(), Worker{ID: "worker-a", Address: "http://example.com"}, "local-1", nil, nil))
	if calls.Load() != 0 {
		test.Fatal("invalid request dispatched")
	}
}

func TestTransportRandomIDAndBoundedConfiguration(test *testing.T) {
	seen := make(map[string]bool)
	for index := 0; index < 128; index++ {
		identifier := transportToken(test)
		if len(identifier) != 32 || strings.Trim(identifier, "0123456789abcdef") != "" || seen[identifier] {
			test.Fatal("invalid random identifier")
		}
		seen[identifier] = true
	}
	transport := transportInstance(test, "http://127.0.0.1").(*httpControlTransport)
	httpTransport := transport.client.Transport.(*http.Transport)
	if httpTransport.Proxy != nil || !httpTransport.DisableKeepAlives || transport.client.Timeout <= 0 || httpTransport.ResponseHeaderTimeout <= 0 || httpTransport.MaxResponseHeaderBytes != 16384 {
		test.Fatal("unbounded or retry-capable configuration")
	}
}

func TestTransportDeadlineAfterDispatch(test *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		select {
		case <-request.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := transportInstance(test, server.URL).Authority(ctx, "deadline-1", map[string]any{"command": "shutdown"}, nil)
	if failure := transportFailure(test, err); !failure.Ambiguous || !errors.Is(err, context.DeadlineExceeded) {
		test.Fatal("deadline did not bound dispatched RPC")
	}
}

func TestTransportConnectFailureBeforeDispatch(test *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL
	server.Close()
	err := transportInstance(test, endpoint).Authority(context.Background(), "dial-1", map[string]any{"command": "shutdown"}, nil)
	if failure := transportFailure(test, err); failure.Ambiguous || failure.Rejected {
		test.Fatal("failed dial incorrectly claims dispatch or server rejection")
	}
}
