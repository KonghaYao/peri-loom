package sdk

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type countingListener struct {
	net.Listener
	accepted *int64
}

func (listener *countingListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err == nil {
		atomic.AddInt64(listener.accepted, 1)
	}
	return connection, err
}

func newFixture(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *int64, *int64) {
	t.Helper()
	var accepted, requests int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		atomic.AddInt64(&requests, 1)
		handler(writer, request)
	}))
	server.Listener = &countingListener{Listener: server.Listener, accepted: &accepted}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, &accepted, &requests
}

func reuseEnvelope(identifier string) string {
	return `{"version":1,"request_id":"` + identifier + `","ok":true,"result":[],"error":null}`
}

func newTestClient(t *testing.T, server *httptest.Server, reuse bool) *Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	client, err := NewClient(server.URL, "cccccccccccccccccccccccccccccccccccc", Options{RootCAs: pool, ConnectionReuse: reuse})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return client
}

func TestConnectionReuseUsesOneConnection(t *testing.T) {
	server, accepted, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(reuseEnvelope(request.Header.Get("X-Request-ID"))))
	})
	client := newTestClient(t, server, true)
	for index := 0; index < 3; index++ {
		var rows [][]any
		identifier := "reuse-" + string(rune('a'+index))
		if err := client.call(context.Background(), "/v1/sql/query", identifier, map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
			t.Fatalf("call %d: %v", index, err)
		}
	}
	if atomic.LoadInt64(requests) != 3 {
		t.Fatalf("requests: %d", atomic.LoadInt64(requests))
	}
	if atomic.LoadInt64(accepted) != 1 {
		t.Fatalf("reuse must use exactly one connection, got %d", atomic.LoadInt64(accepted))
	}
}

func TestPerRequestModeCreatesConnectionPerCall(t *testing.T) {
	server, accepted, _ := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(reuseEnvelope(request.Header.Get("X-Request-ID"))))
	})
	client := newTestClient(t, server, false)
	for index := 0; index < 3; index++ {
		var rows [][]any
		identifier := "plain-" + string(rune('a'+index))
		if err := client.call(context.Background(), "/v1/sql/query", identifier, map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
			t.Fatalf("call %d: %v", index, err)
		}
	}
	if atomic.LoadInt64(accepted) != 3 {
		t.Fatalf("per-request mode must not reuse connections, got %d", atomic.LoadInt64(accepted))
	}
}

func TestMidFlightFailureIsUnknownAndNeverReplayed(t *testing.T) {
	var abort int64
	server, accepted, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		if atomic.LoadInt64(&abort) == 1 {
			panic(http.ErrAbortHandler)
		}
		_, _ = writer.Write([]byte(reuseEnvelope(request.Header.Get("X-Request-ID"))))
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	if err := client.call(context.Background(), "/v1/sql/query", "first", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	atomic.StoreInt64(&abort, 1)
	before := atomic.LoadInt64(requests)
	failure := client.call(context.Background(), "/v1/sql/query", "second", map[string]string{"sql": "SELECT 1"}, &rows, true)
	if failure == nil {
		t.Fatal("mid-flight abort must not be reported as success")
	}
	var typed *Error
	if !errors.As(failure, &typed) || typed.Code != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %v", failure)
	}
	if atomic.LoadInt64(requests) != before+1 {
		t.Fatalf("request was replayed: %d -> %d", before, atomic.LoadInt64(requests))
	}
	atomic.StoreInt64(&abort, 0)
	if err := client.call(context.Background(), "/v1/sql/query", "third", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("next call must open a fresh connection: %v", err)
	}
	if atomic.LoadInt64(accepted) != 2 {
		t.Fatalf("expected a fresh connection after failure, accepted=%d", atomic.LoadInt64(accepted))
	}
}

func echoPayload(identifier string) string {
	return `{"version":1,"request_id":"` + identifier + `","ok":true,"result":[["` + identifier + `"]],"error":null}`
}

// 覆盖 Go Transport 的 reused-connection 重试分支：请求已被服务端收到后才断连。
// 客户端必须只派发一次、返回 outcome_unknown，绝不因重试而重复执行 SQL。
func TestReusedConnectionMidFlightAbortIsSingleDispatch(t *testing.T) {
	var abort int64
	server, accepted, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		if atomic.LoadInt64(&abort) == 1 {
			panic(http.ErrAbortHandler)
		}
		_, _ = writer.Write([]byte(envelope_of(request)))
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	if err := client.call(context.Background(), "/v1/sql/query", "warm", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	if atomic.LoadInt64(accepted) != 1 {
		t.Fatalf("warmup must establish one connection, got %d", atomic.LoadInt64(accepted))
	}
	atomic.StoreInt64(&abort, 1)
	before := atomic.LoadInt64(requests)
	failure := client.call(context.Background(), "/v1/sql/query", "second", map[string]string{"sql": "SELECT 2"}, &rows, true)
	if failure == nil {
		t.Fatal("abort after receipt must not be reported as success")
	}
	var typed *Error
	if !errors.As(failure, &typed) || typed.Code != "outcome_unknown" || !typed.OutcomeUnknown {
		t.Fatalf("expected outcome_unknown, got %v", failure)
	}
	if got := atomic.LoadInt64(requests) - before; got != 1 {
		t.Fatalf("dispatched %d times on a reused connection, want exactly 1", got)
	}
}

func envelope_of(request *http.Request) string {
	return `{"version":1,"request_id":"` + request.Header.Get("X-Request-ID") + `","ok":true,"result":[],"error":null}`
}

// 显式 Connection: close：服务端在响应后关闭连接。
// 下一次是"从未派发"的新请求，允许重新建连（不得与"已收到请求禁止重发"混淆）。
func TestExplicitConnectionCloseForcesReconnect(t *testing.T) {
	server, accepted, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Connection", "close")
		_, _ = writer.Write([]byte(envelope_of(request)))
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	for index := 0; index < 2; index++ {
		identifier := fmt.Sprintf("close-%d", index)
		if err := client.call(context.Background(), "/v1/sql/query", identifier, map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
			t.Fatalf("call %d: %v", index, err)
		}
	}
	if atomic.LoadInt64(requests) != 2 || atomic.LoadInt64(accepted) != 2 {
		t.Fatalf("requests=%d accepted=%d, want 2/2", atomic.LoadInt64(requests), atomic.LoadInt64(accepted))
	}
}

// 静默关闭（无 Connection: close）：劫持连接写完完整合法响应后直接关闭 TCP。
// 客户端已把该连接视为空闲；下一次未派发请求必须重新建连并成功。同步确定，不用 sleep。
func TestSilentlyClosedIdleConnectionIsRecovered(t *testing.T) {
	server, accepted, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		hijacker, ok := writer.(http.Hijacker)
		if !ok {
			t.Error("hijack unsupported")
			return
		}
		connection, buffer, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		payload := envelope_of(request)
		_, _ = buffer.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
			fmt.Sprint(len(payload)) + "\r\n\r\n" + payload)
		_ = buffer.Flush()
		_ = connection.Close()
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	if err := client.call(context.Background(), "/v1/sql/query", "silent-1", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("first: %v", err)
	}
	if atomic.LoadInt64(accepted) != 1 {
		t.Fatalf("warmup connections: %d", atomic.LoadInt64(accepted))
	}
	if err := client.call(context.Background(), "/v1/sql/query", "silent-2", map[string]string{"sql": "SELECT 2"}, &rows, true); err != nil {
		t.Fatalf("after silent idle close: %v", err)
	}
	if atomic.LoadInt64(requests) != 2 || atomic.LoadInt64(accepted) != 2 {
		t.Fatalf("requests=%d accepted=%d, want 2/2", atomic.LoadInt64(requests), atomic.LoadInt64(accepted))
	}
}

// 响应截断（Content-Length 与实际字节不符）：必须 outcome_unknown，且不重放。
func TestTruncatedResponseIsUnknownWithoutReplay(t *testing.T) {
	var truncate int64
	server, _, requests := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		if atomic.LoadInt64(&truncate) == 1 {
			writer.Header().Set("Content-Length", "512")
			_, _ = writer.Write([]byte(`{"version":1,"request_id":"`))
			panic(http.ErrAbortHandler)
		}
		_, _ = writer.Write([]byte(envelope_of(request)))
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	if err := client.call(context.Background(), "/v1/sql/query", "trunc-warm", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	atomic.StoreInt64(&truncate, 1)
	before := atomic.LoadInt64(requests)
	failure := client.call(context.Background(), "/v1/sql/query", "trunc-2", map[string]string{"sql": "SELECT 2"}, &rows, true)
	if failure == nil {
		t.Fatal("truncated response must not be reported as success")
	}
	var typed *Error
	if !errors.As(failure, &typed) || typed.Code != "outcome_unknown" {
		t.Fatalf("expected outcome_unknown, got %v", failure)
	}
	if got := atomic.LoadInt64(requests) - before; got != 1 {
		t.Fatalf("truncated response was replayed: %d dispatches", got)
	}
}

// 并发调用：各自独立连接、响应不混读（按 request_id 回显校验）。
func TestConcurrentCallsDoNotMixResponses(t *testing.T) {
	server, accepted, _ := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(echoPayload(request.Header.Get("X-Request-ID"))))
	})
	client := newTestClient(t, server, true)
	var wait sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < 2; index++ {
				identifier := fmt.Sprintf("mix-%d-%d", worker, index)
				var rows [][]any
				if err := client.call(context.Background(), "/v1/sql/query", identifier, map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
					t.Errorf("worker %d: %v", worker, err)
					return
				}
				if len(rows) != 1 || rows[0][0] != identifier {
					t.Errorf("worker %d read another call's payload: %v", worker, rows)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
	if atomic.LoadInt64(accepted) < 4 {
		t.Fatalf("concurrent calls must not share a stream, accepted=%d", atomic.LoadInt64(accepted))
	}
}

// 清理：显式关闭空闲连接后，下次调用新建连接。
func TestCloseIdleConnectionsForcesFreshConnection(t *testing.T) {
	server, accepted, _ := newFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(envelope_of(request)))
	})
	client := newTestClient(t, server, true)
	var rows [][]any
	if err := client.call(context.Background(), "/v1/sql/query", "clean-1", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("first: %v", err)
	}
	client.http.CloseIdleConnections()
	client.http.CloseIdleConnections()
	if err := client.call(context.Background(), "/v1/sql/query", "clean-2", map[string]string{"sql": "SELECT 1"}, &rows, true); err != nil {
		t.Fatalf("after cleanup: %v", err)
	}
	if atomic.LoadInt64(accepted) != 2 {
		t.Fatalf("expected exactly one fresh connection after cleanup, accepted=%d", atomic.LoadInt64(accepted))
	}
}
