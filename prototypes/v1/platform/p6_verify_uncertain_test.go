package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestP6VerifyActualNativeReplyLostEOFHoldsPermit(test *testing.T) {
	var armed, completed atomic.Bool
	var dispatches atomic.Int32
	proxyClient := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 10 * time.Second}
	test.Cleanup(proxyClient.CloseIdleConnections)
	route := func(workerID, actual string) string {
		proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(io.LimitReader(request.Body, 1024*1024))
			if err != nil {
				http.Error(writer, "fixture read failure", 502)
				return
			}
			var envelope struct {
				Command struct {
					Command string `json:"command"`
					SQL     string `json:"sql"`
				} `json:"command"`
			}
			if json.Unmarshal(body, &envelope) != nil {
				http.Error(writer, "fixture envelope failure", 502)
				return
			}
			drop := request.URL.Path == "/sql" && envelope.Command.Command == "execute" && envelope.Command.SQL == "INSERT INTO proof VALUES(1)" && armed.CompareAndSwap(true, false)
			if request.URL.Path == "/sql" {
				dispatches.Add(1)
			}
			forward, err := http.NewRequestWithContext(request.Context(), request.Method, actual+request.URL.Path, bytes.NewReader(body))
			if err != nil {
				http.Error(writer, "fixture forwarding failure", 502)
				return
			}
			forward.Header = request.Header.Clone()
			response, err := proxyClient.Do(forward)
			if err != nil {
				http.Error(writer, "fixture upstream failure", 502)
				return
			}
			defer response.Body.Close()
			encoded, err := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024))
			if err != nil {
				http.Error(writer, "fixture response failure", 502)
				return
			}
			if drop {
				var negative struct {
					OK                 bool `json:"ok"`
					ExecutionCompleted bool `json:"execution_completed"`
				}
				if json.Unmarshal(encoded, &negative) == nil && !negative.OK && negative.ExecutionCompleted {
					completed.Store(true)
				}
				hijacker, ok := writer.(http.Hijacker)
				if !ok {
					http.Error(writer, "fixture hijack failure", 502)
					return
				}
				connection, _, err := hijacker.Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
			for name, values := range response.Header {
				for _, value := range values {
					writer.Header().Add(name, value)
				}
			}
			writer.WriteHeader(response.StatusCode)
			_, _ = writer.Write(encoded)
		}))
		test.Cleanup(proxy.Close)
		return proxy.URL
	}
	fixture := p6VerifyNewRouted(test, Quotas{MaxConcurrent: 1}, route)
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY); INSERT INTO proof VALUES(1)")
	armed.Store(true)
	status, unknown := fixture.call(fixture.credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO proof VALUES(1)"})
	if !completed.Load() || status != 502 || unknown.Error.Code != "outcome_unknown" || unknown.Error.ExecutionCompleted || unknown.Error.MayHaveCommitted {
		test.Fatalf("real native terminal reply lost EOF status=%d code=%s observed_terminal_native=%t falsely_terminal_gateway=%t", status, unknown.Error.Code, completed.Load(), unknown.Error.ExecutionCompleted)
	}
	before := dispatches.Load()
	status, denied := fixture.call(fixture.credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM proof"})
	if status != 429 || denied.Error.Code != "quota_exceeded" || dispatches.Load() != before {
		test.Fatal("unknown EOF released SQL permit or dispatched replacement SQL")
	}
	status, denied = fixture.call(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	if status != 502 || denied.Error.Code != "outcome_unknown" || dispatches.Load() != before {
		test.Fatal("poisoned-session close fabricated completion")
	}
	if err := fixture.platform.command.Process.Signal(syscall.SIGTERM); err != nil {
		test.Fatal(err)
	}
	select {
	case <-fixture.platform.finished:
		var exit *exec.ExitError
		if !errors.As(fixture.platform.err, &exit) || exit.ExitCode() != 1 {
			test.Fatal("uncertain GoCLI shutdown falsely reported clean completion")
		}
	case <-time.After(15 * time.Second):
		test.Fatal("uncertain GoCLI shutdown did not actually finish")
	}
	test.Log("actual native error completed upstream; proxy discarded full reply then socket EOF; gateway unknown/no terminal flags, MaxConcurrent1 retained, poisoned close did not fabricate completion; actual SIGTERM exit1 refuses clean proof")
}
