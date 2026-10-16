package platform

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestGatewayClientCancellationDoesNotReleaseLateUpstreamWork(test *testing.T) {
	fixture := newGatewayFixture(test)
	entered, settled := make(chan struct{}), make(chan struct{})
	var applied atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		_ = json.NewDecoder(request.Body).Decode(&envelope)
		_, _ = io.Copy(io.Discard, request.Body)
		if envelope.Command["command"] == "open_session" {
			_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"session": strings.Repeat("a", 64), "readonly": false}))
			return
		}
		// The classification probe is a prepare, not the dispatch this test holds.
		if envelope.Command["command"] == "describe" {
			_ = json.NewEncoder(writer).Encode(sqlTestDescribeReply(envelope.RequestID))
			return
		}
		close(entered)
		select {
		case <-request.Context().Done():
		case <-time.After(time.Second):
		}
		applied.Add(1)
		close(settled)
	}))
	defer server.Close()
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		worker := metadata.Workers["worker-tenant-a"]
		worker.Address = server.URL
		metadata.Workers[worker.ID] = worker
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	upstream, err := NewSQLUpstream(map[string]string{"worker-tenant-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	fixture.gateway.config.Upstream = upstream
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	body, _ := json.Marshal(map[string]any{"session": session, "sql": "COMMIT", "request_id": "cancel-late"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest("POST", "/v1/sql/execute", strings.NewReader(string(body))).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	response := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() { fixture.gateway.ServeHTTP(response, request); close(finished) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		test.Fatal("request never dispatched")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		test.Fatal("client cancellation not bounded")
	}
	if gatewayCode(test, response) != "outcome_unknown" {
		test.Fatal("cancellation pretended SQL rolled back")
	}
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		test.Fatal("simulated late execution never settled")
	}
	if applied.Load() != 1 || fixture.gateway.Metrics()["unknown_held_permits"] != 1 || fixture.gateway.Metrics()["active_permits"] != 1 {
		test.Fatal("late completion or HTTP cancellation fake-released conservative permit")
	}
}

func TestSQLUpstreamRequestLimitAndStreamedResponseLimit(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		writer.(http.Flusher).Flush()
		chunk := strings.Repeat("x", 1024*1024)
		for index := 0; index < 33; index++ {
			if _, err := io.WriteString(writer, chunk); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	worker := controller.Worker{ID: "worker-a", Address: server.URL}
	if !sqlTestFailure(test, upstream.Call(context.Background(), worker, "stream-limit", map[string]any{"command": "query"}, nil)).Ambiguous {
		test.Fatal("streamed oversized response accepted")
	}
	err = upstream.Call(context.Background(), worker, "request-limit", map[string]any{"command": "query", "sql": strings.Repeat("x", sqlWireLimit)}, nil)
	if sqlTestFailure(test, err).Ambiguous || calls.Load() != 1 {
		test.Fatal("oversized request dispatched")
	}
}
