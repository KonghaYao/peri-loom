package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	controller "peri-loom/controller"
)

const hranaTraceWrites = `{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO records(id) VALUES(?)","args":[{"type":"integer","value":"1"}]}},{"type":"execute","stmt":{"sql":"INSERT INTO records(id) VALUES(?)","args":[{"type":"integer","value":"2"}]}},{"type":"close"}]}`

func hranaTraceCall(test *testing.T, handler http.Handler, token, body string, headers []string) *httptest.ResponseRecorder {
	test.Helper()
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	for _, value := range headers {
		request.Header.Add("X-Request-ID", value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"type":"error"`) {
		test.Fatalf("pipeline failed: %d %s", response.Code, response.Body.String())
	}
	return response
}

// Correlation must survive the public adapter while each committing inner call
// owns a separate intent for the exact command that reaches the worker.
func TestHranaTracePipelineKeepsDistinctInnerIntents(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	const outer = "http-pipeline-one"
	var dispatched []gatewayCall
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "guarded_readonly_statement":
			return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
		case "statement":
			fixture.upstream.mu.Lock()
			call := fixture.upstream.calls[len(fixture.upstream.calls)-1]
			fixture.upstream.mu.Unlock()
			state, err := fixture.state.Snapshot()
			if err != nil {
				return nil, err
			}
			matching := 0
			for _, record := range state.Operations {
				if record.RequestID == call.requestID && record.Action == "statement" && record.State == "in_flight" && record.Digest == commandDigest(call.encoded) {
					matching++
				}
			}
			if call.requestID == outer || matching != 1 {
				return nil, fmt.Errorf("write has %d matching intents or reused outer ID", matching)
			}
			dispatched = append(dispatched, call)
			return cachedWriteReply(), nil
		}
		return nil, nil
	})
	hranaTraceCall(test, newHranaHTTP(fixture.gateway), fixture.tokenA, hranaTraceWrites, []string{outer})
	records := requestPhaseRecords(test, observer)
	if len(records) != 4 || len(dispatched) != 2 {
		test.Fatalf("records=%d writes=%d, want open + two writes + close", len(records), len(dispatched))
	}
	seen := map[string]bool{}
	for index, record := range records {
		if record.OuterRequestID != outer || record.HranaStep != uint64(index+1) || !sqlLabel(record.RequestID) || record.RequestID == outer || seen[record.RequestID] {
			test.Fatalf("invalid inner correlation: %+v", record)
		}
		seen[record.RequestID] = true
	}
	if dispatched[0].requestID == dispatched[1].requestID || commandDigest(dispatched[0].encoded) == commandDigest(dispatched[1].encoded) {
		test.Fatal("distinct bound writes reused an operation identity or command digest")
	}
	for _, call := range dispatched {
		operations := operationRecordsOf(test, fixture.state, call.requestID)
		if !seen[call.requestID] || len(operations) != 1 || operations[0].State != "terminal" || operations[0].BusinessOutcome != "completed" || operations[0].RetryAllowed {
			test.Fatalf("inner request lost its terminal intent: %+v", operations)
		}
		if strings.Contains(string(call.encoded), outer) || strings.Contains(string(call.encoded), "hrana_step") {
			test.Fatal("diagnostic correlation entered the frozen worker command")
		}
	}
	if operations := operationRecordsOf(test, fixture.state, outer); len(operations) != 0 {
		test.Fatal("the HTTP correlation ID became a durable operation ID")
	}
}

func TestHranaTraceHTTPRequestsStaySeparateAcrossBaton(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	handler := newHranaHTTP(fixture.gateway)
	first := hranaTraceCall(test, handler, fixture.tokenA,
		`{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO records VALUES(1)"}}]}`, []string{"http-one"})
	var response hranaResponse
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Baton == nil || *response.Baton == "" {
		test.Fatal("first request did not preserve the stream baton")
	}
	body, _ := json.Marshal(map[string]any{"baton": *response.Baton, "requests": []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records VALUES(2)"}},
		map[string]any{"type": "close"},
	}})
	hranaTraceCall(test, handler, fixture.tokenA, string(body), []string{"http-two"})
	records := requestPhaseRecords(test, observer)
	if len(records) != 4 {
		test.Fatalf("records=%d, want two per HTTP request", len(records))
	}
	seen := map[string]bool{}
	for index, record := range records {
		outer := "http-one"
		if index >= 2 {
			outer = "http-two"
		}
		if record.OuterRequestID != outer || record.HranaStep != uint64(index%2+1) || seen[record.RequestID] {
			test.Fatalf("HTTP requests shared correlation or an inner ID: %+v", record)
		}
		seen[record.RequestID] = true
	}
}

func TestHranaTraceInvalidOrRepeatedHeadersNeverEnterRecords(test *testing.T) {
	cases := [][]string{nil, {""}, {"contains space"}, {"has\nnewline"}, {"non-ascii-é"},
		{strings.Repeat("a", 129)}, {"valid-one", "valid-two"}, {"same-id", "same-id"}, {"one,two"}}
	generated := map[string]bool{}
	for index, headers := range cases {
		test.Run(fmt.Sprint(index), func(test *testing.T) {
			fixture, observer := requestPhaseObserverFixtureAll(test)
			hranaTraceCall(test, newHranaHTTP(fixture.gateway), fixture.tokenA, hranaTraceWrites, headers)
			records := requestPhaseRecords(test, observer)
			if len(records) != 4 {
				test.Fatalf("got %d records", len(records))
			}
			outer := records[0].OuterRequestID
			if !sqlLabel(outer) || generated[outer] {
				test.Fatalf("invalid or reused generated HTTP ID %q", outer)
			}
			generated[outer] = true
			for _, record := range records {
				if record.OuterRequestID != outer {
					test.Fatal("one HTTP request split into different correlations")
				}
				for _, rejected := range headers {
					if record.OuterRequestID == rejected || record.RequestID == rejected {
						test.Fatalf("rejected header entered a phase record: %q", rejected)
					}
				}
			}
		})
	}
}

func TestHranaTraceObserverOffPreservesContextAndWireBehavior(test *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v2/pipeline", nil)
	request.Header.Set("X-Request-ID", "outer-disabled")
	if bound := beginHranaCorrelation(request, nil); bound != request || nextHranaStep(bound.Context()) != request.Context() {
		test.Fatal("disabled observer changed the request or context")
	}
	var bodies []string
	var commands [][]map[string]any
	for _, enabled := range []bool{false, true} {
		fixture := newGatewayFixture(test)
		var observer *requestPhaseObserver
		if enabled {
			observer = newRequestPhaseObserverWithConfig(requestPhaseConfig{SlowThreshold: 0})
			fixture.gateway.config.OpenObserver = NewSQLOpenObserver()
			fixture.gateway.config.OpenObserver.phases = observer
		}
		response := hranaTraceCall(test, newHranaHTTP(fixture.gateway), fixture.tokenA, hranaTraceWrites, []string{"outer-disabled"})
		bodies = append(bodies, response.Body.String())
		var calls []map[string]any
		for _, call := range fixture.upstream.calls {
			calls = append(calls, call.command)
		}
		commands = append(commands, calls)
		if enabled && len(requestPhaseRecords(test, observer)) != 4 {
			test.Fatal("enabled observer did not capture the same four gateway calls")
		}
	}
	if bodies[0] != bodies[1] || !reflect.DeepEqual(commands[0], commands[1]) {
		test.Fatalf("observer changed Hrana replies or worker commands: %v", bodies)
	}
}

func TestHranaTraceCorrelationSurvivesPrivateSink(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	sink, err := phaseSink(context.Background(), observer, directory)
	if err != nil {
		test.Fatal(err)
	}
	stopped := false
	test.Cleanup(func() {
		if !stopped {
			sink.stop()
		}
	})
	hranaTraceCall(test, newHranaHTTP(fixture.gateway), fixture.tokenA, hranaTraceWrites, []string{"persisted-outer"})
	stopped = true
	if !sink.stop() {
		test.Fatal("phase sink did not join")
	}
	encoded, err := os.ReadFile(filepath.Join(directory, requestPhaseFileName))
	if err != nil {
		test.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
	if len(lines) != 5 {
		test.Fatalf("sink wrote %d lines, want four requests and close", len(lines))
	}
	for index, line := range lines[:4] {
		var record requestPhaseRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil || record.OuterRequestID != "persisted-outer" || record.HranaStep != uint64(index+1) || !sqlLabel(record.RequestID) {
			test.Fatalf("sink dropped correlation: %v %s", err, line)
		}
	}
}

func TestHranaTraceConcurrentDispatchesHaveDistinctStepNumbers(test *testing.T) {
	request := beginHranaCorrelation(httptest.NewRequest(http.MethodPost, "/v2/pipeline", nil), newRequestPhaseObserver())
	steps := make(chan hranaStep, 64)
	var group sync.WaitGroup
	for range cap(steps) {
		group.Add(1)
		go func() {
			defer group.Done()
			steps <- nextHranaStep(request.Context()).Value(hranaStepKey{}).(hranaStep)
		}()
	}
	group.Wait()
	close(steps)
	seen := map[uint64]bool{}
	outer := request.Context().Value(hranaCorrelationKey{}).(*hranaCorrelation).outer
	for step := range steps {
		if step.outer != outer || step.index < 1 || step.index > 64 || seen[step.index] {
			test.Fatalf("concurrent steps collided: %+v", step)
		}
		seen[step.index] = true
	}
}
