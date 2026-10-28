package platform

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// The guarded send is a native wait even though it may prove that no intent is
// needed. Exercise all three exits with observation on and off: observation must
// neither replay an unknown call nor alter the intent-before-write boundary.
func TestRequestPhaseGuardedStatementNativeBoundary(test *testing.T) {
	for _, enabled := range []bool{false, true} {
		profile := "off"
		if enabled {
			profile = "on"
		}
		for _, outcome := range []string{"read", "unknown", "requires_intent"} {
			test.Run(profile+"/"+outcome, func(test *testing.T) {
				fixture := newGatewayFixture(test)
				store := newGatedStateStore(fixture.state)
				fixture.gateway.config.State = store
				var observer *requestPhaseObserver
				if enabled {
					open := NewSQLOpenObserver()
					observer = newRequestPhaseObserver()
					observer.slowThreshold = 0
					open.phases = observer
					fixture.gateway.config.OpenObserver = open
				}
				session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
				requestPhaseRecords(test, observer)
				before := operationCommits(test, store)
				const requestID = "guarded-phase-request"
				const outerID = "guarded-phase-outer"
				var waitedNS uint64
				fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
					name := command["command"]
					if name != "guarded_readonly_statement" && name != "statement" {
						return nil, nil
					}
					trace, _ := ctx.Value(requestPhaseKey{}).(*requestPhaseTrace)
					if !enabled {
						if trace != nil {
							test.Fatal("disabled observation bound a trace during dispatch")
						}
					} else {
						if trace == nil {
							test.Fatal("enabled dispatch has no request trace")
						}
						trace.mutex.Lock()
						frames := append([]requestPhaseFrame(nil), trace.frames...)
						outer := trace.outer
						trace.mutex.Unlock()
						if len(frames) < 2 || frames[len(frames)-1].phase != requestPhaseNative || frames[len(frames)-2].phase != requestPhaseWorker {
							test.Fatalf("%s dispatch is not nested native inside worker: %+v", name, frames)
						}
						if outer.outer != outerID || outer.index != 7 {
							test.Fatalf("dispatch lost its exact outer request owner: %+v", outer)
						}
						// Bound a real wait inside the native span without asserting an
						// upper latency bound that would fail on a busy test runner.
						started := time.Now()
						time.Sleep(time.Millisecond)
						waitedNS += uint64(time.Since(started))
					}
					if name == "statement" {
						records := operationRecordsOf(test, fixture.state, requestID)
						if len(records) != 1 || records[0].State != "in_flight" {
							test.Fatal("committing dispatch did not wait for its durable intent")
						}
						return guardedReadonlyReply([][]any{}), nil
					}
					switch outcome {
					case "unknown":
						return nil, &SQLRPCError{Ambiguous: true}
					case "requires_intent":
						return map[string]any{"requires_intent": true, "reason": "engine_writable"}, nil
					default:
						return guardedReadonlyReply([][]any{{"42"}}), nil
					}
				})
				body, err := json.Marshal(map[string]any{"session": session, "sql": "SELECT 42", "request_id": requestID})
				if err != nil {
					test.Fatal(err)
				}
				request := httptest.NewRequest("POST", "/v1/sql/statement", strings.NewReader(string(body)))
				request = request.WithContext(context.WithValue(request.Context(), hranaStepKey{}, hranaStep{outer: outerID, index: 7}))
				request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
				response := httptest.NewRecorder()
				fixture.gateway.ServeHTTP(response, request)
				wantStatus, wantCommits, wantCalls, wantOutcome := 200, 0, 1, "ok"
				if outcome == "unknown" {
					wantStatus, wantCommits, wantOutcome = 502, 1, "outcome_unknown"
				} else if outcome == "requires_intent" {
					wantCommits, wantCalls = 2, 2
				}
				if response.Code != wantStatus {
					test.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
				}
				if outcome == "unknown" && gatewayCode(test, response) != "outcome_unknown" {
					test.Fatal("ambiguous guarded result lost Unknown semantics")
				}
				if fixture.upstream.count("guarded_readonly_statement") != 1 || fixture.upstream.count("statement") != wantCalls-1 {
					test.Fatal("observation changed dispatch count or replayed SQL")
				}
				if grew := operationCommits(test, store) - before; grew != wantCommits {
					test.Fatalf("operation commits changed: got %d want %d", grew, wantCommits)
				}
				for _, call := range fixture.upstream.calls {
					if call.command["command"] == "guarded_readonly_statement" && (call.requestID == "" || call.requestID == requestID) {
						test.Fatal("guarded dispatch lost its distinct probe ID")
					}
					if call.command["command"] == "statement" && call.requestID != requestID {
						test.Fatal("committing dispatch changed the original request ID")
					}
				}
				records := requestPhaseRecords(test, observer)
				if !enabled {
					if len(records) != 0 {
						test.Fatal("disabled observation emitted records")
					}
					return
				}
				if len(records) != 1 {
					test.Fatalf("request emitted %d phase records, want one", len(records))
				}
				record := records[0]
				if record.RequestID != requestID || record.OuterRequestID != outerID || record.HranaStep != 7 || record.Action != "statement" || record.Outcome != wantOutcome || record.Truncated {
					test.Fatalf("invalid phase record ownership/outcome: %+v", record)
				}
				var nativeNS, sum uint64
				nativeCount, workerCount := 0, 0
				for _, span := range record.Spans {
					sum += span.NS
					if span.Phase == requestPhaseNative {
						nativeCount++
						nativeNS += span.NS
						if span.Kind != requestPhaseWait {
							test.Fatal("native boundary was not recorded as wait")
						}
					}
					if span.Phase == requestPhaseWorker {
						workerCount++
					}
				}
				if nativeCount != wantCalls || workerCount != wantCalls || nativeNS < waitedNS {
					test.Fatalf("native wait attribution wrong: native=%d worker=%d ns=%d waited=%d", nativeCount, workerCount, nativeNS, waitedNS)
				}
				if sum != record.AccountedNS || sum > record.TotalNS || sum+record.UnaccountedNS != record.TotalNS {
					test.Fatal("nested native wait was double-counted or lost from the partition")
				}
			})
		}
	}
}
