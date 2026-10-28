package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func prepareSum(t durabilityPrepareTimings) uint64 {
	return t.StateLockNS + t.CompactionGateNS + t.StateGateOtherNS + t.EntryChecksNS + t.EditSetupNS + t.EditNS + t.RetentionNS + t.ValidateNS + t.DeltaNS + t.SubmitEntryNS + t.QueueLockNS + t.OrderNS + t.EncodeNS + t.EnqueueOtherNS
}

func TestOperationPreparationPartitionsAndLinks(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	observer := newDurabilityObserver(64)
	store.operations.observation.Store(observer)
	if err := beginOperation(store, securityDatabase(), "current-intent", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	events := collectDurabilityEvents(observer)
	var preparation, namespace *durabilityEvent
	var batchMember durabilityMember
	for i := range events {
		e := &events[i]
		switch e.Kind {
		case "journal_prepare":
			if preparation != nil {
				test.Fatal("preparation emitted twice")
			}
			preparation = e
		case "journal_namespace_wait":
			namespace = e
		case "journal_batch":
			batchMember = e.Members[0]
		}
	}
	if preparation == nil || namespace == nil || preparation.Preparation == nil || prepareSum(*preparation.Preparation) != preparation.TotalNS {
		test.Fatalf("non-disjoint preparation: %+v", events)
	}
	if namespace.FinishedUnixNS > preparation.StartedUnixNS || namespace.TotalNS == 0 || preparation.Preparation.StateLockNS == 0 || preparation.Preparation.EncodeNS == 0 {
		test.Fatalf("missing or overlapping boundary: %+v", events)
	}
	if preparation.ID != batchMember.ID || namespace.ID != batchMember.ID || batchMember.RequestID != "current-intent" || batchMember.Correlation != "caller_verified" || batchMember.Phase != "intent" || preparation.Outcome != "enqueued" {
		test.Fatalf("caller was not joined through member ID: %+v", events)
	}
}

func TestOperationPreparationCallerCannotGuessMultiOwner(test *testing.T) {
	primary := OperationRecord{OperationStatus: OperationStatus{OperationID: "close-op", RequestID: "current-close", Source: "gateway", State: "terminal"}}
	older := OperationRecord{OperationStatus: OperationStatus{OperationID: "old-open-op", RequestID: "old-open", Source: "gateway", State: "terminal"}}
	delta := operationDelta{Set: map[string]OperationRecord{"primary": primary, "older": older}}
	fallback := durabilityOperationMember(7, 9, delta, nil)
	if fallback.Owner != "multiple" {
		test.Fatal("fixture must have ambiguous stored owners")
	}
	for _, scenario := range []struct {
		name, operation, request, phase string
		valid                           bool
	}{
		{"matching-close", "close-op", "current-close", "terminal", true},
		{"wrong-operation", "missing", "current-close", "terminal", false},
		{"wrong-request", "close-op", "old-open", "terminal", false},
		{"wrong-phase", "close-op", "current-close", "intent", false},
		{"credential", "close-op", "plk1_private", "terminal", false},
		{"sql", "close-op", "SELECT private FROM hidden", "terminal", false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			p := &durabilityPreparation{caller: &durabilityCaller{operation: scenario.operation, request: scenario.request, phase: scenario.phase}}
			member := p.correlate(fallback, delta, nil)
			if scenario.valid {
				if member.RequestID != "current-close" || member.Correlation != "caller_verified" || !p.callerVerified {
					test.Fatalf("missing current caller: %+v", member)
				}
			} else if member != fallback || p.callerVerified {
				test.Fatalf("guessed a caller: %+v", member)
			}
		})
	}
	p := &durabilityPreparation{caller: &durabilityCaller{operation: "close-op", request: "current-close", phase: "completion"}}
	if got := p.correlate(fallback, delta, &AuditEvent{RequestID: "unrelated"}); got != fallback || p.callerVerified {
		test.Fatal("conflicting audit identity was accepted")
	}
	duplicate := primary
	duplicate.Database = securityDatabase()
	delta.Set["other-namespace"] = duplicate
	p = &durabilityPreparation{caller: &durabilityCaller{operation: "close-op", request: "current-close", phase: "terminal"}}
	if got := p.correlate(fallback, delta, nil); got != fallback || p.callerVerified {
		test.Fatal("duplicate primary across keys/namespaces was accepted")
	}
	delete(delta.Set, "other-namespace")
	primary.ReleaseRequestID = "administrative-release"
	delta.Set["primary"] = primary
	p = &durabilityPreparation{caller: &durabilityCaller{operation: "close-op", request: "current-close", phase: "terminal"}}
	if got := p.correlate(fallback, delta, nil); got != fallback || p.callerVerified {
		test.Fatal("historical administrative owner was relabeled")
	}
}

type observedGatewayResult struct {
	WriteStatus, CloseStatus, ExecuteCalls, CloseCalls int
	WriteCode, CloseCode                               string
	Records                                            []string
}

// Real durable registry + Gateway HTTP entry points: attaching the observer must
// neither retry dispatch nor change Unknown, session close or retained outcomes.
func TestOperationObservationGatewayOnOffAndMultiDeltaClose(test *testing.T) {
	for _, unknown := range []bool{false, true} {
		test.Run(fmt.Sprint(unknown), func(test *testing.T) {
			var baseline observedGatewayResult
			for _, enabled := range []bool{false, true} {
				state, gateway, upstream, _, credential := securityGatewayFixture(test)
				store := state.(*stateStore)
				observer := newDurabilityObserver(256)
				if enabled {
					store.operations.observation.Store(observer)
				}
				open := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": securityDatabase(), "request_id": "old-open"})
				var opened struct {
					Result struct {
						Session string `json:"session"`
					} `json:"result"`
				}
				if open.Code != 200 || json.Unmarshal(open.Body.Bytes(), &opened) != nil {
					test.Fatalf("open failed: %d %s", open.Code, open.Body.String())
				}
				if unknown {
					upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
						if command["command"] == "execute" {
							return nil, &SQLRPCError{Ambiguous: true}
						}
						return nil, nil
					})
				}
				secret := "INSERT INTO private_payload VALUES ('never-log-this')"
				write := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": opened.Result.Session, "request_id": "current-write", "sql": secret})
				closeReply := gatewayHTTP(test, gateway, credential, "/v1/sql/close", map[string]any{"session": opened.Result.Session, "request_id": "current-close"})
				result := observedGatewayResult{WriteStatus: write.Code, CloseStatus: closeReply.Code, WriteCode: gatewayCode(test, write), CloseCode: gatewayCode(test, closeReply), ExecuteCalls: upstream.count("execute"), CloseCalls: upstream.count("close_session")}
				if result.ExecuteCalls != 1 || unknown != (result.WriteCode == "outcome_unknown") || (!unknown && (result.CloseCalls != 1 || result.CloseStatus != 200)) || (unknown && (result.CloseCalls != 0 || result.CloseCode != "outcome_unknown")) {
					test.Fatalf("unexpected behavioral fixture: %+v", result)
				}
				view, err := store.Snapshot()
				if err != nil {
					test.Fatal(err)
				}
				for _, record := range view.Operations {
					result.Records = append(result.Records, fmt.Sprintf("%s/%s/%s/%s/%t/%t", record.RequestID, record.State, record.BusinessOutcome, record.CleanupOutcome, record.MayHaveCommitted, record.ExecutionProven))
				}
				sort.Strings(result.Records)
				events := collectDurabilityEvents(observer)
				encoded, _ := json.Marshal(events)
				if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), credential) || strings.Contains(string(encoded), "private_payload") || strings.Contains(string(encoded), opened.Result.Session) {
					test.Fatal("observer leaked private content")
				}
				if !enabled {
					baseline = result
					if len(events) != 0 || newDurabilityCaller(store, "unused", "current-close", "terminal", "gateway") != nil {
						test.Fatal("disabled observer allocated caller/events")
					}
					continue
				}
				if !reflect.DeepEqual(result, baseline) {
					test.Fatalf("ON changed behavior:\nON %+v\nOFF %+v", result, baseline)
				}
				var closePrepares int
				for _, event := range events {
					if event.Kind == "journal_prepare" && len(event.Members) == 1 && event.Members[0].RequestID == "current-close" && event.Members[0].Phase == "terminal" {
						closePrepares++
						if event.Members[0].Correlation != "caller_verified" || event.Owner != "request" {
							test.Fatal("close fell back to historical owners")
						}
						if event.Preparation == nil || prepareSum(*event.Preparation) != event.TotalNS {
							test.Fatal("close prepare is not a partition")
						}
					}
				}
				wantedClosePrepares := 1
				if unknown {
					wantedClosePrepares = 0
				}
				if closePrepares != wantedClosePrepares {
					test.Fatalf("multi-delta close lost exact caller: %+v", events)
				}
			}
		})
	}
}

// A caller waiting for a prior compaction is measured separately from state
// mutex acquisition. The existing mutation gate still refuses to admit it early.
func TestOperationPreparationCompactionGate(test *testing.T) {
	store, _ := commitQueueStore(test)
	defer store.Close()
	observer := newDurabilityObserver(64)
	store.operations.observation.Store(observer)
	gate := make(chan struct{})
	store.operationCompacting = gate
	before := store.cost.snapshot()["state_lock_waits_total"]
	result := make(chan error, 1)
	go func() {
		result <- beginOperation(store, securityDatabase(), "after-compaction", "gateway", "execute", "tool_rpc", "worker-a", "", nil)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for store.cost.snapshot()["state_lock_waits_total"] == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	store.disk.mutex.Lock()
	store.operationCompacting = nil
	close(gate)
	store.disk.mutex.Unlock()
	if err := awaitCompactionResult(test, result); err != nil {
		test.Fatal(err)
	}
	var found bool
	for _, event := range collectDurabilityEvents(observer) {
		if event.Kind == "journal_prepare" {
			found = true
			if event.Preparation.CompactionGateNS == 0 || event.Preparation.StateLockNS == 0 || prepareSum(*event.Preparation) != event.TotalNS {
				test.Fatalf("gate partition missing: %+v", event)
			}
		}
	}
	if !found {
		test.Fatal("no preparation")
	}
}
