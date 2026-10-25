package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// holdCloseLane saturates the project's session-close capacity so the pipeline's
// own close is refused before dispatch.
func holdCloseLane(test *testing.T, fixture *gatewayFixture) {
	test.Helper()
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	quota := state.Projects["project-a"].Quotas
	for count := 0; count < max(1, quota.MaxConcurrent); count++ {
		permit, reason := fixture.gateway.admission.reserve("project-a", quota, false, true)
		if reason != rejectionNone {
			test.Fatal(reason)
		}
		test.Cleanup(permit.finish)
	}
}

func TestHranaPipelineCloseKeepsFirstFailure(t *testing.T) {
	for _, entry := range []string{
		`{"type":"execute","stmt":{"sql":"SELECT 1"}}`,
		`{"type":"sequence","sql":"SELECT 1"}`,
		`{"type":"batch","batch":{"steps":[{"stmt":{"sql":"SELECT 1"}},{"stmt":{"sql":"SELECT 2"}}]}}`,
		`{"type":"describe","sql":"SELECT 1"}`,
		`{"type":"get_autocommit"}`,
	} {
		for _, closeFails := range []bool{false, true} {
			t.Run(entry+"/closeFails="+map[bool]string{false: "false", true: "true"}[closeFails], func(t *testing.T) {
				fixture := newGatewayFixture(t)
				handler := newHranaHTTP(fixture.gateway)
				path := hranaV3JSONPath + "/pipeline"
				opened := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`)
				baton := hranaV3JSONDocument(t, opened)["baton"]
				session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
				fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
					if command["command"] == "execute" {
						return nil, &SQLRPCError{Ambiguous: true}
					}
					return nil, nil
				})
				unknown := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT"})
				if gatewayCode(t, unknown) != "outcome_unknown" {
					t.Fatal("未建立隔离前置条件")
				}
				if closeFails {
					holdCloseLane(t, fixture)
				}
				body, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{json.RawMessage(entry), map[string]any{"type": "close"}}})
				if err != nil {
					t.Fatal(err)
				}
				response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", string(body))
				document := hranaV3JSONDocument(t, response)
				if closeFails {
					failure, _ := document["error"].(map[string]any)
					cleanup, _ := document["cleanup_error"].(map[string]any)
					cleanupFailure, _ := cleanup["error"].(map[string]any)
					if response.Code != 409 || failure["code"] != "database_isolated" || cleanup["status"] != float64(429) || cleanupFailure["code"] != "quota_exceeded" {
						t.Fatalf("清理覆盖了语句失败: %d %s", response.Code, response.Body.String())
					}
					if fixture.upstream.count("close_session") != 0 {
						t.Fatal("被限流的 close 不应派发")
					}
				} else if response.Code != 200 || document["baton"] != nil || document["cleanup_error"] != nil || fixture.upstream.count("close_session") != 1 {
					t.Fatalf("close 成功路径改变: %d %s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestHranaPipelineLocalFailureSurvivesCloseFailure(t *testing.T) {
	for _, entry := range []map[string]any{
		{"type": "describe", "sql_id": 99},
		{"type": "sequence", "sql_id": 99},
	} {
		for _, closeFails := range []bool{false, true} {
			name := entry["type"].(string) + "/closeFails=" + map[bool]string{false: "false", true: "true"}[closeFails]
			t.Run(name, func(t *testing.T) {
				fixture := newGatewayFixture(t)
				handler := newHranaHTTP(fixture.gateway)
				path := hranaV3JSONPath + "/pipeline"
				opened := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`)
				baton := hranaV3JSONDocument(t, opened)["baton"]
				if closeFails {
					holdCloseLane(t, fixture)
				}
				body, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{entry, map[string]any{"type": "close"}}})
				if err != nil {
					t.Fatal(err)
				}
				response := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", string(body))
				document := hranaV3JSONDocument(t, response)
				results, _ := document["results"].([]any)
				if len(results) == 0 || results[0].(map[string]any)["error"].(map[string]any)["code"] != "invalid_arguments" {
					t.Fatalf("首个本地失败从 results 丢失: %d %s", response.Code, response.Body.String())
				}
				if closeFails {
					if len(results) != 1 {
						t.Fatalf("close 失败不应产生成功结果: %s", response.Body.String())
					}
					failure, _ := document["error"].(map[string]any)
					cleanup, _ := document["cleanup_error"].(map[string]any)
					cleanupFailure, _ := cleanup["error"].(map[string]any)
					if response.Code != 400 || failure["code"] != "invalid_arguments" || cleanup["status"] != float64(429) || cleanupFailure["code"] != "quota_exceeded" {
						t.Fatalf("清理失败覆盖了本地校验收到的首个失败: %d %s", response.Code, response.Body.String())
					}
					if fixture.upstream.count("close_session") != 0 {
						t.Fatal("被限流的 close 不应派发")
					}
				} else if response.Code != 200 || document["error"] != nil || document["cleanup_error"] != nil || len(results) != 2 || fixture.upstream.count("close_session") != 1 {
					t.Fatalf("close 成功路径改变: %d %s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func gatewaySessionCount(gateway *Gateway) int {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	return len(gateway.sessions)
}

// TestHranaConcurrentClosePipelinesStayWithinQuota reproduces the first half of
// issue #9: two pipelines that each close their own new stream collided on a
// single close lane, so one close was refused and its session leaked. Both
// closes must finish inside the project's MaxConcurrent quota.
func TestHranaConcurrentClosePipelinesStayWithinQuota(test *testing.T) {
	fixture := newGatewayFixture(test)
	handler := newHranaHTTP(fixture.gateway)
	arrived := make(chan struct{}, 2)
	released := make(chan struct{})
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] != "close_session" {
			return nil, nil
		}
		// Hold both closes inside the worker call so their control permits
		// overlap; a single close lane refuses the second one before this point.
		arrived <- struct{}{}
		select {
		case <-released:
		case <-time.After(5 * time.Second):
		}
		return nil, nil
	})
	go func() {
		deadline := time.After(5 * time.Second)
		for count := 0; count < 2; count++ {
			select {
			case <-arrived:
			case <-deadline:
			}
		}
		close(released)
	}()
	var responses [2]*httptest.ResponseRecorder
	var calls sync.WaitGroup
	calls.Add(2)
	for index := range responses {
		go func(index int) {
			defer calls.Done()
			responses[index] = hranaV3JSONCall(handler, fixture.tokenA, "/db/tenant-a/db-a/main/v2/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}`)
		}(index)
	}
	calls.Wait()
	for index, response := range responses {
		document := hranaV3JSONDocument(test, response)
		if response.Code != http.StatusOK || document["baton"] != nil || document["error"] != nil {
			test.Fatalf("concurrent close %d: %d %s", index, response.Code, response.Body.String())
		}
	}
	if sessions, seats := gatewaySessionCount(fixture.gateway), projectSessions(fixture, "project-a"); sessions != 0 || seats != 0 {
		test.Fatalf("concurrent closes leaked a session: registered=%d seats=%d", sessions, seats)
	}
	if counts := gatewayMetricCounts(test, fixture.gateway, "rejection_reasons_total"); counts[string(rejectionCloseLane)] != 0 {
		test.Fatalf("concurrent closes were refused: %v", counts)
	}
	if closes := fixture.upstream.count("close_session"); closes != 2 {
		test.Fatalf("concurrent closes dispatched %d close_session calls", closes)
	}
}

// TestHranaRejectedCloseReclaimsUnattachedSession reproduces the second half of
// issue #9: a new stream whose close is refused never registers a baton, so the
// platform must retire the session it opened. The quota admits one close and the
// test holds that slot, so the close is refused; the deferred cleanup must
// reclaim the session once the slot is free.
func TestHranaRejectedCloseReclaimsUnattachedSession(test *testing.T) {
	fixture := newGatewayFixture(test)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 1 })
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	held, reason := fixture.gateway.admission.reserve("project-a", state.Projects["project-a"].Quotas, false, true)
	if reason != rejectionNone || held == nil {
		test.Fatalf("close slot unavailable: %q", reason)
	}
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	response := hranaV3JSONCall(handler, fixture.tokenA, "/db/tenant-a/db-a/main/v2/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}`)
	document := hranaV3JSONDocument(test, response)
	failure, _ := document["error"].(map[string]any)
	if response.Code != http.StatusTooManyRequests || failure["code"] != "quota_exceeded" || failure["outcome_unknown"] != false {
		test.Fatalf("refused close changed its contract: %d %s", response.Code, response.Body.String())
	}
	handler.mu.Lock()
	streams := len(handler.streams)
	handler.mu.Unlock()
	if streams != 0 {
		test.Fatal("refused close registered a baton")
	}
	held.finish()
	deadline := time.Now().Add(5 * time.Second)
	for (gatewaySessionCount(fixture.gateway) != 0 || projectSessions(fixture, "project-a") != 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sessions, seats := gatewaySessionCount(fixture.gateway), projectSessions(fixture, "project-a"); sessions != 0 || seats != 0 {
		test.Fatalf("reclaimed session leaked: registered=%d seats=%d", sessions, seats)
	}
	if opens, closes := fixture.upstream.count("open_session"), fixture.upstream.count("close_session"); opens != 1 || closes != 1 {
		test.Fatalf("reclaim dispatched opens=%d closes=%d", opens, closes)
	}
}

// TestHranaDeliveredBatonKeepsItsSession guards the boundary of the deferred
// cleanup: it belongs only to a new stream that failed before its baton reached
// the client. A stream that returned a baton keeps both the registered stream
// and its native session.
func TestHranaDeliveredBatonKeepsItsSession(test *testing.T) {
	fixture := newGatewayFixture(test)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	response := hranaV3JSONCall(handler, fixture.tokenA, "/db/tenant-a/db-a/main/v2/pipeline", "application/json", `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`)
	document := hranaV3JSONDocument(test, response)
	baton, _ := document["baton"].(string)
	if response.Code != http.StatusOK || baton == "" {
		test.Fatalf("pipeline did not return a baton: %d %s", response.Code, response.Body.String())
	}
	handler.mu.Lock()
	registered := handler.streams[baton]
	handler.mu.Unlock()
	if registered == nil || gatewaySessionCount(fixture.gateway) != 1 || projectSessions(fixture, "project-a") != 1 {
		test.Fatalf("delivered baton lost its owners: stream=%t sessions=%d seats=%d", registered != nil, gatewaySessionCount(fixture.gateway), projectSessions(fixture, "project-a"))
	}
	closed := hranaV3JSONCall(handler, fixture.tokenA, "/db/tenant-a/db-a/main/v2/pipeline", "application/json", `{"baton":"`+baton+`","requests":[{"type":"close"}]}`)
	if closed.Code != http.StatusOK || gatewaySessionCount(fixture.gateway) != 0 {
		test.Fatalf("delivered stream did not close: %d %s", closed.Code, closed.Body.String())
	}
}

func TestHranaPipelineCleanupEvidenceIsIndependent(t *testing.T) {
	primary := hranaGatewayReply(409, []byte(`{"ok":false,"error":{"code":"database_isolated","operation_id":"statement-operation","stage":"tool_rpc","possible_commit":true,"execution_proven":false,"retry_allowed":false}}`), "/v1/sql/statement")
	cleanup := hranaGatewayReply(429, []byte(`{"ok":false,"error":{"code":"quota_exceeded","operation_id":"cleanup-operation","stage":"close_session","execution_completed":false,"may_have_committed":false}}`), "/v1/sql/close")
	baseline, response := httptest.NewRecorder(), httptest.NewRecorder()
	hranaPipelineFailure(baseline, "stream-baton", true, primary)
	hranaPipelineFailure(response, "stream-baton", true, primary, cleanup)
	before, after := hranaV3JSONDocument(t, baseline), hranaV3JSONDocument(t, response)
	if response.Code != http.StatusConflict || !reflect.DeepEqual(before["error"], after["error"]) || before["baton"] != after["baton"] {
		t.Fatalf("主失败证据被改写: %s", response.Body.String())
	}
	if after["cleanup_error"].(map[string]any)["error"].(map[string]any)["operation_id"] != "cleanup-operation" {
		t.Fatal("缺失独立清理关联标识")
	}
}
