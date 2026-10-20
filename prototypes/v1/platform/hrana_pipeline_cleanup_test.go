package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	controller "peri-loom/controller"
)

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
					state, err := fixture.state.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					permit, reason := fixture.gateway.admission.reserve("project-a", state.Projects["project-a"].Quotas, false, true)
					if reason != rejectionNone {
						t.Fatal(reason)
					}
					defer permit.finish()
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
					state, err := fixture.state.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					permit, reason := fixture.gateway.admission.reserve("project-a", state.Projects["project-a"].Quotas, false, true)
					if reason != rejectionNone {
						t.Fatal(reason)
					}
					defer permit.finish()
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
