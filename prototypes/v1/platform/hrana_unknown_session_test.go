package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// TestHranaUnknownSessionAutocommitKeepsThePlatformFailure pins the reported
// chain: one ambiguous write poisons the session, and every later pipeline must
// answer with the platform's own unresolved outcome — code, operation id, stage,
// commit risk and baton — instead of a plain-text "autocommit unavailable" that
// hides an isolated database behind a protocol feature name.
func TestHranaUnknownSessionAutocommitKeepsThePlatformFailure(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			if sql, _ := command["sql"].(string); strings.Contains(sql, "INSERT") {
				return nil, testRPCError(false, false, SQLRPCError{Ambiguous: true})
			}
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway)
	path := hranaV3JSONPath + "/pipeline"
	first := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", `{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}}]}`)
	baton, _ := hranaV3JSONDocument(t, first)["baton"].(string)
	if first.Code != http.StatusOK || baton == "" {
		t.Fatalf("read pipeline failed: %d %s", first.Code, first.Body.String())
	}
	encoded, _ := json.Marshal(map[string]any{
		"baton":    baton,
		"requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO t VALUES (1)"}}},
	})
	write := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", string(encoded))
	writeFailure := hranaV3JSONDocument(t, write)["error"].(map[string]any)
	if write.Code != http.StatusBadGateway || writeFailure["code"] != "outcome_unknown" || writeFailure["partial_execution_possible"] != true {
		t.Fatalf("ambiguous write lost its proof: %d %s", write.Code, write.Body.String())
	}
	probe, _ := json.Marshal(map[string]any{
		"baton":    baton,
		"requests": []any{map[string]any{"type": "get_autocommit"}},
	})
	third := hranaV3JSONCall(handler, fixture.tokenA, path, "application/json", string(probe))
	document := hranaV3JSONDocument(t, third)
	failure, ok := document["error"].(map[string]any)
	if third.Code != http.StatusBadGateway || !ok {
		t.Fatalf("autocommit probe was not a structured failure: %d %s", third.Code, third.Body.String())
	}
	if failure["code"] != "outcome_unknown" || failure["outcome_unknown"] != true || failure["retry_allowed"] != false {
		t.Fatalf("autocommit probe lost the platform's verdict: %s", third.Body.String())
	}
	if failure["operation_id"] == nil || failure["operation_id"] == "" || failure["stage"] != "tool_rpc" || failure["possible_commit"] != true {
		t.Fatalf("autocommit probe lost its correlation: %s", third.Body.String())
	}
	if document["baton"] != baton {
		t.Fatalf("autocommit failure did not identify the baton: %s", third.Body.String())
	}
	if strings.Contains(third.Body.String(), "native_autocommit_unavailable") {
		t.Fatalf("synthetic autocommit code survived: %s", third.Body.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.gateway.Close(ctx); err == nil {
		t.Fatal("unresolved write let the gateway close without a terminal proof")
	}
}
