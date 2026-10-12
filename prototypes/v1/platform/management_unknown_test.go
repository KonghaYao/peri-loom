package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	controller "peri-loom/controller"
)

func TestManagementUnknownOperationIdentity(test *testing.T) {
	for _, scenario := range []struct {
		name        string
		failure     error
		operationID string
	}{
		{"bare_rpc", &SQLRPCError{Ambiguous: true}, ""},
		{"wrapped_rpc", fmt.Errorf("wrapped: %w", &SQLRPCError{Ambiguous: true}), ""},
		{"unregistered_tool", &nativeToolError{stage: "open_session", cause: &SQLRPCError{Ambiguous: true}}, ""},
		{"predispatch_tool", &nativeToolError{stage: "placement", cause: &SQLRPCError{Ambiguous: true}}, ""},
		{"registered_tool", &nativeToolError{stage: "tool_rpc", operationID: "server_operation", cause: &SQLRPCError{Ambiguous: true}}, "server_operation"},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			response := httptest.NewRecorder()
			managementUnknownReply(response, "correlation_only", scenario.failure)
			var reply struct {
				RequestID string                     `json:"request_id"`
				Error     map[string]json.RawMessage `json:"error"`
			}
			if response.Code != 502 || json.Unmarshal(response.Body.Bytes(), &reply) != nil || reply.RequestID != "correlation_only" {
				test.Fatal("unknown response lost correlation")
			}
			if scenario.name == "predispatch_tool" && string(reply.Error["stage"]) != `"unknown"` {
				test.Fatal("error stage escaped SDK enum")
			}
			encoded, exists := reply.Error["operation_id"]
			if scenario.operationID == "" {
				if exists {
					test.Fatal("correlation masqueraded as operation record identity")
				}
			} else {
				var actual string
				if !exists || json.Unmarshal(encoded, &actual) != nil || actual != scenario.operationID {
					test.Fatal("registered operation identity lost")
				}
			}
		})
	}
}

func TestManagementUnknownBlocksFreshSession(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	management.config.State = fixture.state
	failure := &SQLRPCError{Ambiguous: true, Rejected: true, ExecutionCompleted: true}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "checkpoint" {
			return nil, failure
		}
		return nil, nil
	})
	_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "fresh_fault", Database: fixture.first}, "/v1/databases/checkpoint")
	if !errors.Is(err, failure) {
		test.Fatalf("lost flags: %v", err)
	}
	_, _ = management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "safe_read", Database: fixture.first}, "/v1/databases/diagnostics")
	_, _ = management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "no_retry", Database: fixture.first}, "/v1/databases/checkpoint")
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 || fixture.upstream.count("checkpoint") != 1 {
		test.Fatal("unknown database opened, mutated, or blindly closed again")
	}
}

func TestOperationReloadRestoresHeldAdmission(test *testing.T) {
	fixture := newGatewayFixture(test)
	if err := beginOperation(fixture.state, fixture.first, "lost_open", "gateway", "open", "open_session", "worker-tenant-a", "", map[string]string{"command": "open_session"}); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.state, fixture.first, "lost_open", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	reloaded, err := NewGateway(fixture.gateway.config)
	if err != nil {
		test.Fatal(err)
	}
	if reloaded.Metrics()["unknown_held_permits"] != 1 {
		test.Fatal("restart silently released unknown admission")
	}
}
