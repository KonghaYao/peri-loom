package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	controller "peri-loom/controller"
)

type nativeDiagnosticIdentity struct {
	Identity
	failure error
}

func (identity *nativeDiagnosticIdentity) Authorize(Principal, controller.DatabaseID, string) error {
	return identity.failure
}

type nativeDiagnosticTransport struct{ controller.ControlTransport }

type nativeDiagnosticStore struct {
	controller.MetadataStore
	failure error
}

func (store *nativeDiagnosticStore) Snapshot() (controller.Metadata, error) {
	if store.failure != nil {
		return controller.Metadata{}, store.failure
	}
	return store.MetadataStore.Snapshot()
}

func nativeDiagnosticManagement(test *testing.T) (*Management, *gatewayFixture, *nativeDiagnosticStore, *nativeDiagnosticIdentity) {
	test.Helper()
	fixture := newGatewayFixture(test)
	initial := &gatewayTestPlacements{metadata: controller.Metadata{Version: 1, Workers: map[string]controller.Worker{}, Placements: map[string]controller.Placement{}}}
	statusStore := &nativeDiagnosticStore{MetadataStore: initial}
	control, err := controller.NewController(statusStore, nativeDiagnosticTransport{}, controller.ReconcileConfig{})
	if err != nil {
		test.Fatal(err)
	}
	statusStore.MetadataStore = fixture.placements
	identity := &nativeDiagnosticIdentity{}
	management := &Management{config: ManagementConfig{State: fixture.state, Controller: control, Placements: fixture.placements, Identity: identity, Upstream: fixture.upstream}}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "checkpoint" {
			return map[string]bool{"checkpointed": true}, nil
		}
		return nil, nil
	})
	return management, fixture, statusStore, identity
}

func TestManagementNativeDiagnosticsStagesPreserveFailureAndCleanup(test *testing.T) {
	for _, stage := range []string{"authorize", "placement", "metadata", "worker_lookup", "open_session", "tool_rpc"} {
		test.Run(stage, func(test *testing.T) {
			management, fixture, statusStore, identity := nativeDiagnosticManagement(test)
			sentinel := errors.New("private-token namespace SQL-text must-not-be-logged")
			switch stage {
			case "authorize":
				identity.failure = sentinel
			case "placement":
				statusStore.failure = sentinel
			case "metadata":
				management.config.Placements = &nativeDiagnosticStore{failure: sentinel}
			case "worker_lookup":
				fixture.placements.metadata.Workers = map[string]controller.Worker{}
			case "open_session", "tool_rpc":
				fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
					if command["command"] == "checkpoint" && stage == "tool_rpc" || command["command"] == "open_session" && stage == "open_session" {
						return nil, sentinel
					}
					return nil, nil
				})
			}
			_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "stage-test", Database: fixture.first}, "/v1/databases/checkpoint")
			var failure *nativeToolError
			if !errors.As(err, &failure) || failure.stage != stage {
				test.Fatalf("incorrect stage: %v", err)
			}
			if stage == "placement" && err.Error() != "database is not active" {
				test.Fatal("original placement failure changed")
			}
			if stage != "worker_lookup" && stage != "placement" && (!errors.Is(err, sentinel) || err.Error() != sentinel.Error()) {
				test.Fatal("original failure changed")
			}
			if fixture.upstream.count("close_session") != 0 {
				test.Fatal("failed native tool triggered automatic cleanup")
			}
		})
	}
}

func TestManagementNativeDiagnosticsRenewPhasesStillRejectBeforeOpen(test *testing.T) {
	for _, phase := range []string{"renew_intent", "renew_pending", "renew_worker_pending"} {
		test.Run(phase, func(test *testing.T) {
			management, fixture, _, _ := nativeDiagnosticManagement(test)
			placement := fixture.placements.metadata.Placements[fixture.first.Key()]
			placement.Phase = phase
			fixture.placements.metadata.Placements[fixture.first.Key()] = placement
			_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "renew-proof", Database: fixture.first}, "/v1/databases/checkpoint")
			var failure *nativeToolError
			if !errors.As(err, &failure) || failure.stage != "placement" || err.Error() != "database is not active" {
				test.Fatalf("renew rejection contract changed: %v", err)
			}
			if fixture.upstream.count("open_session") != 0 || fixture.upstream.count("checkpoint") != 0 {
				test.Fatal("rejected renewal dispatched upstream")
			}
		})
	}
}

func TestManagementNativeDiagnosticsSuccessStillCloses(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	result, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "success", Database: fixture.first}, "/v1/databases/checkpoint")
	if err != nil || result == nil || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("checkpoint") != 1 || fixture.upstream.count("close_session") != 1 || management.nativeDiagnostics.count != 0 {
		test.Fatalf("successful native tool contract changed: %v", err)
	}
}

func TestManagementNativeDiagnosticsClosedFieldsAndTypedOutcomes(test *testing.T) {
	secret := errors.New("secret-token SQL-text private-namespace")
	for _, cause := range []error{secret, fmt.Errorf("private context: %w", &SQLRPCError{Ambiguous: true, cause: secret}), &SQLRPCError{Rejected: true}, &SQLRPCError{ExecutionCompleted: true}} {
		budget := nativeDiagnosticBudget{}
		var output bytes.Buffer
		record := nativeToolRecord("/v1/databases/checkpoint", "tool_rpc", "secret-phase", "safe-id", cause)
		budget.emit(record, &output)
		var decoded nativeDiagnosticRecord
		if json.Unmarshal(output.Bytes(), &decoded) != nil || decoded.Phase != "unknown" || decoded.Event != "management_native_tool_error" {
			test.Fatal("invalid closed record")
		}
		var rpc *SQLRPCError
		if errors.As(cause, &rpc) {
			if !decoded.RPCError || decoded.Ambiguous != rpc.Ambiguous || decoded.Rejected != rpc.Rejected || decoded.ExecutionCompleted != rpc.ExecutionCompleted {
				test.Fatal("typed RPC outcome changed")
			}
		} else if decoded.RPCError || decoded.Ambiguous || decoded.Rejected || decoded.ExecutionCompleted {
			test.Fatal("untyped failure claimed RPC outcome")
		}
		for _, private := range []string{"secret", "SQL-text", "private", "namespace", "context"} {
			if strings.Contains(output.String(), private) {
				test.Fatal("private information leaked")
			}
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(output.Bytes(), &fields) != nil || len(fields) != 9 {
			test.Fatal("unexpected diagnostic fields")
		}
	}
}

func TestManagementNativeDiagnosticsBoundsAndInvalidLabels(test *testing.T) {
	budget := nativeDiagnosticBudget{}
	record := nativeToolRecord("/v1/databases/checkpoint", "tool_rpc", "active", "safe", errors.New("secret"))
	var output bytes.Buffer
	for _, identifier := range []string{"", "bad\nsecret", strings.Repeat("x", 65)} {
		invalid := record
		invalid.RequestID = identifier
		budget.emit(invalid, &output)
	}
	for _, field := range []string{"action", "stage"} {
		invalid := record
		if field == "action" {
			invalid.Action = "private"
		} else {
			invalid.Stage = "private"
		}
		budget.emit(invalid, &output)
	}
	if output.Len() != 0 || budget.count != 0 {
		test.Fatal("invalid record consumed capacity")
	}
	var workers sync.WaitGroup
	for attempt := 0; attempt < 100; attempt++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			budget.emit(record, &output)
		}()
	}
	workers.Wait()
	if bytes.Count(output.Bytes(), []byte("\n")) != nativeDiagnosticLimit {
		test.Fatal("per-instance limit not enforced")
	}
	other := &Management{}
	other.nativeDiagnostics.emit(record, &output)
	if bytes.Count(output.Bytes(), []byte("\n")) != nativeDiagnosticLimit+1 {
		test.Fatal("distinct management did not receive its own budget")
	}
}
