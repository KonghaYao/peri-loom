package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

type managementStatusTransport struct {
	test          *testing.T
	database      controller.DatabaseID
	worker        controller.Worker
	native        controller.Actual
	workerFailure error
	refsFailure   error
	workerIDs     []string
	refsCalls     int
}

func (transport *managementStatusTransport) Authority(_ context.Context, requestID string, command any, output any) error {
	transport.test.Helper()
	encoded, err := json.Marshal(command)
	var action struct {
		Command   string                `json:"command"`
		RequestID string                `json:"request_id"`
		Database  controller.DatabaseID `json:"database"`
		Operation struct {
			Operation string `json:"operation"`
		} `json:"operation"`
	}
	if err != nil || json.Unmarshal(encoded, &action) != nil || action.Command != "maintenance" || action.Operation.Operation != "refs" || action.Database != transport.database || action.RequestID != requestID {
		transport.test.Fatal("status dispatched unexpected authority command")
	}
	transport.refsCalls++
	if transport.refsFailure != nil {
		return transport.refsFailure
	}
	result, ok := output.(*any)
	if !ok {
		transport.test.Fatal("status refs output type changed")
	}
	*result = map[string]any{"branches": map[string]any{}, "snapshots": map[string]any{}}
	return nil
}

func (transport *managementStatusTransport) Worker(_ context.Context, worker controller.Worker, requestID string, command any, output any) error {
	transport.test.Helper()
	encoded, err := json.Marshal(command)
	var action struct {
		Command  string                `json:"command"`
		Database controller.DatabaseID `json:"database"`
	}
	if err != nil || json.Unmarshal(encoded, &action) != nil || action.Command != "get_database_state" || action.Database != transport.database || worker != transport.worker || requestID == "" {
		transport.test.Fatal("status dispatched unexpected worker command")
	}
	transport.workerIDs = append(transport.workerIDs, requestID)
	if transport.workerFailure != nil {
		return transport.workerFailure
	}
	result, ok := output.(*controller.Actual)
	if !ok {
		transport.test.Fatal("status ownership state output type changed")
	}
	*result = transport.native
	return nil
}

func managementStatusFixture(test *testing.T) (*Management, *gatewayFixture, *managementStatusTransport) {
	test.Helper()
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	metadata, err := fixture.placements.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	placement := metadata.Placements[fixture.first.Key()]
	binding := placement.Grant.Binding
	transport := &managementStatusTransport{test: test, database: fixture.first, worker: metadata.Workers[placement.DesiredWorker],
		native: controller.Actual{Database: fixture.first, Mode: placement.Role, Binding: &binding, Epoch: placement.Epoch, RemainingTTLMS: placement.Grant.RemainingTTLMS}}
	management.config.Authority = &isolatedControlTransport{ControlTransport: transport, store: fixture.state}
	fixture.upstream.setHook(func(context.Context, controller.Worker, map[string]any) (any, error) {
		return nil, &SQLRPCError{Ambiguous: true}
	})
	return management, fixture, transport
}

func managementStatusUnchanged(test *testing.T, fixture *gatewayFixture, before State) {
	test.Helper()
	after, err := fixture.state.Snapshot()
	if err != nil || !reflect.DeepEqual(before, after) {
		test.Fatal("read-only status changed operations or durable state")
	}
	fixture.upstream.mu.Lock()
	calls := len(fixture.upstream.calls)
	fixture.upstream.mu.Unlock()
	if calls != 0 {
		test.Fatal("read-only status dispatched SQL upstream")
	}
}

func TestManagementStatusDeadWorkerDoesNotRegisterUnknown(test *testing.T) {
	for _, failure := range []error{errors.New("private-control-failure"), &controller.RPCError{Ambiguous: true}, &controller.RPCError{Rejected: true}} {
		management, fixture, transport := managementStatusFixture(test)
		transport.workerFailure = failure
		before, err := fixture.state.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-poll", Database: fixture.first}, "/v1/databases/status")
			if err != nil {
				test.Fatal("optional ownership state failure escaped status")
			}
			status := result.(map[string]any)
			if len(status) != 4 || status["worker"] == nil || status["refs"] == nil || status["placement"].(controller.Placement).Phase != "active" || status["diagnostics"].(map[string]string)["status"] != "unavailable" || status["diagnostics"].(map[string]string)["scope"] != "native_control_ownership_state" {
				test.Fatal("dead worker erased placement or claimed diagnostics success")
			}
			encoded, err := json.Marshal(result)
			if err != nil || strings.Contains(string(encoded), "private-control-failure") {
				test.Fatal("status exposed control error details")
			}
		}
		if transport.refsCalls != 2 || len(transport.workerIDs) != 2 || transport.workerIDs[0] == transport.workerIDs[1] || transport.workerIDs[0] == "status-poll" || transport.workerIDs[1] == "status-poll" {
			test.Fatal("status did not use independent read-only control request identities")
		}
		managementStatusUnchanged(test, fixture, before)
	}
}

func TestManagementStatusReadOnlyOwnershipScopeAndRefsFallback(test *testing.T) {
	management, fixture, transport := managementStatusFixture(test)
	transport.refsFailure = errors.New("private-refs-failure")
	before, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-read", Database: fixture.first}, "/v1/databases/status")
	if err != nil {
		test.Fatal(err)
	}
	status := result.(map[string]any)
	diagnostics := status["diagnostics"].(map[string]any)
	if diagnostics["status"] != "available" || diagnostics["scope"] != "native_control_ownership_state" || diagnostics["read_only"] != true || !reflect.DeepEqual(diagnostics["state"], transport.native) {
		test.Fatal("status misrepresented ownership state as SQL diagnostics")
	}
	if status["refs"].(map[string]string)["reason"] != "authority_inspection_failed" {
		test.Fatal("status refs fallback changed")
	}
	managementStatusUnchanged(test, fixture, before)
}

func TestManagementStatusRejectsMismatchedOwnershipState(test *testing.T) {
	for _, mismatch := range []string{"database", "binding_database", "binding_worker", "binding_epoch"} {
		test.Run(mismatch, func(test *testing.T) {
			management, fixture, transport := managementStatusFixture(test)
			switch mismatch {
			case "database":
				transport.native.Database = fixture.second
			case "binding_database":
				transport.native.Binding.Database = fixture.second
			case "binding_worker":
				transport.native.Binding.WorkerID = "other-worker"
			case "binding_epoch":
				transport.native.Binding.Epoch++
			}
			before, err := fixture.state.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-mismatch", Database: fixture.first}, "/v1/databases/status")
			if err != nil || result.(map[string]any)["diagnostics"].(map[string]string)["status"] != "unavailable" {
				test.Fatal("status accepted mismatched control state")
			}
			managementStatusUnchanged(test, fixture, before)
		})
	}
}

func TestManagementStatusPendingPlacementDoesNotDispatchWorker(test *testing.T) {
	management, fixture, transport := managementStatusFixture(test)
	placement := fixture.placements.metadata.Placements[fixture.first.Key()]
	placement.Phase, placement.Grant = "grant_pending", nil
	fixture.placements.metadata.Placements[fixture.first.Key()] = placement
	before, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-pending", Database: fixture.first}, "/v1/databases/status")
	if err != nil || result.(map[string]any)["placement"].(controller.Placement).Phase != "grant_pending" || len(transport.workerIDs) != 0 || result.(map[string]any)["diagnostics"].(map[string]string)["reason"] != "placement_not_active" {
		test.Fatal("pending status dispatched native ownership diagnostics")
	}
	managementStatusUnchanged(test, fixture, before)
}

func TestManagementStatusMissingWorkerDoesNotDispatchSQLOrControl(test *testing.T) {
	management, fixture, transport := managementStatusFixture(test)
	delete(fixture.placements.metadata.Workers, transport.worker.ID)
	before, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-missing-worker", Database: fixture.first}, "/v1/databases/status")
	if err != nil {
		test.Fatal(err)
	}
	status := result.(map[string]any)
	if len(status) != 4 || status["placement"].(controller.Placement).Phase != "active" || status["worker"].(controller.Worker).ID != "" || status["refs"] == nil ||
		status["diagnostics"].(map[string]string)["status"] != "unavailable" || status["diagnostics"].(map[string]string)["scope"] != "native_control_ownership_state" || len(transport.workerIDs) != 0 {
		test.Fatal("missing worker status changed shape or dispatched diagnostics")
	}
	managementStatusUnchanged(test, fixture, before)
}

func TestManagementStatusExplicitDiagnosticsStillIsolatesUnknown(test *testing.T) {
	management, fixture, transport := managementStatusFixture(test)
	failure := &SQLRPCError{Ambiguous: true}
	fixture.upstream.setHook(func(context.Context, controller.Worker, map[string]any) (any, error) {
		return nil, failure
	})
	request := httptest.NewRequest("POST", "/v1/databases/diagnostics", nil)
	_, err := management.dispatch(request, Principal{}, managementRequest{RequestID: "explicit-diagnostics", Database: fixture.first})
	if !errors.Is(err, failure) || !managementOutcomeUnknown(err) || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 {
		test.Fatal("explicit diagnostics Unknown or SQL cleanup contract changed")
	}
	before, err := fixture.state.Snapshot()
	if err != nil || len(before.Operations) != 1 {
		test.Fatal("explicit diagnostics lost its durable operation")
	}
	for _, record := range before.Operations {
		if record.State != "quarantined" || record.Action != "diagnostics" || record.BusinessOutcome != "unknown" || record.RetryAllowed {
			test.Fatal("explicit diagnostics lost durable Unknown isolation")
		}
	}
	result, err := management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-isolated", Database: fixture.first}, "/v1/databases/status")
	if err != nil || result.(map[string]any)["operations"] == nil || result.(map[string]any)["placement"] != nil || transport.refsCalls != 0 || len(transport.workerIDs) != 0 {
		test.Fatal("status bypassed preexisting Unknown isolation")
	}
	after, err := fixture.state.Snapshot()
	if err != nil || !reflect.DeepEqual(before, after) || fixture.upstream.count("open_session") != 1 {
		test.Fatal("isolated status changed operations or dispatched SQL")
	}
}

func TestManagementStatusAuthorizationPrecedesControlReads(test *testing.T) {
	management, fixture, transport := managementStatusFixture(test)
	management.config.Identity.(*nativeDiagnosticIdentity).failure = errors.New("forbidden")
	before, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	_, err = management.inspect(context.Background(), Principal{}, managementRequest{RequestID: "status-denied", Database: fixture.first}, "/v1/databases/status")
	if err == nil || transport.refsCalls != 0 || len(transport.workerIDs) != 0 {
		test.Fatal("unauthorized status dispatched control reads")
	}
	managementStatusUnchanged(test, fixture, before)
}
