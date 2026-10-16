package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// contractEvidence is what a reply may claim about execution: the worker proved
// the statement settled, proved it never entered execution, or proved nothing.
type contractEvidence int

const (
	noProof contractEvidence = iota
	provenCompleted
	provenRejected
)

func unknownContractResponse(test *testing.T, response *httptest.ResponseRecorder, requestID, code string, evidence contractEvidence) {
	test.Helper()
	var reply struct {
		RequestID string          `json:"request_id"`
		OK        bool            `json:"ok"`
		Result    json.RawMessage `json:"result"`
		Error     struct {
			Code               string `json:"code"`
			ExecutionCompleted *bool  `json:"execution_completed"`
			MayHaveCommitted   *bool  `json:"may_have_committed"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
		test.Fatal(err)
	}
	if reply.RequestID != requestID || reply.OK || string(reply.Result) != "null" || reply.Error.Code != code {
		test.Fatalf("incorrect correlated failure: %s", response.Body.String())
	}
	switch evidence {
	case provenCompleted:
		if reply.Error.ExecutionCompleted == nil || !*reply.Error.ExecutionCompleted || reply.Error.MayHaveCommitted == nil || !*reply.Error.MayHaveCommitted {
			test.Fatalf("terminal SQL failure lost flags: %s", response.Body.String())
		}
	case provenRejected:
		if reply.Error.ExecutionCompleted == nil || *reply.Error.ExecutionCompleted || reply.Error.MayHaveCommitted == nil || *reply.Error.MayHaveCommitted {
			test.Fatalf("rejected SQL failure lost its proof of non-execution: %s", response.Body.String())
		}
	default:
		if reply.Error.ExecutionCompleted != nil || reply.Error.MayHaveCommitted != nil {
			test.Fatalf("unproven failure gained terminal proof: %s", response.Body.String())
		}
	}
}

func unknownContractAdmission(test *testing.T, fixture *gatewayFixture, active, sessions, unknown int) {
	test.Helper()
	projects, totalActive, totalUnknown := fixture.gateway.admission.status()
	project := projects["project-a"].(map[string]any)
	if totalActive != active || totalUnknown != unknown || project["sessions"] != sessions || project["control_active"] != 0 {
		test.Fatalf("admission changed: active=%d unknown=%d project=%v", totalActive, totalUnknown, project)
	}
}

func TestGatewayUnknownContractOpenFailureMatrix(test *testing.T) {
	for _, scenario := range []struct {
		name     string
		failure  error
		status   int
		code     string
		evidence contractEvidence
		retained bool
	}{
		{"untyped", errors.New("synthetic private cause"), 502, "outcome_unknown", noProof, true},
		{"wrapped_ambiguous_completed_rejected", fmt.Errorf("synthetic wrapper: %w", testRPCError(true, true, SQLRPCError{Ambiguous: true})), 502, "outcome_unknown", noProof, true},
		{"wrapped_completed", fmt.Errorf("synthetic wrapper: %w", testRPCError(true, false, SQLRPCError{})), 400, "sql_error", provenCompleted, false},
		{"wrapped_rejected", fmt.Errorf("synthetic wrapper: %w", testRPCError(false, true, SQLRPCError{})), 502, "upstream_rejected", provenRejected, false},
		{"wrapped_unavailable", fmt.Errorf("synthetic wrapper: %w", &SQLRPCError{}), 503, "upstream_unavailable", noProof, false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			observer := NewSQLOpenObserver()
			fixture.gateway.config.OpenObserver = observer
			if err := fixture.state.Update(func(state *State) error {
				project := state.Projects["project-a"]
				project.Quotas.MaxSessions = 1
				state.Projects[project.ID] = project
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			fixture.upstream.setHook(func(context.Context, controller.Worker, map[string]any) (any, error) {
				return nil, scenario.failure
			})
			requestID := "contract_open_" + scenario.name
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first, "request_id": requestID})
			if response.Code != scenario.status {
				test.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			unknownContractResponse(test, response, requestID, scenario.code, scenario.evidence)
			fixture.audit.mu.Lock()
			event := fixture.audit.events[len(fixture.audit.events)-1]
			fixture.audit.mu.Unlock()
			if event.RequestID != requestID || event.Action != "open" || event.Outcome != scenario.code || strings.Contains(response.Body.String(), "synthetic") {
				test.Fatal("Open audit lost correlation or response leaked private cause")
			}
			fixture.gateway.mu.Lock()
			registered := len(fixture.gateway.sessions)
			fixture.gateway.mu.Unlock()
			if registered != 0 || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 {
				test.Fatal("failed Open registered, retried, or auto-closed a session")
			}
			held := 0
			if scenario.retained {
				held = 1
			}
			unknownContractAdmission(test, fixture, held, held, held)
			if scenario.retained {
				response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
				if response.Code != 429 || fixture.upstream.count("open_session") != 1 {
					test.Fatal("unknown Open released session quota for another Open")
				}
				if err := fixture.gateway.Close(context.Background()); err == nil || fixture.upstream.count("close_session") != 0 {
					test.Fatal("shutdown treated unknown Open as terminal")
				}
				unknownContractAdmission(test, fixture, 1, 1, 1)
			}
			records := sqlOpenTestRecords(observer)
			if len(records) != 1 || records[0].RequestID != requestID || records[0].CallSequence != 1 || records[0].Classification != scenario.code || records[0].Reason != "upstream_classification" {
				test.Fatalf("Open observation lost correlation: %+v", records)
			}
			var rpc *SQLRPCError
			isRPC := errors.As(scenario.failure, &rpc)
			if records[0].RPCError != isRPC || isRPC && (records[0].Ambiguous != rpc.Ambiguous || records[0].Rejected != rpc.evidence.Rejected() || records[0].ExecutionCompleted != rpc.evidence.Executed()) {
				test.Fatalf("wrapped error flags lost: record=%+v rpc=%+v", records[0], rpc)
			}
			fixture.upstream.mu.Lock()
			actualID := fixture.upstream.calls[0].requestID
			fixture.upstream.mu.Unlock()
			if actualID != requestID || strings.Contains(response.Body.String(), "synthetic") {
				test.Fatal("request ID changed or private cause leaked")
			}
		})
	}
}

func TestGatewayUnknownContractSessionTerminalProof(test *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		test.Run(fmt.Sprintf("ambiguous_%t", ambiguous), func(test *testing.T) {
			fixture, writer := newWriterGatewayFixture(test)
			public := writerGatewaySession(test, fixture)
			fixture.gateway.mu.Lock()
			session := fixture.gateway.sessions[public]
			fixture.gateway.mu.Unlock()
			writer.hook = func(command map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
				if command["sql"] == "COMMIT" {
					return state, fmt.Errorf("synthetic wrapper: %w", testRPCError(true, false, SQLRPCError{Ambiguous: ambiguous}))
				}
				return state, err
			}
			before := fixture.upstream.count("execute")
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": public, "sql": "COMMIT", "request_id": "contract_commit"})
			status, code, held, evidence := 400, "sql_error", 0, provenCompleted
			if ambiguous {
				status, code, held, evidence = 502, "outcome_unknown", 1, noProof
			}
			if response.Code != status {
				test.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			unknownContractResponse(test, response, "contract_commit", code, evidence)
			unknownContractAdmission(test, fixture, held, 1, held)
			session.mu.Lock()
			poisoned, closed, owner := session.poisoned, session.closed, session.writerOwner
			session.mu.Unlock()
			if poisoned != ambiguous || closed || !owner || fixture.upstream.count("execute") != before+1 {
				test.Fatal("terminal classification retried SQL, removed session, or released owner")
			}
			if ambiguous {
				for _, action := range []string{"execute", "query", "close"} {
					body := map[string]any{"session": public}
					if action != "close" {
						body["sql"] = "ROLLBACK"
					}
					response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/"+action, body)
					if response.Code != 502 || gatewayCode(test, response) != "outcome_unknown" {
						test.Fatalf("poisoned session action=%s status=%d body=%s", action, response.Code, response.Body.String())
					}
				}
				if fixture.gateway.Close(context.Background()) == nil || fixture.upstream.count("close_session") != 0 || fixture.upstream.count("query") != 0 || fixture.upstream.count("execute") != before+1 {
					test.Fatal("unknown was retried or auto-cleaned")
				}
				unknownContractAdmission(test, fixture, 1, 1, 1)
			} else {
				response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": public, "sql": "ROLLBACK"})
				if response.Code != 200 || fixture.upstream.count("execute") != before+2 {
					test.Fatal("completed SQL error prevented explicit acknowledged rollback")
				}
				unknownContractAdmission(test, fixture, 0, 1, 0)
			}
		})
	}
}

func TestGatewayUnknownContractCheckpointNoCleanup(test *testing.T) {
	for _, stage := range []string{"open_session", "checkpoint"} {
		test.Run(stage, func(test *testing.T) {
			management, fixture, _, _ := nativeDiagnosticManagement(test)
			// A reply that proves nothing is the only one this path can carry, so the
			// diagnostic must report both directions as unproven.
			failure := &SQLRPCError{Ambiguous: true}
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == stage {
					return nil, fmt.Errorf("synthetic wrapper: %w", failure)
				}
				return nil, nil
			})
			_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "contract_checkpoint", Database: fixture.first}, "/v1/databases/checkpoint")
			var rpc *SQLRPCError
			if !errors.As(err, &rpc) || rpc != failure {
				test.Fatalf("Checkpoint error chain lost flags: %v", err)
			}
			var toolFailure *nativeToolError
			expectedStage := "open_session"
			if stage == "checkpoint" {
				expectedStage = "tool_rpc"
			}
			if !errors.As(err, &toolFailure) || toolFailure.stage != expectedStage {
				test.Fatalf("Checkpoint failure stage lost: %v", err)
			}
			record := nativeToolRecord("/v1/databases/checkpoint", toolFailure.stage, "active", "contract_checkpoint", err)
			if record.RequestID != "contract_checkpoint" || !record.RPCError || !record.Ambiguous || record.Rejected || record.ExecutionCompleted {
				test.Fatalf("Checkpoint diagnostic flags lost: %+v", record)
			}
			expectedCheckpoint := 0
			if stage == "checkpoint" {
				expectedCheckpoint = 1
			}
			if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("checkpoint") != expectedCheckpoint {
				test.Fatal("unknown Checkpoint retried Open or tool RPC")
			}
			fixture.upstream.mu.Lock()
			for _, call := range fixture.upstream.calls {
				expectedID := "contract_checkpoint"
				if call.command["command"] == "open_session" {
					expectedID += "-open"
				} else if call.command["command"] == "close_session" {
					expectedID += "-close"
				}
				if call.requestID != expectedID {
					test.Errorf("Checkpoint request ID changed: %+v", call)
				}
			}
			fixture.upstream.mu.Unlock()
			if count := fixture.upstream.count("close_session"); count != 0 {
				test.Fatalf("unknown Checkpoint auto-closed possibly in-flight session: close_session calls=%d", count)
			}
		})
	}
}

func TestGatewayUnknownContractCheckpointCleanupMatrix(test *testing.T) {
	privateCause := errors.New("synthetic private Close cause")
	closeRPC := &SQLRPCError{Ambiguous: true, cause: privateCause}
	wrappedClose := fmt.Errorf("synthetic private wrapper: %w", closeRPC)
	completedRPC := testRPCError(true, false, SQLRPCError{cause: privateCause})
	wrappedCompleted := fmt.Errorf("synthetic private completed: %w", completedRPC)
	for _, scenario := range []struct {
		name       string
		closeReply any
		closeError error
		toolError  error
		stage      string
		closeCalls int
	}{
		{"success", map[string]bool{"closed": true}, nil, nil, "", 1},
		{"missing_close_ack", map[string]any{}, nil, nil, "close_session", 1},
		{"false_close_ack", map[string]bool{"closed": false}, nil, nil, "close_session", 1},
		{"wrapped_close_error", nil, wrappedClose, nil, "close_session", 1},
		{"untyped_close_error", nil, privateCause, nil, "close_session", 1},
		{"completed_tool_error_no_close", nil, nil, wrappedCompleted, "tool_rpc", 0},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			management, fixture, _, _ := nativeDiagnosticManagement(test)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "checkpoint":
					if scenario.toolError != nil {
						return nil, scenario.toolError
					}
					return map[string]bool{"checkpointed": true}, nil
				case "close_session":
					return scenario.closeReply, scenario.closeError
				default:
					return nil, nil
				}
			})
			requestID := "contract_cleanup_" + scenario.name
			output, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: requestID, Database: fixture.first}, "/v1/databases/checkpoint")
			if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("checkpoint") != 1 || fixture.upstream.count("close_session") != scenario.closeCalls {
				test.Fatal("Checkpoint or cleanup was retried, omitted, or issued after tool failure")
			}
			fixture.upstream.mu.Lock()
			calls := append([]gatewayCall(nil), fixture.upstream.calls...)
			fixture.upstream.mu.Unlock()
			for index, call := range calls {
				expectedCommand, expectedID := "checkpoint", requestID
				if index == 0 {
					expectedCommand, expectedID = "open_session", requestID+"-open"
				} else if index == 2 {
					expectedCommand, expectedID = "close_session", requestID+"-close"
				}
				if call.command["command"] != expectedCommand || call.requestID != expectedID || index > 0 && call.command["session"] != fmt.Sprintf("%064x", 1) {
					test.Fatalf("cleanup command/session/correlation changed: %+v", call)
				}
			}
			if scenario.stage == "" {
				encoded, marshalErr := json.Marshal(output)
				if err != nil || marshalErr != nil || string(encoded) != `{"checkpointed":true}` || management.nativeDiagnostics.count != 0 {
					test.Fatalf("successful Checkpoint lost result or gained failure: output=%s error=%v", encoded, err)
				}
				return
			}
			var toolFailure *nativeToolError
			if output != nil || !errors.As(err, &toolFailure) || toolFailure.stage != scenario.stage || management.nativeDiagnostics.count != 1 {
				test.Fatalf("failure retained success output or lost stage: output=%v error=%v", output, err)
			}
			injected := scenario.closeError
			if scenario.toolError != nil {
				injected = scenario.toolError
			}
			var rpc *SQLRPCError
			isRPC := errors.As(err, &rpc)
			if injected != nil {
				if !errors.Is(err, injected) || !errors.Is(err, privateCause) {
					test.Fatal("cleanup/tool failure replaced original error chain")
				}
			} else if !isRPC || !rpc.Ambiguous || rpc.evidence.Executed() || rpc.evidence.Rejected() {
				test.Fatalf("missing Close ACK gained terminal proof: %v", err)
			}
			if scenario.closeError == wrappedClose && rpc != closeRPC || scenario.toolError == wrappedCompleted && rpc != completedRPC {
				test.Fatal("wrapped RPC flags were replaced")
			}
			record := nativeToolRecord("/v1/databases/checkpoint", toolFailure.stage, "active", requestID, err)
			if record.RPCError != isRPC || isRPC && (record.Ambiguous != rpc.Ambiguous || record.Rejected != rpc.evidence.Rejected() || record.ExecutionCompleted != rpc.evidence.Executed()) {
				test.Fatalf("diagnostic lost original flags: %+v", record)
			}
			var diagnostic bytes.Buffer
			budget := nativeDiagnosticBudget{}
			budget.emit(record, &diagnostic)
			record.Event = "management_native_tool_error"
			var emitted nativeDiagnosticRecord
			if decodeErr := json.Unmarshal(diagnostic.Bytes(), &emitted); decodeErr != nil || emitted != record || budget.count != 1 || strings.Contains(diagnostic.String(), "synthetic private") {
				test.Fatalf("cleanup diagnostic rejected stage, changed correlation/flags, or leaked cause: %s", diagnostic.String())
			}
		})
	}
}

func TestGatewayUnknownContractCanceledOpenRetainsPermit(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.gateway.config.OpenObserver = NewSQLOpenObserver()
	entered, returned := make(chan struct{}), make(chan struct{})
	fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] != "open_session" {
			return nil, nil
		}
		close(entered)
		<-ctx.Done()
		return nil, fmt.Errorf("synthetic cancellation: %w", &SQLRPCError{Ambiguous: true, cause: ctx.Err()})
	})
	body, err := json.Marshal(map[string]any{"database": fixture.first, "request_id": "contract_canceled_open"})
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest("POST", "/v1/sql/open", strings.NewReader(string(body))).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	response := httptest.NewRecorder()
	go func() {
		defer close(returned)
		fixture.gateway.ServeHTTP(response, request)
	}()
	defer func() {
		cancel()
		select {
		case <-returned:
		case <-time.After(time.Second):
			test.Error("canceled mock did not return")
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		test.Fatal("Open did not enter controlled upstream")
	}
	unknownContractAdmission(test, fixture, 1, 1, 0)
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		test.Fatal("canceled Open did not return")
	}
	if response.Code != 502 {
		test.Fatalf("cancellation gained terminal proof: %d %s", response.Code, response.Body.String())
	}
	unknownContractResponse(test, response, "contract_canceled_open", "outcome_unknown", noProof)
	unknownContractAdmission(test, fixture, 1, 1, 1)
	if fixture.gateway.Close(context.Background()) == nil || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 {
		test.Fatal("cancellation released/retried/cleaned unknown Open")
	}
	unknownContractAdmission(test, fixture, 1, 1, 1)
}

func TestGatewayUnknownContractCompletedWithoutWriterProof(test *testing.T) {
	for _, proof := range []string{"missing", "stale"} {
		test.Run(proof, func(test *testing.T) {
			fixture, writer := newWriterGatewayFixture(test)
			public := writerGatewaySession(test, fixture)
			writer.hook = func(command map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
				if command["sql"] != "COMMIT" {
					return state, err
				}
				completed := fmt.Errorf("synthetic completed: %w", testRPCError(true, false, SQLRPCError{}))
				if proof == "missing" {
					return nil, completed
				}
				// The proof this worker answers is the one the open already produced:
				// this session has passed it, whatever else was dispatched in between.
				state.Sequence = 0
				return state, completed
			}
			before := fixture.upstream.count("execute")
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": public, "sql": "COMMIT", "request_id": "contract_missing_proof"})
			if response.Code != 502 {
				test.Fatalf("untrusted writer proof accepted: %d %s", response.Code, response.Body.String())
			}
			unknownContractResponse(test, response, "contract_missing_proof", "outcome_unknown", noProof)
			unknownContractAdmission(test, fixture, 1, 1, 1)
			fixture.gateway.mu.Lock()
			session := fixture.gateway.sessions[public]
			fixture.gateway.mu.Unlock()
			session.mu.Lock()
			poisoned, closed, owner := session.poisoned, session.closed, session.writerOwner
			session.mu.Unlock()
			if !poisoned || closed || !owner || fixture.upstream.count("execute") != before+1 || fixture.upstream.count("close_session") != 0 {
				test.Fatal("completed flag without writer proof released or retried session")
			}
			projects, _, _ := fixture.gateway.admission.status()
			if projects["project-a"].(map[string]any)["owner_active"] != 1 {
				test.Fatal("unknown owner permit was released")
			}
		})
	}
}

// A terminal SQL failure on an action that cannot commit must report
// may_have_committed=false: the client uses that flag to decide whether it has
// to reconcile durable state, and describe plus read cursors are refused by the
// worker before any write can land.
func TestGatewayNonCommittingFailureReportsNoCommit(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, fields map[string]any) (any, error) {
		if fields["command"] == "open_session" {
			return nil, nil
		}
		return nil, testRPCError(true, false, SQLRPCError{})
	})
	for _, scenario := range []struct {
		name       string
		path       string
		body       map[string]any
		committing bool
	}{
		{"describe", "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT 1"}, false},
		{"read_cursor_open", "/v1/sql/cursor/open", map[string]any{"session": session, "sql": "SELECT 1", "args": []any{}, "named_args": []any{}}, false},
		{"read_cursor_fetch", "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 1}, false},
		{"read_cursor_close", "/v1/sql/cursor/close", map[string]any{"session": session}, false},
		{"execute", "/v1/sql/execute", map[string]any{"session": session, "sql": "SELECT 1"}, true},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			requestID := "contract_noncommitting_" + scenario.name
			body := map[string]any{}
			for field, value := range scenario.body {
				body[field] = value
			}
			body["request_id"] = requestID
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, scenario.path, body)
			if response.Code != 400 {
				test.Fatalf("terminal failure status=%d body=%s", response.Code, response.Body.String())
			}
			var reply struct {
				Error struct {
					Code               string `json:"code"`
					ExecutionCompleted *bool  `json:"execution_completed"`
					MayHaveCommitted   *bool  `json:"may_have_committed"`
				} `json:"error"`
			}
			if json.Unmarshal(response.Body.Bytes(), &reply) != nil || reply.Error.Code != "sql_error" {
				test.Fatalf("incorrect terminal failure: %s", response.Body.String())
			}
			if reply.Error.ExecutionCompleted == nil || !*reply.Error.ExecutionCompleted {
				test.Fatalf("terminal SQL failure lost completion proof: %s", response.Body.String())
			}
			if reply.Error.MayHaveCommitted == nil || *reply.Error.MayHaveCommitted != scenario.committing {
				test.Fatalf("may_have_committed want=%t: %s", scenario.committing, response.Body.String())
			}
		})
	}
}
