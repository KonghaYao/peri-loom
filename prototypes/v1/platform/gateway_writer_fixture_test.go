package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	controller "peri-loom/controller"
)

type gatewayWriterUpstream struct {
	base         *gatewayTestUpstream
	placements   *gatewayTestPlacements
	mu           sync.Mutex
	states       map[string]SessionWriterState
	hook         func(map[string]any, *SessionWriterState, error) (*SessionWriterState, error)
	writerCalls  int
	profileError error
	profileCalls int
}

func (upstream *gatewayWriterUpstream) WriterProfile(context.Context, controller.Worker, string) error {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	upstream.profileCalls++
	return upstream.profileError
}

func (upstream *gatewayWriterUpstream) Call(ctx context.Context, worker controller.Worker, requestID string, command any, output any) error {
	return upstream.base.Call(ctx, worker, requestID, command, output)
}

func (upstream *gatewayWriterUpstream) CallWriter(ctx context.Context, worker controller.Worker, requestID string, command any, output any) (*SessionWriterState, error) {
	encoded, _ := json.Marshal(command)
	var fields map[string]any
	_ = json.Unmarshal(encoded, &fields)
	err := upstream.base.Call(ctx, worker, requestID, command, output)
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	upstream.writerCalls++
	var state SessionWriterState
	if fields["command"] == "open_session" && err == nil {
		var reply struct {
			Session string `json:"session"`
		}
		encoded, _ = json.Marshal(output)
		_ = json.Unmarshal(encoded, &reply)
		var database controller.DatabaseID
		encoded, _ = json.Marshal(fields["database"])
		_ = json.Unmarshal(encoded, &database)
		metadata, _ := upstream.placements.Snapshot()
		state = SessionWriterState{Session: reply.Session, Binding: metadata.Placements[database.Key()].Grant.Binding}
	} else {
		state = upstream.states[fields["session"].(string)]
		state.Sequence++
		if fields["sql"] == "acquire-writer" {
			state.Owner = true
		}
		if fields["sql"] == "ROLLBACK" || fields["command"] == "close_session" {
			state.Owner = false
		}
	}
	upstream.states[state.Session] = state
	if upstream.hook != nil {
		return upstream.hook(fields, &state, err)
	}
	var failure *SQLRPCError
	if err != nil && (!errors.As(err, &failure) || failure.Ambiguous || !failure.ExecutionCompleted) {
		return nil, err
	}
	return &state, err
}

func newWriterGatewayFixture(test *testing.T) (*gatewayFixture, *gatewayWriterUpstream) {
	test.Helper()
	fixture := newGatewayFixture(test)
	if err := fixture.state.Update(func(state *State) error {
		project := state.Projects["project-a"]
		project.Quotas.MaxConcurrent = 4
		state.Projects[project.ID] = project
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	upstream := &gatewayWriterUpstream{base: fixture.upstream, placements: fixture.placements, states: map[string]SessionWriterState{}}
	fixture.gateway.config.Upstream = upstream
	fixture.gateway.config.WriterAdmission = true
	fixture.gateway.admission.writerAdmission = true
	return fixture, upstream
}

func writerGatewaySession(test *testing.T, fixture *gatewayFixture) string {
	test.Helper()
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "acquire-writer"})
	if response.Code != 200 {
		test.Fatalf("owner acquisition: %s", response.Body.String())
	}
	return session
}
