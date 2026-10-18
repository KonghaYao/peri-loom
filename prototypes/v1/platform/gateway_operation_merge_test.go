package platform

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// securityGatewayFixture builds a gateway over the durable registry so that the
// operation sidecar revisions one request costs can be counted.
func securityGatewayFixture(test *testing.T) (StateStore, *Gateway, *gatewayTestUpstream, *gatewayTestAudit, string) {
	test.Helper()
	store, identity, _, credential := identityFixture(test, "sql")
	if err := store.Update(func(state *State) error {
		project := state.Projects["alpha"]
		project.Quotas = Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 1 << 20}
		state.Projects["alpha"] = project
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	database := securityDatabase()
	binding := securityBinding()
	worker := controller.Worker{ID: binding.WorkerID, Address: "http://127.0.0.1:1234", Healthy: true, Capacity: 8, Load: 1, LastHeartbeatMS: time.Now().UnixMilli()}
	placements := &gatewayTestPlacements{metadata: controller.Metadata{Version: 1, Workers: map[string]controller.Worker{worker.ID: worker}, Placements: map[string]controller.Placement{}}}
	placements.metadata.Placements[database.Key()] = controller.Placement{Database: database, DesiredWorker: worker.ID, Epoch: 10, LeaseID: binding.LeaseID, Role: "writer", DesiredState: "active", Phase: "active", Grant: &controller.Grant{Binding: binding, RemainingTTLMS: 10000}}
	upstream := &gatewayTestUpstream{}
	audit := &gatewayTestAudit{}
	upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "describe":
			sql, _ := command["sql"].(string)
			return testDescribeVerdict(sql), nil
		case "open_read_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_read_cursor":
			return map[string]any{"rows": [][]any{{1}}, "done": true}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		case "statement":
			return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 1, "last_insert_rowid": nil, "autocommit": true}, nil
		}
		return nil, nil
	})
	gateway, err := NewGateway(GatewayConfig{State: store, Placements: placements, Identity: identity, Upstream: upstream, Audit: audit})
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Close(ctx)
	})
	return store, gateway, upstream, audit, credential
}

func (audit *gatewayTestAudit) actions() []string {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	actions := make([]string, 0, len(audit.events))
	for _, event := range audit.events {
		actions = append(actions, event.Action)
	}
	return actions
}

func sidecarRevision(store StateStore) uint64 {
	return store.(*stateStore).operations.revision
}

// A read cursor, a describe and a statement the worker proved read-only cannot
// commit, so their audit row is the only durable record and the operation sidecar
// stays untouched. A writable statement still pays registration plus settlement.
func TestNonCommittingOperationKeepsNoSidecarRecord(test *testing.T) {
	store, gateway, upstream, audit, credential := securityGatewayFixture(test)
	database := securityDatabase()
	var opened struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": database})
	if response.Code != 200 {
		test.Fatalf("open failed: %d %s", response.Code, response.Body.String())
	}
	if json.Unmarshal(response.Body.Bytes(), &opened) != nil || !publicSessionID(opened.Result.Session) {
		test.Fatalf("invalid public session: %s", response.Body.String())
	}
	session := opened.Result.Session
	requests := []struct {
		name   string
		path   string
		body   map[string]any
		action string
	}{
		{"describe", "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT id FROM records"}, "describe"},
		{"read cursor open", "/v1/sql/cursor/open", map[string]any{"session": session, "sql": "SELECT id FROM records", "args": []any{}, "named_args": []any{}, "write_cursor": false}, "cursor_open"},
		{"read cursor fetch", "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 1, "write_cursor": false}, "cursor_fetch"},
		{"read cursor close", "/v1/sql/cursor/close", map[string]any{"session": session, "write_cursor": false}, "cursor_close"},
	}
	for _, request := range requests {
		before := sidecarRevision(store)
		response := gatewayHTTP(test, gateway, credential, request.path, request.body)
		if response.Code != 200 {
			test.Fatalf("%s failed: %d %s", request.name, response.Code, response.Body.String())
		}
		if grew := sidecarRevision(store) - before; grew != 0 {
			test.Fatalf("%s cost %d operation sidecar revisions, want 0", request.name, grew)
		}
		state, err := store.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		for _, record := range state.Operations {
			if record.Action == request.action {
				test.Fatalf("%s kept a sidecar record: %+v", request.name, record.OperationStatus)
			}
		}
		if outcomes := audit.outcomesOf(request.action); len(outcomes) != 1 || outcomes[0] != "ok" {
			test.Fatalf("%s lost its durable audit row: %v", request.name, outcomes)
		}
	}
	before := sidecarRevision(store)
	if response := gatewayHTTP(test, gateway, credential, "/v1/sql/statement", map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(1)", "args": []any{}, "named_args": []any{}}); response.Code != 200 {
		test.Fatalf("statement failed: %d %s", response.Code, response.Body.String())
	}
	if grew := sidecarRevision(store) - before; grew != 2 {
		test.Fatalf("writable statement cost %d operation sidecar revisions, want registration plus settlement", grew)
	}
	if upstream.count("statement") != 1 {
		test.Fatal("statement was not dispatched exactly once")
	}
}

// An isolated database refuses a non-committing operation before SQL reaches
// the worker, even though its record is written after the call.
func TestNonCommittingOperationRefusedWhileDatabaseIsIsolated(test *testing.T) {
	store, gateway, upstream, _, credential := securityGatewayFixture(test)
	database := securityDatabase()
	var opened struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/open", map[string]any{"database": database})
	if response.Code != 200 {
		test.Fatalf("open failed: %d %s", response.Code, response.Body.String())
	}
	if json.Unmarshal(response.Body.Bytes(), &opened) != nil || !publicSessionID(opened.Result.Session) {
		test.Fatalf("invalid public session: %s", response.Body.String())
	}
	session := opened.Result.Session
	if err := beginOperation(store, database, "isolated-probe", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "isolated-probe", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	dispatchBefore := countUpstreamCalls(upstream)
	response = gatewayHTTP(test, gateway, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": "SELECT id FROM records"})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("isolated describe admitted: %d %s", response.Code, response.Body.String())
	}
	if countUpstreamCalls(upstream) != dispatchBefore {
		test.Fatal("isolated describe reached the worker")
	}
}

func countUpstreamCalls(upstream *gatewayTestUpstream) int {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	return len(upstream.calls)
}
