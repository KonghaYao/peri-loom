package platform

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestGatewayTenantProjectSubjectAndOpaqueIsolation(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	for _, attempt := range []struct {
		token string
		path  string
		body  any
	}{
		{fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.second}},
		{fixture.tokenB, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"}},
		{fixture.viewer, "/v1/sql/open", map[string]any{"database": fixture.first}},
		{fixture.tokenA, "/v1/sql/query", map[string]any{"session": strings.Repeat("0", 32), "sql": "SELECT 1"}},
		{fixture.tokenA, "/v1/sql/query", map[string]any{"session": strings.Repeat("0", 64), "sql": "SELECT 1"}},
		{fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "command": "ownership"}},
	} {
		response := gatewayHTTP(test, fixture.gateway, attempt.token, attempt.path, attempt.body)
		if response.Code == 200 {
			test.Fatal("unauthorized dispatch accepted")
		}
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("query") != 0 {
		test.Fatal("rejected action reached SQL worker")
	}
	_, sameProject, err := NewAPIKey(fixture.state, "other-subject", "project-a", "sql", false)
	if err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, sameProject, "/v1/sql/close", map[string]any{"session": session})
	if gatewayCode(test, response) != "invalid_session" {
		test.Fatal("same-project other subject stole handle")
	}
}

func TestGatewayReauthRevocationAndExpiryEveryDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	expires := time.Now().Add(200 * time.Millisecond).UnixMilli()
	token, err := fixture.identity.IssueToken(principal, fixture.first, "writer", expires)
	if err != nil {
		test.Fatal(err)
	}
	session := gatewayOpen(test, fixture, token, fixture.first)
	time.Sleep(time.Until(time.UnixMilli(expires)) + time.Millisecond)
	response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 401 || fixture.upstream.count("query") != 0 {
		test.Fatal("expired token reused cached authorization")
	}
	session = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	if err := RevokeAPIKey(fixture.state, fixture.keyA.ID); err != nil {
		test.Fatal(err)
	}
	for _, action := range []string{"execute", "query", "close"} {
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/"+action, map[string]any{"session": session, "sql": "COMMIT"})
		if response.Code != 401 {
			test.Fatal("revoked issuer allowed dispatch")
		}
	}
	if fixture.upstream.count("execute")+fixture.upstream.count("query")+fixture.upstream.count("close_session") != 0 {
		test.Fatal("revoked credential reached native worker")
	}
}

func TestGatewayMigrationInvalidatesOldSessionAndFreshOpenRoutesNewWorker(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Phase = "migrate_drain"
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	})
	if err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "placement_unavailable" || fixture.upstream.count("query") != 0 {
		test.Fatal("old session dispatched during migration")
	}
	err = fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		worker := metadata.Workers[placement.DesiredWorker]
		worker.ID = "worker-new"
		worker.Address = "http://127.0.0.1:5678"
		metadata.Workers[worker.ID] = worker
		placement.DesiredWorker = worker.ID
		placement.Epoch = 2
		placement.LeaseID = "lease-2"
		placement.Phase = "active"
		placement.Grant.Binding.WorkerID = worker.ID
		placement.Grant.Binding.Epoch = 2
		placement.Grant.Binding.LeaseID = "lease-2"
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	})
	if err != nil {
		test.Fatal(err)
	}
	fresh := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	if fresh == session {
		test.Fatal("session automatically rebound")
	}
	fixture.upstream.mu.Lock()
	last := fixture.upstream.calls[len(fixture.upstream.calls)-1]
	fixture.upstream.mu.Unlock()
	if last.worker.ID != "worker-new" {
		test.Fatal("fresh open routed stale worker")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "session_moved" {
		test.Fatal("old handle revived after migration")
	}
}

func TestGatewayReadonlyRequiresWholeNativeReadonlyPlacement(test *testing.T) {
	fixture := newGatewayFixture(test)
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	token, err := fixture.identity.IssueToken(principal, fixture.first, "read_only", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/open", map[string]any{"database": fixture.first})
	if gatewayCode(test, response) != "readonly_placement_required" || fixture.upstream.count("open_session") != 0 {
		test.Fatal("readonly token admitted to Writer placement")
	}
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Role = "read_only"
		placement.Grant.Binding.Role = "read_only"
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	fixture.upstream.mu.Lock()
	fixture.upstream.readonly = true
	fixture.upstream.mu.Unlock()
	session := gatewayOpen(test, fixture, token, fixture.first)
	response = gatewayHTTP(test, fixture.gateway, token, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 200 {
		test.Fatal("native readonly query denied")
	}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "execute" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	response = gatewayHTTP(test, fixture.gateway, token, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO t VALUES(1); PRAGMA journal_mode=WAL;"})
	if response.Code == 200 || fixture.upstream.count("execute") != 1 {
		test.Fatal("gateway rewrote or falsely accepted native refusal")
	}
}

func TestGatewayDBTokenPlacementIncarnationCheckClosesStateUpdateWindow(test *testing.T) {
	fixture := newGatewayFixture(test)
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	token, err := fixture.identity.IssueToken(principal, fixture.first, "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	session := gatewayOpen(test, fixture, token, fixture.first)
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Grant.Binding.BranchIncarnation++
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	for _, action := range []string{"open", "execute", "query", "close"} {
		body := map[string]any{"session": session, "sql": "SELECT 1"}
		if action == "open" {
			body = map[string]any{"database": fixture.first}
		} else if action == "close" {
			delete(body, "sql")
		}
		response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/"+action, body)
		if gatewayCode(test, response) != "token_binding_mismatch" {
			test.Fatalf("%s used stale token incarnation", action)
		}
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("query")+fixture.upstream.count("execute") != 0 {
		test.Fatal("stale token dispatched through restore window")
	}
}

func TestGatewayAuditAfterCommitFailurePoisonsAndRedacts(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.audit.mu.Lock()
	fixture.audit.failAction = "execute"
	fixture.audit.mu.Unlock()
	privateSQL := "COMMIT; private SQL marker"
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": privateSQL, "request_id": "audit-commit"})
	if gatewayCode(test, response) != "outcome_unknown" || fixture.upstream.count("execute") != 1 {
		test.Fatal("audit error pretended SQL rolled back")
	}
	fixture.audit.mu.Lock()
	encoded, _ := json.Marshal(fixture.audit.events)
	fixture.audit.mu.Unlock()
	if strings.Contains(string(encoded), privateSQL) || strings.Contains(string(encoded), fixture.tokenA) || strings.Contains(response.Body.String(), fixture.tokenA) {
		test.Fatal("audit or response leaked SQL/credential")
	}
	if fixture.gateway.Metrics()["unknown_held_permits"] != 1 {
		test.Fatal("audit unknown permit released")
	}
}
