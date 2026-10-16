package platform

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// narrowStateStore records whether the registry was visited or cloned, so a
// regression back to a full clone in the session-expiry path is visible.
type narrowStateStore struct {
	inner       *gatewayTestState
	snapshots   int
	narrowReads int
}

func (store *narrowStateStore) Snapshot() (State, error) {
	store.snapshots++
	return store.inner.Snapshot()
}

func (store *narrowStateStore) Update(update func(*State) error) error {
	return store.inner.Update(update)
}

func (*narrowStateStore) Close() error { return nil }

func (store *narrowStateStore) readOperations(visit func(State) error) error {
	store.narrowReads++
	state, err := store.inner.Snapshot()
	if err != nil {
		return err
	}
	return visit(state)
}

func TestSessionCleanupQuotaVisitsRegistryInsteadOfCloningIt(test *testing.T) {
	quota := Quotas{MaxDatabases: 8, MaxSessions: 3, MaxConcurrent: 2, RequestsPerSecond: 100, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	store := &narrowStateStore{inner: &gatewayTestState{state: State{
		Version:   1,
		Projects:  map[string]Project{"project-a": {ID: "project-a", Organization: "org", Tenant: "tenant-a", Quotas: quota}},
		Databases: map[string]Database{database.Key(): {ID: database, Project: "project-a", State: "active"}},
	}}}
	session := &gatewaySession{project: "project-a", database: database}
	resolved, err := sessionCleanupQuota(store, session)
	if err != nil {
		test.Fatal(err)
	}
	if resolved != quota {
		test.Fatalf("expiry resolved an unexpected quota: %+v", resolved)
	}
	if store.narrowReads != 1 || store.snapshots != 0 {
		test.Fatalf("session expiry cloned the registry: narrow=%d clones=%d", store.narrowReads, store.snapshots)
	}

	if _, err := sessionCleanupQuota(store, &gatewaySession{project: "missing", database: database}); !errors.Is(err, errSessionProjectUnavailable) {
		test.Fatalf("unknown session project was not refused: %v", err)
	}
	quarantined := &narrowStateStore{inner: &gatewayTestState{state: State{
		Version:    1,
		Projects:   map[string]Project{"project-a": {ID: "project-a", Tenant: "tenant-a", Quotas: quota}},
		Databases:  map[string]Database{database.Key(): {ID: database, Project: "project-a", State: "active"}},
		Operations: map[string]OperationRecord{"op": {OperationStatus: OperationStatus{OperationID: "op", Database: database, State: "quarantined"}}},
	}}}
	// Isolation refuses new work on the namespace, but it must not refuse the
	// close of a session that is already open: this close is the only path that
	// returns that session and its quota to an accounted end.
	if resolved, err := sessionCleanupQuota(quarantined, session); err != nil || resolved != quota {
		test.Fatalf("isolation blocked the quota of a registered session's cleanup: %+v %v", resolved, err)
	}
}

// countingPlacements records how often one dispatch consults the placement
// store; routing reads it under its own lock and every extra read queues the
// request behind placement writers.
type countingPlacements struct {
	inner     controller.MetadataStore
	routing   int
	snapshots int
}

func (store *countingPlacements) Snapshot() (controller.Metadata, error) {
	store.snapshots++
	return store.inner.Snapshot()
}

func (store *countingPlacements) Update(update func(*controller.Metadata) error) error {
	return store.inner.Update(update)
}

func (*countingPlacements) Close() error { return nil }

func (store *countingPlacements) RoutingView(database controller.DatabaseID) (controller.Placement, controller.Worker, bool, error) {
	store.routing++
	metadata, err := store.inner.Snapshot()
	if err != nil {
		return controller.Placement{}, controller.Worker{}, false, err
	}
	placement, found := metadata.Placements[database.Key()]
	return placement, metadata.Workers[placement.DesiredWorker], found, nil
}

// gatewayOpenOn opens a session through the given gateway instance, which may
// differ from the fixture's own gateway when a test wraps its stores.
func gatewayOpenOn(test *testing.T, gateway *Gateway, token string, database controller.DatabaseID) string {
	test.Helper()
	response := gatewayHTTP(test, gateway, token, "/v1/sql/open", map[string]any{"database": database})
	if response.Code != 200 {
		test.Fatalf("session open failed: %d %s", response.Code, response.Body.String())
	}
	var opened struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &opened); err != nil || !publicSessionID(opened.Result.Session) {
		test.Fatalf("session open returned no session: %s", response.Body.String())
	}
	return opened.Result.Session
}

func TestDatabaseScopedDispatchReadsPlacementOnce(test *testing.T) {
	fixture := newGatewayFixture(test)
	placements := &countingPlacements{inner: fixture.placements}
	gateway, err := NewGateway(GatewayConfig{State: fixture.state, Placements: placements, Identity: fixture.identity, Upstream: fixture.upstream, Audit: fixture.audit})
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Close(ctx)
	})
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	token, err := fixture.identity.IssueToken(principal, fixture.first, "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	session := gatewayOpenOn(test, gateway, token, fixture.first)
	placements.routing, placements.snapshots = 0, 0
	if response := gatewayHTTP(test, gateway, token, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"}); response.Code != 200 {
		test.Fatalf("database-scoped dispatch failed: %d %s", response.Code, response.Body.String())
	}
	if placements.routing != 1 || placements.snapshots != 0 {
		test.Fatalf("dispatch read the placement store %d times and cloned it %d times", placements.routing, placements.snapshots)
	}
}

func TestDatabaseScopedDispatchStillRefusesStaleSessionBinding(test *testing.T) {
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
	// A renewed placement with a fresh lease invalidates the bound session.
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.LeaseID = "lease-2"
		placement.Grant.Binding.LeaseID = "lease-2"
		placement.Grant.Binding.RenewalSequence++
		placement.Grant.RemainingTTLMS = 10000
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "session_moved" {
		test.Fatalf("moved session was not refused: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 0 {
		test.Fatal("moved session reached the native worker")
	}
}
