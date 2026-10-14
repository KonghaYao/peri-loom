package platform

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type gatewayTestState struct {
	mu    sync.Mutex
	state State
}

func (store *gatewayTestState) Snapshot() (State, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var copy State
	encoded, _ := json.Marshal(store.state)
	err := json.Unmarshal(encoded, &copy)
	return copy, err
}
func (store *gatewayTestState) Update(update func(*State) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	var copy State
	encoded, _ := json.Marshal(store.state)
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return err
	}
	if err := update(&copy); err != nil {
		return err
	}
	store.state = copy
	return nil
}
func (*gatewayTestState) Close() error { return nil }

type gatewayTestPlacements struct {
	mu       sync.Mutex
	metadata controller.Metadata
}

func (store *gatewayTestPlacements) Snapshot() (controller.Metadata, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var copy controller.Metadata
	encoded, _ := json.Marshal(store.metadata)
	err := json.Unmarshal(encoded, &copy)
	return copy, err
}
func (store *gatewayTestPlacements) Update(update func(*controller.Metadata) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	var copy controller.Metadata
	encoded, _ := json.Marshal(store.metadata)
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return err
	}
	if err := update(&copy); err != nil {
		return err
	}
	store.metadata = copy
	return nil
}
func (*gatewayTestPlacements) Close() error { return nil }

type gatewayTestAudit struct {
	mu         sync.Mutex
	events     []AuditEvent
	failAction string
}

func (audit *gatewayTestAudit) Record(event AuditEvent) error {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	audit.events = append(audit.events, event)
	if event.Action == audit.failAction {
		return errors.New("private audit failure")
	}
	return nil
}
func (*gatewayTestAudit) Close() error { return nil }

type gatewayCall struct {
	worker    controller.Worker
	command   map[string]any
	requestID string
}
type gatewayTestUpstream struct {
	mu       sync.Mutex
	calls    []gatewayCall
	hook     func(context.Context, controller.Worker, map[string]any) (any, error)
	readonly bool
}

func (upstream *gatewayTestUpstream) Call(ctx context.Context, worker controller.Worker, requestID string, command any, output any) error {
	encoded, err := json.Marshal(command)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	upstream.mu.Lock()
	upstream.calls = append(upstream.calls, gatewayCall{worker, fields, requestID})
	count := len(upstream.calls)
	hook := upstream.hook
	readonly := upstream.readonly
	upstream.mu.Unlock()
	var value any
	if hook != nil {
		value, err = hook(ctx, worker, fields)
		if err != nil {
			return err
		}
	}
	if value == nil {
		switch fields["command"] {
		case "open_session":
			value = map[string]any{"session": fmt.Sprintf("%064x", count), "readonly": readonly}
		case "execute":
			value = map[string]bool{"executed": true}
		case "close_session":
			value = map[string]bool{"closed": true}
		case "query":
			value = [][]any{{1, "one"}, {2, "two"}}
		case "statement":
			value = map[string]any{"cols": []any{map[string]any{"name": "message", "decltype": "TEXT"}}, "rows": [][]any{{"hello"}}, "affected_row_count": 0, "last_insert_rowid": nil, "autocommit": true}
		default:
			return errors.New("unknown fixture action")
		}
	}
	if output == nil {
		return nil
	}
	encoded, err = json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, output)
}
func (upstream *gatewayTestUpstream) count(operation string) int {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	count := 0
	for _, call := range upstream.calls {
		if call.command["command"] == operation {
			count++
		}
	}
	return count
}
func (upstream *gatewayTestUpstream) setHook(hook func(context.Context, controller.Worker, map[string]any) (any, error)) {
	upstream.mu.Lock()
	upstream.hook = hook
	upstream.mu.Unlock()
}

type gatewayFixture struct {
	gateway                *Gateway
	state                  *gatewayTestState
	placements             *gatewayTestPlacements
	upstream               *gatewayTestUpstream
	audit                  *gatewayTestAudit
	identity               Identity
	first, second          controller.DatabaseID
	tokenA, tokenB, viewer string
	keyA                   APIKey
}

func newGatewayFixture(test *testing.T) *gatewayFixture {
	test.Helper()
	first := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	second := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-b", Database: "db-b"}, Branch: "main"}
	quota := Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 2, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	state := &gatewayTestState{state: State{Version: 1, Organizations: map[string]Organization{"org": {ID: "org", Name: "tests"}}, Projects: map[string]Project{"project-a": {ID: "project-a", Organization: "org", Tenant: "tenant-a", Quotas: quota}, "project-b": {ID: "project-b", Organization: "org", Tenant: "tenant-b", Quotas: quota}}, Databases: map[string]Database{first.Key(): {ID: first, Project: "project-a", State: "active", RootIncarnation: "root-a", BranchIncarnation: 1, Encrypted: true}, second.Key(): {ID: second, Project: "project-b", State: "active", RootIncarnation: "root-b", BranchIncarnation: 1, Encrypted: true}}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{"tenant-a/db-a": {Namespace: first.Namespace, State: "ready", RootIncarnation: "root-a", DomainID: "domain-a"}, "tenant-b/db-b": {Namespace: second.Namespace, State: "ready", RootIncarnation: "root-b", DomainID: "domain-b"}}}}
	placements := &gatewayTestPlacements{metadata: controller.Metadata{Version: 1, Workers: map[string]controller.Worker{}, Placements: map[string]controller.Placement{}}}
	for _, database := range []controller.DatabaseID{first, second} {
		worker := controller.Worker{ID: "worker-" + database.Namespace.Tenant, Address: "http://127.0.0.1:1234", Healthy: true, Capacity: 8, Load: 1, LastHeartbeatMS: time.Now().UnixMilli()}
		binding := controller.Binding{Database: database, RootIncarnation: "root-" + strings.TrimPrefix(database.Namespace.Tenant, "tenant-"), AuthorityTerm: 1, BranchIncarnation: 1, Epoch: 1, WorkerID: worker.ID, LeaseID: "lease-1", GrantID: "grant-1", Role: "writer"}
		placements.metadata.Workers[worker.ID] = worker
		placements.metadata.Placements[database.Key()] = controller.Placement{Database: database, DesiredWorker: worker.ID, Epoch: 1, LeaseID: "lease-1", Role: "writer", DesiredState: "active", Phase: "active", Grant: &controller.Grant{Binding: binding, RemainingTTLMS: 10000}}
	}
	var signing [32]byte
	if _, err := rand.Read(signing[:]); err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(state, signing[:])
	if err != nil {
		test.Fatal(err)
	}
	keyA, tokenA, err := NewAPIKey(state, "subject-a", "project-a", "sql", false)
	if err != nil {
		test.Fatal(err)
	}
	_, tokenB, err := NewAPIKey(state, "subject-b", "project-b", "sql", false)
	if err != nil {
		test.Fatal(err)
	}
	_, viewer, err := NewAPIKey(state, "viewer-a", "project-a", "viewer", false)
	if err != nil {
		test.Fatal(err)
	}
	upstream, audit := &gatewayTestUpstream{}, &gatewayTestAudit{}
	gateway, err := NewGateway(GatewayConfig{State: state, Placements: placements, Identity: identity, Upstream: upstream, Audit: audit})
	if err != nil {
		test.Fatal(err)
	}
	fixture := &gatewayFixture{gateway, state, placements, upstream, audit, identity, first, second, tokenA, tokenB, viewer, keyA}
	test.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Close(ctx)
	})
	return fixture
}

func gatewayHTTP(test *testing.T, gateway *Gateway, token, path string, body any) *httptest.ResponseRecorder {
	test.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest("POST", path, strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	return response
}

func gatewayOpen(test *testing.T, fixture *gatewayFixture, token string, database controller.DatabaseID) string {
	test.Helper()
	response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/open", map[string]any{"database": database})
	if response.Code != 200 {
		test.Fatalf("open failed %d: %s", response.Code, response.Body.String())
	}
	var value struct {
		Result struct {
			Session string `json:"session"`
		} `json:"result"`
	}
	if json.Unmarshal(response.Body.Bytes(), &value) != nil || !publicSessionID(value.Result.Session) {
		test.Fatal("invalid public session")
	}
	return value.Result.Session
}

func gatewayCode(test *testing.T, response *httptest.ResponseRecorder) string {
	test.Helper()
	var value struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(response.Body.Bytes(), &value) != nil {
		test.Fatal("invalid gateway error")
	}
	return value.Error.Code
}
