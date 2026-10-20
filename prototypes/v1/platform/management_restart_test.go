package platform

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// restartTransport models a controller that restarted after a clean stop: the
// authority still holds the grants while every worker lost its local instance.
type restartTransport struct {
	mu       sync.Mutex
	actual   map[string]controller.Actual
	workers  map[string]map[string]controller.Actual
	delay    map[string]time.Duration
	rejects  int
	acquires int
	// rejectDetail is what the authority puts in the private RPC detail of a
	// transient rejection, so tests can prove it never reaches the client.
	rejectDetail string
}

func newRestartTransport(placements controller.Metadata) *restartTransport {
	transport := &restartTransport{actual: make(map[string]controller.Actual), workers: make(map[string]map[string]controller.Actual), delay: make(map[string]time.Duration)}
	for key, placement := range placements.Placements {
		binding := placement.Grant.Binding
		transport.actual[key] = controller.Actual{Database: placement.Database, Mode: placement.Role, Binding: &binding, Epoch: placement.Epoch, RemainingTTLMS: placement.TTLMS}
	}
	return transport
}

func (transport *restartTransport) Authority(ctx context.Context, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var action struct {
		Command string           `json:"command"`
		Request *json.RawMessage `json:"request"`
	}
	encoded, _ := json.Marshal(input)
	if err := json.Unmarshal(encoded, &action); err != nil {
		return err
	}
	transport.mu.Lock()
	switch action.Command {
	case "get_database_state":
		var fields struct {
			Database controller.DatabaseID `json:"database"`
		}
		if err := json.Unmarshal(encoded, &fields); err != nil {
			transport.mu.Unlock()
			return err
		}
		delay := transport.delay[fields.Database.Key()]
		reject := transport.rejects > 0
		if reject {
			transport.rejects--
		}
		actual, exists := transport.actual[fields.Database.Key()]
		if !exists {
			// A branch that the authority does not hold yet still has an identity;
			// returning the zero value would fail the controller's identity check.
			actual = controller.Actual{Database: fields.Database, Mode: "detached"}
		}
		transport.mu.Unlock()
		if reject {
			return &controller.RPCError{Rejected: true, Message: "transient authority rejection", Detail: transport.rejectDetail}
		}
		time.Sleep(delay)
		return decodeTestJSON(actual, output)
	case "challenge":
		transport.mu.Unlock()
		return decodeTestJSON(map[string]any{"nonce": "nonce-" + identifier, "expires_in_ms": 5000}, output)
	case "grant", "renew":
		if action.Request == nil {
			transport.mu.Unlock()
			return &controller.RPCError{Rejected: true, Message: "request missing"}
		}
		var request struct {
			RequestID string `json:"request_id"`
			Intent    struct {
				Database      controller.DatabaseID `json:"database"`
				WorkerID      string                `json:"worker_id"`
				Epoch         uint64                `json:"epoch"`
				ExpectedEpoch uint64                `json:"expected_epoch"`
				LeaseID       string                `json:"lease_id"`
				TTLMS         uint64                `json:"ttl_ms"`
				Role          string                `json:"role"`
			} `json:"intent"`
		}
		if err := json.Unmarshal(*action.Request, &request); err != nil {
			transport.mu.Unlock()
			return err
		}
		grant := controller.Grant{Binding: controller.Binding{
			Database: request.Intent.Database, RootIncarnation: "root", AuthorityTerm: 1, BranchIncarnation: 1,
			Epoch: request.Intent.Epoch, WorkerID: request.Intent.WorkerID, LeaseID: request.Intent.LeaseID,
			GrantID: request.RequestID, Role: request.Intent.Role,
		}, RemainingTTLMS: request.Intent.TTLMS}
		transport.actual[grant.Binding.Database.Key()] = controller.Actual{Database: grant.Binding.Database, Mode: grant.Binding.Role, Binding: &grant.Binding, Epoch: grant.Binding.Epoch, RemainingTTLMS: grant.RemainingTTLMS}
		transport.mu.Unlock()
		return decodeTestJSON(grant, output)
	}
	transport.mu.Unlock()
	return &controller.RPCError{Rejected: true, Message: "unsupported authority command"}
}

func (transport *restartTransport) Worker(ctx context.Context, worker controller.Worker, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var action struct {
		Command  string                 `json:"command"`
		Database *controller.DatabaseID `json:"database"`
		Lease    *controller.Grant      `json:"lease"`
		Role     string                 `json:"role"`
	}
	encoded, _ := json.Marshal(input)
	if err := json.Unmarshal(encoded, &action); err != nil {
		return err
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.workers[worker.ID] == nil {
		transport.workers[worker.ID] = make(map[string]controller.Actual)
	}
	switch action.Command {
	case "get_worker_heartbeat":
		return decodeTestJSON(map[string]any{
			"worker_id": worker.ID, "databases": []any{}, "failures": []any{},
			"resources": map[string]any{"loaded_databases": 0, "loading_databases": 0, "active_sessions": 0, "resident_count": 0, "oldest_resident_age_ms": 0, "quarantined_count": 0, "oldest_quarantined_age_ms": 0, "databases": []any{}},
		}, output)
	case "get_database_state":
		actual, exists := transport.workers[worker.ID][action.Database.Key()]
		if !exists {
			actual = controller.Actual{Database: *action.Database, Mode: "detached"}
		}
		return decodeTestJSON(actual, output)
	case "acquire_database":
		if action.Lease == nil {
			return &controller.RPCError{Rejected: true, Message: "lease missing"}
		}
		transport.acquires++
		database := action.Lease.Binding.Database
		transport.workers[worker.ID][database.Key()] = controller.Actual{Database: database, Mode: action.Lease.Binding.Role, Binding: &action.Lease.Binding, Epoch: action.Lease.Binding.Epoch, RemainingTTLMS: action.Lease.RemainingTTLMS}
		return decodeTestJSON(map[string]bool{"acquired": true}, output)
	}
	return &controller.RPCError{Rejected: true, Message: "unsupported worker command"}
}

func decodeTestJSON(input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, output)
}

// TestManagementReconcileReadyAfterRestart drives the management endpoint the
// launcher calls after a clean stop. Two placements must be reacquired after the
// workers lost their local instances, and the first authority observation is
// delayed past the heartbeat window so the pass has to re-probe instead of
// migrating. A transient rejection also exercises the bounded retry.
func TestManagementReconcileReadyAfterRestart(test *testing.T) {
	first := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	second := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-b", Database: "db-b"}, Branch: "main"}
	metadata := controller.Metadata{Version: 1, Workers: map[string]controller.Worker{}, Placements: map[string]controller.Placement{}}
	for index, database := range []controller.DatabaseID{first, second} {
		identity := "worker-" + database.Namespace.Tenant
		worker := controller.Worker{ID: identity, Address: "http://127.0.0.1:1234", Region: "local", Healthy: true, Capacity: 8, LastHeartbeatMS: time.Now().UnixMilli()}
		binding := controller.Binding{Database: database, RootIncarnation: "root", AuthorityTerm: 1, BranchIncarnation: 1, Epoch: uint64(index + 1), WorkerID: identity, LeaseID: "lease-" + identity, GrantID: "grant-" + identity, Role: "writer"}
		metadata.Workers[identity] = worker
		metadata.Placements[database.Key()] = controller.Placement{Database: database, DesiredWorker: identity, Epoch: uint64(index + 1), LeaseID: binding.LeaseID, TTLMS: 1000, DesiredState: "active", Role: "writer", Phase: "active", Grant: &controller.Grant{Binding: binding, RemainingTTLMS: 1000}}
	}
	state := &gatewayTestState{state: State{Version: 1, APIKeys: map[string]APIKey{}}}
	if err := BootstrapAPIKey(state, "restart-bootstrap-credential-0123456789"); err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(state, make([]byte, 32))
	if err != nil {
		test.Fatal(err)
	}
	placements := &gatewayTestPlacements{metadata: metadata}
	transport := newRestartTransport(metadata)
	transport.delay[first.Key()] = 500 * time.Millisecond
	transport.rejects = 1
	control, err := controller.NewController(placements, transport, controller.ReconcileConfig{LeaseTTL: time.Second, HeartbeatTimeout: 250 * time.Millisecond, Interval: time.Millisecond})
	if err != nil {
		test.Fatal(err)
	}
	management := &Management{config: ManagementConfig{State: state, Placements: placements, Controller: control, Identity: identity, Audit: &gatewayTestAudit{}}}
	request := httptest.NewRequest("POST", "/v1/reconcile", strings.NewReader(`{"request_id":"restart-reconcile"}`))
	request.Header.Set("Authorization", "Bearer restart-bootstrap-credential-0123456789")
	transport.rejectDetail = "secret-marker\nforged=1"
	response := httptest.NewRecorder()
	management.ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"ok":true`) {
		test.Fatalf("restart reconcile was not ready: %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret-marker") || strings.ContainsRune(response.Body.String(), 0) {
		test.Fatalf("private backend detail reached the public reply: %s", response.Body.String())
	}
	after, err := placements.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, database := range []controller.DatabaseID{first, second} {
		placement := after.Placements[database.Key()]
		if placement.Phase != "active" || placement.DesiredWorker != "worker-"+database.Namespace.Tenant {
			test.Fatalf("restart moved or stalled %s: %+v", database.Key(), placement)
		}
	}
	if transport.acquires != 2 {
		test.Fatalf("restart did not reacquire both placements: %d", transport.acquires)
	}
}

func TestManagementDetailStaysPrivateAndSanitized(test *testing.T) {
	detail := "secret-marker\nforged=1\x00" + strings.Repeat("x", managementLogDetailLimit*2)
	failure := &controller.RPCError{Rejected: true, Message: "rejected", Detail: detail}
	status, code := managementFailure(failure)
	if status != 409 || code != "operation_incomplete" {
		test.Fatalf("detail changed the public failure mapping: %d %s", status, code)
	}
	response := httptest.NewRecorder()
	managementReply(response, status, "request-detail", nil, code)
	if strings.Contains(response.Body.String(), "secret-marker") || strings.ContainsRune(response.Body.String(), 0) {
		test.Fatalf("private backend detail reached the public reply: %s", response.Body.String())
	}
	sanitized := managementLogDetail(detail)
	if strings.ContainsRune(sanitized, '\n') || strings.ContainsRune(sanitized, 0) {
		test.Fatalf("detail kept raw control characters: %q", sanitized)
	}
	if !strings.Contains(sanitized, `secret-marker\nforged=1\x00`) {
		test.Fatalf("detail was not escaped for the private log: %q", sanitized)
	}
	if len(sanitized) > 4*managementLogDetailLimit+2 {
		test.Fatalf("detail was not bounded before logging: %d bytes", len(sanitized))
	}
}
