package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// s02AuthorityAction and s02WorkerAction mirror the controller's wire structs.
// They stay local because the platform must treat the control protocol as an
// external contract, not reach into the controller's private types.
type s02AuthorityAction struct {
	Command  string                 `json:"command"`
	Database *controller.DatabaseID `json:"database,omitempty"`
	Intent   *struct {
		Kind   string          `json:"kind"`
		Intent json.RawMessage `json:"intent"`
	} `json:"intent,omitempty"`
	Request *struct {
		RequestID string          `json:"request_id"`
		Nonce     string          `json:"nonce"`
		Intent    json.RawMessage `json:"intent"`
	} `json:"request,omitempty"`
}

type s02WorkerAction struct {
	Command       string                 `json:"command"`
	Database      *controller.DatabaseID `json:"database,omitempty"`
	ExpectedEpoch *uint64                `json:"expected_epoch,omitempty"`
	Lease         *controller.Grant      `json:"lease,omitempty"`
	Role          string                 `json:"role,omitempty"`
}

type s02GrantIntent struct {
	Database      controller.DatabaseID `json:"database"`
	WorkerID      string                `json:"worker_id"`
	Epoch         uint64                `json:"epoch"`
	ExpectedEpoch uint64                `json:"expected_epoch"`
	LeaseID       string                `json:"lease_id"`
	TTLMS         uint64                `json:"ttl_ms"`
	Role          string                `json:"role"`
}

func s02Decode(input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, output)
}

// s02ControlStub is a controller control plane small enough to count durable
// activations. It stores just the facts the wake path reads: who holds which
// lease, and what each worker currently owns.
type s02ControlStub struct {
	mutex     sync.Mutex
	authority map[string]controller.Actual
	workers   map[string]map[string]controller.Actual
	extraLoad map[string]int
	grants    int
	acquires  int

	acquireGate    chan struct{}
	acquireEntered chan struct{}
	enteredOnce    sync.Once
}

func s02NewControlStub() *s02ControlStub {
	return &s02ControlStub{authority: map[string]controller.Actual{}, workers: map[string]map[string]controller.Actual{}, extraLoad: map[string]int{}}
}

func (stub *s02ControlStub) counts() (int, int) {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.grants, stub.acquires
}

func (stub *s02ControlStub) Authority(ctx context.Context, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var action s02AuthorityAction
	if err := s02Decode(input, &action); err != nil {
		return err
	}
	switch action.Command {
	case "get_database_state":
		stub.mutex.Lock()
		actual, exists := stub.authority[action.Database.Key()]
		stub.mutex.Unlock()
		if !exists {
			actual = controller.Actual{Database: *action.Database, Mode: "detached"}
		}
		return s02Decode(actual, output)
	case "challenge":
		return s02Decode(map[string]any{"nonce": "nonce-" + identifier, "expires_in_ms": 5000}, output)
	case "grant":
		if action.Request == nil || action.Request.RequestID != identifier || action.Request.Intent == nil {
			return errors.New("grant request shape mismatch")
		}
		var intent s02GrantIntent
		if err := json.Unmarshal(action.Request.Intent, &intent); err != nil {
			return err
		}
		stub.mutex.Lock()
		defer stub.mutex.Unlock()
		actual := stub.authority[intent.Database.Key()]
		if actual.Epoch != intent.ExpectedEpoch {
			return errors.New("grant fenced by a newer epoch")
		}
		binding := controller.Binding{
			Database:          intent.Database,
			RootIncarnation:   "root-a",
			AuthorityTerm:     1,
			BranchIncarnation: 1,
			Epoch:             intent.Epoch,
			WorkerID:          intent.WorkerID,
			LeaseID:           intent.LeaseID,
			GrantID:           identifier,
			Role:              intent.Role,
		}
		stub.authority[intent.Database.Key()] = controller.Actual{Database: intent.Database, Mode: intent.Role, Binding: &binding, Epoch: intent.Epoch, RemainingTTLMS: intent.TTLMS}
		stub.grants++
		return s02Decode(controller.Grant{Binding: binding, RemainingTTLMS: intent.TTLMS}, output)
	case "renew":
		if action.Request == nil || action.Request.Intent == nil {
			return errors.New("renew request shape mismatch")
		}
		var intent struct {
			Binding  controller.Binding `json:"binding"`
			Sequence uint64             `json:"sequence"`
			TTLMS    uint64             `json:"ttl_ms"`
		}
		if err := json.Unmarshal(action.Request.Intent, &intent); err != nil {
			return err
		}
		stub.mutex.Lock()
		defer stub.mutex.Unlock()
		binding := intent.Binding
		binding.RenewalSequence = intent.Sequence
		stub.authority[binding.Database.Key()] = controller.Actual{Database: binding.Database, Mode: binding.Role, Binding: &binding, Epoch: binding.Epoch, RemainingTTLMS: intent.TTLMS}
		return s02Decode(controller.Grant{Binding: binding, RemainingTTLMS: intent.TTLMS}, output)
	default:
		return errors.New("unsupported authority command")
	}
}

func (stub *s02ControlStub) Worker(ctx context.Context, worker controller.Worker, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var action s02WorkerAction
	if err := s02Decode(input, &action); err != nil {
		return err
	}
	switch action.Command {
	case "get_worker_heartbeat":
		stub.mutex.Lock()
		load := stub.extraLoad[worker.ID]
		for _, actual := range stub.workers[worker.ID] {
			if actual.Mode != "detached" && actual.Mode != "revoked" {
				load++
			}
		}
		stub.mutex.Unlock()
		return s02Decode(map[string]any{
			"worker_id": worker.ID,
			"databases": []any{},
			"failures":  []any{},
			"resources": map[string]any{
				"loaded_databases":          load,
				"loading_databases":         0,
				"active_sessions":           0,
				"resident_count":            load,
				"oldest_resident_age_ms":    0,
				"quarantined_count":         0,
				"oldest_quarantined_age_ms": 0,
				"databases":                 []any{},
			},
		}, output)
	case "get_database_state":
		stub.mutex.Lock()
		actual, exists := stub.workers[worker.ID][action.Database.Key()]
		stub.mutex.Unlock()
		if !exists {
			actual = controller.Actual{Database: *action.Database, Mode: "detached"}
		}
		return s02Decode(actual, output)
	case "acquire_database":
		if stub.acquireGate != nil {
			if stub.acquireEntered != nil {
				stub.enteredOnce.Do(func() { close(stub.acquireEntered) })
			}
			select {
			case <-stub.acquireGate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if action.Lease == nil || action.Role == "" {
			return errors.New("acquire request shape mismatch")
		}
		database := action.Lease.Binding.Database
		stub.mutex.Lock()
		defer stub.mutex.Unlock()
		actual := stub.authority[database.Key()]
		if actual.Binding == nil || *actual.Binding != action.Lease.Binding || action.Lease.Binding.WorkerID != worker.ID || action.Role != action.Lease.Binding.Role {
			return errors.New("worker acquired before exact root grant")
		}
		if stub.workers[worker.ID] == nil {
			stub.workers[worker.ID] = map[string]controller.Actual{}
		}
		resident := true
		stub.workers[worker.ID][database.Key()] = controller.Actual{Database: database, Mode: action.Role, Binding: &action.Lease.Binding, Epoch: action.Lease.Binding.Epoch, RemainingTTLMS: action.Lease.RemainingTTLMS, Resident: &resident}
		stub.acquires++
		return s02Decode(map[string]bool{"ok": true}, output)
	case "revoke_database":
		stub.mutex.Lock()
		defer stub.mutex.Unlock()
		if stub.workers[worker.ID] != nil && action.Database != nil {
			actual := stub.workers[worker.ID][action.Database.Key()]
			actual.Database = *action.Database
			actual.Mode = "revoked"
			actual.Binding = nil
			actual.RemainingTTLMS = 0
			stub.workers[worker.ID][action.Database.Key()] = actual
		}
		return s02Decode(map[string]bool{"ok": true}, output)
	default:
		return errors.New("unsupported worker command")
	}
}

type s02ActivationFixture struct {
	base     *gatewayFixture
	gateway  *Gateway
	store    controller.MetadataStore
	control  *s02ControlStub
	worker   controller.Worker
	database controller.DatabaseID
}

func s02IdlePlacement(database controller.DatabaseID, workerID, phase, reason, policy, desiredState string) controller.Placement {
	leaseID := "lease-s02-idle"
	binding := controller.Binding{
		Database:          database,
		RootIncarnation:   "root-a",
		AuthorityTerm:     1,
		BranchIncarnation: 1,
		Epoch:             1,
		WorkerID:          workerID,
		LeaseID:           leaseID,
		GrantID:           "grant-s02-idle",
		Role:              "writer",
	}
	return controller.Placement{
		Database:      database,
		DesiredWorker: workerID,
		Region:        "local",
		Epoch:         1,
		LeaseID:       leaseID,
		TTLMS:         1000,
		DesiredState:  desiredState,
		Role:          "writer",
		Phase:         phase,
		Grant:         &controller.Grant{Binding: binding, RemainingTTLMS: 1000},
		SuspendReason: reason,
		WakePolicy:    policy,
		IdleSinceMS:   time.Now().UnixMilli(),
	}
}

func s02NewActivationFixture(test *testing.T, phase, reason, policy, desiredState string, extraLoad int, waitBudget time.Duration) *s02ActivationFixture {
	test.Helper()
	base := newGatewayFixture(test)
	if err := base.state.Update(func(state *State) error {
		project := state.Projects["project-a"]
		project.Quotas.MaxSessions = 256
		project.Quotas.MaxConcurrent = 256
		project.Quotas.RequestsPerSecond = 100000
		state.Projects[project.ID] = project
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	store, err := controller.OpenStore(filepath.Join(test.TempDir(), "metadata"))
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = store.Close() })
	worker := controller.Worker{ID: "worker-s02", Address: "http://127.0.0.1:12345", Region: "local", Capacity: 8}
	if err := controller.RegisterWorker(store, worker); err != nil {
		test.Fatal(err)
	}
	placement := s02IdlePlacement(base.first, worker.ID, phase, reason, policy, desiredState)
	if err := store.Update(func(metadata *controller.Metadata) error {
		metadata.Epoch = placement.Epoch
		metadata.Placements[placement.Database.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	control := s02NewControlStub()
	control.extraLoad[worker.ID] = extraLoad
	reconciler, err := controller.NewController(store, control, controller.ReconcileConfig{LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond})
	if err != nil {
		test.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{State: base.state, Placements: store, Controller: reconciler, Identity: base.identity, Upstream: base.upstream, Audit: base.audit, WakeWaitBudget: waitBudget})
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Close(ctx)
	})
	return &s02ActivationFixture{base: base, gateway: gateway, store: store, control: control, worker: worker, database: base.first}
}

func (fixture *s02ActivationFixture) waitActive(test *testing.T) {
	test.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		metadata, err := fixture.store.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		placement := metadata.Placements[fixture.database.Key()]
		if placement.Phase == "active" {
			return
		}
		if time.Now().After(deadline) {
			test.Fatalf("database did not activate: %+v", placement)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func s02OpenConcurrently(fixture *s02ActivationFixture, requests int) []int {
	encoded, err := json.Marshal(map[string]any{"database": fixture.database})
	if err != nil {
		panic(err)
	}
	codes := make([]int, requests)
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			request := httptest.NewRequest("POST", "/v1/sql/open", strings.NewReader(string(encoded)))
			request.Header.Set("Authorization", "Bearer "+fixture.base.tokenA)
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			codes[index] = response.Code
		}()
	}
	group.Wait()
	return codes
}

func TestRequestBodyActivationMergesConcurrentOpens(test *testing.T) {
	fixture := s02NewActivationFixture(test, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 10*time.Second)
	fixture.control.acquireGate = make(chan struct{})
	fixture.control.acquireEntered = make(chan struct{})
	results := make(chan []int, 1)
	go func() { results <- s02OpenConcurrently(fixture, 100) }()
	select {
	case <-fixture.control.acquireEntered:
	case <-time.After(5 * time.Second):
		test.Fatal("activation never reached the worker acquire")
	}
	// Give the remaining opens time to join the one activation before it finishes.
	time.Sleep(100 * time.Millisecond)
	close(fixture.control.acquireGate)
	codes := <-results
	for index, code := range codes {
		if code != http.StatusOK {
			test.Fatalf("concurrent open %d failed: %d", index, code)
		}
	}
	grants, acquires := fixture.control.counts()
	if grants != 1 || acquires != 1 {
		test.Fatalf("100 concurrent opens started more than one activation: grants=%d acquires=%d", grants, acquires)
	}
	fixture.waitActive(test)
}

func TestRequestOpenPublishesStructuredLoadingAfterWaitBudget(test *testing.T) {
	fixture := s02NewActivationFixture(test, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 50*time.Millisecond)
	fixture.control.acquireGate = make(chan struct{})
	fixture.control.acquireEntered = make(chan struct{})
	response := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database})
	if response.Code != http.StatusAccepted {
		test.Fatalf("open did not publish a loading state: %d %s", response.Code, response.Body.String())
	}
	var body struct {
		OK     bool `json:"ok"`
		Result struct {
			Loading     bool   `json:"loading"`
			State       string `json:"state"`
			OperationID string `json:"operation_id"`
			Database    string `json:"database"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		test.Fatal(err)
	}
	if !body.OK || !body.Result.Loading || body.Result.OperationID == "" || body.Result.State != controller.ActivationWaking || body.Result.Database != fixture.database.Key() {
		test.Fatalf("loading result is not actionable: %s", response.Body.String())
	}
	select {
	case <-fixture.control.acquireEntered:
	case <-time.After(5 * time.Second):
		test.Fatal("activation did not continue after the waiter left")
	}
	close(fixture.control.acquireGate)
	fixture.waitActive(test)
	if retry := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database}); retry.Code != http.StatusOK {
		test.Fatalf("retry after activation failed: %d %s", retry.Code, retry.Body.String())
	}
	grants, acquires := fixture.control.counts()
	if grants != 1 || acquires != 1 {
		test.Fatalf("the loading waiter started another activation: grants=%d acquires=%d", grants, acquires)
	}
}

func TestRequestCancellationDoesNotDuplicateActivation(test *testing.T) {
	fixture := s02NewActivationFixture(test, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 10*time.Second)
	fixture.control.acquireGate = make(chan struct{})
	fixture.control.acquireEntered = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoded, err := json.Marshal(map[string]any{"database": fixture.database})
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/sql/open", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+fixture.base.tokenA)
	request = request.WithContext(ctx)
	response := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		fixture.gateway.ServeHTTP(response, request)
		close(served)
	}()
	select {
	case <-fixture.control.acquireEntered:
	case <-time.After(5 * time.Second):
		test.Fatal("activation never reached the worker acquire")
	}
	cancel()
	<-served
	if response.Code != http.StatusServiceUnavailable || gatewayCode(test, response) != "request_cancelled" {
		test.Fatalf("cancelled request was not reported: %d %s", response.Code, response.Body.String())
	}
	close(fixture.control.acquireGate)
	fixture.waitActive(test)
	if retry := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database}); retry.Code != http.StatusOK {
		test.Fatalf("retry after cancellation failed: %d %s", retry.Code, retry.Body.String())
	}
	grants, acquires := fixture.control.counts()
	if grants != 1 || acquires != 1 {
		test.Fatalf("a cancelled waiter repeated the activation: grants=%d acquires=%d", grants, acquires)
	}
}

func TestAdministratorSuspensionIsNotWokenByOpen(test *testing.T) {
	fixture := s02NewActivationFixture(test, "suspended", controller.SuspendReasonAdmin, controller.WakePolicyManual, "suspended", 0, time.Second)
	response := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database})
	if response.Code != http.StatusConflict || gatewayCode(test, response) != "placement_unavailable" {
		test.Fatalf("administrator suspension answered %d %s", response.Code, response.Body.String())
	}
	grants, acquires := fixture.control.counts()
	if grants != 0 || acquires != 0 {
		test.Fatalf("an administrator suspension was activated: grants=%d acquires=%d", grants, acquires)
	}
	metadata, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	placement := metadata.Placements[fixture.database.Key()]
	if placement.Phase != "suspended" || placement.SuspendReason != controller.SuspendReasonAdmin {
		test.Fatalf("suspension changed under a request: %+v", placement)
	}
}

func TestUnauthorizedOpenDoesNotSpendActivationBudget(test *testing.T) {
	fixture := s02NewActivationFixture(test, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, time.Second)
	response := gatewayHTTP(test, fixture.gateway, fixture.base.tokenB, "/v1/sql/open", map[string]any{"database": fixture.database})
	if response.Code != http.StatusForbidden {
		test.Fatalf("cross-tenant open answered %d %s", response.Code, response.Body.String())
	}
	grants, acquires := fixture.control.counts()
	if grants != 0 || acquires != 0 {
		test.Fatalf("an unauthorized request triggered activation: grants=%d acquires=%d", grants, acquires)
	}
}

func TestActivationRefusesWhenWorkerCapacityIsFull(test *testing.T) {
	fixture := s02NewActivationFixture(test, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 8, 200*time.Millisecond)
	response := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database})
	if response.Code != http.StatusConflict || gatewayCode(test, response) != "placement_unavailable" {
		test.Fatalf("full worker answered %d %s", response.Code, response.Body.String())
	}
	grants, acquires := fixture.control.counts()
	if grants != 0 || acquires != 0 {
		test.Fatalf("a full worker still allocated an activation: grants=%d acquires=%d", grants, acquires)
	}
	metadata, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if placement := metadata.Placements[fixture.database.Key()]; placement.Phase != "idle" {
		test.Fatalf("refused activation did not stay idle: %+v", placement)
	}
}
