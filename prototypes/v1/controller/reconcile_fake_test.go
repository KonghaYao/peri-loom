package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type reconcileFake struct {
	mutex           sync.Mutex
	store           MetadataStore
	authority       map[string]Actual
	workers         map[string]map[string]Actual
	offline         map[string]bool
	busy            map[string]bool
	receipts        map[string]Grant
	payloads        map[string]string
	lost            map[string]int
	calls           map[string]int
	delay           map[string]time.Duration
	heartbeatProbes map[string]int
	// authorityReject fails that many authority state reads, so a test can hold
	// one reconcile pass transiently incomplete.
	authorityReject int
	trace           []string
	rejectGrant     bool
}

func newReconcileFake(store MetadataStore) *reconcileFake {
	return &reconcileFake{store: store, authority: make(map[string]Actual), workers: make(map[string]map[string]Actual), offline: make(map[string]bool), busy: make(map[string]bool), receipts: make(map[string]Grant), payloads: make(map[string]string), lost: make(map[string]int), calls: make(map[string]int), delay: make(map[string]time.Duration), heartbeatProbes: make(map[string]int)}
}

func reconcileDecode(input, output any) error {
	bytes, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, output)
}

func (fake *reconcileFake) Authority(ctx context.Context, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var action authorityAction
	if err := reconcileDecode(input, &action); err != nil {
		return err
	}
	if action.Command == "get_database_state" && action.Database != nil {
		fake.mutex.Lock()
		delay := fake.delay[action.Database.Key()]
		reject := fake.authorityReject > 0
		if reject {
			fake.authorityReject--
		}
		fake.mutex.Unlock()
		if reject {
			return &RPCError{Rejected: true, Message: "transient authority rejection"}
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	metadata, err := fake.store.Snapshot()
	if err != nil {
		return err
	}
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.calls[action.Command]++
	fake.trace = append(fake.trace, action.Command)
	if action.Command == "get_database_state" {
		actual, exists := fake.authority[action.Database.Key()]
		if !exists {
			actual = Actual{Database: *action.Database, Mode: "detached"}
		}
		return reconcileDecode(actual, output)
	}
	if action.Command == "challenge" {
		if action.Intent == nil {
			return errors.New("challenge intent missing")
		}
		if action.Intent.Kind == "grant" {
			var intent grantIntent
			if err := json.Unmarshal(action.Intent.Intent, &intent); err != nil {
				return err
			}
			placement := metadata.Placements[intent.Database.Key()]
			if metadata.Epoch < intent.Epoch || placement.Epoch != intent.Epoch || placement.Phase != "grant_intent" {
				return errors.New("epoch or desired intent was not durable before challenge")
			}
		}
		return reconcileDecode(map[string]any{"nonce": "nonce-" + identifier, "expires_in_ms": 5000}, output)
	}
	if action.Command == "revoke" {
		var revoke revokeAction
		if err := reconcileDecode(input, &revoke); err != nil {
			return err
		}
		placement := metadata.Placements[revoke.Request.Database.Key()]
		if placement.PendingID != identifier || placement.Phase != "revoke_pending" {
			return errors.New("revoke not durable")
		}
		actual := fake.authority[revoke.Request.Database.Key()]
		if actual.Epoch != revoke.Request.ExpectedEpoch {
			return &RPCError{Rejected: true, PreconditionFailed: true}
		}
		actual.Mode = "revoked"
		actual.RemainingTTLMS = 0
		actual.Binding = nil
		fake.authority[actual.Database.Key()] = actual
		if fake.lose("revoke") {
			return &RPCError{Ambiguous: true, Message: "lost acknowledgement"}
		}
		return reconcileDecode(actual, output)
	}
	if action.Request == nil || action.Request.RequestID != identifier {
		return errors.New("inner and outer request IDs differ")
	}
	bytes, err := json.Marshal(input)
	if err != nil {
		return err
	}
	if previous, exists := fake.payloads[identifier]; exists {
		if previous != string(bytes) {
			return errors.New("receipt payload changed")
		}
		grant := fake.receipts[identifier]
		return reconcileDecode(grant, output)
	}
	var grant Grant
	var database DatabaseID
	if action.Command == "grant" {
		var intent grantIntent
		if err := json.Unmarshal(action.Request.Intent, &intent); err != nil {
			return err
		}
		database = intent.Database
		if fake.rejectGrant || fake.authority[database.Key()].Epoch != intent.ExpectedEpoch {
			return &RPCError{Rejected: true, PreconditionFailed: true, Message: "fenced precondition"}
		}
		grant = Grant{Binding: Binding{Database: database, RootIncarnation: "root", AuthorityTerm: 1, BranchIncarnation: 1, Epoch: intent.Epoch, WorkerID: intent.WorkerID, LeaseID: intent.LeaseID, GrantID: identifier, Role: intent.Role}, RemainingTTLMS: intent.TTLMS}
	} else if action.Command == "renew" {
		var intent renewIntent
		if err := json.Unmarshal(action.Request.Intent, &intent); err != nil {
			return err
		}
		database = intent.Binding.Database
		actual := fake.authority[database.Key()]
		if actual.Binding == nil || *actual.Binding != intent.Binding || intent.Sequence != intent.Binding.RenewalSequence+1 {
			return &RPCError{Rejected: true, PreconditionFailed: true}
		}
		grant = Grant{Binding: intent.Binding, RemainingTTLMS: intent.TTLMS}
		grant.Binding.RenewalSequence = intent.Sequence
	} else {
		return errors.New("unknown authority action")
	}
	placement := metadata.Placements[database.Key()]
	if placement.PendingID != identifier || string(placement.PendingCommand) != string(bytes) || placement.Phase != action.Command+"_pending" {
		return errors.New("full nonce and request were not durable before mutation")
	}
	fake.payloads[identifier] = string(bytes)
	fake.receipts[identifier] = grant
	fake.authority[database.Key()] = Actual{Database: database, Mode: grant.Binding.Role, Binding: &grant.Binding, Epoch: grant.Binding.Epoch, RemainingTTLMS: grant.RemainingTTLMS}
	if fake.lose(action.Command) {
		return &RPCError{Ambiguous: true, Message: "lost acknowledgement"}
	}
	return reconcileDecode(grant, output)
}

func (fake *reconcileFake) lose(command string) bool {
	if fake.lost[command] == 0 {
		return false
	}
	fake.lost[command]--
	return true
}

func (fake *reconcileFake) Worker(ctx context.Context, worker Worker, identifier string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := fake.store.Snapshot(); err != nil {
		return err
	}
	var action workerAction
	if err := reconcileDecode(input, &action); err != nil {
		return err
	}
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.calls[action.Command]++
	fake.trace = append(fake.trace, worker.ID+":"+action.Command)
	if action.Command == "get_worker_heartbeat" {
		fake.heartbeatProbes[worker.ID]++
	}
	if fake.offline[worker.ID] {
		return &RPCError{Ambiguous: true, Message: "worker unavailable"}
	}
	if fake.workers[worker.ID] == nil {
		fake.workers[worker.ID] = make(map[string]Actual)
	}
	if action.Command == "get_worker_state" || action.Command == "get_worker_heartbeat" {
		report := workerReport{WorkerID: worker.ID}
		for _, actual := range fake.workers[worker.ID] {
			report.Databases = append(report.Databases, actual)
			if actual.Mode != "detached" && actual.Mode != "revoked" {
				report.Resources.LoadedDatabases++
			}
		}
		return reconcileDecode(report, output)
	}
	database := action.Database
	if action.Lease != nil {
		database = &action.Lease.Binding.Database
	}
	if database == nil {
		return errors.New("worker database missing")
	}
	actual, exists := fake.workers[worker.ID][database.Key()]
	if !exists {
		actual = Actual{Database: *database, Mode: "detached"}
	}
	if action.Command == "get_database_state" {
		return reconcileDecode(actual, output)
	}
	switch action.Command {
	case "acquire_database":
		root := fake.authority[database.Key()]
		if root.Binding == nil || *root.Binding != action.Lease.Binding || action.Lease.Binding.WorkerID != worker.ID || action.Role != action.Lease.Binding.Role {
			return errors.New("worker acquired before exact root grant")
		}
		if actual.Mode != "detached" && actual.Mode != "revoked" {
			return errors.New("duplicate worker acquisition")
		}
		actual = Actual{Database: *database, Mode: action.Role, Binding: &action.Lease.Binding, Epoch: action.Lease.Binding.Epoch, RemainingTTLMS: action.Lease.RemainingTTLMS}
	case "renew_lease":
		if actual.Binding == nil || !sameLease(*actual.Binding, action.Lease.Binding) || action.Lease.Binding.RenewalSequence <= actual.Binding.RenewalSequence {
			return errors.New("duplicate or invalid worker renewal")
		}
		actual.Binding = &action.Lease.Binding
		actual.RemainingTTLMS = action.Lease.RemainingTTLMS
	case "drain_database":
		if action.ExpectedEpoch == nil || actual.Epoch != *action.ExpectedEpoch {
			return errors.New("drain epoch mismatch")
		}
		actual.Mode = "detached"
		actual.RemainingTTLMS = 0
		if fake.busy[worker.ID] {
			actual.Mode = "draining"
			actual.RemainingTTLMS = 500
		}
	case "revoke_database":
		actual.Mode = "revoked"
		actual.RemainingTTLMS = 0
	default:
		return fmt.Errorf("unknown worker action %s", action.Command)
	}
	fake.workers[worker.ID][database.Key()] = actual
	if fake.lose(action.Command) {
		return &RPCError{Ambiguous: true, Message: "lost acknowledgement"}
	}
	return reconcileDecode(map[string]bool{"ok": true}, output)
}

type reconcileFixture struct {
	store      MetadataStore
	fake       *reconcileFake
	controller *Controller
	directory  string
	config     ReconcileConfig
}

func newReconcileFixture(test *testing.T) *reconcileFixture {
	test.Helper()
	directory := filepath.Join(test.TempDir(), "metadata")
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	fixture := &reconcileFixture{store: store, directory: directory, config: ReconcileConfig{LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond}}
	fixture.fake = newReconcileFake(store)
	for index, identity := range []string{"worker-a", "worker-b"} {
		if err := RegisterWorker(store, Worker{ID: identity, Address: fmt.Sprintf("http://127.0.0.1:%d", 11001+index), Region: "local", Capacity: 8}); err != nil {
			test.Fatal(err)
		}
	}
	fixture.controller, err = NewController(store, fixture.fake, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { fixture.store.Close() })
	return fixture
}

func (fixture *reconcileFixture) restart(test *testing.T) {
	test.Helper()
	if err := fixture.store.Close(); err != nil {
		test.Fatal(err)
	}
	store, err := OpenStore(fixture.directory)
	if err != nil {
		test.Fatal(err)
	}
	fixture.store = store
	fixture.fake.store = store
	fixture.controller, err = NewController(store, fixture.fake, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
}

func reconcileDatabase(name string) DatabaseID {
	return DatabaseID{Namespace: Namespace{Tenant: "reconcile", Database: name}, Branch: "main"}
}
