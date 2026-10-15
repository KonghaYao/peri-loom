package platform

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	controller "peri-loom/controller"
)

func securityState(test *testing.T) StateStore {
	test.Helper()
	store, err := OpenStateStore(test.TempDir())
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { store.Close() })
	err = store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "test"}
		state.Projects["alpha"] = Project{ID: "alpha", Organization: "org", Tenant: "tenant_a"}
		state.Projects["beta"] = Project{ID: "beta", Organization: "org", Tenant: "tenant_b"}
		return nil
	})
	if err != nil {
		test.Fatal(err)
	}
	return store
}

func securityKMS(test *testing.T) KMS {
	test.Helper()
	kms, err := NewLocalKMS(filepath.Join(test.TempDir(), "ring.json"))
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { CloseLocalKMS(kms) })
	return kms
}
func securityDatabase() controller.DatabaseID {
	return controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant_a", Database: "database"}, Branch: "main"}
}

func activeSecurityDatabase(test *testing.T, store StateStore, kms KMS) KeyDescriptor {
	test.Helper()
	database := securityDatabase()
	descriptor := KeyDescriptor{ID: "descriptor", ProvisionID: "provision", Revision: 2, Namespace: database.Namespace, RootIncarnation: "root-a", DomainID: "domain", DEKID: "dek", Cipher: "aegis256", State: "ready"}
	var dek [32]byte
	for index := range dek {
		dek[index] = byte(index)
	}
	var err error
	descriptor.Envelope, err = kms.Seal(dek[:], descriptorAAD(descriptor))
	if err != nil {
		test.Fatal(err)
	}
	err = store.Update(func(state *State) error {
		state.Keys[namespaceKey(database.Namespace)] = descriptor
		state.Databases[database.Key()] = Database{ID: database, Project: "alpha", State: "active", Encrypted: true, OperationID: "provision", RootIncarnation: "root-a", BranchIncarnation: 1}
		return nil
	})
	if err != nil {
		test.Fatal(err)
	}
	return descriptor
}

type narrowAuthProbe struct {
	StateStore
	snapshots int
}

func (probe *narrowAuthProbe) Snapshot() (State, error) {
	probe.snapshots++
	return State{}, errors.New("full state snapshot on auth path")
}

func (probe *narrowAuthProbe) credentialByDigest(digest string) (APIKey, bool, error) {
	return probe.StateStore.(*stateStore).credentialByDigest(digest)
}

func (probe *narrowAuthProbe) authorizationState(keyID, projectID string, database controller.DatabaseID) (State, error) {
	return probe.StateStore.(*stateStore).authorizationState(keyID, projectID, database)
}

func (probe *narrowAuthProbe) gatewayRequestState(projectID string, database *controller.DatabaseID) (State, error) {
	return probe.StateStore.(*stateStore).gatewayRequestState(projectID, database)
}

func TestStateStoreNarrowAuthSeesCommittedRevocationWithoutFullSnapshot(test *testing.T) {
	store, _, key, credential := identityFixture(test, "sql")
	probe := &narrowAuthProbe{StateStore: store}
	identity := &identity{store: probe}
	principal, err := identity.Authenticate(credential)
	if err != nil || principal.KeyID != key.ID || identity.Authorize(principal, securityDatabase(), "sql") != nil {
		test.Fatalf("narrow auth denied current key: %v", err)
	}
	gateway := &Gateway{config: GatewayConfig{State: probe, Identity: identity}}
	if _, err := gateway.authorizeQuota(principal, securityDatabase()); err != nil {
		test.Fatalf("gateway narrow authorization failed: %v", err)
	}
	if probe.snapshots != 0 {
		test.Fatal("hot authentication cloned full state")
	}
	if err := RevokeAPIKey(store, key.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := identity.Authenticate(credential); err == nil || identity.Authorize(principal, securityDatabase(), "sql") == nil || probe.snapshots != 0 {
		test.Fatal("revocation was stale or authentication fell back to full snapshot")
	}
}

func TestGatewayRequestUsesNarrowProjectRead(test *testing.T) {
	store, identity, _, credential := identityFixture(test, "sql")
	if err := store.Update(func(state *State) error {
		project := state.Projects["alpha"]
		project.Quotas = Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 2, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096}
		state.Projects["alpha"] = project
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{State: store, Placements: &gatewayTestPlacements{}, Identity: identity, Upstream: &gatewayTestUpstream{}, Audit: &gatewayTestAudit{}})
	if err != nil {
		test.Fatal(err)
	}
	probe := &narrowAuthProbe{StateStore: store}
	gateway.config.State = probe
	response := gatewayHTTP(test, gateway, credential, "/v1/sql/execute", map[string]any{"session": "00000000000000000000000000000000", "sql": "SELECT 1"})
	if response.Code != 403 || gatewayCode(test, response) != "invalid_session" || probe.snapshots != 0 {
		test.Fatalf("gateway cloned the full registry or rejected narrow project read: %d %s", response.Code, response.Body.String())
	}
	database := securityDatabase()
	view, err := readGatewayRequestState(probe, "", &database)
	if err != nil || view.Databases[securityDatabase().Key()].Project != "alpha" || view.Projects["alpha"].ID != "alpha" || probe.snapshots != 0 {
		test.Fatal("administrator database lookup did not read its project narrowly")
	}
}

func TestStateLockReopenAndConcurrentRevisions(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := OpenStateStore(directory); !errors.Is(err, ErrStateLocked) {
		test.Fatalf("lock missing: %v", err)
	}
	initial, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := store.Update(func(state *State) error { return nil }); err != nil {
				test.Error(err)
			}
		}()
	}
	group.Wait()
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil || state.Revision != initial.Revision+16 {
		test.Fatalf("reopened revision: %d %v", state.Revision, err)
	}
}

func TestOperationSidecarDoesNotRewriteRegistry(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	directory := filepath.Dir(store.(*stateStore).disk.path)
	registry := filepath.Join(directory, "state.json")
	before, err := os.ReadFile(registry)
	if err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, database, "sidecar", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	after, err := os.ReadFile(registry)
	if err != nil {
		test.Fatal(err)
	}
	if string(before) != string(after) {
		test.Fatal("operation-only commit rewrote the registry")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil || state.Operations[operationKey(database, "sidecar")].State != "quarantined" {
		test.Fatalf("sidecar operation lost across restart: %v", err)
	}
}

func TestOperationSidecarRegistryCheckpointWinsAfterCrash(test *testing.T) {
	store := operationDurableStore(test)
	database := securityDatabase()
	directory := filepath.Dir(store.(*stateStore).disk.path)
	if err := beginOperation(store, database, "checkpoint", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "checkpoint", nil, true); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		state.Organizations["second"] = Organization{ID: "second", Name: "second"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	// Model a crash after the registry commit but before its sidecar checkpoint:
	// the older sidecar is valid, yet the newer registry is authoritative.
	concrete := store.(*stateStore)
	concrete.operations.mutex.Lock()
	if err := concrete.operations.commit(concrete.operations.revision+1, durableOperations{
		Version: 1, BaseRevision: concrete.state.Revision - 1,
		Records: map[string]OperationRecord{},
	}); err != nil {
		concrete.operations.mutex.Unlock()
		test.Fatal(err)
	}
	concrete.operations.mutex.Unlock()
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Snapshot()
	if err != nil || state.Organizations["second"].ID != "second" || state.Operations[operationKey(database, "checkpoint")].State != "terminal" {
		test.Fatalf("newer registry did not restore sidecar: %v", err)
	}
}

func TestOperationSidecarMissingFailsClosed(test *testing.T) {
	store := operationDurableStore(test)
	directory := filepath.Dir(store.(*stateStore).disk.path)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(directory, "operations")); err != nil {
		test.Fatal(err)
	}
	if reopened, err := OpenStateStore(directory); err == nil {
		reopened.Close()
		test.Fatal("missing mandatory operation storage was recreated")
	}
}

func TestOperationSidecarWriteFailurePoisonsBeforeDispatch(test *testing.T) {
	store := operationDurableStore(test)
	defer store.Close()
	path := store.(*stateStore).operations.path
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		test.Fatal(err)
	}
	if err := beginOperation(store, securityDatabase(), "unsafe", "gateway", "execute", "tool_rpc", "worker-a", "", nil); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("sidecar write failure did not prevent dispatch: %v", err)
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("store remained available after uncertain operation commit")
	}
}

func TestOperationSidecarMigratesLegacyRegistry(test *testing.T) {
	source := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(source, database, "legacy", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(source, database, "legacy", nil, true); err != nil {
		test.Fatal(err)
	}
	legacy, err := source.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	legacy.Revision, legacy.OperationStorageVersion = 0, 0
	directory := test.TempDir()
	disk, err := openDurable(filepath.Join(directory, "state.json"), legacy)
	if err != nil {
		test.Fatal(err)
	}
	if err := disk.close(); err != nil {
		test.Fatal(err)
	}
	migrated, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	state, err := migrated.Snapshot()
	if err != nil || state.OperationStorageVersion != 1 || state.Operations[operationKey(database, "legacy")].State != "terminal" {
		test.Fatalf("migration lost legacy operation: %v", err)
	}
	if err := migrated.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	state, err = reopened.Snapshot()
	if err != nil || state.Operations[operationKey(database, "legacy")].State != "terminal" {
		test.Fatalf("migrated operation lost after restart: %v", err)
	}
}

func TestStateSnapshotAndRetainedCallbackDetached(test *testing.T) {
	store := securityState(test)
	var retained *State
	if err := store.Update(func(state *State) error { retained = state; return nil }); err != nil {
		test.Fatal(err)
	}
	delete(retained.Projects, "alpha")
	first, _ := store.Snapshot()
	delete(first.Projects, "beta")
	second, _ := store.Snapshot()
	if len(second.Projects) != 2 {
		test.Fatal("state aliases callback or snapshot")
	}
}

func TestStateCorruptionMissingFilesAndRollbackNeverReset(test *testing.T) {
	for _, kind := range []string{"checksum", "rollback", "floor", "missing", "all-missing"} {
		test.Run(kind, func(test *testing.T) {
			directory := test.TempDir()
			store, err := OpenStateStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			old, err := os.ReadFile(filepath.Join(directory, "state.json"))
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Update(func(state *State) error { return nil }); err != nil {
				test.Fatal(err)
			}
			store.Close()
			switch kind {
			case "checksum":
				err = os.WriteFile(filepath.Join(directory, "state.json"), []byte(`{"version":1}`), 0600)
			case "rollback":
				err = os.WriteFile(filepath.Join(directory, "state.json"), old, 0600)
			case "floor":
				err = os.Remove(filepath.Join(directory, "state.json.floor"))
			case "missing":
				err = os.Remove(filepath.Join(directory, "state.json"))
			case "all-missing":
				err = os.Remove(filepath.Join(directory, "state.json"))
				if err == nil {
					err = os.Remove(filepath.Join(directory, "state.json.floor"))
				}
			}
			if err != nil {
				test.Fatal(err)
			}
			if _, err := OpenStateStore(directory); err == nil {
				test.Fatal("corrupt/missing state reset")
			}
		})
	}
}

func TestStateInterruptedWritePoisonsBeforeReallocation(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	path := filepath.Join(directory, "state.json")
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error { return nil }); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("unknown outcome: %v", err)
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("poisoned snapshot succeeded")
	}
	called := false
	if err := store.Update(func(state *State) error { called = true; return nil }); !errors.Is(err, ErrStateUncertain) || called {
		test.Fatal("poisoned mutation admitted")
	}
}

func TestStateRejectsTenantForgeryAndNamespaceKeyDeletion(test *testing.T) {
	store := securityState(test)
	kms := securityKMS(test)
	activeSecurityDatabase(test, store, kms)
	if err := store.Update(func(state *State) error {
		database := state.Databases[securityDatabase().Key()]
		database.Project = "beta"
		state.Databases[database.ID.Key()] = database
		return nil
	}); err == nil {
		test.Fatal("cross-tenant DB accepted")
	}
	if err := store.Update(func(state *State) error { delete(state.Keys, namespaceKey(securityDatabase().Namespace)); return nil }); err == nil {
		test.Fatal("key reservation removed")
	}
	if err := store.Update(func(state *State) error {
		database := state.Databases[securityDatabase().Key()]
		database.RootIncarnation = "replacement"
		state.Databases[database.ID.Key()] = database
		return nil
	}); err == nil {
		test.Fatal("root identity replaced")
	}
}
