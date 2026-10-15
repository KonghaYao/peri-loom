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

func TestStateLockReopenAndConcurrentRevisions(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := OpenStateStore(directory); !errors.Is(err, ErrStateLocked) {
		test.Fatalf("lock missing: %v", err)
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
	if err != nil || state.Revision != 16 {
		test.Fatalf("reopened revision: %d %v", state.Revision, err)
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
