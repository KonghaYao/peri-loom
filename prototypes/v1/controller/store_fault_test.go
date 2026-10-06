package controller

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestStoreInterruptedMetadataWritePoisonsAndFloorPreventsReallocation(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(directory, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, "metadata.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "metadata.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 90; return nil }); !errors.Is(err, ErrStoreUncertain) {
		t.Fatalf("write uncertainty: %v", err)
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrStoreUncertain) {
		t.Fatalf("poisoned snapshot: %v", err)
	}
	called := false
	if err := store.Update(func(metadata *Metadata) error { called = true; metadata.Epoch++; return nil }); !errors.Is(err, ErrStoreUncertain) || called {
		t.Fatal("poisoned store allocated again")
	}
	var floor epochFloor
	if _, err := readEnvelope(directory, "epoch-floor.json", &floor); err != nil || floor.Epoch != 90 || floor.Revision != 1 {
		t.Fatalf("floor not durable before metadata: %+v %v", floor, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, "metadata.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), old, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(directory); err == nil {
		t.Fatal("incomplete state silently reallocated")
	}
}

func TestStoreProcessLockChild(t *testing.T) {
	directory := os.Getenv("PERI_STORE_TEST_DIRECTORY")
	if directory == "" {
		t.Skip("subprocess helper")
	}
	if store, err := OpenStore(directory); !errors.Is(err, ErrStoreLocked) {
		if store != nil {
			store.Close()
		}
		t.Fatalf("cross-process lock missing: %v", err)
	}
}

func TestStoreLockExcludesOtherProcess(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	command := exec.Command(os.Args[0], "-test.run=^TestStoreProcessLockChild$")
	command.Env = []string{"PERI_STORE_TEST_DIRECTORY=" + directory}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock child: %s %v", output, err)
	}
}

func TestStorePrivateFilesAndIncompleteInitialization(t *testing.T) {
	for _, kind := range []string{"marker-only", "foreign-file", "symlink-root", "unsafe-file", "unsafe-directory"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			switch kind {
			case "marker-only":
				if err := os.Chmod(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign-file":
				if err := os.WriteFile(filepath.Join(directory, "user-data"), []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink-root":
				link := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(directory, link); err != nil {
					t.Fatal(err)
				}
				directory = link
			default:
				store, err := OpenStore(directory)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(directory, "metadata.json")
				mode := os.FileMode(0644)
				if kind == "unsafe-directory" {
					path = directory
					mode = 0755
				}
				if err := os.Chmod(path, mode); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenStore(directory); err == nil {
				t.Fatal("unsafe or incomplete state reset")
			}
		})
	}
}

func TestStoreCallerRetainedMutationAndNestedSnapshotCannotAlterState(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var retained *Metadata
	database := DatabaseID{Namespace: Namespace{Tenant: "tenant", Database: "database"}, Branch: "main"}
	if err := store.Update(func(metadata *Metadata) error {
		retained = metadata
		metadata.Epoch = 1
		metadata.Placements[database.Key()] = Placement{Database: database, DesiredWorker: "worker", Epoch: 1, TTLMS: 1000, DesiredState: "active", Phase: "grant_pending", Role: "writer", PendingID: "request", PendingCommand: json.RawMessage(`{"request_id":"request","nonce":"test-nonce"}`)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retained.Epoch = 900
	delete(retained.Placements, database.Key())
	first, _ := store.Snapshot()
	placement := first.Placements[database.Key()]
	placement.PendingCommand[0] = 'x'
	first.Placements[database.Key()] = placement
	second, err := store.Snapshot()
	if err != nil || second.Epoch != 1 || !json.Valid(second.Placements[database.Key()].PendingCommand) {
		t.Fatalf("aliased nested snapshot: %v", err)
	}
}

func TestStoreEpochAndRevisionExhaustionRefusesWraparound(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = math.MaxUint64; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err == nil {
		t.Fatal("epoch wrapped")
	}
	durable := store.(*diskStore)
	durable.metadata.Revision = math.MaxUint64
	if err := store.Update(func(metadata *Metadata) error { return nil }); err == nil {
		t.Fatal("revision wrapped")
	}
}
