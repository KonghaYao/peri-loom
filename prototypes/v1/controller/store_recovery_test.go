package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeLegacyState(test *testing.T, directory string, metadata Metadata) {
	test.Helper()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
		test.Fatal(err)
	}
	encoded, encodedFloor, err := metadataCommitFiles(metadata)
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, metadataSnapshotName), encoded, 0600); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "epoch-floor.json"), encodedFloor, 0600); err != nil {
		test.Fatal(err)
	}
}

func emptyMetadata(revision, epoch uint64) Metadata {
	return Metadata{Version: 1, Revision: revision, Epoch: epoch, Workers: make(map[string]Worker), Placements: make(map[string]Placement)}
}

func TestStoreLegacySnapshotMigratesThroughChecksumAndFloor(test *testing.T) {
	directory := test.TempDir()
	metadata := emptyMetadata(2, 5)
	metadata.Workers["worker"] = Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 4}
	writeLegacyState(test, directory, metadata)
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 2 || actual.Epoch != 5 || actual.Workers["worker"].ID != "worker" {
		test.Fatalf("legacy snapshot migration: %+v %v", actual, err)
	}
	snapshot, err := readSnapshotFile(directory)
	if err != nil || snapshot.legacy || snapshot.metadata.Revision != 2 {
		test.Fatalf("legacy snapshot was not rewritten: %v %v", snapshot, err)
	}
	info, err := os.Stat(filepath.Join(directory, metadataJournalName))
	if err != nil || info.Size() != 0 {
		test.Fatalf("journal not initialized: %v %v", info, err)
	}
	if _, err := os.Lstat(filepath.Join(directory, "epoch-floor.json")); !os.IsNotExist(err) {
		test.Fatal("legacy epoch floor survived migration")
	}
	if err := store.Update(func(candidate *Metadata) error { candidate.Epoch++; return nil }); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	actual, err = reopened.Snapshot()
	if err != nil || actual.Revision != 3 || actual.Epoch != 6 {
		test.Fatalf("migrated store did not commit incrementally: %+v %v", actual, err)
	}
}

func TestStoreLegacyMigrationRefusesUnsafeStates(test *testing.T) {
	for _, mutation := range []string{"missing-floor", "floor-mismatch", "unknown-field", "unsupported-version", "journal-conflict", "pending-conflict"} {
		test.Run(mutation, func(test *testing.T) {
			directory := test.TempDir()
			metadata := emptyMetadata(2, 5)
			switch mutation {
			case "missing-floor":
				writeLegacyState(test, directory, metadata)
				if err := os.Remove(filepath.Join(directory, "epoch-floor.json")); err != nil {
					test.Fatal(err)
				}
			case "floor-mismatch":
				writeLegacyState(test, directory, metadata)
				mismatched := emptyMetadata(1, 5)
				_, floor, err := metadataCommitFiles(mismatched)
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "epoch-floor.json"), floor, 0600); err != nil {
					test.Fatal(err)
				}
			case "unknown-field":
				if err := os.Chmod(directory, 0700); err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
					test.Fatal(err)
				}
				payload, err := json.Marshal(struct {
					Metadata
					Credentials string `json:"credentials"`
				}{Metadata: metadata, Credentials: "test-secret"})
				if err != nil {
					test.Fatal(err)
				}
				checksum := sha256Sum(payload)
				wrapped, err := json.Marshal(stateEnvelope{Version: 1, Revision: metadata.Revision, Checksum: checksum, Payload: payload})
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, metadataSnapshotName), wrapped, 0600); err != nil {
					test.Fatal(err)
				}
				floorJSON, _, err := encodeEnvelope(metadata.Revision, epochFloor{Version: 1, Revision: metadata.Revision, Epoch: metadata.Epoch, MetadataChecksum: checksum})
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "epoch-floor.json"), floorJSON, 0600); err != nil {
					test.Fatal(err)
				}
			case "unsupported-version":
				if err := os.Chmod(directory, 0700); err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
					test.Fatal(err)
				}
				payload := []byte(`{}`)
				wrapped, err := json.Marshal(stateEnvelope{Version: 3, Revision: 2, Checksum: sha256Sum(payload), Payload: payload})
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, metadataSnapshotName), wrapped, 0600); err != nil {
					test.Fatal(err)
				}
			case "journal-conflict":
				writeLegacyState(test, directory, metadata)
				if err := os.WriteFile(filepath.Join(directory, metadataJournalName), nil, 0600); err != nil {
					test.Fatal(err)
				}
			case "pending-conflict":
				writeLegacyState(test, directory, metadata)
				if err := os.WriteFile(filepath.Join(directory, metadataPendingName), []byte("{}"), 0600); err != nil {
					test.Fatal(err)
				}
			}
			if _, err := OpenStore(directory); err == nil {
				test.Fatal("unsafe legacy state was migrated")
			}
		})
	}
}

func TestStoreLegacyPendingIntentRecoversBeforeMigration(test *testing.T) {
	directory := test.TempDir()
	before := emptyMetadata(1, 4)
	before.Workers["worker"] = Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 4}
	writeLegacyState(test, directory, before)
	after, err := cloneMetadata(before)
	if err != nil {
		test.Fatal(err)
	}
	after.Revision = 2
	after.Epoch = 5
	intent, _, err := encodeEnvelope(after.Revision, metadataCommit{Version: 1, Before: &before, After: after})
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicFile(directory, metadataPendingName, intent); err != nil {
		test.Fatal(err)
	}
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 2 || actual.Epoch != 5 {
		test.Fatalf("legacy intent was not rolled forward: %+v %v", actual, err)
	}
	if _, err := os.Lstat(filepath.Join(directory, metadataPendingName)); !os.IsNotExist(err) {
		test.Fatal("legacy intent survived recovery")
	}
}

func TestStoreLegacyInitializationIntentMigrates(test *testing.T) {
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
		test.Fatal(err)
	}
	initial := emptyMetadata(0, 0)
	intent, _, err := encodeEnvelope(0, metadataCommit{Version: 1, After: initial})
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicFile(directory, metadataPendingName, intent); err != nil {
		test.Fatal(err)
	}
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 0 || actual.Epoch != 0 {
		test.Fatalf("legacy initialization intent: %+v %v", actual, err)
	}
	snapshot, err := readSnapshotFile(directory)
	if err != nil || snapshot.legacy {
		test.Fatalf("initialization did not migrate: %v %v", snapshot, err)
	}
}
