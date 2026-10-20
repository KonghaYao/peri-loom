package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStorePlacementEpochsAndCredentialsValidatedBeforePersistence(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	database := DatabaseID{Namespace: Namespace{Tenant: "tenant", Database: "database"}, Branch: "main"}
	placement := Placement{Database: database, DesiredWorker: "worker", Epoch: 10, TTLMS: 1000, DesiredState: "active", Phase: "grant_pending", Role: "writer", PendingID: "request", PendingCommand: json.RawMessage(`{"request_id":"request","nonce":"public-test-nonce"}`)}
	if err := store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 10
		metadata.Placements[database.Key()] = placement
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*Metadata){
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.TTLMS = 99
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.PendingID = ""
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.Phase = ""
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.Epoch = 9
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.Epoch = 11
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) {
			candidate := metadata.Placements[database.Key()]
			candidate.PendingCommand = json.RawMessage(`{"nested":[{"secret_key":"pure-test-secret"}]}`)
			metadata.Placements[database.Key()] = candidate
		},
		func(metadata *Metadata) { metadata.Workers = nil },
		func(metadata *Metadata) { metadata.Version = 2 },
	} {
		if err := store.Update(func(metadata *Metadata) error { mutation(metadata); return nil }); err == nil {
			t.Fatal("invalid metadata persisted")
		}
	}
	actual, err := store.Snapshot()
	if err != nil || actual.Revision != 1 || actual.Epoch != 10 {
		t.Fatalf("validation mutated state: %+v %v", actual, err)
	}
}

func TestStoreJournalRejectsEpochAndRevisionRollback(t *testing.T) {
	for _, mutation := range []string{"epoch-decrease", "revision-duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 10; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			journalPath := filepath.Join(directory, metadataJournalName)
			records, _, _, err := readJournalFile(journalPath)
			if err != nil || len(records) == 0 {
				t.Fatalf("journal fixture: %v", err)
			}
			last := records[len(records)-1]
			var line []byte
			switch mutation {
			case "epoch-decrease":
				epoch := uint64(9)
				line, _, err = encodeJournalRecord(last.revision+1, last.checksum, metadataDelta{Epoch: &epoch})
			case "revision-duplicate":
				line, _, err = encodeJournalRecord(last.revision, last.checksum, metadataDelta{})
			}
			if err == nil {
				err = appendJournalBytes(journalPath, line)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenStore(directory); err == nil {
				t.Fatal("journal rollback was accepted")
			}
		})
	}
}

func TestStoreUnknownSchemaAndTempFileRecovery(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "new-parent", "private")
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ".state-crashed.tmp"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, _, err := encodeEnvelope(0, map[string]any{"version": 1, "revision": 0, "epoch": 0, "workers": map[string]any{}, "placements": map[string]any{}, "credentials": "pure-test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(directory); err == nil {
		t.Fatal("unknown metadata schema accepted")
	}
}

func TestStoreClosedOperationsAndDiskPermissions(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metadata.json", "metadata.journal", "metadata.lock"} {
		info, err := os.Stat(filepath.Join(directory, name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private file mode: %v", err)
		}
	}
	info, err := os.Stat(directory)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("private directory mode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(); err == nil {
		t.Fatal("closed snapshot")
	}
	if err := store.Update(func(metadata *Metadata) error { return nil }); err == nil {
		t.Fatal("closed update")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
