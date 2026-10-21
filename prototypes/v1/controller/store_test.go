package controller

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStoreAtomicReopenAndSnapshotsAreDetached(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error {
		metadata.Epoch = 10
		metadata.Workers["worker"] = Worker{ID: "worker", Address: "http://127.0.0.1:9001", Capacity: 2}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first.Workers["worker"] = Worker{}
	second, err := store.Snapshot()
	if err != nil || second.Workers["worker"].ID != "worker" {
		t.Fatalf("snapshot aliases state: %v", err)
	}
	if second.Revision != 1 || second.Epoch != 10 {
		t.Fatalf("unexpected state: %+v", second)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	actual, err := reopened.Snapshot()
	if err != nil || actual.Revision != 1 || actual.Epoch != 10 {
		t.Fatalf("reopen: %+v %v", actual, err)
	}
}

func TestStoreLifetimeLockAndMissingStateNeverReset(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(directory); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, "metadata.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(directory); err == nil {
		t.Fatal("missing metadata reset")
	}
}

func TestStoreConcurrentEpochsAndCallbackRollback(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var group sync.WaitGroup
	for index := 0; index < 24; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 1; return nil }); err == nil {
		t.Fatal("epoch rollback accepted")
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Revision++; return nil }); err == nil {
		t.Fatal("caller revision accepted")
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return errors.New("abort") }); err == nil {
		t.Fatal("callback error swallowed")
	}
	actual, err := store.Snapshot()
	if err != nil || actual.Epoch != 24 || actual.Revision != 24 {
		t.Fatalf("concurrent state: %+v %v", actual, err)
	}
}

func TestStoreJournalCorruptionAndMissingSnapshotRefused(t *testing.T) {
	for _, corruption := range []string{"snapshot-checksum", "journal-checksum", "journal-gap", "missing-snapshot", "unsupported-snapshot"} {
		t.Run(corruption, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 11; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			journalPath := filepath.Join(directory, metadataJournalName)
			switch corruption {
			case "snapshot-checksum":
				err = os.WriteFile(filepath.Join(directory, metadataSnapshotName), []byte(`{"version":2}`), 0600)
			case "journal-checksum":
				err = appendJournalBytes(journalPath, []byte(`{"version":1,"revision":2,"prev":"broken","checksum":"deadbeef","delta":{}}`+"\n"))
			case "journal-gap":
				records, _, _, readErr := readJournalFile(journalPath)
				if readErr != nil || len(records) == 0 {
					t.Fatalf("journal fixture: %v", readErr)
				}
				var line []byte
				line, _, err = encodeJournalRecord(3, records[0].checksum, metadataDelta{})
				if err == nil {
					err = appendJournalBytes(journalPath, line)
				}
			case "missing-snapshot":
				err = os.Remove(filepath.Join(directory, metadataSnapshotName))
			case "unsupported-snapshot":
				payload := []byte(`{}`)
				encoded, marshalErr := json.Marshal(stateEnvelope{Version: 3, Revision: 1, Checksum: sha256Sum(payload), Payload: payload})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				err = os.WriteFile(filepath.Join(directory, metadataSnapshotName), encoded, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenStore(directory); err == nil {
				t.Fatal("corruption or unsupported state was accepted")
			}
		})
	}
}

func TestStoreJournalReplaysDurablePrefixAfterSnapshotRollback(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(directory, metadataSnapshotName))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 11; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, metadataSnapshotName), old, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	actual, err := reopened.Snapshot()
	if err != nil || actual.Revision != 1 || actual.Epoch != 11 {
		t.Fatalf("journal did not restore the durable revision: %+v %v", actual, err)
	}
}

func appendJournalBytes(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Sync(), file.Close())
}
