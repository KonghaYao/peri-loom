package controller

import (
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

func TestStoreChecksumAndDurableFloorDetectRollback(t *testing.T) {
	for _, corruption := range []string{"checksum", "rollback", "missing-floor", "missing-both"} {
		t.Run(corruption, func(t *testing.T) {
			directory := t.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			old, err := os.ReadFile(filepath.Join(directory, "metadata.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 11; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "checksum":
				err = os.WriteFile(filepath.Join(directory, "metadata.json"), []byte(`{"version":1}`), 0600)
			case "rollback":
				err = os.WriteFile(filepath.Join(directory, "metadata.json"), old, 0600)
			case "missing-floor":
				err = os.Remove(filepath.Join(directory, "epoch-floor.json"))
			case "missing-both":
				err = os.Remove(filepath.Join(directory, "metadata.json"))
				if err == nil {
					err = os.Remove(filepath.Join(directory, "epoch-floor.json"))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenStore(directory); err == nil {
				t.Fatal("corruption/rollback reset")
			}
		})
	}
}
