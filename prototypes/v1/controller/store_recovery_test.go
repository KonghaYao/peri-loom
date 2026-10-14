package controller

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

type recoveryIntentFixture struct {
	Version uint32    `json:"version"`
	Before  *Metadata `json:"before"`
	After   Metadata  `json:"after"`
}

func recoveryFiles(test *testing.T, metadata Metadata) ([]byte, []byte) {
	test.Helper()
	encoded, checksum, err := encodeEnvelope(metadata.Revision, metadata)
	if err != nil {
		test.Fatal(err)
	}
	floor, _, err := encodeEnvelope(metadata.Revision, epochFloor{Version: 1, Revision: metadata.Revision, Epoch: metadata.Epoch, MetadataChecksum: checksum})
	if err != nil {
		test.Fatal(err)
	}
	return encoded, floor
}

func TestStoreRecoveryRollsForwardOnlyDurableCommitPrefixes(test *testing.T) {
	for _, prefix := range []string{"intent", "floor", "metadata"} {
		test.Run(prefix, func(test *testing.T) {
			directory := test.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 4; return nil }); err != nil {
				test.Fatal(err)
			}
			before, err := store.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			after, err := cloneMetadata(before)
			if err != nil {
				test.Fatal(err)
			}
			after.Revision++
			after.Epoch++
			intent, _, err := encodeEnvelope(after.Revision, recoveryIntentFixture{Version: 1, Before: &before, After: after})
			if err != nil {
				test.Fatal(err)
			}
			if err := atomicFile(directory, "metadata.pending.json", intent); err != nil {
				test.Fatal(err)
			}
			metadata, floor := recoveryFiles(test, after)
			if prefix != "intent" {
				if err := atomicFile(directory, "epoch-floor.json", floor); err != nil {
					test.Fatal(err)
				}
			}
			if prefix == "metadata" {
				if err := atomicFile(directory, "metadata.json", metadata); err != nil {
					test.Fatal(err)
				}
			}
			for reload := 0; reload < 2; reload++ {
				recovered, err := OpenStore(directory)
				if err != nil {
					test.Fatal(err)
				}
				actual, err := recovered.Snapshot()
				if err != nil || actual.Revision != after.Revision || actual.Epoch != after.Epoch {
					test.Fatalf("durable candidate not recovered: %+v %v", actual, err)
				}
				if err := recovered.Close(); err != nil {
					test.Fatal(err)
				}
			}
			if _, err := os.Lstat(filepath.Join(directory, "metadata.pending.json")); !os.IsNotExist(err) {
				test.Fatal("completed intent was not durably removed")
			}
		})
	}
}

func TestStoreRecoveryRejectsUnrelatedOrInvalidIntent(test *testing.T) {
	for _, mutation := range []string{"revision_gap", "epoch_rollback", "missing_metadata", "missing_floor", "reversed_prefix", "stale_intent", "corrupt_intent", "symlink_intent"} {
		test.Run(mutation, func(test *testing.T) {
			directory := test.TempDir()
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 4; return nil }); err != nil {
				test.Fatal(err)
			}
			before, err := store.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			if err := store.Close(); err != nil {
				test.Fatal(err)
			}
			after, err := cloneMetadata(before)
			if err != nil {
				test.Fatal(err)
			}
			after.Revision++
			after.Epoch++
			switch mutation {
			case "revision_gap":
				after.Revision++
			case "epoch_rollback":
				after.Epoch = before.Epoch - 1
			case "missing_metadata":
				if err := os.Remove(filepath.Join(directory, "metadata.json")); err != nil {
					test.Fatal(err)
				}
			case "missing_floor":
				if err := os.Remove(filepath.Join(directory, "epoch-floor.json")); err != nil {
					test.Fatal(err)
				}
			case "reversed_prefix":
				metadata, _ := recoveryFiles(test, after)
				if err := atomicFile(directory, "metadata.json", metadata); err != nil {
					test.Fatal(err)
				}
			case "stale_intent":
				future, err := cloneMetadata(after)
				if err != nil {
					test.Fatal(err)
				}
				future.Revision++
				metadata, floor := recoveryFiles(test, future)
				if err := atomicFile(directory, "epoch-floor.json", floor); err != nil {
					test.Fatal(err)
				}
				if err := atomicFile(directory, "metadata.json", metadata); err != nil {
					test.Fatal(err)
				}
			}
			intent, _, err := encodeEnvelope(after.Revision, recoveryIntentFixture{Version: 1, Before: &before, After: after})
			if err != nil {
				test.Fatal(err)
			}
			if mutation == "corrupt_intent" {
				intent = []byte(`{"version":1}`)
			}
			if mutation == "symlink_intent" {
				if err := os.Symlink(filepath.Join(directory, "metadata.json"), filepath.Join(directory, "metadata.pending.json")); err != nil {
					test.Fatal(err)
				}
			} else if err := atomicFile(directory, "metadata.pending.json", intent); err != nil {
				test.Fatal(err)
			}
			if recovered, err := OpenStore(directory); err == nil {
				recovered.Close()
				test.Fatal("invalid recovery proof authorized state adoption")
			}
		})
	}
}

func TestStoreRecoveryInitialCommitRequiresCompleteProof(test *testing.T) {
	for _, prefix := range []string{"intent", "floor", "metadata"} {
		test.Run(prefix, func(test *testing.T) {
			directory := test.TempDir()
			if err := os.Chmod(directory, 0700); err != nil {
				test.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "metadata.lock"), nil, 0600); err != nil {
				test.Fatal(err)
			}
			initial := Metadata{Version: 1, Workers: make(map[string]Worker), Placements: make(map[string]Placement)}
			intent, _, err := encodeEnvelope(0, recoveryIntentFixture{Version: 1, After: initial})
			if err != nil {
				test.Fatal(err)
			}
			if err := atomicFile(directory, metadataPendingName, intent); err != nil {
				test.Fatal(err)
			}
			metadata, floor := recoveryFiles(test, initial)
			if prefix != "intent" {
				if err := atomicFile(directory, "epoch-floor.json", floor); err != nil {
					test.Fatal(err)
				}
			}
			if prefix == "metadata" {
				if err := atomicFile(directory, "metadata.json", metadata); err != nil {
					test.Fatal(err)
				}
			}
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			defer store.Close()
			actual, err := store.Snapshot()
			if err != nil || actual.Epoch != 0 || actual.Revision != 0 {
				test.Fatalf("invalid recovered initialization: %+v %v", actual, err)
			}
		})
	}
}

func TestStoreRecoverySIGKILLWriter(test *testing.T) {
	directory := os.Getenv("PERI_CONTROLLER_COMMIT_KILL_DIRECTORY")
	if directory == "" {
		test.Skip("subprocess helper")
	}
	store, err := OpenStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(metadata *Metadata) error { metadata.Epoch = 9; return nil }); err != nil {
		test.Fatal(err)
	}
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	after, err := cloneMetadata(before)
	if err != nil {
		test.Fatal(err)
	}
	after.Revision++
	after.Epoch++
	intent := metadataCommit{Version: 1, Before: &before, After: after}
	if err := validateMetadataCommit(intent, after.Revision); err != nil {
		test.Fatal(err)
	}
	encodedIntent, _, err := encodeEnvelope(after.Revision, intent)
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicFile(directory, metadataPendingName, encodedIntent); err != nil {
		test.Fatal(err)
	}
	metadata, floor, err := metadataCommitFiles(after)
	if err != nil {
		test.Fatal(err)
	}
	prefix := os.Getenv("PERI_CONTROLLER_COMMIT_KILL_PREFIX")
	if prefix != "intent" {
		if err := atomicFile(directory, "epoch-floor.json", floor); err != nil {
			test.Fatal(err)
		}
	}
	if prefix == "metadata" {
		if err := atomicFile(directory, "metadata.json", metadata); err != nil {
			test.Fatal(err)
		}
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		test.Fatal(err)
	}
	test.Fatal("SIGKILL did not terminate writer")
}

func TestStoreRecoveryActualSIGKILLReleasesLockAndPreservesAllocatedFloor(test *testing.T) {
	for _, prefix := range []string{"intent", "floor", "metadata"} {
		test.Run(prefix, func(test *testing.T) {
			directory := test.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestStoreRecoverySIGKILLWriter$")
			command.Env = []string{"PERI_CONTROLLER_COMMIT_KILL_DIRECTORY=" + directory, "PERI_CONTROLLER_COMMIT_KILL_PREFIX=" + prefix}
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				test.Fatalf("writer did not exit by SIGKILL: %s %v", output, err)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				test.Fatalf("wrong process exit: %s %v", output, err)
			}
			store, err := OpenStore(directory)
			if err != nil {
				test.Fatal(err)
			}
			defer store.Close()
			actual, err := store.Snapshot()
			if err != nil || actual.Epoch != 10 || actual.Revision != 2 {
				test.Fatalf("crash lost durable allocation: %+v %v", actual, err)
			}
			if err := store.Update(func(metadata *Metadata) error { metadata.Epoch++; return nil }); err != nil {
				test.Fatal(err)
			}
			actual, err = store.Snapshot()
			if err != nil || actual.Epoch != 11 || actual.Revision != 3 {
				test.Fatalf("recovered allocation reused an old epoch: %+v %v", actual, err)
			}
		})
	}
}
