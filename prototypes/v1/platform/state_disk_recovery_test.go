package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type durableRecoveryFixtureIntent struct {
	Version uint32        `json:"version"`
	Target  string        `json:"target"`
	Before  *diskEnvelope `json:"before"`
	After   diskEnvelope  `json:"after"`
}

func durableRecoveryFixture(test *testing.T) (string, diskEnvelope, diskEnvelope) {
	test.Helper()
	store := operationDurableStore(test)
	database := securityDatabase()
	if err := beginOperation(store, database, "lost", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, "lost", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	// Recovery fixtures exercise registry commits, so checkpoint the operation
	// sidecar into the registry before constructing the interrupted commit.
	if err := store.Update(func(*State) error { return nil }); err != nil {
		test.Fatal(err)
	}
	path := store.(*stateStore).disk.path
	var before diskEnvelope
	if err := readPrivate(path, &before); err != nil {
		test.Fatal(err)
	}
	var after State
	if err := strictDecode(before.Payload, &after); err != nil {
		test.Fatal(err)
	}
	after.Revision++
	encoded, err := json.Marshal(after)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	return path, before, diskEnvelope{Version: 1, Revision: after.Revision, Checksum: digestHex(encoded), Payload: encoded}
}

func durableRecoveryWriteJSON(test *testing.T, path string, value any) {
	test.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicPrivate(path, encoded); err != nil {
		test.Fatal(err)
	}
}

func durableRecoveryWriteIntent(test *testing.T, path string, before *diskEnvelope, after diskEnvelope) {
	test.Helper()
	intent := durableRecoveryFixtureIntent{Version: 1, Target: path, Before: before, After: after}
	encoded, err := json.Marshal(intent)
	if err != nil {
		test.Fatal(err)
	}
	durableRecoveryWriteJSON(test, path+".pending", diskEnvelope{Version: 1, Revision: after.Revision, Checksum: digestHex(encoded), Payload: encoded})
}

func TestDurableRecoveryLegalPrefixesPreserveUnknown(test *testing.T) {
	for _, prefix := range []string{"old_old", "old_new", "new_new"} {
		test.Run(prefix, func(test *testing.T) {
			path, before, after := durableRecoveryFixture(test)
			durableRecoveryWriteIntent(test, path, &before, after)
			if prefix != "old_old" {
				durableRecoveryWriteJSON(test, path+".floor", diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum})
			}
			if prefix == "new_new" {
				durableRecoveryWriteJSON(test, path, after)
			}
			store, err := OpenStateStore(filepath.Dir(path))
			if err != nil {
				test.Fatal(err)
			}
			defer store.Close()
			state, err := store.Snapshot()
			if err != nil || state.Revision != after.Revision || state.Operations[operationKey(securityDatabase(), "lost")].State != "quarantined" {
				test.Fatal("journal recovery lost Unknown or failed to advance")
			}
			if _, err := os.Lstat(path + ".pending"); !os.IsNotExist(err) {
				test.Fatal("durable recovery did not consume exact intent")
			}
		})
	}
}

func TestDurableRecoveryNoIntentRollbackStillRejected(test *testing.T) {
	path, _, after := durableRecoveryFixture(test)
	durableRecoveryWriteJSON(test, path+".floor", diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum})
	if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
		store.Close()
		test.Fatal("floor-first rollback accepted without complete intent")
	}
}

func TestDurableRecoveryInvalidIntentNeverMutatesFiles(test *testing.T) {
	for _, scenario := range []string{"new_old", "missing_payload", "missing_floor", "missing_both", "unsafe_intent", "symlink_intent", "unsafe_payload", "directory_floor", "wrong_target", "wrong_parent", "revision_jump", "checksum", "marker_only", "temp_only", "drop_unknown", "duplicate_json", "physical_limit"} {
		test.Run(scenario, func(test *testing.T) {
			path, before, after := durableRecoveryFixture(test)
			durableRecoveryWriteIntent(test, path, &before, after)
			switch scenario {
			case "new_old":
				durableRecoveryWriteJSON(test, path, after)
			case "missing_payload":
				if err := os.Remove(path); err != nil {
					test.Fatal(err)
				}
			case "missing_floor":
				if err := os.Remove(path + ".floor"); err != nil {
					test.Fatal(err)
				}
			case "missing_both":
				for _, target := range []string{path, path + ".floor"} {
					if err := os.Remove(target); err != nil {
						test.Fatal(err)
					}
				}
			case "unsafe_intent":
				if err := os.Chmod(path+".pending", 0644); err != nil {
					test.Fatal(err)
				}
			case "symlink_intent":
				if err := os.Rename(path+".pending", filepath.Join(filepath.Dir(path), ".private-proof.tmp")); err != nil {
					test.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(filepath.Dir(path), ".private-proof.tmp"), path+".pending"); err != nil {
					test.Fatal(err)
				}
			case "unsafe_payload":
				if err := os.Chmod(path, 0644); err != nil {
					test.Fatal(err)
				}
			case "directory_floor":
				if err := os.Remove(path + ".floor"); err != nil {
					test.Fatal(err)
				}
				if err := os.Mkdir(path+".floor", 0700); err != nil {
					test.Fatal(err)
				}
			case "wrong_target":
				var wrapped diskEnvelope
				if err := readPrivate(path+".pending", &wrapped); err != nil {
					test.Fatal(err)
				}
				var intent durableRecoveryFixtureIntent
				if err := strictDecode(wrapped.Payload, &intent); err != nil {
					test.Fatal(err)
				}
				intent.Target = filepath.Join(filepath.Dir(path), "foreign.json")
				encoded, _ := json.Marshal(intent)
				wrapped.Payload, wrapped.Checksum = encoded, digestHex(encoded)
				durableRecoveryWriteJSON(test, path+".pending", wrapped)
			case "wrong_parent":
				var parent State
				strictDecode(before.Payload, &parent)
				parent.Revision--
				encoded, _ := json.Marshal(parent)
				before.Revision, before.Payload, before.Checksum = parent.Revision, encoded, digestHex(encoded)
				durableRecoveryWriteIntent(test, path, &before, after)
			case "revision_jump":
				var state State
				strictDecode(after.Payload, &state)
				state.Revision++
				encoded, _ := json.Marshal(state)
				after.Revision, after.Payload, after.Checksum = state.Revision, encoded, digestHex(encoded)
				durableRecoveryWriteIntent(test, path, &before, after)
			case "checksum":
				after.Checksum = strings.Repeat("0", 64)
				durableRecoveryWriteIntent(test, path, &before, after)
			case "marker_only":
				durableRecoveryWriteJSON(test, path+".pending", map[string]any{"version": 1, "revision": after.Revision, "checksum": after.Checksum})
			case "temp_only":
				if err := os.Rename(path+".pending", filepath.Join(filepath.Dir(path), ".private-proof.tmp")); err != nil {
					test.Fatal(err)
				}
				durableRecoveryWriteJSON(test, path+".floor", diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum})
			case "drop_unknown":
				var state State
				strictDecode(after.Payload, &state)
				delete(state.Operations, operationKey(securityDatabase(), "lost"))
				encoded, _ := json.Marshal(state)
				after.Payload, after.Checksum = encoded, digestHex(encoded)
				durableRecoveryWriteIntent(test, path, &before, after)
			case "duplicate_json":
				encoded, err := os.ReadFile(path + ".pending")
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(path+".pending", bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), 0600); err != nil {
					test.Fatal(err)
				}
			case "physical_limit":
				file, err := os.OpenFile(path+".pending", os.O_WRONLY, 0600)
				if err != nil {
					test.Fatal(err)
				}
				if err := file.Truncate(durableIntentBytes + 1); err != nil {
					test.Fatal(err)
				}
				file.Close()
			}
			payloadBefore, _ := os.ReadFile(path)
			floorBefore, _ := os.ReadFile(path + ".floor")
			if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
				store.Close()
				test.Fatal("invalid/unsafe intent accepted")
			}
			payloadAfter, _ := os.ReadFile(path)
			floorAfter, _ := os.ReadFile(path + ".floor")
			if !bytes.Equal(payloadBefore, payloadAfter) || !bytes.Equal(floorBefore, floorAfter) {
				test.Fatal("invalid intent altered durable state/floor")
			}
		})
	}
}

func TestDurableRecoveryFailurePoisonsUntilVerifiedReopen(test *testing.T) {
	store := operationDurableStore(test)
	path := store.(*stateStore).disk.path
	var before diskEnvelope
	if err := readPrivate(path, &before); err != nil {
		test.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error { return nil }); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("failed commit not uncertain")
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("uncertain store stayed readable")
	}
	called := false
	if err := store.Update(func(state *State) error { called = true; return nil }); !errors.Is(err, ErrStateUncertain) || called {
		test.Fatal("uncertain store dispatched another mutation")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	if recovered, err := OpenStateStore(filepath.Dir(path)); err == nil {
		recovered.Close()
		test.Fatal("unsafe payload became recovery proof")
	}
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	durableRecoveryWriteJSON(test, path, before)
	recovered, err := OpenStateStore(filepath.Dir(path))
	if err != nil {
		test.Fatal(err)
	}
	defer recovered.Close()
	state, err := recovered.Snapshot()
	if err != nil || state.Revision != before.Revision+1 {
		test.Fatal("exact prefix did not recover durable next revision")
	}
}

func TestDurableRecoveryAuditHeadRequiresActualAppendChain(test *testing.T) {
	for _, appendState := range []string{"complete", "absent", "partial", "tampered"} {
		test.Run(appendState, func(test *testing.T) {
			path := filepath.Join(test.TempDir(), "events.jsonl")
			audit, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			var before diskEnvelope
			if err := readPrivate(path+".head", &before); err != nil {
				test.Fatal(err)
			}
			if err := audit.Close(); err != nil {
				test.Fatal(err)
			}
			payload := auditPayload{Version: 1, Sequence: 1, Previous: strings.Repeat("0", 64), Event: auditFixtureEvent()}
			encoded, _ := json.Marshal(payload)
			hash := digestHex(encoded)
			line, _ := json.Marshal(auditRecord{Payload: payload, Hash: hash})
			line = append(line, '\n')
			headPayload, _ := json.Marshal(auditHead{Version: 1, Sequence: 1, Hash: hash})
			after := diskEnvelope{Version: 1, Revision: 1, Checksum: digestHex(headPayload), Payload: headPayload}
			durableRecoveryWriteIntent(test, path+".head", &before, after)
			durableRecoveryWriteJSON(test, path+".head.floor", diskFloor{Version: 1, Revision: 1, Checksum: after.Checksum})
			if appendState != "absent" {
				if appendState == "partial" {
					line = line[:len(line)-1]
				}
				if appendState == "tampered" {
					line = bytes.Replace(line, []byte(`"outcome":"ok"`), []byte(`"outcome":"failed"`), 1)
				}
				if err := os.WriteFile(path, line, 0600); err != nil {
					test.Fatal(err)
				}
			}
			chainBefore, err := os.ReadFile(path)
			if err != nil {
				test.Fatal(err)
			}
			reopened, err := OpenAudit(path)
			if appendState != "complete" {
				if err == nil {
					reopened.Close()
					test.Fatal("head intent fabricated audit append ACK")
				}
				var current diskEnvelope
				if err := readPrivate(path+".head", &current); err != nil || current.Revision != 1 {
					test.Fatal("unknown append lowered durable head")
				}
				chainAfter, _ := os.ReadFile(path)
				if !bytes.Equal(chainBefore, chainAfter) {
					test.Fatal("generic recovery appended or rewrote audit chain")
				}
				return
			}
			if err != nil {
				test.Fatal(err)
			}
			defer reopened.Close()
			if reopened.(*fileAudit).state.Sequence != 1 {
				test.Fatal("committed append sequence changed")
			}
			chainAfter, _ := os.ReadFile(path)
			if !bytes.Equal(chainBefore, chainAfter) {
				test.Fatal("audit recovery replayed append")
			}
			if err := reopened.Record(auditFixtureEvent()); err != nil {
				test.Fatal(err)
			}
			if reopened.(*fileAudit).state.Sequence != 2 {
				test.Fatal("new append did not advance exactly once")
			}
		})
	}
}

func TestDurableRecoveryCannotExceedBusinessPayloadLimit(test *testing.T) {
	path, before, after := durableRecoveryFixture(test)
	after.Payload = json.RawMessage(`"` + strings.Repeat("x", durablePayloadBytes) + `"`)
	after.Checksum = digestHex(after.Payload)
	durableRecoveryWriteIntent(test, path, &before, after)
	if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
		store.Close()
		test.Fatal("journal physical limit expanded business payload limit")
	}
}

func TestDurableRecoveryInitializationRequiresCompleteExactIntent(test *testing.T) {
	for _, kind := range []string{"state", "audit", "kms"} {
		for _, prefix := range []string{"missing_missing", "missing_new", "new_new"} {
			test.Run(kind+"/"+prefix, func(test *testing.T) {
				directory := test.TempDir()
				path := filepath.Join(directory, "state.json")
				var initial any = State{Version: 1, Organizations: map[string]Organization{}, Projects: map[string]Project{}, Databases: map[string]Database{}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{}}
				if kind == "audit" {
					path = filepath.Join(directory, "events.head")
					initial = auditHead{Version: 1, Hash: strings.Repeat("0", 64)}
				}
				if kind == "kms" {
					path = filepath.Join(directory, "ring.json")
					initial = keyRing{Version: 1, Active: 1, Keys: map[string]string{"1": strings.Repeat("a", 64)}, Uses: map[string]uint64{"1": 0}}
				}
				disk, err := openDurable(path, initial)
				if err != nil {
					test.Fatal(err)
				}
				var after diskEnvelope
				if err := readPrivate(path, &after); err != nil {
					test.Fatal(err)
				}
				if err := disk.close(); err != nil {
					test.Fatal(err)
				}
				durableRecoveryWriteIntent(test, path, nil, after)
				if prefix != "new_new" {
					if err := os.Remove(path); err != nil {
						test.Fatal(err)
					}
				}
				if prefix == "missing_missing" {
					if err := os.Remove(path + ".floor"); err != nil {
						test.Fatal(err)
					}
				}
				recovered, err := openDurable(path, initial)
				if err != nil {
					test.Fatal(err)
				}
				defer recovered.close()
				if !recovered.fresh || recovered.revision != 0 || !bytes.Equal(recovered.payload, after.Payload) {
					test.Fatal("initial recovery reset or changed initialization")
				}
			})
		}
	}
	for _, scenario := range []string{"marker_only", "temp_only", "nonempty_state", "normal_update_as_init"} {
		test.Run(scenario, func(test *testing.T) {
			path, before, after := durableRecoveryFixture(test)
			if scenario == "normal_update_as_init" {
				initial := State{Version: 1, Organizations: map[string]Organization{}, Projects: map[string]Project{}, Databases: map[string]Database{}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{}}
				encoded, _ := json.Marshal(initial)
				after = diskEnvelope{Version: 1, Checksum: digestHex(encoded), Payload: encoded}
			} else {
				var state State
				strictDecode(after.Payload, &state)
				state.Revision = 0
				encoded, _ := json.Marshal(state)
				after = diskEnvelope{Version: 1, Checksum: digestHex(encoded), Payload: encoded}
			}
			durableRecoveryWriteIntent(test, path, nil, after)
			if scenario == "marker_only" || scenario == "temp_only" {
				for _, target := range []string{path, path + ".floor"} {
					if err := os.Remove(target); err != nil {
						test.Fatal(err)
					}
				}
				if scenario == "marker_only" {
					if err := os.Remove(path + ".pending"); err != nil {
						test.Fatal(err)
					}
				} else {
					if err := os.Rename(path+".pending", filepath.Join(filepath.Dir(path), ".private-init.tmp")); err != nil {
						test.Fatal(err)
					}
				}
			}
			if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
				store.Close()
				test.Fatal("invalid initialization reset established Unknown")
			}
			if scenario == "nonempty_state" || scenario == "normal_update_as_init" {
				var observed diskEnvelope
				if err := readPrivate(path, &observed); err != nil || !bytes.Equal(observed.Payload, before.Payload) {
					test.Fatal("invalid init altered established state")
				}
			}
		})
	}
}
