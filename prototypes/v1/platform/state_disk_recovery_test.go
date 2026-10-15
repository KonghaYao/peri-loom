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

// The payload is durable before the rollback floor advances, so every crash
// prefix the writer can produce must recover: the previous revision when the
// payload never landed, and the next revision (with the floor advanced) when it
// landed before the floor.
func TestDurableRecoveryCommitPrefixes(test *testing.T) {
	for _, prefix := range []string{"old_old", "new_old", "new_new"} {
		test.Run(prefix, func(test *testing.T) {
			path, before, after := durableRecoveryFixture(test)
			if prefix != "old_old" {
				durableRecoveryWriteJSON(test, path, after)
			}
			if prefix == "new_new" {
				durableRecoveryWriteJSON(test, path+".floor", diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum})
			}
			store, err := OpenStateStore(filepath.Dir(path))
			if err != nil {
				test.Fatal(err)
			}
			defer store.Close()
			state, err := store.Snapshot()
			want := before.Revision
			if prefix != "old_old" {
				want = after.Revision
			}
			if err != nil || state.Revision < want || state.Operations[operationKey(securityDatabase(), "lost")].State != "quarantined" {
				test.Fatal("durable recovery lost Unknown or failed to advance")
			}
			var floor diskFloor
			if err := readPrivate(path+".floor", &floor); err != nil {
				test.Fatal(err)
			}
			if floor.Revision > store.(*stateStore).disk.revision {
				test.Fatal("durable recovery left the rollback floor ahead of the payload")
			}
		})
	}
}

func TestDurableRecoveryRollbackStillRejected(test *testing.T) {
	for _, scenario := range []string{"floor_ahead", "floor_checksum_mismatch"} {
		test.Run(scenario, func(test *testing.T) {
			path, _, after := durableRecoveryFixture(test)
			floor := diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum}
			if scenario == "floor_checksum_mismatch" {
				durableRecoveryWriteJSON(test, path, after)
				floor.Checksum = strings.Repeat("0", 64)
			}
			durableRecoveryWriteJSON(test, path+".floor", floor)
			if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
				store.Close()
				test.Fatal("rollback floor accepted")
			}
		})
	}
}

func TestDurableRecoveryUnsafeFilesNeverMutate(test *testing.T) {
	for _, scenario := range []string{"missing_payload", "missing_floor", "missing_both", "unsafe_payload", "unsafe_floor", "symlink_payload", "directory_floor", "leftover_intent", "revision_jump", "checksum", "duplicate_json", "temp_only", "physical_limit"} {
		test.Run(scenario, func(test *testing.T) {
			path, before, after := durableRecoveryFixture(test)
			switch scenario {
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
			case "unsafe_payload":
				if err := os.Chmod(path, 0644); err != nil {
					test.Fatal(err)
				}
			case "unsafe_floor":
				if err := os.Chmod(path+".floor", 0644); err != nil {
					test.Fatal(err)
				}
			case "symlink_payload":
				proof := filepath.Join(filepath.Dir(path), ".private-proof.tmp")
				if err := os.Rename(path, proof); err != nil {
					test.Fatal(err)
				}
				if err := os.Symlink(proof, path); err != nil {
					test.Fatal(err)
				}
			case "directory_floor":
				if err := os.Remove(path + ".floor"); err != nil {
					test.Fatal(err)
				}
				if err := os.Mkdir(path+".floor", 0700); err != nil {
					test.Fatal(err)
				}
			case "leftover_intent":
				durableRecoveryWriteJSON(test, path+".pending", diskEnvelope{Version: 1, Revision: after.Revision, Checksum: after.Checksum, Payload: after.Payload})
			case "revision_jump":
				var state State
				if err := strictDecode(after.Payload, &state); err != nil {
					test.Fatal(err)
				}
				state.Revision++
				encoded, _ := json.Marshal(state)
				durableRecoveryWriteJSON(test, path, diskEnvelope{Version: 1, Revision: state.Revision, Checksum: digestHex(encoded), Payload: encoded})
			case "checksum":
				after.Checksum = strings.Repeat("0", 64)
				durableRecoveryWriteJSON(test, path, after)
			case "duplicate_json":
				encoded, err := os.ReadFile(path)
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(path, bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), 0600); err != nil {
					test.Fatal(err)
				}
			case "temp_only":
				if err := os.Rename(path, filepath.Join(filepath.Dir(path), ".private-proof.tmp")); err != nil {
					test.Fatal(err)
				}
				durableRecoveryWriteJSON(test, path+".floor", diskFloor{Version: 1, Revision: after.Revision, Checksum: after.Checksum})
			case "physical_limit":
				file, err := os.OpenFile(path, os.O_WRONLY, 0600)
				if err != nil {
					test.Fatal(err)
				}
				if err := file.Truncate(durableEnvelopeBytes + 1); err != nil {
					test.Fatal(err)
				}
				file.Close()
			}
			payloadBefore, _ := os.ReadFile(path)
			floorBefore, _ := os.ReadFile(path + ".floor")
			if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
				store.Close()
				test.Fatal("invalid/unsafe durable state accepted")
			}
			_ = before
			payloadAfter, _ := os.ReadFile(path)
			floorAfter, _ := os.ReadFile(path + ".floor")
			if !bytes.Equal(payloadBefore, payloadAfter) || !bytes.Equal(floorBefore, floorAfter) {
				test.Fatal("failed recovery altered durable state/floor")
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
	if err != nil || state.Revision < before.Revision {
		test.Fatal("exact prefix did not recover durable revision")
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
			if err := audit.Close(); err != nil {
				test.Fatal(err)
			}
			payload := auditPayload{Version: 1, Sequence: 1, Previous: strings.Repeat("0", 64), Event: auditFixtureEvent()}
			encoded, _ := json.Marshal(payload)
			hash := digestHex(encoded)
			line, _ := json.Marshal(auditRecord{Payload: payload, Hash: hash})
			line = append(line, '\n')
			headPayload, _ := json.Marshal(auditHead{Version: 1, Sequence: 1, Hash: hash})
			head := diskEnvelope{Version: 1, Revision: 1, Checksum: digestHex(headPayload), Payload: headPayload}
			// A head that claims an append must be backed by the chain itself:
			// the head is durable before its segment record, so recovery may
			// never fabricate the ACK from the head alone.
			durableRecoveryWriteJSON(test, path+".head", head)
			durableRecoveryWriteJSON(test, path+".head.floor", diskFloor{Version: 1, Revision: 1, Checksum: head.Checksum})
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
					test.Fatal("head fabricated audit append ACK")
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
	path, _, _ := durableRecoveryFixture(test)
	oversized := json.RawMessage(`"` + strings.Repeat("x", durablePayloadBytes) + `"`)
	durableRecoveryWriteJSON(test, path, diskEnvelope{Version: 1, Revision: 1, Checksum: digestHex(oversized), Payload: oversized})
	if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
		store.Close()
		test.Fatal("oversized payload expanded the business limit")
	}
}

// The floor is durable before the payload, so an initialization that never wrote
// its payload resumes while the floor still proves revision zero. A store missing
// both files cannot be told apart from a wiped one and must stay refused rather
// than silently reinitialize.
func TestDurableRecoveryInitializationRequiresExactPayload(test *testing.T) {
	for _, kind := range []string{"state", "audit", "kms"} {
		for _, prefix := range []string{"settled", "interrupted", "wiped"} {
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
				if prefix == "interrupted" {
					if err := os.Remove(path); err != nil {
						test.Fatal(err)
					}
				}
				if prefix == "wiped" {
					for _, target := range []string{path, path + ".floor"} {
						if err := os.Remove(target); err != nil {
							test.Fatal(err)
						}
					}
				}
				recovered, err := openDurable(path, initial)
				if prefix == "wiped" {
					if err == nil {
						recovered.close()
						test.Fatal("wiped initialization replayed without floor proof")
					}
					return
				}
				if err != nil {
					test.Fatal(err)
				}
				defer recovered.close()
				if recovered.revision != 0 || !bytes.Equal(recovered.payload, after.Payload) {
					test.Fatal("initial recovery reset or changed initialization")
				}
				if recovered.fresh != (prefix == "interrupted") {
					test.Fatal("initial recovery changed initialization freshness")
				}
			})
		}
	}
	for _, scenario := range []string{"marker_only", "temp_only", "nonempty_state", "normal_update_as_init"} {
		test.Run(scenario, func(test *testing.T) {
			path, _, after := durableRecoveryFixture(test)
			if scenario == "normal_update_as_init" {
				initial := State{Version: 1, Organizations: map[string]Organization{}, Projects: map[string]Project{}, Databases: map[string]Database{}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{}}
				encoded, _ := json.Marshal(initial)
				after = diskEnvelope{Version: 1, Checksum: digestHex(encoded), Payload: encoded}
			} else {
				var state State
				if err := strictDecode(after.Payload, &state); err != nil {
					test.Fatal(err)
				}
				state.Revision = 0
				encoded, _ := json.Marshal(state)
				after = diskEnvelope{Version: 1, Checksum: digestHex(encoded), Payload: encoded}
			}
			if scenario == "marker_only" || scenario == "temp_only" {
				for _, target := range []string{path, path + ".floor"} {
					if err := os.Remove(target); err != nil {
						test.Fatal(err)
					}
				}
				if scenario == "temp_only" {
					// A temp file left by an interrupted rename is never the only
					// copy of a payload, so it cannot authorize reinitialization.
					durableRecoveryWriteJSON(test, filepath.Join(filepath.Dir(path), ".private-init.tmp"), after)
				}
			}
			if scenario == "normal_update_as_init" || scenario == "nonempty_state" {
				durableRecoveryWriteJSON(test, path, after)
			}
			if store, err := OpenStateStore(filepath.Dir(path)); err == nil {
				store.Close()
				test.Fatal("initialization replay accepted")
			}
		})
	}
}
