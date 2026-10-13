package platform

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func auditFixtureEvent() AuditEvent {
	return AuditEvent{RequestID: "request", Subject: "alice", Project: "alpha", Database: securityDatabase().Key(), Action: "/v1/sql/execute", Outcome: "ok", TimestampMS: time.Now().UnixMilli()}
}

func TestAuditAppendFsyncReopenAndExclusiveLock(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	audit, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := OpenAudit(path); !errors.Is(err, ErrStateLocked) {
		test.Fatalf("audit lock: %v", err)
	}
	for index := 0; index < 3; index++ {
		if err := audit.Record(auditFixtureEvent()); err != nil {
			test.Fatal(err)
		}
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != 3 {
		test.Fatal("audit sequence not durable")
	}
	if err := reopened.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
}

func TestAuditRefusesSQLCredentialsAndTamperedChain(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	audit, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	for _, unsafe := range []string{"SELECT customer_secret FROM users", "pldb1.test.secret", "plk1_test-secret"} {
		event := auditFixtureEvent()
		event.Action = unsafe
		if err := audit.Record(event); err == nil {
			test.Fatal("unsafe audit data accepted")
		}
	}
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	audit.Close()
	encoded, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("customer_secret")) {
		test.Fatal("SQL leaked to audit")
	}
	tampered := bytes.Replace(encoded, []byte(`"outcome":"ok"`), []byte(`"outcome":"failed"`), 1)
	if err := os.WriteFile(path, tampered, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := OpenAudit(path); err == nil {
		test.Fatal("audit tamper ignored")
	}
}

func TestAuditInterruptedAppendAndTruncatedNewlineFailClosed(test *testing.T) {
	for _, interrupted := range []bool{false, true} {
		test.Run(map[bool]string{false: "truncated-newline", true: "failed-write"}[interrupted], func(test *testing.T) {
			path := filepath.Join(test.TempDir(), "audit.jsonl")
			service, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			audit := service.(*fileAudit)
			if interrupted {
				audit.file.Close()
				if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateUncertain) {
					test.Fatal("unknown append not poisoned")
				}
			} else {
				if err := audit.Record(auditFixtureEvent()); err != nil {
					test.Fatal(err)
				}
			}
			audit.Close()
			if !interrupted {
				encoded, err := os.ReadFile(path)
				if err != nil {
					test.Fatal(err)
				}
				if err := os.WriteFile(path, encoded[:len(encoded)-1], 0600); err != nil {
					test.Fatal(err)
				}
			}
			if _, err := OpenAudit(path); err == nil {
				test.Fatal("incomplete audit append accepted")
			}
		})
	}
}

func auditRolledFixture(test *testing.T) (string, auditHead) {
	test.Helper()
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	previous := audit.state
	audit.size = auditSegmentBytes
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	return path, previous
}

func TestAuditRotationContinuousChainAndArchiveTampering(test *testing.T) {
	path, _ := auditRolledFixture(test)
	archive := auditSegmentPath(path, 1)
	if info, err := os.Stat(archive); err != nil || info.Size() == 0 {
		test.Fatal("rotation did not preserve first segment")
	}
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	if audit.state.Sequence != 2 || audit.start != 2 {
		test.Fatal("sequence did not cross segment boundary")
	}
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	encoded, err := os.ReadFile(archive)
	if err != nil {
		test.Fatal(err)
	}
	tampered := bytes.Replace(encoded, []byte(`"outcome":"ok"`), []byte(`"outcome":"no"`), 1)
	if err := os.WriteFile(archive, tampered, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := OpenAudit(path); err == nil {
		test.Fatal("archived chain tamper accepted")
	}
}

func TestAuditRotationCrashPrefixes(test *testing.T) {
	for _, point := range []string{"intent", "renamed", "new-active"} {
		test.Run(point, func(test *testing.T) {
			path := filepath.Join(test.TempDir(), "audit.jsonl")
			service, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			audit := service.(*fileAudit)
			if err := audit.Record(auditFixtureEvent()); err != nil {
				test.Fatal(err)
			}
			state := audit.state
			if err := audit.Close(); err != nil {
				test.Fatal(err)
			}
			intent, _ := json.Marshal(auditRotation{Version: 1, Start: 1, Sequence: state.Sequence, Hash: state.Hash})
			if err := atomicPrivate(path+".rotate", intent); err != nil {
				test.Fatal(err)
			}
			if point != "intent" {
				if err := os.Rename(path, auditSegmentPath(path, 1)); err != nil {
					test.Fatal(err)
				}
				if point == "new-active" {
					if err := os.WriteFile(path, nil, 0600); err != nil {
						test.Fatal(err)
					}
				}
			}
			reopened, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			if _, err := os.Lstat(path + ".rotate"); !errors.Is(err, os.ErrNotExist) {
				test.Fatal("rotation intent was not resolved")
			}
			if err := reopened.Record(auditFixtureEvent()); err != nil {
				test.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				test.Fatal(err)
			}
			verified, err := OpenAudit(path)
			if err != nil {
				test.Fatal(err)
			}
			defer verified.Close()
			if verified.(*fileAudit).state.Sequence != 2 {
				test.Fatal("rotation recovery broke chain")
			}
		})
	}
}

func TestAuditRotationIncompleteStatesFailClosed(test *testing.T) {
	for _, point := range []string{"missing-archive", "missing-active", "bad-intent", "extra-archive", "advanced-head", "unsafe-sidecar", "unknown-state-sidecar"} {
		test.Run(point, func(test *testing.T) {
			if point == "unknown-state-sidecar" {
				directory := test.TempDir()
				path := filepath.Join(directory, "state.json")
				store, err := OpenStateStore(directory)
				if err != nil {
					test.Fatal(err)
				}
				store.Close()
				if err := os.WriteFile(path+".segment.00000000000000000001", []byte("unexpected"), 0600); err != nil {
					test.Fatal(err)
				}
				if _, err := OpenStateStore(directory); err == nil {
					test.Fatal("non-audit durable accepted audit sidecar")
				}
				return
			}
			path, previous := auditRolledFixture(test)
			switch point {
			case "missing-archive":
				os.Remove(auditSegmentPath(path, 1))
			case "missing-active":
				os.Remove(path)
			case "bad-intent":
				intent, _ := json.Marshal(auditRotation{Version: 1, Start: 2, Sequence: previous.Sequence, Hash: previous.Hash})
				if err := atomicPrivate(path+".rotate", intent); err != nil {
					test.Fatal(err)
				}
			case "extra-archive":
				if err := os.WriteFile(auditSegmentPath(path, 3), []byte("unexpected\n"), 0600); err != nil {
					test.Fatal(err)
				}
			case "advanced-head":
				service, err := OpenAudit(path)
				if err != nil {
					test.Fatal(err)
				}
				audit := service.(*fileAudit)
				if err := audit.head.commit(audit.state.Sequence+1, auditHead{Version: 1, Sequence: audit.state.Sequence + 1, Hash: audit.state.Hash}); err != nil {
					test.Fatal(err)
				}
				audit.Close()
			case "unsafe-sidecar":
				if err := os.Symlink(path, auditSegmentPath(path, 3)); err != nil {
					test.Fatal(err)
				}
			}
			if _, err := OpenAudit(path); err == nil {
				test.Fatal("incomplete audit storage accepted")
			}
		})
	}
}

func TestAuditCrossesPhysical64MiBBoundary(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	event := auditFixtureEvent()
	event.RequestID = string(bytes.Repeat([]byte("r"), 128))
	event.Subject = string(bytes.Repeat([]byte("s"), 128))
	event.Project = string(bytes.Repeat([]byte("p"), 128))
	event.Outcome = string(bytes.Repeat([]byte("o"), 128))
	state := audit.state
	writer := bufio.NewWriterSize(audit.file, 1<<20)
	var size int64
	for {
		payload := auditPayload{Version: 1, Sequence: state.Sequence + 1, Previous: state.Hash, Event: event}
		encoded, _ := json.Marshal(payload)
		hash := digestHex(encoded)
		line, _ := json.Marshal(auditRecord{Payload: payload, Hash: hash})
		line = append(line, '\n')
		if size+2*int64(len(line)) > auditSegmentBytes {
			break
		}
		if _, err := writer.Write(line); err != nil {
			test.Fatal(err)
		}
		size += int64(len(line))
		state.Sequence++
		state.Hash = hash
	}
	if err := writer.Flush(); err != nil {
		test.Fatal(err)
	}
	if err := audit.file.Sync(); err != nil {
		test.Fatal(err)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
	encoded, _ := json.Marshal(state)
	checksum := digestHex(encoded)
	head, _ := json.Marshal(diskEnvelope{Version: 1, Revision: state.Sequence, Checksum: checksum, Payload: encoded})
	floor, _ := json.Marshal(diskFloor{Version: 1, Revision: state.Sequence, Checksum: checksum})
	if err := atomicPrivate(path+".head", head); err != nil {
		test.Fatal(err)
	}
	if err := atomicPrivate(path+".head.floor", floor); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	active := reopened.(*fileAudit)
	if active.size != size || active.state != state {
		test.Fatal("physical boundary fixture did not verify")
	}
	if err := active.Record(event); err != nil {
		test.Fatal(err)
	}
	fullSize := active.size
	if err := active.Record(event); err != nil {
		test.Fatal(err)
	}
	if err := active.Close(); err != nil {
		test.Fatal(err)
	}
	archive, err := os.Stat(auditSegmentPath(path, 1))
	if err != nil || archive.Size() != fullSize || fullSize+active.size <= auditSegmentBytes {
		test.Fatal("64MiB segment not archived intact")
	}
	verified, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer verified.Close()
	if verified.(*fileAudit).state.Sequence != state.Sequence+2 {
		test.Fatal("chain did not continue past original threshold")
	}
}

func TestAuditRotationIOFailurePoisonsUntilRecovered(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	if err := audit.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	audit.size = auditSegmentBytes
	audit.file.Close()
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("rotation IO failure was not uncertain: %v", err)
	}
	if err := audit.Record(auditFixtureEvent()); !errors.Is(err, ErrStateUncertain) {
		test.Fatalf("failed audit resumed writing: %v", err)
	}
	audit.Close()
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Record(auditFixtureEvent()); err != nil {
		test.Fatal(err)
	}
	if reopened.(*fileAudit).state.Sequence != 2 {
		test.Fatal("IO failure recovery lost or duplicated event")
	}
}
