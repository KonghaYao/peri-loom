package platform

import (
	"bytes"
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
