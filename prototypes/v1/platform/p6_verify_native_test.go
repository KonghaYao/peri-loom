package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestP6VerifyWholeStackEncryptedACK(test *testing.T) {
	fixture := p6VerifyNew(test)
	session := fixture.open(fixture.credential, fixture.database)
	marker := "independent_payload_" + p6VerifyToken(test)
	fixture.secrets = append(fixture.secrets, marker)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY, payload TEXT UNIQUE)")
	fixture.sql(session, "execute", "INSERT INTO proof VALUES(1,'"+marker+"')")
	rows := fixture.sql(session, "query", "SELECT id FROM proof")
	if string(rows.Result) != "[[1]]" {
		test.Fatalf("acknowledged row result=%s", rows.Result)
	}
	payloadRows := fixture.sql(session, "query", "SELECT payload FROM proof")
	var restored [][]string
	if json.Unmarshal(payloadRows.Result, &restored) != nil || len(restored) != 1 || len(restored[0]) != 1 || restored[0][0] != marker {
		test.Fatal("actual native payload roundtrip failed")
	}
	integrity := fixture.sql(session, "query", "PRAGMA integrity_check")
	if string(integrity.Result) != `[["ok"]]` {
		test.Fatalf("native integrity=%s", integrity.Result)
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	fixture.must(fixture.credential, "/v1/databases/checkpoint", map[string]any{"database": fixture.database})
	segments, walFrames, caches := p6VerifyArtifacts(test, fixture.directory, marker)
	fixture.platform.stop(test, true)
	for _, path := range []string{filepath.Join(fixture.config.StateDirectory, "state.json"), fixture.config.AuditPath} {
		encoded, err := os.ReadFile(path)
		if err != nil {
			test.Fatal(err)
		}
		for _, secret := range fixture.secrets {
			if bytes.Contains(encoded, []byte(secret)) {
				test.Fatalf("secret persisted in %s", filepath.Base(path))
			}
		}
	}
	test.Logf("actual GoCLI+Broker+Authority+2Workers: native payload roundtrip+ACK+integrity; decoded mainSegments=%d nativeWALWrites=%d SSDsegments=%d; state/audit secret scan", segments, walFrames, caches)
}

func TestP6VerifyConstraintRollbackReusesSession(test *testing.T) {
	fixture := p6VerifyNewWithQuotas(test, Quotas{MaxConcurrent: 1})
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY)")
	fixture.sql(session, "execute", "INSERT INTO proof VALUES(1)")
	fixture.sql(session, "execute", "BEGIN")
	fixture.sql(session, "execute", "INSERT INTO proof VALUES(2)")
	status, failed := fixture.call(fixture.credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO proof VALUES(1)"})
	if status != 400 || failed.OK || failed.Error.Code != "sql_error" || !failed.Error.ExecutionCompleted || !failed.Error.MayHaveCommitted {
		test.Fatalf("native negative status=%d code=%s execution_completed=%t may_have_committed=%t", status, failed.Error.Code, failed.Error.ExecutionCompleted, failed.Error.MayHaveCommitted)
	}
	fixture.sql(session, "execute", "ROLLBACK")
	rows := fixture.sql(session, "query", "SELECT id FROM proof ORDER BY id")
	if string(rows.Result) != "[[1]]" {
		test.Fatalf("explicit rollback rows=%s", rows.Result)
	}
	fixture.sql(session, "execute", "INSERT INTO proof VALUES(3)")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	test.Log("actual native UNIQUE failure, typed terminal SQL error, same-session explicit rollback and subsequent write")
}

// A long-lived client pages a read cursor to `done` and then closes it. The
// drained read already released its worker slot, so the close has to reach the
// documented postcondition instead of failing as an unknown-outcome 400.
func TestP6VerifyNativeDrainedCursorCloseIsIdempotent(test *testing.T) {
	fixture := p6VerifyNew(test)
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
	var values strings.Builder
	for row := 1; row <= 40; row++ {
		if row > 1 {
			values.WriteString(",")
		}
		fmt.Fprintf(&values, "(%d,'row_%d')", row, row)
	}
	fixture.sql(session, "execute", "INSERT INTO items(id,payload) VALUES "+values.String())
	fixture.must(fixture.credential, "/v1/sql/cursor/open", map[string]any{"session": session, "sql": "SELECT id FROM items ORDER BY id", "args": []any{}, "named_args": []any{}, "write_cursor": false})
	fetched, pages, drained := 0, 0, false
	for !drained {
		reply := fixture.must(fixture.credential, "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 16, "write_cursor": false})
		var page struct {
			Rows [][]json.RawMessage `json:"rows"`
			Done bool                `json:"done"`
		}
		if json.Unmarshal(reply.Result, &page) != nil {
			test.Fatalf("native cursor page invalid: %s", reply.Result)
		}
		fetched, drained, pages = fetched+len(page.Rows), page.Done, pages+1
		if pages > 4 {
			test.Fatal("native read cursor never drained")
		}
	}
	if fetched != 40 || pages < 2 {
		test.Fatalf("native read cursor rows=%d pages=%d", fetched, pages)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		fixture.must(fixture.credential, "/v1/sql/cursor/close", map[string]any{"session": session, "write_cursor": false})
	}
	reused := fixture.sql(session, "query", "SELECT count(*) FROM items")
	if string(reused.Result) != "[[40]]" {
		test.Fatalf("session unusable after drained close: %s", reused.Result)
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	test.Logf("actual native cursor drain (%d rows/%d pages), close after done and repeated close accepted, session reusable", fetched, pages)
}
