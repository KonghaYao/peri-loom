package platform

import (
	"bytes"
	"testing"
)

func TestOperationRetentionNativeKnownCloseThenDelete(test *testing.T) {
	fixture := p6VerifyNew(test)
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE known_retention_ledger(id INTEGER PRIMARY KEY)")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	before := fixture.must(fixture.credential, "/v1/databases/operations", map[string]any{"database": fixture.database})
	if !bytes.Contains(before.Result, []byte(`"known_terminal_limit":256`)) || !bytes.Contains(before.Result, []byte(`"cleanup_outcome":"completed"`)) {
		test.Fatal("actual native Close ACK not exposed with bounded retention")
	}
	fixture.must(fixture.credential, "/v1/databases/delete", map[string]any{"database": fixture.database})
	after := fixture.must(fixture.credential, "/v1/databases/operations", map[string]any{"database": fixture.database})
	if !bytes.Contains(after.Result, []byte(`"operations":[]`)) || !bytes.Contains(after.Result, []byte(`"retry_allowed":false`)) || !bytes.Contains(after.Result, []byte(`"history_complete":false`)) {
		test.Fatal("normal delete retained unsafe history semantics")
	}
	status, _ := fixture.call(fixture.credential, "/v1/databases/create", map[string]any{"project": "primary", "database": fixture.database, "role": "writer"})
	if status == 200 {
		test.Fatal("normal deletion rebound reserved database identity")
	}
	test.Log("KNOWN_RETENTION_NATIVE close_ack=1 delete_success=1 safe_history_removed=1 database_identity_reserved=1 missing_history_allows_replay=0")
}
