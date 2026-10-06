package platform

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func (fixture *p6VerifyFixture) token(databaseInput map[string]any, credential string, lifetime time.Duration) string {
	fixture.test.Helper()
	databaseInput["expires_ms"] = time.Now().Add(lifetime).UnixMilli()
	reply := fixture.must(credential, "/v1/tokens", databaseInput)
	var token string
	if json.Unmarshal(reply.Result, &token) != nil || token == "" {
		fixture.test.Fatal("scoped token missing")
	}
	fixture.secrets = append(fixture.secrets, token)
	return token
}

func (fixture *p6VerifyFixture) deny(token, path string, input map[string]any) {
	fixture.test.Helper()
	status, reply := fixture.call(token, path, input)
	if status < 400 || reply.OK {
		fixture.test.Fatalf("unauthorized request accepted %s status=%d", path, status)
	}
}

func (fixture *p6VerifyFixture) alteredSignedClaim(token, field string, value any) string {
	fixture.test.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		fixture.test.Fatal("invalid issued scoped token")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		fixture.test.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(encoded, &claims); err != nil {
		fixture.test.Fatal(err)
	}
	claims[field] = value
	encoded, err = json.Marshal(claims)
	if err != nil {
		fixture.test.Fatal(err)
	}
	key, err := os.ReadFile(fixture.config.SigningKeyFile)
	if err != nil {
		fixture.test.Fatal(err)
	}
	mac := hmac.New(sha256.New, key)
	clear(key)
	payload := "pldb1." + base64.RawURLEncoding.EncodeToString(encoded)
	_, _ = mac.Write([]byte(payload))
	altered := payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	fixture.secrets = append(fixture.secrets, altered)
	return altered
}

func TestP6VerifyScopedTokensAndNativeReadonly(test *testing.T) {
	fixture := p6VerifyNew(test)
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY)")
	fixture.sql(session, "execute", "INSERT INTO proof VALUES(7)")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	scoped := fixture.token(map[string]any{"database": fixture.database, "role": "writer"}, fixture.credential, time.Minute)
	controlSession := fixture.open(scoped, fixture.database)
	fixture.must(scoped, "/v1/sql/close", map[string]any{"session": controlSession})
	for _, field := range []string{"root_incarnation", "domain_id"} {
		altered := fixture.alteredSignedClaim(scoped, field, p6VerifyToken(test)[:32])
		fixture.deny(altered, "/v1/sql/open", map[string]any{"database": fixture.database})
	}
	other := fixture.database
	other.Namespace.Tenant = "verify_secondary"
	fixture.must(fixture.config.BootstrapKey, "/v1/projects/create", map[string]any{"id": "secondary", "organization": "verify", "region": "local"})
	secondKey := fixture.must(fixture.config.BootstrapKey, "/v1/keys/create", map[string]any{"project": "secondary", "subject": "other-admin", "role": "admin"})
	var second struct {
		Credential string `json:"credential"`
	}
	if json.Unmarshal(secondKey.Result, &second) != nil || second.Credential == "" {
		test.Fatal("second tenant credential missing")
	}
	fixture.secrets = append(fixture.secrets, second.Credential)
	fixture.must(second.Credential, "/v1/databases/create", map[string]any{"project": "secondary", "database": other, "role": "writer"})
	otherSession := fixture.open(second.Credential, other)
	fixture.must(second.Credential, "/v1/sql/execute", map[string]any{"session": otherSession, "sql": "CREATE TABLE other_tenant(id INTEGER PRIMARY KEY); INSERT INTO other_tenant VALUES(99)"})
	fixture.must(second.Credential, "/v1/sql/close", map[string]any{"session": otherSession})
	fixture.deny(scoped, "/v1/sql/open", map[string]any{"database": other})
	fixture.deny(fixture.credential, "/v1/sql/open", map[string]any{"database": other})
	fixture.deny(scoped, "/v1/databases/delete", map[string]any{"database": fixture.database})
	fixture.deny(fixture.config.BootstrapKey, "/v1/sql/open", map[string]any{"database": fixture.database})
	expiring := fixture.token(map[string]any{"database": fixture.database, "role": "writer"}, fixture.credential, 200*time.Millisecond)
	time.Sleep(230 * time.Millisecond)
	fixture.deny(expiring, "/v1/sql/open", map[string]any{"database": fixture.database})
	fixture.must(fixture.credential, "/v1/databases/snapshot", map[string]any{"database": fixture.database, "name": "safe"})
	fixture.must(fixture.credential, "/v1/databases/clone", map[string]any{"database": fixture.database, "snapshot": "safe", "name": "readonly", "role": "read_only"})
	clone := fixture.database
	clone.Branch = "readonly"
	readonly := fixture.token(map[string]any{"database": clone, "role": "read_only"}, fixture.credential, time.Minute)
	cloneSession := fixture.open(readonly, clone)
	status, rejected := fixture.call(readonly, "/v1/sql/query", map[string]any{"session": cloneSession, "sql": "INSERT INTO proof VALUES(9) RETURNING id"})
	if status != 400 || rejected.OK || rejected.Error.Code != "sql_error" || !rejected.Error.ExecutionCompleted {
		test.Fatalf("native readonly query write status=%d code=%s execution_completed=%t", status, rejected.Error.Code, rejected.Error.ExecutionCompleted)
	}
	rows := fixture.must(readonly, "/v1/sql/query", map[string]any{"session": cloneSession, "sql": "SELECT id FROM proof"})
	if string(rows.Result) != "[[7]]" {
		test.Fatalf("readonly clone changed rows=%s", rows.Result)
	}
	fixture.must(readonly, "/v1/sql/close", map[string]any{"session": cloneSession})
	fixture.must(fixture.credential, "/v1/databases/restore", map[string]any{"database": fixture.database, "snapshot": "safe"})
	fixture.deny(scoped, "/v1/sql/open", map[string]any{"database": fixture.database})
	issued := fixture.must(fixture.config.BootstrapKey, "/v1/keys/create", map[string]any{"project": "primary", "subject": "revocable", "role": "sql"})
	var key struct {
		ID         string `json:"key"`
		Credential string `json:"credential"`
	}
	if json.Unmarshal(issued.Result, &key) != nil {
		test.Fatal("invalid API key result")
	}
	fixture.secrets = append(fixture.secrets, key.Credential)
	revocable := fixture.token(map[string]any{"database": fixture.database, "role": "writer"}, key.Credential, time.Minute)
	fixture.must(fixture.config.BootstrapKey, "/v1/keys/revoke", map[string]any{"id": key.ID})
	fixture.deny(revocable, "/v1/sql/open", map[string]any{"database": fixture.database})
	test.Log("actual scoped tenant/management/expiry/issuer-revocation/restore-incarnation denial; native read-only query-write blocked")
}

func TestP6VerifyBoundedNDJSONAndCommittedPrefix(test *testing.T) {
	fixture := p6VerifyNewWithQuotas(test, Quotas{MaxResultBytes: 1024, MaxConcurrent: 1})
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY)")
	status, failed := fixture.call(fixture.credential, "/v1/sql/execute", map[string]any{"session": session, "sql": "INSERT INTO proof VALUES(1); INSERT INTO missing_table VALUES(2)"})
	if status != 400 || failed.Error.Code != "sql_error" || !failed.Error.ExecutionCompleted || !failed.Error.MayHaveCommitted {
		test.Fatalf("partial SQL status=%d code=%s terminal=%t maycommit=%t", status, failed.Error.Code, failed.Error.ExecutionCompleted, failed.Error.MayHaveCommitted)
	}
	rows := fixture.sql(session, "query", "SELECT id FROM proof")
	if string(rows.Result) != "[[1]]" {
		test.Fatalf("committed prefix rows=%s", rows.Result)
	}
	body, _ := json.Marshal(map[string]any{"session": session, "sql": "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<17) SELECT x FROM numbers", "request_id": "stream-proof"})
	request, err := http.NewRequest("POST", fixture.endpoint+"/v1/sql/query", bytes.NewReader(body))
	if err != nil {
		test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+fixture.credential)
	request.Header.Set("Accept", "application/x-ndjson")
	response, err := fixture.client.Do(request)
	if err != nil {
		test.Fatal(err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		test.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(encoded), []byte{'\n'})
	if response.StatusCode != 200 || len(lines) != 18 {
		test.Fatalf("NDJSON status=%d lines=%d", response.StatusCode, len(lines))
	}
	for index, line := range lines {
		var frame struct {
			Type      string `json:"type"`
			Row       []int  `json:"row"`
			Rows      int    `json:"rows"`
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(line, &frame) != nil {
			test.Fatal("malformed NDJSON frame")
		}
		if index < 17 {
			if frame.Type != "row" || len(frame.Row) != 1 || frame.Row[0] != index+1 {
				test.Fatal("incorrect NDJSON row")
			}
		} else if frame.Type != "complete" || frame.Rows != 17 || frame.RequestID != "stream-proof" {
			test.Fatal("incorrect NDJSON completion")
		}
	}
	streamBody, _ := json.Marshal(map[string]any{"session": session, "sql": "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<80) SELECT x FROM numbers", "request_id": "stream-bounded"})
	streamRequest, err := http.NewRequest("POST", fixture.endpoint+"/v1/sql/query", bytes.NewReader(streamBody))
	if err != nil {
		test.Fatal(err)
	}
	streamRequest.Header.Set("Authorization", "Bearer "+fixture.credential)
	streamRequest.Header.Set("Accept", "application/x-ndjson")
	streamResponse, err := fixture.client.Do(streamRequest)
	if err != nil {
		test.Fatal(err)
	}
	var streamLimit p6VerifyReply
	decodeError := json.NewDecoder(io.LimitReader(streamResponse.Body, 4096)).Decode(&streamLimit)
	_ = streamResponse.Body.Close()
	if decodeError != nil || streamResponse.StatusCode != 413 || streamLimit.Error.Code != "result_limit" || !streamLimit.Error.ExecutionCompleted {
		test.Fatal("actual NDJSON overhead exceeded bounded output without terminal limit error")
	}
	limitStatus, limited := fixture.call(fixture.credential, "/v1/sql/query", map[string]any{"session": session, "sql": "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<100) SELECT printf('%032d',x) FROM numbers"})
	if limitStatus != 413 || limited.Error.Code != "result_limit" || !limited.Error.ExecutionCompleted {
		test.Fatalf("native bounded results status=%d code=%s execution_completed=%t", limitStatus, limited.Error.Code, limited.Error.ExecutionCompleted)
	}
	if reuse := fixture.sql(session, "query", "SELECT id FROM proof"); string(reuse.Result) != "[[1]]" {
		test.Fatal("terminal result limit retained SQL permit or poisoned session")
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	test.Log("actual committed multi-statement prefix; 17 ordered NDJSON rows+completion, not incremental native execution")
}
