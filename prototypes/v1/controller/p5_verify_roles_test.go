package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func p5VerifyAPIRequest(test *testing.T, endpoint, path, token string, input any) (int, []byte) {
	test.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(body))
	if err != nil {
		test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		test.Fatal("management API request failed")
	}
	defer response.Body.Close()
	result, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(result) > 1024*1024 {
		test.Fatal("management API response unreadable")
	}
	return response.StatusCode, result
}

func TestP5VerifyAuthenticatedAPIProducesNativeReadonlyAndRejectsRoleAmplificationWithoutCredentialEcho(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	store := p5VerifyStore(test, filepath.Join(cluster.root, "controller-state"))
	cluster.register(store)
	control := p5VerifyController(test, store, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("readonly-api")
	writer, err := control.Place(ctx, database, "local", "writer")
	if err != nil {
		test.Fatal(err)
	}
	session := cluster.open(writer.DesiredWorker, database)
	cluster.execute(writer.DesiredWorker, session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(7)")
	if !cluster.sql(writer.DesiredWorker, map[string]any{"command": "compact", "session": session}).OK {
		test.Fatal("seed writer compaction refused")
	}
	for index, operation := range []any{
		map[string]string{"operation": "snapshot", "name": "readonly-seed"},
		map[string]string{"operation": "clone", "snapshot": "readonly-seed", "name": "readonly"},
	} {
		identity := []string{"readonly-seed-snapshot", "readonly-seed-clone"}[index]
		var result json.RawMessage
		if err := cluster.transport.Authority(ctx, identity, map[string]any{"command": "maintenance", "request_id": identity, "database": database, "operation": operation}, &result); err != nil {
			test.Fatal(err)
		}
	}
	readonlyDatabase := database
	readonlyDatabase.Branch = "readonly"
	apiToken := p5VerifyToken(test)
	cluster.tokens = append(cluster.tokens, apiToken)
	api, err := NewAPI(store, control, apiToken)
	if err != nil {
		test.Fatal(err)
	}
	server := httptest.NewServer(api)
	test.Cleanup(server.Close)
	input := map[string]any{"database": readonlyDatabase, "region": "local", "role": "read_only"}
	denied, body := p5VerifyAPIRequest(test, server.URL, "/v1/databases/place", cluster.authorityToken, input)
	if denied != 403 || strings.Contains(string(body), cluster.authorityToken) {
		test.Fatal("authority credential crossed management API boundary or was echoed")
	}
	status, body := p5VerifyAPIRequest(test, server.URL, "/v1/databases/place", apiToken, input)
	var readonly Placement
	if status != 200 || json.Unmarshal(body, &readonly) != nil || readonly.Phase != "active" || readonly.Grant == nil || readonly.Role != "read_only" || readonly.Grant.Binding.Role != "read_only" {
		test.Fatalf("authenticated native readonly placement failed: HTTP%d", status)
	}
	worker := cluster.workers[readonly.DesiredWorker]
	for _, command := range []string{"open_session", "validate_lease"} {
		reply, status, err := p5VerifyRPC(cluster.authority.address, "/", worker.authorityToken, map[string]any{"command": command, "binding": readonly.Grant.Binding, "role": "writer"})
		if err != nil || status != 200 || reply.OK {
			test.Fatalf("readonly capability amplification %s not denied", command)
		}
	}
	opened := cluster.sql(readonly.DesiredWorker, map[string]any{"command": "open_session", "database": readonlyDatabase})
	var native struct {
		Session  string `json:"session"`
		Readonly bool   `json:"readonly"`
	}
	if !opened.OK || json.Unmarshal(opened.Result, &native) != nil || !native.Readonly || native.Session == "" {
		test.Fatal("Go readonly role did not reach native readonly session")
	}
	p5VerifyRequireRows(test, cluster.rows(readonly.DesiredWorker, native.Session, "SELECT id FROM records"), "[[7]]")
	for _, attempted := range []map[string]any{
		{"command": "query", "session": native.Session, "sql": "INSERT INTO records VALUES(8) RETURNING id"},
		{"command": "execute", "session": native.Session, "sql": "PRAGMA query_only=OFF; INSERT INTO records VALUES(9)"},
	} {
		if cluster.sql(readonly.DesiredWorker, attempted).OK {
			test.Fatal("native readonly query/pragma bypass wrote data")
		}
	}
	p5VerifyRequireRows(test, cluster.rows(readonly.DesiredWorker, native.Session, "SELECT id FROM records"), "[[7]]")
	p5VerifyRequireRows(test, cluster.rows(readonly.DesiredWorker, native.Session, "PRAGMA integrity_check"), `[["ok"]]`)
	invalid := Worker{ID: "bad-registry", Address: "http://user:" + apiToken + "@127.0.0.1:1", Capacity: 1}
	status, body = p5VerifyAPIRequest(test, server.URL, "/v1/workers/register", apiToken, map[string]any{"worker": invalid})
	if status != 409 {
		test.Fatal("credential-bearing invalid registry endpoint was accepted")
	}
	for _, token := range cluster.tokens {
		if strings.Contains(string(body), token) {
			test.Fatal("management validation error exposed credential")
		}
	}
	metadata, err := store.Snapshot()
	if err != nil || len(metadata.Workers) != 2 {
		test.Fatal("invalid endpoint changed durable worker registry")
	}
	test.Logf("actual Go HTTP management API acquired nativeReadOnly epoch%d; worker Writer amplification/query injection denied, ACK7/integrity preserved, credential boundaries/errors redacted", readonly.Epoch)
}
