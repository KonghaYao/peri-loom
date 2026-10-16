package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

// frozenTestCommand freezes the command of a hand-built registration. Fixtures
// that never dispatch pass nil, which freezes as the JSON literal null.
func frozenTestCommand(test *testing.T, command any) frozenCommand {
	test.Helper()
	frozen, err := freezeCommandValue(command)
	if err != nil {
		test.Fatal(err)
	}
	return frozen
}

// digestCommandHook answers every worker command the digest vectors send, so one
// table can cover the whole client surface.
func digestCommandHook() func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "execute":
			return map[string]bool{"executed": true}, nil
		case "query":
			return [][]any{{1, "one"}, {2, "two"}}, nil
		case "statement":
			return map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 1, "last_insert_rowid": nil, "autocommit": true}, nil
		case "describe":
			return map[string]any{"params": []any{}, "cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}, "is_explain": false, "is_readonly": true}, nil
		case "session_autocommit":
			return map[string]bool{"autocommit": true}, nil
		case "open_read_cursor", "open_write_cursor":
			return map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}}}, nil
		case "fetch_read_cursor":
			return map[string]any{"rows": [][]any{{1}}, "done": true}, nil
		case "fetch_write_cursor":
			return map[string]any{"rows": [][]any{{1}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}, nil
		case "close_read_cursor", "close_write_cursor", "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	}
}

// transmittedCommands returns the bytes of every captured worker command with one
// operation name.
func transmittedCommands(upstream *gatewayTestUpstream, operation string) [][]byte {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	var captured [][]byte
	for _, call := range upstream.calls {
		if call.command["command"] == operation {
			captured = append(captured, call.encoded)
		}
	}
	return captured
}

// transmittedCommand returns the bytes of the single captured worker command for
// one operation name.
func transmittedCommand(test *testing.T, upstream *gatewayTestUpstream, operation string) []byte {
	test.Helper()
	captured := transmittedCommands(upstream, operation)
	if len(captured) != 1 {
		test.Fatalf("expected one captured %s command, found %d", operation, len(captured))
	}
	return captured[0]
}

func operationByRequestID(test *testing.T, store StateStore, requestID string) OperationRecord {
	test.Helper()
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var found []OperationRecord
	for _, record := range state.Operations {
		if record.RequestID == requestID {
			found = append(found, record)
		}
	}
	if len(found) != 1 {
		test.Fatalf("expected one durable record for %s, found %d", requestID, len(found))
	}
	return found[0]
}

func gatewayRawHTTP(test *testing.T, gateway *Gateway, token, path, body string) *httptest.ResponseRecorder {
	test.Helper()
	request := httptest.NewRequest("POST", path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	return response
}

// The durable digest is the identity of the command a worker actually received.
// Every client entry point is checked the same way: recompute the digest from the
// captured command bytes and require the record to carry exactly that value.
func TestOperationDigestBindsTransmittedCommand(test *testing.T) {
	vectors := []struct {
		name      string
		path      string
		requestID string
		operation string
		// The open vector is the one entry that must not open a session first:
		// it is the request that opens it.
		noSession bool
		body      func(*gatewayFixture, string) map[string]any
	}{
		{"open", "/v1/sql/open", "digest_open", "open_session", true, func(fixture *gatewayFixture, _ string) map[string]any {
			return map[string]any{"database": fixture.first}
		}},
		{"execute", "/v1/sql/execute", "digest_execute", "execute", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(1)"}
		}},
		{"query", "/v1/sql/query", "digest_query", "query", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "SELECT id FROM records"}
		}},
		{"statement", "/v1/sql/statement", "digest_statement", "statement", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(?)", "args": []any{map[string]any{"type": "integer", "value": "1"}}, "named_args": []any{}}
		}},
		{"statement_named_arguments", "/v1/sql/statement", "digest_statement_named", "statement", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(:id)", "named_args": []any{map[string]any{"name": "id", "value": map[string]any{"type": "integer", "value": "007"}}}}
		}},
		{"statement_absent_want_rows", "/v1/sql/statement", "digest_statement_default_rows", "statement", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "SELECT ?", "args": []any{map[string]any{"type": "text", "value": "hello"}}}
		}},
		{"statement_discarded_rows", "/v1/sql/statement", "digest_statement_no_rows", "statement", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "SELECT ?", "args": []any{map[string]any{"type": "text", "value": "hello"}}, "want_rows": false}
		}},
		{"describe", "/v1/sql/describe", "digest_describe", "describe", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "SELECT ? FROM records"}
		}},
		{"session_state", "/v1/sql/session_state", "digest_session_state", "session_autocommit", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session}
		}},
		{"close", "/v1/sql/close", "digest_close", "close_session", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session}
		}},
		{"cursor_open_read", "/v1/sql/cursor/open", "digest_cursor_open_read", "open_read_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "SELECT ? FROM records", "args": []any{map[string]any{"type": "integer", "value": "1"}}, "named_args": []any{}}
		}},
		{"cursor_open_write", "/v1/sql/cursor/open", "digest_cursor_open_write", "open_write_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(?) RETURNING id", "args": []any{map[string]any{"type": "integer", "value": "1"}}, "named_args": []any{}, "write_cursor": true}
		}},
		{"cursor_fetch_read", "/v1/sql/cursor/fetch", "digest_cursor_fetch_read", "fetch_read_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "max_rows": 2}
		}},
		{"cursor_fetch_write", "/v1/sql/cursor/fetch", "digest_cursor_fetch_write", "fetch_write_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "max_rows": 2, "write_cursor": true}
		}},
		{"cursor_close_read", "/v1/sql/cursor/close", "digest_cursor_close_read", "close_read_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session}
		}},
		{"cursor_close_write", "/v1/sql/cursor/close", "digest_cursor_close_write", "close_write_cursor", false, func(_ *gatewayFixture, session string) map[string]any {
			return map[string]any{"session": session, "write_cursor": true}
		}},
	}
	for _, vector := range vectors {
		test.Run(vector.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			fixture.upstream.setHook(digestCommandHook())
			session := ""
			if !vector.noSession {
				session = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			}
			body := vector.body(fixture, session)
			body["request_id"] = vector.requestID
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, vector.path, body)
			if response.Code != 200 {
				test.Fatalf("%s rejected: %d %s", vector.name, response.Code, response.Body.String())
			}
			record := operationByRequestID(test, fixture.state, vector.requestID)
			sent := transmittedCommand(test, fixture.upstream, vector.operation)
			if record.Digest != commandDigest(sent) {
				test.Fatalf("%s digest does not cover the transmitted command: %s vs %s", vector.name, record.Digest, commandDigest(sent))
			}
			if !taggedCommandDigest(record.Digest) {
				test.Fatalf("%s record carries no current-scheme digest: %s", vector.name, record.Digest)
			}
		})
	}
}

// The counterexamples of the review: these pairs used to collapse onto one
// digest, so the durable evidence could not tell the two executions apart.
func TestOperationDigestDistinguishesSemanticFields(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(digestCommandHook())
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	sql := "INSERT INTO records(id) VALUES(?)"
	statement := func(requestID, value string, wantRows *bool) OperationRecord {
		body := map[string]any{"session": session, "sql": sql, "args": []any{map[string]any{"type": "integer", "value": value}}, "request_id": requestID}
		if wantRows != nil {
			body["want_rows"] = *wantRows
		}
		if response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", body); response.Code != 200 {
			test.Fatalf("statement %s rejected: %d %s", requestID, response.Code, response.Body.String())
		}
		return operationByRequestID(test, fixture.state, requestID)
	}
	discarded := false
	first, second := statement("digest_value_one", "1", nil), statement("digest_value_two", "2", nil)
	if first.Digest == second.Digest {
		test.Fatal("two bound values share one durable digest")
	}
	projected, discardedRows := statement("digest_rows_kept", "1", nil), statement("digest_rows_discarded", "1", &discarded)
	if projected.Digest == discardedRows.Digest {
		test.Fatal("the row projection is missing from the durable digest")
	}
	// The registration-time command of the reviewed version was this exact map:
	// the record was hashed before the bound arguments were attached, so both
	// statements above shared one digest.
	registrationTime, err := json.Marshal(map[string]any{"command": "statement", "session": session, "sql": sql})
	if err != nil {
		test.Fatal(err)
	}
	if first.Digest == commandDigest(registrationTime) || second.Digest == commandDigest(registrationTime) {
		test.Fatal("the durable digest is still the digest of the registration-time command")
	}
	// Close and session_state are two different worker commands, and each record
	// must name the one it sends.
	if response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/session_state", map[string]any{"session": session, "request_id": "digest_state"}); response.Code != 200 {
		test.Fatalf("session_state rejected: %d %s", response.Code, response.Body.String())
	}
	state := operationByRequestID(test, fixture.state, "digest_state")
	if response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session, "request_id": "digest_closed"}); response.Code != 200 {
		test.Fatalf("close rejected: %d %s", response.Code, response.Body.String())
	}
	closed := operationByRequestID(test, fixture.state, "digest_closed")
	if state.Digest == closed.Digest {
		test.Fatal("close and session_state share one durable digest")
	}
	if state.Digest != commandDigest(transmittedCommand(test, fixture.upstream, "session_autocommit")) || closed.Digest != commandDigest(transmittedCommand(test, fixture.upstream, "close_session")) {
		test.Fatal("renamed command not covered by its own digest")
	}
}

// One logical command keeps one identity: the envelope it travelled in is not
// part of it, and neither is the JSON spelling the client chose for a value.
func TestOperationDigestIgnoresEnvelopeAndArgumentSpelling(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	body := `{"session":"` + session + `","sql":"SELECT ?","args":[{"type":"integer","value":"007"},{"type":"float","value":1},{"type":"text","value":"\u0041"}],"request_id":"spelling_a"}`
	response := gatewayRawHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", body)
	if response.Code != 200 {
		test.Fatalf("first spelling rejected: %d %s", response.Code, response.Body.String())
	}
	reordered := `{ "request_id" : "spelling_b" , "args" : [ { "value" : "7" , "type" : "integer" } , { "type" : "float" , "value" : 1.0 } , { "value" : "A" , "type" : "text" } ] , "sql" : "SELECT ?" , "session" : "` + session + `" }`
	response = gatewayRawHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", reordered)
	if response.Code != 200 {
		test.Fatalf("second spelling rejected: %d %s", response.Code, response.Body.String())
	}
	first := operationByRequestID(test, fixture.state, "spelling_a")
	second := operationByRequestID(test, fixture.state, "spelling_b")
	if first.Digest != second.Digest {
		test.Fatalf("one logical command carries two digests: %s vs %s", first.Digest, second.Digest)
	}
	if first.RequestID == second.RequestID || first.OperationID == second.OperationID {
		test.Fatal("the envelope identity of the two requests collapsed")
	}
	sent := transmittedCommands(fixture.upstream, "statement")
	if len(sent) != 2 || string(sent[0]) != string(sent[1]) {
		test.Fatalf("the two spellings did not normalize to one wire body: %s / %s", sent[0], sent[1])
	}
	for _, encoded := range sent {
		if !bytes.Contains(encoded, []byte(`{"type":"integer","value":"7"}`)) || !bytes.Contains(encoded, []byte(`{"type":"float","value":1}`)) || !bytes.Contains(encoded, []byte(`{"type":"text","value":"A"}`)) || !bytes.Contains(encoded, []byte(`"want_rows":true`)) {
			test.Fatalf("wire body kept the client spelling: %s", encoded)
		}
	}
}

// The management record of one diagnostic covers an open, the tool call and a
// close. It binds the command it authorizes at registration - the session open -
// instead of the HTTP descriptor it was reached through.
func TestManagementDiagnosticDigestBindsAuthorizedCommand(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	if _, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "digest_tool", Database: fixture.first}, "/v1/databases/checkpoint"); err != nil {
		test.Fatal(err)
	}
	record := operationByRequestID(test, fixture.state, "digest_tool")
	if record.Source != "management" || record.Action != "checkpoint" || record.State != "terminal" {
		test.Fatalf("management record semantics changed: %+v", record.OperationStatus)
	}
	if record.Digest != commandDigest(transmittedCommand(test, fixture.upstream, "open_session")) {
		test.Fatal("management digest does not cover the command the record authorizes")
	}
	descriptor, err := json.Marshal(map[string]any{"path": "/v1/databases/checkpoint", "database": fixture.first})
	if err != nil {
		test.Fatal(err)
	}
	if record.Digest == commandDigest(descriptor) {
		test.Fatal("management record still binds the HTTP request descriptor")
	}
	if fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 1 {
		test.Fatal("management record no longer covers one open and one close")
	}
}

// A digest written before the scheme tag existed still loads, and an unknown
// scheme is refused instead of being read under rules it never promised.
func TestOperationDigestSchemeLegacyAndUnknown(test *testing.T) {
	tagged := operationViewRecord("scheme_record", 1)
	state := operationViewState()
	state.OperationSequence = 1
	key := operationKey(tagged.Database, tagged.OperationID)
	loaded := func(digest string) error {
		candidate := tagged
		candidate.Digest = digest
		state.Operations = map[string]OperationRecord{key: candidate}
		return validateOperations(state, nil)
	}
	if err := loaded(tagged.Digest); err != nil {
		test.Fatalf("current scheme rejected at load: %v", err)
	}
	legacy := strings.Repeat("a", 64)
	if err := loaded(legacy); err != nil {
		test.Fatalf("legacy digest rejected at load: %v", err)
	}
	if taggedCommandDigest(legacy) {
		test.Fatal("legacy digest reported as comparable")
	}
	for _, digest := range []string{"", "invalid", "future_scheme:" + legacy, commandDigestPrefix + strings.Repeat("A", 64), commandDigestPrefix + strings.Repeat("a", 63), commandDigestPrefix} {
		if err := loaded(digest); err == nil {
			test.Fatalf("unknown digest scheme accepted at load: %q", digest)
		}
	}
	// No judgment reads the digest, so the domain it was written in cannot change
	// one: the isolation and recovery interpretations of both domains agree.
	quarantined := tagged
	quarantined.State, quarantined.MayHaveCommitted, quarantined.BusinessOutcome = "quarantined", true, "unknown"
	historic := quarantined
	historic.Digest = legacy
	isolation := func(record OperationRecord) (OperationStatus, bool) {
		view := operationViewState()
		view.Operations[key] = record
		return isolatedOperation(view, record.Database, false)
	}
	current, currentFound := isolation(quarantined)
	before, beforeFound := isolation(historic)
	if !currentFound || !beforeFound || current != before || operationNeedsRecovery(quarantined) != operationNeedsRecovery(historic) {
		test.Fatal("digest domain changed an isolation or recovery judgment")
	}
	// A new record never starts in the legacy domain.
	store := operationDurableStore(test)
	database := securityDatabase()
	if _, err := settleNonCommittingOperation(store, database, "legacy_write", "gateway", "describe", "tool_rpc", "worker-a", "", legacy, nil); err == nil {
		test.Fatal("the write path accepted an untagged digest")
	}
	if _, err := settleNonCommittingOperation(store, database, "tagged_write", "gateway", "describe", "tool_rpc", "worker-a", "", frozenTestCommand(test, nil).digest(), nil); err != nil {
		test.Fatalf("the write path rejected the current scheme: %v", err)
	}
}

// The command text of a statement - SQL and bound values - is durable evidence
// only as a digest: neither the recorded state nor the log may carry it.
func TestOperationDigestKeepsCommandTextOutOfDurableEvidence(test *testing.T) {
	fixture := newGatewayFixture(test)
	marker, sql := "private-argument-9f2c", "SELECT ? FROM sealed_evidence"
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	test.Cleanup(func() { log.SetOutput(previous) })
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": sql, "args": []any{map[string]any{"type": "text", "value": marker}}, "request_id": "digest_evidence"})
	if response.Code != 200 {
		test.Fatalf("statement rejected: %d %s", response.Code, response.Body.String())
	}
	record := operationByRequestID(test, fixture.state, "digest_evidence")
	if record.Digest != commandDigest(transmittedCommand(test, fixture.upstream, "statement")) {
		test.Fatal("the recorded digest does not cover the transmitted command")
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	durable, err := json.Marshal(state)
	if err != nil {
		test.Fatal(err)
	}
	for _, text := range []string{marker, sql} {
		if bytes.Contains(durable, []byte(text)) {
			test.Fatalf("command text reached the durable operation view: %s", text)
		}
		if strings.Contains(logged.String(), text) {
			test.Fatalf("command text reached the log: %s", text)
		}
		if strings.Contains(response.Body.String(), text) {
			test.Fatalf("command text echoed back to the caller: %s", text)
		}
	}
}

// The writer protocol is the default transport, and it is the stricter one: the
// platform validates the statement shape before the request leaves the process.
// The bytes the record digested have to be the bytes that pass that validation.
func TestOperationDigestCoversWriterProtocolStatement(test *testing.T) {
	fixture, writer := newWriterGatewayFixture(test)
	session := writerGatewaySession(test, fixture)
	before := writer.writerOperationCount("statement")
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "INSERT INTO records(id) VALUES(?)", "args": []any{map[string]any{"type": "integer", "value": "1"}}, "request_id": "writer_statement"})
	if response.Code != 200 {
		test.Fatalf("writer statement rejected: %d %s", response.Code, response.Body.String())
	}
	if statements := writer.writerOperationCount("statement") - before; statements != 1 {
		test.Fatalf("statement did not travel the writer protocol path: %d calls", statements)
	}
	record := operationByRequestID(test, fixture.state, "writer_statement")
	sent := transmittedCommand(test, fixture.upstream, "statement")
	if record.Digest != commandDigest(sent) {
		test.Fatalf("writer digest does not cover the transmitted statement: %s vs %s", record.Digest, commandDigest(sent))
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(sent, &fields) != nil || !writerCommandValid("statement", sent, fields) {
		test.Fatalf("writer protocol rejects the digested statement bytes: %s", sent)
	}
	if string(fields["want_rows"]) != "true" || len(fields) != 6 {
		test.Fatalf("writer statement carries no materialized row projection: %s", sent)
	}
}

// The real writer transport is the only place that decides whether a command
// shape may reach a worker. Fixed frozen bytes must arrive byte for byte and be
// accepted: no second encoding stands between the digest and the wire.
func TestFrozenCommandDigestMatchesWriterWire(test *testing.T) {
	frozen, err := freezeCommand(map[string]any{
		"command":    "statement",
		"session":    strings.Repeat("a", 64),
		"sql":        "INSERT INTO records(id) VALUES(?)",
		"args":       []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}},
		"named_args": []namedSQLArgument{},
		"want_rows":  true,
	})
	if err != nil {
		test.Fatal(err)
	}
	reply := writerEnvelope(test, writerTestState(), true, false)
	var captured atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			test.Error(readErr)
			return
		}
		captured.Store(body)
		_, _ = writer.Write(reply)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker": strings.Repeat("t", 32)})
	if err != nil {
		test.Fatal(err)
	}
	state, err := upstream.(WriterSQLUpstream).CallWriter(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "request", frozen, new(any))
	if err != nil || state == nil {
		test.Fatalf("writer transport rejected the frozen statement: %v", err)
	}
	body, reached := captured.Load().([]byte)
	if !reached {
		test.Fatal("the frozen statement never reached the worker transport")
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil || string(envelope["version"]) != "2" {
		test.Fatalf("invalid writer envelope: %s", body)
	}
	if !bytes.Equal(envelope["command"], frozen.bytes()) {
		test.Fatalf("writer wire body is not the frozen command: %s vs %s", envelope["command"], frozen.bytes())
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(frozen.bytes(), &fields) != nil || !writerCommandValid(frozen.operationName(), frozen.bytes(), fields) {
		test.Fatalf("writer protocol rejects the digested bytes: %s", frozen.bytes())
	}
	if string(fields["want_rows"]) != "true" || len(fields) != 6 {
		test.Fatalf("frozen statement carries no materialized row projection: %s", frozen.bytes())
	}
	if frozen.digest() != commandDigest(frozen.bytes()) {
		test.Fatal("the digest does not cover the frozen bytes")
	}
}
