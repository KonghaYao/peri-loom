package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

func writerTestState() SessionWriterState {
	return SessionWriterState{Session: strings.Repeat("a", 64), Binding: controller.Binding{Database: controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant", Database: "db"}, Branch: "main"}, RootIncarnation: "root", AuthorityTerm: 1, BranchIncarnation: 1, Epoch: 1, WorkerID: "worker", LeaseID: "lease", GrantID: "grant", Role: "writer"}, Owner: true, Sequence: 9007199254740993}
}

func writerEnvelope(test *testing.T, state any, success, completed bool) []byte {
	test.Helper()
	fields := map[string]any{"version": 2, "request_id": "request", "ok": success, "result": nil, "error": nil}
	if success {
		fields["result"] = [][]json.Number{{"9223372036854775807", "-9007199254740993"}}
	} else {
		fields["error"] = "native failed"
	}
	if completed {
		fields["execution_completed"] = true
	}
	if state != nil {
		fields["session_state"] = state
	}
	body, err := json.Marshal(fields)
	if err != nil {
		test.Fatal(err)
	}
	return body
}

func TestSQLWriterCodecSettledStateAndExactNumbers(test *testing.T) {
	for _, success := range []bool{true, false} {
		var output any
		state, err := sqlDecodeWriter(context.Background(), writerEnvelope(test, writerTestState(), success, !success), "request", &output)
		if state == nil || state.Sequence != 9007199254740993 || !state.Owner {
			test.Fatal("trusted state lost integer precision or owner")
		}
		if success {
			if err != nil {
				test.Fatal(err)
			}
			encoded, _ := json.Marshal(output)
			if string(encoded) != "[[9223372036854775807,-9007199254740993]]" {
				test.Fatalf("lossy result: %s", encoded)
			}
		} else {
			var failure *SQLRPCError
			if !errors.As(err, &failure) || failure.Ambiguous || !failure.evidence.Executed() {
				test.Fatal("known constraint terminal lost its owner state")
			}
		}
	}
}

func TestSQLWriterCodecStrictInvalidAndUnknown(test *testing.T) {
	valid := writerEnvelope(test, writerTestState(), true, false)
	mutations := []func(map[string]any){
		func(fields map[string]any) { delete(fields, "session_state") },
		func(fields map[string]any) { fields["session_state"] = nil },
		func(fields map[string]any) { fields["version"] = 1 },
		func(fields map[string]any) { fields["request_id"] = "wrong" },
		func(fields map[string]any) { fields["client_owner"] = true },
		func(fields map[string]any) { fields["execution_completed"] = true },
	}
	for _, field := range []string{"owner", "sequence", "session", "binding"} {
		name := field
		mutations = append(mutations, func(fields map[string]any) {
			state := fields["session_state"].(map[string]any)
			state[name] = nil
		})
	}
	for _, mutate := range mutations {
		var changed map[string]any
		_ = json.Unmarshal(valid, &changed)
		mutate(changed)
		body, _ := json.Marshal(changed)
		output := any("unchanged")
		state, err := sqlDecodeWriter(context.Background(), body, "request", &output)
		var failure *SQLRPCError
		if state != nil || !errors.As(err, &failure) || !failure.Ambiguous || output != "unchanged" {
			test.Fatalf("invalid hint accepted or result changed: %s", body)
		}
	}
	for _, state := range []any{nil, writerTestState()} {
		hint, err := sqlDecodeWriter(context.Background(), writerEnvelope(test, state, false, false), "request", nil)
		var failure *SQLRPCError
		if hint != nil || !errors.As(err, &failure) || !failure.Ambiguous {
			test.Fatal("unknown response adopted hint")
		}
	}
	if err := sqlDecode(context.Background(), valid, "request", nil); err == nil {
		test.Fatal("v1 decoder accepted v2")
	}
	invalidUTF8 := strings.Replace(string(valid), `"root"`, "\"\xff\"", 1)
	if _, err := sqlDecodeWriter(context.Background(), []byte(invalidUTF8), "request", nil); err == nil {
		test.Fatal("invalid UTF8 was normalized into a trusted state")
	}
}

func TestSQLWriterProfileStrictActualHandshake(test *testing.T) {
	profile := map[string]any{"protocol": 2, "writer_profile": true, "max_inflight": 8, "control_reserve": 2, "sql_total": 6, "general_sql": 4, "owner_sql": 1, "cleanup_sql": 1}
	encode := func(result any) []byte {
		body, _ := json.Marshal(map[string]any{"version": 2, "request_id": "probe", "ok": true, "result": result, "error": nil})
		return body
	}
	if err := sqlDecodeWriterProfile(context.Background(), encode(profile), "probe"); err != nil {
		test.Fatal("valid enabled worker profile rejected")
	}
	for _, mutation := range []func(map[string]any){
		func(fields map[string]any) { fields["writer_profile"] = false },
		func(fields map[string]any) { fields["protocol"] = 1 },
		func(fields map[string]any) { fields["sql_total"] = 2 },
		func(fields map[string]any) { fields["general_sql"] = 6 },
		func(fields map[string]any) { fields["owner_sql"] = 0 },
		func(fields map[string]any) { fields["cleanup_sql"] = 0 },
		func(fields map[string]any) { fields["max_inflight"] = 9 },
		func(fields map[string]any) { fields["control_reserve"] = 0 },
		func(fields map[string]any) { fields["owner_sql"] = nil },
		func(fields map[string]any) { delete(fields, "sql_total") },
		func(fields map[string]any) { fields["extra"] = true },
	} {
		copy := map[string]any{}
		for name, value := range profile {
			copy[name] = value
		}
		mutation(copy)
		if sqlDecodeWriterProfile(context.Background(), encode(copy), "probe") == nil {
			test.Fatalf("invalid profile accepted: %v", copy)
		}
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope map[string]json.RawMessage
		if json.NewDecoder(request.Body).Decode(&envelope) != nil || string(envelope["version"]) != "2" || string(envelope["command"]) != `{"command":"writer_profile_status"}` {
			test.Error("profile was not probed through actual v2 wire")
		}
		_, _ = writer.Write(encode(profile))
	}))
	defer server.Close()
	upstream, _ := NewSQLUpstream(map[string]string{"worker": strings.Repeat("t", 32)})
	if err := upstream.(WriterSQLUpstream).WriterProfile(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "probe"); err != nil || calls.Load() != 1 {
		test.Fatal("actual authenticated profile handshake failed", err)
	}
}

func TestSQLWriterWireVersionAndNoFallback(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope struct {
			Version   uint32         `json:"version"`
			RequestID string         `json:"request_id"`
			Command   map[string]any `json:"command"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != 2 || envelope.RequestID != "request" || request.URL.Path != "/sql" {
			test.Error("wrong private v2 request")
		}
		writer.WriteHeader(400)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker": strings.Repeat("t", 32)})
	if err != nil {
		test.Fatal(err)
	}
	state, err := upstream.(WriterSQLUpstream).CallWriter(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "request", map[string]any{"command": "query", "session": strings.Repeat("a", 64), "sql": "SELECT 1"}, new(any))
	var failure *SQLRPCError
	// A bare 400 body is not SQL evidence: the transport learns that the worker
	// answered, not that the statement was refused.
	if state != nil || !errors.As(err, &failure) || !failure.Ambiguous || failure.evidence.Rejected() || failure.evidence.Executed() || calls.Load() != 1 {
		test.Fatal("unsupported v2 retried or fell back")
	}
	_, _ = upstream.(WriterSQLUpstream).CallWriter(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "request", map[string]any{"command": "checkpoint", "session": strings.Repeat("a", 64)}, nil)
	if calls.Load() != 1 {
		test.Fatal("management command moved into v2")
	}
}

func TestSQLWriterWriteCursorCommandsReachVersionedWorker(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope struct {
			Version uint32 `json:"version"`
			Command struct {
				Command string `json:"command"`
			} `json:"command"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != 2 || !strings.Contains(envelope.Command.Command, "write_cursor") {
			test.Error("write cursor did not reach native v2 worker transport")
		}
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker": strings.Repeat("t", 32)})
	if err != nil {
		test.Fatal(err)
	}
	session := strings.Repeat("a", 64)
	commands := []map[string]any{
		{"command": "open_write_cursor", "session": session, "sql": "INSERT INTO records VALUES(1) RETURNING id", "args": []sqlArgument{}, "named_args": []namedSQLArgument{}},
		{"command": "fetch_write_cursor", "session": session, "max_rows": 256},
		{"command": "close_write_cursor", "session": session},
	}
	for _, command := range commands {
		_, err := upstream.(WriterSQLUpstream).CallWriter(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "cursor", command, new(any))
		var failure *SQLRPCError
		if !errors.As(err, &failure) || !failure.Ambiguous || failure.evidence.Rejected() || failure.evidence.Executed() {
			test.Fatalf("cursor transport claimed execution state from a status code: %v", err)
		}
	}
	if calls.Load() != int32(len(commands)) {
		test.Fatalf("write cursor command rejected before worker transport: %d", calls.Load())
	}
}
