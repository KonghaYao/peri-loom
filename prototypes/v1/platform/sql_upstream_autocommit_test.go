package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

func TestSQLSessionAutocommitWireSupportsV1AndV2(test *testing.T) {
	for _, version := range []uint32{1, 2} {
		for _, autocommit := range []bool{false, true} {
			test.Run(fmt.Sprintf("v%d/%t", version, autocommit), func(test *testing.T) {
				token := sqlTestToken(test)
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					calls.Add(1)
					var envelope struct {
						Version   uint32            `json:"version"`
						RequestID string            `json:"request_id"`
						Command   map[string]string `json:"command"`
					}
					if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != version || envelope.RequestID != "autocommit-wire" || len(envelope.Command) != 2 || envelope.Command["command"] != "session_autocommit" || envelope.Command["session"] != strings.Repeat("a", 64) || request.Header.Get("Authorization") != "Bearer "+token {
						test.Error("autocommit changed protocol, identity, or capability")
					}
					reply := map[string]any{"version": version, "request_id": envelope.RequestID, "ok": true, "result": map[string]bool{"autocommit": autocommit}, "error": nil}
					if version == 2 {
						reply["session_state"] = writerTestState()
					}
					_ = json.NewEncoder(writer).Encode(reply)
				}))
				defer server.Close()
				upstream, err := NewSQLUpstream(map[string]string{"worker": token})
				if err != nil {
					test.Fatal(err)
				}
				var reply struct {
					Autocommit *bool `json:"autocommit"`
				}
				state, err := upstream.(*sqlHTTPUpstream).callVersion(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "autocommit-wire", map[string]any{"command": "session_autocommit", "session": strings.Repeat("a", 64)}, &reply, version)
				if err != nil || reply.Autocommit == nil || *reply.Autocommit != autocommit || calls.Load() != 1 || (state != nil) != (version == 2) {
					test.Fatalf("autocommit dispatch failed: state=%+v reply=%+v calls=%d err=%v", state, reply, calls.Load(), err)
				}
				if state != nil && *state != writerTestState() {
					test.Fatal("autocommit lost the private writer state")
				}
			})
		}
	}
}

func TestSQLSessionAutocommitStrictBeforeDispatchAndMaintenanceStaysV1(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	worker := controller.Worker{ID: "worker", Address: server.URL}
	session := strings.Repeat("a", 64)
	for _, version := range []uint32{1, 2} {
		for _, command := range []any{
			map[string]any{"command": "session_autocommit"},
			map[string]any{"command": "session_autocommit", "session": nil},
			map[string]any{"command": "session_autocommit", "session": 7},
			map[string]any{"command": "session_autocommit", "session": "short"},
			map[string]any{"command": "session_autocommit", "session": session, "sql": "COMMIT"},
			json.RawMessage(`{"command":"session_autocommit","session":"` + session + `","session":"` + session + `"}`),
			json.RawMessage(`{"command":"diagnostics","command":"session_autocommit","session":"` + session + `"}`),
		} {
			_, err := upstream.(*sqlHTTPUpstream).callVersion(context.Background(), worker, "invalid-autocommit", command, nil, version)
			failure := sqlTestFailure(test, err)
			if failure.Ambiguous || failure.evidence.Executed() {
				test.Fatal("invalid autocommit was not rejected before dispatch")
			}
		}
	}
	for _, version := range []uint32{0, 3} {
		if _, err := upstream.(*sqlHTTPUpstream).callVersion(context.Background(), worker, "invalid-version", map[string]any{"command": "session_autocommit", "session": session}, nil, version); err == nil {
			test.Fatal("unsupported protocol version accepted")
		}
	}
	for _, operation := range []string{"checkpoint", "compact", "diagnostics"} {
		if _, err := upstream.(WriterSQLUpstream).CallWriter(context.Background(), worker, "private-v1-only", map[string]any{"command": operation, "session": session}, nil); err == nil {
			test.Fatalf("%s became a v2 capability", operation)
		}
	}
	if calls.Load() != 0 {
		test.Fatalf("invalid or v1-only command reached transport %d times", calls.Load())
	}
}

func TestSQLSessionAutocommitV2UnprovenReplyNeverDowngrades(test *testing.T) {
	for _, mode := range []string{"v1-reply", "missing-state", "wrong-request"} {
		test.Run(mode, func(test *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				var envelope struct {
					Version   uint32 `json:"version"`
					RequestID string `json:"request_id"`
				}
				if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != 2 {
					test.Error("autocommit was downgraded")
				}
				reply := map[string]any{"version": 2, "request_id": envelope.RequestID, "ok": true, "result": map[string]bool{"autocommit": true}, "error": nil, "session_state": writerTestState()}
				switch mode {
				case "v1-reply":
					reply["version"] = 1
					delete(reply, "session_state")
				case "missing-state":
					delete(reply, "session_state")
				case "wrong-request":
					reply["request_id"] = "unrelated"
				}
				_ = json.NewEncoder(writer).Encode(reply)
			}))
			defer server.Close()
			upstream, _ := NewSQLUpstream(map[string]string{"worker": sqlTestToken(test)})
			output := any("unchanged")
			state, err := upstream.(WriterSQLUpstream).CallWriter(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "request", map[string]any{"command": "session_autocommit", "session": strings.Repeat("a", 64)}, &output)
			var failure *SQLRPCError
			if !errors.As(err, &failure) || !failure.Ambiguous || state != nil || output != "unchanged" || calls.Load() != 1 {
				test.Fatalf("unproven reply changed state or retried: state=%+v calls=%d err=%v", state, calls.Load(), err)
			}
		})
	}
}

// Exercise the public Hrana adapter and the real Go HTTP upstream together.
// A direct gateway fixture alone bypasses the version gate that broke the SDK's
// get_autocommit request when writer admission selected protocol v2.
func TestHranaWriterAutocommitThroughHTTPUpstream(test *testing.T) {
	for _, autocommit := range []bool{false, true} {
		test.Run(fmt.Sprint(autocommit), func(test *testing.T) {
			fixture, _ := newWriterGatewayFixture(test)
			metadata, _ := fixture.placements.Snapshot()
			binding := metadata.Placements[fixture.first.Key()].Grant.Binding
			token := sqlTestToken(test)
			var sequence uint64
			var probes, statements atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					Version   uint32                     `json:"version"`
					RequestID string                     `json:"request_id"`
					Command   map[string]json.RawMessage `json:"command"`
				}
				if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != 2 || request.Header.Get("Authorization") != "Bearer "+token {
					test.Error("Hrana writer request did not use authenticated v2 transport")
				}
				var operation string
				_ = json.Unmarshal(envelope.Command["command"], &operation)
				reply := map[string]any{"version": 2, "request_id": envelope.RequestID, "ok": true, "error": nil}
				if operation == "writer_profile_status" {
					reply["result"] = map[string]any{"protocol": 2, "writer_profile": true, "max_inflight": 8, "control_reserve": 2, "sql_total": 6, "general_sql": 4, "owner_sql": 1, "cleanup_sql": 1}
				} else {
					sequence++
					reply["session_state"] = SessionWriterState{Session: strings.Repeat("a", 64), Binding: binding, Sequence: sequence}
					switch operation {
					case "open_session":
						reply["result"] = map[string]any{"session": strings.Repeat("a", 64), "readonly": false}
					case "session_autocommit":
						probes.Add(1)
						reply["result"] = map[string]bool{"autocommit": autocommit}
					case "guarded_readonly_statement":
						statements.Add(1)
						reply["result"] = guardedReadonlyReply([][]any{{1}})
					case "close_session":
						reply["result"] = map[string]bool{"closed": true}
					default:
						test.Errorf("unexpected worker command %q", operation)
					}
				}
				_ = json.NewEncoder(writer).Encode(reply)
			}))
			defer server.Close()
			if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
				worker := metadata.Workers[binding.WorkerID]
				worker.Address = server.URL
				metadata.Workers[binding.WorkerID] = worker
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			upstream, err := NewSQLUpstream(map[string]string{binding.WorkerID: token})
			if err != nil {
				test.Fatal(err)
			}
			fixture.gateway.config.Upstream = upstream
			body := `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"get_autocommit"},{"type":"batch","batch":{"steps":[{"condition":{"type":"is_autocommit"},"stmt":{"sql":"SELECT 2"}}]}},{"type":"close"}]}`
			response := hranaTraceCall(test, newHranaHTTP(fixture.gateway), fixture.tokenA, body, nil)
			if !strings.Contains(response.Body.String(), fmt.Sprintf(`"is_autocommit":%t`, autocommit)) || probes.Load() != 2 {
				test.Fatalf("SDK autocommit probe failed: probes=%d response=%s", probes.Load(), response.Body.String())
			}
			wantStatements := int32(1)
			if autocommit {
				wantStatements++
			}
			if statements.Load() != wantStatements {
				test.Fatal("autocommit condition executed the wrong number of SQL statements")
			}
			state, _ := fixture.state.Snapshot()
			for _, record := range state.Operations {
				if record.Action == "session_state" || record.Ambiguous {
					test.Fatalf("successful probe left a durable or ambiguous operation: %+v", record)
				}
			}
			if len(fixture.audit.outcomesOf("session_state")) != 0 || fixture.gateway.Metrics()["active_permits"] != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
				test.Fatal("autocommit probe added an audit row or retained a permit")
			}
			for _, forbidden := range []string{"session_state", "binding", "owner", "sequence"} {
				if strings.Contains(response.Body.String(), `"`+forbidden+`"`) {
					test.Fatal("private writer state leaked through Hrana")
				}
			}
		})
	}
}
