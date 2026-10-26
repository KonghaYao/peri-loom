package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

func TestSQLCursorFirstWireVersionsAndLostReplyNeverReplay(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		for _, scenario := range []string{"success", "truncated", "disconnect"} {
			t.Run(fmt.Sprintf("v%d/%s", version, scenario), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					calls.Add(1)
					var envelope struct {
						Version   uint32                     `json:"version"`
						RequestID string                     `json:"request_id"`
						Command   map[string]json.RawMessage `json:"command"`
					}
					if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Version != version || len(envelope.Command) != 6 || string(envelope.Command["command"]) != `"open_write_cursor_first"` || string(envelope.Command["max_rows"]) != "256" {
						t.Error("first-page command changed on wire")
					}
					if scenario == "disconnect" {
						conn, _, err := writer.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
					if scenario == "truncated" {
						_, _ = writer.Write([]byte(`{"version":`))
						return
					}
					response := map[string]any{"version": version, "request_id": envelope.RequestID, "ok": true, "error": nil, "result": map[string]any{"cols": []any{}, "rows": []any{}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}}
					if version == 2 {
						response["session_state"] = writerTestState()
					}
					_ = json.NewEncoder(writer).Encode(response)
				}))
				defer server.Close()
				upstream, err := NewSQLUpstream(map[string]string{"worker": sqlTestToken(t)})
				if err != nil {
					t.Fatal(err)
				}
				command := map[string]any{"command": "open_write_cursor_first", "session": strings.Repeat("a", 64), "sql": "INSERT INTO items VALUES(?)", "args": []sqlArgument{{Type: "integer", Value: json.RawMessage(`"1"`)}}, "named_args": []namedSQLArgument{}, "max_rows": 256}
				var result json.RawMessage
				state, err := upstream.(*sqlHTTPUpstream).callVersion(context.Background(), controller.Worker{ID: "worker", Address: server.URL}, "first-wire", command, &result, version)
				if calls.Load() != 1 {
					t.Fatalf("first page repeated %d times", calls.Load())
				}
				if scenario == "success" {
					if err != nil || !validCursorFirstResponse(result, 256, true) || (state != nil) != (version == 2) {
						t.Fatalf("first page failed: state=%+v err=%v body=%s", state, err, result)
					}
				} else {
					failure := sqlTestFailure(t, err)
					if !failure.Ambiguous || failure.evidence.Proven() {
						t.Fatalf("lost reply invented execution proof: %+v", failure)
					}
				}
			})
		}
	}
}
