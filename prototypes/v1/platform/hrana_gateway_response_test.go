package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestHranaGatewayResponseMatchesEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  gatewayResult
		capture bool
	}{
		{"statement", gatewayResult{value: json.RawMessage(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`)}, true},
		{"cursor_first", gatewayResult{value: json.RawMessage(" \n { \"cols\": [], \"rows\": [], \"done\": true, \"autocommit\": true } \t")}, true},
		{"html_and_unicode", gatewayResult{value: json.RawMessage("{\"text\":\"<>&\u2028\u2029\",\"escaped\":\"\\u003c\\u2028\"}")}, true},
		{"non_utf8", gatewayResult{value: json.RawMessage{'[', '"', 0xff, '"', ']'}}, true},
		{"numbers", gatewayResult{value: json.RawMessage(`[9223372036854775807,-0,1e+09,1.2300,1e999]`)}, true},
		{"scalar", gatewayResult{status: 200, value: json.RawMessage(` " spaced < text " `)}, true},
		{"boolean", gatewayResult{value: json.RawMessage(`true`)}, true},
		// Native replies are bounded to 128 levels before this handoff. The
		// enclosing HTTP envelope therefore stays below encoding/json's limit.
		{"native_depth_bound", gatewayResult{value: json.RawMessage(strings.Repeat("[", 128) + "0" + strings.Repeat("]", 128))}, true},
		{"null", gatewayResult{value: json.RawMessage(`null`)}, true},
		{"nil_raw", gatewayResult{value: json.RawMessage(nil)}, false},
		{"empty_raw", gatewayResult{value: json.RawMessage{}}, false},
		{"malformed_raw", gatewayResult{value: json.RawMessage(`{"rows":[}`)}, false},
		{"trailing_value", gatewayResult{value: json.RawMessage(`{} null`)}, false},
		{"non_json_html", gatewayResult{value: json.RawMessage(`<html>failed</html>`)}, false},
		{"map", gatewayResult{value: map[string]any{"session": "abc", "readonly": false}}, false},
		{"nil_value", gatewayResult{}, false},
		{"accepted", gatewayResult{status: 202, value: json.RawMessage(`{"state":"loading"}`)}, false},
		{"other_success_status", gatewayResult{status: 201, value: json.RawMessage(`{}`)}, false},
		{"unauthenticated", gatewayResult{status: 401, code: "unauthenticated", value: json.RawMessage(`{}`)}, false},
		{"audit_failure", gatewayResult{status: 503, code: "audit_unavailable", dispatched: true, auditExecutionCompleted: true, committing: true, value: json.RawMessage(`{}`)}, false},
		{"unknown", gatewayResult{status: 502, code: "outcome_unknown", operationID: "operation-1", operationStage: "statement", value: json.RawMessage(`{}`)}, false},
		{"stream", gatewayResult{value: json.RawMessage(`{}`), stream: []byte("{\"rows\":[]}\n")}, false},
		{"empty_stream", gatewayResult{value: json.RawMessage(`{}`), stream: []byte{}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway := &Gateway{}
			legacy := httptest.NewRecorder()
			gateway.writeResponse(legacy, "same-request", test.result)
			want := hranaGatewayReply(legacy.Code, legacy.Body.Bytes(), "/test")
			internal := newHranaGatewayResponse()
			gateway.writeResponse(internal, "same-request", test.result)
			got := internal.reply("/test")
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("internal reply differs from envelope: got %+v result=%q; want %+v result=%q", got, got.result, want, want.result)
			}
			if captured := internal.result != nil; captured != test.capture {
				t.Fatalf("capture=%t; want %t", captured, test.capture)
			}
			if internal.Code != legacy.Code || !reflect.DeepEqual(internal.Header(), legacy.Header()) {
				t.Fatalf("status or headers changed: internal=%d %v legacy=%d %v", internal.Code, internal.Header(), legacy.Code, legacy.Header())
			}
			if test.capture {
				if internal.Body.Len() != 0 {
					t.Fatal("successful raw result still encoded an internal HTTP envelope")
				}
			} else if !bytes.Equal(internal.Body.Bytes(), legacy.Body.Bytes()) {
				t.Fatal("fallback changed the original HTTP response bytes")
			}
		})
	}
}

func TestHranaGatewayResponseOwnsCompactResult(t *testing.T) {
	raw := json.RawMessage("\n { \"rows\" : [ [ \"<>&\" ] ], \"done\" : true } \t")
	response := newHranaGatewayResponse()
	(&Gateway{}).writeResponse(response, "ownership", gatewayResult{value: raw})
	reply := response.reply("/test")
	want := `{"rows":[["<>&"]],"done":true}`
	if string(reply.result) != want || len(reply.result) != len(want) {
		t.Fatalf("result budget would count different bytes: %q", reply.result)
	}
	for index := range raw {
		raw[index] = 'x'
	}
	next := newHranaGatewayResponse()
	(&Gateway{}).writeResponse(next, "next", gatewayResult{value: json.RawMessage(`{"rows":[],"done":true}`)})
	if string(reply.result) != want {
		t.Fatal("a source buffer change or later request changed the retained result")
	}
}

func TestHranaGatewayResponseWaitsForAudit(t *testing.T) {
	fixture := newGatewayFixture(t)
	audit := newGatedAudit()
	fixture.gateway.config.Audit = audit
	t.Cleanup(audit.unblock)
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	audit.holdNextRow()
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	request := httptest.NewRequest(http.MethodPost, "/test", nil)
	answered := make(chan hranaReply, 1)
	go func() {
		answered <- handler.call(request, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
	}()
	awaitClosed(t, audit.entered, "statement audit barrier")
	select {
	case reply := <-answered:
		t.Fatalf("raw result escaped before audit completion: %+v", reply)
	default:
	}
	audit.unblock()
	select {
	case reply := <-answered:
		if reply.status != 200 || reply.code != "" || !validStatementResult(reply.result) {
			t.Fatalf("statement did not succeed after audit completion: %+v", reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("statement did not answer after audit completion")
	}
	if fixture.upstream.count("statement") != 1 || len(audit.outcomesOf("statement")) != 1 {
		t.Fatal("raw handoff changed SQL dispatch or audit row count")
	}
}

func TestHranaGatewayResponseKeepsCompletionFailures(t *testing.T) {
	for _, terminalFailure := range []bool{false, true} {
		name := "audit"
		if terminalFailure {
			name = "terminal"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			store := newGatedStateStore(fixture.state)
			fixture.gateway.config.State = store
			session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
			wantCode, wantStatus := "audit_unavailable", http.StatusServiceUnavailable
			if terminalFailure {
				store.refuseCommit(2)
				wantCode, wantStatus = "outcome_unknown", http.StatusBadGateway
			} else {
				fixture.audit.failAction = "statement"
			}
			reply := newHranaHTTP(fixture.gateway).(*hranaHTTP).call(httptest.NewRequest(http.MethodPost, "/test", nil), fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
			if reply.status != wantStatus || reply.code != wantCode || reply.failure == nil || reply.result != nil {
				t.Fatalf("successful native result hid completion failure: %+v", reply)
			}
			if !terminalFailure && (!reply.failure.Executed() || !reply.failure.MayHaveCommitted() || reply.unknown) {
				t.Fatalf("audit failure lost the settled statement proof: %+v", reply)
			}
			if terminalFailure && !reply.unknown {
				t.Fatal("unconfirmed terminal record became a known outcome")
			}
			if fixture.upstream.count("statement") != 1 || len(fixture.audit.outcomesOf("statement")) != 1 {
				t.Fatal("completion failure changed SQL dispatch or audit row count")
			}
		})
	}
}

func TestHranaGatewayResponseKeepsCursorFirstValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		page  any
		quota int
		valid bool
	}{
		{"first_page", map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true, "autocommit": true}, 4096, true},
		{"malformed_first_page", map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1, 2}}, "done": true}, 4096, false},
		{"over_result_quota", map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{1}}, "done": true}, 16, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				if command["command"] == "open_read_cursor_first" {
					return test.page, nil
				}
				return nil, nil
			})
			session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
			if err := fixture.state.Update(func(state *State) error {
				project := state.Projects["project-a"]
				project.Quotas.MaxResultBytes = test.quota
				state.Projects[project.ID] = project
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			reply := newHranaHTTP(fixture.gateway).(*hranaHTTP).startCursor(httptest.NewRequest(http.MethodPost, "/test", nil), fixture.tokenA, map[string]any{"session": session, "sql": "SELECT 1", "max_rows": 256})
			if test.valid {
				if reply.status != 200 || reply.code != "" || !validCursorFirstResponse(reply.result, 256, false) {
					t.Fatalf("valid first page changed: %+v", reply)
				}
			} else if reply.status != 502 || reply.code != "outcome_unknown" || !reply.unknown || reply.result != nil {
				t.Fatalf("invalid or oversized first page escaped gateway validation: %+v", reply)
			}
			if fixture.upstream.count("open_read_cursor_first") != 1 || fixture.upstream.count("open_read_cursor") != 0 || fixture.upstream.count("fetch_read_cursor") != 0 {
				t.Fatal("first page was replayed or split into multiple RPCs")
			}
		})
	}
}

var benchmarkHranaGatewayReply hranaReply

// This measures only the internal response handoff. Authentication, native RPC,
// durable audit, client encoding and the connect/all/close lifecycle are outside
// this benchmark, so these timings are not business latency or sqld parity.
func BenchmarkHranaGatewayResultHandoff(b *testing.B) {
	for _, test := range []struct {
		name     string
		rows     int
		textSize int
	}{
		{"empty_page", 0, 0},
		{"first_page_20", 20, 64},
		{"page_256_1KiB", 256, 1024},
		{"page_256_16KiB", 256, 16 * 1024},
	} {
		rows := make([][]any, test.rows)
		for index := range rows {
			rows[index] = []any{index, strings.Repeat("x", test.textSize)}
		}
		raw, err := json.Marshal(map[string]any{"cols": []any{map[string]any{"name": "id", "decltype": "INTEGER"}, map[string]any{"name": "text", "decltype": "TEXT"}}, "rows": rows, "done": true, "autocommit": true})
		if err != nil {
			b.Fatal(err)
		}
		for _, internal := range []bool{false, true} {
			name := "HTTPEnvelope"
			if internal {
				name = "InternalResult"
			}
			b.Run(test.name+"/"+name, func(b *testing.B) {
				gateway := &Gateway{}
				result := gatewayResult{value: json.RawMessage(raw)}
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				var envelopeBytes int64
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					if internal {
						response := newHranaGatewayResponse()
						gateway.writeResponse(response, "same-request", result)
						benchmarkHranaGatewayReply = response.reply("/test")
						envelopeBytes += int64(response.Body.Len())
					} else {
						response := httptest.NewRecorder()
						gateway.writeResponse(response, "same-request", result)
						benchmarkHranaGatewayReply = hranaGatewayReply(response.Code, response.Body.Bytes(), "/test")
						envelopeBytes += int64(response.Body.Len())
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(envelopeBytes)/float64(b.N), "envelope_B/op")
			})
		}
	}
}
