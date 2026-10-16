package stress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func respondWireError(writer http.ResponseWriter, identifier string, failure map[string]any, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": false, "result": nil, "error": failure})
}

func TestTerminalSQLErrorContractAndUncertainStop(t *testing.T) {
	cases := []struct {
		name      string
		code      string
		completed any
		committed any
		status    int
		known     bool
		risk      bool
	}{
		{"known_negative", "sql_error", true, true, 400, true, true},
		{"settled_without_commit_risk", "sql_error", true, false, 400, true, false},
		{"missing_completed", "sql_error", nil, true, 400, false, false},
		{"missing_committed", "sql_error", true, nil, 400, false, false},
		{"contradictory_pair", "sql_error", false, true, 400, false, false},
		// The proof decides, not the status the platform happened to answer with:
		// a settled statement stays a definite answer even on a platform failure.
		{"platform_status_with_proof", "sql_error", true, true, 500, true, true},
		{"unknown_even_with_flags", "outcome_unknown", true, true, 400, false, false},
		{"malformed_flag", "sql_error", "true", true, 400, false, false},
	}
	for _, testcase := range cases {
		t.Run(testcase.name, func(t *testing.T) {
			var state sync.Mutex
			requests := 0
			identifiers := map[string]bool{}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				state.Lock()
				defer state.Unlock()
				var input wireRequest
				if json.NewDecoder(request.Body).Decode(&input) != nil {
					t.Error("invalid request")
				}
				requests++
				if identifiers[input.RequestID] {
					t.Error("request ID reused")
				}
				identifiers[input.RequestID] = true
				if requests == 1 {
					failure := map[string]any{"code": testcase.code}
					if testcase.completed != nil {
						failure["execution_completed"] = testcase.completed
					}
					if testcase.committed != nil {
						failure["may_have_committed"] = testcase.committed
					}
					respondWireError(writer, input.RequestID, failure, testcase.status)
				} else if request.URL.Path == "/v1/sql/close" {
					respond(writer, input.RequestID, map[string]any{"closed": true}, "", 200)
				} else {
					respond(writer, input.RequestID, [][]any{{1}}, "", 200)
				}
			}))
			defer server.Close()
			session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
			_, _, failure := session.SQL(context.Background(), false, "INSERT INTO example VALUES(1)")
			if failure == nil || failure.Unknown == testcase.known || session.poisoned == testcase.known {
				t.Fatalf("incorrect terminal classification: %+v", failure)
			}
			if testcase.known && (failure.Class != "sql_error" || !failure.ExecutionCompleted || failure.MayHaveCommitted != testcase.risk) {
				t.Fatalf("typed flags lost: %+v", failure)
			}
			_, _, next := session.SQL(context.Background(), true, "SELECT 1")
			closed := session.Close(context.Background())
			state.Lock()
			defer state.Unlock()
			if testcase.known {
				if next != nil || closed != nil || requests != 3 {
					t.Fatalf("known negative poisoned session or retried: requests=%d next=%v close=%v", requests, next, closed)
				}
			} else if next == nil || closed == nil || requests != 1 {
				t.Fatalf("uncertain result allowed further requests: %d", requests)
			}
		})
	}
}

func TestConstraintErrorSameSessionExplicitRollbackAndReuse(t *testing.T) {
	for _, outcome := range []string{"known", "unknown", "lost_reply", "rollback_unknown", "unexpected_success"} {
		t.Run(outcome, func(t *testing.T) {
			var state sync.Mutex
			opaque := strings.Repeat("b", 32)
			balance := int64(100000)
			accountBalance := int64(100000)
			transaction := false
			var actions []string
			identifiers := map[string]bool{}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				state.Lock()
				defer state.Unlock()
				var input wireRequest
				if json.NewDecoder(request.Body).Decode(&input) != nil {
					t.Error("invalid input")
				}
				if identifiers[input.RequestID] {
					t.Error("request replay")
				}
				identifiers[input.RequestID] = true
				if request.URL.Path == "/v1/sql/open" {
					actions = append(actions, "open")
					respond(writer, input.RequestID, map[string]any{"session": opaque, "readonly": false}, "", 200)
					return
				}
				if input.Session != opaque {
					t.Error("opaque session changed")
				}
				if request.URL.Path == "/v1/sql/close" {
					actions = append(actions, "close")
					if transaction {
						t.Error("close before explicit rollback")
					}
					respond(writer, input.RequestID, map[string]any{"closed": true}, "", 200)
					return
				}
				var result any = map[string]any{"executed": true}
				switch {
				case strings.HasPrefix(input.SQL, "CREATE TABLE"):
					actions = append(actions, "seed")
				case input.SQL == "BEGIN":
					actions = append(actions, "begin")
					transaction = true
				case strings.HasPrefix(input.SQL, "UPDATE"):
					actions = append(actions, "update")
					if !transaction {
						t.Error("update outside explicit transaction")
					}
					balance += 17
				case strings.HasPrefix(input.SQL, "INSERT INTO"):
					actions = append(actions, "duplicate")
					if !transaction || balance != 100017 {
						t.Error("duplicate must follow uncommitted update")
					}
					if outcome == "lost_reply" {
						connection, _, err := writer.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = connection.Close()
						return
					}
					if outcome != "unexpected_success" {
						code := "sql_error"
						if outcome == "unknown" {
							code = "outcome_unknown"
						}
						respondWireError(writer, input.RequestID, map[string]any{"code": code, "execution_completed": true, "may_have_committed": true}, 400)
						return
					}
				case input.SQL == "ROLLBACK":
					actions = append(actions, "rollback")
					if !transaction || balance != 100017 {
						t.Error("must not assume error implicitly rolled back")
					}
					if outcome == "rollback_unknown" {
						respondWireError(writer, input.RequestID, map[string]any{"code": "outcome_unknown"}, 503)
						return
					}
					balance, transaction = 100000, false
				case strings.Contains(input.SQL, "constraint_accounts"):
					actions = append(actions, "seed_query")
					result = [][]any{{balance}}
				case strings.HasPrefix(input.SQL, "BEGIN IMMEDIATE"):
					actions = append(actions, "ledger_write")
					accountBalance++
				case strings.HasPrefix(input.SQL, "SELECT balance"):
					actions = append(actions, "account_query")
					result = [][]any{{accountBalance}}
				default:
					t.Errorf("unexpected SQL: %s", input.SQL)
				}
				respond(writer, input.RequestID, result, "", 200)
			}))
			defer server.Close()
			fixture := &Fixture{Client: unitClient(t, server.URL, DefaultSettings()), Model: NewModel("stress_fixture", 1000, 42)}
			directory := filepath.Join(t.TempDir(), "proof")
			artifacts, err := NewArtifacts(directory, 1<<20, []string{testKey})
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.Close()
			failure := constraintErrorReuse(context.Background(), fixture, artifacts)
			state.Lock()
			defer state.Unlock()
			expected := []string{"open", "seed", "seed_query", "begin", "update", "duplicate"}
			switch outcome {
			case "known":
				expected = append(expected, "rollback", "seed_query", "ledger_write", "account_query", "close")
				if failure != nil || len(fixture.Acked) != 1 || balance != 100000 || accountBalance != 100001 {
					t.Fatalf("proof failed: %v acked=%v", failure, fixture.Acked)
				}
			case "unexpected_success":
				expected = append(expected, "rollback", "close")
				if failure == nil || failure.Class != "correctness" {
					t.Fatalf("unexpected success incorrectly passed: %v", failure)
				}
			default:
				if outcome == "rollback_unknown" {
					expected = append(expected, "rollback")
				}
				if failure == nil || !failure.Unknown || len(fixture.Acked) != 0 {
					t.Fatalf("uncertain operation did not stop: %v", failure)
				}
			}
			if !reflect.DeepEqual(actions, expected) {
				t.Fatalf("sequence got %v want %v", actions, expected)
			}
			encoded, err := os.ReadFile(filepath.Join(directory, "operations.jsonl"))
			var proof struct {
				Status string           `json:"status"`
				Steps  []constraintStep `json:"steps"`
			}
			if err != nil || json.Unmarshal(encoded, &proof) != nil {
				t.Fatalf("proof unavailable: %v", err)
			}
			status := "UNRESOLVED"
			if outcome == "known" {
				status = "PASS"
			} else if outcome == "unexpected_success" {
				status = "FAIL"
			}
			if proof.Status != status {
				t.Fatalf("proof status got %s want %s", proof.Status, status)
			}
			if outcome == "known" {
				negative := proof.Steps[4]
				if negative.Name != "duplicate_primary_key" || negative.Acked || negative.Failure == nil || !negative.Failure.ExecutionCompleted || !negative.Failure.MayHaveCommitted {
					t.Fatalf("expected SQL error falsely counted as ack or lost flags: %+v", negative)
				}
			}
		})
	}
}

func TestMetricsScopesKeepActualWholeNativeMap(t *testing.T) {
	actual := json.RawMessage(`{"native":{"authority":{"get_total":3},"workers":{"worker_a":{"vm_steps":123,"cold":7,"wal_bytes":456,"segments":2,"cache_bytes":789}}},"measurement_scopes":{"authority":"global","workers":"per_worker"},"future_actual_counter":9223372036854775807}`)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "GET" || request.URL.Path != "/v1/metrics" || request.Header.Get("Authorization") != "Bearer "+testKey {
			t.Error("metrics auth or route changed")
		}
		respond(writer, "metrics", actual, "", 200)
	}))
	defer server.Close()
	metrics, failure := unitClient(t, server.URL, DefaultSettings()).Metrics(context.Background())
	if failure != nil || string(metrics) != string(actual) {
		t.Fatalf("actual map changed: %s %v", metrics, failure)
	}
	status, scopes := metricsScopes(metrics)
	if status != "available_reported_map" || string(scopes) != `{"authority":"global","workers":"per_worker"}` {
		t.Fatalf("actual scopes lost: %s %s", status, scopes)
	}
	for _, missing := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`{"native":null}`), json.RawMessage(`{"native":0}`)} {
		status, scopes = metricsScopes(missing)
		if status != "unavailable" || scopes != nil {
			t.Fatalf("missing metrics fabricated: %s %s", status, scopes)
		}
	}
	for _, scenario := range Scenarios() {
		if scenario.Name == "constraint_error_reuse" && scenario.Capability != "SUPPORTED" {
			t.Fatal("constraint reuse still skipped")
		}
	}
}
