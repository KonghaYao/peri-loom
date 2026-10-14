package faultprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

type testRequest struct {
	RequestID string     `json:"request_id"`
	Session   string     `json:"session"`
	SQL       string     `json:"sql"`
	Database  DatabaseID `json:"database"`
	WorkerID  string     `json:"worker_id"`
}

func wireReply(writer http.ResponseWriter, request testRequest, result any, code string, status int) {
	var failure any
	if code != "" {
		failure = map[string]any{"code": code}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": request.RequestID, "ok": code == "", "result": result, "error": failure})
}

func grantSnapshot(database DatabaseID, epoch uint64, worker string) any {
	return map[string]any{"placement": map[string]any{"database": database, "epoch": epoch, "phase": "active", "desired_worker": worker, "ttl_ms": 5000, "grant": map[string]any{"binding": map[string]any{"worker_id": worker, "epoch": epoch, "role": "writer", "renewal_sequence": 1, "lease_id": "private-lease"}}}, "diagnostics": map[string]any{"status": "unavailable", "reason": "optional_native_diagnostics"}}
}

func TestMockOwnerCrashACKLedgerNewEpochIntegrity(t *testing.T) {
	for _, mode := range []string{"recover", "unknown_write", "corrupt_recovery", "owner_changed"} {
		t.Run(mode, func(t *testing.T) {
			config, connection, topology, host, log := unitFixture(t)
			var state sync.Mutex
			requests := map[string]bool{}
			sessions, writes, statusCalls := 0, 0, 0
			balance := int64(100000)
			ledger := [][]any{}
			oldSession := ""
			fixture := &Fixture{Database: connection.Database, Prefix: "peri_stress_unit", log: log}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				state.Lock()
				defer state.Unlock()
				var input testRequest
				if json.NewDecoder(request.Body).Decode(&input) != nil {
					t.Error("invalid request")
				}
				if requests[input.RequestID] {
					t.Error("request ID replay")
				}
				requests[input.RequestID] = true
				if request.Header.Get("Authorization") != "Bearer "+connection.ProjectAPIKey && request.Header.Get("Authorization") != "Bearer "+connection.GlobalAPIKey {
					t.Error("unexpected credential")
				}
				switch request.URL.Path {
				case "/v1/databases/status":
					statusCalls++
					epoch := uint64(18446744073709551000)
					worker := "worker-a"
					if host.killed() || mode == "owner_changed" && statusCalls > 1 {
						epoch++
						worker = "worker-b"
					}
					wireReply(writer, input, grantSnapshot(input.Database, epoch, worker), "", 200)
				case "/v1/sql/open":
					sessions++
					opaque := fmt.Sprintf("%032x", sessions)
					if oldSession == "" {
						oldSession = opaque
					}
					wireReply(writer, input, map[string]any{"session": opaque, "readonly": false}, "", 200)
				case "/v1/sql/execute":
					if host.killed() && input.Session == oldSession {
						t.Error("old killed worker session reused")
					}
					if strings.HasPrefix(input.SQL, "BEGIN IMMEDIATE") {
						writes++
						if mode == "unknown_write" && writes == 2 {
							wireReply(writer, input, nil, "outcome_unknown", 502)
							return
						}
						delta := int64(writes%7) - 3
						balance += delta
						ledger = append(ledger, []any{writes, fmt.Sprintf("%s_business_%04d", fixture.Prefix, writes), delta})
						if !strings.Contains(input.SQL, "COMMIT") || !strings.Contains(input.SQL, "_ledger") {
							t.Error("write lacks transactional business ledger")
						}
					}
					wireReply(writer, input, map[string]any{"executed": true}, "", 200)
				case "/v1/sql/query":
					var rows [][]any
					switch {
					case strings.Contains(input.SQL, "_ledger"):
						rows = ledger
						if mode == "corrupt_recovery" {
							rows = ledger[1:]
						}
					case strings.Contains(input.SQL, "_account"):
						rows = [][]any{{1, balance}}
					case input.SQL == "PRAGMA integrity_check":
						rows = [][]any{{"ok"}}
					default:
						t.Error("unsupported fixture query")
					}
					wireReply(writer, input, rows, "", 200)
				case "/v1/sql/close":
					if input.Session == oldSession {
						t.Error("close attempted against deliberately killed session")
					}
					wireReply(writer, input, map[string]any{"closed": true}, "", 200)
				default:
					t.Errorf("unexpected actual API: %s", request.URL.Path)
				}
			}))
			defer server.Close()
			connection.Endpoint = server.URL
			gateway, err := newClient(connection, connection.ProjectAPIKey, "test_sql", config, log)
			if err != nil {
				t.Fatal(err)
			}
			management, err := newClient(connection, connection.GlobalAPIKey, "test_management", config, log)
			if err != nil {
				t.Fatal(err)
			}
			identities := map[string]ProcessIdentity{"worker-a": host.identities[topology.Processes["worker-a"].PID], "worker-b": host.identities[topology.Processes["worker-b"].PID]}
			failure := ownerCrash(context.Background(), config, topology, gateway, management, fixture, host, identities, log)
			state.Lock()
			defer state.Unlock()
			switch mode {
			case "recover":
				if failure != nil || !host.killed() || sessions != 2 || len(fixture.ACKs) != config.ACKs+1 {
					t.Fatalf("bounded fake recovery failed: %v", failure)
				}
			case "unknown_write":
				if failure == nil || !failure.Unknown || host.killed() || writes != 2 || sessions != 1 || len(fixture.ACKs) != 1 {
					t.Fatalf("unknown write did not stop fault: %v", failure)
				}
			case "corrupt_recovery":
				if failure == nil || failure.Code != "ack_ledger_mismatch" {
					t.Fatalf("lost ACK not detected: %v", failure)
				}
			case "owner_changed":
				if failure == nil || host.killed() {
					t.Fatal("changed owner signaled")
				}
			}
		})
	}
}

func TestUnknownReplyPoisonsNoSQLRetryOrClose(t *testing.T) {
	config, connection, _, _, log := unitFixture(t)
	var state sync.Mutex
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		state.Lock()
		defer state.Unlock()
		calls++
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	connection.Endpoint = server.URL
	client, err := newClient(connection, connection.ProjectAPIKey, "single", config, log)
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{client: client, id: strings.Repeat("a", 32)}
	_, failure := session.SQL(context.Background(), false, "COMMIT")
	if failure == nil || !failure.Unknown {
		t.Fatal("lost reply not unknown")
	}
	_, next := session.SQL(context.Background(), true, "SELECT 1")
	closed := session.Close(context.Background())
	state.Lock()
	defer state.Unlock()
	if next == nil || closed == nil || calls != 1 {
		t.Fatal("uncertain outcome allowed more requests")
	}
}

func TestBoundedBodyAndRedirectNeverReplay(t *testing.T) {
	for _, mode := range []string{"oversized", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			config, connection, _, _, log := unitFixture(t)
			var state sync.Mutex
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				state.Lock()
				defer state.Unlock()
				calls++
				if mode == "redirect" {
					writer.Header().Set("Location", "/other")
					writer.WriteHeader(307)
				} else {
					writer.Header().Set("Content-Length", "1048577")
					writer.WriteHeader(200)
				}
			}))
			defer server.Close()
			connection.Endpoint = server.URL
			client, err := newClient(connection, connection.ProjectAPIKey, "bound", config, log)
			if err != nil {
				t.Fatal(err)
			}
			session := &Session{client: client, id: strings.Repeat("a", 32)}
			if _, failure := session.SQL(context.Background(), false, strings.Repeat("x", 65537)); failure == nil || failure.Unknown {
				t.Fatal("SQL preflight bound bypass")
			}
			if _, failure := session.SQL(context.Background(), true, "SELECT 1"); failure == nil || !failure.Unknown {
				t.Fatal("bad response not uncertain")
			}
			state.Lock()
			defer state.Unlock()
			if calls != 1 {
				t.Fatal("body bound/redirect retried")
			}
		})
	}
}

func TestTerminalNegativeSQLDoesNotPoisonSession(t *testing.T) {
	config, connection, _, _, log := unitFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input testRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		if input.SQL == "duplicate" {
			writer.WriteHeader(400)
			_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": input.RequestID, "ok": false, "result": nil, "error": map[string]any{"code": "sql_error", "execution_completed": true, "may_have_committed": true}})
			return
		}
		wireReply(writer, input, map[string]any{"executed": true}, "", 200)
	}))
	defer server.Close()
	connection.Endpoint = server.URL
	client, err := newClient(connection, connection.ProjectAPIKey, "negative", config, log)
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{client: client, id: strings.Repeat("a", 32)}
	_, failure := session.SQL(context.Background(), false, "duplicate")
	if failure == nil || failure.Unknown || !failure.MayHaveCommitted || session.poisoned {
		t.Fatal("terminal negative poisoned")
	}
	if _, failure = session.SQL(context.Background(), false, "ROLLBACK"); failure != nil {
		t.Fatal("explicit rollback unusable")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.file.Sync() != nil {
		t.Fatal("evidence sync")
	}
	body, err := os.ReadFile(log.file.Name())
	if err != nil || strings.Contains(string(body), connection.ProjectAPIKey) {
		t.Fatal("credential leaked")
	}
}
