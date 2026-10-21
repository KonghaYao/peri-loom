package platform

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type gatewayExpiredAuditIdentity struct {
	Identity
}

func (identity gatewayExpiredAuditIdentity) Authenticate(string) (Principal, error) {
	return Principal{Subject: "expired-subject", Project: "project-a", ExpiresMS: time.Now().Add(-time.Second).UnixMilli()}, nil
}

func gatewayFileAuditFixture(test *testing.T) (*gatewayFixture, Audit, string) {
	test.Helper()
	fixture := newGatewayFixture(test)
	path := filepath.Join(test.TempDir(), "gateway-audit.jsonl")
	audit, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	fixture.gateway.config.Audit = audit
	test.Cleanup(func() { _ = audit.Close() })
	return fixture, audit, path
}

func TestGatewayRealAuditUnauthenticatedDenials(test *testing.T) {
	for _, scenario := range []string{"missing-bearer", "invalid-token", "invalid-scheme", "revoked-token", "stale-db-token", "expired-principal", "bad-method", "bad-route", "bad-querystring"} {
		test.Run(scenario, func(test *testing.T) {
			fixture, audit, path := gatewayFileAuditFixture(test)
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			credential := fixture.tokenA
			method, route := "POST", "/v1/sql/query"
			expectedStatus, expectedCode := 401, "unauthenticated"
			switch scenario {
			case "invalid-token":
				credential = "plk1_invalid-private-credential"
			case "revoked-token":
				key, token, err := NewAPIKey(fixture.state, "revoked-subject", "project-a", "sql", false)
				if err != nil {
					test.Fatal(err)
				}
				credential = token
				if err := RevokeAPIKey(fixture.state, key.ID); err != nil {
					test.Fatal(err)
				}
			case "stale-db-token":
				principal, err := fixture.identity.Authenticate(fixture.tokenA)
				if err != nil {
					test.Fatal(err)
				}
				credential, err = fixture.identity.IssueToken(principal, fixture.first, "writer", time.Now().Add(time.Minute).UnixMilli())
				if err != nil {
					test.Fatal(err)
				}
				if err := fixture.state.Update(func(state *State) error {
					database := state.Databases[fixture.first.Key()]
					database.BranchIncarnation++
					state.Databases[fixture.first.Key()] = database
					return nil
				}); err != nil {
					test.Fatal(err)
				}
			case "expired-principal":
				fixture.gateway.config.Identity = gatewayExpiredAuditIdentity{fixture.identity}
			case "bad-method":
				method, expectedStatus, expectedCode = "GET", 400, "invalid_request"
			case "bad-route":
				route, expectedStatus, expectedCode = "/v1/sql/plk1_private-path", 400, "invalid_request"
			case "bad-querystring":
				route, expectedStatus, expectedCode = "/v1/sql/query?credential=private-query-secret", 400, "invalid_request"
			}
			body, _ := json.Marshal(map[string]any{"session": session, "sql": "SELECT private_audit_sql_secret", "request_id": "ignored-unauthenticated-id"})
			request := httptest.NewRequest(method, route, strings.NewReader(string(body)))
			if scenario != "missing-bearer" {
				request.Header.Set("Authorization", "Bearer "+credential)
			}
			if scenario == "invalid-scheme" {
				request.Header.Set("Authorization", "Basic "+credential)
			}
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			fixture.gateway.config.Identity = fixture.identity
			if response.Code != expectedStatus || gatewayCode(test, response) != expectedCode {
				test.Fatalf("auth/route denial changed by real audit: %d %s", response.Code, response.Body.String())
			}
			if fixture.upstream.count("query") != 0 || fixture.gateway.Metrics()["active_permits"] != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
				test.Fatal("preauthentication denial dispatched SQL or poisoned quota")
			}
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT private_audit_sql_secret"})
			if response.Code != 200 || fixture.upstream.count("query") != 1 {
				test.Fatal("valid request after auth denial could not reuse healthy session")
			}
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
			if response.Code != 200 {
				test.Fatal("healthy session close failed after auth denial")
			}
			if err := audit.Close(); err != nil {
				test.Fatal(err)
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				test.Fatal(err)
			}
			for _, secret := range []string{credential, fixture.tokenA, "private_audit_sql_secret", "private-query-secret", "plk1_private-path", "ignored-unauthenticated-id", "expired-subject"} {
				if strings.Contains(string(encoded), secret) {
					test.Fatal("audit contains untrusted credential, SQL, request data, or expired identity")
				}
			}
			lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
			if len(lines) != 4 {
				test.Fatal("audit omitted denial or subsequent authenticated actions")
			}
			for index, line := range lines {
				var record auditRecord
				if json.Unmarshal([]byte(line), &record) != nil {
					test.Fatal("invalid structured audit record")
				}
				event := record.Payload.Event
				if index == 1 {
					if event.Subject != "unauthenticated" || event.Project != "unauthenticated" || event.Database != "" || event.Outcome != expectedCode {
						test.Fatal("preauthentication audit fabricated identity or lost actual denial")
					}
				} else if event.Subject != "subject-a" || event.Project != "project-a" || event.Outcome != "ok" {
					test.Fatal("authenticated audit identity replaced with anonymous placeholder")
				}
			}
			reopened, err := OpenAudit(path)
			if err != nil {
				test.Fatal("real audit chain did not reopen after denied and valid requests")
			}
			_ = reopened.Close()
			if fixture.gateway.Metrics()["errors_total"].(map[string]uint64)["audit_unavailable"] != 0 {
				test.Fatal("healthy audit fabricated audit_unavailable metric")
			}
		})
	}
}

func TestGatewayRealAuditFailureStillFailClosed(test *testing.T) {
	for _, stage := range []string{"preauthentication", "after-sql"} {
		test.Run(stage, func(test *testing.T) {
			fixture, audit, _ := gatewayFileAuditFixture(test)
			session := ""
			if stage == "after-sql" {
				session = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			}
			if err := audit.Close(); err != nil {
				test.Fatal(err)
			}
			if stage == "preauthentication" {
				response := gatewayHTTP(test, fixture.gateway, "invalid-private-token", "/v1/sql/query", map[string]any{"session": "forged", "sql": "COMMIT"})
				if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" || fixture.upstream.count("query") != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
					test.Fatal("real preauthentication audit failure swallowed or pretended SQL dispatch")
				}
				return
			}
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT"})
			if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" || fixture.upstream.count("execute") != 1 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
				test.Fatal("real post-SQL audit failure lost known execution outcome")
			}
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
			if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" || fixture.upstream.count("query") != 1 {
				test.Fatal("audit outage confused session health with audit availability")
			}
		})
	}
}

// Capacity is knowable before dispatch, so a full completion reserve refuses
// the request at the entry instead of executing SQL it cannot record.
func TestGatewayAuditCapacityRefusesBeforeDispatch(test *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	held := make([]AuditReservation, 0, auditCompletionMaximum)
	for index := 0; index < auditCompletionMaximum; index++ {
		reservation, err := audit.ReserveCompletion()
		if err != nil {
			test.Fatalf("reserve %d: %v", index, err)
		}
		held = append(held, reservation)
	}
	defer func() {
		for _, reservation := range held {
			reservation.Release()
		}
	}()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 503 || gatewayCode(test, response) != "audit_unavailable" {
		test.Fatalf("full audit reserve did not refuse before dispatch: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("query") != 0 {
		test.Fatal("SQL was dispatched after audit admission refused")
	}
	status, ok := fixture.gateway.Metrics()["audit"].(map[string]any)
	if !ok || status["reserved_completion_slots"].(int) != auditCompletionMaximum {
		test.Fatalf("gateway metrics omit reserved completion slots: %+v", status)
	}
	if auditDroppedStatus(status, "completion") == 0 {
		test.Fatalf("refused completion capacity was not counted: %+v", status)
	}
	for _, reservation := range held {
		reservation.Release()
	}
	held = nil
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 200 || fixture.upstream.count("query") != 1 {
		test.Fatalf("released audit capacity did not resume service: %d %s", response.Code, response.Body.String())
	}
}

// An unauthenticated storm fills only the rejection budget. The authenticated
// query still reserves a completion slot and reaches the worker; its audit row
// waits for the same slow barrier instead of being refused.
func TestGatewayRejectionStormKeepsCompletionCapacity(test *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(test)
	audit := service.(*fileAudit)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	entered, release := blockAuditSync(audit)
	test.Cleanup(release)
	const extra = 16
	storm := auditRejectionQueueMaximum + extra
	responses := make(chan int, storm)
	for index := 0; index < storm; index++ {
		go func() {
			request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(`{"session":"forged","sql":"SELECT 1"}`))
			request.Header.Set("Authorization", "Bearer invalid-private-token")
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			responses <- response.Code
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		test.Fatal("audit writer did not reach the barrier")
	}
	waitAuditStatus(test, audit, "a full rejection budget", func(status map[string]any) bool {
		return auditClassStatus(status, "admitted_by_class", "rejection") == auditRejectionQueueMaximum &&
			auditDroppedStatus(status, "rejection") >= extra
	})
	queryResult := make(chan int, 1)
	go func() {
		request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(`{"session":"`+session+`","sql":"SELECT 1"}`))
		request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
		response := httptest.NewRecorder()
		fixture.gateway.ServeHTTP(response, request)
		queryResult <- response.Code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for fixture.upstream.count("query") != 1 {
		if time.Now().After(deadline) {
			test.Fatal("authenticated query was blocked by the unauthenticated storm")
		}
		time.Sleep(time.Millisecond)
	}
	release()
	for index := 0; index < storm; index++ {
		if code := <-responses; code != 401 {
			test.Fatalf("unauthenticated storm answered with %d, want 401", code)
		}
	}
	if code := <-queryResult; code != 200 {
		test.Fatalf("authenticated query lost its completion slot: %d", code)
	}
	if status := audit.AuditStatus(); status["admitted_count"].(int) != 0 {
		test.Fatalf("audit queue did not drain after the storm: %+v", status)
	}
}
