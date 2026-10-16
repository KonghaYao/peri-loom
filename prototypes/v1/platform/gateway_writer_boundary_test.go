package platform

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGatewayWriterUnsupportedProfileNoNativeOpen(test *testing.T) {
	fixture, upstream := newWriterGatewayFixture(test)
	upstream.profileError = testRPCError(false, true, SQLRPCError{})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if gatewayCode(test, response) != "writer_profile_unavailable" || fixture.upstream.count("open_session") != 0 || upstream.profileCalls != 1 {
		test.Fatal("old/disabled worker was treated as reserve capable")
	}
	_, active, unknown := fixture.gateway.admission.status()
	if active != 0 || unknown != 0 {
		test.Fatal("stateless capability failure retained SQL permit")
	}
}

func TestGatewayWriterLowQuotaLegacyThenUpgradeRequiresHandshake(test *testing.T) {
	fixture, upstream := newWriterGatewayFixture(test)
	_ = fixture.state.Update(func(state *State) error {
		project := state.Projects["project-a"]
		project.Quotas.MaxConcurrent = 1
		state.Projects[project.ID] = project
		return nil
	})
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "acquire-writer"})
	if response.Code != 200 || upstream.writerCalls != 0 || upstream.profileCalls != 0 || fixture.gateway.admission.projects["project-a"].profile {
		test.Fatal("quota1 was expanded or forced to v2")
	}
	_ = fixture.state.Update(func(state *State) error {
		project := state.Projects["project-a"]
		project.Quotas.MaxConcurrent = 4
		state.Projects[project.ID] = project
		return nil
	})
	before := fixture.upstream.count("execute")
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT"})
	if gatewayCode(test, response) != "writer_profile_unavailable" || fixture.upstream.count("execute") != before {
		test.Fatal("unnegotiated old session claimed full-profile eligibility")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 {
		test.Fatal("legacy session could not explicitly close")
	}
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	if upstream.profileCalls != 1 || upstream.writerCalls != 1 {
		test.Fatal("fresh session skipped actual capability and v2 handshake")
	}
}

func TestGatewayWriterClientHintAndSQLPrefixCannotSelectOwner(test *testing.T) {
	fixture, _ := newWriterGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	before := fixture.upstream.count("execute")
	for _, field := range []string{"owner", "sequence", "session_state", "binding"} {
		body := map[string]any{"session": session, "sql": "COMMIT", field: true}
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", body)
		if gatewayCode(test, response) != "invalid_request" {
			test.Fatal("client owner hint accepted")
		}
	}
	if fixture.upstream.count("execute") != before {
		test.Fatal("forged hint dispatched SQL")
	}
	fixture.gateway.admission.mu.Lock()
	fixture.gateway.admission.projects["project-a"].tokens = 0
	fixture.gateway.admission.projects["project-a"].last = time.Now().Add(time.Second)
	fixture.gateway.admission.mu.Unlock()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT"})
	if gatewayCode(test, response) != "quota_exceeded" || fixture.upstream.count("execute") != before {
		test.Fatal("SQL prefix bypassed general rate through owner lane")
	}
}

func TestGatewayWriterOwnerRateCreditSurvivesGeneralExhaustion(test *testing.T) {
	fixture, _ := newWriterGatewayFixture(test)
	session := writerGatewaySession(test, fixture)
	fixture.gateway.admission.mu.Lock()
	fixture.gateway.admission.projects["project-a"].tokens = 0
	fixture.gateway.admission.projects["project-a"].last = time.Now().Add(time.Second)
	fixture.gateway.admission.mu.Unlock()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 200 {
		test.Fatal("general exhausted owner rate credit")
	}
	fixture.gateway.admission.mu.Lock()
	state := fixture.gateway.admission.projects["project-a"]
	exhausted := state.tokens < 1 && state.ownerTokens == 0
	state.last = time.Now().Add(time.Second)
	fixture.gateway.admission.mu.Unlock()
	if !exhausted {
		test.Fatal("first owner request did not consume emergency credit with general exhausted")
	}
	before := fixture.upstream.count("query")
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if gatewayCode(test, response) != "quota_exceeded" || fixture.upstream.count("query") != before {
		test.Fatal("owner bypassed finite original rate budget")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
	if response.Code != 200 {
		test.Fatal("bounded cleanup was subject to exhausted SQL rate")
	}
}

func TestGatewayWriterShutdownUsesSettledV2Cleanup(test *testing.T) {
	fixture, upstream := newWriterGatewayFixture(test)
	session := writerGatewaySession(test, fixture)
	record := fixture.gateway.sessions[session]
	before := upstream.writerCalls
	if err := fixture.gateway.Close(context.Background()); err != nil {
		test.Fatal(err)
	}
	if !record.closed || fixture.gateway.sessions[session] != nil {
		test.Fatal("shutdown failed to remove session")
	}
	if upstream.writerCalls != before+1 || fixture.upstream.count("close_session") != 1 {
		test.Fatal("shutdown skipped v2 cleanup verification")
	}
}

func TestWriterAdmissionClassificationBoundedAndSeparate(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxConcurrent: 4, RequestsPerSecond: 4, MaxSessions: 4, MaxSQLBytes: 64, MaxResultBytes: 64}
	var releases []func()
	for count := 0; count < 6; count++ {
		release, reason := gate.classify("project", quota)
		if reason != rejectionNone {
			test.Fatal("classification bound below C+2")
		}
		releases = append(releases, release)
	}
	if _, reason := gate.classify("project", quota); reason != rejectionClassifyLimit {
		test.Fatal("unbounded classification queue")
	}
	owner, reason := gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("parsing consumed SQL execution reserve")
	}
	owner.finish()
	for _, release := range releases {
		release()
		release()
	}
	if gate.projects["project"].classifying != 0 {
		test.Fatal("classification release not exact/idempotent")
	}
}

func TestGatewayWriterNativeStateNeverLeaksIntoNDJSON(test *testing.T) {
	fixture, _ := newWriterGatewayFixture(test)
	session := writerGatewaySession(test, fixture)
	request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(`{"session":"`+session+`","sql":"SELECT 1"}`))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Accept", "application/x-ndjson")
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Header().Get("Content-Type"), "ndjson") {
		test.Fatalf("NDJSON changed: %s", response.Body.String())
	}
	for _, field := range []string{"session_state", "binding", "owner", "sequence"} {
		if strings.Contains(response.Body.String(), `"`+field+`"`) {
			test.Fatal("private state leaked into row envelope")
		}
	}
}

func TestGatewayWriterProfileRequiresVersionedTransport(test *testing.T) {
	fixture := newGatewayFixture(test)
	config := fixture.gateway.config
	config.WriterAdmission = true
	if _, err := NewGateway(config); err == nil {
		test.Fatal("legacy transport silently claimed full profile")
	}
}
