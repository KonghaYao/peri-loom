package platform

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func quotaManagementStore(test *testing.T) (StateStore, Identity, *gatewayTestAudit, string) {
	test.Helper()
	store, err := OpenStateStore(test.TempDir())
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = store.Close() })
	custom := legacyProjectQuotas
	custom.RequestsPerSecond = 42
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-legacy", Database: "db-main"}, Branch: "main"}
	if err := store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "quota tests"}
		state.Projects["legacy"] = Project{ID: "legacy", Organization: "org", Tenant: "tenant-legacy", Region: "local", Quotas: legacyProjectQuotas}
		state.Projects["custom"] = Project{ID: "custom", Organization: "org", Tenant: "tenant-custom", Region: "local", Quotas: custom}
		state.Databases[database.Key()] = Database{ID: database, Project: "legacy", State: "active"}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	credential := "quota-admin-credential-0123456789abcdef"
	if err := BootstrapAPIKey(store, credential); err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(store, bytes.Repeat([]byte{29}, 32))
	if err != nil {
		test.Fatal(err)
	}
	return store, identity, &gatewayTestAudit{}, credential
}

func quotaManagement(test *testing.T) (*Management, StateStore, *gatewayTestAudit, string) {
	test.Helper()
	store, identity, audit, credential := quotaManagementStore(test)
	return &Management{config: ManagementConfig{State: store, Identity: identity, Audit: audit}}, store, audit, credential
}

func quotaCall(management *Management, credential, path string, input any) (*httptest.ResponseRecorder, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+credential)
	response := httptest.NewRecorder()
	management.ServeHTTP(response, request)
	return response, nil
}

func quotaReply(test *testing.T, response *httptest.ResponseRecorder) (bool, uint64, Project, string) {
	test.Helper()
	var reply struct {
		OK     bool `json:"ok"`
		Result struct {
			Project  Project `json:"project"`
			Revision uint64  `json:"revision"`
		} `json:"result"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
		test.Fatalf("invalid quota reply: %s", response.Body.String())
	}
	return reply.OK, reply.Result.Revision, reply.Result.Project, reply.Error.Code
}

func TestQuotaUpdateRaisesExistingProjectWithoutRecreation(test *testing.T) {
	management, store, audit, credential := quotaManagement(test)
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	quotas := before.Projects["legacy"].Quotas
	quotas.RequestsPerSecond = 500
	response, err := quotaCall(management, credential, projectQuotasPath, map[string]any{
		"request_id": "quota_raise", "id": "legacy", "revision": before.Revision, "quotas": quotas,
	})
	if err != nil || response.Code != 200 {
		test.Fatalf("quota update failed: %v %s", err, response.Body.String())
	}
	ok, revision, project, code := quotaReply(test, response)
	if !ok || code != "" || revision != before.Revision+1 || project.Quotas.RequestsPerSecond != 500 {
		test.Fatalf("unexpected quota reply: ok=%t revision=%d code=%s quotas=%+v", ok, revision, code, project.Quotas)
	}
	after, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	stored := after.Projects["legacy"]
	if stored.Quotas.RequestsPerSecond != 500 || stored.Organization != "org" || stored.Tenant != "tenant-legacy" {
		test.Fatalf("project identity/quotas changed incorrectly: %+v", stored)
	}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-legacy", Database: "db-main"}, Branch: "main"}
	if _, exists := after.Databases[database.Key()]; !exists {
		test.Fatal("quota update required recreating the project's database")
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	found := false
	for _, event := range audit.events {
		if event.Action == projectQuotasPath && event.Project == "legacy" && event.RequestID == "quota_raise" && event.Outcome == "complete" {
			found = true
		}
	}
	if !found {
		test.Fatalf("quota update was not audited against its target project: %+v", audit.events)
	}
}

func TestQuotaMigrationPreservesExplicitValues(test *testing.T) {
	management, store, _, credential := quotaManagement(test)
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	response, err := quotaCall(management, credential, projectQuotasMigratePath, map[string]any{
		"request_id": "quota_migrate", "id": "legacy", "revision": before.Revision,
	})
	if err != nil || response.Code != 200 {
		test.Fatalf("quota migration failed: %v %s", err, response.Body.String())
	}
	_, _, migrated, code := quotaReply(test, response)
	if code != "" || migrated.Quotas.RequestsPerSecond != 500 {
		test.Fatalf("legacy project did not migrate to 500/s: code=%s quotas=%+v", code, migrated.Quotas)
	}
	after, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	response, err = quotaCall(management, credential, projectQuotasMigratePath, map[string]any{
		"request_id": "quota_migrate_custom", "id": "custom", "revision": after.Revision,
	})
	if err != nil || response.Code != 200 {
		test.Fatalf("custom quota migration failed: %v %s", err, response.Body.String())
	}
	var reply struct {
		Result struct {
			Migrated bool    `json:"migrated"`
			Project  Project `json:"project"`
		} `json:"result"`
	}
	if json.Unmarshal(response.Body.Bytes(), &reply) != nil {
		test.Fatal("invalid migration reply")
	}
	if reply.Result.Migrated || reply.Result.Project.Quotas.RequestsPerSecond != 42 {
		test.Fatalf("explicit 42 RPS was overwritten by defaults: %+v", reply.Result)
	}
	after, err = store.Snapshot()
	if err != nil || after.Projects["custom"].Quotas.RequestsPerSecond != 42 {
		test.Fatalf("explicit 42 RPS was not durable: %v %+v", err, after.Projects["custom"].Quotas)
	}
}

func TestQuotaUpdateRejectsStaleRevision(test *testing.T) {
	management, store, _, credential := quotaManagement(test)
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	quotas := before.Projects["legacy"].Quotas
	quotas.RequestsPerSecond = 250
	first, err := quotaCall(management, credential, projectQuotasPath, map[string]any{
		"request_id": "quota_first", "id": "legacy", "revision": before.Revision, "quotas": quotas,
	})
	if err != nil || first.Code != 200 {
		test.Fatalf("first quota update failed: %v %s", err, first.Body.String())
	}
	stale := quotas
	stale.RequestsPerSecond = 300
	second, err := quotaCall(management, credential, projectQuotasPath, map[string]any{
		"request_id": "quota_stale", "id": "legacy", "revision": before.Revision, "quotas": stale,
	})
	if err != nil {
		test.Fatal(err)
	}
	if ok, _, _, code := quotaReply(test, second); ok || second.Code != 409 || code != "revision_mismatch" {
		test.Fatalf("stale revision was accepted: status=%d code=%s body=%s", second.Code, code, second.Body.String())
	}
	after, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if after.Projects["legacy"].Quotas.RequestsPerSecond != 250 || after.Revision != before.Revision+1 {
		test.Fatalf("stale update changed durable state: revision=%d quotas=%+v", after.Revision, after.Projects["legacy"].Quotas)
	}
}

func TestQuotaConcurrentUpdatesAcceptOneRevision(test *testing.T) {
	management, store, _, credential := quotaManagement(test)
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	var wait sync.WaitGroup
	statuses := make([]int, 2)
	codes := make([]string, 2)
	for index, rate := range []int{210, 220} {
		wait.Add(1)
		go func(index, rate int) {
			defer wait.Done()
			quotas := before.Projects["legacy"].Quotas
			quotas.RequestsPerSecond = rate
			response, err := quotaCall(management, credential, projectQuotasPath, map[string]any{
				"request_id": "quota_concurrent", "id": "legacy", "revision": before.Revision, "quotas": quotas,
			})
			if err != nil {
				codes[index] = err.Error()
				return
			}
			statuses[index] = response.Code
			var reply struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if json.Unmarshal(response.Body.Bytes(), &reply) == nil {
				codes[index] = reply.Error.Code
			}
		}(index, rate)
	}
	wait.Wait()
	accepted := 0
	for index := range statuses {
		switch statuses[index] {
		case 200:
			accepted++
		case 409:
			if codes[index] != "revision_mismatch" {
				test.Fatalf("losing concurrent update used code %q", codes[index])
			}
		default:
			test.Fatalf("unexpected concurrent update status=%d code=%s", statuses[index], codes[index])
		}
	}
	if accepted != 1 {
		test.Fatalf("concurrent quota updates accepted %d writers, want 1 (statuses=%v codes=%v)", accepted, statuses, codes)
	}
	after, err := store.Snapshot()
	if err != nil || after.Revision != before.Revision+1 {
		test.Fatalf("concurrent updates wrote %d revisions: %v", after.Revision-before.Revision, err)
	}
}

func TestQuotaUpdateRejectsUnauthorizedPrincipal(test *testing.T) {
	management, store, audit, _ := quotaManagement(test)
	_, token, err := NewAPIKey(store, "project-admin", "legacy", "admin", false)
	if err != nil {
		test.Fatal(err)
	}
	before, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	quotas := before.Projects["legacy"].Quotas
	quotas.RequestsPerSecond = 500
	response, err := quotaCall(management, token, projectQuotasPath, map[string]any{
		"request_id": "quota_denied", "id": "legacy", "revision": before.Revision, "quotas": quotas,
	})
	if err != nil || response.Code != 403 {
		test.Fatalf("project-scoped admin was allowed to change quotas: %v %s", err, response.Body.String())
	}
	if _, _, _, code := quotaReply(test, response); code != "forbidden" {
		test.Fatalf("unauthorized quota update code = %q", code)
	}
	after, err := store.Snapshot()
	if err != nil || after.Projects["legacy"].Quotas != before.Projects["legacy"].Quotas || after.Revision != before.Revision {
		test.Fatalf("unauthorized quota update changed state: %v", err)
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	found := false
	for _, event := range audit.events {
		if event.Action == projectQuotasPath && event.RequestID == "quota_denied" && event.Outcome == "incomplete" {
			found = true
		}
	}
	if !found {
		test.Fatalf("denied quota update was not audited: %+v", audit.events)
	}
}

func TestGatewayReadsUpdatedQuotasWithoutRestart(test *testing.T) {
	fixture := newGatewayFixture(test)
	management := &Management{config: ManagementConfig{State: fixture.state, Identity: fixture.identity, Audit: fixture.audit}}
	credential := "gateway-quota-admin-0123456789abcdef"
	if err := fixture.state.Update(func(state *State) error {
		state.APIKeys["quotaadmin"] = APIKey{ID: "quotaadmin", Subject: "operator", Project: "*", Role: "admin", Digest: digestHex([]byte(credential))}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	before, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	quotas := before.Projects["project-a"].Quotas
	quotas.MaxSQLBytes = 8
	response, err := quotaCall(management, credential, projectQuotasPath, map[string]any{
		"request_id": "quota_gateway", "id": "project-a", "revision": before.Revision, "quotas": quotas,
	})
	if err != nil || response.Code != 200 {
		test.Fatalf("quota update failed: %v %s", err, response.Body.String())
	}
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	reply := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "SELECT 123"})
	if reply.Code != 413 || gatewayCode(test, reply) != "sql_limit" {
		test.Fatalf("running gateway kept the old quota: status=%d body=%s", reply.Code, reply.Body.String())
	}
}

func TestQuotaRateCutKeepsAccruedCredits(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
	quota := Quotas{MaxDatabases: 8, MaxSessions: 4, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 1024, MaxResultBytes: 1024}
	for attempt := 0; attempt < 2; attempt++ {
		permit, reason := gate.reserve("project", quota, false, false)
		if permit == nil || reason != rejectionNone {
			test.Fatalf("initial reservation refused: %s", reason)
		}
		permit.finish()
	}
	gate.mu.Lock()
	state := gate.projects["project"]
	accrued := state.tokens
	state.last = time.Now().Add(-time.Second)
	gate.mu.Unlock()
	if accrued < 98 {
		test.Fatalf("unexpected accrued credits: %v", accrued)
	}
	lowered := quota
	lowered.RequestsPerSecond = 10
	permit, reason := gate.reserve("project", lowered, false, false)
	if permit == nil || reason != rejectionNone {
		test.Fatalf("rate cut refused an already earned credit: %s", reason)
	}
	permit.finish()
	gate.mu.Lock()
	after := state.tokens
	gate.mu.Unlock()
	if after <= 50 {
		test.Fatalf("rate cut discarded accrued credits: before=%v after=%v", accrued, after)
	}
	if after > accrued {
		test.Fatalf("rate cut invented credits: before=%v after=%v", accrued, after)
	}
	// While the surplus remains above the new rate the bucket must drain by
	// spending credits, not refill itself back up to the old level.
	gate.mu.Lock()
	state.last = time.Now().Add(-time.Second)
	gate.mu.Unlock()
	permit, reason = gate.reserve("project", lowered, false, false)
	if permit == nil || reason != rejectionNone {
		test.Fatalf("surplus credit was not spendable: %s", reason)
	}
	permit.finish()
	gate.mu.Lock()
	drained := state.tokens
	gate.mu.Unlock()
	if drained >= after {
		test.Fatalf("bucket refilled above the lowered rate: after=%v drained=%v", after, drained)
	}
}

func TestQuotaDecreaseLimitsOnlyNewAdmissions(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
	quota := Quotas{MaxDatabases: 8, MaxSessions: 2, MaxConcurrent: 2, RequestsPerSecond: 1000, MaxSQLBytes: 1024, MaxResultBytes: 1024}
	first, reason := gate.reserve("project", quota, true, false)
	if first == nil || reason != rejectionNone {
		test.Fatalf("first admission refused: %s", reason)
	}
	second, reason := gate.reserve("project", quota, true, false)
	if second == nil || reason != rejectionNone {
		test.Fatalf("second admission refused: %s", reason)
	}
	lowered := quota
	lowered.MaxSessions, lowered.MaxConcurrent, lowered.RequestsPerSecond = 1, 1, 10
	if _, reason := gate.reserve("project", lowered, true, false); reason != rejectionConcurrency {
		test.Fatalf("lowered concurrency limit admitted new work: %s", reason)
	}
	first.finish()
	if _, reason := gate.reserve("project", lowered, true, false); reason != rejectionConcurrency {
		test.Fatalf("lowered concurrency limit admitted work while one transaction still ran: %s", reason)
	}
	// The transaction admitted before the cut is not interrupted by it.
	second.finish()
	replacement, reason := gate.reserve("project", lowered, true, false)
	if replacement == nil || reason != rejectionNone {
		test.Fatalf("released seat was not reusable: %s", reason)
	}
	replacement.finish()
	projects, active, unknown := gate.status()
	state := projects["project"].(map[string]any)
	if active != 0 || unknown != 0 || state["sessions"].(int) != 0 || state["active"].(int) != 0 {
		test.Fatalf("quota decrease leaked admission slots: %+v active=%d unknown=%d", state, active, unknown)
	}
}
