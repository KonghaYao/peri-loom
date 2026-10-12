package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	controller "peri-loom/controller"
)

type managementErrorIdentity struct {
	Identity
	failure error
}

func (identity managementErrorIdentity) Authorize(Principal, controller.DatabaseID, string) error {
	return identity.failure
}

func TestManagementBusinessErrorReplies(test *testing.T) {
	for _, scenario := range []struct {
		name, path, token, code string
		input                   map[string]any
		status                  int
		setup                   func(*gatewayFixture, *Management)
	}{
		{"role_denied", "/v1/organizations/create", "viewer", "forbidden", map[string]any{"id": "new-org", "name": "New"}, 403, nil},
		{"invalid_input", "/v1/organizations/create", "viewer", "operation_incomplete", map[string]any{"id": "bad id", "name": "New"}, 409, nil},
		{"create_denied", "/v1/databases/create", "viewer", "forbidden", map[string]any{"database": controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "new"}, Branch: "main"}, "project": "project-a"}, 403, nil},
		{"authorize_denied", "/v1/databases/delete", "viewer", "forbidden", nil, 403, nil},
		{"operations_denied", "/v1/databases/operations", "viewer", "forbidden", nil, 403, nil},
		{"database_quota", "/v1/databases/create", "admin", "quota_exceeded", map[string]any{"database": controller.DatabaseID{Namespace: controller.Namespace{Tenant: "tenant-a", Database: "new"}, Branch: "main"}, "project": "project-a"}, 429, func(fixture *gatewayFixture, _ *Management) {
			fixture.state.state.Projects["project-a"] = Project{ID: "project-a", Organization: "org", Tenant: "tenant-a", Quotas: Quotas{MaxDatabases: 1}}
		}},
		{"branch_quota", "/v1/databases/branch", "admin", "quota_exceeded", map[string]any{"name": "other"}, 429, func(fixture *gatewayFixture, _ *Management) {
			fixture.state.state.Projects["project-a"] = Project{ID: "project-a", Organization: "org", Tenant: "tenant-a", Quotas: Quotas{MaxDatabases: 1}}
		}},
		{"text_not_type", "/v1/databases/delete", "admin", "operation_incomplete", nil, 409, func(_ *gatewayFixture, management *Management) {
			management.config.Identity = managementErrorIdentity{Identity: management.config.Identity, failure: errors.New("forbidden: database quota exceeded")}
		}},
		{"native_not_business", "/v1/databases/status", "admin", "operation_incomplete", nil, 409, func(_ *gatewayFixture, management *Management) {
			management.config.Identity = managementErrorIdentity{Identity: management.config.Identity, failure: &nativeToolError{stage: "authorize", cause: errManagementForbidden}}
		}},
		{"unknown_stays_unknown", "/v1/databases/delete", "admin", "outcome_unknown", nil, 502, func(_ *gatewayFixture, management *Management) {
			management.config.Identity = managementErrorIdentity{Identity: management.config.Identity, failure: &SQLRPCError{Ambiguous: true}}
		}},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			management := &Management{config: ManagementConfig{State: fixture.state, Identity: fixture.identity, Audit: fixture.audit}}
			token := fixture.viewer
			if scenario.token == "admin" {
				var err error
				_, token, err = NewAPIKey(fixture.state, "developer", "project-a", "developer", false)
				if err != nil {
					test.Fatal(err)
				}
			}
			if scenario.setup != nil {
				scenario.setup(fixture, management)
			}
			input := map[string]any{"request_id": "business-error", "database": fixture.first}
			for key, value := range scenario.input {
				input[key] = value
			}
			body, err := json.Marshal(input)
			if err != nil {
				test.Fatal(err)
			}
			request := httptest.NewRequest("POST", scenario.path, bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			management.ServeHTTP(response, request)
			var reply struct {
				RequestID string `json:"request_id"`
				Error     struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
				test.Fatal(err)
			}
			if response.Code != scenario.status || reply.Error.Code != scenario.code || reply.RequestID != "business-error" {
				test.Fatal(fmt.Sprintf("got status=%d code=%s request=%s; want %d %s", response.Code, reply.Error.Code, reply.RequestID, scenario.status, scenario.code))
			}
		})
	}
}
