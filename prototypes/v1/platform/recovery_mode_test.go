package platform

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServiceRecoveryModeRefusesUnknownProfiles(t *testing.T) {
	for value, expected := range map[string]bool{"": false, "0": false, "1": true} {
		t.Run("value="+value, func(t *testing.T) {
			t.Setenv("PERI_RECOVERY_ONLY", value)
			recovery, err := serviceRecoveryMode()
			if err != nil || recovery != expected {
				t.Fatalf("recovery=%v err=%v, want %v", recovery, err, expected)
			}
		})
	}
	t.Setenv("PERI_RECOVERY_ONLY", "yes")
	if _, err := serviceRecoveryMode(); err == nil {
		t.Fatal("unrecognized recovery profile was accepted")
	}
}

func TestRecoveryMuxDisablesEverySQLSurface(t *testing.T) {
	management := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(204)
	})
	mux := newPublicMux(nil, management, true)
	for _, path := range []string{"/v1/sql/open", "/v1/sql/execute", "/db/tenant/database/main/v3", "/"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != 503 {
			t.Fatalf("%s status=%d, want 503", path, response.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s body: %v", path, err)
		}
		errorBody, _ := body["error"].(map[string]any)
		if errorBody["code"] != "sql_unavailable" || errorBody["recovery"] != true || errorBody["retry_allowed"] != false {
			t.Fatalf("%s error=%v", path, body["error"])
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/databases/operations", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != 204 {
		t.Fatalf("management status=%d, want 204", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/platform/capabilities", nil)
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	var capabilities map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &capabilities); err != nil {
		t.Fatal(err)
	}
	if capabilities["recovery"] != true || capabilities["sql_enabled"] != false || capabilities["hrana_http"] != false || capabilities["hrana_ws"] != false {
		t.Fatalf("capabilities=%v", capabilities)
	}
}

func TestRecoveryManagementOnlyAdmitsIsolationRecoveryPaths(t *testing.T) {
	management := &Management{config: ManagementConfig{Recovery: true}}
	for _, path := range []string{
		"/v1/reconcile", "/v1/organizations/create", "/v1/projects/create", "/v1/keys/create",
		"/v1/databases/create", "/v1/databases/suspend", "/v1/databases/resume",
		"/v1/databases/operations/clear", "/v1/databases/snapshot", "/v1/tokens",
	} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		if _, err := management.dispatch(request, Principal{}, managementRequest{}); !errors.Is(err, errRecoveryRestricted) {
			t.Fatalf("%s err=%v, want recovery restriction", path, err)
		}
	}
	for _, path := range []string{
		"/v1/databases/operations", "/v1/databases/operations/decision",
		"/v1/databases/operations/retire-local", "/v1/databases/isolation/release",
		"/v1/databases/status",
	} {
		if !recoveryManagementPath(path) {
			t.Fatalf("%s is not admitted in recovery", path)
		}
	}
}
