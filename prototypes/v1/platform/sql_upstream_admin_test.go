package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

func TestSQLUpstreamPrivateCheckpointCompactWire(test *testing.T) {
	for _, operation := range []string{"checkpoint", "compact"} {
		test.Run(operation, func(test *testing.T) {
			token := sqlTestToken(test)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				var envelope struct {
					Version   uint32            `json:"version"`
					RequestID string            `json:"request_id"`
					Command   map[string]string `json:"command"`
				}
				decoder := json.NewDecoder(request.Body)
				decoder.DisallowUnknownFields()
				if decoder.Decode(&envelope) != nil || envelope.Version != 1 || envelope.RequestID != "admin-1" || len(envelope.Command) != 2 || envelope.Command["command"] != operation || envelope.Command["session"] != strings.Repeat("a", 64) || request.Method != "POST" || request.URL.Path != "/sql" || request.Header.Get("Authorization") != "Bearer "+token {
					test.Error("private maintenance request changed command, credential, or envelope")
				}
				field := "checkpointed"
				if operation == "compact" {
					field = "compacted"
				}
				_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]bool{field: true}))
			}))
			defer server.Close()
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": token})
			if err != nil {
				test.Fatal(err)
			}
			var reply map[string]bool
			err = upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "admin-1", map[string]any{"command": operation, "session": strings.Repeat("a", 64)}, &reply)
			if err != nil || len(reply) != 1 || calls.Load() != 1 || !(reply["checkpointed"] || reply["compacted"]) {
				test.Fatal("private maintenance command did not dispatch exactly once")
			}
		})
	}
}

func TestSQLUpstreamPrivateMaintenanceStrictSessionOnly(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	for _, operation := range []string{"checkpoint", "compact", "diagnostics"} {
		for _, fields := range []map[string]any{
			{"command": operation},
			{"command": operation, "session": nil},
			{"command": operation, "session": 123},
			{"command": operation, "session": ""},
			{"command": operation, "session": strings.Repeat("a", 64), "sql": "COMMIT"},
			{"command": operation, "session": strings.Repeat("a", 64), "database": "arbitrary"},
		} {
			failure := sqlTestFailure(test, upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "invalid-admin", fields, nil))
			if failure.Ambiguous || failure.evidence.Executed() {
				test.Fatal("invalid private command was not rejected before dispatch")
			}
		}
	}
	if calls.Load() != 0 {
		test.Fatal("malformed private maintenance reached worker")
	}
}

func TestGatewayPublicMaintenanceCommandsDenied(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	for _, scenario := range []struct {
		path string
		body map[string]any
	}{
		{"/v1/sql/checkpoint", map[string]any{"session": session}},
		{"/v1/sql/compact", map[string]any{"session": session}},
		{"/v1/sql/diagnostics", map[string]any{"session": session}},
		{"/v1/sql/execute", map[string]any{"session": session, "sql": "SELECT 1", "command": "checkpoint"}},
		{"/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "command": "compact"}},
		{"/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "command": "diagnostics"}},
		{"/v1/sql/execute", map[string]any{"session": session, "sql": "SELECT 1", "command": "ownership"}},
	} {
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, scenario.path, scenario.body)
		if response.Code != 400 || gatewayCode(test, response) != "invalid_request" {
			test.Fatal("public SQL gateway accepted private maintenance capability")
		}
	}
	for _, operation := range []string{"checkpoint", "compact", "diagnostics", "execute", "query", "ownership"} {
		if fixture.upstream.count(operation) != 0 {
			test.Fatal("public private-command attempt reached worker")
		}
	}
	if fixture.gateway.Metrics()["active_permits"] != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
		test.Fatal("public command rejection leaked quota")
	}
}

func TestSQLUpstreamPrivateDiagnosticsWire(test *testing.T) {
	token := sqlTestToken(test)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var envelope struct {
			Version   uint32            `json:"version"`
			RequestID string            `json:"request_id"`
			Command   map[string]string `json:"command"`
		}
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&envelope) != nil || envelope.Version != 1 || envelope.RequestID != "diagnostics-1" || len(envelope.Command) != 2 || envelope.Command["command"] != "diagnostics" || envelope.Command["session"] != strings.Repeat("a", 64) || request.Method != "POST" || request.URL.Path != "/sql" || request.Header.Get("Authorization") != "Bearer "+token {
			test.Error("private diagnostics wire changed capability, scope, or envelope")
		}
		_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"generation": 7, "manifest_hash": "fixture-manifest", "segment_count": 3, "snapshot_consistency": "diagnostic_only_not_authority"}))
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": token})
	if err != nil {
		test.Fatal(err)
	}
	var reply struct {
		Generation          uint64 `json:"generation"`
		ManifestHash        string `json:"manifest_hash"`
		SegmentCount        uint64 `json:"segment_count"`
		SnapshotConsistency string `json:"snapshot_consistency"`
	}
	err = upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "diagnostics-1", map[string]any{"command": "diagnostics", "session": strings.Repeat("a", 64)}, &reply)
	if err != nil || calls.Load() != 1 || reply.Generation != 7 || reply.ManifestHash != "fixture-manifest" || reply.SegmentCount != 3 || reply.SnapshotConsistency != "diagnostic_only_not_authority" {
		test.Fatal("private diagnostics not dispatched once or response modified")
	}
}
