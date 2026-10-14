package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

const telemetryLargeCount = "18446744073709551615"

type telemetryRPCRequest struct {
	Version   uint32 `json:"version"`
	RequestID string `json:"request_id"`
	Command   struct {
		Command string `json:"command"`
	} `json:"command"`
}

func telemetryMetricsFixture() map[string]any {
	metrics := map[string]any{}
	for _, operation := range []string{"query", "execute", "checkpoint", "get", "put", "list", "delete", "segment_get", "segment_put", "wal_durability", "native_commit_durability", "cold_open"} {
		metrics[operation] = map[string]any{
			"total": json.Number(telemetryLargeCount), "errors": json.Number("2"), "bytes": json.Number("9007199254740993"),
			"latency_total_ns": json.Number("9007199254740995"), "latency_max_ns": json.Number("5000000001"),
			"latency_buckets": []any{json.Number("1"), json.Number("2"), json.Number("3"), json.Number("4"), json.Number("5"), json.Number("6"), json.Number("7")},
		}
	}
	metrics["latency_bounds_ns"] = []any{json.Number("100000"), json.Number("1000000"), json.Number("10000000"), json.Number("100000000"), json.Number("1000000000"), json.Number("5000000000")}
	metrics["measurement_scopes"] = map[string]any{
		"segment_cache": "RAM and SSD lookups, not pager hit ratio", "segment_io": "logical session RPC, not raw backend IO",
		"wal_durability": "confirmed durable WAL index publication", "native_commit_durability": "validated commit subset",
		"cold_open": "cold load, not reused sessions", "bytes": "successful payload only",
	}
	return metrics
}

func telemetryWorkerFixture() map[string]any {
	return map[string]any{
		"metrics": telemetryMetricsFixture(), "resources": map[string]any{
			"segment_ram_bytes": json.Number("123"), "segment_disk_bytes": json.Number("456"),
			"dirty_segment_bytes": json.Number("789"), "wal_bytes": json.Number("1011"),
			"pending_wal_bytes": json.Number("1213"), "segment_loads": json.Number(telemetryLargeCount),
			"segment_cache_hits_ram": json.Number("14"), "segment_cache_hits_disk": json.Number("15"),
			"segment_cache_misses": json.Number("16"), "segment_cache_evictions": json.Number("17"),
			"segment_cache_lookups": json.Number("45"), "segment_cache_hit_ratio": nil,
		}, "http_inflight": json.Number("4"), "sql_inflight": json.Number("3"), "sessions": json.Number("2"),
	}
}

func telemetryRPCServer(test *testing.T, authority bool, token string, count *atomic.Int64, reply func(http.ResponseWriter, telemetryRPCRequest)) *httptest.Server {
	test.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		count.Add(1)
		path := "/control"
		if authority {
			path = "/"
		}
		var input telemetryRPCRequest
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if request.Method != http.MethodPost || request.URL.Path != path || request.Header.Get("Authorization") != "Bearer "+token || request.Header.Get("Content-Type") != "application/json" || decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Version != 1 || input.RequestID == "" || input.Command.Command != "get_metrics" {
			test.Error("invalid telemetry RPC destination, credentials or envelope")
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		reply(response, input)
	}))
	test.Cleanup(server.Close)
	return server
}

func telemetryReply(response http.ResponseWriter, input telemetryRPCRequest, authority bool, result any) {
	envelope := map[string]any{"version": 1, "request_id": input.RequestID, "ok": true, "result": result, "error": nil}
	if authority {
		envelope["precondition_failed"] = false
	}
	_ = json.NewEncoder(response).Encode(envelope)
}

func telemetryFixture(test *testing.T, authorityURL, workerURL string) *Telemetry {
	test.Helper()
	metadata, err := controller.OpenStore(test.TempDir())
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { metadata.Close() })
	if err := controller.RegisterWorker(metadata, controller.Worker{ID: "worker-a", Address: workerURL, Region: "local", Capacity: 4}); err != nil {
		test.Fatal(err)
	}
	transport, err := controller.NewTransport(authorityURL, strings.Repeat("authority-test-", 3), nil)
	if err != nil {
		test.Fatal(err)
	}
	telemetry, err := NewTelemetry(transport, metadata, map[string]string{"worker-a": strings.Repeat("worker-test-", 3)})
	if err != nil {
		test.Fatal(err)
	}
	return telemetry
}

func TestTelemetryCompleteAggregationPreservesLargeNumbers(test *testing.T) {
	var authorityCalls, workerCalls atomic.Int64
	authority := telemetryRPCServer(test, true, strings.Repeat("authority-test-", 3), &authorityCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		telemetryReply(response, input, true, telemetryMetricsFixture())
	})
	worker := telemetryRPCServer(test, false, strings.Repeat("worker-test-", 3), &workerCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		telemetryReply(response, input, false, telemetryWorkerFixture())
	})
	result := telemetryFixture(test, authority.URL, worker.URL).Collect(context.Background())
	if result["scope"] != "actual_process_metrics_not_cluster_HA" || authorityCalls.Load() != 1 || workerCalls.Load() != 1 {
		test.Fatal("aggregation scope or RPC fanout incorrect")
	}
	if !reflect.DeepEqual(result["authority"], telemetryMetricsFixture()) {
		test.Error("authority metrics incomplete or integer precision lost")
	}
	workers, ok := result["workers"].(map[string]any)
	if !ok || !reflect.DeepEqual(workers["worker-a"], telemetryWorkerFixture()) {
		test.Fatal("worker metrics/resources incomplete or integer precision lost")
	}
}

func TestTelemetryUnavailableWorkerDoesNotInventZeroMetrics(test *testing.T) {
	var authorityCalls, workerCalls atomic.Int64
	authority := telemetryRPCServer(test, true, strings.Repeat("authority-test-", 3), &authorityCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		telemetryReply(response, input, true, map[string]any{"healthy": true})
	})
	worker := telemetryRPCServer(test, false, strings.Repeat("worker-test-", 3), &workerCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		response.WriteHeader(http.StatusServiceUnavailable)
	})
	result := telemetryFixture(test, authority.URL, worker.URL).Collect(context.Background())
	workers := result["workers"].(map[string]any)
	if !reflect.DeepEqual(workers["worker-a"], map[string]string{"status": "unavailable"}) || !reflect.DeepEqual(result["authority"], map[string]any{"healthy": true}) {
		test.Fatal("worker failure masked as zero or corrupted successful authority metrics")
	}
	if authorityCalls.Load() != 1 || workerCalls.Load() != 1 {
		test.Fatal("unexpected retries or skipped collection")
	}
}

func TestTelemetryRejectsInvalidRPCResponses(test *testing.T) {
	for _, target := range []string{"authority", "worker"} {
		for _, fault := range []string{"version", "request_id", "oversize", "oversize_padding", "error", "unknown_field", "trailing_json", "duplicate_field"} {
			test.Run(target+"/"+fault, func(test *testing.T) {
				var authorityCalls, workerCalls atomic.Int64
				respond := func(isAuthority bool) func(http.ResponseWriter, telemetryRPCRequest) {
					return func(response http.ResponseWriter, input telemetryRPCRequest) {
						if isAuthority != (target == "authority") {
							telemetryReply(response, input, isAuthority, map[string]any{"healthy": true})
							return
						}
						envelope := map[string]any{"version": 1, "request_id": input.RequestID, "ok": true, "result": map[string]bool{"healthy": true}, "error": nil}
						if isAuthority {
							envelope["precondition_failed"] = false
						}
						switch fault {
						case "version":
							envelope["version"] = 2
						case "request_id":
							envelope["request_id"] = "not-the-request"
						case "error":
							envelope["error"] = "unexpected failure"
						case "unknown_field":
							envelope["extra"] = true
						case "oversize":
							envelope["result"] = strings.Repeat("x", 32*1024*1024)
						}
						encoded, err := json.Marshal(envelope)
						if err != nil {
							test.Error(err)
							return
						}
						if fault == "oversize_padding" {
							encoded = append(encoded, bytes.Repeat([]byte{' '}, 32*1024*1024-len(encoded)+1)...)
							encoded = append(encoded, []byte(`{"hidden":true}`)...)
						}
						if fault == "trailing_json" {
							encoded = append(encoded, []byte(`{"hidden":true}`)...)
						}
						if fault == "duplicate_field" {
							encoded = append([]byte(`{"version":2,`), encoded[1:]...)
						}
						_, _ = response.Write(encoded)
					}
				}
				authority := telemetryRPCServer(test, true, strings.Repeat("authority-test-", 3), &authorityCalls, respond(true))
				worker := telemetryRPCServer(test, false, strings.Repeat("worker-test-", 3), &workerCalls, respond(false))
				result := telemetryFixture(test, authority.URL, worker.URL).Collect(context.Background())
				invalid := result["authority"]
				valid := result["workers"].(map[string]any)["worker-a"]
				if target == "worker" {
					invalid, valid = valid, invalid
				}
				if !reflect.DeepEqual(invalid, map[string]string{"status": "unavailable"}) || !reflect.DeepEqual(valid, map[string]any{"healthy": true}) {
					test.Fatal("invalid RPC accepted or independent successful peer lost")
				}
			})
		}
	}
}

type telemetryLocalMetrics struct{}

func (telemetryLocalMetrics) Metrics() map[string]any {
	return map[string]any{"active_permits": uint64(7), "errors_total": map[string]uint64{"quota_exceeded": 3}}
}

func TestTelemetryManagementMetricsAggregatesOnlyForGlobalAdmin(test *testing.T) {
	store := securityState(test)
	activeSecurityDatabase(test, store, securityKMS(test))
	bootstrap := strings.Repeat("metrics-bootstrap-test-", 2)
	if err := BootstrapAPIKey(store, bootstrap); err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(store, bytes.Repeat([]byte{31}, 32))
	if err != nil {
		test.Fatal(err)
	}
	credentials := map[string]string{"global_admin": bootstrap, "invalid": "invalid"}
	for _, role := range []string{"admin", "developer", "sql", "viewer"} {
		_, credential, err := NewAPIKey(store, "metrics-"+role, "alpha", role, false)
		if err != nil {
			test.Fatal(err)
		}
		credentials[role] = credential
		if role == "admin" {
			principal, err := identity.Authenticate(credential)
			if err != nil {
				test.Fatal(err)
			}
			credentials["database_token"], err = identity.IssueToken(principal, securityDatabase(), "read_only", time.Now().Add(time.Minute).UnixMilli())
			if err != nil {
				test.Fatal(err)
			}
		}
	}
	var authorityCalls, workerCalls atomic.Int64
	authority := telemetryRPCServer(test, true, strings.Repeat("authority-test-", 3), &authorityCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		telemetryReply(response, input, true, map[string]any{"healthy": true})
	})
	worker := telemetryRPCServer(test, false, strings.Repeat("worker-test-", 3), &workerCalls, func(response http.ResponseWriter, input telemetryRPCRequest) {
		telemetryReply(response, input, false, telemetryWorkerFixture())
	})
	management := &Management{config: ManagementConfig{Identity: identity, Metrics: telemetryLocalMetrics{}, Telemetry: telemetryFixture(test, authority.URL, worker.URL)}}
	for _, name := range []string{"admin", "developer", "sql", "viewer", "database_token", "invalid", "global_admin"} {
		test.Run(name, func(test *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
			request.Header.Set("Authorization", "Bearer "+credentials[name])
			response := httptest.NewRecorder()
			management.ServeHTTP(response, request)
			if name != "global_admin" {
				if response.Code != http.StatusForbidden || authorityCalls.Load() != 0 || workerCalls.Load() != 0 || strings.Contains(response.Body.String(), "native") {
					test.Fatal("metrics permission leak or unauthorized RPC fanout")
				}
				return
			}
			if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || authorityCalls.Load() != 1 || workerCalls.Load() != 1 {
				test.Fatal("global admin metrics status, cache policy or fanout incorrect")
			}
			var envelope map[string]any
			decoder := json.NewDecoder(response.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&envelope); err != nil {
				test.Fatal(err)
			}
			if envelope["ok"] != true || envelope["version"] != json.Number("1") || envelope["error"] != nil {
				test.Fatal("invalid management success envelope")
			}
			result, ok := envelope["result"].(map[string]any)
			if !ok || result["active_permits"] != json.Number("7") || !reflect.DeepEqual(result["errors_total"], map[string]any{"quota_exceeded": json.Number("3")}) {
				test.Fatal("local gateway counters not preserved")
			}
			native, ok := result["native"].(map[string]any)
			if !ok || native["scope"] != "actual_process_metrics_not_cluster_HA" || !reflect.DeepEqual(native["authority"], map[string]any{"healthy": true}) {
				test.Fatal("management did not include native aggregation")
			}
			workers, ok := native["workers"].(map[string]any)
			if !ok || !reflect.DeepEqual(workers["worker-a"], telemetryWorkerFixture()) {
				test.Fatalf("management worker metrics lost: %T", native["workers"])
			}
		})
	}
}
