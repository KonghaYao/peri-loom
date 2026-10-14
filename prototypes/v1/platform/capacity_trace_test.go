package platform

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

type capacityTraceTestIdentity struct{ Identity }

func (capacityTraceTestIdentity) Authenticate(credential string) (Principal, error) {
	switch credential {
	case "metrics-key":
		return Principal{Role: "admin", Project: "*"}, nil
	case "project-key":
		return Principal{Role: "admin", Project: "project-a"}, nil
	case "reader-key":
		return Principal{Role: "reader", Project: "*"}, nil
	default:
		return Principal{}, errors.New("unauthorized")
	}
}

type capacityTraceTestMetrics struct{}

func (capacityTraceTestMetrics) Metrics() map[string]any { return map[string]any{} }

func capacityTraceTestFixture(test *testing.T) (*Management, *atomic.Int64, *atomic.Value) {
	test.Helper()
	calls, mode := &atomic.Int64{}, &atomic.Value{}
	mode.Store("normal")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var input struct {
			RequestID string `json:"request_id"`
			Command   struct {
				Command string `json:"command"`
				Stream  string `json:"stream_id"`
				After   uint64 `json:"after_sequence"`
				Maximum uint64 `json:"max_events"`
			} `json:"command"`
		}
		if request.Method != http.MethodPost || json.NewDecoder(request.Body).Decode(&input) != nil || input.Command.Command != "get_capacity_trace" || input.Command.Stream != "opaque-stream-1" || input.Command.After != 7 || input.Command.Maximum != 128 {
			test.Error("bridge dispatched anything other than exact read-only page")
			response.WriteHeader(400)
			return
		}
		token := strings.Repeat("a", 64)
		if request.URL.Path == "/control" {
			token = strings.Repeat("b", 64)
		}
		if request.Header.Get("Authorization") != "Bearer "+token {
			test.Error("wrong private control credential")
		}
		page := map[string]any{"schemaVersion": 1, "enabled": true, "source": map[string]any{"generation": "generation-1"}, "stream": map[string]any{"streamId": "opaque-stream-1", "events": []any{map[string]any{"sequence": 8}}}, "afterSequence": 7, "nextSequence": 8, "hasMore": false, "status": "available"}
		var result any = page
		kind := mode.Load().(string)
		switch kind {
		case "redirect":
			response.Header().Set("Location", serverRedirectTarget())
			response.WriteHeader(302)
			return
		case "oversize":
			response.Write(bytes.Repeat([]byte("x"), capacityTracePageBytes+1))
			return
		case "page_oversize":
			page["padding"] = strings.Repeat("x", capacityTracePageBytes)
		case "wrong_identity":
			input.RequestID = "wrong"
		case "native_error":
			result = nil
		case "token_leak":
			page["credential"] = strings.Repeat("b", 64)
		case "escaped_token_leak":
			result = json.RawMessage(`{"schemaVersion":1,"credential":"` + strings.Repeat(`\u0062`, 64) + `"}`)
		case "wrong_schema":
			page["schemaVersion"] = 2
		case "final_envelope_oversize":
			result = json.RawMessage(`{"schemaVersion":1,"padding":"` + strings.Repeat("<", 14000) + `"}`)
		case "duplicate":
			response.Write([]byte(`{"version":1,"version":1}`))
			return
		}
		envelope := map[string]any{"version": 1, "request_id": input.RequestID, "ok": kind != "native_error", "result": result, "error": nil}
		if kind == "native_error" {
			envelope["error"] = "private SQL session Bearer " + token
		}
		if request.URL.Path == "/" {
			envelope["precondition_failed"] = false
		}
		encoder := json.NewEncoder(response)
		encoder.SetEscapeHTML(false)
		encoder.Encode(envelope)
	}))
	test.Cleanup(server.Close)
	bridge, err := newCapacityTraceBridge(controller.ServiceConfig{AuthorityURL: server.URL, AuthorityToken: strings.Repeat("a", 64), Workers: []controller.ConfiguredWorker{{Worker: controller.Worker{ID: "worker-a", Address: server.URL}, ControlToken: strings.Repeat("b", 64)}}})
	if err != nil {
		test.Fatal(err)
	}
	return &Management{config: ManagementConfig{Identity: capacityTraceTestIdentity{}, Metrics: capacityTraceTestMetrics{}, CapacityTrace: bridge}}, calls, mode
}

func serverRedirectTarget() string { return "http://127.0.0.1:1/private" }

func capacityTraceTestGet(management *Management, token, query string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/v1/capacity-trace?"+query, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	management.ServeHTTP(response, request)
	return response
}

func TestCapacityTraceStrictQueryAndMetricsAuthorization(test *testing.T) {
	management, calls, _ := capacityTraceTestFixture(test)
	valid := "role=worker&worker_id=worker-a&stream_id=opaque-stream-1&after_sequence=7&max_events=128"
	for _, credential := range []string{"", "invalid", "project-key", "reader-key"} {
		if response := capacityTraceTestGet(management, credential, valid); response.Code != 403 {
			test.Fatal("non-metrics credential read process trace")
		}
	}
	for _, query := range []string{
		valid + "&role=worker", valid + "&pid=1", valid + "&max_events=1", strings.Replace(valid, "worker-a", "worker-b", 1), strings.Replace(valid, "role=worker", "role=authority", 1), strings.Replace(valid, "role=worker", "role=sql", 1), strings.Replace(valid, "128", "129", 1), strings.Replace(valid, "128", "0", 1), strings.Replace(valid, "=7", "=-1", 1), strings.Replace(valid, "=7", "=18446744073709551616", 1), strings.Replace(valid, "=7", "=07", 1), strings.Replace(valid, "opaque-stream-1", "bad%2Fstream", 1), strings.Replace(valid, "opaque-stream-1", strings.Repeat("a", 129), 1), "role=authority", "role=authority&stream_id=%GG", "role=authority&stream_id=opaque-stream-1&after_sequence=7",
	} {
		if response := capacityTraceTestGet(management, "metrics-key", query); response.Code != 400 {
			test.Fatalf("invalid query admitted: %s", query)
		}
	}
	if calls.Load() != 0 {
		test.Fatal("invalid or unauthorized query reached Native")
	}
}

func TestCapacityTraceBoundedReadOnlyForwarding(test *testing.T) {
	management, calls, mode := capacityTraceTestFixture(test)
	for _, role := range []string{"authority", "worker"} {
		query := "role=" + role + "&stream_id=opaque-stream-1&after_sequence=7&max_events=128"
		if role == "worker" {
			query += "&worker_id=worker-a"
		}
		response := capacityTraceTestGet(management, "metrics-key", query)
		var envelope struct {
			Version   int            `json:"version"`
			RequestID string         `json:"request_id"`
			OK        bool           `json:"ok"`
			Result    map[string]any `json:"result"`
		}
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || envelope.Version != 1 || envelope.RequestID != "" || !envelope.OK || envelope.Result["nextSequence"] != float64(8) {
			test.Fatal("page or public v1 envelope changed")
		}
		if response.Header().Get("Cache-Control") != "no-store" || response.Body.Len() > capacityTracePageBytes {
			test.Fatal("unbounded/cacheable trace")
		}
	}
	if calls.Load() != 2 {
		test.Fatal("trace replayed or registered other Native commands")
	}
	for _, kind := range []string{"redirect", "oversize", "page_oversize", "wrong_identity", "native_error", "token_leak", "escaped_token_leak", "wrong_schema", "final_envelope_oversize", "duplicate"} {
		mode.Store(kind)
		response := capacityTraceTestGet(management, "metrics-key", "role=worker&worker_id=worker-a&stream_id=opaque-stream-1&after_sequence=7&max_events=128")
		if response.Code != 503 || response.Body.Len() > capacityTracePageBytes || strings.Contains(response.Body.String(), "private") || strings.Contains(response.Body.String(), strings.Repeat("b", 64)) {
			test.Fatalf("unsafe response in %s", kind)
		}
	}
	if calls.Load() != 12 {
		test.Fatal("native read redirected or retried")
	}
}

func TestCapacityTraceCanceledAndSaturatedDoNotReplay(test *testing.T) {
	management, calls, _ := capacityTraceTestFixture(test)
	query, err := parseCapacityTraceQuery("role=authority&stream_id=opaque-stream-1&after_sequence=7&max_events=128")
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := management.config.CapacityTrace.page(ctx, query); err == nil {
		test.Fatal("canceled read accepted")
	}
	for index := 0; index < cap(management.config.CapacityTrace.slots); index++ {
		management.config.CapacityTrace.slots <- struct{}{}
	}
	if _, err := management.config.CapacityTrace.page(context.Background(), query); err == nil {
		test.Fatal("saturated trace read accepted")
	}
	if calls.Load() != 0 {
		test.Fatal("canceled/saturated read dispatched")
	}
}

func TestCapacityTraceUsesProperPublicCA(test *testing.T) {
	management, _, _ := capacityTraceTestFixture(test)
	server := httptest.NewTLSServer(management)
	defer server.Close()
	client := server.Client()
	transport := client.Transport.(*http.Transport)
	transport.TLSClientConfig.InsecureSkipVerify = false
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	transport.TLSClientConfig.RootCAs = pool
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/capacity-trace?role=authority&stream_id=opaque-stream-1&after_sequence=7&max_events=128", nil)
	request.Header.Set("Authorization", "Bearer metrics-key")
	response, err := client.Do(request)
	if err != nil {
		test.Fatal(err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || !bytes.Contains(encoded, []byte(`"schemaVersion":1`)) {
		test.Fatal("CA verified public page unavailable")
	}
	var result map[string]any
	if json.Unmarshal(encoded, &result) != nil || !reflect.DeepEqual(result["request_id"], "") {
		test.Fatal("GET correlation contract changed")
	}
}

func TestCapacityTraceRejectsUntrustedConfiguredTargets(test *testing.T) {
	for _, address := range []string{"http://example.com:123", "https://127.0.0.1:123", "http://user:secret@127.0.0.1:123", "http://127.0.0.1:123/path", "http://127.0.0.1:123?secret=1", "http://127.0.0.1:123#fragment", "http://127.0.0.1"} {
		if _, err := newCapacityTraceBridge(controller.ServiceConfig{AuthorityURL: address, AuthorityToken: strings.Repeat("a", 64)}); err == nil {
			test.Fatal("untrusted target accepted")
		}
	}
}
