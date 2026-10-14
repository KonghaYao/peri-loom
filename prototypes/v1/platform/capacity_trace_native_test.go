package platform

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestCapacityTraceNativeReadOnlyPages(test *testing.T) {
	proxy := &unknownResponseLossProxy{counts: map[string]int{}}
	fixture := p6VerifyNewRouted(test, Quotas{}, func(worker, target string) string { return proxy.route(test, worker, target) })
	for _, role := range []string{"authority", "worker"} {
		query := "role=" + role + "&stream_id=opaque-stream-1&after_sequence=0&max_events=128"
		if role == "worker" {
			query += "&worker_id=worker-a"
		}
		for _, authorized := range []bool{false, true} {
			request, err := http.NewRequest(http.MethodGet, fixture.endpoint+"/v1/capacity-trace?"+query, nil)
			if err != nil {
				test.Fatal(err)
			}
			credential := fixture.credential
			if authorized {
				credential = fixture.config.BootstrapKey
			}
			request.Header.Set("Authorization", "Bearer "+credential)
			response, err := fixture.client.Do(request)
			if err != nil {
				test.Fatal(err)
			}
			encoded, err := io.ReadAll(io.LimitReader(response.Body, capacityTracePageBytes+1))
			response.Body.Close()
			if err != nil || len(encoded) > capacityTracePageBytes {
				test.Fatal("unbounded actual CLI capacity page")
			}
			if !authorized {
				if response.StatusCode != 403 {
					test.Fatal("project key read Native trace")
				}
				continue
			}
			var envelope struct {
				Version int  `json:"version"`
				OK      bool `json:"ok"`
				Result  struct {
					SchemaVersion int    `json:"schemaVersion"`
					Status        string `json:"status"`
				} `json:"result"`
			}
			if response.StatusCode != 200 || json.Unmarshal(encoded, &envelope) != nil || envelope.Version != 1 || !envelope.OK || envelope.Result.SchemaVersion != 1 || envelope.Result.Status != "disabled" {
				test.Fatalf("actual Native trace page unavailable: role=%s status=%d", role, response.StatusCode)
			}
			for _, secret := range fixture.secrets {
				if bytes.Contains(encoded, []byte(secret)) {
					test.Fatal("private Native credential leaked")
				}
			}
		}
	}
	status, read := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "trace_no_operations", map[string]any{"database": fixture.database})
	if status != 200 || !bytes.Contains(read["result"], []byte(`"operations":[]`)) {
		test.Fatal("capacity read registered a native operation")
	}
	proxy.mutex.Lock()
	defer proxy.mutex.Unlock()
	if len(proxy.counts) != 0 {
		test.Fatal("trace read dispatched SQL/Open/Close")
	}
	test.Log("CAPACITY_TRACE_NATIVE authority_page=1 worker_page=1 default_disabled=1 metrics_key_only=1 native_sql_calls=0 operation_records=0 bounded_response=1")
}
