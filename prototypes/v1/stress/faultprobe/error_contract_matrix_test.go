package faultprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"peri-loom/platform/sqlcontract"
)

const contractCredential = "synthetic-probe-credential________________"

// TestFaultprobeAgreesWithTheFailureContract drives the probe's failure decode
// with the conformance kit: every published code, every evidence shape and every
// status the platform answers with. The probe must reach exactly the answer
// sqlcontract reaches, so a code the platform adds cannot arrive as
// unrecognized_error and a proven failure cannot arrive as unknown.
func TestFaultprobeAgreesWithTheFailureContract(t *testing.T) {
	vector := &contractVector{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input map[string]any
		_ = json.NewDecoder(request.Body).Decode(&input)
		identifier, _ := input["request_id"].(string)
		if strings.HasSuffix(request.URL.Path, "/open") {
			_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": true, "result": map[string]any{"session": strings.Repeat("a", 32), "readonly": false}, "error": nil})
			return
		}
		vector.serve(writer, identifier)
	}))
	defer server.Close()
	for _, current := range sqlcontract.Vectors() {
		vector.set(current)
		// The probe bounds how many requests one client may send, so every vector
		// gets its own rather than inheriting the previous budget.
		client := &Client{endpoint: server.URL, key: contractCredential, prefix: "contract", http: server.Client()}
		failure := current.Failure()
		_, observed := client.call(context.Background(), "/v1/sql/query", map[string]any{"session": strings.Repeat("a", 32), "sql": "SELECT 1"})
		if observed == nil {
			t.Fatalf("%s: no probe failure", current.Name)
		}
		if observed.Code != failure.Code() || observed.Unknown != failure.Uncertain() ||
			observed.ExecutionCompleted != failure.Executed() || observed.MayHaveCommitted != failure.MayHaveCommitted() {
			t.Fatalf("%s: probe=%+v want code=%s unknown=%t executed=%t committed=%t",
				current.Name, observed, failure.Code(), failure.Uncertain(), failure.Executed(), failure.MayHaveCommitted())
		}
	}
}

type contractVector struct {
	mutex  sync.Mutex
	body   []byte
	status int
}

func (vector *contractVector) set(current sqlcontract.Vector) {
	vector.mutex.Lock()
	defer vector.mutex.Unlock()
	vector.body, vector.status = current.Body(), current.Status
}

func (vector *contractVector) serve(writer http.ResponseWriter, requestID string) {
	vector.mutex.Lock()
	defer vector.mutex.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(vector.status)
	_, _ = writer.Write(bytes.Replace(vector.body, []byte(`"request_id":"vector"`), []byte(`"request_id":"`+requestID+`"`), 1))
}
