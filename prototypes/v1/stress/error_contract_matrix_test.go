package stress

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

// TestStressAgreesWithTheFailureContract drives the harness's failure decode with
// the conformance kit: every published code, every evidence shape and every
// status the platform answers with. The harness must reach exactly the answer
// sqlcontract reaches, so a code the platform adds cannot arrive as
// unrecognized_server_code and a proven failure cannot arrive as unknown.
func TestStressAgreesWithTheFailureContract(t *testing.T) {
	vector := &contractVector{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input wireRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		if request.URL.Path == "/v1/sql/open" {
			respond(writer, input.RequestID, map[string]any{"session": strings.Repeat("a", 32), "readonly": false}, "", 200)
			return
		}
		vector.serve(writer, input.RequestID)
	}))
	defer server.Close()
	session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
	for _, current := range sqlcontract.Vectors() {
		vector.set(current)
		failure := current.Failure()
		_, _, observed := session.SQL(context.Background(), true, "SELECT 1")
		if observed == nil {
			t.Fatalf("%s: no harness failure", current.Name)
		}
		if observed.Code != failure.Code() || observed.Unknown != failure.Uncertain() ||
			observed.ExecutionCompleted != failure.Executed() || observed.MayHaveCommitted != failure.MayHaveCommitted() {
			t.Fatalf("%s: harness=%+v want code=%s unknown=%t executed=%t committed=%t",
				current.Name, observed, failure.Code(), failure.Uncertain(), failure.Executed(), failure.MayHaveCommitted())
		}
		// A harness that met an unresolved outcome stops using the session, so
		// every vector gets a fresh one rather than inheriting the last verdict.
		session.poisoned = false
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
