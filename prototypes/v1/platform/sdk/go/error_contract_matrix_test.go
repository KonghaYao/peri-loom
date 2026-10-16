package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"peri-loom/platform/sqlcontract"
)

// contractServer serves the conformance vector the matrix is currently on.
type contractServer struct {
	mutex  sync.Mutex
	body   []byte
	status int
}

func (server *contractServer) set(vector sqlcontract.Vector) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.body, server.status = vector.Body(), vector.Status
}

func (server *contractServer) serve(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		RequestID string `json:"request_id"`
	}
	_ = json.NewDecoder(request.Body).Decode(&body)
	if request.URL.Path == "/v1/sql/open" {
		writeReply(writer, body.RequestID, map[string]any{"session": testSession, "readonly": false})
		return
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	// The conformance body carries a fixed request id; every consumer checks the
	// correlation of the id it sent, so the vector is re-bound to this request.
	encoded := bytes.Replace(server.body, []byte(`"request_id":"vector"`), []byte(`"request_id":"`+body.RequestID+`"`), 1)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(server.status)
	_, _ = writer.Write(encoded)
}

// TestSDKAgreesWithTheFailureContract drives the SDK with the conformance kit:
// every published code, every evidence shape and every status the platform
// answers with. The SDK must reach exactly the answer sqlcontract reaches, so a
// code added to the platform cannot arrive as a definite failure and a failure
// the platform proved cannot arrive as an unknown outcome.
func TestSDKAgreesWithTheFailureContract(t *testing.T) {
	vector := &contractServer{}
	server := httptest.NewServer(http.HandlerFunc(vector.serve))
	defer server.Close()
	client, err := NewClient(server.URL, testCredential, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range sqlcontract.Vectors() {
		vector.set(current)
		// A session that met an unknown outcome is unusable by contract, so the
		// matrix opens the session the vector belongs to instead of reusing one.
		session, err := client.Open(context.Background(), testDatabase())
		if err != nil {
			t.Fatal(err)
		}
		failure := current.Failure()
		unknown := failure.Unknown(current.Status)
		wantCode := failure.Code()
		if unknown {
			wantCode = "outcome_unknown"
		}
		_, queryErr := session.Query(context.Background(), "SELECT 1", "contract")
		var observed *Error
		if !errors.As(queryErr, &observed) {
			t.Fatalf("%s: no SDK failure: %v", current.Name, queryErr)
		}
		if observed.Code != wantCode || observed.OutcomeUnknown != unknown ||
			observed.ExecutionCompleted != failure.Executed() || observed.MayHaveCommitted != failure.MayHaveCommitted() {
			t.Fatalf("%s: sdk code=%s unknown=%t executed=%t committed=%t; want code=%s unknown=%t executed=%t committed=%t",
				current.Name, observed.Code, observed.OutcomeUnknown, observed.ExecutionCompleted, observed.MayHaveCommitted,
				wantCode, unknown, failure.Executed(), failure.MayHaveCommitted())
		}
	}
}
