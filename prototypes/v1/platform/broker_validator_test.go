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

func TestLeaseValidatorRealHTTPUsesWorkerAuthorityCredentialAndExactBinding(test *testing.T) {
	workerToken := strings.Repeat("worker-authority-test-", 3)
	controlToken := strings.Repeat("control-test-token-", 3)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bearer "+workerToken {
			writer.WriteHeader(403)
			return
		}
		var envelope struct {
			Version   uint32 `json:"version"`
			RequestID string `json:"request_id"`
			Command   struct {
				Command string             `json:"command"`
				Binding controller.Binding `json:"binding"`
				Role    string             `json:"role"`
			} `json:"command"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil || envelope.Command.Command != "validate_lease" || envelope.Command.Binding != securityBinding() {
			test.Error("validator altered binding/command")
		}
		grant := controller.Grant{Binding: envelope.Command.Binding, RemainingTTLMS: 1000}
		grant.Binding.RenewalSequence = 2
		json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": envelope.RequestID, "ok": true, "result": grant, "error": nil, "precondition_failed": false})
	}))
	defer server.Close()
	validator, err := NewLeaseValidator(server.URL, map[string]string{"worker-a": workerToken})
	if err != nil {
		test.Fatal(err)
	}
	grant, err := validator.Validate(context.Background(), securityBinding(), "writer")
	if err != nil || grant.Binding.RenewalSequence != 2 {
		test.Fatalf("real validation: %v", err)
	}
	forged := securityBinding()
	forged.WorkerID = "worker-b"
	if _, err := validator.Validate(context.Background(), forged, "writer"); err == nil || calls.Load() != 1 {
		test.Fatal("worker mismatch forwarded")
	}
	control, err := NewLeaseValidator(server.URL, map[string]string{"worker-a": controlToken})
	if err != nil {
		test.Fatal(err)
	}
	if _, err := control.Validate(context.Background(), securityBinding(), "writer"); err == nil {
		test.Fatal("control principal treated as worker validator")
	}
	if _, err := NewLeaseValidator(server.URL, nil); err == nil {
		test.Fatal("missing worker authority credentials")
	}
	store := securityState(test)
	kms := securityKMS(test)
	if _, err := NewKeyBroker(store, kms, validator, map[string]string{"worker-a": workerToken}); err == nil {
		test.Fatal("authority credential reused for key broker")
	}
}
