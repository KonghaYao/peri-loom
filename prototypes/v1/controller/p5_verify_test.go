package controller

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestP5VerifyActualWorkerIdentityCannotFollowRegistryEndpointSwap(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	var actual struct {
		WorkerID  string          `json:"worker_id"`
		Databases json.RawMessage `json:"databases"`
		Resources json.RawMessage `json:"resources"`
	}
	if err := cluster.transport.Worker(ctx, cluster.workers["verify-a"].worker, "actual-worker-a", map[string]string{"command": "get_worker_state"}, &actual); err != nil {
		test.Fatal(err)
	}
	if actual.WorkerID != "verify-a" {
		test.Fatalf("actual worker identity mismatch: %s", actual.WorkerID)
	}
	advertised := cluster.workers["verify-a"].worker
	advertised.Address = cluster.workers["verify-b"].worker.Address
	err := cluster.transport.Worker(ctx, advertised, "swapped-worker-endpoint", map[string]string{"command": "get_worker_state"}, &actual)
	var failure *RPCError
	if !errors.As(err, &failure) || !failure.Rejected || failure.StatusCode != 403 || failure.Ambiguous {
		test.Fatalf("swapped endpoint did not reject wrong worker principal: %v", err)
	}
	for _, token := range cluster.tokens {
		if strings.Contains(err.Error(), token) {
			test.Fatal("transport error exposed fixture credential")
		}
	}
	test.Log("actual worker principal rejected registry endpoint swap with HTTP403; credentials absent from error")
}
