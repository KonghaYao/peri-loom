package controller

import (
	"errors"
	"strings"
	"testing"
)

func TestP5VerifyActualWorkerIdentityCannotFollowRegistryEndpointSwap(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	// The identity check reads the worker report through the same strict model the
	// reconcile loop uses, so a worker field this build cannot verify fails here
	// as well: a second, private shape would let the test pass while every real
	// heartbeat was rejected.
	var actual workerReport
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
