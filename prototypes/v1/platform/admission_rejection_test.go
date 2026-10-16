package platform

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// The refusal reasons are counter labels and log fields, not reply fields: the
// published failure body carries exactly its published keys, because
// platform/web/rpc.js answers a failure object with any other key as an unknown
// outcome and poisons the session. These tests pin both halves of that decision —
// the reason is recorded at the layer that refused, and the reply is unchanged.

func refusalReasonQuota() Quotas {
	return Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 4096, MaxResultBytes: 4096}
}

func gatewayMetricCount(test *testing.T, gateway *Gateway, key string) uint64 {
	test.Helper()
	value, ok := gateway.Metrics()[key].(uint64)
	if !ok {
		test.Fatalf("metric %s is not a count", key)
	}
	return value
}

func gatewayMetricCounts(test *testing.T, gateway *Gateway, key string) map[string]uint64 {
	test.Helper()
	value, ok := gateway.Metrics()[key].(map[string]uint64)
	if !ok {
		test.Fatalf("metric %s is not a count map", key)
	}
	return value
}

// assertRefusedReason checks the whole account of one refusal: the layer is named,
// the refusal is not counted as success, and no other layer is blamed.
func assertRefusedReason(test *testing.T, gateway *Gateway, reason rejectionReason) {
	test.Helper()
	counts := gatewayMetricCounts(test, gateway, "rejection_reasons_total")
	if counts[string(reason)] != 1 || len(counts) != 1 {
		test.Fatalf("refusal account: %v, want exactly one %s", counts, reason)
	}
	if gatewayMetricCount(test, gateway, "rejected_requests_total") != 1 {
		test.Fatalf("rejected_requests_total did not count the refusal")
	}
}

// TestAdmissionRefusalReasonsPerLane pins every admission lane to its own reason.
// A lane that reports the wrong reason is worse than a lane that reports none: it
// sends an operator tuning the wrong quota.
func TestAdmissionRefusalReasonsPerLane(test *testing.T) {
	// The holder stays live across the second attempt: seats, lanes and the close
	// lane are only occupied while a permit is unfinished, and a test that finishes
	// the holder first proves nothing about a full lane.
	admitted := func(test *testing.T, permit *admissionPermit, reason rejectionReason) {
		test.Helper()
		if reason != rejectionNone || permit == nil {
			test.Fatalf("lane refused an available permit: %q", reason)
		}
	}
	test.Run("session limit", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
		quota := refusalReasonQuota()
		quota.MaxSessions = 1
		host, reason := gate.reserve("project", quota, true, false)
		admitted(test, host, reason)
		defer host.finish()
		if _, reason := gate.reserve("project", quota, true, false); reason != rejectionSessionLimit {
			test.Fatalf("session seats: %q", reason)
		}
	})
	test.Run("concurrency limit", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
		quota := refusalReasonQuota()
		quota.MaxConcurrent = 1
		lane, reason := gate.reserve("project", quota, false, false)
		admitted(test, lane, reason)
		defer lane.finish()
		if _, reason := gate.reserve("project", quota, false, false); reason != rejectionConcurrency {
			test.Fatalf("active lanes: %q", reason)
		}
	})
	test.Run("writer reserved lane", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
		quota := refusalReasonQuota()
		quota.MaxConcurrent = 2
		lane, reason := gate.reserve("project", quota, false, false)
		admitted(test, lane, reason)
		defer lane.finish()
		if _, reason := gate.reserve("project", quota, false, false); reason != rejectionConcurrency {
			test.Fatalf("reserved writer lane: %q", reason)
		}
	})
	test.Run("writer busy", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
		quota := refusalReasonQuota()
		owner, reason := gate.reserveWriter("project", quota, true)
		admitted(test, owner, reason)
		defer owner.finish()
		if _, reason := gate.reserveWriter("project", quota, true); reason != rejectionWriterBusy {
			test.Fatalf("writer lane serialization: %q", reason)
		}
	})
	test.Run("rate limit", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
		quota := refusalReasonQuota()
		quota.RequestsPerSecond = 1
		credit, reason := gate.reserve("project", quota, false, false)
		admitted(test, credit, reason)
		// Finishing refunds the lane but never the credit: the burst is spent.
		credit.finish()
		if _, reason := gate.reserve("project", quota, false, false); reason != rejectionRate {
			test.Fatalf("token bucket: %q", reason)
		}
	})
	test.Run("classification limit", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
		quota := refusalReasonQuota()
		for count := 0; count < min(quota.MaxConcurrent, 128)+2; count++ {
			release, reason := gate.classify("project", quota)
			if reason != rejectionNone || release == nil {
				test.Fatalf("classification slot refused below the bound: %q", reason)
			}
		}
		if _, reason := gate.classify("project", quota); reason != rejectionClassifyLimit {
			test.Fatalf("classification bound: %q", reason)
		}
	})
	test.Run("close lane", func(test *testing.T) {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
		quota := refusalReasonQuota()
		closePermit, reason := gate.reserve("project", quota, false, true)
		admitted(test, closePermit, reason)
		defer closePermit.finish()
		if _, reason := gate.reserve("project", quota, false, true); reason != rejectionCloseLane {
			test.Fatalf("session-close lane: %q", reason)
		}
	})
}

// TestGatewayRefusalReasonsAtTheRefusalSite drives each layer through the gateway
// and checks the reason the counters report. The response codes are asserted with
// the values the platform published before this account existed: the finer reason
// is additive evidence and must not move the code a client decodes.
func TestGatewayRefusalReasonsAtTheRefusalSite(test *testing.T) {
	test.Run("session limit", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSessions = 1 })
		_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
		if response.Code != 429 || gatewayCode(test, response) != "quota_exceeded" || fixture.upstream.count("open_session") != 1 {
			test.Fatalf("session refusal: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionSessionLimit)
	})
	test.Run("concurrency limit", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxConcurrent = 1 })
		state, err := fixture.state.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		held, reason := fixture.gateway.admission.reserve("project-a", state.Projects["project-a"].Quotas, false, false)
		if reason != rejectionNone {
			test.Fatalf("lane unavailable: %q", reason)
		}
		defer held.finish()
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
		if response.Code != 429 || gatewayCode(test, response) != "quota_exceeded" || fixture.upstream.count("query") != 0 {
			test.Fatalf("concurrency refusal: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionConcurrency)
	})
	test.Run("rate limit", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.RequestsPerSecond = 1 })
		fixture.gateway.admission.mu.Lock()
		bucket := fixture.gateway.admission.projects["project-a"]
		bucket.tokens, bucket.last = 0, time.Now()
		fixture.gateway.admission.mu.Unlock()
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
		if response.Code != 429 || gatewayCode(test, response) != "quota_exceeded" || fixture.upstream.count("query") != 0 {
			test.Fatalf("rate refusal: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionRate)
	})
	test.Run("statement bytes", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSQLBytes = 8 })
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": strings.Repeat("x", 9)})
		if response.Code != 413 || gatewayCode(test, response) != "sql_limit" || fixture.upstream.count("query") != 0 {
			test.Fatalf("statement budget refusal: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionSQLBytes)
	})
	test.Run("memory budget", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 2 })
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
		if response.Code != 413 || gatewayCode(test, response) != "result_limit" || fixture.upstream.count("query") != 1 {
			test.Fatalf("result budget refusal: %d %s", response.Code, response.Body.String())
		}
		// The statement ran and settled; what the platform refused is the result. The
		// account says so instead of implying the work never happened.
		if !strings.Contains(response.Body.String(), `"execution_completed":true`) {
			test.Fatal("result budget refusal hid the execution it observed")
		}
		assertRefusedReason(test, fixture.gateway, rejectionMemoryBudget)
	})
	test.Run("quarantine", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
			if command["command"] == "execute" {
				return nil, errors.New("private unknown response")
			}
			return nil, nil
		})
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "COMMIT"})
		if gatewayCode(test, response) != "outcome_unknown" {
			test.Fatalf("unknown outcome not reached: %s", response.Body.String())
		}
		response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
		if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
			test.Fatalf("quarantine refusal: %d %s", response.Code, response.Body.String())
		}
		counts := gatewayMetricCounts(test, fixture.gateway, "rejection_reasons_total")
		if counts[string(rejectionQuarantine)] != 1 || counts[string(rejectionMemoryBudget)] != 0 {
			test.Fatalf("quarantine account: %v", counts)
		}
		// The 502 that isolated the namespace is an unknown outcome, not a refusal, and
		// must not be filed as one: the platform does not know the statement failed.
		if gatewayMetricCount(test, fixture.gateway, "successful_requests_total") != 1 {
			test.Fatal("the open before the isolation stopped being the only success")
		}
	})
	test.Run("deadline before execution", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
			if command["command"] == "execute" {
				return nil, testRPCError(false, true, SQLRPCError{workerFailure: "SQL execution not entered: writer admission deadline: query duration budget exceeded"})
			}
			return nil, nil
		})
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "UPDATE t SET a = 1"})
		if response.Code != 504 || gatewayCode(test, response) != "sql_timeout" {
			test.Fatalf("proven deadline refusal: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionDeadline)
	})
	test.Run("deadline after execution is not a refusal", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
			if command["command"] == "execute" {
				return nil, testRPCError(true, false, SQLRPCError{workerFailure: "SQL execution settled: writer admission deadline: query duration budget exceeded"})
			}
			return nil, nil
		})
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "UPDATE t SET a = 1"})
		if response.Code != 504 || gatewayCode(test, response) != "sql_timeout" {
			test.Fatalf("settled deadline failure: %d %s", response.Code, response.Body.String())
		}
		// A deadline after execution may have committed. Counting it as a refusal would
		// tell an operator work was turned away when the platform cannot know that.
		if counts := gatewayMetricCounts(test, fixture.gateway, "rejection_reasons_total"); len(counts) != 0 {
			test.Fatalf("settled deadline became a refusal: %v", counts)
		}
		if gatewayMetricCount(test, fixture.gateway, "rejected_requests_total") != 0 {
			test.Fatal("settled deadline entered the refusal account")
		}
	})
	test.Run("writer busy proved by the worker", func(test *testing.T) {
		fixture := newGatewayFixture(test)
		session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
		fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
			if command["command"] == "execute" {
				return nil, testRPCError(false, true, SQLRPCError{workerFailure: "SQL execution not entered: writer admission conflict"})
			}
			return nil, nil
		})
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "UPDATE t SET a = 1"})
		if response.Code != 409 || gatewayCode(test, response) != "sql_conflict" {
			test.Fatalf("worker writer conflict: %d %s", response.Code, response.Body.String())
		}
		assertRefusedReason(test, fixture.gateway, rejectionWriterBusy)
	})
}

// TestOperationCapacityRefusalIsNamed pins the durable side of admission: the
// operation log is bounded by count and its append queue by depth, and both
// refusals keep the code they published while naming the bound that was reached.
func TestOperationCapacityRefusalIsNamed(test *testing.T) {
	result := operationAdmissionFailure(ErrOperationCapacity)
	if result.status != 503 || result.code != "operation_registration_unavailable" || result.rejection != rejectionOperationCapacity {
		test.Fatalf("operation capacity refusal: %+v", result)
	}
	queue := operationAdmissionFailure(ErrOperationQueueFull)
	if queue.status != 503 || queue.code != "operation_registration_unavailable" || queue.rejection != rejectionOperationQueueFull {
		test.Fatalf("operation queue refusal: %+v", queue)
	}
	if isolated := operationAdmissionFailure(ErrDatabaseQuarantined); isolated.rejection != rejectionQuarantine || isolated.code != "database_isolated" {
		test.Fatalf("quarantine refusal: %+v", isolated)
	}
	replay := operationAdmissionFailure(ErrOperationExists)
	if replay.code != "operation_recorded" || replay.rejection != rejectionNone {
		test.Fatalf("replay refusal gained a layer it does not have: %+v", replay)
	}
}

// TestRefusedRequestStaysOutOfSuccessfulLatency is the account the research
// document asks for: a refusal is answered cheaply and must never be able to lift
// the latency of served work. The refused request here is deliberately slow — a
// result budget refusal observed after a 30 ms worker call — so the two accounts
// are separated by three orders of magnitude rather than by luck.
func TestRefusedRequestStaysOutOfSuccessfulLatency(test *testing.T) {
	fixture := newGatewayFixture(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "query" {
			time.Sleep(30 * time.Millisecond)
			return [][]any{{strings.Repeat("x", 4096)}}, nil
		}
		return nil, nil
	})
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxResultBytes = 64 })
	refused := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if refused.Code != 413 || gatewayCode(test, refused) != "result_limit" {
		test.Fatalf("slow refusal not reached: %d %s", refused.Code, refused.Body.String())
	}
	served := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": session, "sql": "UPDATE t SET a = 1"})
	if served.Code != 200 {
		test.Fatalf("served request failed: %s", served.Body.String())
	}
	all := gatewayMetricCounts(test, fixture.gateway, "action_latency_ns_total")
	goodput := gatewayMetricCounts(test, fixture.gateway, "successful_latency_ns_total")
	if all["query"] < uint64(25*time.Millisecond) {
		test.Fatalf("the refused query was not the slow one: %d ns", all["query"])
	}
	if goodput["query"] != 0 {
		test.Fatalf("a refused request entered the successful latency account: %d ns", goodput["query"])
	}
	if goodput["execute"] == 0 || all["execute"] == 0 {
		test.Fatal("the served request is missing from the latency accounts")
	}
	// Three answered requests: the open, the refused query, and the served execute.
	// The refused query was admitted — it held a lane and reached the worker — and is
	// counted as admitted and rejected, which is exactly what a refusal after
	// admission is.
	if gatewayMetricCount(test, fixture.gateway, "admitted_requests_total") != 3 {
		test.Fatal("admission permit account lost a request that held a permit")
	}
	if gatewayMetricCount(test, fixture.gateway, "successful_requests_total") != 2 {
		test.Fatal("success account did not separate the refusal")
	}
	if gatewayMetricCount(test, fixture.gateway, "rejected_requests_total") != 1 {
		test.Fatal("refusal account did not count the result budget refusal")
	}
}

// TestRejectionReasonVocabularyIsClosed keeps the vocabulary a vocabulary: the
// values are bounded and stable, they use the character set a published failure
// code uses, and the export publishes exactly the closed list.
func TestRejectionReasonVocabularyIsClosed(test *testing.T) {
	fixture := newGatewayFixture(test)
	pattern := regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	seen := map[string]bool{}
	for _, reason := range rejectionReasons {
		if reason == rejectionNone || !pattern.MatchString(string(reason)) {
			test.Fatalf("reason outside the published label contract: %q", reason)
		}
		if seen[string(reason)] {
			test.Fatalf("duplicate reason: %q", reason)
		}
		seen[string(reason)] = true
	}
	if len(rejectionReasons) < 7 {
		test.Fatalf("the documented layers are not all named: %v", rejectionReasons)
	}
	published, ok := fixture.gateway.Metrics()["rejection_reason_values"].([]string)
	if !ok || len(published) != len(rejectionReasons) {
		test.Fatalf("the export does not publish the closed vocabulary: %#v", fixture.gateway.Metrics()["rejection_reason_values"])
	}
	for index, name := range rejectionReasonNames() {
		if published[index] != name {
			test.Fatalf("published vocabulary differs at %d: %q", index, published[index])
		}
	}
	for reason := range gatewayMetricCounts(test, fixture.gateway, "rejection_reasons_total") {
		if !seen[reason] {
			test.Fatalf("an unlisted reason reached the counters: %q", reason)
		}
	}
}

// TestGatewayRefusalReplyKeepsPublishedFailureFields guards the protocol half of
// this change. platform/web/rpc.js treats a failure object carrying any key other
// than these four as malformed and answers it as an unknown outcome, which would
// turn a refusal into a poisoned session, so a finer reason must never be added to
// the reply body.
func TestGatewayRefusalReplyKeepsPublishedFailureFields(test *testing.T) {
	fixture := newGatewayFixture(test)
	gatewayQuota(test, fixture, "project-a", func(quota *Quotas) { quota.MaxSessions = 1 })
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != 429 {
		test.Fatalf("refusal not reached: %d", response.Code)
	}
	var envelope struct {
		Error map[string]any `json:"error"`
	}
	if json.Unmarshal(response.Body.Bytes(), &envelope) != nil || envelope.Error == nil {
		test.Fatalf("invalid refusal envelope: %s", response.Body.String())
	}
	for key := range envelope.Error {
		if key != "code" {
			test.Fatalf("refusal body carries %q, which the published clients reject", key)
		}
	}
}
