package platform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// Evidence contract, observed end to end against a real Authority, worker and
// Gateway: a worker failure carries the execution evidence of that request, and
// the platform maps the states to distinct public outcomes.

func TestGatewayOutcomeEvidenceNative(test *testing.T) {
	fixture := p6VerifyNewWithQuotas(test, Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096})
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE evidence_items(value INTEGER)")

	// 未进入执行：准备期就失败，回执必须说明没有执行过，且客户端 SQL 错误按 400 归类。
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/execute", "evidence_rejected", map[string]any{"session": session, "sql": "INSERT INTO absent VALUES(1)"})
	failure := evidenceFailure(test, reply)
	if status != 400 || failure.Code != "sql_error" || failure.ExecutionCompleted || failure.MayHaveCommitted || failure.Message == "" {
		test.Fatalf("not-executed rejection misreported: status=%d error=%s", status, reply["error"])
	}
	test.Log("NATIVE_OUTCOME_EVIDENCE case=not_executed status=400 code=sql_error execution_completed=false may_have_committed=false")

	// 已收束：第一句已提交，第二句执行失败，回执必须声明可能已提交。
	status, reply = unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/execute", "evidence_settled", map[string]any{"session": session, "sql": "INSERT INTO evidence_items VALUES(1); INSERT INTO absent VALUES(1)"})
	failure = evidenceFailure(test, reply)
	if status != 400 || failure.Code != "sql_error" || !failure.ExecutionCompleted || !failure.MayHaveCommitted {
		test.Fatalf("settled failure misreported: status=%d error=%s", status, reply["error"])
	}
	reader := fixture.open(fixture.credential, fixture.database)
	if rows := fixture.sql(reader, "query", "SELECT count(*) FROM evidence_items"); string(rows.Result) != "[[1]]" {
		test.Fatalf("committed prefix not independently observable: %s", rows.Result)
	}
	test.Log("NATIVE_OUTCOME_EVIDENCE case=settled_after_commit status=400 code=sql_error execution_completed=true may_have_committed=true independent_rows=1")

	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": reader})
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
}

// The duration budget interrupts a request that already committed a prefix, so
// it must travel the same evidence channel instead of claiming nothing ran, and
// it must stay distinguishable from a statement the client wrote wrong.
func TestGatewayDurationBudgetEvidenceNative(test *testing.T) {
	// 预算在进程启动时读取，必须在 fixture 拉起 worker 之前生效；1s 足以稳定击穿。
	// VM step 预算必须同时抬高：0 会被 worker 拒绝，默认 10M 又会在 1s 内先耗尽，
	// 只有 duration 成为唯一绑定约束，本用例才真正覆盖 duration 证据。
	test.Setenv("PERI_RUST_BINARY", durationBudgetWrapper(test,
		"PERI_SQL_MAX_DURATION_MS=1000 PERI_SQL_MAX_VM_STEPS=18446744073709551615"))
	fixture := p6VerifyNewWithQuotas(test, Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096})
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE budget_items(value INTEGER)")
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/execute", "evidence_duration", map[string]any{"session": session, "sql": "INSERT INTO budget_items VALUES(1); WITH RECURSIVE forever(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM forever) SELECT sum(x) FROM forever"})
	failure := evidenceFailure(test, reply)
	if status != 504 || failure.Code != "sql_timeout" || !failure.ExecutionCompleted || !failure.MayHaveCommitted || !strings.Contains(failure.Message, "PERI_SQL_MAX_DURATION_MS") {
		test.Fatalf("duration budget failure misreported: status=%d error=%s", status, reply["error"])
	}
	witnessed := false
	for _, worker := range fixture.workers {
		witnessed = witnessed || strings.Contains(worker.log.text(fixture.secrets), `"cause_class":"duration_budget"`)
	}
	if !witnessed {
		test.Fatal("duration budget was reported without a duration_budget diagnostic")
	}
	reader := fixture.open(fixture.credential, fixture.database)
	if rows := fixture.sql(reader, "query", "SELECT count(*) FROM budget_items"); string(rows.Result) != "[[1]]" {
		test.Fatalf("duration budget prefix not committed: %s", rows.Result)
	}
	test.Log("NATIVE_OUTCOME_EVIDENCE case=duration_budget status=504 code=sql_timeout execution_completed=true may_have_committed=true worker_cause=duration_budget independent_rows=1")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": reader})
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
}

// A durable Unknown keeps the database isolated even when placement routing is
// also unavailable: the isolation cause must win so clients cannot read an
// ambiguous write as a retryable routing problem.
func TestGatewayIsolationOutranksUnavailablePlacement(test *testing.T) {
	fixture := newGatewayFixture(test)
	if err := beginOperation(fixture.state, fixture.first, "unknown_probe", "gateway", "execute", "tool_rpc", "worker-tenant-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.state, fixture.first, "unknown_probe", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		for _, database := range []controller.DatabaseID{fixture.first, fixture.second} {
			placement := metadata.Placements[database.Key()]
			placement.Phase = "drain_intent"
			metadata.Placements[database.Key()] = placement
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != 409 || gatewayCode(test, response) != "database_isolated" {
		test.Fatalf("isolated database reported as unavailable placement: %d %s", response.Code, response.Body.String())
	}
	control := gatewayHTTP(test, fixture.gateway, fixture.tokenB, "/v1/sql/open", map[string]any{"database": fixture.second})
	if control.Code != 409 || gatewayCode(test, control) != "placement_unavailable" {
		test.Fatalf("healthy namespace lost its placement diagnosis: %d %s", control.Code, control.Body.String())
	}
	test.Log("NATIVE_MAPPING isolated_and_unavailable status=409 code=database_isolated control=placement_unavailable")
}

type evidenceReply struct {
	Code               string `json:"code"`
	Message            string `json:"message"`
	ExecutionCompleted bool   `json:"execution_completed"`
	MayHaveCommitted   bool   `json:"may_have_committed"`
}

func evidenceFailure(test *testing.T, reply map[string]json.RawMessage) evidenceReply {
	test.Helper()
	var failure evidenceReply
	if json.Unmarshal(reply["error"], &failure) != nil {
		test.Fatalf("SQL failure envelope missing: %s", reply["error"])
	}
	return failure
}

func durationBudgetWrapper(test *testing.T, override string) string {
	test.Helper()
	binary := p6VerifyRustBinary(test)
	absolute, err := filepath.Abs(binary)
	if err != nil {
		test.Fatal(err)
	}
	wrapper := filepath.Join(test.TempDir(), "peri-loom-v1-budget-wrapper")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\n"+override+" exec \""+absolute+"\" \"$@\"\n"), 0700); err != nil {
		test.Fatal(err)
	}
	return wrapper
}
