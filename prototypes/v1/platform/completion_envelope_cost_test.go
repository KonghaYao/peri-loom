package platform

// Q04 cost instrumentation. The fixture is deliberately local: it measures the
// durable appends the gateway owns, not the worker RPC or the object store, which
// only the frozen stack can attribute. Every number is read from the production
// counters, so a run with the phase observer enabled can be compared with these
// segments directly.

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func completionPercentile(samples []time.Duration, fraction float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func completionDurationDelta(after, before map[string]uint64, key string) uint64 {
	if after[key] <= before[key] {
		return 0
	}
	return after[key] - before[key]
}

func completionAuditDelta(after, before map[string]any, key string) uint64 {
	current, ok := after[key].(uint64)
	if !ok {
		return 0
	}
	previous, ok := before[key].(uint64)
	if !ok || current <= previous {
		return 0
	}
	return current - previous
}

// TestCompletionPathSegmentCost reports the sequential cost of one committing
// request's completion path: intent fsync, terminal fsync, and the completion
// audit fsync. Worker RPC and object staging are not measured here; the report
// says so instead of attributing them to the fixture.
func TestCompletionPathSegmentCost(test *testing.T) {
	if testing.Short() {
		test.Skip("completion cost measurement runs a real barrier per segment")
	}
	const samples = 20
	database := securityDatabase()
	store := operationDurableStore(test).(*stateStore)
	service, err := OpenAudit(filepath.Join(test.TempDir(), "audit.jsonl"))
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = service.Close() })
	audit := service.(*fileAudit)

	before := store.operations.stats()
	auditBefore := audit.AuditStatus()
	intents := make([]time.Duration, 0, samples)
	terminals := make([]time.Duration, 0, samples)
	audits := make([]time.Duration, 0, samples)
	for index := 0; index < samples; index++ {
		identifier := fmt.Sprintf("q04cost%02d", index)
		started := time.Now()
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		intents = append(intents, time.Since(started))
		started = time.Now()
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			test.Fatal(err)
		}
		terminals = append(terminals, time.Since(started))
		started = time.Now()
		if err := audit.Record(completionAuditEvent(identifier)); err != nil {
			test.Fatal(err)
		}
		audits = append(audits, time.Since(started))
	}
	after := store.operations.stats()
	auditAfter := audit.AuditStatus()

	journalSyncs := completionDurationDelta(after, before, "operation_journal_syncs_total")
	journalSyncNS := completionDurationDelta(after, before, "operation_journal_sync_ns_total")
	auditSyncs := completionAuditDelta(auditAfter, auditBefore, "fsync_count")
	auditSyncNS := completionAuditDelta(auditAfter, auditBefore, "fsync_ns_total")
	if journalSyncs != 2*samples {
		test.Fatalf("journal paid %d barriers for %d intent+terminal appends", journalSyncs, samples)
	}
	if auditSyncs != samples {
		test.Fatalf("audit paid %d barriers for %d records", auditSyncs, samples)
	}
	if journalSyncNS == 0 || auditSyncNS == 0 {
		test.Fatalf("barrier time was not accounted: journal=%d audit=%d", journalSyncNS, auditSyncNS)
	}

	test.Logf("completion segments (%d sequential samples, real fsync):", samples)
	test.Logf("  intent  p50=%s p95=%s", completionPercentile(intents, 0.50), completionPercentile(intents, 0.95))
	test.Logf("  terminal p50=%s p95=%s", completionPercentile(terminals, 0.50), completionPercentile(terminals, 0.95))
	test.Logf("  audit   p50=%s p95=%s", completionPercentile(audits, 0.50), completionPercentile(audits, 0.95))
	test.Logf("  journal barriers=%d (%0.1f/request) sync_ns_total=%d", journalSyncs, float64(journalSyncs)/samples, journalSyncNS)
	test.Logf("  audit barriers=%d (%0.1f/request) sync_ns_total=%d", auditSyncs, float64(auditSyncs)/samples, auditSyncNS)
	test.Logf("  journal stages ns: encode=%d verify=%d assemble=%d write=%d sync=%d witness=%d wait=%d",
		completionDurationDelta(after, before, "operation_journal_encode_ns_total"),
		completionDurationDelta(after, before, "operation_journal_verify_ns_total"),
		completionDurationDelta(after, before, "operation_journal_assemble_ns_total"),
		completionDurationDelta(after, before, "operation_journal_write_ns_total"),
		completionDurationDelta(after, before, "operation_journal_sync_ns_total"),
		completionDurationDelta(after, before, "operation_journal_witness_ns_total"),
		completionDurationDelta(after, before, "operation_journal_wait_ns_total"))
	test.Logf("  worker RPC and object-store staging: NOT MEASURED in this fixture")
}

func completionBenchStore(bench *testing.B) *stateStore {
	bench.Helper()
	store, err := OpenStateStore(bench.TempDir())
	if err != nil {
		bench.Fatal(err)
	}
	bench.Cleanup(func() { _ = store.Close() })
	if err := store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "test"}
		state.Projects["alpha"] = Project{ID: "alpha", Organization: "org", Tenant: "tenant_a"}
		database := securityDatabase()
		state.Databases[database.Key()] = Database{ID: database, Project: "alpha", State: "active"}
		return nil
	}); err != nil {
		bench.Fatal(err)
	}
	return store.(*stateStore)
}

func completionBenchSyncs(bench *testing.B, store *stateStore) {
	bench.ReportMetric(float64(store.operations.stats()["operation_journal_syncs_total"])/float64(bench.N), "fsync/op")
}

func BenchmarkCompletionIntent(bench *testing.B) {
	store := completionBenchStore(bench)
	database := securityDatabase()
	bench.ResetTimer()
	for index := 0; index < bench.N; index++ {
		identifier := fmt.Sprintf("benchintent%08d", index)
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			bench.Fatal(err)
		}
	}
	bench.StopTimer()
	completionBenchSyncs(bench, store)
}

func BenchmarkCompletionIntentTerminalPair(bench *testing.B) {
	store := completionBenchStore(bench)
	database := securityDatabase()
	bench.ResetTimer()
	for index := 0; index < bench.N; index++ {
		identifier := fmt.Sprintf("benchpair%08d", index)
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			bench.Fatal(err)
		}
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			bench.Fatal(err)
		}
	}
	bench.StopTimer()
	completionBenchSyncs(bench, store)
}

func BenchmarkCompletionTerminalOnly(bench *testing.B) {
	store := completionBenchStore(bench)
	database := securityDatabase()
	frozen, err := freezeCommandValue(nil)
	if err != nil {
		bench.Fatal(err)
	}
	bench.ResetTimer()
	for index := 0; index < bench.N; index++ {
		identifier := fmt.Sprintf("benchterminal%08d", index)
		if _, err := settleNonCommittingOperation(store, database, identifier, "gateway", "describe", "tool_rpc", "worker-a", "", frozen.digest(), func(record *OperationRecord) {
			record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "not_executed", false
		}); err != nil {
			bench.Fatal(err)
		}
	}
	bench.StopTimer()
	completionBenchSyncs(bench, store)
}

func BenchmarkCompletionAudit(bench *testing.B) {
	service, err := OpenAudit(filepath.Join(bench.TempDir(), "audit.jsonl"))
	if err != nil {
		bench.Fatal(err)
	}
	defer service.Close()
	audit := service.(*fileAudit)
	bench.ResetTimer()
	for index := 0; index < bench.N; index++ {
		if err := audit.Record(completionAuditEvent(fmt.Sprintf("benchaudit%08d", index))); err != nil {
			bench.Fatal(err)
		}
	}
	bench.StopTimer()
	status := audit.AuditStatus()
	bench.ReportMetric(float64(status["fsync_count"].(uint64))/float64(bench.N), "fsync/op")
}

func BenchmarkCompletionMixed(bench *testing.B) {
	store := completionBenchStore(bench)
	database := securityDatabase()
	service, err := OpenAudit(filepath.Join(bench.TempDir(), "audit.jsonl"))
	if err != nil {
		bench.Fatal(err)
	}
	defer service.Close()
	audit := service.(*fileAudit)
	bench.ResetTimer()
	for index := 0; index < bench.N; index++ {
		identifier := fmt.Sprintf("benchmixed%08d", index)
		if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			bench.Fatal(err)
		}
		if err := finishOperation(store, database, identifier, nil, true); err != nil {
			bench.Fatal(err)
		}
		if err := audit.Record(completionAuditEvent(identifier)); err != nil {
			bench.Fatal(err)
		}
	}
	bench.StopTimer()
	completionBenchSyncs(bench, store)
	status := audit.AuditStatus()
	bench.ReportMetric(float64(status["fsync_count"].(uint64))/float64(bench.N), "audit_fsync/op")
}
