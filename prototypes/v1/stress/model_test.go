package stress

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
)

func TestLatencyQuantilesNearestRankAndMissing(t *testing.T) {
	if result := LatencyQuantiles(nil); result.Count != 0 || result.P50 != nil || result.Max != nil {
		t.Fatal("empty sample fabricated latency")
	}
	input := []int64{40, 10, 30, 20}
	result := LatencyQuantiles(input)
	if *result.P50 != 20 || *result.P95 != 40 || *result.P99 != 40 || *result.Max != 40 || input[0] != 40 {
		t.Fatal("incorrect quantiles or input mutation")
	}
}

func TestDeterministicModelAndReadOracles(t *testing.T) {
	first, second := NewModel("stress_unit", 1000, 41), NewModel("stress_unit", 1000, 41)
	for _, kind := range []string{"point_read", "index_range", "pagination", "sort_aggregate_join", "large_values"} {
		left, right := first.Task(kind, "business1", rand.New(rand.NewSource(9))), second.Task(kind, "business1", rand.New(rand.NewSource(9)))
		if left.SQL != right.SQL || HashRows(left.Expected) != HashRows(right.Expected) {
			t.Fatalf("nondeterministic %s", kind)
		}
		encoded, _ := json.Marshal(left.Expected)
		var native [][]any
		if decodeStrict(encoded, &native) != nil || !left.Check(native) {
			t.Fatalf("native JSON oracle mismatch %s", kind)
		}
		if len(native) > 0 {
			native[0][0] = "wrong"
			if left.Check(native) {
				t.Fatal("bad row accepted")
			}
		}
	}
}

func TestWriteLedgerAtomicBusinessOperationAndHotKeyBounds(t *testing.T) {
	model := NewModel("stress_unit", 1000, 17)
	bulk := model.Task("bulk_tx", "business_0001", rand.New(rand.NewSource(3)))
	if len(bulk.Mutations) != 8 || !strings.HasPrefix(bulk.SQL, "BEGIN IMMEDIATE;") || !strings.HasSuffix(bulk.SQL, "COMMIT") {
		t.Fatal("bulk transaction not atomic modeled unit")
	}
	for ordinal, mutation := range bulk.Mutations {
		if mutation.OperationID != "business_0001" || mutation.Ordinal != ordinal || mutation.Account < 1 || mutation.Account > 1000 {
			t.Fatal("invalid modeled ledger")
		}
	}
	for seed := int64(0); seed < 32; seed++ {
		hot := model.Task("hot_keys", "business_hot", rand.New(rand.NewSource(seed)))
		if len(hot.Mutations) != 1 || hot.Mutations[0].Account > 4 {
			t.Fatal("hot key escaped explicit domain")
		}
	}
}

func TestAggregateOracleChecksConservation(t *testing.T) {
	model := NewModel("stress_unit", 10000, 91)
	task := model.Task("sort_aggregate_join", "", rand.New(rand.NewSource(1)))
	var count, score int64
	for _, row := range task.Expected {
		count += row[1].(int64)
		score += row[2].(int64)
	}
	var baseline int64
	for _, account := range model.Accounts {
		baseline += account.Score
	}
	if count != 10000 || score != baseline {
		t.Fatal("aggregate oracle does not conserve independent dataset")
	}
}

func TestRollbackHasNoAcknowledgedMutationAndIndexPlanRequiresEvidence(t *testing.T) {
	model := NewModel("stress_unit", 1000, 1)
	task := model.Task("rollback", "business_1", rand.New(rand.NewSource(1)))
	if len(task.Mutations) != 0 || !strings.Contains(task.SQL, "ROLLBACK") {
		t.Fatal("rollback recorded committed mutation")
	}
	plan := model.Task("index_plan", "", rand.New(rand.NewSource(1)))
	if plan.Check([][]any{{"SCAN accounts"}}) || !plan.Check([][]any{{"SEARCH USING " + model.Table("group_index")}}) {
		t.Fatal("index plan assertion fabricated usage")
	}
}

func TestCPUTimeParsing(t *testing.T) {
	for input, expected := range map[string]float64{"00:01.50": 1.5, "02:03": 123, "01:02:03": 3723, "2-01:02:03": 176523} {
		actual, valid := parseCPUTime(input)
		if !valid || actual != expected {
			t.Fatalf("invalid CPU-time parsing for %s", input)
		}
	}
	if _, valid := parseCPUTime("not_cpu_time"); valid {
		t.Fatal("invented CPU time")
	}
}

func TestArchitectureOrderingAndUnsupportedExplicitSkips(t *testing.T) {
	cases, err := SelectScenarios([]string{"point_read", "migrate_fault", "many_db"})
	if err != nil || cases[0].Phase != "architecture" || cases[len(cases)-1].Name != "point_read" {
		t.Fatal("SQL was ordered before architecture")
	}
	for _, item := range cases {
		if item.Name == "migrate_fault" && (item.Capability != "SKIP" || item.Reason == "") {
			t.Fatal("unsupported fault falsely supported")
		}
	}
	if _, err := SelectScenarios([]string{"unsupported_name"}); err == nil {
		t.Fatal("unknown workload accepted")
	}
}

func TestSkippedPhaseDoesNotFabricateZeroMeasurement(t *testing.T) {
	encoded, err := json.Marshal(PhaseResult{Scenario: "noisy_tenant", Status: "SKIP", Reason: "requires two sessions"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if json.Unmarshal(encoded, &fields) != nil || fields["elapsed_ns"] != nil || fields["all_workflow_latency"] != nil || fields["measurement_status"] != "unavailable_not_started" {
		t.Fatal("skipped scenario fabricated measurement")
	}
}
