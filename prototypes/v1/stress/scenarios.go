package stress

import (
	"errors"
	"sort"
)

type Scenario struct {
	Name       string `json:"name"`
	Phase      string `json:"phase"`
	Capability string `json:"capability"`
	Reason     string `json:"reason,omitempty"`
}

func Scenarios() []Scenario {
	return []Scenario{
		{"session_cycles", "architecture", "SUPPORTED", "repeated actual gateway session open/read/close; not native cold eviction"},
		{"many_db", "architecture", "REQUIRES_CONFIG", "at least two supplied isolated database IDs"},
		{"noisy_tenant", "architecture", "REQUIRES_CONFIG", "second independent project key and concurrency >=2; isolated and combined phases"},
		{"capacity_competition", "architecture", "REQUIRES_CONFIG", "independent projects; sustained durable writes in isolated and combined phases; combined concurrency 1 is NOT MEASURED"},
		{"capacity_read", "architecture", "SUPPORTED", "bounded read capacity across supplied independent databases/projects"},
		{"capacity_short_writer", "architecture", "SUPPORTED", "bounded transactional writes with per-phase durable ledger"},
		{"capacity_held_writer", "architecture", "SUPPORTED", "100ms held writer, alternating COMMIT/ROLLBACK with reserved cleanup deadline"},
		{"capacity_io_checkpoint", "architecture", "REQUIRES_CONFIG", "actual acknowledged write/checkpoint competition; physical IO must be independently observed"},
		{"capacity_mixed", "architecture", "SUPPORTED", "quiet C1 70-percent read mixed OLTP gate; observed gateway actions delta only, no business-to-RPC conversion"},
		{"saturation_admission", "architecture", "SUPPORTED", "offered-rate scheduling and actual admission errors; no claim of saturation if none observed"},
		{"idle_suspend_resume", "architecture", "REQUIRES_CONFIG", "management key; verifies suspended/active placement and durable rows, not cache eviction"},
		{"graceful_migrate", "architecture", "REQUIRES_CONFIG", "explicit two-worker target and management key; acknowledged ledger after fresh session"},
		{"cold_vs_warm", "architecture", "SKIP", "gateway API does not prove an empty native cache or expose a safe cold-cache reset"},
		{"migrate_fault", "architecture", "SKIP", "fixture-only injector contract not established; never signals topology PIDs"},
		{"lease_expiry_controller_loss", "architecture", "SKIP", "no approved fixture-scoped fault injector or terminal fencing oracle"},
		{"storage_fault_gc_reader_pins", "architecture", "SKIP", "no isolated safe storage fault/pin/delayed-deletion hooks"},
		{"point_read", "sql", "SUPPORTED", ""},
		{"index_range", "sql", "SUPPORTED", ""},
		{"pagination", "sql", "SUPPORTED", ""},
		{"mixed_oltp", "sql", "SUPPORTED", "70 percent reads; transactional write ledger and account sums"},
		{"hot_keys", "sql", "SUPPORTED", "four contended accounts"},
		{"bulk_tx", "sql", "SUPPORTED", "eight updates and ledger entries per acknowledged transaction"},
		{"sort_aggregate_join", "sql", "SUPPORTED", "independent seeded group-count and score-sum oracle"},
		{"index_plan", "sql", "SUPPORTED", "actual EXPLAIN output must name the created index"},
		{"rollback", "sql", "SUPPORTED", "rolled-back unique probe must remain absent"},
		{"constraint_error_reuse", "sql", "SUPPORTED", "bounded correctness proof: terminal typed SQL error, explicit same-session rollback and acknowledged write/read reuse"},
		{"large_values", "sql", "SUPPORTED", "16KiB deterministic text values within native SQL/result budgets"},
		{"sustained_wal", "sql", "SUPPORTED", "durable writes; WAL backlog unavailable unless exposed by actual management metrics"},
		{"checkpoint_reopen", "sql", "REQUIRES_CONFIG", "management checkpoint plus actual new gateway session; not native empty-cache restart"},
		{"snapshot_branch_pitr_under_load", "sql", "SKIP", "mutation/restore fault schedules and independent retention oracles not implemented"},
	}
}

func SelectScenarios(names []string) ([]Scenario, error) {
	chosen := make(map[string]bool)
	for _, name := range names {
		chosen[name] = true
	}
	known := map[string]bool{"all": true, "architecture": true, "sql": true}
	var output []Scenario
	for _, scenario := range Scenarios() {
		known[scenario.Name] = true
		if chosen["all"] || chosen[scenario.Phase] || chosen[scenario.Name] {
			output = append(output, scenario)
		}
	}
	for name := range chosen {
		if !known[name] {
			return nil, errors.New("unknown scenario")
		}
	}
	sort.SliceStable(output, func(first, second int) bool {
		return output[first].Phase == "architecture" && output[second].Phase != "architecture"
	})
	return output, nil
}
