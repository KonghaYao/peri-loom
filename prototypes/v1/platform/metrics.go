package platform

import (
	"sync"
	"time"
)

type gatewayCounters struct {
	mu      sync.Mutex
	actions map[string]uint64
	errors  map[string]uint64
	latency map[string]uint64
}

func (metrics *gatewayCounters) observe(action, code string, elapsed time.Duration) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.actions[action]++
	metrics.latency[action] += uint64(elapsed.Nanoseconds())
	if code != "" {
		metrics.errors[code]++
	}
}

func (gateway *Gateway) Metrics() map[string]any {
	metrics := gateway.counters
	metrics.mu.Lock()
	actions, failures, latency := map[string]uint64{}, map[string]uint64{}, map[string]uint64{}
	for action, count := range metrics.actions {
		actions[action] = count
	}
	for code, count := range metrics.errors {
		failures[code] = count
	}
	for action, total := range metrics.latency {
		latency[action] = total
	}
	metrics.mu.Unlock()
	projects, active, unknown := gateway.admission.status()
	workers := map[string]any{}
	if metadata, err := gateway.config.Placements.Snapshot(); err == nil {
		for identifier, worker := range metadata.Workers {
			workers[identifier] = map[string]any{"observed_load": worker.Load, "healthy": worker.Healthy, "last_heartbeat_ms": worker.LastHeartbeatMS}
		}
	}
	return map[string]any{"actions_total": actions, "errors_total": failures, "action_latency_ns_total": latency, "execute_latency_semantics": "execute_not_all_commits", "projects": projects, "active_permits": active, "unknown_held_permits": unknown, "observed_workers": workers, "cold_load_instrumentation": "not_exposed_by_worker_sql_protocol", "streaming_semantics": "bounded_worker_response_then_ndjson", "quota_scope": "single_gateway_process", "unknown_recovery": "operator_worker_terminal_proof_required_no_automatic_release", "request_id_semantics": "correlation_only_not_sql_deduplication", "state_operation_cost": stateOperationCost(gateway.config.State)}
}

// stateOperationCost exposes the shared lock and operation log cost of a commit, which
// otherwise lands on requests that never see it in their own latency.
func stateOperationCost(state StateStore) map[string]any {
	provider, ok := state.(interface{ operationStats() map[string]any })
	if !ok {
		return nil
	}
	return provider.operationStats()
}
