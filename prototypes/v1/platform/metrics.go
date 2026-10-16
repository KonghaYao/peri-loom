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
	// admitted counts the requests that held an admission permit. A refusal the
	// platform observed after granting one — a result budget that no longer fitted,
	// a deadline the worker proved it turned away before execution — is counted here
	// and in rejected: the request was admitted and then refused, and reporting
	// either half alone would misstate what happened.
	admitted uint64
	// succeeded and rejected split the account the research document asks for:
	// admitted goodput against refusals. The two are not complements of each other,
	// because a request can fail without a named refusal and can be refused after
	// admission.
	succeeded uint64
	rejected  uint64
	reasons   map[string]uint64
	// goodput is the latency account of successful requests only. A refusal is
	// usually the cheapest answer the platform gives, and mixing it into the latency
	// of served work is what makes an overloaded gateway look fast.
	goodput map[string]uint64
}

// gatewayObservation is one answered request. admitted reports that the request
// held an admission permit, and rejection names the layer that refused it when a
// layer refused it; both are read from the refusal site, never from the code.
type gatewayObservation struct {
	action    string
	code      string
	rejection rejectionReason
	admitted  bool
	elapsed   time.Duration
}

func (metrics *gatewayCounters) observe(observation gatewayObservation) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	nanoseconds := uint64(observation.elapsed.Nanoseconds())
	metrics.actions[observation.action]++
	metrics.latency[observation.action] += nanoseconds
	if observation.admitted {
		metrics.admitted++
	}
	if observation.rejection != rejectionNone {
		metrics.rejected++
		metrics.reasons[string(observation.rejection)]++
	}
	if observation.code != "" {
		metrics.errors[observation.code]++
		return
	}
	metrics.succeeded++
	metrics.goodput[observation.action] += nanoseconds
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
	// Only observed reasons are reported, and the published vocabulary below is what
	// tells a consumer a reason is defined but unobserved. The labels stay bounded:
	// reasons are a closed set, actions and codes are a closed set, and no database,
	// session, request or operation identity ever becomes a label.
	reasons, goodput := map[string]uint64{}, map[string]uint64{}
	for reason, count := range metrics.reasons {
		reasons[reason] = count
	}
	for action, total := range metrics.goodput {
		goodput[action] = total
	}
	admitted, succeeded, rejected := metrics.admitted, metrics.succeeded, metrics.rejected
	metrics.mu.Unlock()
	projects, active, unknown := gateway.admission.status()
	workers := map[string]any{}
	if metadata, err := gateway.config.Placements.Snapshot(); err == nil {
		for identifier, worker := range metadata.Workers {
			workers[identifier] = map[string]any{"observed_load": worker.Load, "healthy": worker.Healthy, "last_heartbeat_ms": worker.LastHeartbeatMS}
		}
	}
	return map[string]any{"actions_total": actions, "errors_total": failures, "action_latency_ns_total": latency, "execute_latency_semantics": "execute_not_all_commits", "admitted_requests_total": admitted, "successful_requests_total": succeeded, "rejected_requests_total": rejected, "rejection_reasons_total": reasons, "rejection_reason_values": rejectionReasonNames(), "successful_latency_ns_total": goodput, "admitted_requests_semantics": "held_an_admission_permit_a_refusal_after_the_permit_counts_as_admitted_and_rejected", "successful_latency_semantics": "successful_requests_only_refusals_never_enter_this_account", "action_latency_semantics": "all_answered_requests_including_refusals", "rejection_reason_semantics": "named_platform_refusals_recorded_at_the_refusal_site_the_client_reply_keeps_its_published_code_and_fields", "projects": projects, "active_permits": active, "unknown_held_permits": unknown, "observed_workers": workers, "cold_load_instrumentation": "not_exposed_by_worker_sql_protocol", "streaming_semantics": "bounded_worker_response_then_ndjson", "quota_scope": "single_gateway_process", "unknown_recovery": "operator_worker_terminal_proof_required_no_automatic_release", "request_id_semantics": "correlation_only_not_sql_deduplication", "state_operation_cost": stateOperationCost(gateway.config.State), "cleanup_ledger": gateway.cleanupLedger(), "cleanup_retry": "bounded_close_retry_no_sql_replay", "cleanup_age_semantics": "in_process_observation_durable_operation_records_carry_no_timestamp"}
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
