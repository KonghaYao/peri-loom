package stress

import (
	"encoding/json"
	"sort"
	"strconv"
)

type Quantiles struct {
	Count int    `json:"count"`
	P50   *int64 `json:"p50_ns"`
	P95   *int64 `json:"p95_ns"`
	P99   *int64 `json:"p99_ns"`
	Max   *int64 `json:"max_ns"`
}

func LatencyQuantiles(samples []int64) Quantiles {
	result := Quantiles{Count: len(samples)}
	if len(samples) == 0 {
		return result
	}
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(first, second int) bool { return ordered[first] < ordered[second] })
	percentile := func(percent int) *int64 {
		index := (len(ordered)*percent+99)/100 - 1
		value := ordered[index]
		return &value
	}
	result.P50, result.P95, result.P99 = percentile(50), percentile(95), percentile(99)
	maximum := ordered[len(ordered)-1]
	result.Max = &maximum
	return result
}

func integer(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		integer, err := typed.Int64()
		return integer, err == nil
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		integer, err := strconv.ParseInt(typed, 10, 64)
		return integer, err == nil
	default:
		return 0, false
	}
}

type OperationRecord struct {
	Type             string     `json:"type"`
	Scenario         string     `json:"scenario"`
	Phase            string     `json:"phase"`
	Sequence         int        `json:"offered_sequence"`
	BusinessID       string     `json:"business_operation_id"`
	RequestIDs       []string   `json:"request_ids"`
	Project          string     `json:"project"`
	Database         DatabaseID `json:"database"`
	Slot             int        `json:"session_slot"`
	OfferedAt        string     `json:"offered_at_observed"`
	StartedAt        string     `json:"started_at_observed"`
	FinishedAt       string     `json:"finished_at_observed"`
	QueueNS          int64      `json:"client_queue_ns"`
	PrepareNS        int64      `json:"client_prepare_ns"`
	ElapsedNS        int64      `json:"http_workflow_ns"`
	EndToEndNS       int64      `json:"offered_to_response_ns"`
	Acked            bool       `json:"acked"`
	Correct          bool       `json:"correct"`
	Failure          *Failure   `json:"failure"`
	Mutations        []Mutation `json:"acknowledged_mutations,omitempty"`
	ExpectedRowsHash string     `json:"expected_rows_hash,omitempty"`
	ActualRowsHash   string     `json:"actual_rows_hash,omitempty"`
}

type PhaseResult struct {
	Scenario               string         `json:"scenario"`
	Phase                  string         `json:"phase"`
	Concurrency            int            `json:"concurrency"`
	Status                 string         `json:"status"`
	Reason                 string         `json:"reason,omitempty"`
	StartedAt              string         `json:"started_at_observed"`
	FinishedAt             string         `json:"finished_at_observed"`
	ElapsedNS              int64          `json:"elapsed_ns"`
	OfferingElapsedNS      int64          `json:"offering_elapsed_ns"`
	ConfiguredRate         int            `json:"configured_offered_per_second"`
	RequestedDurationNS    int64          `json:"requested_duration_ns"`
	PlannedOffers          int            `json:"planned_offers"`
	MissedOffers           int            `json:"scheduled_offers_not_emitted"`
	OperationCapReached    bool           `json:"operation_cap_reached"`
	ActiveDatabases        int            `json:"independently_active_databases"`
	Offered                int            `json:"offered"`
	ClientAdmissionDropped int            `json:"client_admission_dropped"`
	QueueExpired           int            `json:"client_queue_expired"`
	Attempted              int            `json:"attempted"`
	Completed              int            `json:"completed_workflows"`
	Acked                  int            `json:"acked_workflows"`
	CorrectnessFailures    int            `json:"correctness_failures"`
	Errors                 map[string]int `json:"errors_by_class"`
	Timeouts               int            `json:"timeouts"`
	Unknown                int            `json:"unknown"`
	CleanupUnknown         int            `json:"cleanup_unknown"`
	OfferedRate            float64        `json:"observed_offered_per_second"`
	AchievedRate           float64        `json:"acked_per_second_including_drain"`
	AllLatency             Quantiles      `json:"all_workflow_latency"`
	SuccessLatency         Quantiles      `json:"acked_workflow_latency"`
	QueueLatency           Quantiles      `json:"client_queue_latency"`
	PerProjectAcked        map[string]int `json:"per_project_acked"`
	PerProjectOffered      map[string]int `json:"per_project_offered"`
	SaturationObserved     bool           `json:"actual_admission_rejection_observed"`
	ShortGate              bool           `json:"short_gate_not_benchmark"`
}

type ScenarioResult struct {
	Scenario    string        `json:"scenario"`
	Status      string        `json:"status"`
	Reason      string        `json:"reason,omitempty"`
	Phases      []PhaseResult `json:"phases,omitempty"`
	Correctness []Correctness `json:"correctness,omitempty"`
}

func (result PhaseResult) MarshalJSON() ([]byte, error) {
	if result.StartedAt == "" {
		return json.Marshal(map[string]any{"scenario": result.Scenario, "phase": result.Phase, "concurrency": result.Concurrency,
			"status": result.Status, "reason": result.Reason, "errors_by_class": result.Errors, "unknown": result.Unknown, "cleanup_unknown": result.CleanupUnknown,
			"measurement_status": "unavailable_not_started", "elapsed_ns": nil, "offering_elapsed_ns": nil, "all_workflow_latency": nil})
	}
	type plain PhaseResult
	return json.Marshal(plain(result))
}
