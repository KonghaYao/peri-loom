package platform

import (
	"sync"
	"time"
)

// rejectionReason names the layer that refused a request. Every value is
// produced where the refusal happens — the lane that was full, the budget that
// was exhausted — and never inferred from the answer afterwards, so a caller
// reading the counters sees which layer refused rather than which code was
// returned.
//
// The values are stable strings because they are counter labels and appear in
// the operator log. They are additive evidence next to the published failure
// code: the code a client decodes keeps its name and meaning, and the reply body
// keeps exactly the fields it published, because platform/web/rpc.js answers a
// failure object carrying any other key as an unknown outcome.
type rejectionReason string

const (
	// rejectionNone is the absence of a refusal, not a refusal of its own.
	rejectionNone rejectionReason = ""

	// rejectionSessionLimit is the project's session seats (Quotas.MaxSessions).
	rejectionSessionLimit rejectionReason = "session_limit"
	// rejectionConcurrency is the project's active lanes (Quotas.MaxConcurrent),
	// including the lane a writer-admission profile keeps for the writer owner.
	rejectionConcurrency rejectionReason = "concurrency_limit"
	// rejectionRate is the project's token bucket (Quotas.RequestsPerSecond).
	rejectionRate rejectionReason = "rate_limit"
	// rejectionWriterBusy is the per-database writer lane: either this project's
	// writer owner is already executing, or the worker proved that another writer
	// holds the writer admission of the database.
	rejectionWriterBusy rejectionReason = "writer_busy"
	// rejectionClassifyLimit is the bound on concurrent read-only classification
	// calls of one project.
	rejectionClassifyLimit rejectionReason = "classify_limit"
	// rejectionCloseLane is the single session-close lane of a project.
	rejectionCloseLane rejectionReason = "close_lane_busy"
	// rejectionQuarantine is a namespace isolated by an unresolved outcome.
	rejectionQuarantine rejectionReason = "quarantine"
	// rejectionMemoryBudget is the result materialization budget
	// (Quotas.MaxResultBytes, or the worker's own result budget).
	rejectionMemoryBudget rejectionReason = "memory_budget"
	// rejectionSQLBytes is the request and statement byte budget
	// (Quotas.MaxSQLBytes and the wire limit).
	rejectionSQLBytes rejectionReason = "sql_bytes"
	// rejectionDeadline is work the worker proved it turned away before execution
	// because an admission deadline had elapsed. A deadline after execution is a
	// settled failure, not a refusal, and is never reported as this reason.
	rejectionDeadline rejectionReason = "deadline"
	// rejectionOperationCapacity is the durable operation budget
	// (operationCapacity unresolved records).
	rejectionOperationCapacity rejectionReason = "operation_capacity"
	// rejectionOperationQueueFull is the bounded durable append queue refusing new
	// work before anything was written (operationJournalQueueMaximum).
	rejectionOperationQueueFull rejectionReason = "operation_queue_full"
)

// rejectionReasons is the closed vocabulary in publication order. A set that is
// not closed is not a vocabulary: consumers enumerate this list, so adding a
// reason is one edit here and one landing site, not a new string somewhere.
var rejectionReasons = []rejectionReason{
	rejectionSessionLimit,
	rejectionConcurrency,
	rejectionRate,
	rejectionWriterBusy,
	rejectionClassifyLimit,
	rejectionCloseLane,
	rejectionQuarantine,
	rejectionMemoryBudget,
	rejectionSQLBytes,
	rejectionDeadline,
	rejectionOperationCapacity,
	rejectionOperationQueueFull,
}

func rejectionReasonNames() []string {
	names := make([]string, 0, len(rejectionReasons))
	for _, reason := range rejectionReasons {
		names = append(names, string(reason))
	}
	return names
}

type projectAdmission struct {
	active      int
	ownerActive int
	ownerTokens float64
	profile     bool
	classifying int
	control     int
	sessions    int
	unknown     int
	tokens      float64
	last        time.Time
}

type gatewayAdmission struct {
	mu              sync.Mutex
	projects        map[string]*projectAdmission
	writerAdmission bool
}

type admissionPermit struct {
	gate        *gatewayAdmission
	project     string
	session     bool
	control     bool
	owner       bool
	keepSession bool
	retained    bool
	finished    bool
	// laneReleased records that a retained permit handed its concurrency lane
	// back while it keeps accounting for the unresolved outcome.
	laneReleased bool
}

func quotaValid(quota Quotas) bool {
	return quota.MaxSessions > 0 && quota.MaxConcurrent > 0 && quota.RequestsPerSecond > 0 && quota.MaxSQLBytes > 0 && quota.MaxResultBytes > 0
}

func (gate *gatewayAdmission) reserve(project string, quota Quotas, session, control bool) (*admissionPermit, rejectionReason) {
	return gate.reserveLane(project, quota, session, control, false)
}

func (gate *gatewayAdmission) reserveWriter(project string, quota Quotas, owner bool) (*admissionPermit, rejectionReason) {
	return gate.reserveLane(project, quota, false, false, owner)
}

func (gate *gatewayAdmission) eligible(quota Quotas) bool {
	return gate.writerAdmission && quota.MaxConcurrent >= 2 && quota.RequestsPerSecond >= 2
}

func (gate *gatewayAdmission) classify(project string, quota Quotas) (func(), rejectionReason) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	state := gate.projects[project]
	if state == nil {
		state = &projectAdmission{tokens: float64(quota.RequestsPerSecond), last: time.Now()}
		gate.projects[project] = state
	}
	if state.classifying >= min(quota.MaxConcurrent, 128)+2 {
		return nil, rejectionClassifyLimit
	}
	state.classifying++
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mu.Lock()
			state.classifying--
			gate.mu.Unlock()
		})
	}, rejectionNone
}

func (gate *gatewayAdmission) reserveLane(project string, quota Quotas, session, control, owner bool) (*admissionPermit, rejectionReason) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	state := gate.projects[project]
	if state == nil {
		state = &projectAdmission{tokens: float64(quota.RequestsPerSecond), last: time.Now()}
		gate.projects[project] = state
	}
	profile := gate.eligible(quota)
	if state.profile != profile {
		if profile {
			state.ownerTokens = min(1, state.tokens)
			state.tokens -= state.ownerTokens
		} else {
			state.tokens += state.ownerTokens
			state.ownerTokens = 0
		}
		state.profile = profile
	}
	if control {
		if state.control >= 1 {
			return nil, rejectionCloseLane
		}
		state.control++
	} else {
		if state.active >= quota.MaxConcurrent {
			return nil, rejectionConcurrency
		}
		if session && state.sessions >= quota.MaxSessions {
			return nil, rejectionSessionLimit
		}
		if profile {
			if owner && state.ownerActive >= 1 {
				return nil, rejectionWriterBusy
			}
			if !owner && state.active-state.ownerActive >= quota.MaxConcurrent-1 {
				return nil, rejectionConcurrency
			}
		}
		now := time.Now()
		elapsed := max(0, now.Sub(state.last).Seconds())
		state.last = now
		generalRate := float64(quota.RequestsPerSecond)
		if profile {
			generalRate--
			state.ownerTokens = min(1, state.ownerTokens+elapsed)
		}
		state.tokens = min(generalRate, state.tokens+elapsed*generalRate)
		credit := &state.tokens
		if profile && owner && state.tokens < 1 {
			credit = &state.ownerTokens
		}
		if *credit < 1 {
			return nil, rejectionRate
		}
		*credit--
		state.active++
		if owner {
			state.ownerActive++
		}
	}
	if session {
		state.sessions++
	}
	return &admissionPermit{gate: gate, project: project, session: session, control: control, owner: owner}, rejectionNone
}

func (permit *admissionPermit) retain() {
	if permit == nil {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	if permit.retained || permit.finished {
		return
	}
	permit.retained = true
	permit.gate.projects[permit.project].unknown++
}

func (permit *admissionPermit) finish() {
	if permit == nil {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	permit.finishLocked()
}

func (permit *admissionPermit) finishLocked() {
	if permit.retained || permit.finished {
		return
	}
	permit.finished = true
	state := permit.gate.projects[permit.project]
	if !permit.laneReleased {
		if permit.control {
			state.control--
		} else {
			state.active--
			if permit.owner {
				state.ownerActive--
			}
		}
	}
	if permit.session && !permit.keepSession {
		state.sessions--
	}
}

// releaseLane hands the concurrency lane of a retained permit back while the
// permit keeps accounting for the unresolved outcome. The control lane is the
// project's session-close lane, and an unresolved close is not running any more:
// holding that lane would block every later close of the project — the cleanup
// that resolves the unknown included — with no proof that anything is still
// running. The unknown itself, the session seat and the durable record stay, so
// nothing here releases an outcome without proof.
func (permit *admissionPermit) releaseLane() {
	if permit == nil {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	if !permit.retained || permit.laneReleased || permit.finished {
		return
	}
	permit.laneReleased = true
	state := permit.gate.projects[permit.project]
	if permit.control {
		state.control--
	} else {
		state.active--
		if permit.owner {
			state.ownerActive--
		}
	}
}

func (permit *admissionPermit) releaseRetained() {
	if permit == nil {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	if !permit.retained {
		return
	}
	permit.gate.projects[permit.project].unknown--
	permit.retained = false
	permit.finishLocked()
}

func (gate *gatewayAdmission) removeSession(project string) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if state := gate.projects[project]; state != nil && state.sessions > 0 {
		state.sessions--
	}
}

func (gate *gatewayAdmission) status() (map[string]any, int, int) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	projects := make(map[string]any, len(gate.projects))
	active, unknown := 0, 0
	for project, state := range gate.projects {
		projects[project] = map[string]any{"active": state.active, "control_active": state.control, "sessions": state.sessions, "unknown_held": state.unknown, "observed_rate_tokens": state.tokens, "writer_admission": state.profile, "owner_active": state.ownerActive, "owner_rate_tokens": state.ownerTokens, "classifying": state.classifying}
		active += state.active + state.control
		unknown += state.unknown
	}
	return projects, active, unknown
}
