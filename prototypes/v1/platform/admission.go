package platform

import (
	"sync"
	"time"
)

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
}

func quotaValid(quota Quotas) bool {
	return quota.MaxSessions > 0 && quota.MaxConcurrent > 0 && quota.RequestsPerSecond > 0 && quota.MaxSQLBytes > 0 && quota.MaxResultBytes > 0
}

func (gate *gatewayAdmission) reserve(project string, quota Quotas, session, control bool) (*admissionPermit, bool) {
	return gate.reserveLane(project, quota, session, control, false)
}

func (gate *gatewayAdmission) reserveWriter(project string, quota Quotas, owner bool) (*admissionPermit, bool) {
	return gate.reserveLane(project, quota, false, false, owner)
}

func (gate *gatewayAdmission) eligible(quota Quotas) bool {
	return gate.writerAdmission && quota.MaxConcurrent >= 2 && quota.RequestsPerSecond >= 2
}

func (gate *gatewayAdmission) classify(project string, quota Quotas) (func(), bool) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	state := gate.projects[project]
	if state == nil {
		state = &projectAdmission{tokens: float64(quota.RequestsPerSecond), last: time.Now()}
		gate.projects[project] = state
	}
	if state.classifying >= min(quota.MaxConcurrent, 128)+2 {
		return nil, false
	}
	state.classifying++
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mu.Lock()
			state.classifying--
			gate.mu.Unlock()
		})
	}, true
}

func (gate *gatewayAdmission) reserveLane(project string, quota Quotas, session, control, owner bool) (*admissionPermit, bool) {
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
			return nil, false
		}
		state.control++
	} else {
		if state.active >= quota.MaxConcurrent || (session && state.sessions >= quota.MaxSessions) {
			return nil, false
		}
		if profile && ((owner && state.ownerActive >= 1) || (!owner && state.active-state.ownerActive >= quota.MaxConcurrent-1)) {
			return nil, false
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
		if profile && owner {
			credit = &state.ownerTokens
		}
		if *credit < 1 {
			return nil, false
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
	return &admissionPermit{gate: gate, project: project, session: session, control: control, owner: owner}, true
}

func (permit *admissionPermit) retain() {
	if permit == nil || permit.retained {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	permit.retained = true
	permit.gate.projects[permit.project].unknown++
}

func (permit *admissionPermit) finish() {
	if permit == nil || permit.retained {
		return
	}
	permit.gate.mu.Lock()
	defer permit.gate.mu.Unlock()
	state := permit.gate.projects[permit.project]
	if permit.control {
		state.control--
	} else {
		state.active--
		if permit.owner {
			state.ownerActive--
		}
	}
	if permit.session && !permit.keepSession {
		state.sessions--
	}
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
