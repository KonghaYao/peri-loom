package platform

import (
	"sync"
	"time"
)

type projectAdmission struct {
	active   int
	control  int
	sessions int
	unknown  int
	tokens   float64
	last     time.Time
}

type gatewayAdmission struct {
	mu       sync.Mutex
	projects map[string]*projectAdmission
}

type admissionPermit struct {
	gate        *gatewayAdmission
	project     string
	session     bool
	control     bool
	keepSession bool
	retained    bool
}

func quotaValid(quota Quotas) bool {
	return quota.MaxSessions > 0 && quota.MaxConcurrent > 0 && quota.RequestsPerSecond > 0 && quota.MaxSQLBytes > 0 && quota.MaxResultBytes > 0
}

func (gate *gatewayAdmission) reserve(project string, quota Quotas, session, control bool) (*admissionPermit, bool) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	state := gate.projects[project]
	if state == nil {
		state = &projectAdmission{tokens: float64(quota.RequestsPerSecond), last: time.Now()}
		gate.projects[project] = state
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
		now := time.Now()
		state.tokens += now.Sub(state.last).Seconds() * float64(quota.RequestsPerSecond)
		state.last = now
		if state.tokens > float64(quota.RequestsPerSecond) {
			state.tokens = float64(quota.RequestsPerSecond)
		}
		if state.tokens < 1 {
			return nil, false
		}
		state.tokens--
		state.active++
	}
	if session {
		state.sessions++
	}
	return &admissionPermit{gate: gate, project: project, session: session, control: control}, true
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
		projects[project] = map[string]any{"active": state.active, "control_active": state.control, "sessions": state.sessions, "unknown_held": state.unknown, "observed_rate_tokens": state.tokens}
		active += state.active + state.control
		unknown += state.unknown
	}
	return projects, active, unknown
}
