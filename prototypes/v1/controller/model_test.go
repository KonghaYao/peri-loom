package controller

import "testing"

// TestPlacementTransienceSeparatesConvergenceFromOperatorDecisions pins the
// answer a gateway waits on. Waiting is only truthful for a placement the
// controller will move by itself; an administrator's suspension, a durable
// failure and a plain suspend drain stay final until an operator acts.
func TestPlacementTransienceSeparatesConvergenceFromOperatorDecisions(test *testing.T) {
	for _, scenario := range []struct {
		name      string
		phase     string
		reason    string
		policy    string
		transient bool
	}{
		{name: "grant pending without wake policy", phase: "grant_pending", transient: true},
		{name: "grant intent without wake policy", phase: "grant_intent", transient: true},
		{name: "acquire pending without wake policy", phase: "acquire_pending", transient: true},
		{name: "migrate drain", phase: "migrate_drain", transient: true},
		{name: "idle drain", phase: "idle_drain", transient: true},
		{name: "idle auto wake", phase: "idle", reason: SuspendReasonIdle, policy: WakePolicyAuto, transient: true},
		{name: "idle manual wake", phase: "idle", reason: SuspendReasonIdle, policy: WakePolicyManual, transient: false},
		{name: "revoke pending auto wake", phase: "revoke_pending", reason: SuspendReasonIdle, policy: WakePolicyAuto, transient: true},
		{name: "revoke pending manual wake", phase: "revoke_pending", reason: SuspendReasonIdle, policy: WakePolicyManual, transient: false},
		{name: "administrator suspension", phase: "suspended", reason: SuspendReasonAdmin, policy: WakePolicyManual, transient: false},
		{name: "failed activation", phase: "suspended", reason: SuspendReasonFailure, policy: "", transient: false},
		{name: "suspend drain", phase: "suspend_drain", reason: SuspendReasonAdmin, policy: WakePolicyManual, transient: false},
		{name: "active placement", phase: "active", transient: false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			placement := Placement{Phase: scenario.phase, SuspendReason: scenario.reason, WakePolicy: scenario.policy}
			if transient := placement.Transient(); transient != scenario.transient {
				test.Fatalf("phase %q reason %q policy %q: Transient()=%t, want %t", scenario.phase, scenario.reason, scenario.policy, transient, scenario.transient)
			}
		})
	}
}
