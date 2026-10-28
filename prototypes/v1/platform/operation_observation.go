package platform

import (
	"time"

	controller "peri-loom/controller"
)

// Caller identity is supplied by the live Gateway call, never inferred from a
// historical record changed by close/retirement. It is in-memory diagnostic
// metadata only and exists only while the durability observer is enabled.
type durabilityCaller struct {
	operation, request, phase           string
	namespaceStarted, namespaceFinished time.Time
}

func newDurabilityCaller(store StateStore, operation, request, phase, source string) *durabilityCaller {
	durable, ok := store.(*stateStore)
	if !ok || durable == nil || durable.operations == nil || durable.operations.observation.Load() == nil || source != "gateway" || durabilityRequestMember(0, 0, request).Owner != "request" {
		return nil
	}
	return &durabilityCaller{operation: operation, request: request, phase: phase}
}

func lockObservedOperationNamespace(namespace controller.Namespace, caller *durabilityCaller) func() {
	if caller == nil {
		return lockOperationNamespace(namespace)
	}
	caller.namespaceStarted = time.Now()
	unlock := lockOperationNamespace(namespace)
	caller.namespaceFinished = time.Now()
	return unlock
}

// These durations partition only the prepare interval, from store mutation
// entry until enqueue. They must not be added to a request-phase intent/terminal
// span, which contains this interval. The namespace event is separate because
// it ends before store mutation entry. Existing member wait encode_ns is an
// independent historical counter, not another disjoint interval to sum here.
type durabilityPrepareTimings struct {
	StateLockNS      uint64 `json:"state_lock_ns"`
	CompactionGateNS uint64 `json:"compaction_gate_ns"`
	StateGateOtherNS uint64 `json:"state_gate_other_ns"`
	EntryChecksNS    uint64 `json:"entry_checks_ns"`
	EditSetupNS      uint64 `json:"edit_setup_ns"`
	EditNS           uint64 `json:"edit_ns"`
	RetentionNS      uint64 `json:"retention_ns"`
	ValidateNS       uint64 `json:"validate_ns"`
	DeltaNS          uint64 `json:"delta_ns"`
	SubmitEntryNS    uint64 `json:"submit_entry_ns"`
	QueueLockNS      uint64 `json:"queue_lock_ns"`
	OrderNS          uint64 `json:"order_ns"`
	EncodeNS         uint64 `json:"encode_ns"`
	EnqueueOtherNS   uint64 `json:"enqueue_other_ns"`
}

type durabilityPreparation struct {
	callerVerified    bool
	observer          *durabilityObserver
	caller            *durabilityCaller
	started, previous time.Time
	timings           durabilityPrepareTimings
}

func (store *stateStore) observeOperationPreparation() *durabilityPreparation {
	if store.operations == nil {
		return nil
	}
	// operations is installed once by OpenStateStore before publication; Close
	// never replaces it. Observer attachment/detachment uses an atomic pointer.
	observer := store.operations.observation.Load()
	if observer == nil {
		return nil
	}
	now := time.Now()
	return &durabilityPreparation{observer: observer, started: now, previous: now}
}

func (preparation *durabilityPreparation) mark(phase string) {
	if preparation == nil {
		return
	}
	now := time.Now()
	duration := uint64(now.Sub(preparation.previous))
	preparation.previous = now
	switch phase {
	case "state_gate":
		preparation.timings.StateGateOtherNS = duration - preparation.timings.StateLockNS - preparation.timings.CompactionGateNS
	case "entry_checks":
		preparation.timings.EntryChecksNS = duration
	case "edit_setup":
		preparation.timings.EditSetupNS = duration
	case "edit":
		preparation.timings.EditNS = duration
	case "retention":
		preparation.timings.RetentionNS = duration
	case "validate":
		preparation.timings.ValidateNS = duration
	case "delta":
		preparation.timings.DeltaNS = duration
	case "submit_entry":
		preparation.timings.SubmitEntryNS = duration
	case "queue_lock":
		preparation.timings.QueueLockNS = duration
	case "order":
		preparation.timings.OrderNS = duration
	case "encode":
		preparation.timings.EncodeNS = duration
	}
}

func (preparation *durabilityPreparation) correlate(fallback durabilityMember, delta operationDelta, event *AuditEvent) durabilityMember {
	caller := preparation.caller
	if caller == nil {
		return fallback
	}
	// The primary operation must actually be part of this mutation, with the
	// matching current request and no administrative retirement/release owner.
	// Invalid metadata never changes the existing conservative fallback.
	var primary OperationRecord
	matches := 0
	for _, record := range delta.Set {
		if record.OperationID == caller.operation && record.RequestID == caller.request {
			primary = record
			matches++
		}
	}
	if matches != 1 || primary.Source != "gateway" || primary.DecisionRequestID != "" || primary.RetirementRequestID != "" || primary.ReleaseRequestID != "" {
		return fallback
	}
	valid := caller.phase == "intent" && primary.State == "in_flight" || (caller.phase == "terminal" || caller.phase == "completion") && (primary.State == "terminal" || primary.State == "quarantined")
	if !valid || event != nil && event.RequestID != caller.request {
		return fallback
	}
	member := durabilityRequestMember(fallback.ID, fallback.Revision, caller.request)
	if member.Owner != "request" {
		return fallback
	}
	preparation.callerVerified = true
	member.Correlation = "caller_verified"
	member.Phase = caller.phase
	if event != nil {
		member.Phase = "completion"
	}
	return member
}

func (preparation *durabilityPreparation) finish(pending *durabilityPending) {
	if preparation == nil || pending.observer != preparation.observer {
		return
	}
	preparation.timings.EnqueueOtherNS = uint64(pending.enqueued.Sub(preparation.previous))
	member := pending.member
	preparation.observer.record(durabilityEvent{Kind: "journal_prepare", ID: member.ID,
		Owner: member.Owner, Outcome: "enqueued", StartedUnixNS: preparation.started.UnixNano(), FinishedUnixNS: pending.enqueued.UnixNano(),
		TotalNS: uint64(pending.enqueued.Sub(preparation.started)), Preparation: &preparation.timings, Members: []durabilityMember{member}})
	caller := preparation.caller
	if caller != nil && preparation.callerVerified && member.Owner == "request" && member.RequestID == caller.request && !caller.namespaceFinished.IsZero() {
		preparation.observer.record(durabilityEvent{Kind: "journal_namespace_wait", ID: member.ID,
			Owner: member.Owner, Outcome: "acquired", StartedUnixNS: caller.namespaceStarted.UnixNano(), FinishedUnixNS: caller.namespaceFinished.UnixNano(),
			TotalNS: uint64(caller.namespaceFinished.Sub(caller.namespaceStarted)), Members: []durabilityMember{member}})
	}
}
