package platform

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This observer owns no durability decision. It receives bounded, value-only
// diagnostics after the existing boundaries; no durable record contains it.
const durabilityObservationCapacity = 4096

type durabilityTimings struct {
	QueueNS      uint64 `json:"queue_ns,omitempty"`
	WaitNS       uint64 `json:"wait_ns,omitempty"`
	LockNS       uint64 `json:"write_lock_ns,omitempty"`
	EncodeNS     uint64 `json:"encode_ns,omitempty"`
	VerifyNS     uint64 `json:"verify_ns,omitempty"`
	AssembleNS   uint64 `json:"assemble_ns,omitempty"`
	WriteNS      uint64 `json:"write_ns,omitempty"`
	SyncNS       uint64 `json:"sync_ns,omitempty"`
	WitnessNS    uint64 `json:"witness_ns,omitempty"`
	CheckpointNS uint64 `json:"checkpoint_ns,omitempty"`
	RotationNS   uint64 `json:"rotation_ns,omitempty"`
	CaptureNS    uint64 `json:"capture_ns,omitempty"`
	SnapshotNS   uint64 `json:"snapshot_persist_ns,omitempty"`
	TruncateNS   uint64 `json:"truncate_ns,omitempty"`
}

type durabilityMember struct {
	Correlation string `json:"correlation,omitempty"`
	ID          uint64 `json:"member_id"`
	Revision    uint64 `json:"revision,omitempty"`
	Sequence    uint64 `json:"audit_sequence,omitempty"`
	Owner       string `json:"owner"`
	Phase       string `json:"phase,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
}

type durabilityEvent struct {
	Schema              string                    `json:"schema"`
	Stream              string                    `json:"stream"`
	EventSequence       uint64                    `json:"event_sequence"`
	Kind                string                    `json:"kind"`
	Mode                string                    `json:"mode,omitempty"`
	ID                  uint64                    `json:"id"`
	BatchID             uint64                    `json:"batch_id,omitempty"`
	PreviousMaintenance uint64                    `json:"previous_maintenance_id,omitempty"`
	Revision            uint64                    `json:"revision,omitempty"`
	Count               int                       `json:"count,omitempty"`
	Bytes               int                       `json:"bytes,omitempty"`
	Owner               string                    `json:"owner"`
	Outcome             string                    `json:"outcome"`
	StartedUnixNS       int64                     `json:"started_unix_ns"`
	FinishedUnixNS      int64                     `json:"finished_unix_ns"`
	TotalNS             uint64                    `json:"total_ns"`
	Timings             durabilityTimings         `json:"timings"`
	Preparation         *durabilityPrepareTimings `json:"preparation,omitempty"`
	Members             []durabilityMember        `json:"members,omitempty"`
}

type durabilityObserver struct {
	ids      atomic.Uint64
	mutex    sync.Mutex
	queue    chan durabilityEvent
	sealed   bool
	produced uint64
	dropped  uint64
}

func newDurabilityObserver(capacity int) *durabilityObserver {
	return &durabilityObserver{queue: make(chan durabilityEvent, capacity)}
}

func (observer *durabilityObserver) nextID() uint64 { return observer.ids.Add(1) }

func (observer *durabilityObserver) record(event durabilityEvent) {
	if observer == nil {
		return
	}
	// This lock only arbitrates a nonblocking channel send and sealing. There
	// is no sink callback, encoding or file I/O under any producer's lock.
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.produced++
	event.EventSequence = observer.produced
	if observer.sealed {
		observer.dropped++
		return
	}
	select {
	case observer.queue <- event:
	default:
		observer.dropped++
	}
}

func (observer *durabilityObserver) seal() (produced, dropped uint64) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.sealed = true
	return observer.produced, observer.dropped
}

func (observer *durabilityObserver) counts() (produced, dropped uint64) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	return observer.produced, observer.dropped
}

func durabilityOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func durabilityRequestMember(id, revision uint64, request string) durabilityMember {
	member := durabilityMember{ID: id, Revision: revision, Owner: "background"}
	if sqlLabel(request) && !strings.Contains(request, "plk1_") && !strings.Contains(request, "pldb1.") {
		member.RequestID, member.Owner = request, "request"
	}
	return member
}

// A delta may settle several prior operations. Their original request IDs do
// not identify the administrative caller, so do not guess one unique owner.
func durabilityOperationMember(id, revision uint64, delta operationDelta, event *AuditEvent) durabilityMember {
	if event != nil {
		member := durabilityRequestMember(id, revision, event.RequestID)
		member.Phase = "completion"
		return member
	}
	member := durabilityRequestMember(id, revision, "")
	if len(delta.Set) != 1 {
		if len(delta.Set) > 1 {
			member.Owner = "multiple"
		}
		return member
	}
	for _, record := range delta.Set {
		// Only ordinary Gateway registration/settlement has the stored request
		// as its caller. Retirement/release/recovery refer to historical owners.
		if record.Source == "gateway" && record.DecisionRequestID == "" && record.RetirementRequestID == "" && record.ReleaseRequestID == "" && (record.State == "in_flight" || record.State == "terminal") {
			member = durabilityRequestMember(id, revision, record.RequestID)
			member.Phase = "intent"
			if record.State == "terminal" {
				member.Phase = "terminal"
			}
		}
	}
	return member
}

type durabilityPending struct {
	observer *durabilityObserver
	member   durabilityMember
	enqueued time.Time
	selected time.Time
	batch    uint64
	encodeNS uint64
}

func (pending *durabilityPending) finished(kind string, waitStarted time.Time, err error) {
	if pending == nil {
		return
	}
	now := time.Now()
	timings := durabilityTimings{WaitNS: uint64(now.Sub(waitStarted)), EncodeNS: pending.encodeNS}
	if !pending.selected.IsZero() {
		timings.QueueNS = uint64(pending.selected.Sub(pending.enqueued))
	}
	pending.observer.record(durabilityEvent{Kind: kind, ID: pending.member.ID, BatchID: pending.batch,
		Owner: pending.member.Owner, Outcome: durabilityOutcome(err), StartedUnixNS: pending.enqueued.UnixNano(),
		FinishedUnixNS: now.UnixNano(), TotalNS: uint64(now.Sub(pending.enqueued)), Timings: timings,
		Members: []durabilityMember{pending.member}})
}

func (journal *operationJournal) waitObserved(commit *operationCommit) {
	if commit.observation == nil {
		<-commit.settled
		return
	}
	started := time.Now()
	<-commit.settled
	commit.observation.finished("journal_member_wait", started, commit.err)
}

func (audit *fileAudit) waitObserved(request *auditRequest) error {
	if request.observation == nil {
		return <-request.done
	}
	started := time.Now()
	err := <-request.done
	request.observation.finished("audit_member_wait", started, err)
	return err
}

func durabilityBatchOwner(members []durabilityMember) string {
	if len(members) == 0 {
		return "background"
	}
	if len(members) == 1 {
		return members[0].Owner
	}
	return "multiple"
}

type durabilityMaintenance struct {
	observer *durabilityObserver
	event    durabilityEvent
	started  time.Time
}

func (maintenance *durabilityMaintenance) finish(err error) {
	if maintenance == nil {
		return
	}
	now := time.Now()
	maintenance.event.StartedUnixNS = maintenance.started.UnixNano()
	maintenance.event.FinishedUnixNS = now.UnixNano()
	maintenance.event.TotalNS = uint64(now.Sub(maintenance.started))
	maintenance.event.Outcome = durabilityOutcome(err)
	maintenance.observer.record(maintenance.event)
}

func (audit *fileAudit) observeBatch(batch []*auditRequest) *durabilityMaintenance {
	observer := audit.observation.Load()
	if observer == nil {
		return nil
	}
	observation := &durabilityMaintenance{observer: observer, started: time.Now(), event: durabilityEvent{Kind: "audit_batch", ID: observer.nextID(), Count: len(batch), Owner: "multiple", Mode: "legacy_v1"}}
	if audit.completionStore != nil {
		observation.event.Mode = "completion_v2_delegated"
	}
	for _, request := range batch[:min(len(batch), auditBatchMaximum)] {
		if request.observation != nil {
			request.observation.batch = observation.event.ID
		}
	}
	return observation
}
