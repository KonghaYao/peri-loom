package platform

import (
	"log"
	"time"
)

// A batch at the witness bound already has the same durable append that let
// its callers return before this maintenance existed. Start the head update
// after notifying those callers, instead of charging it to the next request.
// The existing drain remains the only writer: the next batch cannot append
// until this returns, and Close joins the maintenance through writers.Wait.
func (audit *fileAudit) checkpointAfterBatch() {
	if audit.completionStore != nil {
		return
	}
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	audit.head.mutex.Lock()
	defer audit.head.mutex.Unlock()
	if audit.closed || audit.head.available() != nil || audit.state.Sequence <= audit.head.revision || audit.state.Sequence-audit.head.revision < auditBatchMaximum {
		return
	}
	if gate := audit.testCheckpointGate; gate != nil {
		gate()
	}
	if err := audit.commitHead("post_batch"); err != nil {
		// commitAuditBatch poisons the head. No later append can pass its
		// availability check, and Close reports the failed checkpoint too.
		log.Printf("audit head checkpoint failed; subsequent records refused")
	}
}

// The caller owns the audit writer and head state. Count all head updates,
// including the original pre-batch guard and final shutdown checkpoint, so
// diagnostics do not hide maintenance by moving it off a request's span.
func (audit *fileAudit) commitHead(mode string) error {
	audit.headCheckpointInProgress.Store(true)
	defer audit.headCheckpointInProgress.Store(false)
	started := time.Now()
	err := audit.head.commitAuditBatch(audit.state.Sequence, audit.state)
	audit.headCheckpointNS.Add(uint64(time.Since(started)))
	audit.headCheckpointCount.Add(1)
	if err != nil {
		audit.headCheckpointFailures.Add(1)
	}
	if observer := audit.observation.Load(); observer != nil {
		identity := observer.nextID()
		audit.observationMaintenance = identity
		now := time.Now()
		observer.record(durabilityEvent{Kind: "audit_checkpoint", Mode: mode, ID: identity, BatchID: audit.observationBatch,
			Revision: audit.state.Sequence, Owner: "background", Outcome: durabilityOutcome(err),
			StartedUnixNS: started.UnixNano(), FinishedUnixNS: now.UnixNano(), TotalNS: uint64(now.Sub(started))})
	}
	return err
}
