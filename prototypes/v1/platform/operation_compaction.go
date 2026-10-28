package platform

import (
	"encoding/json"
	"errors"
	"maps"
	"time"
)

// A v1 snapshot keeps the journal writer exclusive, but its file and directory
// barriers must not hold the shared registry lock. The gate prevents a new
// optimistic operation edit while that writer is frozen: a reader would otherwise
// settle that edit under the registry lock and inherit the same slow barrier.
// Both mutation entry and post-append settlement use the gate, so an older waiter
// cannot start a second compaction or miss the first one's failure.
func (store *stateStore) lockOperationMutationState() func() {
	return store.lockOperationMutationStateObserved(nil)
}

func (store *stateStore) lockOperationMutationStateObserved(preparation *durabilityPreparation) func() {
	for {
		var started time.Time
		if preparation != nil {
			started = time.Now()
		}
		unlock := store.lockState()
		if preparation != nil {
			preparation.timings.StateLockNS += uint64(time.Since(started))
		}
		if store.operationCompacting == nil {
			return unlock
		}
		finished := store.operationCompacting
		unlock()
		if preparation != nil {
			started = time.Now()
		}
		<-finished
		if preparation != nil {
			preparation.timings.CompactionGateNS += uint64(time.Since(started))
		}
	}
}

type operationCompaction struct {
	observation *durabilityMaintenance
	snapshot    operationSnapshot
	origin      string
	started     time.Time
}

// The caller owns the state lock and has already settled its operation view.
// The returned task owns journal.write until finishOperationCompaction, while
// its immutable snapshot permits registry and operation reads to keep progressing.
func (store *stateStore) beginOperationCompactionLocked() (*operationCompaction, error) {
	journal := store.operations
	if store.maintenanceClosing || store.checkpointed || len(store.unconfirmed) != 0 || !journal.compactionDue() {
		return nil, nil
	}
	journal.write.Lock()
	journal.mutex.Lock()
	if err := journal.available(); err != nil {
		journal.mutex.Unlock()
		journal.write.Unlock()
		return nil, err
	}
	if journal.completion != nil || journal.writing || len(journal.commits) != 0 {
		journal.mutex.Unlock()
		journal.write.Unlock()
		return nil, nil
	}
	compaction := journal.captureCompaction(store.state)
	journal.mutex.Unlock()
	store.operationCompacting = make(chan struct{})
	store.maintenance.Add(1)
	return compaction, nil
}

// The triggering request still waits for the same durability work. Only lock
// ownership changes: no queue or state lock is held during encoding or I/O.
func (store *stateStore) finishOperationCompaction(compaction *operationCompaction) error {
	journal := store.operations
	timings, err := journal.persistCompaction(compaction)
	journal.mutex.Lock()
	journal.recordCompactionCost(compaction.started, timings, err)
	if err == nil {
		journal.installCompaction(compaction)
	} else {
		journal.poisoned = true
	}
	journal.mutex.Unlock()
	journal.write.Unlock()
	unlock := store.lockState()
	if err != nil {
		store.disk.poisoned = true
		store.operationCompactionFailure = err
	}
	close(store.operationCompacting)
	store.operationCompacting = nil
	poisoned := store.disk.poisoned
	unlock()
	store.maintenance.Done()
	if err == nil && poisoned {
		return ErrStateUncertain
	}
	return err
}

// The caller holds the journal metadata lock, or owns the journal during open.
// OperationRecord contains values only; a detached map therefore also detaches
// the snapshot from a registry update replacing the live view.
func (journal *operationJournal) captureCompaction(view State) *operationCompaction {
	compaction := &operationCompaction{
		started: time.Now(), origin: operationSnapshotOriginNative,
		snapshot: operationSnapshot{
			Version: operationJournalVersion, BaseRevision: view.Revision,
			Revision: journal.revision, Digest: journal.digest,
			Sequence: view.OperationSequence, Evictions: view.OperationEvictions,
			Records: maps.Clone(view.Operations),
		},
	}
	if journal.completion != nil {
		compaction.snapshot.Version = completionJournalVersion
		head := journal.auditHead
		compaction.snapshot.CompletionAudit = &head
	}
	// Re-materializing a legacy-accepted view must preserve its provenance.
	if journal.snapshot != nil && journal.snapshot.Origin == operationSnapshotOriginReseal {
		compaction.origin = operationSnapshotOriginReseal
	}
	if observer := journal.observation.Load(); observer != nil {
		identity := observer.nextID()
		journal.observationMaintenance = identity
		mode := "legacy_v1"
		if journal.completion != nil {
			mode = "completion_v2"
		}
		compaction.observation = &durabilityMaintenance{observer: observer, started: compaction.started,
			event: durabilityEvent{Kind: "journal_compaction", Mode: mode, ID: identity, Revision: journal.revision, Owner: "background", Count: len(compaction.snapshot.Records),
				Timings: durabilityTimings{CaptureNS: uint64(time.Since(compaction.started))}}}
	}
	return compaction
}

type operationCompactionTimings struct {
	snapshot uint64
	sync     uint64
	truncate uint64
}

// This method changes only durable files and the detached snapshot. The same
// snapshot -> witness sync -> truncate order is used for v1, v2 and recovery.
func (journal *operationJournal) persistCompaction(compaction *operationCompaction) (timings operationCompactionTimings, failure error) {
	if compaction.observation != nil {
		defer func() { compaction.observation.finish(failure) }()
	}
	if err := journal.compactionGate("before_snapshot"); err != nil {
		return timings, err
	}
	started := time.Now()
	if err := sealSnapshot(&compaction.snapshot, compaction.origin); err != nil {
		return timings, err
	}
	encoded, err := json.Marshal(compaction.snapshot)
	if err != nil || len(encoded) > operationJournalMaximumBytes {
		return timings, errors.New("invalid operation snapshot")
	}
	var persistStarted time.Time
	if compaction.observation != nil {
		compaction.observation.event.Timings.EncodeNS = uint64(time.Since(started))
		compaction.observation.event.Bytes = len(encoded)
		persistStarted = time.Now()
	}
	err = atomicPrivate(journal.snapshotPath(), encoded)
	timings.snapshot = uint64(time.Since(started))
	if compaction.observation != nil {
		compaction.observation.event.Timings.SnapshotNS = uint64(time.Since(persistStarted))
	}
	if err != nil {
		return timings, err
	}
	if err := journal.compactionGate("after_snapshot"); err != nil {
		return timings, err
	}
	started = time.Now()
	err = journal.file.Sync()
	timings.sync = uint64(time.Since(started))
	if compaction.observation != nil {
		compaction.observation.event.Timings.SyncNS = timings.sync
	}
	if err != nil {
		return timings, err
	}
	if err := journal.compactionGate("before_truncate"); err != nil {
		return timings, err
	}
	started = time.Now()
	err = journal.file.Truncate(operationJournalWitnessBytes)
	timings.truncate = uint64(time.Since(started))
	if compaction.observation != nil {
		compaction.observation.event.Timings.TruncateNS = timings.truncate
	}
	return timings, err
}

func (journal *operationJournal) compactionGate(phase string) error {
	if journal.testCompactionGate != nil {
		return journal.testCompactionGate(phase)
	}
	return nil
}

func (journal *operationJournal) installCompaction(compaction *operationCompaction) {
	if journal.completion != nil {
		journal.retainedBytes -= journal.size - operationJournalWitnessBytes
	}
	journal.size = operationJournalWitnessBytes
	journal.records = nil
	journal.appends = 0
	journal.snapshot = &compaction.snapshot
	journal.compactions++
}

func (journal *operationJournal) recordCompactionCost(started time.Time, timings operationCompactionTimings, err error) {
	elapsed := uint64(time.Since(started))
	journal.compactionNS += elapsed
	journal.compactionMaxNS = max(journal.compactionMaxNS, elapsed)
	journal.compactionSnapshotNS += timings.snapshot
	journal.compactionSyncNS += timings.sync
	journal.compactionTruncateNS += timings.truncate
	if err != nil {
		journal.compactionFailures++
	}
}
