package platform

import (
	"encoding/json"
	"errors"
	"time"
)

// The operation log used to hold the shared state lock across its fsync, so every narrow
// read - including authentication - queued behind a flush it had nothing to do with. An
// ordered commit queue moves the barrier off that lock: appends are ordered here, the
// committer coalesces the appends already waiting when it picks up a batch, and each caller
// waits for its own durability instead of for a lock.
//
// Two locks keep that promise. The queue lock only covers bookkeeping, so a submitter never
// waits for a barrier; the write lock serializes the file itself and is held by the
// committer alone. Coalescing merges waiters and never delays them: the first submitter
// starts the committer immediately, so a lone request pays exactly one barrier. Both the
// queue and the batch are bounded, so a stalled device refuses new work instead of growing
// without limit.
const (
	operationJournalQueueMaximum = 256
	operationJournalBatchMaximum = 32
	operationJournalBatchBytes   = 1 << 20
)

// ErrOperationQueueFull is a refusal before anything was written: the caller must treat the
// operation as unregistered and must not dispatch SQL.
var ErrOperationQueueFull = errors.New("durable operation queue exhausted")

// operationCommit is one ordered append waiting for its barrier. Its chain position is
// fixed when it is queued, so the order the log records is the order submitters were
// serialized in, and the digest covers the bytes that are actually written.
type operationCommit struct {
	observation *durabilityPending
	revision    uint64
	payload     journalMutation
	line        []byte
	hash        string

	// settled closes once the barrier's result is known; err is written before it closes,
	// so a waiter that observes the close also observes the result.
	settled chan struct{}
	err     error

	// applied marks that a failed commit's in-place edit was rolled back. Only the side
	// holding the view lock writes it, so a rollback happens exactly once.
	applied bool
}

// submit orders one delta behind every append already queued and starts the committer. The
// caller holds the state lock for the view it passes, so the chain order and the view order
// are the same order. It never waits for the file: only the committer does.
func (journal *operationJournal) submit(view State, delta operationDelta) (*operationCommit, error) {
	return journal.submitAudit(view, delta, nil, false)
}

func (journal *operationJournal) submitAudit(view State, delta operationDelta, event *AuditEvent, reserved bool) (*operationCommit, error) {
	return journal.submitAuditPrepared(view, delta, event, reserved, nil)
}

func (journal *operationJournal) submitAuditPrepared(view State, delta operationDelta, event *AuditEvent, reserved bool, preparation *durabilityPreparation) (*operationCommit, error) {
	preparation.mark("submit_entry")
	journal.mutex.Lock()
	preparation.mark("queue_lock")
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return nil, err
	}
	queueMaximum := operationJournalQueueMaximum
	if reserved && journal.completion != nil {
		queueMaximum += completionReservedMaximum
	}
	if len(journal.commits) >= queueMaximum {
		return nil, ErrOperationQueueFull
	}
	// The chain position is the last append admitted, which stays fixed even after its
	// batch was popped: until that batch's barrier returns, journal.revision/digest still
	// name the older durable head it replaces. Falling back to them here would let a new
	// append reuse the in-flight revision and fork the chain.
	previous, revision := journal.digest, journal.revision
	if journal.orderedTail != nil {
		previous, revision = journal.orderedTail.hash, journal.orderedTail.revision
	}
	if revision == ^uint64(0) {
		return nil, ErrOperationCapacity
	}
	mutation := journalMutation{Version: operationJournalVersion, Revision: revision + 1, BaseRevision: view.Revision, Previous: previous, Sequence: view.OperationSequence, Evictions: view.OperationEvictions, Set: delta.Set, Remove: delta.Remove}
	if journal.completion != nil {
		mutation.Version = completionJournalVersion
	}
	preparation.mark("order")
	if event != nil {
		if journal.completion == nil || validateAudit(*event) != nil || journal.orderedAudit.Sequence == ^uint64(0) {
			return nil, errors.New("invalid completion audit")
		}
		payload := auditPayload{Version: 1, Sequence: journal.orderedAudit.Sequence + 1, Previous: journal.orderedAudit.Hash, Event: *event}
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		mutation.Audit = &auditRecord{Payload: payload, Hash: digestHex(body)}
	}
	encodeStarted := time.Now()
	encoded, err := json.Marshal(mutation)
	if err != nil || len(encoded) > operationJournalMaximumRecordBytes {
		return nil, errors.New("invalid operation mutation")
	}
	hash := digestHex(encoded)
	line, err := json.Marshal(journalEntry{Payload: mutation, Hash: hash})
	if err != nil {
		return nil, errors.New("invalid operation mutation")
	}
	preparation.mark("encode")
	if journal.completion != nil {
		maximum := completionRetentionSoftBytes
		activeMaximum := int64(operationJournalMaximumBytes)
		if reserved {
			maximum = completionRetentionMaximumBytes
			activeMaximum = completionActiveMaximumBytes
		}
		queueBytes := int64(completionQueueMaximumBytes)
		if reserved {
			queueBytes += completionReservedMaximum * completionRecordReservationBytes
		}
		if journal.queuedBytes+int64(len(line)+1) > queueBytes || journal.retainedBytes+journal.queuedBytes+int64(len(line)+1) > maximum || journal.size+journal.queuedBytes+int64(len(line)+1) > activeMaximum {
			return nil, ErrOperationQueueFull
		}
	}
	encodeNS := uint64(time.Since(encodeStarted))
	journal.encodeNS += encodeNS
	commit := &operationCommit{revision: mutation.Revision, payload: mutation, line: append(line, '\n'), hash: hash, settled: make(chan struct{})}
	if observer := journal.observation.Load(); observer != nil {
		member := durabilityOperationMember(observer.nextID(), mutation.Revision, delta, event)
		if preparation != nil {
			member = preparation.correlate(member, delta, event)
		}
		commit.observation = &durabilityPending{observer: observer, member: member, enqueued: time.Now(), encodeNS: encodeNS}
		preparation.finish(commit.observation)
	}
	journal.commits = append(journal.commits, commit)
	journal.queuedBytes += int64(len(commit.line))
	if mutation.Audit != nil {
		journal.orderedAudit = auditHead{Version: 1, Sequence: mutation.Audit.Payload.Sequence, Hash: mutation.Audit.Hash}
	}
	journal.orderedTail = commit
	journal.ordered++
	if len(journal.commits) > journal.queueMaximum {
		journal.queueMaximum = len(journal.commits)
	}
	if !journal.committer {
		journal.committer = true
		journal.committerGroup.Add(1)
		go journal.flush()
	}
	return commit, nil
}

// await waits for one commit's barrier without holding any lock the committer needs, so a
// caller's own durability is never paid for by unrelated readers.
func (journal *operationJournal) await(commit *operationCommit) error {
	if commit == nil {
		return errors.New("operation commit missing")
	}
	started := time.Now()
	journal.waitObserved(commit)
	journal.observeWait(time.Since(started))
	return commit.err
}

func (journal *operationJournal) observeWait(elapsed time.Duration) {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	journal.waitNS += uint64(elapsed)
	if uint64(elapsed) > journal.waitMaxNS {
		journal.waitMaxNS = uint64(elapsed)
	}
}

// flush writes queued appends in order and pays one barrier per batch. It never takes the
// state lock: a reader waiting for a commit's result holds that lock, and the committer must
// not need it to finish. A failed batch poisons the log and is reported to every waiter in
// it, so no waiter can be told "durable" by a barrier that failed.
func (journal *operationJournal) flush() {
	defer journal.committerGroup.Done()
	for {
		journal.mutex.Lock()
		if len(journal.commits) == 0 {
			journal.committer = false
			journal.mutex.Unlock()
			return
		}
		journal.mutex.Unlock()
		if journal.testPopGate != nil {
			// Held before the batch is chosen, so a test can let appends pile up first.
			journal.testPopGate()
		}
		observer := journal.observation.Load()
		var observedStart time.Time
		if observer != nil {
			observedStart = time.Now()
		}
		journal.write.Lock()
		var lockNS uint64
		if observer != nil {
			lockNS = uint64(time.Since(observedStart))
		}
		journal.mutex.Lock()
		if len(journal.commits) == 0 {
			journal.mutex.Unlock()
			journal.write.Unlock()
			continue
		}
		batch := journal.takeBatchLocked()
		var observedBatch uint64
		var observedMembers []durabilityMember
		previousMaintenance := journal.observationMaintenance
		if observer != nil {
			observedBatch = observer.nextID()
			selected := time.Now()
			observedMembers = make([]durabilityMember, 0, len(batch))
			for _, commit := range batch {
				if commit.observation != nil {
					commit.observation.batch, commit.observation.selected = observedBatch, selected
					observedMembers = append(observedMembers, commit.observation.member)
				}
			}
		}
		journal.writing = true
		offset := journal.size
		failure := journal.available()
		journal.mutex.Unlock()
		err := failure
		var timings operationWriteTimings
		if err == nil {
			timings, err = journal.writeBatchAt(batch, offset)
		}
		journal.mutex.Lock()
		journal.verifyNS += timings.verify
		journal.assembleNS += timings.assemble
		journal.writeNS += timings.write
		journal.syncNS += timings.sync
		journal.witnessNS += timings.witness
		journal.writing = false
		for _, commit := range batch {
			journal.queuedBytes -= int64(len(commit.line))
		}
		if err == nil {
			journal.recordBatchLocked(batch)
		} else if !errors.Is(err, ErrStateClosed) {
			journal.poisoned = true
		}
		journal.mutex.Unlock()
		journal.write.Unlock()
		if observer != nil {
			now := time.Now()
			observer.record(durabilityEvent{Kind: "journal_batch", ID: observedBatch, Owner: durabilityBatchOwner(observedMembers),
				PreviousMaintenance: previousMaintenance, Outcome: durabilityOutcome(err), Count: len(batch),
				StartedUnixNS: observedStart.UnixNano(), FinishedUnixNS: now.UnixNano(), TotalNS: uint64(now.Sub(observedStart)),
				Timings: durabilityTimings{LockNS: lockNS, VerifyNS: timings.verify, AssembleNS: timings.assemble, WriteNS: timings.write, SyncNS: timings.sync, WitnessNS: timings.witness}, Members: observedMembers})
		}
		for _, commit := range batch {
			commit.err = err
			close(commit.settled)
		}
	}
}

// takeBatchLocked pops the waiting appends a single barrier will cover.
func (journal *operationJournal) takeBatchLocked() []*operationCommit {
	count, bytes := 0, 0
	for count < len(journal.commits) && count < operationJournalBatchMaximum {
		next := len(journal.commits[count].line)
		if count > 0 && bytes+next > operationJournalBatchBytes {
			break
		}
		bytes += next
		count++
	}
	batch := append([]*operationCommit(nil), journal.commits[:count]...)
	clear(journal.commits[:count])
	journal.commits = journal.commits[count:]
	if count > journal.batchMaximum {
		journal.batchMaximum = count
	}
	return batch
}

// writeBatchAt writes the batch as one run of bytes at the offset the queue reserved for it
// and pays one barrier for all of them. The caller holds the write lock and has already
// reserved that offset, so no other writer can be interleaved.
// operationWriteTimings separates the costs one barrier batch pays. The counters are
// accumulated under the queue lock by the committer, so a reader can tell an encode-heavy
// batch from a slow device without adding timing to the request path.
type operationWriteTimings struct {
	verify   uint64
	assemble uint64
	write    uint64
	sync     uint64
	witness  uint64
}

func (journal *operationJournal) writeBatchAt(batch []*operationCommit, offset int64) (operationWriteTimings, error) {
	var timings operationWriteTimings
	if journal.testWriteGate != nil {
		journal.testWriteGate()
	}
	started := time.Now()
	if err := journal.verifyPathIdentity(); err != nil {
		return timings, ErrStateUncertain
	}
	timings.verify = uint64(time.Since(started))
	// Size the run to the batch's actual bytes. Reserving the batch ceiling would charge
	// every commit, including a lone short terminal record, a 1 MiB allocation.
	started = time.Now()
	total := 0
	for _, commit := range batch {
		total += len(commit.line)
	}
	run := make([]byte, 0, total)
	for _, commit := range batch {
		run = append(run, commit.line...)
	}
	timings.assemble = uint64(time.Since(started))
	started = time.Now()
	if _, err := journal.file.WriteAt(run, offset); err != nil {
		timings.write = uint64(time.Since(started))
		return timings, ErrStateUncertain
	}
	timings.write = uint64(time.Since(started))
	if journal.testSyncGate != nil {
		journal.testSyncGate()
	}
	started = time.Now()
	if err := journal.file.Sync(); err != nil {
		timings.sync = uint64(time.Since(started))
		return timings, ErrStateUncertain
	}
	timings.sync = uint64(time.Since(started))
	started = time.Now()
	err := journal.writeWitnessFor(batch[len(batch)-1])
	timings.witness = uint64(time.Since(started))
	return timings, err
}

// recordBatchLocked advances the chain state a written batch owns. The caller holds the
// queue lock and the write lock, so the revision it records is the revision on disk.
func (journal *operationJournal) recordBatchLocked(batch []*operationCommit) {
	journal.syncs++
	journal.coalesced += uint64(len(batch) - 1)
	for _, commit := range batch {
		journal.size += int64(len(commit.line))
		journal.retainedBytes += int64(len(commit.line))
		journal.revision = commit.revision
		journal.digest = commit.hash
		journal.records = append(journal.records, journalEntry{Payload: commit.payload, Hash: commit.hash})
		if commit.payload.Audit != nil {
			journal.auditHead = auditHead{Version: 1, Sequence: commit.payload.Audit.Payload.Sequence, Hash: commit.payload.Audit.Hash}
		}
	}
	journal.appends += uint64(len(batch))
	journal.appendsTotal += uint64(len(batch))
}

// compactionDue reports whether the log passed its append budget and should be materialized.
// The materialization needs the confirmed view, so it is driven by the store, not the
// committer.
func (journal *operationJournal) compactionDue() bool {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.closed || journal.poisoned {
		return false
	}
	return journal.appends >= operationJournalCompactRecords || journal.size >= operationJournalMaximumBytes
}

func (journal *operationJournal) poison() {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	journal.poisoned = true
}

// compactConfirmed materializes the view the caller confirmed. It proceeds only while the
// log is quiescent: with a queued or in-flight append, the caller's view would already be
// ahead of the revision the snapshot claims, and truncating the log would drop the
// difference between them.
func (journal *operationJournal) compactConfirmed(view State) error {
	journal.write.Lock()
	defer journal.write.Unlock()
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return err
	}
	if len(journal.commits) > 0 || journal.writing {
		return nil
	}
	return journal.compact(view)
}
