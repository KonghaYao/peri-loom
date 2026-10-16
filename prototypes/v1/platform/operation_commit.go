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
	revision uint64
	payload  journalMutation
	line     []byte
	hash     string

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
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return nil, err
	}
	if len(journal.commits) >= operationJournalQueueMaximum {
		return nil, ErrOperationQueueFull
	}
	previous, revision := journal.digest, journal.revision
	if count := len(journal.commits); count > 0 {
		last := journal.commits[count-1]
		previous, revision = last.hash, last.revision
	}
	if revision == ^uint64(0) {
		return nil, ErrOperationCapacity
	}
	mutation := journalMutation{Version: operationJournalVersion, Revision: revision + 1, BaseRevision: view.Revision, Previous: previous, Sequence: view.OperationSequence, Evictions: view.OperationEvictions, Set: delta.Set, Remove: delta.Remove}
	encoded, err := json.Marshal(mutation)
	if err != nil || len(encoded) > operationJournalMaximumRecordBytes {
		return nil, errors.New("invalid operation mutation")
	}
	hash := digestHex(encoded)
	line, err := json.Marshal(journalEntry{Payload: mutation, Hash: hash})
	if err != nil {
		return nil, errors.New("invalid operation mutation")
	}
	commit := &operationCommit{revision: mutation.Revision, payload: mutation, line: append(line, '\n'), hash: hash, settled: make(chan struct{})}
	journal.commits = append(journal.commits, commit)
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
	<-commit.settled
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
		journal.write.Lock()
		journal.mutex.Lock()
		if len(journal.commits) == 0 {
			journal.mutex.Unlock()
			journal.write.Unlock()
			continue
		}
		batch := journal.takeBatchLocked()
		journal.writing = true
		offset := journal.size
		failure := journal.available()
		journal.mutex.Unlock()
		err := failure
		if err == nil {
			err = journal.writeBatchAt(batch, offset)
		}
		journal.mutex.Lock()
		journal.writing = false
		if err == nil {
			journal.recordBatchLocked(batch)
		} else if !errors.Is(err, ErrStateClosed) {
			journal.poisoned = true
		}
		journal.mutex.Unlock()
		journal.write.Unlock()
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
func (journal *operationJournal) writeBatchAt(batch []*operationCommit, offset int64) error {
	if journal.testWriteGate != nil {
		journal.testWriteGate()
	}
	if err := journal.verifyPathIdentity(); err != nil {
		return ErrStateUncertain
	}
	run := make([]byte, 0, operationJournalBatchBytes)
	for _, commit := range batch {
		run = append(run, commit.line...)
	}
	if _, err := journal.file.WriteAt(run, offset); err != nil {
		return ErrStateUncertain
	}
	if err := journal.file.Sync(); err != nil {
		return ErrStateUncertain
	}
	return journal.writeWitnessFor(batch[len(batch)-1])
}

// recordBatchLocked advances the chain state a written batch owns. The caller holds the
// queue lock and the write lock, so the revision it records is the revision on disk.
func (journal *operationJournal) recordBatchLocked(batch []*operationCommit) {
	journal.syncs++
	journal.coalesced += uint64(len(batch) - 1)
	for _, commit := range batch {
		journal.size += int64(len(commit.line))
		journal.revision = commit.revision
		journal.digest = commit.hash
		journal.records = append(journal.records, journalEntry{Payload: commit.payload, Hash: commit.hash})
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
