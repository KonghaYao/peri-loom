package platform

import (
	"errors"
	"os"
	"path/filepath"
)

func openOperationDisk(directory string, registry *durableFile, state *State) (*operationJournal, error) {
	path := filepath.Join(directory, "operations", "journal")
	if state.OperationStorageVersion >= 1 {
		if _, err := os.Lstat(path + ".lock"); err != nil {
			return nil, errors.New("required operation storage missing")
		}
	}
	journal, err := openOperationJournal(filepath.Join(directory, "operations"))
	if err != nil {
		return nil, err
	}
	if (state.OperationStorageVersion == completionJournalVersion) != (journal.completion != nil) {
		journal.close()
		return nil, errors.New("completion format transition incomplete; restore the consistent pre-migration copy")
	}
	if journal.completion != nil && journal.completion.Seal != state.CompletionGenesis {
		journal.close()
		return nil, errors.New("completion genesis does not match registry")
	}
	ok := false
	defer func() {
		if !ok {
			journal.close()
		}
	}()
	if state.OperationStorageVersion == 0 {
		// No operation-only requests may run until the registry records that this
		// log is mandatory. A crashed migration still uses the registry.
		candidate := *state
		candidate.OperationStorageVersion = 1
		candidate.Revision++
		if err := validateState(candidate, state); err != nil {
			return nil, err
		}
		if err := registry.commit(candidate.Revision, candidate); err != nil {
			return nil, err
		}
		*state = candidate
	}
	// The registry payload is the recovered base view; the log only carries the
	// deltas appended after that revision, so a log that trails the registry is
	// already contained in the payload.
	journal.applyView(state.Revision, state)
	if err := validateState(*state, nil); err != nil {
		return nil, err
	}
	if journal.completion != nil && len(journal.records) > 0 && journal.records[0].Payload.Revision <= journal.archivedRevision {
		// Recovery has proved every overlapping byte against the immutable
		// archives. Finish an interrupted archive/snapshot/truncate cycle before
		// accepting more entries; otherwise the same prefix could be archived
		// again under another range on the next compaction.
		if err := journal.compact(*state); err != nil {
			return nil, err
		}
	}
	ok = true
	return journal, nil
}

// registryParentOperations rebuilds the operation view recovery would join into a
// registry payload. Between registry commits the log is the only writer of
// operations, so the persisted payload alone is not the parent a successor has to
// be legal against: an operation etched into that payload can settle and be evicted
// afterwards, and validating against the stale copy would reject a valid commit and
// poison the registry.
func registryParentOperations(journal *operationJournal) func(*State) {
	return func(parent *State) {
		journal.mutex.Lock()
		defer journal.mutex.Unlock()
		if journal.closed {
			return
		}
		journal.applyView(parent.Revision, parent)
	}
}

// operationPendingEdit is an edit that is already applied to the live view while its delta
// is only ordered in the journal. The view lock owns it: submit and settle are both called
// with that lock held, so the queue order and the view order stay the same order.
type operationPendingEdit struct {
	commit *operationCommit
	revert func()

	// admission marks an edit whose reversion would change an admission answer. Every
	// other pending edit leaves the isolation set and the unresolved count alone, so a
	// read may visit the view while it is undecided.
	admission bool
}

// settleOperations confirms every edit that is applied to the view but not yet durable.
// The caller holds the view lock. It waits for the journal's own barrier and nothing else -
// the committer never takes the view lock - so no reader can deadlock against a flush. A
// failed barrier rolls its edits back in reverse order and poisons the store, because an
// uncertain append must never leave a confirmed-looking record behind.
func (store *stateStore) settleOperations() error {
	pending := store.unconfirmed
	if len(pending) == 0 {
		return nil
	}
	store.unconfirmed = nil
	var failure error
	for _, entry := range pending {
		if err := store.operations.await(entry.commit); err != nil {
			if failure == nil {
				failure = err
			}
			continue
		}
	}
	if failure == nil {
		return nil
	}
	// Only an append that failed is rolled back. An append whose barrier returned is durable,
	// and undoing it in the view would misreport what the log already holds; the store stays
	// fail-closed either way, and a failed append can leave a later key at an earlier value
	// only until the store is reopened.
	for index := len(pending) - 1; index >= 0; index-- {
		entry := pending[index]
		if entry.commit.err == nil || entry.commit.applied {
			continue
		}
		entry.commit.applied = true
		entry.revert()
	}
	// A shutdown refusal proves the append never happened; only an uncertain append leaves
	// persistence in doubt and poisons the store.
	if !errors.Is(failure, ErrStateClosed) {
		store.disk.poisoned.Store(true)
	}
	return failure
}

// readOperations visits the committed operation view without copying the rest of the
// registry. The callback must not retain or mutate the state. A visitor must not decide on
// a record whose append was never confirmed when that decision could differ from the one a
// reversion would produce, so the read settles the pending edits that can still change an
// admission answer and reports the uncertainty such an append had. A pending edit that
// leaves the isolation set and the unresolved count alone is visited in place: both
// answers are the same either way, and waiting for it would put a barrier in the admission
// path of every request.
func (store *stateStore) readOperations(visit func(State) error) error {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("operation read requires a visitor")
	}
	if store.pendingAdmissionEdit() {
		if err := store.settleOperations(); err != nil {
			return err
		}
		return visit(store.state)
	}
	if err := visit(store.state); err != nil {
		return err
	}
	// A close that lands while the visitor runs can turn an append it relied on into a
	// refusal; report the refusal instead of a decision taken on a record whose barrier
	// will never be reported.
	return store.disk.available()
}

// pendingAdmissionEdit reports whether any applied edit could still flip an admission
// answer. Edits that cannot are safe to observe undecided: their reversion would leave the
// accessors every visitor uses with the same values.
func (store *stateStore) pendingAdmissionEdit() bool {
	for _, entry := range store.unconfirmed {
		if entry.admission {
			return true
		}
	}
	return false
}

// updateOperations applies one edit and returns only after its appended delta is durable.
// The edit is applied under the view lock, so no reader observes an unconfirmed record; the
// barrier itself runs outside that lock, so a flush no longer blocks authentication and the
// other narrow reads that never look at operations.
func (store *stateStore) updateOperations(change func(*operationEdit) error) error {
	return store.updateOperationsAudit(change, nil, false)
}

// enqueueOperationsAudit applies the edit to the live view and orders its delta in the
// journal, but returns before the barrier. It reports the queued commit through out so a
// caller can drop a namespace fence before paying the barrier: holding that fence across the
// barrier is what keeps two commits of one namespace from ever sharing one, which is the whole
// reason same-namespace concurrency stays fsync-bound. A no-op mutation leaves out nil.
func (store *stateStore) enqueueOperationsAudit(change func(*operationEdit) error, event *AuditEvent, reserved bool, out **operationCommit) error {
	preparation := store.observeOperationPreparation()
	unlock := store.lockOperationMutationStateObserved(preparation)
	commit, err := store.enqueueOperationsAuditLocked(preparation, change, event, reserved)
	unlock()
	*out = commit
	return err
}

// enqueueOperationsAuditLocked assumes the operation state lock is held. A batch of
// audit completions enters it once and still resolves every entry into its own commit.
func (store *stateStore) enqueueOperationsAuditLocked(preparation *durabilityPreparation, change func(*operationEdit) error, event *AuditEvent, reserved bool) (*operationCommit, error) {
	preparation.mark("state_gate")
	if err := store.disk.available(); err != nil {
		return nil, err
	}
	if store.maintenanceClosing {
		return nil, ErrStateClosed
	}
	// The shutdown checkpoint names the revision that materialized the operation
	// view; an operation committed afterwards would make that name a lie, so the
	// write is refused loudly instead of silently invalidating the payload a
	// launcher reads.
	if store.checkpointed {
		return nil, ErrStateFrozen
	}
	if change == nil && event == nil {
		return nil, errors.New("operation mutation required")
	}
	if store.state.OperationStorageVersion != 1 && store.state.OperationStorageVersion != completionJournalVersion {
		return nil, errors.New("operation storage identity unavailable")
	}
	// The edit is built on the view as it stands, including edits whose barrier is still
	// outstanding. That is safe because the queue keeps them in submit order and a failure
	// reverts them in reverse order, and it is what lets concurrent appends share one
	// barrier: waiting here would serialize every commit behind the oldest one.
	preparation.mark("entry_checks")
	if store.operationsIndex == nil {
		store.operationsIndex = newOperationIndex(store.state.Operations)
		store.cost.observeIndexRebuild(len(store.operationsIndex.records))
	}
	edit := newOperationEdit(&store.state, store.operationsIndex)
	preparation.mark("edit_setup")
	if change != nil {
		if err := change(edit); err != nil {
			edit.revert()
			return nil, err
		}
	}
	preparation.mark("edit")
	if err := edit.settleRetention(); err != nil {
		edit.revert()
		return nil, err
	}
	preparation.mark("retention")
	if err := edit.validate(); err != nil {
		edit.revert()
		return nil, err
	}
	preparation.mark("validate")
	delta := edit.delta()
	preparation.mark("delta")
	if event == nil && delta.empty() && store.state.OperationSequence == edit.priorSequence && store.state.OperationEvictions == edit.evictions {
		// A mutation that changed nothing needs no barrier at all.
		return nil, nil
	}
	if preparation != nil {
		preparation.caller = edit.caller
	}
	commit, err := store.operations.submitAuditPrepared(store.state, delta, event, reserved, preparation)
	if err != nil {
		// A full queue is a refusal this change introduced before any write, so it poisons
		// nothing; every other registration refusal keeps the base behaviour of failing the
		// store closed.
		if !errors.Is(err, ErrOperationQueueFull) {
			store.disk.poisoned.Store(true)
		}
		edit.revert()
		return nil, err
	}
	store.unconfirmed = append(store.unconfirmed, operationPendingEdit{commit: commit, revert: edit.revert, admission: edit.admissionRelevant()})
	store.cost.observeCommit()
	return commit, nil
}

// submitOperationsAudit queues one edit and hands back the wait function that makes it
// durable. The wait is separate so the caller can release its namespace fence first.
func (store *stateStore) submitOperationsAudit(change func(*operationEdit) error, event *AuditEvent, reserved bool) (func() error, error) {
	var commit *operationCommit
	if err := store.enqueueOperationsAudit(change, event, reserved, &commit); err != nil {
		return nil, err
	}
	if commit == nil {
		return func() error { return nil }, nil
	}
	return func() error { return store.awaitOperationCommit(commit) }, nil
}

func (store *stateStore) submitOperations(change func(*operationEdit) error) (func() error, error) {
	return store.submitOperationsAudit(change, nil, false)
}

// operationAuditEntry is one audit-linked edit of a completion batch.
type operationAuditEntry struct {
	change   func(*operationEdit) error
	event    *AuditEvent
	reserved bool
}

// submitOperationsAuditBatch queues a batch of independent edits under one
// operation state lock. Every entry still becomes its own journal commit with its
// own barrier and its own refusal, so a failed member never hides behind its
// neighbours; sharing the lock only keeps a drainer batch from resolving the same
// contention once per member.
func (store *stateStore) submitOperationsAuditBatch(entries []operationAuditEntry) ([]func() error, []error) {
	waits := make([]func() error, len(entries))
	failures := make([]error, len(entries))
	if len(entries) == 0 {
		return waits, failures
	}
	preparation := store.observeOperationPreparation()
	unlock := store.lockOperationMutationStateObserved(preparation)
	defer unlock()
	for index := range entries {
		observed := preparation
		if index > 0 {
			// One shared entry covers the lock acquisition; later members still keep
			// their own caller so a slow member stays attributable.
			observed = store.observeOperationPreparation()
		}
		commit, err := store.enqueueOperationsAuditLocked(observed, entries[index].change, entries[index].event, entries[index].reserved)
		if err != nil {
			failures[index] = err
			continue
		}
		if commit == nil {
			waits[index] = func() error { return nil }
			continue
		}
		waits[index] = func() error { return store.awaitOperationCommit(commit) }
	}
	return waits, failures
}

// updateOperationsAudit applies one edit and returns only after its appended delta is durable.
func (store *stateStore) updateOperationsAudit(change func(*operationEdit) error, event *AuditEvent, reserved bool) error {
	wait, err := store.submitOperationsAudit(change, event, reserved)
	if err != nil {
		return err
	}
	return wait()
}

// awaitOperationCommit pays one commit's barrier and settles the view without holding the
// namespace fence.
func (store *stateStore) awaitOperationCommit(commit *operationCommit) error {
	store.operations.waitObserved(commit)
	unlock := store.lockOperationMutationState()
	// Another waiter may have settled this commit before us; settling again is what makes
	// the outcome definite either way.
	settleErr := store.settleOperations()
	var compaction *operationCompaction
	if settleErr == nil {
		if store.operations.completion != nil {
			store.scheduleCompletionMaintenanceLocked()
		} else {
			compaction, settleErr = store.beginOperationCompactionLocked()
		}
	}
	poisoned := store.disk.poisoned.Load()
	unlock()
	if compaction != nil {
		settleErr = store.finishOperationCompaction(compaction)
	}
	if settleErr != nil {
		return settleErr
	}
	if commit.err != nil {
		return commit.err
	}
	if poisoned {
		// This delta is durable, but the log is now uncertain for another commit, so the
		// caller must not treat the registration as usable.
		return ErrStateUncertain
	}
	return nil
}
