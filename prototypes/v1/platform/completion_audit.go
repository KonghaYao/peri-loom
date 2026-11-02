package platform

import "errors"

func unifiedCompletionReservation(reservation AuditReservation) bool {
	held, ok := reservation.(*auditReservation)
	return ok && held != nil && held.audit != nil && held.audit.completionStore != nil
}

// RecordOperationDeferred queues the unified record for this reservation and
// returns its barrier instead of paying it here. The caller holds a namespace
// fence while it queues, so it must drop the fence before calling the wait:
// otherwise every completion of one database waits alone.
func (reservation *auditReservation) RecordOperationDeferred(event AuditEvent, change func(*operationEdit) error) (func() error, error) {
	if !unifiedCompletionReservation(reservation) || change == nil {
		return nil, errors.New("unified completion reservation required")
	}
	request, err := reservation.audit.prepareCompletionOperation(event, reservation, change)
	if err != nil {
		return nil, err
	}
	return reservation.audit.enqueueCompletion(request)
}

func (journal *operationJournal) completionAdmissionAvailable() bool {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	return journal.available() == nil && journal.retainedBytes+journal.queuedBytes < completionRetentionSoftBytes && journal.size+journal.queuedBytes < operationJournalMaximumBytes
}

func (journal *operationJournal) completionStatus() map[string]any {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	return map[string]any{
		"version":                   completionJournalVersion,
		"confirmed_revision":        journal.revision,
		"confirmed_audit_sequence":  journal.auditHead.Sequence,
		"confirmed_audit_hash":      journal.auditHead.Hash,
		"archived_revision":         journal.archivedRevision,
		"active_bytes":              journal.size,
		"retained_bytes":            journal.retainedBytes,
		"queued_bytes":              journal.queuedBytes,
		"retention_soft_bytes":      completionRetentionSoftBytes,
		"retention_hard_bytes":      completionRetentionMaximumBytes,
		"reserved_completion_slots": completionReservedMaximum,
		"shared_fsync_count":        journal.syncs,
		"shared_fsync_ns_total":     journal.syncNS,
		"retention_policy":          "retain all audit; refuse new admissions at soft limit",
	}
}

// These are already admitted requests. Each may enqueue one bounded operation
// delta. Records are submitted in audit order and awaited together: submission
// order is the only ordering the view and the audit chain need, and waiting
// afterwards lets the whole batch share one barrier. The submit loop enters the
// operation state lock once for the batch instead of once per record.
func (audit *fileAudit) recordUnifiedBatch(batch []*auditRequest) error {
	if len(batch) == 0 || len(batch) > auditBatchMaximum {
		return errors.New("invalid completion batch")
	}
	entries := make([]operationAuditEntry, len(batch))
	for index, request := range batch {
		entries[index] = operationAuditEntry{change: request.operationChange, event: &request.event, reserved: request.class == AuditClassCompletion}
	}
	commits, failures := audit.completionStore.submitOperationsAuditBatch(entries)
	batchErr := audit.completionStore.awaitOperationBatch(commits)
	for index, request := range batch {
		request.failure = errors.Join(failures[index], batchErr, operationCommitFailure(commits[index]))
	}
	return nil
}

// operationCommitFailure reports the barrier outcome of one batch member. A member without a
// commit changed nothing that needed durability, so it has nothing to report.
func operationCommitFailure(commit *operationCommit) error {
	if commit == nil {
		return nil
	}
	return commit.err
}
