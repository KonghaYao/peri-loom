package platform

import (
	"errors"
	"sync"
)

func unifiedCompletionReservation(reservation AuditReservation) bool {
	held, ok := reservation.(*auditReservation)
	return ok && held != nil && held.audit != nil && held.audit.completionStore != nil
}

func (reservation *auditReservation) RecordOperation(event AuditEvent, change func(*operationEdit) error) error {
	if !unifiedCompletionReservation(reservation) || change == nil {
		return errors.New("unified completion reservation required")
	}
	return reservation.audit.recordCompletionOperation(event, reservation, change)
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
// delta, and the journal orders audit hashes under its own queue lock. Parallel
// submitters share the existing group commit without a timer or another fsync.
func (audit *fileAudit) recordUnifiedBatch(batch []*auditRequest) error {
	if len(batch) == 0 || len(batch) > auditBatchMaximum {
		return errors.New("invalid completion batch")
	}
	var group sync.WaitGroup
	group.Add(len(batch))
	for _, request := range batch {
		go func(request *auditRequest) {
			defer group.Done()
			request.failure = audit.completionStore.updateOperationsAudit(request.operationChange, &request.event, request.class == AuditClassCompletion)
		}(request)
	}
	group.Wait()
	return nil
}
