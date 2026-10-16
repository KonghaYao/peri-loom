package platform

import (
	"sync"
	"time"
)

// A commit holds the shared state lock across its fsync, so its cost is invisible in
// the calling request's own timing and is paid by every narrow read that queues behind
// it. These counters keep the two apart: wait time is what the lock imposed, and index
// rebuilds are the only full-set work a commit can still trigger. Operation commits
// themselves share the live view instead of copying it, so no entry count is charged
// per commit.
type stateCost struct {
	mutex            sync.Mutex
	lockWaits        uint64
	lockWaitNS       uint64
	lockWaitMaxNS    uint64
	operationCommits uint64
	indexRebuilds    uint64
	indexEntries     uint64
}

func (cost *stateCost) observeWait(elapsed time.Duration) {
	cost.mutex.Lock()
	defer cost.mutex.Unlock()
	cost.lockWaits++
	cost.lockWaitNS += uint64(elapsed)
	if uint64(elapsed) > cost.lockWaitMaxNS {
		cost.lockWaitMaxNS = uint64(elapsed)
	}
}

func (cost *stateCost) observeCommit() {
	cost.mutex.Lock()
	defer cost.mutex.Unlock()
	cost.operationCommits++
}

func (cost *stateCost) observeIndexRebuild(entries int) {
	cost.mutex.Lock()
	defer cost.mutex.Unlock()
	cost.indexRebuilds++
	cost.indexEntries += uint64(entries)
}

func (cost *stateCost) snapshot() map[string]uint64 {
	cost.mutex.Lock()
	defer cost.mutex.Unlock()
	return map[string]uint64{
		"state_lock_waits_total":         cost.lockWaits,
		"state_lock_wait_ns_total":       cost.lockWaitNS,
		"state_lock_wait_max_ns":         cost.lockWaitMaxNS,
		"operation_commits_total":        cost.operationCommits,
		"operation_index_rebuilds_total": cost.indexRebuilds,
		"operation_index_entries_total":  cost.indexEntries,
	}
}

// lockState acquires the shared state lock and charges the wait to the counters, so a
// request that only queued behind another database's commit is attributable. The caller
// releases with the returned function.
func (store *stateStore) lockState() func() {
	started := time.Now()
	store.disk.mutex.Lock()
	store.cost.observeWait(time.Since(started))
	return store.disk.mutex.Unlock
}

// operationStats reports what one operation commit costs beyond its own work. It is read
// through the gateway metrics map instead of a dedicated endpoint.
func (store *stateStore) operationStats() map[string]any {
	stats := map[string]any{}
	for name, value := range store.cost.snapshot() {
		stats[name] = value
	}
	if store.operations != nil {
		for name, value := range store.operations.stats() {
			stats[name] = value
		}
	}
	return stats
}

// The commit queue pays one barrier per batch, so a reader divides syncs by appends for the
// barriers per request and reads coalesced_total for what the queue merged. A snapshot
// replaces the whole chain, so its frequency and cost stay visible separately. Wait time is
// what a caller or a settling reader spent waiting for a barrier it needed.
func (journal *operationJournal) stats() map[string]uint64 {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	return map[string]uint64{
		"operation_journal_appends_total":        journal.appendsTotal,
		"operation_journal_syncs_total":          journal.syncs,
		"operation_journal_ordered_total":        journal.ordered,
		"operation_journal_coalesced_total":      journal.coalesced,
		"operation_journal_batch_max":            uint64(journal.batchMaximum),
		"operation_journal_queue_max":            uint64(journal.queueMaximum),
		"operation_journal_batch_limit":          uint64(operationJournalBatchMaximum),
		"operation_journal_queue_limit":          uint64(operationJournalQueueMaximum),
		"operation_journal_queued":               uint64(len(journal.commits)),
		"operation_journal_wait_ns_total":        journal.waitNS,
		"operation_journal_wait_max_ns":          journal.waitMaxNS,
		"operation_journal_records":              uint64(len(journal.records)),
		"operation_journal_compactions_total":    journal.compactions,
		"operation_journal_compaction_ns_total":  journal.compactionNS,
		"operation_journal_compact_record_limit": operationJournalCompactRecords,
		"operation_journal_maximum_bytes":        operationJournalMaximumBytes,
	}
}
