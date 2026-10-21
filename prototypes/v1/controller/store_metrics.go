package controller

import "log"

// StoreMetrics exposes the durable commit pipeline without leaking the
// metadata itself. ReplayEntries is measured for the most recent open, while
// compaction counters are process-local.
type StoreMetrics struct {
	SnapshotRevision uint64 `json:"snapshot_revision"`
	Revision         uint64 `json:"revision"`
	JournalEntries   int    `json:"journal_entries"`
	JournalBytes     int64  `json:"journal_bytes"`
	ReplayEntries    int    `json:"replay_entries"`
	Compactions      uint64 `json:"compactions"`
	LastCompactionMS int64  `json:"last_compaction_ms"`
	CompactionError  string `json:"compaction_error,omitempty"`
	Closed           bool   `json:"closed"`
	Uncertain        bool   `json:"uncertain"`
}

func (store *diskStore) Metrics() StoreMetrics {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return StoreMetrics{
		SnapshotRevision: store.snapshotRevision,
		Revision:         store.metadata.Revision,
		JournalEntries:   store.journalEntries,
		JournalBytes:     store.journalBytes,
		ReplayEntries:    store.replayEntries,
		Compactions:      store.compactions,
		LastCompactionMS: store.lastCompactionMS,
		CompactionError:  store.compactionErrorMsg,
		Closed:           store.closed,
		Uncertain:        store.uncertain,
	}
}

func ReadStoreMetrics(store MetadataStore) (StoreMetrics, bool) {
	provider, ok := store.(interface{ Metrics() StoreMetrics })
	if !ok {
		return StoreMetrics{}, false
	}
	return provider.Metrics(), true
}

func logCompactionFailure(err error) {
	log.Printf("控制面元数据 compaction 失败：%v", err)
}
