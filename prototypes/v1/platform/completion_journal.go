package platform

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const completionJournalVersion = 2
const completionReservedMaximum = 64
const completionRecordReservationBytes = operationJournalMaximumRecordBytes + 256
const completionQueueMaximumBytes = 8 << 20
const completionActiveMaximumBytes = operationJournalMaximumBytes + completionReservedMaximum*completionRecordReservationBytes
const completionRetentionSoftBytes int64 = 1 << 30
const completionRetentionMaximumBytes int64 = completionRetentionSoftBytes + completionReservedMaximum*completionRecordReservationBytes

// The registry binds this genesis seal. The legacy audit remains an immutable
// content-verified prefix; AuditPath documents its original location but does
// not prevent restoring a consistent backup at a different filesystem path.
type completionGenesis struct {
	Version         uint32    `json:"version"`
	StateRevision   uint64    `json:"state_revision"`
	StateDigest     string    `json:"state_digest"`
	JournalRevision uint64    `json:"journal_revision"`
	JournalDigest   string    `json:"journal_digest"`
	AuditPath       string    `json:"audit_path"`
	Audit           auditHead `json:"audit"`
	Seal            string    `json:"seal"`
}

func (genesis completionGenesis) digest() (string, error) {
	genesis.Seal = ""
	encoded, err := json.Marshal(genesis)
	if err != nil {
		return "", err
	}
	return digestHex(encoded), nil
}

func (journal *operationJournal) loadCompletionGenesis() error {
	path := filepath.Join(filepath.Dir(journal.path), "completion.genesis")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	var genesis completionGenesis
	if err := readDurableBounded(path, 65536, &genesis); err != nil {
		return err
	}
	digest, err := genesis.digest()
	if err != nil || genesis.Version != completionJournalVersion || digest != genesis.Seal ||
		len(genesis.StateDigest) != 64 || !filepath.IsAbs(genesis.AuditPath) ||
		genesis.Audit.Version != 1 || len(genesis.Audit.Hash) != 64 ||
		genesis.JournalRevision != 0 && len(genesis.JournalDigest) != 64 {
		return errors.New("invalid completion genesis")
	}
	journal.completion = &genesis
	journal.auditHead, journal.orderedAudit = genesis.Audit, genesis.Audit
	return nil
}

// OpenCompletionAudit is the service's single audit opener. A v2 registry always
// selects the unified log, even when an opt-in flag is absent on a later run.
func OpenCompletionAudit(state StateStore, path string, enable bool) (Audit, error) {
	store, ok := state.(*stateStore)
	if !ok {
		return nil, errors.New("completion requires durable state")
	}
	legacy, err := OpenAudit(path)
	if err != nil {
		return nil, err
	}
	audit := legacy.(*fileAudit)
	valid := false
	defer func() {
		if !valid {
			_ = audit.Close()
		}
	}()
	if store.state.OperationStorageVersion == completionJournalVersion {
		genesis := store.operations.completion
		if genesis == nil ||
			audit.state != genesis.Audit {
			return nil, errors.New("completion legacy audit prefix does not match genesis")
		}
		audit.completionStore = store
	} else if enable {
		if err := store.enableCompletion(audit); err != nil {
			return nil, err
		}
		audit.completionStore = store
	}
	valid = true
	return audit, nil
}

func (store *stateStore) enableCompletion(audit *fileAudit) error {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if store.state.OperationStorageVersion != 1 || len(store.unconfirmed) != 0 {
		return errors.New("completion migration requires a settled v1 store")
	}
	_, unresolved := operationCounts(store.state)
	fresh := len(store.state.Organizations) == 0 && len(store.state.Operations) == 0
	if unresolved != 0 || !fresh && !operationsCheckpointMaterialized(store.state) {
		return errors.New("completion migration requires clean shutdown with no unresolved operations")
	}
	journal := store.operations
	journal.write.Lock()
	defer journal.write.Unlock()
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return err
	}
	if journal.writing || len(journal.commits) != 0 || journal.completion != nil {
		return errors.New("completion migration requires an idle journal")
	}
	// Finalize the old audit witness before binding its immutable tail.
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if audit.state.Sequence > audit.head.revision {
		if err := audit.head.commitAuditBatch(audit.state.Sequence, audit.state); err != nil {
			return err
		}
	}
	if err := audit.file.Sync(); err != nil {
		return err
	}
	genesis := completionGenesis{
		Version: completionJournalVersion, StateRevision: store.state.Revision,
		StateDigest: digestHex(store.disk.payload), JournalRevision: journal.revision,
		JournalDigest: journal.digest, AuditPath: audit.file.Name(), Audit: audit.state,
	}
	genesis.AuditPath, _ = filepath.Abs(genesis.AuditPath)
	seal, err := genesis.digest()
	if err != nil {
		return err
	}
	genesis.Seal = seal
	encoded, err := json.Marshal(genesis)
	if err != nil {
		return err
	}
	if err := atomicPrivate(filepath.Join(filepath.Dir(journal.path), "completion.genesis"), encoded); err != nil {
		return err
	}
	candidate, err := cloneState(store.state)
	if err != nil {
		return err
	}
	candidate.OperationStorageVersion, candidate.CompletionGenesis = completionJournalVersion, seal
	// commitLocked may ask the operation journal to rebuild its parent, so drop
	// its bookkeeping mutex while retaining the exclusive file writer lock.
	journal.mutex.Unlock()
	err = store.commitLocked(candidate)
	journal.mutex.Lock()
	if err != nil {
		store.disk.poisoned = true
		return err
	}
	journal.completion = &genesis
	journal.auditHead, journal.orderedAudit = genesis.Audit, genesis.Audit
	journal.retainedBytes = journal.size
	return nil
}

func completionServiceEnabled() (bool, error) {
	switch strings.TrimSpace(os.Getenv("PERI_COMPLETION_JOURNAL")) {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, errors.New("PERI_COMPLETION_JOURNAL must be 0 or 1")
	}
}

// A successful shutdown checkpoint must also make the last rollback witness
// durable. The ordinary request still pays only its envelope's single Sync;
// this additional barrier happens after audit admission and SQL have drained.
func (journal *operationJournal) checkpointCompletionTail() error {
	if journal.completion == nil {
		return nil
	}
	journal.write.Lock()
	defer journal.write.Unlock()
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return err
	}
	if journal.writing || len(journal.commits) != 0 {
		return errors.New("completion checkpoint requires a drained journal")
	}
	if err := journal.verifyPathIdentity(); err != nil {
		return err
	}
	return journal.file.Sync()
}

// The state lock excludes a concurrent close and freezes the confirmed view.
// Maintenance work is bounded by the active journal limit and never owns SQL
// execution. At the hard byte ceiling submit refuses new intents.
func (store *stateStore) scheduleCompletionMaintenanceLocked() {
	journal := store.operations
	if store.maintenanceClosing || journal.maintenancePending || !journal.compactionDue() {
		return
	}
	journal.maintenancePending = true
	store.maintenance.Add(1)
	go func() {
		defer store.maintenance.Done()
		unlock := store.lockState()
		defer unlock()
		defer func() { journal.maintenancePending = false }()
		if store.maintenanceClosing || store.checkpointed {
			return
		}
		if err := store.settleOperations(); err != nil {
			store.disk.poisoned = true
			return
		}
		if err := store.compactOperationsLocked(); err != nil {
			store.disk.poisoned = true
		}
	}()
}
