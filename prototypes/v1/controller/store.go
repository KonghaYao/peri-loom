package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var ErrStoreLocked = errors.New("metadata store already locked")
var ErrStoreClosed = errors.New("metadata store closed")
var ErrStoreUncertain = errors.New("metadata persistence outcome uncertain; reopen requires durable verification")
var ErrStoreRevisionMismatch = errors.New("metadata store revision mismatch")

// RevisionedStore is an optional compare-and-commit capability. Callers that
// derived a mutation from an earlier snapshot can reject stale writes instead
// of overwriting a newer revision.
type RevisionedStore interface {
	UpdateAtRevision(uint64, func(*Metadata) error) error
}

type diskStore struct {
	mutex         sync.Mutex
	directory     string
	lock          *os.File
	journal       *os.File
	metadata      Metadata
	heartbeats    map[string]heartbeatObservation
	reconcileKeys map[string]struct{}
	closed        bool
	uncertain     bool

	// The journal is the only durable revision authority. The snapshot is a
	// derived checkpoint, and journalTail links the next record to the durable
	// prefix so replay can reject gaps, rewrites and stale compaction leftovers.
	snapshotRevision   uint64
	journalEntries     int
	journalBytes       int64
	journalTail        string
	replayEntries      int
	compactions        uint64
	lastCompactionMS   int64
	compactionErrorMsg string
}

type epochFloor struct {
	Version          uint32 `json:"version"`
	Revision         uint64 `json:"revision"`
	Epoch            uint64 `json:"epoch"`
	MetadataChecksum string `json:"metadata_checksum"`
}

func OpenStore(directory string) (MetadataStore, error) {
	if directory == "" {
		return nil, errors.New("private metadata directory required")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err := createPrivateDirectory(absolute); err != nil {
		return nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, err
	}
	stat, owned := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !owned || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("metadata directory must be owned and not a symlink")
	}
	entries, err := os.ReadDir(absolute)
	if err != nil {
		return nil, err
	}
	fresh := len(entries) == 0
	if fresh {
		if err := os.Chmod(absolute, 0700); err != nil {
			return nil, err
		}
	}
	if err := privateFile(absolute, true); err != nil {
		return nil, err
	}
	if !fresh {
		if err := privateFile(filepath.Join(absolute, "metadata.lock"), false); err != nil {
			return nil, fmt.Errorf("missing or unsafe initialization marker: %w", err)
		}
	}
	lock, err := openLock(absolute)
	if err != nil {
		return nil, err
	}
	store := &diskStore{directory: absolute, lock: lock, heartbeats: make(map[string]heartbeatObservation)}
	success := false
	defer func() {
		if !success {
			store.Close()
		}
	}()
	if err := lock.Sync(); err != nil {
		return nil, err
	}
	if err := syncDirectory(absolute); err != nil {
		return nil, err
	}
	entries, err = os.ReadDir(absolute)
	if err != nil {
		return nil, err
	}
	onlyLock := len(entries) == 1 && entries[0].Name() == "metadata.lock"
	if fresh && onlyLock {
		if err := store.initialize(); err != nil {
			return nil, err
		}
	} else {
		if err := validateDirectory(entries); err != nil {
			return nil, err
		}
		if err := store.load(); err != nil {
			return nil, err
		}
	}
	// A persisted heartbeat is historical evidence, not a live probe after
	// process restart. Keep the durable image intact for recovery.
	for identifier, worker := range store.metadata.Workers {
		store.heartbeats[identifier] = heartbeatObservation{address: worker.Address, load: 0, lastHeartbeatMS: worker.LastHeartbeatMS}
	}
	store.reconcileKeys = actionablePlacements(store.metadata.Placements)
	success = true
	return store, nil
}

func (store *diskStore) Snapshot() (Metadata, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.available(); err != nil {
		return Metadata{}, err
	}
	view, err := cloneMetadata(store.metadata)
	if err != nil {
		return Metadata{}, err
	}
	store.applyHeartbeats(view.Workers)
	return view, nil
}

func (store *diskStore) Update(mutation func(*Metadata) error) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.updateLocked(nil, mutation)
}

func (store *diskStore) UpdateAtRevision(expected uint64, mutation func(*Metadata) error) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.updateLocked(&expected, mutation)
}

func (store *diskStore) updateLocked(expected *uint64, mutation func(*Metadata) error) error {
	if err := store.available(); err != nil {
		return err
	}
	if mutation == nil {
		return errors.New("metadata mutation required")
	}
	if store.journal == nil {
		return errors.New("metadata journal unavailable")
	}
	if expected != nil && store.metadata.Revision != *expected {
		return fmt.Errorf("%w: expected revision %d, current %d", ErrStoreRevisionMismatch, *expected, store.metadata.Revision)
	}
	if store.shouldCompact() {
		if err := store.compactDurable(); err != nil {
			store.compactionErrorMsg = err.Error()
			logCompactionFailure(err)
		}
	}
	if store.journalEntries >= journalMaxEntries || store.journalBytes >= journalFileLimit {
		return errors.New("metadata journal requires compaction before more commits")
	}
	candidate := cloneMetadataForUpdate(store.metadata)
	store.applyHeartbeats(candidate.Workers)
	if err := mutation(&candidate); err != nil {
		return err
	}
	if candidate.Version != store.metadata.Version || candidate.Revision != store.metadata.Revision {
		return errors.New("metadata version/revision is store owned")
	}
	if candidate.Version != 1 || candidate.Workers == nil || candidate.Placements == nil {
		return errors.New("invalid metadata version or maps")
	}
	if candidate.Revision == math.MaxUint64 {
		return errors.New("metadata revision exhausted")
	}
	candidate.Revision++
	delta := diffMetadata(store.metadata, candidate)
	if err := validateMetadataDelta(store.metadata, delta); err != nil {
		return err
	}
	record, checksum, err := encodeJournalRecord(candidate.Revision, store.journalTail, delta)
	if err != nil {
		return err
	}
	if store.journalBytes+int64(len(record)) > journalFileLimit {
		return errors.New("metadata journal would exceed its size limit")
	}
	store.commitPoint("journal.append.before")
	if err := writeJournalRecord(store.journal, record); err != nil {
		return store.uncertainFailure(err)
	}
	store.commitPoint("journal.append.after")
	if err := store.journal.Sync(); err != nil {
		return store.uncertainFailure(err)
	}
	store.commitPoint("journal.fsync.after")
	store.commitPoint("index.before")
	// Publish only after the record is durable, and apply the delta to the
	// store's own maps rather than the callback's candidate. A callback that
	// retains its pointer can therefore never mutate committed state.
	next := store.metadata
	applyMetadataDelta(&next, delta, true)
	next.Version = candidate.Version
	next.Revision = candidate.Revision
	store.metadata = next
	store.journalTail = checksum
	store.journalEntries++
	store.journalBytes += int64(len(record))
	store.updateReconcileKeys(delta)
	store.refreshHeartbeatObservations()
	store.commitPoint("index.after")
	if store.shouldCompact() {
		if err := store.compactDurable(); err != nil {
			store.compactionErrorMsg = err.Error()
			logCompactionFailure(err)
		}
	}
	return nil
}

func (store *diskStore) available() error {
	if store.closed {
		return ErrStoreClosed
	}
	if store.uncertain {
		return ErrStoreUncertain
	}
	return nil
}

func (store *diskStore) uncertainFailure(err error) error {
	store.uncertain = true
	return fmt.Errorf("%w: %v", ErrStoreUncertain, err)
}

func (store *diskStore) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	unlock := syscall.Flock(int(store.lock.Fd()), syscall.LOCK_UN)
	closed := store.lock.Close()
	var journalClosed error
	if store.journal != nil {
		journalClosed = store.journal.Close()
	}
	return errors.Join(unlock, closed, journalClosed)
}

func cloneMetadata(metadata Metadata) (Metadata, error) {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return Metadata{}, err
	}
	var cloned Metadata
	err = json.Unmarshal(encoded, &cloned)
	return cloned, err
}
