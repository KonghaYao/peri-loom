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

type diskStore struct {
	mutex         sync.Mutex
	directory     string
	lock          *os.File
	metadata      Metadata
	heartbeats    map[string]heartbeatObservation
	reconcileKeys map[string]struct{}
	closed        bool
	uncertain     bool
	initialized   bool
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
		store.metadata = Metadata{Version: 1, Workers: make(map[string]Worker), Placements: make(map[string]Placement)}
		if err := store.persist(store.metadata); err != nil {
			return nil, err
		}
	} else {
		if err := validateDirectory(entries); err != nil {
			return nil, err
		}
		if err := recoverMetadataCommit(absolute); err != nil {
			return nil, err
		}
		metadata, floor, err := loadState(absolute)
		if err != nil {
			return nil, err
		}
		if floor.Version != 1 || floor.Revision != metadata.Revision || floor.Epoch != metadata.Epoch {
			return nil, errors.New("durable epoch/revision floor mismatch")
		}
		if err := validateMetadata(metadata, nil); err != nil {
			return nil, err
		}
		store.metadata = metadata
		// A persisted heartbeat is historical evidence, not a live probe after
		// process restart. Keep the durable before image intact for recovery.
		for identifier, worker := range metadata.Workers {
			store.heartbeats[identifier] = heartbeatObservation{address: worker.Address, load: 0, lastHeartbeatMS: worker.LastHeartbeatMS}
		}
	}
	store.reconcileKeys = actionablePlacements(store.metadata.Placements)
	store.initialized = true
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
	if err := store.available(); err != nil {
		return err
	}
	if mutation == nil {
		return errors.New("metadata mutation required")
	}
	candidate, err := cloneMetadata(store.metadata)
	if err != nil {
		return err
	}
	store.applyHeartbeats(candidate.Workers)
	if err := mutation(&candidate); err != nil {
		return err
	}
	if candidate.Version != store.metadata.Version || candidate.Revision != store.metadata.Revision {
		return errors.New("metadata version/revision is store owned")
	}
	if candidate.Revision == math.MaxUint64 {
		return errors.New("metadata revision exhausted")
	}
	candidate.Revision++
	committed, err := cloneMetadata(candidate)
	if err != nil {
		return err
	}
	if err := validateMetadata(committed, &store.metadata); err != nil {
		return err
	}
	keys := actionablePlacements(committed.Placements)
	if err := store.persist(committed); err != nil {
		store.uncertain = true
		return fmt.Errorf("%w: %v", ErrStoreUncertain, err)
	}
	store.metadata = committed
	store.reconcileKeys = keys
	store.refreshHeartbeatObservations()
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

func (store *diskStore) persist(metadata Metadata) error {
	encoded, encodedFloor, err := metadataCommitFiles(metadata)
	if err != nil {
		return err
	}
	intent := metadataCommit{Version: 1, After: metadata}
	if store.initialized {
		intent.Before = &store.metadata
	}
	if _, err := os.Lstat(filepath.Join(store.directory, metadataPendingName)); !errors.Is(err, os.ErrNotExist) {
		return errors.New("unfinished or unavailable metadata commit intent")
	}
	encodedIntent, _, err := encodeEnvelope(metadata.Revision, intent)
	if err != nil {
		return err
	}
	if err := atomicFile(store.directory, metadataPendingName, encodedIntent); err != nil {
		return err
	}
	if err := atomicFile(store.directory, "epoch-floor.json", encodedFloor); err != nil {
		return err
	}
	if err := atomicFile(store.directory, "metadata.json", encoded); err != nil {
		return err
	}
	return removeMetadataIntent(store.directory)
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
	return errors.Join(unlock, closed)
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
