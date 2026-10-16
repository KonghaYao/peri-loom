package platform

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

var ErrStateLocked = errors.New("private state locked")
var ErrStateClosed = errors.New("private state closed")
var ErrStateUncertain = errors.New("private persistence uncertain; durable verification required")
var ErrStateFrozen = errors.New("private state frozen by the operation checkpoint")

// auditBatchMaximum bounds how many audit records one durable head commit
// carries; recovery must accept exactly the revision gaps this permits.
const auditBatchMaximum = 32

type durableFile struct {
	mutex     sync.Mutex
	path      string
	lock      *os.File
	payload   json.RawMessage
	revision  uint64
	fresh     bool
	closed    bool
	poisoned  bool
	prototype any
	// parentOperations rebuilds the durable operation view that recovery merges
	// into the persisted parent. An operation sidecar commits without rewriting
	// the registry payload, so the persisted snapshot trails the sidecar between
	// registry commits; validation has to compare a candidate against the parent
	// state recovery would reconstruct, not against that older snapshot. Nil for
	// every durable file that owns its whole state.
	parentOperations func(*State)
}

type diskEnvelope struct {
	Version  uint32          `json:"version"`
	Revision uint64          `json:"revision"`
	Checksum string          `json:"checksum"`
	Payload  json.RawMessage `json:"payload"`
}

type diskFloor struct {
	Version  uint32 `json:"version"`
	Revision uint64 `json:"revision"`
	Checksum string `json:"checksum"`
}

func openDurable(path string, initial any) (*durableFile, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	directory := filepath.Dir(absolute)
	if err := privateDirectory(directory); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	fresh := len(entries) == 0
	if !fresh {
		if err := privatePath(absolute+".lock", false); err != nil {
			return nil, errors.New("missing initialization marker")
		}
	}
	lock, err := lockPath(absolute + ".lock")
	if err != nil {
		return nil, err
	}
	disk := &durableFile{path: absolute, lock: lock, prototype: initial}
	ok := false
	defer func() {
		if !ok {
			disk.close()
		}
	}()
	if err := lock.Sync(); err != nil {
		return nil, err
	}
	if err := fsyncDirectory(directory); err != nil {
		return nil, err
	}
	entries, err = os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	if fresh && len(entries) == 1 && entries[0].Name() == filepath.Base(absolute)+".lock" {
		if err := disk.initialize(initial); err != nil {
			return nil, err
		}
	} else {
		for _, entry := range entries {
			name := entry.Name()
			if filepath.Base(absolute) == "state.json" && name == "operations" && entry.IsDir() {
				continue
			}
			if name == filepath.Base(absolute) || name == filepath.Base(absolute)+".floor" || name == filepath.Base(absolute)+".lock" || (strings.HasPrefix(name, ".private-") && strings.HasSuffix(name, ".tmp") && entry.Type().IsRegular()) {
				continue
			}
			if strings.HasSuffix(filepath.Base(absolute), ".head") && name == strings.TrimSuffix(filepath.Base(absolute), ".head") {
				continue
			}
			if _, isAudit := disk.prototype.(auditHead); isAudit && allowedAuditSidecar(strings.TrimSuffix(filepath.Base(absolute), ".head"), entry) {
				continue
			}
			return nil, errors.New("unexpected private state file")
		}
		var envelope diskEnvelope
		readErr := readDurableEnvelope(absolute, &envelope)
		if errors.Is(readErr, os.ErrNotExist) {
			// The floor is durable before the payload, so a missing payload is
			// either an interrupted initialization or a wiped store. Only a floor
			// that never left revision zero can prove the initialization.
			if err := disk.resumeInitialization(); err != nil {
				return nil, err
			}
			ok = true
			return disk, nil
		}
		if readErr != nil {
			return nil, readErr
		}
		var floor diskFloor
		if err := readPrivate(absolute+".floor", &floor); err != nil {
			return nil, err
		}
		digest := digestHex(envelope.Payload)
		if !validDurableEnvelope(envelope) || floor.Version != 1 || floor.Revision > envelope.Revision {
			return nil, errors.New("state checksum/rollback/incomplete persistence")
		}
		if floor.Revision < envelope.Revision {
			// The payload is written before the floor, so a crash between the two
			// can only leave the floor behind a fully durable payload. Advancing it
			// here keeps the rollback bound monotonic without replaying anything,
			// but no writer can leave a gap wider than one durable transition.
			if envelope.Revision-floor.Revision > durableRevisionStep(disk.prototype) {
				return nil, errors.New("state checksum/rollback/incomplete persistence")
			}
			encoded, err := json.Marshal(diskFloor{Version: 1, Revision: envelope.Revision, Checksum: digest})
			if err != nil {
				return nil, err
			}
			if err := atomicPrivate(absolute+".floor", encoded); err != nil {
				return nil, err
			}
		} else if floor.Checksum != digest {
			return nil, errors.New("state checksum/rollback/incomplete persistence")
		}
		disk.payload = append(json.RawMessage(nil), envelope.Payload...)
		disk.revision = envelope.Revision
	}
	ok = true
	return disk, nil
}

// initialize writes revision zero. The floor is durable before the payload so an
// interrupted initialization stays resumable from the floor alone, and the
// rollback bound never trails a payload that already landed.
func (disk *durableFile) initialize(value any) error {
	payload, err := json.Marshal(value)
	if err != nil || len(payload) > durablePayloadBytes {
		return errors.New("invalid or oversized private state")
	}
	after := diskEnvelope{Version: 1, Revision: 0, Checksum: digestHex(payload), Payload: payload}
	if err := validateDurableTransition(disk, after); err != nil {
		return err
	}
	floor, _ := json.Marshal(diskFloor{Version: 1, Revision: 0, Checksum: after.Checksum})
	if err := atomicPrivate(disk.path+".floor", floor); err != nil {
		return err
	}
	envelope, _ := json.Marshal(after)
	if err := atomicPrivate(disk.path, envelope); err != nil {
		return err
	}
	disk.payload = payload
	disk.revision = 0
	disk.fresh = true
	return nil
}

// resumeInitialization finishes an initialization whose payload never landed.
// Any other missing payload is a wiped or unreadable store and stays refused.
func (disk *durableFile) resumeInitialization() error {
	var floor diskFloor
	if err := readPrivate(disk.path+".floor", &floor); err != nil || floor.Version != 1 || floor.Revision != 0 {
		return errors.New("state checksum/rollback/incomplete persistence")
	}
	return disk.initialize(disk.prototype)
}

func (disk *durableFile) persist(revision uint64, value any) error {
	payload, err := json.Marshal(value)
	if err != nil || len(payload) > durablePayloadBytes {
		return errors.New("invalid or oversized private state")
	}
	checksum := digestHex(payload)
	after := diskEnvelope{Version: 1, Revision: revision, Checksum: checksum, Payload: payload}
	if err := validateDurableTransition(disk, after); err != nil {
		return err
	}
	envelope, _ := json.Marshal(after)
	floor, _ := json.Marshal(diskFloor{Version: 1, Revision: revision, Checksum: checksum})
	if err := atomicPrivate(disk.path, envelope); err != nil {
		return err
	}
	if err := atomicPrivate(disk.path+".floor", floor); err != nil {
		return err
	}
	disk.revision = revision
	disk.payload = payload
	return nil
}

func (disk *durableFile) commit(revision uint64, value any) error {
	if err := disk.available(); err != nil {
		return err
	}
	if disk.revision == ^uint64(0) || revision != disk.revision+1 {
		return errors.New("private revision must increment exactly")
	}
	if err := disk.persist(revision, value); err != nil {
		disk.poisoned = true
		return ErrStateUncertain
	}
	return nil
}

func (disk *durableFile) commitAuditBatch(revision uint64, value auditHead) error {
	if err := disk.available(); err != nil {
		return err
	}
	if revision <= disk.revision || revision-disk.revision > auditBatchMaximum || value.Sequence != revision {
		return errors.New("invalid audit batch revision")
	}
	if err := disk.persist(revision, value); err != nil {
		disk.poisoned = true
		return ErrStateUncertain
	}
	return nil
}

func (disk *durableFile) available() error {
	if disk.closed {
		return ErrStateClosed
	}
	if disk.poisoned {
		return ErrStateUncertain
	}
	return nil
}

func (disk *durableFile) close() error {
	disk.mutex.Lock()
	defer disk.mutex.Unlock()
	if disk.closed {
		return nil
	}
	disk.closed = true
	return errors.Join(syscall.Flock(int(disk.lock.Fd()), syscall.LOCK_UN), disk.lock.Close())
}

func digestHex(bytes []byte) string { hash := sha256.Sum256(bytes); return hex.EncodeToString(hash[:]) }

func privateDirectory(directory string) error {
	var missing []string
	for current := directory; ; current = filepath.Dir(current) {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := fsyncDirectory(filepath.Dir(missing[index])); err != nil {
			return err
		}
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("private directory ownership/type invalid")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		if err := os.Chmod(directory, 0700); err != nil {
			return err
		}
	}
	return privatePath(directory, true)
}

func privatePath(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&077 != 0 || info.Mode()&os.ModeSymlink != 0 || directory != info.IsDir() || (!directory && !info.Mode().IsRegular()) || !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("private path ownership/mode/type invalid")
	}
	return nil
}

func lockPath(path string) (*os.File, error) {
	descriptor, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if err := privatePath(path, false); err != nil {
		file.Close()
		return nil, err
	}
	if err := syscall.Flock(descriptor, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, ErrStateLocked
	}
	return file, nil
}

func fsyncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func atomicPrivate(path string, bytes []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".private-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(bytes); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return fsyncDirectory(filepath.Dir(path))
}

func readPrivate(path string, value any) error {
	if err := privatePath(path, false); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := io.ReadAll(io.LimitReader(file, (16<<20)+1))
	if err != nil || len(bytes) > 16<<20 {
		return errors.New("private state read failed or oversized")
	}
	return strictDecode(bytes, value)
}
