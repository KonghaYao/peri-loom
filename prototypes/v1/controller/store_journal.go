package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
)

const (
	metadataSnapshotName = "metadata.json"
	metadataJournalName  = "metadata.journal"

	journalRecordVersion = 1
	journalRecordLimit   = 64 << 20
	journalFileLimit     = 256 << 20
	// journalMaxEntries bounds startup replay. Compaction runs well before this
	// limit, so a store that keeps failing compaction fails closed instead of
	// replaying an unbounded journal.
	journalMaxEntries = 8192
)

var (
	journalCompactEntries = 256
	journalCompactBytes   = int64(8 << 20)
)

// storeCommitHook is installed by tests to terminate the process at a durable
// boundary. Production code leaves it nil.
var storeCommitHook func(point string)

type metadataDelta struct {
	Epoch             *uint64              `json:"epoch,omitempty"`
	Workers           map[string]Worker    `json:"workers,omitempty"`
	RemovedWorkers    []string             `json:"removed_workers,omitempty"`
	Placements        map[string]Placement `json:"placements,omitempty"`
	RemovedPlacements []string             `json:"removed_placements,omitempty"`
}

type journalRecord struct {
	Version  uint32          `json:"version"`
	Revision uint64          `json:"revision"`
	Prev     string          `json:"prev"`
	Checksum string          `json:"checksum"`
	Delta    json.RawMessage `json:"delta"`
}

type decodedJournalRecord struct {
	revision uint64
	prev     string
	checksum string
	delta    metadataDelta
}

func (store *diskStore) commitPoint(point string) {
	if storeCommitHook != nil {
		storeCommitHook(point)
	}
}

func cloneMetadataForUpdate(metadata Metadata) Metadata {
	clone := Metadata{
		Version:    metadata.Version,
		Revision:   metadata.Revision,
		Epoch:      metadata.Epoch,
		Workers:    make(map[string]Worker, len(metadata.Workers)),
		Placements: make(map[string]Placement, len(metadata.Placements)),
	}
	for key, worker := range metadata.Workers {
		clone.Workers[key] = worker
	}
	for key, placement := range metadata.Placements {
		clone.Placements[key] = clonePlacement(placement)
	}
	return clone
}

func diffMetadata(previous, next Metadata) metadataDelta {
	var delta metadataDelta
	if next.Epoch != previous.Epoch {
		epoch := next.Epoch
		delta.Epoch = &epoch
	}
	for key, worker := range next.Workers {
		if old, exists := previous.Workers[key]; !exists || !reflect.DeepEqual(old, worker) {
			if delta.Workers == nil {
				delta.Workers = make(map[string]Worker)
			}
			delta.Workers[key] = worker
		}
	}
	for key := range previous.Workers {
		if _, exists := next.Workers[key]; !exists {
			delta.RemovedWorkers = append(delta.RemovedWorkers, key)
		}
	}
	for key, placement := range next.Placements {
		if old, exists := previous.Placements[key]; !exists || !reflect.DeepEqual(old, placement) {
			if delta.Placements == nil {
				delta.Placements = make(map[string]Placement)
			}
			delta.Placements[key] = placement
		}
	}
	for key := range previous.Placements {
		if _, exists := next.Placements[key]; !exists {
			delta.RemovedPlacements = append(delta.RemovedPlacements, key)
		}
	}
	sort.Strings(delta.RemovedWorkers)
	sort.Strings(delta.RemovedPlacements)
	return delta
}

func validateMetadataDelta(previous Metadata, delta metadataDelta) error {
	if delta.Epoch != nil && *delta.Epoch < previous.Epoch {
		return errors.New("metadata epoch cannot decrease")
	}
	epoch := previous.Epoch
	if delta.Epoch != nil {
		epoch = *delta.Epoch
	}
	removedWorkers := make(map[string]struct{}, len(delta.RemovedWorkers))
	for _, key := range delta.RemovedWorkers {
		if _, duplicate := removedWorkers[key]; duplicate {
			return errors.New("duplicate worker removal")
		}
		removedWorkers[key] = struct{}{}
		if _, exists := previous.Workers[key]; !exists {
			return errors.New("worker removal references unknown worker")
		}
		if _, conflict := delta.Workers[key]; conflict {
			return errors.New("worker both removed and upserted")
		}
	}
	for key, worker := range delta.Workers {
		var old *Worker
		if existing, exists := previous.Workers[key]; exists {
			existing := existing
			old = &existing
		}
		if err := validateWorkerEntry(key, worker, old); err != nil {
			return err
		}
	}
	removedPlacements := make(map[string]struct{}, len(delta.RemovedPlacements))
	for _, key := range delta.RemovedPlacements {
		if _, duplicate := removedPlacements[key]; duplicate {
			return errors.New("duplicate placement removal")
		}
		removedPlacements[key] = struct{}{}
		if _, exists := previous.Placements[key]; !exists {
			return errors.New("placement removal references unknown placement")
		}
		if _, conflict := delta.Placements[key]; conflict {
			return errors.New("placement both removed and upserted")
		}
	}
	for key, placement := range delta.Placements {
		var old *Placement
		if existing, exists := previous.Placements[key]; exists {
			existing := existing
			old = &existing
		}
		if err := validatePlacementEntry(key, placement, epoch, old); err != nil {
			return err
		}
	}
	return nil
}

// applyMetadataDelta applies a validated delta. When detach is set, mutable
// placement fields are cloned so a callback that retains its candidate cannot
// mutate the published store through shared pointers or slices.
func applyMetadataDelta(metadata *Metadata, delta metadataDelta, detach bool) {
	if delta.Epoch != nil {
		metadata.Epoch = *delta.Epoch
	}
	for _, key := range delta.RemovedWorkers {
		delete(metadata.Workers, key)
	}
	for key, worker := range delta.Workers {
		metadata.Workers[key] = worker
	}
	for _, key := range delta.RemovedPlacements {
		delete(metadata.Placements, key)
	}
	for key, placement := range delta.Placements {
		if detach {
			placement = clonePlacement(placement)
		}
		metadata.Placements[key] = placement
	}
}

func (store *diskStore) updateReconcileKeys(delta metadataDelta) {
	for _, key := range delta.RemovedPlacements {
		delete(store.reconcileKeys, key)
	}
	for key, placement := range delta.Placements {
		if placement.Phase == "suspended" {
			delete(store.reconcileKeys, key)
		} else {
			store.reconcileKeys[key] = struct{}{}
		}
	}
}

func sha256Sum(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func journalChecksum(revision uint64, prev string, payload []byte) string {
	hash := sha256.New()
	io.WriteString(hash, "peri-controller-metadata-journal-v1\n")
	var encodedRevision [8]byte
	binary.BigEndian.PutUint64(encodedRevision[:], revision)
	hash.Write(encodedRevision[:])
	io.WriteString(hash, prev)
	hash.Write([]byte{0})
	hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func encodeJournalRecord(revision uint64, prev string, delta metadataDelta) ([]byte, string, error) {
	payload, err := json.Marshal(delta)
	if err != nil {
		return nil, "", err
	}
	checksum := journalChecksum(revision, prev, payload)
	record := journalRecord{Version: journalRecordVersion, Revision: revision, Prev: prev, Checksum: checksum, Delta: payload}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, "", err
	}
	if len(encoded)+1 > journalRecordLimit {
		return nil, "", errors.New("metadata journal record exceeds size limit")
	}
	return append(encoded, '\n'), checksum, nil
}

func decodeJournalRecord(line []byte) (decodedJournalRecord, error) {
	var record journalRecord
	if err := strictJSON(line, &record); err != nil {
		return decodedJournalRecord{}, fmt.Errorf("invalid metadata journal record: %w", err)
	}
	if record.Version != journalRecordVersion {
		return decodedJournalRecord{}, fmt.Errorf("unsupported metadata journal version %d", record.Version)
	}
	if record.Revision == 0 {
		return decodedJournalRecord{}, errors.New("invalid metadata journal revision")
	}
	if len(record.Delta) == 0 || bytes.Equal(record.Delta, []byte("null")) {
		return decodedJournalRecord{}, errors.New("missing metadata journal delta")
	}
	var delta metadataDelta
	if err := strictJSON(record.Delta, &delta); err != nil {
		return decodedJournalRecord{}, fmt.Errorf("invalid metadata journal delta: %w", err)
	}
	payload, err := json.Marshal(delta)
	if err != nil {
		return decodedJournalRecord{}, err
	}
	if journalChecksum(record.Revision, record.Prev, payload) != record.Checksum {
		return decodedJournalRecord{}, errors.New("metadata journal checksum mismatch")
	}
	return decodedJournalRecord{revision: record.Revision, prev: record.Prev, checksum: record.Checksum, delta: delta}, nil
}

// readJournalFile reads complete newline-terminated records and reports the
// valid prefix. A tail without a terminating newline is a torn append: it was
// never published and is discarded by the caller. A complete record that fails
// checksum or schema validation is corruption and is rejected.
func readJournalFile(path string) ([]decodedJournalRecord, int64, int64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, 0, 0, err
	}
	if err := privateFile(path, false); err != nil {
		return nil, 0, 0, err
	}
	if info.Size() > journalFileLimit {
		return nil, 0, 0, errors.New("metadata journal exceeds size limit")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, 0, err
	}
	var records []decodedJournalRecord
	offset := 0
	for offset < len(encoded) {
		newline := bytes.IndexByte(encoded[offset:], '\n')
		if newline < 0 {
			break
		}
		line := encoded[offset : offset+newline]
		if len(line) == 0 {
			return nil, 0, 0, errors.New("empty metadata journal record")
		}
		if len(line)+1 > journalRecordLimit {
			return nil, 0, 0, errors.New("metadata journal record exceeds size limit")
		}
		record, err := decodeJournalRecord(line)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("metadata journal at offset %d: %w", offset, err)
		}
		if len(records) > 0 {
			last := records[len(records)-1]
			if record.revision != last.revision+1 {
				return nil, 0, 0, errors.New("metadata journal revision gap or duplicate")
			}
			if record.prev != last.checksum {
				return nil, 0, 0, errors.New("metadata journal checksum chain mismatch")
			}
		}
		records = append(records, record)
		if len(records) > journalMaxEntries {
			return nil, 0, 0, errors.New("metadata journal exceeds bounded replay limit")
		}
		offset += newline + 1
	}
	return records, int64(offset), int64(len(encoded)), nil
}

func writeJournalRecord(file *os.File, record []byte) error {
	for len(record) > 0 {
		written, err := file.Write(record)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		record = record[written:]
	}
	return nil
}

func openJournalFile(directory string) (*os.File, error) {
	path := filepath.Join(directory, metadataJournalName)
	descriptor, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_APPEND|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if err := privateFile(path, false); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func createJournalFile(directory string) (*os.File, error) {
	path := filepath.Join(directory, metadataJournalName)
	descriptor, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_APPEND|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if err := privateFile(path, false); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}
	if err := syncDirectory(directory); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}
