package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const operationJournalVersion = 1

// A durable operation mutation is one appended record plus one fsync. A periodic
// snapshot replaces "rewrite the whole bounded set twice per commit" with "append
// the delta, materialize the set once every operationJournalCompactRecords", which
// keeps the serial commit cost independent of the retained set.
const operationJournalCompactRecords = 256
const operationJournalMaximumBytes = 8 << 20
const operationJournalMaximumRecordBytes = 1 << 20

// journalMutation is one durable delta of the operation view. The registry stays
// the authority for the whole view; a mutation carries only what changed after the
// registry revision it binds to.
type journalMutation struct {
	Version      uint32                     `json:"version"`
	Revision     uint64                     `json:"revision"`
	BaseRevision uint64                     `json:"base_revision"`
	Previous     string                     `json:"previous"`
	Sequence     uint64                     `json:"sequence"`
	Evictions    uint64                     `json:"evictions"`
	Set          map[string]OperationRecord `json:"set,omitempty"`
	Remove       []string                   `json:"remove,omitempty"`
}

type journalEntry struct {
	Payload journalMutation `json:"payload"`
	Hash    string          `json:"hash"`
}

// operationSnapshot materializes the bounded set so replay never walks the whole
// chain. It binds the chain position it was taken at, so a snapshot that is not
// provably part of the chain is refused instead of trusted.
type operationSnapshot struct {
	Version      uint32                     `json:"version"`
	BaseRevision uint64                     `json:"base_revision"`
	Revision     uint64                     `json:"revision"`
	Digest       string                     `json:"digest"`
	Sequence     uint64                     `json:"sequence"`
	Evictions    uint64                     `json:"evictions"`
	Records      map[string]OperationRecord `json:"records"`
}

// journalWitness is the rollback witness at offset zero of the log. It is written
// after the record it describes was fsynced, so it can never claim a revision that
// is not durable, and the next append's fsync flush covers it because it lives in
// the same file. Losing it costs rollback detection, never recovery.
type journalWitness struct {
	Version  uint32 `json:"version"`
	Revision string `json:"revision"`
	Digest   string `json:"digest"`
	Checksum string `json:"checksum"`
}

type operationDelta struct {
	Set    map[string]OperationRecord
	Remove []string
}

func (delta operationDelta) empty() bool { return len(delta.Set) == 0 && len(delta.Remove) == 0 }

// operationDeltaBetween reports the durable difference between two operation
// views. Records are comparable, so a rewrite that changes nothing needs no
// barrier at all.
func operationDeltaBetween(previous, next map[string]OperationRecord) operationDelta {
	delta := operationDelta{}
	for key, record := range next {
		if before, exists := previous[key]; !exists || before != record {
			if delta.Set == nil {
				delta.Set = map[string]OperationRecord{}
			}
			delta.Set[key] = record
		}
	}
	for key := range previous {
		if _, exists := next[key]; !exists {
			delta.Remove = append(delta.Remove, key)
		}
	}
	sort.Strings(delta.Remove)
	return delta
}

type operationJournal struct {
	mutex        sync.Mutex
	path         string
	lock         *os.File
	file         *os.File
	revision     uint64
	digest       string
	size         int64
	records      []journalEntry
	snapshot     *operationSnapshot
	appends      uint64
	appendsTotal uint64
	syncs        uint64
	compactions  uint64
	compactionNS uint64
	closed       bool
	poisoned     bool
}

func journalWitnessLine(revision uint64, digest string) []byte {
	if digest == "" {
		digest = strings.Repeat("0", 64)
	}
	witness := journalWitness{Version: operationJournalVersion, Revision: fmt.Sprintf("%020d", revision), Digest: digest, Checksum: witnessChecksum(revision, digest)}
	encoded, _ := json.Marshal(witness)
	return append(encoded, '\n')
}

func witnessChecksum(revision uint64, digest string) string {
	return digestHex([]byte(fmt.Sprintf("%d|%020d|%s", operationJournalVersion, revision, digest)))
}

// The witness line has a constant width so it can be rewritten in place without
// moving a single record.
var operationJournalWitnessBytes = int64(len(journalWitnessLine(0, "")))

func operationJournalSnapshotPath(directory string) string {
	return filepath.Join(directory, "snapshot")
}

// readOperationJournalWitness decodes the witness line. A torn or foreign line is
// reported as absent: it proves nothing, and refusing here would turn a crash
// during the in-place witness write into an outage.
func readOperationJournalWitness(encoded []byte) (journalWitness, bool) {
	var witness journalWitness
	if int64(len(encoded)) != operationJournalWitnessBytes-1 || strictDecode(encoded, &witness) != nil || witness.Version != operationJournalVersion {
		return journalWitness{}, false
	}
	revision, err := parseJournalRevision(witness.Revision)
	if err != nil || len(witness.Digest) != 64 || witness.Checksum != witnessChecksum(revision, witness.Digest) {
		return journalWitness{}, false
	}
	return witness, true
}

func parseJournalRevision(encoded string) (uint64, error) {
	if len(encoded) != 20 {
		return 0, errors.New("invalid journal revision width")
	}
	revision, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil || fmt.Sprintf("%020d", revision) != encoded {
		return 0, errors.New("invalid journal revision")
	}
	return revision, nil
}

func allowedJournalFile(entry os.DirEntry) bool {
	name := entry.Name()
	if strings.HasPrefix(name, ".private-") && strings.HasSuffix(name, ".tmp") {
		return entry.Type().IsRegular()
	}
	return (name == "journal" || name == "journal.lock" || name == "snapshot") && entry.Type().IsRegular()
}

// openOperationJournal opens and replays the append log. Recovery only accepts a
// verified prefix of the recorded deltas: it truncates an unverifiable tail (an
// append whose fsync never returned was never acknowledged) and refuses when the
// witness proves that a verified record is missing.
func openOperationJournal(directory string) (*operationJournal, error) {
	path := filepath.Join(directory, "journal")
	if err := privateDirectory(directory); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	fresh := len(entries) == 0
	if !fresh {
		if err := privatePath(path+".lock", false); err != nil {
			return nil, errors.New("missing initialization marker")
		}
	}
	for _, entry := range entries {
		if !allowedJournalFile(entry) {
			return nil, errors.New("unexpected private operation file")
		}
	}
	lock, err := lockPath(path + ".lock")
	if err != nil {
		return nil, err
	}
	journal := &operationJournal{path: path, lock: lock}
	ok := false
	defer func() {
		if !ok {
			journal.close()
		}
	}()
	if fresh {
		// The witness line is durable before the first record, so a crash right after
		// creation leaves a readable log instead of an empty file that fails closed.
		if err := atomicPrivate(path, journalWitnessLine(0, "")); err != nil {
			return nil, err
		}
	}
	descriptor, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	journal.file = os.NewFile(uintptr(descriptor), path)
	if err := journal.replay(); err != nil {
		return nil, err
	}
	ok = true
	return journal, nil
}

func (journal *operationJournal) replay() error {
	encoded, err := readOperationJournalBytes(journal.path, operationJournalMaximumBytes)
	if err != nil {
		return err
	}
	witness, witnessed := readOperationJournalWitness(encoded[:operationJournalWitnessBytes-1])
	if err := journal.loadSnapshot(); err != nil {
		return err
	}
	entries, consumed := journal.parse(encoded[operationJournalWitnessBytes:])
	journal.size = operationJournalWitnessBytes + consumed
	if journal.size != int64(len(encoded)) {
		// The unverifiable tail is an append that never returned success; drop it
		// instead of refusing to start after an ordinary crash.
		if err := journal.file.Truncate(journal.size); err != nil {
			return err
		}
	}
	if len(entries) > 0 {
		journal.records = entries
		journal.revision = entries[len(entries)-1].Payload.Revision
		journal.digest = entries[len(entries)-1].Hash
	}
	if journal.snapshot != nil && journal.snapshot.Revision > journal.revision {
		// The log was truncated at the snapshot, so the snapshot carries the head.
		journal.revision = journal.snapshot.Revision
		journal.digest = journal.snapshot.Digest
	}
	if err := journal.verifySnapshot(entries); err != nil {
		return err
	}
	return journal.verifyWitness(witness, witnessed)
}

func readOperationJournalBytes(path string, maximum int64) ([]byte, error) {
	if err := privatePath(path, false); err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil || before.Size() < operationJournalWitnessBytes || before.Size() > maximum {
		return nil, errors.New("operation journal missing or oversized")
	}
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	defer file.Close()
	actual, err := file.Stat()
	if err != nil || !os.SameFile(before, actual) || !actual.Mode().IsRegular() || actual.Mode().Perm()&0077 != 0 {
		return nil, errors.New("operation journal changed")
	}
	encoded := make([]byte, actual.Size())
	if _, err := file.ReadAt(encoded, 0); err != nil {
		return nil, errors.New("operation journal unreadable")
	}
	return encoded, nil
}

// parse verifies the chain and returns the entries that verified together with the
// byte length they occupy. Only a tail that fails to verify may be dropped; a
// corrupted record in the middle stops the prefix and is then caught by the
// witness check.
func (journal *operationJournal) parse(encoded []byte) ([]journalEntry, int64) {
	entries := []journalEntry{}
	consumed := int64(0)
	previousRevision, previousHash := uint64(0), ""
	for len(encoded) > 0 {
		end := bytes.IndexByte(encoded, '\n')
		if end < 0 {
			break
		}
		line := encoded[:end]
		encoded = encoded[end+1:]
		var entry journalEntry
		if len(line) > operationJournalMaximumRecordBytes || strictDecode(line, &entry) != nil {
			break
		}
		body, err := json.Marshal(entry.Payload)
		if err != nil || entry.Payload.Version != operationJournalVersion || entry.Hash != digestHex(body) {
			break
		}
		continuous := entry.Payload.Revision == previousRevision+1 && entry.Payload.Previous == previousHash
		if previousRevision == 0 {
			// Genesis either starts the log or continues the snapshot the log was
			// truncated at.
			continuous = entry.Payload.Revision == 1 && entry.Payload.Previous == ""
			if journal.snapshot != nil {
				continuous = entry.Payload.Revision == journal.snapshot.Revision+1 && entry.Payload.Previous == journal.snapshot.Digest
			}
		}
		if !continuous {
			break
		}
		previousRevision, previousHash = entry.Payload.Revision, entry.Hash
		entries = append(entries, entry)
		consumed += int64(end) + 1
	}
	return entries, consumed
}

func (journal *operationJournal) loadSnapshot() error {
	path := operationJournalSnapshotPath(filepath.Dir(journal.path))
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	var snapshot operationSnapshot
	if err := readDurableBounded(path, operationJournalMaximumBytes, &snapshot); err != nil {
		return err
	}
	if snapshot.Version != operationJournalVersion || snapshot.Revision == 0 || len(snapshot.Digest) != 64 || snapshot.Records == nil {
		return errors.New("invalid operation snapshot")
	}
	journal.snapshot = &snapshot
	return nil
}

// verifySnapshot proves the snapshot is a position of the surviving chain: either
// the log still carries that revision's record, or the log was truncated there.
func (journal *operationJournal) verifySnapshot(entries []journalEntry) error {
	if journal.snapshot == nil {
		return nil
	}
	for _, entry := range entries {
		if entry.Payload.Revision == journal.snapshot.Revision {
			if entry.Hash != journal.snapshot.Digest {
				return errors.New("operation snapshot does not bind the chain")
			}
			return nil
		}
	}
	if len(entries) > 0 && journal.snapshot.Revision < entries[0].Payload.Revision {
		// The snapshot is older than the surviving chain and its record is gone:
		// replay cannot prove what it materialized, so it is refused.
		if journal.snapshot.Revision+1 != entries[0].Payload.Revision || journal.snapshot.Digest != entries[0].Payload.Previous {
			return errors.New("operation snapshot does not bind the chain")
		}
	}
	return nil
}

// verifyWitness refuses when the durable chain cannot cover the witness. Dropping a
// witnessed revision would silently remove an acknowledged mutation, so it is
// treated as a rollback instead of a recoverable tail.
func (journal *operationJournal) verifyWitness(witness journalWitness, witnessed bool) error {
	if !witnessed {
		return nil
	}
	revision, err := parseJournalRevision(witness.Revision)
	if err != nil {
		return nil
	}
	if revision == 0 {
		return nil
	}
	if revision > journal.revision {
		return errors.New("operation journal rollback detected")
	}
	for _, entry := range journal.records {
		if entry.Payload.Revision == revision {
			if entry.Hash != witness.Digest {
				return errors.New("operation journal chain mismatch")
			}
			return nil
		}
	}
	if journal.snapshot != nil && journal.snapshot.Revision == revision && journal.snapshot.Digest == witness.Digest {
		return nil
	}
	return errors.New("operation journal chain mismatch")
}

func (journal *operationJournal) snapshotPath() string {
	return operationJournalSnapshotPath(filepath.Dir(journal.path))
}

func (journal *operationJournal) available() error {
	if journal.closed {
		return ErrStateClosed
	}
	if journal.poisoned {
		return ErrStateUncertain
	}
	return nil
}

// The log is appended through a held descriptor, so a path that was replaced or
// removed underneath would silently orphan acknowledged mutations. Identity is
// verified on every append instead of only when the log is opened.
func (journal *operationJournal) verifyPathIdentity() error {
	info, err := os.Lstat(journal.path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("operation journal replaced")
	}
	actual, err := journal.file.Stat()
	if err != nil || !os.SameFile(info, actual) {
		return errors.New("operation journal replaced")
	}
	return nil
}

// appendOperations records one durable delta and returns only after the record's
// fsync. The caller holds the state lock, so the view and the delta describe the
// same commit.
func (journal *operationJournal) appendOperations(view State, delta operationDelta) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if err := journal.available(); err != nil {
		return err
	}
	if err := journal.verifyPathIdentity(); err != nil {
		journal.poisoned = true
		return ErrStateUncertain
	}
	if journal.revision == ^uint64(0) {
		return ErrOperationCapacity
	}
	mutation := journalMutation{Version: operationJournalVersion, Revision: journal.revision + 1, BaseRevision: view.Revision, Previous: journal.digest, Sequence: view.OperationSequence, Evictions: view.OperationEvictions, Set: delta.Set, Remove: delta.Remove}
	encoded, err := json.Marshal(mutation)
	if err != nil || len(encoded) > operationJournalMaximumRecordBytes {
		return errors.New("invalid operation mutation")
	}
	hash := digestHex(encoded)
	line, err := json.Marshal(journalEntry{Payload: mutation, Hash: hash})
	if err != nil {
		return errors.New("invalid operation mutation")
	}
	line = append(line, '\n')
	if _, err := journal.file.WriteAt(line, journal.size); err != nil {
		journal.poisoned = true
		return ErrStateUncertain
	}
	if err := journal.file.Sync(); err != nil {
		journal.poisoned = true
		return ErrStateUncertain
	}
	journal.syncs++
	journal.size += int64(len(line))
	journal.revision = mutation.Revision
	journal.digest = hash
	journal.records = append(journal.records, journalEntry{Payload: mutation, Hash: hash})
	journal.appends++
	journal.appendsTotal++
	if err := journal.writeWitness(); err != nil {
		journal.poisoned = true
		return ErrStateUncertain
	}
	if journal.appends >= operationJournalCompactRecords || journal.size >= operationJournalMaximumBytes {
		if err := journal.compact(view); err != nil {
			journal.poisoned = true
			return ErrStateUncertain
		}
	}
	return nil
}

func (journal *operationJournal) writeWitness() error {
	_, err := journal.file.WriteAt(journalWitnessLine(journal.revision, journal.digest), 0)
	return err
}

// compact materializes the view so replay stays bounded. The snapshot is durable
// before the log is truncated, so a crash between the two is recovered by replay:
// the snapshot binds a chain position the surviving records still verify.
func (journal *operationJournal) compact(view State) error {
	started := time.Now()
	defer func() { journal.compactionNS += uint64(time.Since(started)) }()
	snapshot := operationSnapshot{Version: operationJournalVersion, BaseRevision: view.Revision, Revision: journal.revision, Digest: journal.digest, Sequence: view.OperationSequence, Evictions: view.OperationEvictions, Records: view.Operations}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > operationJournalMaximumBytes {
		return errors.New("invalid operation snapshot")
	}
	if err := atomicPrivate(journal.snapshotPath(), encoded); err != nil {
		return err
	}
	if err := journal.file.Sync(); err != nil {
		return err
	}
	if err := journal.file.Truncate(operationJournalWitnessBytes); err != nil {
		return err
	}
	journal.size = operationJournalWitnessBytes
	journal.records = nil
	journal.appends = 0
	journal.snapshot = &snapshot
	journal.compactions++
	return nil
}

// applyView joins the durable deltas onto a view whose revision is base. It is the
// single replay rule shared by recovery and by the registry commit check.
func (journal *operationJournal) applyView(base uint64, state *State) bool {
	changed := false
	applied := uint64(0)
	if journal.snapshot != nil && journal.snapshot.BaseRevision == base {
		applyOperationSnapshot(state, *journal.snapshot)
		applied = journal.snapshot.Revision
		changed = true
	}
	for _, entry := range journal.records {
		if entry.Payload.BaseRevision != base || entry.Payload.Revision <= applied {
			continue
		}
		applyJournalMutation(state, entry.Payload)
		changed = true
	}
	return changed
}

func applyOperationSnapshot(state *State, snapshot operationSnapshot) {
	// The decoded snapshot belongs to the open journal, so the recovered state gets
	// its own copy instead of aliasing a map the journal still holds.
	records := make(map[string]OperationRecord, len(snapshot.Records))
	for key, record := range snapshot.Records {
		records[key] = record
	}
	state.Operations = records
	state.OperationSequence = snapshot.Sequence
	state.OperationEvictions = snapshot.Evictions
}

func applyJournalMutation(state *State, mutation journalMutation) {
	if state.Operations == nil {
		state.Operations = map[string]OperationRecord{}
	}
	for key, record := range mutation.Set {
		state.Operations[key] = record
	}
	for _, key := range mutation.Remove {
		delete(state.Operations, key)
	}
	state.OperationSequence = mutation.Sequence
	state.OperationEvictions = mutation.Evictions
}

func (journal *operationJournal) close() error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	if journal.file == nil {
		return errors.Join(syscall.Flock(int(journal.lock.Fd()), syscall.LOCK_UN), journal.lock.Close())
	}
	// A clean shutdown makes the witness current, so the next open proves every
	// acknowledged mutation is still present.
	return errors.Join(journal.file.Sync(), journal.file.Close(), syscall.Flock(int(journal.lock.Fd()), syscall.LOCK_UN), journal.lock.Close())
}
