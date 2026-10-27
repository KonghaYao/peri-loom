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
	Audit        *auditRecord               `json:"audit,omitempty"`
}

type journalEntry struct {
	Payload journalMutation `json:"payload"`
	Hash    string          `json:"hash"`
}

// operationSnapshotEnvelopeVersion is the snapshot envelope this binary writes.
// Version zero is what a snapshot written before the envelope decodes as: it binds the
// chain position but nothing about its own content, so it is only accepted while the
// surviving chain still proves that position, and replay re-seals it in the current
// envelope before the store opens. A later envelope version must not be read with this
// binary's encoding rules.
const operationSnapshotEnvelopeVersion = 2

// operationSnapshotScope names what a snapshot covers: the platform operation view of
// one state directory, not one DatabaseID's journal. It is a constant marker instead
// of the directory path, because a path is not a durable storage identity.
const operationSnapshotScope = "state_directory/platform.operations"

// operationSnapshotOrigin records how the content of a snapshot was accepted. Content
// that only a pre-envelope snapshot could vouch for keeps saying so after the reseal, so
// the directory's own artifact shows that a migration happened instead of looking like a
// snapshot that was always bound. The marker is advisory, not a control: a writer able to
// replace the file can set or drop it, which is why it stays out of the content digest
// and no verification decision depends on it beyond rejecting values this binary cannot
// interpret.
const (
	operationSnapshotOriginNative = "native"
	operationSnapshotOriginReseal = "legacy_reseal"
)

// operationSnapshot materializes the bounded set so replay never walks the whole
// chain. It binds the chain position it was taken at, so a snapshot that is not
// provably part of the chain is refused instead of trusted.
//
// A chain position proves where a snapshot was taken, never what it stored there:
// dropping an unresolved record keeps every position field, keeps the JSON legal, and
// erases the isolation fact the compacted prefix no longer restates. The envelope
// therefore states the format, the scope that was materialized and a digest of the
// content itself, so a snapshot that no longer matches what was written at that
// position is refused instead of applied.
type operationSnapshot struct {
	EnvelopeVersion uint32                     `json:"envelope_version"`
	Scope           string                     `json:"scope"`
	Version         uint32                     `json:"version"`
	BaseRevision    uint64                     `json:"base_revision"`
	Revision        uint64                     `json:"revision"`
	Digest          string                     `json:"digest"`
	Sequence        uint64                     `json:"sequence"`
	Evictions       uint64                     `json:"evictions"`
	Records         map[string]OperationRecord `json:"records"`
	ContentDigest   string                     `json:"content_digest"`
	Origin          string                     `json:"snapshot_origin"`
	CompletionAudit *auditHead                 `json:"completion_audit,omitempty"`
}

// contentDigest is the single canonical encoding of a snapshot: the writer and the
// loader both call it, so the two sides cannot drift into different format knowledge.
// encoding/json orders map keys, which makes the payload byte-stable for a given value
// instead of depending on Go's map iteration order. The envelope version selects the
// encoding and the digest cannot cover itself, so neither is part of the input. The
// payload is derived from the record structs, so changing their encoded shape changes
// the canonical payload and needs a new envelope version, not a silent edit here.
func (snapshot operationSnapshot) contentDigest() (string, error) {
	content := map[string]any{
		"envelope_version": snapshot.EnvelopeVersion,
		"scope":            snapshot.Scope,
		"version":          snapshot.Version,
		"base_revision":    snapshot.BaseRevision,
		"revision":         snapshot.Revision,
		"digest":           snapshot.Digest,
		"sequence":         snapshot.Sequence,
		"evictions":        snapshot.Evictions,
		"records":          snapshot.Records,
	}
	if snapshot.CompletionAudit != nil {
		content["completion_audit"] = snapshot.CompletionAudit
	}
	payload, err := json.Marshal(content)
	if err != nil {
		return "", errors.New("invalid operation snapshot")
	}
	return digestHex(payload), nil
}

// sealSnapshot stamps the envelope of a snapshot that is about to be written. Content
// is never rewritten: the digest covers exactly the records and counters stored
// beside it.
func sealSnapshot(snapshot *operationSnapshot, origin string) error {
	snapshot.EnvelopeVersion = operationSnapshotEnvelopeVersion
	snapshot.Scope = operationSnapshotScope
	snapshot.Origin = origin
	digest, err := snapshot.contentDigest()
	if err != nil {
		return err
	}
	snapshot.ContentDigest = digest
	return nil
}

// verifySnapshotEnvelope checks a decoded snapshot against its own envelope before
// replay trusts a single record from it.
func verifySnapshotEnvelope(snapshot operationSnapshot) error {
	if (snapshot.Version != operationJournalVersion && snapshot.Version != completionJournalVersion) || snapshot.Revision == 0 || len(snapshot.Digest) != 64 || snapshot.Records == nil {
		return errors.New("invalid operation snapshot")
	}
	if snapshot.EnvelopeVersion == 0 {
		// Legacy snapshot: it carries no content binding, so all that can be checked is
		// its chain position and replay has to re-seal it.
		return nil
	}
	if snapshot.EnvelopeVersion != operationSnapshotEnvelopeVersion {
		return errors.New("unsupported operation snapshot envelope")
	}
	if snapshot.Scope != operationSnapshotScope {
		return errors.New("operation snapshot scope mismatch")
	}
	if snapshot.Origin != operationSnapshotOriginNative && snapshot.Origin != operationSnapshotOriginReseal {
		return errors.New("unknown operation snapshot origin")
	}
	digest, err := snapshot.contentDigest()
	if err != nil || len(snapshot.ContentDigest) != 64 || digest != snapshot.ContentDigest {
		return errors.New("operation snapshot content digest mismatch")
	}
	return nil
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
	mutex                sync.Mutex
	path                 string
	lock                 *os.File
	file                 *os.File
	revision             uint64
	digest               string
	size                 int64
	records              []journalEntry
	snapshot             *operationSnapshot
	appends              uint64
	appendsTotal         uint64
	ordered              uint64
	syncs                uint64
	compactions          uint64
	compactionNS         uint64
	compactionMaxNS      uint64
	compactionFailures   uint64
	compactionSnapshotNS uint64
	compactionSyncNS     uint64
	compactionTruncateNS uint64
	closed               bool
	poisoned             bool
	completion           *completionGenesis
	auditHead            auditHead
	orderedAudit         auditHead
	retainedBytes        int64
	archivedRevision     uint64
	queuedBytes          int64
	maintenancePending   bool

	// The ordered commit queue. The queue lock covers bookkeeping only, so a submitter
	// never queues behind a barrier; write serializes the file itself and is held by the
	// committer (and by a compaction) alone.
	commits        []*operationCommit
	orderedTail    *operationCommit
	committer      bool
	writing        bool
	write          sync.Mutex
	committerGroup sync.WaitGroup
	// Observed maxima, not the configured limits: a reader compares them with
	// operation_journal_queue_limit and operation_journal_batch_limit.
	queueMaximum int
	batchMaximum int
	coalesced    uint64
	waitNS       uint64
	// One batch pays several separable costs. They are accumulated apart so a slow
	// device, a large encode, or an identity check can be told apart in metrics.
	encodeNS   uint64
	verifyNS   uint64
	assembleNS uint64
	writeNS    uint64
	syncNS     uint64
	witnessNS  uint64
	waitMaxNS  uint64
	// testPopGate holds the committer before it chooses a batch, and testWriteGate holds it
	// inside one, so a test can make either side of the queue deterministic. Production
	// leaves both nil; no request path observes them.
	testPopGate        func()
	testWriteGate      func()
	testSyncGate       func()
	testCompactionGate func(string) error
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
	return (name == "journal" || name == "journal.lock" || name == "snapshot" || name == "completion.genesis" || completionArchiveName(name)) && entry.Type().IsRegular()
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
	if err := journal.loadCompletionGenesis(); err != nil {
		return nil, err
	}
	if err := journal.replay(); err != nil {
		return nil, err
	}
	ok = true
	return journal, nil
}

func (journal *operationJournal) replay() error {
	maximum := int64(operationJournalMaximumBytes)
	if journal.completion != nil {
		maximum = completionActiveMaximumBytes
	}
	encoded, err := readOperationJournalBytes(journal.path, maximum)
	if err != nil {
		return err
	}
	witness, witnessed := readOperationJournalWitness(encoded[:operationJournalWitnessBytes-1])
	if err := journal.loadSnapshot(); err != nil {
		return err
	}
	entries, consumed := journal.parse(encoded[operationJournalWitnessBytes:])
	journal.size = operationJournalWitnessBytes + consumed
	if journal.completion != nil && journal.size != int64(len(encoded)) && bytes.IndexByte(encoded[journal.size:], '\n') >= 0 {
		return errors.New("corrupt complete completion record")
	}
	if journal.size != int64(len(encoded)) {
		// The unverifiable tail is an append that never returned success; drop it
		// instead of refusing to start after an ordinary crash.
		if journal.completion == nil {
			if err := journal.file.Truncate(journal.size); err != nil {
				return err
			}
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
	if err := journal.verifyWitness(witness, witnessed); err != nil {
		return err
	}
	// Only a snapshot whose position just verified is re-sealed; a refused one never
	// reaches this point, so unproven content is never stamped as bound content.
	if err := journal.resealLegacySnapshot(); err != nil {
		return err
	}
	if err := journal.verifyCompletionHistory(); err != nil {
		return err
	}
	if journal.completion != nil && journal.size != int64(len(encoded)) {
		return journal.file.Truncate(journal.size)
	}
	return nil
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
		maximum := operationJournalMaximumRecordBytes
		if journal.completion != nil {
			maximum = completionRecordReservationBytes
		}
		if len(line) > maximum || strictDecode(line, &entry) != nil {
			break
		}
		body, err := json.Marshal(entry.Payload)
		validVersion := entry.Payload.Version == operationJournalVersion || journal.completion != nil && entry.Payload.Version == completionJournalVersion
		if err != nil || len(body) > operationJournalMaximumRecordBytes || !validVersion || entry.Hash != digestHex(body) || entry.Payload.Audit != nil && entry.Payload.Version != completionJournalVersion {
			break
		}
		continuous := entry.Payload.Revision == previousRevision+1 && entry.Payload.Previous == previousHash
		if previousRevision == 0 {
			// Genesis either starts the log or continues the snapshot the log was
			// truncated at.
			continuous = entry.Payload.Revision == 1 && entry.Payload.Previous == ""
			if journal.snapshot != nil {
				continuous = entry.Payload.Revision == journal.snapshot.Revision+1 && entry.Payload.Previous == journal.snapshot.Digest
				if journal.completion != nil && entry.Payload.Revision <= journal.snapshot.Revision {
					// An archived prefix may still be active when a crash lands
					// between the snapshot rename and the log truncation. Its
					// first position is proved against the retained archives by
					// verifyCompletionHistory, never trusted on position alone.
					continuous = entry.Payload.Revision == 1 && entry.Payload.Previous == "" || entry.Payload.Revision > 1 && len(entry.Payload.Previous) == 64
				}
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
	if err := verifySnapshotEnvelope(snapshot); err != nil {
		return err
	}
	journal.snapshot = &snapshot
	return nil
}

// resealLegacySnapshot rewrites a snapshot that predates the envelope, so the window in
// which a durable view is trusted without a content binding is the one startup whose
// chain position replay already verified, instead of every startup. The content is
// written back unchanged and a failure refuses the open: a legacy directory that cannot
// be re-sealed must not keep reopening on an unprovable view.
func (journal *operationJournal) resealLegacySnapshot() error {
	if journal.snapshot == nil || journal.snapshot.EnvelopeVersion != 0 {
		return nil
	}
	snapshot := *journal.snapshot
	if err := sealSnapshot(&snapshot, operationSnapshotOriginReseal); err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > operationJournalMaximumBytes {
		return errors.New("invalid operation snapshot")
	}
	if err := atomicPrivate(journal.snapshotPath(), encoded); err != nil {
		return err
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

// appendOperations records one durable delta and returns only after its barrier. It is the
// synchronous single-commit form of submit, for callers that hold no view anyone else must
// settle. The state store submits instead, because its in-place edit has to stay visible to
// whoever settles the commit.
func (journal *operationJournal) appendOperations(view State, delta operationDelta) error {
	commit, err := journal.submit(view, delta)
	if err != nil {
		return err
	}
	return journal.await(commit)
}

// writeWitnessFor moves the rollback witness to the batch that was just made durable. It
// describes the last record of that batch, never a revision the file has not reached.
func (journal *operationJournal) writeWitnessFor(commit *operationCommit) error {
	_, err := journal.file.WriteAt(journalWitnessLine(commit.revision, commit.hash), 0)
	return err
}

// compact materializes the view so replay stays bounded. The snapshot is durable
// before the log is truncated, so a crash between the two is recovered by replay:
// the snapshot binds a chain position the surviving records still verify.
func (journal *operationJournal) compact(view State) error {
	started := time.Now()
	if journal.completion != nil {
		if err := journal.archiveCompletionPrefix(); err != nil {
			journal.recordCompactionCost(started, operationCompactionTimings{}, err)
			return err
		}
	}
	compaction := journal.captureCompaction(view)
	timings, err := journal.persistCompaction(compaction)
	journal.recordCompactionCost(started, timings, err)
	if err == nil {
		journal.installCompaction(compaction)
	}
	return err
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

// close stops accepting appends, fails the ones that were never written, waits for the
// barrier already in flight, and only then releases the file. A commit that was written
// still reports its own result: its bytes are durable, and telling its caller otherwise
// would claim a mutation it owns was lost.
func (journal *operationJournal) close() error {
	journal.mutex.Lock()
	if journal.closed {
		journal.mutex.Unlock()
		return nil
	}
	journal.closed = true
	queued := journal.commits
	journal.commits = nil
	journal.mutex.Unlock()
	for _, commit := range queued {
		commit.err = ErrStateClosed
		close(commit.settled)
	}
	journal.committerGroup.Wait()
	// The committer is gone, so the file has no writer left but a compaction could still
	// be in flight: the write lock is the boundary that says the file is ours to close.
	journal.write.Lock()
	defer journal.write.Unlock()
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.file == nil {
		return errors.Join(syscall.Flock(int(journal.lock.Fd()), syscall.LOCK_UN), journal.lock.Close())
	}
	// A clean shutdown makes the witness current, so the next open proves every
	// acknowledged mutation is still present.
	return errors.Join(journal.file.Sync(), journal.file.Close(), syscall.Flock(int(journal.lock.Fd()), syscall.LOCK_UN), journal.lock.Close())
}
