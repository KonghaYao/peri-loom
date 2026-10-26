package platform

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	controller "peri-loom/controller"
)

type auditHead struct {
	Version  uint32 `json:"version"`
	Sequence uint64 `json:"sequence"`
	Hash     string `json:"hash"`
}
type auditPayload struct {
	Version  uint32     `json:"version"`
	Sequence uint64     `json:"sequence"`
	Previous string     `json:"previous"`
	Event    AuditEvent `json:"event"`
}
type auditRecord struct {
	Payload auditPayload `json:"payload"`
	Hash    string       `json:"hash"`
}

const auditSegmentBytes = 64 << 20

// Audit admission budgets. The per-class limits are separate so a rejection
// storm cannot take the capacity an accepted request needs to settle, and the
// global limits are the memory ceiling over all classes including reserved
// completion slots. Counts include the batch currently being written: a slow
// fsync must not let admission run ahead of the memory the queue already owns.
const (
	auditQueueMaximum          = 4096
	auditQueueBytes            = 8 << 20
	auditRequestQueueMaximum   = 1024
	auditRequestQueueBytes     = 2 << 20
	auditRejectionQueueMaximum = 256
	auditRejectionQueueBytes   = 512 << 10
	auditCompletionMaximum     = 1024
	auditCompletionBytes       = 4 << 20
	// A reservation is charged this much before the event exists, which is what
	// makes the slot itself prove the byte budget. validateAudit bounds every
	// field to 128 ASCII characters, so a real event stays well below it.
	auditCompletionSlotBytes = 4096
	// The chain envelope (version, sequence, previous hash, record hash and JSON
	// punctuation) surrounds one event; it is charged to the queue budget too.
	auditRecordEnvelopeBytes = 256
)

// ErrAuditOverloaded is returned before a record is accepted when the queue or
// one of its class budgets is exhausted. It is a definite refusal, not a
// durability result: the caller decides how to answer and nothing was dropped
// silently.
var ErrAuditOverloaded = errors.New("audit admission capacity exhausted")

type auditRotation struct {
	Version  uint32 `json:"version"`
	Start    uint64 `json:"start"`
	Sequence uint64 `json:"sequence"`
	Hash     string `json:"hash"`
}

type auditRequest struct {
	event           AuditEvent
	class           AuditClass
	bytes           int
	enqueued        time.Time
	done            chan error
	operationChange func(*operationEdit) error
	failure         error
}

// auditReservation moves one completion slot from the reserve into the queue.
// The charge is fixed so ReserveCompletion proves the byte budget too.
type auditReservation struct {
	audit   *fileAudit
	charged int
	active  bool
}

// auditWitness proves one chain position survived: recovery has to find the exact
// sequence and hash, not merely reach the sequence. The head file and the rotation
// intent are periodic witnesses now, so a chain that is longer than they remember
// is expected, while a chain that is shorter or rewritten is refused.
type auditWitness struct {
	sequence uint64
	wanted   string
	found    string
	seen     bool
}

func newAuditWitness(sequence uint64, hash string) *auditWitness {
	// The genesis position is implicit: it exists before the first record and no
	// scan can observe it.
	if sequence == 0 {
		return &auditWitness{sequence: sequence, wanted: hash, found: strings.Repeat("0", 64), seen: true}
	}
	return &auditWitness{sequence: sequence, wanted: hash}
}

func (witness *auditWitness) observe(state auditHead) {
	if !witness.seen && state.Sequence == witness.sequence {
		witness.seen, witness.found = true, state.Hash
	}
}

func (witness *auditWitness) verify() error {
	if !witness.seen || witness.found != witness.wanted {
		return errors.New("audit chain does not cover witness")
	}
	return nil
}

type fileAudit struct {
	completionStore *stateStore
	queueMutex      sync.Mutex
	pending         []*auditRequest
	// writing is the batch the single writer is currently persisting. It keeps
	// its admission charge, and its age is reported too, because a stalled
	// barrier is exactly when the queue age matters.
	writing []*auditRequest
	running bool
	closing bool
	writers sync.WaitGroup

	// admitted counts records accepted into the bounded queue, including the
	// batch currently being written. reservedCompletion is capacity held for
	// accepted SQL before its record exists in the queue.
	admitted                [3]int
	admittedBytes           [3]int
	reservedCompletion      int
	reservedCompletionBytes int
	dropped                 uint64
	droppedByClass          [3]uint64
	admittedMaximum         int
	admittedBytesMaximum    int

	// fsync cost is accumulated atomically because recordBatch holds the write
	// lock across the barrier and status must not queue behind it.
	syncNS    atomic.Uint64
	syncCount atomic.Uint64
	syncMaxNS atomic.Uint64

	// testSyncGate holds the writer before the file barrier so a test can make
	// fsync slow deterministically. Production leaves it nil.
	testSyncGate func()

	mutex  sync.Mutex
	head   *durableFile
	file   *os.File
	state  auditHead
	size   int64
	start  uint64
	closed bool
}

func auditEventBytes(event AuditEvent) int {
	encoded, err := json.Marshal(event)
	if err != nil {
		return auditCompletionSlotBytes
	}
	return len(encoded) + auditRecordEnvelopeBytes
}

func (audit *fileAudit) admittedCountLocked() int {
	return audit.admitted[AuditClassRequest] + audit.admitted[AuditClassRejection] + audit.admitted[AuditClassCompletion] + audit.reservedCompletion
}

func (audit *fileAudit) admittedBytesLocked() int {
	return audit.admittedBytes[AuditClassRequest] + audit.admittedBytes[AuditClassRejection] + audit.admittedBytes[AuditClassCompletion] + audit.reservedCompletionBytes
}

func (audit *fileAudit) observeMaximumLocked() {
	if count := audit.admittedCountLocked(); count > audit.admittedMaximum {
		audit.admittedMaximum = count
	}
	if bytes := audit.admittedBytesLocked(); bytes > audit.admittedBytesMaximum {
		audit.admittedBytesMaximum = bytes
	}
}

func (audit *fileAudit) refuseLocked(class AuditClass) {
	audit.dropped++
	if class >= AuditClassRequest && class <= AuditClassCompletion {
		audit.droppedByClass[class]++
	}
}

// admitLocked charges one event to its class and the global budget. It is the
// only place a non-completion record becomes accepted, so a refusal is always
// counted here rather than after the fact.
func (audit *fileAudit) admitLocked(class AuditClass, bytes int) bool {
	maximumCount, maximumBytes := 0, 0
	switch class {
	case AuditClassRequest:
		maximumCount, maximumBytes = auditRequestQueueMaximum, auditRequestQueueBytes
	case AuditClassRejection:
		maximumCount, maximumBytes = auditRejectionQueueMaximum, auditRejectionQueueBytes
	default:
		return false
	}
	if audit.admitted[class]+1 > maximumCount || audit.admittedBytes[class]+bytes > maximumBytes {
		return false
	}
	if audit.admittedCountLocked()+1 > auditQueueMaximum || audit.admittedBytesLocked()+bytes > auditQueueBytes {
		return false
	}
	audit.admitted[class]++
	audit.admittedBytes[class] += bytes
	audit.observeMaximumLocked()
	return true
}

func (audit *fileAudit) enqueueLocked(request *auditRequest) {
	audit.pending = append(audit.pending, request)
	if !audit.running {
		audit.running = true
		audit.writers.Add(1)
		go audit.drain()
	}
}

func (audit *fileAudit) releaseReservationLocked(reservation *auditReservation) {
	if reservation == nil || !reservation.active {
		return
	}
	reservation.active = false
	audit.reservedCompletion--
	audit.reservedCompletionBytes -= reservation.charged
}

func (reservation *auditReservation) Release() {
	if reservation == nil || reservation.audit == nil {
		return
	}
	reservation.audit.queueMutex.Lock()
	reservation.audit.releaseReservationLocked(reservation)
	reservation.audit.queueMutex.Unlock()
}

func (reservation *auditReservation) Record(event AuditEvent) error {
	if reservation == nil || reservation.audit == nil {
		return ErrStateClosed
	}
	return reservation.audit.recordCompletion(event, reservation)
}

// ReserveCompletion takes one completion slot and its fixed byte charge. The
// slot is held from before dispatch until the record is durable, so queued
// traffic that arrived later can never take it away.
func (audit *fileAudit) ReserveCompletion() (AuditReservation, error) {
	audit.queueMutex.Lock()
	defer audit.queueMutex.Unlock()
	if audit.closing {
		return nil, ErrStateClosed
	}
	completionCount := audit.admitted[AuditClassCompletion] + audit.reservedCompletion
	if audit.completionStore != nil && (completionCount >= completionReservedMaximum || !audit.completionStore.operations.completionAdmissionAvailable()) {
		audit.refuseLocked(AuditClassCompletion)
		return nil, ErrAuditOverloaded
	}
	completionBytes := audit.admittedBytes[AuditClassCompletion] + audit.reservedCompletionBytes
	if completionCount+1 > auditCompletionMaximum || completionBytes+auditCompletionSlotBytes > auditCompletionBytes ||
		audit.admittedCountLocked()+1 > auditQueueMaximum || audit.admittedBytesLocked()+auditCompletionSlotBytes > auditQueueBytes {
		audit.refuseLocked(AuditClassCompletion)
		return nil, ErrAuditOverloaded
	}
	audit.reservedCompletion++
	audit.reservedCompletionBytes += auditCompletionSlotBytes
	audit.observeMaximumLocked()
	return &auditReservation{audit: audit, charged: auditCompletionSlotBytes, active: true}, nil
}

// recordCompletion transfers a held slot into the queue without rechecking
// capacity. Admission of the SQL depended on this capacity, so settling it may
// not be refused by traffic that arrived after the reservation.
func (audit *fileAudit) recordCompletion(event AuditEvent, reservation *auditReservation) error {
	return audit.recordCompletionOperation(event, reservation, nil)
}

func (audit *fileAudit) recordCompletionOperation(event AuditEvent, reservation *auditReservation, change func(*operationEdit) error) error {
	if err := validateAudit(event); err != nil {
		reservation.Release()
		return err
	}
	bytes := min(auditEventBytes(event), reservation.charged)
	request := &auditRequest{event: event, class: AuditClassCompletion, bytes: bytes, enqueued: time.Now(), done: make(chan error, 1), operationChange: change}
	audit.queueMutex.Lock()
	if !reservation.active {
		audit.queueMutex.Unlock()
		return ErrStateClosed
	}
	if audit.closing {
		audit.releaseReservationLocked(reservation)
		audit.queueMutex.Unlock()
		return ErrStateClosed
	}
	audit.releaseReservationLocked(reservation)
	audit.admitted[AuditClassCompletion]++
	audit.admittedBytes[AuditClassCompletion] += request.bytes
	audit.observeMaximumLocked()
	audit.enqueueLocked(request)
	audit.queueMutex.Unlock()
	return <-request.done
}

func auditSegmentPath(path string, start uint64) string {
	return fmt.Sprintf("%s.segment.%020d", path, start)
}

func allowedAuditSidecar(base string, entry os.DirEntry) bool {
	name := entry.Name()
	if !entry.Type().IsRegular() {
		return false
	}
	if name == base+".rotate" {
		return true
	}
	prefix := base + ".segment."
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+20 {
		return false
	}
	start, err := strconv.ParseUint(strings.TrimPrefix(name, prefix), 10, 64)
	return err == nil && start != 0 && name == filepath.Base(auditSegmentPath(base, start))
}

func readAuditSegment(path string, actual *auditHead, allowEmpty bool, witnesses ...*auditWitness) (int64, error) {
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	defer file.Close()
	if err := privatePath(path, false); err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil || info.Size() > auditSegmentBytes || (!allowEmpty && info.Size() == 0) {
		return 0, errors.New("audit segment missing, empty or oversized")
	}
	if info.Size() > 0 {
		var tail [1]byte
		if _, err := file.ReadAt(tail[:], info.Size()-1); err != nil || tail[0] != '\n' {
			return 0, errors.New("audit append lacks terminal newline")
		}
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 65536)
	for scanner.Scan() {
		var record auditRecord
		if strictDecode(scanner.Bytes(), &record) != nil || record.Payload.Version != 1 || actual.Sequence == ^uint64(0) || record.Payload.Sequence != actual.Sequence+1 || record.Payload.Previous != actual.Hash || validateAudit(record.Payload.Event) != nil {
			return 0, errors.New("audit chain corrupted")
		}
		encoded, _ := json.Marshal(record.Payload)
		if record.Hash != digestHex(encoded) {
			return 0, errors.New("audit checksum mismatch")
		}
		actual.Sequence++
		actual.Hash = record.Hash
		for _, witness := range witnesses {
			if witness != nil {
				witness.observe(*actual)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func openAuditActive(path string, create bool) (*os.File, error) {
	flags := syscall.O_RDWR | syscall.O_APPEND | syscall.O_NOFOLLOW
	if create {
		flags |= syscall.O_CREAT | syscall.O_EXCL
	}
	descriptor, err := syscall.Open(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if err := privatePath(path, false); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}
	if err := fsyncDirectory(filepath.Dir(path)); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func finishAuditRotation(path string, start uint64, active *os.File) (*os.File, error) {
	archive := auditSegmentPath(path, start)
	if _, err := os.Lstat(archive); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("audit archive already exists or unavailable")
	}
	if err := active.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(path, archive); err != nil {
		return nil, err
	}
	if err := fsyncDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return openAuditActive(path, true)
}

func clearAuditRotation(path string) error {
	if err := os.Remove(path + ".rotate"); err != nil {
		return err
	}
	return fsyncDirectory(filepath.Dir(path))
}

func OpenAudit(path string) (Audit, error) {
	if path == "" {
		return nil, errors.New("private audit path required")
	}
	head, err := openDurable(path+".head", auditHead{Version: 1, Hash: strings.Repeat("0", 64)})
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			head.close()
		}
	}()
	var expected auditHead
	if strictDecode(head.payload, &expected) != nil || expected.Version != 1 || expected.Sequence != head.revision {
		return nil, errors.New("invalid audit head")
	}
	absolute := strings.TrimSuffix(head.path, ".head")
	actual := auditHead{Version: 1, Hash: strings.Repeat("0", 64)}
	entries, err := os.ReadDir(filepath.Dir(absolute))
	if err != nil {
		return nil, err
	}
	headWitness := newAuditWitness(expected.Sequence, expected.Hash)
	// The rotation intent is read before the chain is walked, because its position
	// has to be witnessed by the archived segments too.
	var rotation auditRotation
	rotating := false
	rotationWitness := (*auditWitness)(nil)
	if err := privatePath(absolute+".rotate", false); err == nil {
		rotating = true
		if err := readPrivate(absolute+".rotate", &rotation); err != nil || rotation.Version != 1 || rotation.Start == 0 {
			return nil, errors.New("invalid audit rotation intent")
		}
		rotationWitness = newAuditWitness(rotation.Sequence, rotation.Hash)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	prefix := filepath.Base(absolute) + ".segment."
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		start := actual.Sequence + 1
		if actual.Sequence == ^uint64(0) || entry.Name() != filepath.Base(auditSegmentPath(absolute, start)) {
			return nil, errors.New("audit archive gap or unexpected segment")
		}
		if _, err := readAuditSegment(filepath.Join(filepath.Dir(absolute), entry.Name()), &actual, false, headWitness, rotationWitness); err != nil {
			return nil, err
		}
	}
	start := actual.Sequence + 1
	archived := rotating && start > rotation.Start
	if rotating && ((start != rotation.Start && !archived) || (archived && (actual.Sequence != rotation.Sequence || actual.Hash != rotation.Hash))) {
		return nil, errors.New("audit rotation does not bind chain")
	}
	var file *os.File
	if archived {
		file, err = openAuditActive(absolute, false)
		if errors.Is(err, os.ErrNotExist) {
			file, err = openAuditActive(absolute, true)
		}
		if err != nil {
			return nil, err
		}
	} else {
		file, err = openAuditActive(absolute, head.fresh)
		if err != nil {
			return nil, err
		}
	}
	valid := false
	defer func() {
		if !valid {
			file.Close()
		}
	}()
	size, err := readAuditSegment(absolute, &actual, true, headWitness, rotationWitness)
	if err != nil {
		return nil, err
	}
	if failure := headWitness.verify(); failure != nil {
		return nil, failure
	}
	if rotationWitness != nil {
		if failure := rotationWitness.verify(); failure != nil {
			return nil, failure
		}
	}
	if archived && size != 0 {
		return nil, errors.New("audit rollback or interrupted append/rotation")
	}
	if rotating && !archived && (start != rotation.Start || actual.Sequence != rotation.Sequence || actual.Hash != rotation.Hash || size == 0) {
		return nil, errors.New("audit rollback or interrupted append/rotation")
	}
	if rotating && !archived {
		fresh, err := finishAuditRotation(absolute, start, file)
		if err != nil {
			return nil, err
		}
		file.Close()
		file = fresh
		size = 0
		start = actual.Sequence + 1
	}
	if rotating {
		if err := clearAuditRotation(absolute); err != nil {
			return nil, err
		}
	}
	ok = true
	valid = true
	return &fileAudit{head: head, file: file, state: actual, size: size, start: start}, nil
}

func (audit *fileAudit) Record(event AuditEvent) error {
	return audit.RecordClass(event, AuditClassRequest)
}

func (audit *fileAudit) RecordClass(event AuditEvent, class AuditClass) error {
	if class != AuditClassRequest && class != AuditClassRejection {
		return errors.New("audit class requires its own admission path")
	}
	if err := validateAudit(event); err != nil {
		return err
	}
	request := &auditRequest{event: event, class: class, bytes: auditEventBytes(event), enqueued: time.Now(), done: make(chan error, 1)}
	audit.queueMutex.Lock()
	if audit.closing {
		audit.queueMutex.Unlock()
		return ErrStateClosed
	}
	if !audit.admitLocked(class, request.bytes) {
		audit.refuseLocked(class)
		audit.queueMutex.Unlock()
		return ErrAuditOverloaded
	}
	audit.enqueueLocked(request)
	audit.queueMutex.Unlock()
	return <-request.done
}

// 只合并已有等待者，不为形成批次延迟孤立请求；每个调用都等待自己的持久化结果。
func (audit *fileAudit) drain() {
	defer audit.writers.Done()
	for {
		audit.queueMutex.Lock()
		if len(audit.pending) == 0 {
			audit.running = false
			audit.queueMutex.Unlock()
			return
		}
		count := min(len(audit.pending), auditBatchMaximum)
		batch := append([]*auditRequest(nil), audit.pending[:count]...)
		clear(audit.pending[:count])
		audit.pending = audit.pending[count:]
		audit.writing = batch
		audit.queueMutex.Unlock()
		err := audit.recordBatch(batch)
		// The batch keeps its admission charge until the barrier returns, so a
		// stalled device cannot be outrun by records admitted while it waits.
		audit.queueMutex.Lock()
		for _, request := range batch {
			audit.admitted[request.class]--
			audit.admittedBytes[request.class] -= request.bytes
		}
		audit.writing = nil
		audit.queueMutex.Unlock()
		for _, request := range batch {
			request.done <- errors.Join(err, request.failure)
		}
	}
}

// AuditStatus reports the bounded queue and the cost of its barrier. It takes
// only the bookkeeping lock, so an in-flight fsync never blocks an operator.
func (audit *fileAudit) AuditStatus() map[string]any {
	audit.queueMutex.Lock()
	pendingBytes := 0
	var oldest time.Time
	for index, request := range audit.pending {
		pendingBytes += request.bytes
		if index == 0 {
			oldest = request.enqueued
		}
	}
	if len(audit.writing) > 0 && (oldest.IsZero() || audit.writing[0].enqueued.Before(oldest)) {
		oldest = audit.writing[0].enqueued
	}
	age := int64(0)
	if !oldest.IsZero() {
		age = time.Since(oldest).Milliseconds()
	}
	admitted := [3]int{audit.admitted[0], audit.admitted[1], audit.admitted[2]}
	admittedBytes := [3]int{audit.admittedBytes[0], audit.admittedBytes[1], audit.admittedBytes[2]}
	droppedByClass := [3]uint64{audit.droppedByClass[0], audit.droppedByClass[1], audit.droppedByClass[2]}
	status := map[string]any{
		"queue_count":               len(audit.pending),
		"queue_bytes":               pendingBytes,
		"queue_age_ms":              age,
		"admitted_count":            audit.admittedCountLocked(),
		"admitted_bytes":            audit.admittedBytesLocked(),
		"in_flight_count":           admitted[AuditClassRequest] + admitted[AuditClassRejection] + admitted[AuditClassCompletion] - len(audit.pending),
		"reserved_completion_slots": audit.reservedCompletion,
		"reserved_completion_bytes": audit.reservedCompletionBytes,
		"dropped_total":             audit.dropped,
		"admitted_by_class": map[string]int{
			"request":    admitted[AuditClassRequest],
			"rejection":  admitted[AuditClassRejection],
			"completion": admitted[AuditClassCompletion],
		},
		"admitted_bytes_by_class": map[string]int{
			"request":    admittedBytes[AuditClassRequest],
			"rejection":  admittedBytes[AuditClassRejection],
			"completion": admittedBytes[AuditClassCompletion],
		},
		"dropped_by_class": map[string]uint64{
			"request":    droppedByClass[AuditClassRequest],
			"rejection":  droppedByClass[AuditClassRejection],
			"completion": droppedByClass[AuditClassCompletion],
		},
		"limits": map[string]any{
			"queue_count":           auditQueueMaximum,
			"queue_bytes":           auditQueueBytes,
			"request_count":         auditRequestQueueMaximum,
			"request_bytes":         auditRequestQueueBytes,
			"rejection_count":       auditRejectionQueueMaximum,
			"rejection_bytes":       auditRejectionQueueBytes,
			"completion_slots":      auditCompletionMaximum,
			"completion_bytes":      auditCompletionBytes,
			"completion_slot_bytes": auditCompletionSlotBytes,
		},
		"observed_admitted_max":       audit.admittedMaximum,
		"observed_admitted_bytes_max": audit.admittedBytesMaximum,
		"fsync_count":                 audit.syncCount.Load(),
		"fsync_ns_total":              audit.syncNS.Load(),
		"fsync_ns_max":                audit.syncMaxNS.Load(),
		"dropped_semantics":           "records refused before admission; accepted records are never dropped",
	}
	audit.queueMutex.Unlock()
	status["durability_source"] = "audit_file_v1"
	status["fsync_scope"] = "audit_file"
	if audit.completionStore != nil {
		status["durability_source"] = "completion_journal_v2"
		status["fsync_scope"] = "immutable_legacy_audit_prefix"
		status["limits"].(map[string]any)["completion_slots"] = completionReservedMaximum
		status["completion_journal"] = audit.completionStore.operations.completionStatus()
	}
	return status
}

func (audit *fileAudit) recordSyncDuration(elapsed time.Duration) {
	nanoseconds := uint64(elapsed)
	audit.syncNS.Add(nanoseconds)
	audit.syncCount.Add(1)
	for {
		maximum := audit.syncMaxNS.Load()
		if nanoseconds <= maximum || audit.syncMaxNS.CompareAndSwap(maximum, nanoseconds) {
			return
		}
	}
}

func (audit *fileAudit) recordBatch(batch []*auditRequest) error {
	if audit.completionStore != nil {
		return audit.recordUnifiedBatch(batch)
	}
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if audit.closed {
		return ErrStateClosed
	}
	if len(batch) == 0 || len(batch) > auditBatchMaximum {
		return errors.New("invalid audit batch size")
	}
	audit.head.mutex.Lock()
	defer audit.head.mutex.Unlock()
	if err := audit.head.available(); err != nil {
		return err
	}
	if uint64(len(batch)) > ^uint64(0)-audit.state.Sequence {
		return errors.New("audit sequence exhausted")
	}
	next := audit.state
	var line []byte
	for _, request := range batch {
		if err := validateAudit(request.event); err != nil {
			return err
		}
		payload := auditPayload{Version: 1, Sequence: next.Sequence + 1, Previous: next.Hash, Event: request.event}
		encoded, _ := json.Marshal(payload)
		hash := digestHex(encoded)
		record, _ := json.Marshal(auditRecord{Payload: payload, Hash: hash})
		line = append(line, record...)
		line = append(line, '\n')
		next = auditHead{Version: 1, Sequence: payload.Sequence, Hash: hash}
	}
	if audit.size+int64(len(line)) > auditSegmentBytes {
		if audit.size == 0 {
			return errors.New("audit event exceeds segment capacity")
		}
		rotation := auditRotation{Version: 1, Start: audit.start, Sequence: audit.state.Sequence, Hash: audit.state.Hash}
		intent, _ := json.Marshal(rotation)
		if err := atomicPrivate(audit.file.Name()+".rotate", intent); err != nil {
			audit.head.poisoned = true
			return ErrStateUncertain
		}
		fresh, err := finishAuditRotation(audit.file.Name(), audit.start, audit.file)
		if err != nil {
			audit.head.poisoned = true
			return ErrStateUncertain
		}
		audit.file.Close()
		audit.file = fresh
		audit.size = 0
		audit.start = audit.state.Sequence + 1
		if err := clearAuditRotation(audit.file.Name()); err != nil {
			audit.head.poisoned = true
			return ErrStateUncertain
		}
	}
	// The appended chain is what makes a record durable, so the head only has to
	// stay close enough to witness it. Advancing it on every record would add four
	// barriers per request for no durability gain, so it moves by at most
	// auditBatchMaximum records at a time, which is also the gap recovery accepts.
	if next.Sequence-audit.head.revision > auditBatchMaximum && audit.state.Sequence > audit.head.revision {
		if err := audit.head.commitAuditBatch(audit.state.Sequence, audit.state); err != nil {
			return err
		}
	}
	if written, err := audit.file.Write(line); err != nil || written != len(line) {
		audit.head.poisoned = true
		return ErrStateUncertain
	}
	if gate := audit.testSyncGate; gate != nil {
		gate()
	}
	syncStarted := time.Now()
	if err := audit.file.Sync(); err != nil {
		audit.head.poisoned = true
		return ErrStateUncertain
	}
	audit.recordSyncDuration(time.Since(syncStarted))
	audit.state = next
	audit.size += int64(len(line))
	return nil
}

func (audit *fileAudit) Close() error {
	audit.queueMutex.Lock()
	audit.closing = true
	audit.queueMutex.Unlock()
	// Admission is stopped before the accepted requests are awaited, so a
	// successful reply never outlives the write that backed it.
	audit.writers.Wait()
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if audit.closed {
		return nil
	}
	audit.closed = true
	// A clean shutdown leaves the witness current, so the next open proves every
	// acknowledged record is still present.
	checkpoint := error(nil)
	if audit.state.Sequence > audit.head.revision {
		checkpoint = audit.head.commitAuditBatch(audit.state.Sequence, audit.state)
	}
	return errors.Join(checkpoint, audit.file.Close(), audit.head.close())
}

func validateAudit(event AuditEvent) error {
	if !identifierPattern.MatchString(event.RequestID) || !subjectPattern.MatchString(event.Subject) || (event.Project != "*" && !identifierPattern.MatchString(event.Project)) || len(event.Action) > 128 || event.TimestampMS < 0 || !identifierPattern.MatchString(event.Outcome) {
		return errors.New("audit accepts only bounded structured identifiers/outcomes")
	}
	action := strings.TrimPrefix(event.Action, "/")
	for _, part := range strings.Split(action, "/") {
		if !identifierPattern.MatchString(part) {
			return errors.New("audit action must be an operation name, never SQL")
		}
	}
	for _, field := range []string{event.RequestID, event.Subject, event.Project, event.Database, event.Action, event.Outcome} {
		if strings.Contains(field, "plk1_") || strings.Contains(field, "pldb1.") {
			return errors.New("credential forbidden in audit event")
		}
	}
	if event.Database != "" {
		parts := strings.Split(event.Database, "/")
		if len(parts) != 3 || (controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[0], Database: parts[1]}, Branch: parts[2]}).Validate() != nil {
			return errors.New("invalid audit database identity")
		}
	}
	return nil
}
