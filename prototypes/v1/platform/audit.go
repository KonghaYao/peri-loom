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
	"syscall"

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

type auditRotation struct {
	Version  uint32 `json:"version"`
	Start    uint64 `json:"start"`
	Sequence uint64 `json:"sequence"`
	Hash     string `json:"hash"`
}

type auditRequest struct {
	event AuditEvent
	done  chan error
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
	queueMutex sync.Mutex
	pending    []*auditRequest
	running    bool
	closing    bool
	writers    sync.WaitGroup
	mutex      sync.Mutex
	head       *durableFile
	file       *os.File
	state      auditHead
	size       int64
	start      uint64
	closed     bool
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
	if err := validateAudit(event); err != nil {
		return err
	}
	request := &auditRequest{event: event, done: make(chan error, 1)}
	audit.queueMutex.Lock()
	if audit.closing {
		audit.queueMutex.Unlock()
		return ErrStateClosed
	}
	audit.pending = append(audit.pending, request)
	if !audit.running {
		audit.running = true
		audit.writers.Add(1)
		go audit.drain()
	}
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
		audit.queueMutex.Unlock()
		err := audit.recordBatch(batch)
		for _, request := range batch {
			request.done <- err
		}
	}
}

func (audit *fileAudit) recordBatch(batch []*auditRequest) error {
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
	if err := audit.file.Sync(); err != nil {
		audit.head.poisoned = true
		return ErrStateUncertain
	}
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
