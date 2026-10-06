package platform

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
type fileAudit struct {
	mutex  sync.Mutex
	head   *durableFile
	file   *os.File
	state  auditHead
	size   int64
	closed bool
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
	flags := syscall.O_RDWR | syscall.O_APPEND | syscall.O_NOFOLLOW
	if head.fresh {
		flags |= syscall.O_CREAT | syscall.O_EXCL
	}
	descriptor, err := syscall.Open(absolute, flags, 0600)
	if err != nil {
		return nil, errors.New("audit log missing or unsafe")
	}
	file := os.NewFile(uintptr(descriptor), absolute)
	valid := false
	defer func() {
		if !valid {
			file.Close()
		}
	}()
	if err := privatePath(absolute, false); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := fsyncDirectory(filepath.Dir(absolute)); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || info.Size() > 64<<20 {
		return nil, errors.New("audit log unavailable/oversized")
	}
	if info.Size() > 0 {
		var tail [1]byte
		if _, err := file.ReadAt(tail[:], info.Size()-1); err != nil || tail[0] != '\n' {
			return nil, errors.New("audit append lacks terminal newline")
		}
	}
	actual := auditHead{Version: 1, Hash: strings.Repeat("0", 64)}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 65536)
	for scanner.Scan() {
		var record auditRecord
		if strictDecode(scanner.Bytes(), &record) != nil || record.Payload.Version != 1 || record.Payload.Sequence != actual.Sequence+1 || record.Payload.Previous != actual.Hash || validateAudit(record.Payload.Event) != nil {
			return nil, errors.New("audit chain corrupted")
		}
		bytes, _ := json.Marshal(record.Payload)
		if record.Hash != digestHex(bytes) {
			return nil, errors.New("audit checksum mismatch")
		}
		actual.Sequence++
		actual.Hash = record.Hash
	}
	if scanner.Err() != nil || actual != expected {
		return nil, errors.New("audit rollback or interrupted append")
	}
	ok = true
	valid = true
	return &fileAudit{head: head, file: file, state: actual, size: info.Size()}, nil
}

func (audit *fileAudit) Record(event AuditEvent) error {
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if audit.closed {
		return ErrStateClosed
	}
	if err := validateAudit(event); err != nil {
		return err
	}
	audit.head.mutex.Lock()
	defer audit.head.mutex.Unlock()
	if err := audit.head.available(); err != nil {
		return err
	}
	if audit.state.Sequence == ^uint64(0) {
		return errors.New("audit sequence exhausted")
	}
	payload := auditPayload{Version: 1, Sequence: audit.state.Sequence + 1, Previous: audit.state.Hash, Event: event}
	encoded, _ := json.Marshal(payload)
	hash := digestHex(encoded)
	line, _ := json.Marshal(auditRecord{Payload: payload, Hash: hash})
	line = append(line, '\n')
	if audit.size+int64(len(line)) > 64<<20 {
		return errors.New("audit size limit reached; explicit archival required")
	}
	next := auditHead{Version: 1, Sequence: payload.Sequence, Hash: hash}
	if err := audit.head.commit(next.Sequence, next); err != nil {
		return err
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
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if audit.closed {
		return nil
	}
	audit.closed = true
	return errors.Join(audit.file.Close(), audit.head.close())
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
