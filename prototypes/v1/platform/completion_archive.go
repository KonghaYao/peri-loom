package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Archives retain the authoritative audit even after the operation view evicts
// a known terminal. They are immutable and bounded individually; the admission
// ceiling bounds total retained history rather than silently deleting audit.
type completionArchive struct {
	Version uint32         `json:"version"`
	Entries []journalEntry `json:"entries"`
	Seal    string         `json:"seal"`
}

func (archive completionArchive) digest() (string, error) {
	archive.Seal = ""
	encoded, err := json.Marshal(archive)
	if err != nil {
		return "", err
	}
	return digestHex(encoded), nil
}

func completionArchiveName(name string) bool {
	if len(name) != len("completion.")+20+1+20 || !strings.HasPrefix(name, "completion.") {
		return false
	}
	halves := strings.Split(strings.TrimPrefix(name, "completion."), ".")
	if len(halves) != 2 {
		return false
	}
	start, first := strconv.ParseUint(halves[0], 10, 64)
	end, second := strconv.ParseUint(halves[1], 10, 64)
	return first == nil && second == nil && start > 0 && end >= start
}

func (journal *operationJournal) archiveCompletionPrefix() error {
	firstUnarchived := 0
	for firstUnarchived < len(journal.records) && journal.records[firstUnarchived].Payload.Revision <= journal.archivedRevision {
		firstUnarchived++
	}
	records := journal.records[firstUnarchived:]
	if len(records) == 0 {
		return nil
	}
	archive := completionArchive{Version: completionJournalVersion, Entries: records}
	seal, err := archive.digest()
	if err != nil {
		return err
	}
	archive.Seal = seal
	encoded, err := json.Marshal(archive)
	if err != nil || len(encoded) > 2*completionActiveMaximumBytes {
		return errors.New("completion archive oversized")
	}
	first, last := records[0].Payload.Revision, records[len(records)-1].Payload.Revision
	path := filepath.Join(filepath.Dir(journal.path), fmt.Sprintf("completion.%020d.%020d", first, last))
	if _, err := os.Lstat(path); err == nil {
		var existing completionArchive
		if err := readDurableBounded(path, 2*completionActiveMaximumBytes, &existing); err != nil {
			return err
		}
		digest, err := existing.digest()
		if err != nil || existing.Seal != seal || digest != seal {
			return errors.New("completion archive collision")
		}
		journal.archivedRevision = last
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := atomicPrivate(path, encoded); err != nil {
		return err
	}
	journal.retainedBytes += int64(len(encoded))
	journal.archivedRevision = last
	return nil
}

func validCompletionEntry(entry journalEntry) bool {
	encoded, err := json.Marshal(entry.Payload)
	return err == nil && digestHex(encoded) == entry.Hash && len(encoded) <= operationJournalMaximumRecordBytes
}

func advanceCompletionAudit(head *auditHead, record *auditRecord) error {
	if record == nil {
		return nil
	}
	payload := record.Payload
	encoded, err := json.Marshal(payload)
	if err != nil || payload.Version != 1 || head.Sequence == ^uint64(0) ||
		payload.Sequence != head.Sequence+1 || payload.Previous != head.Hash ||
		validateAudit(payload.Event) != nil || record.Hash != digestHex(encoded) {
		return errors.New("completion audit chain mismatch")
	}
	*head = auditHead{Version: 1, Sequence: payload.Sequence, Hash: record.Hash}
	return nil
}

// A recovery walk holds at most one bounded archive. It verifies all retained
// audit and every archive boundary before traffic is admitted; the hard retained
// byte limit also bounds this work. Duplicate active bytes are allowed only when
// they exactly match retained archives (snapshot/rotation interrupted by crash).
func (journal *operationJournal) verifyCompletionHistory() error {
	files, err := os.ReadDir(filepath.Dir(journal.path))
	if err != nil {
		return err
	}
	if journal.completion == nil {
		if journal.snapshot != nil && journal.snapshot.Version == completionJournalVersion {
			return errors.New("completion snapshot without genesis")
		}
		for _, file := range files {
			if completionArchiveName(file.Name()) {
				return errors.New("completion archive without genesis")
			}
		}
		return nil
	}
	genesis := journal.completion
	revision, digest, head := genesis.JournalRevision, genesis.JournalDigest, genesis.Audit
	retained := journal.size
	// Keep duplicate proofs only for the bounded active log; retaining a hash
	// map for the whole audit history would make recovery memory grow forever.
	active := make(map[uint64]string, len(journal.records))
	for _, entry := range journal.records {
		active[entry.Payload.Revision] = entry.Hash
	}
	archivedActive := make(map[uint64]string)
	snapshotObserved := journal.snapshot == nil || journal.snapshot.Version != completionJournalVersion
	consume := func(entry journalEntry, duplicate bool) error {
		if !validCompletionEntry(entry) {
			return errors.New("completion entry checksum mismatch")
		}
		if entry.Payload.Revision <= genesis.JournalRevision {
			if entry.Payload.Version != operationJournalVersion || entry.Payload.Audit != nil {
				return errors.New("completion history predates genesis")
			}
			return nil
		}
		if entry.Payload.Revision <= revision {
			if duplicate && archivedActive[entry.Payload.Revision] == entry.Hash {
				return nil
			}
			return errors.New("completion history overlaps")
		}
		if entry.Payload.Version != completionJournalVersion || entry.Payload.Revision != revision+1 || entry.Payload.Previous != digest {
			return errors.New("completion archive gap or fork")
		}
		if err := advanceCompletionAudit(&head, entry.Payload.Audit); err != nil {
			return err
		}
		revision, digest = entry.Payload.Revision, entry.Hash
		if journal.snapshot != nil && journal.snapshot.Version == completionJournalVersion && revision == journal.snapshot.Revision {
			if journal.snapshot.CompletionAudit == nil || *journal.snapshot.CompletionAudit != head || journal.snapshot.Digest != digest {
				return errors.New("completion snapshot does not bind audit tail")
			}
			snapshotObserved = true
		}
		return nil
	}
	for _, file := range files {
		if !completionArchiveName(file.Name()) {
			continue
		}
		info, err := file.Info()
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("invalid completion archive")
		}
		retained += info.Size()
		if retained > completionRetentionMaximumBytes+2*completionActiveMaximumBytes {
			return errors.New("completion retained history exceeds limit")
		}
		var archive completionArchive
		if err := readDurableBounded(filepath.Join(filepath.Dir(journal.path), file.Name()), 2*completionActiveMaximumBytes, &archive); err != nil {
			return err
		}
		seal, err := archive.digest()
		if err != nil || archive.Version != completionJournalVersion || len(archive.Entries) == 0 || seal != archive.Seal {
			return errors.New("completion archive checksum mismatch")
		}
		first, last := archive.Entries[0].Payload.Revision, archive.Entries[len(archive.Entries)-1].Payload.Revision
		if file.Name() != fmt.Sprintf("completion.%020d.%020d", first, last) {
			return errors.New("completion archive identity mismatch")
		}
		for _, entry := range archive.Entries {
			if err := consume(entry, false); err != nil {
				return err
			}
			if hash, found := active[entry.Payload.Revision]; found && hash == entry.Hash {
				archivedActive[entry.Payload.Revision] = entry.Hash
			}
		}
		journal.archivedRevision = last
	}
	for _, entry := range journal.records {
		if err := consume(entry, true); err != nil {
			return err
		}
	}
	if revision != journal.revision || digest != journal.digest || !snapshotObserved {
		return errors.New("completion history does not cover durable head")
	}
	journal.auditHead, journal.orderedAudit = head, head
	journal.retainedBytes = retained
	return nil
}
