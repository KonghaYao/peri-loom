package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

const snapshotEnvelopeVersion = 2

type snapshotPayload struct {
	Metadata
	JournalAnchor string `json:"journal_anchor"`
}

type loadedSnapshot struct {
	metadata Metadata
	anchor   string
	legacy   bool
}

func encodeSnapshot(metadata Metadata, anchor string) ([]byte, string, error) {
	payload, err := json.Marshal(snapshotPayload{Metadata: metadata, JournalAnchor: anchor})
	if err != nil {
		return nil, "", err
	}
	digest := sha256Sum(payload)
	wrapped, err := json.Marshal(stateEnvelope{Version: snapshotEnvelopeVersion, Revision: metadata.Revision, Checksum: digest, Payload: payload})
	if err != nil {
		return nil, "", err
	}
	if len(wrapped) > 64<<20 {
		return nil, "", errors.New("metadata snapshot exceeds size limit")
	}
	return wrapped, digest, nil
}

func readSnapshotFile(directory string) (loadedSnapshot, error) {
	envelope, payload, err := readEnvelopeRaw(directory, metadataSnapshotName)
	if err != nil {
		return loadedSnapshot{}, err
	}
	switch envelope.Version {
	case snapshotEnvelopeVersion:
		var snapshot snapshotPayload
		if err := strictJSON(payload, &snapshot); err != nil {
			return loadedSnapshot{}, fmt.Errorf("invalid metadata snapshot: %w", err)
		}
		if snapshot.Metadata.Revision != envelope.Revision {
			return loadedSnapshot{}, errors.New("metadata snapshot revision mismatch")
		}
		if err := validateMetadata(snapshot.Metadata, nil); err != nil {
			return loadedSnapshot{}, err
		}
		return loadedSnapshot{metadata: snapshot.Metadata, anchor: snapshot.JournalAnchor}, nil
	case 1:
		var metadata Metadata
		if err := strictJSON(payload, &metadata); err != nil {
			return loadedSnapshot{}, fmt.Errorf("invalid legacy metadata snapshot: %w", err)
		}
		if metadata.Revision != envelope.Revision {
			return loadedSnapshot{}, errors.New("legacy metadata snapshot revision mismatch")
		}
		return loadedSnapshot{metadata: metadata, legacy: true}, nil
	default:
		return loadedSnapshot{}, fmt.Errorf("unsupported metadata snapshot version %d", envelope.Version)
	}
}

func (store *diskStore) initialize() error {
	metadata := Metadata{Version: 1, Workers: make(map[string]Worker), Placements: make(map[string]Placement)}
	encoded, _, err := encodeSnapshot(metadata, "")
	if err != nil {
		return err
	}
	if err := atomicFile(store.directory, metadataSnapshotName, encoded); err != nil {
		return err
	}
	journal, err := createJournalFile(store.directory)
	if err != nil {
		return err
	}
	store.metadata = metadata
	store.journal = journal
	store.snapshotRevision = metadata.Revision
	return nil
}

func (store *diskStore) load() error {
	if _, err := os.Lstat(filepath.Join(store.directory, metadataPendingName)); err == nil {
		// Resolve a legacy in-flight commit before interpreting the format. A
		// journaled (version 2) snapshot never writes this intent.
		envelope, _, envelopeErr := readEnvelopeRaw(store.directory, metadataSnapshotName)
		if errors.Is(envelopeErr, os.ErrNotExist) || (envelopeErr == nil && envelope.Version == 1) {
			if err := recoverMetadataCommit(store.directory); err != nil {
				return err
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	snapshot, err := readSnapshotFile(store.directory)
	if err != nil {
		return fmt.Errorf("metadata unavailable: %w", err)
	}
	if snapshot.legacy {
		if _, err := os.Lstat(filepath.Join(store.directory, metadataJournalName)); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return errors.New("legacy metadata snapshot conflicts with metadata journal")
			}
			return err
		}
		if _, err := os.Lstat(filepath.Join(store.directory, metadataPendingName)); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return errors.New("legacy metadata commit intent conflicts with snapshot")
			}
			return err
		}
		return store.migrateLegacy(snapshot.metadata)
	}
	if _, err := os.Lstat(filepath.Join(store.directory, metadataPendingName)); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("legacy metadata commit intent conflicts with journaled state")
		}
		return err
	}
	floor, hasFloor, err := readLegacyFloor(store.directory)
	if err != nil {
		return err
	}
	if hasFloor && (floor.Revision > snapshot.metadata.Revision || floor.Epoch > snapshot.metadata.Epoch) {
		return errors.New("legacy epoch floor is ahead of metadata snapshot")
	}
	if err := store.openJournal(snapshot); err != nil {
		return err
	}
	if hasFloor {
		if err := os.Remove(filepath.Join(store.directory, "epoch-floor.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirectory(store.directory); err != nil {
			return err
		}
	}
	return nil
}

func readLegacyFloor(directory string) (epochFloor, bool, error) {
	path := filepath.Join(directory, "epoch-floor.json")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return epochFloor{}, false, nil
	} else if err != nil {
		return epochFloor{}, false, err
	}
	var floor epochFloor
	if _, err := readEnvelope(directory, "epoch-floor.json", &floor); err != nil {
		return epochFloor{}, false, fmt.Errorf("epoch floor unavailable: %w", err)
	}
	if floor.Version != 1 {
		return epochFloor{}, false, errors.New("unsupported epoch floor version")
	}
	return floor, true, nil
}

func (store *diskStore) migrateLegacy(metadata Metadata) error {
	if err := validateMetadata(metadata, nil); err != nil {
		return err
	}
	_, floor, err := loadState(store.directory)
	if err != nil {
		return err
	}
	if floor.Version != 1 || floor.Revision != metadata.Revision || floor.Epoch != metadata.Epoch {
		return errors.New("durable epoch/revision floor mismatch")
	}
	encoded, _, err := encodeSnapshot(metadata, "")
	if err != nil {
		return err
	}
	if err := atomicFile(store.directory, metadataSnapshotName, encoded); err != nil {
		return err
	}
	journal, err := createJournalFile(store.directory)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(store.directory, "epoch-floor.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		journal.Close()
		return err
	}
	if err := syncDirectory(store.directory); err != nil {
		journal.Close()
		return err
	}
	store.metadata = metadata
	store.journal = journal
	store.snapshotRevision = metadata.Revision
	store.journalTail = ""
	store.journalEntries = 0
	store.journalBytes = 0
	return nil
}

func (store *diskStore) openJournal(snapshot loadedSnapshot) error {
	path := filepath.Join(store.directory, metadataJournalName)
	created := false
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	} else if err != nil {
		return err
	}
	journal, err := openJournalFile(store.directory)
	if err != nil {
		return err
	}
	if created {
		if err := journal.Sync(); err != nil {
			journal.Close()
			return err
		}
		if err := syncDirectory(store.directory); err != nil {
			journal.Close()
			return err
		}
	}
	records, validBytes, size, err := readJournalFile(path)
	if err != nil {
		journal.Close()
		return err
	}
	if validBytes < size {
		if err := journal.Truncate(validBytes); err != nil {
			journal.Close()
			return err
		}
		if err := journal.Sync(); err != nil {
			journal.Close()
			return err
		}
	}
	metadata := snapshot.metadata
	replayed, tail, err := replayJournal(&metadata, snapshot.anchor, records)
	if err != nil {
		journal.Close()
		return err
	}
	store.metadata = metadata
	store.journal = journal
	store.snapshotRevision = snapshot.metadata.Revision
	store.journalTail = tail
	store.journalEntries = len(records)
	store.journalBytes = validBytes
	store.replayEntries = replayed
	if store.shouldCompact() {
		if err := store.compactDurable(); err != nil {
			store.compactionErrorMsg = err.Error()
			logCompactionFailure(err)
		}
	}
	return nil
}

func replayJournal(metadata *Metadata, anchor string, records []decodedJournalRecord) (int, string, error) {
	if len(records) == 0 {
		return 0, anchor, nil
	}
	if metadata.Revision == math.MaxUint64 {
		return 0, "", errors.New("metadata revision exhausted")
	}
	start := -1
	first := records[0]
	switch {
	case first.revision > metadata.Revision+1:
		return 0, "", errors.New("metadata journal starts after snapshot revision")
	case first.revision == metadata.Revision+1:
		if first.prev != anchor {
			return 0, "", errors.New("metadata journal does not link to snapshot")
		}
		start = 0
	default:
		// A crash after the snapshot rename but before journal truncation
		// leaves a stale prefix. It is safe only if it still proves the exact
		// snapshot revision and checksum.
		index := -1
		for position, record := range records {
			if record.revision == metadata.Revision {
				index = position
				break
			}
		}
		if index < 0 {
			return 0, "", errors.New("metadata journal does not cover snapshot revision")
		}
		if records[index].checksum != anchor {
			return 0, "", errors.New("metadata journal does not match snapshot anchor")
		}
		start = index + 1
		if start < len(records) && records[start].prev != records[index].checksum {
			return 0, "", errors.New("metadata journal checksum chain does not continue from snapshot")
		}
	}
	replayed := 0
	for _, record := range records[start:] {
		if record.revision != metadata.Revision+1 {
			return 0, "", errors.New("metadata journal revision gap")
		}
		if err := validateMetadataDelta(*metadata, record.delta); err != nil {
			return 0, "", fmt.Errorf("metadata journal revision %d: %w", record.revision, err)
		}
		applyMetadataDelta(metadata, record.delta, false)
		metadata.Revision = record.revision
		replayed++
	}
	return replayed, records[len(records)-1].checksum, nil
}

func (store *diskStore) shouldCompact() bool {
	return store.journalEntries >= journalCompactEntries || store.journalBytes >= journalCompactBytes
}

func (store *diskStore) compactDurable() error {
	start := time.Now()
	store.commitPoint("compact.snapshot.before")
	encoded, _, err := encodeSnapshot(store.metadata, store.journalTail)
	if err != nil {
		return err
	}
	if err := atomicFile(store.directory, metadataSnapshotName, encoded); err != nil {
		return err
	}
	store.commitPoint("compact.snapshot.after")
	store.snapshotRevision = store.metadata.Revision
	store.commitPoint("compact.truncate.before")
	if err := store.journal.Truncate(0); err != nil {
		return err
	}
	if err := store.journal.Sync(); err != nil {
		return err
	}
	store.commitPoint("compact.truncate.after")
	store.journalEntries = 0
	store.journalBytes = 0
	store.compactions++
	store.lastCompactionMS = time.Since(start).Milliseconds()
	store.compactionErrorMsg = ""
	return nil
}
