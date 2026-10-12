package controller

import (
	"errors"
	"math"
	"os"
	"path/filepath"
)

const metadataPendingName = "metadata.pending.json"

type metadataCommit struct {
	Version uint32    `json:"version"`
	Before  *Metadata `json:"before"`
	After   Metadata  `json:"after"`
}

func metadataCommitFiles(metadata Metadata) ([]byte, []byte, error) {
	encoded, checksum, err := encodeEnvelope(metadata.Revision, metadata)
	if err != nil {
		return nil, nil, err
	}
	floor := epochFloor{Version: 1, Revision: metadata.Revision, Epoch: metadata.Epoch, MetadataChecksum: checksum}
	encodedFloor, _, err := encodeEnvelope(metadata.Revision, floor)
	return encoded, encodedFloor, err
}

func removeMetadataIntent(directory string) error {
	if err := os.Remove(filepath.Join(directory, metadataPendingName)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func validateMetadataCommit(intent metadataCommit, revision uint64) error {
	if intent.Version != 1 || intent.After.Revision != revision {
		return errors.New("invalid metadata commit version/revision")
	}
	if intent.Before == nil {
		if intent.After.Revision != 0 || intent.After.Epoch != 0 || len(intent.After.Workers) != 0 || len(intent.After.Placements) != 0 {
			return errors.New("invalid metadata initialization commit")
		}
	} else {
		if err := validateMetadata(*intent.Before, nil); err != nil {
			return err
		}
		if intent.Before.Revision == math.MaxUint64 || intent.After.Revision != intent.Before.Revision+1 {
			return errors.New("metadata commit must advance exactly one revision")
		}
	}
	return validateMetadata(intent.After, intent.Before)
}

func metadataCommitPosition(directory string, intent metadataCommit) (bool, error) {
	var current Metadata
	envelope, err := readEnvelope(directory, "metadata.json", &current)
	if errors.Is(err, os.ErrNotExist) && intent.Before == nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, candidate := range []*Metadata{&intent.After, intent.Before} {
		if candidate == nil {
			continue
		}
		_, checksum, err := encodeEnvelope(candidate.Revision, *candidate)
		if err != nil {
			return false, err
		}
		if envelope.Revision == candidate.Revision && current.Revision == candidate.Revision && envelope.Checksum == checksum {
			return candidate == &intent.After, nil
		}
	}
	return false, errors.New("metadata does not match durable commit intent")
}

func floorCommitPosition(directory string, intent metadataCommit) (bool, error) {
	var current epochFloor
	envelope, err := readEnvelope(directory, "epoch-floor.json", &current)
	if errors.Is(err, os.ErrNotExist) && intent.Before == nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, candidate := range []*Metadata{&intent.After, intent.Before} {
		if candidate == nil {
			continue
		}
		_, checksum, err := encodeEnvelope(candidate.Revision, *candidate)
		if err != nil {
			return false, err
		}
		if current.Version == 1 && envelope.Revision == candidate.Revision && current.Revision == candidate.Revision && current.Epoch == candidate.Epoch && current.MetadataChecksum == checksum {
			return candidate == &intent.After, nil
		}
	}
	return false, errors.New("epoch floor does not match durable commit intent")
}

func recoverMetadataCommit(directory string) error {
	if _, err := os.Lstat(filepath.Join(directory, metadataPendingName)); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var intent metadataCommit
	envelope, err := readEnvelope(directory, metadataPendingName, &intent)
	if err != nil {
		return err
	}
	if err := validateMetadataCommit(intent, envelope.Revision); err != nil {
		return err
	}
	metadataNext, err := metadataCommitPosition(directory, intent)
	if err != nil {
		return err
	}
	floorNext, err := floorCommitPosition(directory, intent)
	if err != nil {
		return err
	}
	if metadataNext && !floorNext {
		return errors.New("metadata commit has an impossible persistence order")
	}
	encoded, encodedFloor, err := metadataCommitFiles(intent.After)
	if err != nil {
		return err
	}
	if !floorNext {
		if err := atomicFile(directory, "epoch-floor.json", encodedFloor); err != nil {
			return err
		}
	}
	if !metadataNext {
		if err := atomicFile(directory, "metadata.json", encoded); err != nil {
			return err
		}
	}
	return removeMetadataIntent(directory)
}
