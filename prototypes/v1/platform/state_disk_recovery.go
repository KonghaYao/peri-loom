package platform

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const durablePayloadBytes = 16 << 20
const durableEnvelopeBytes = durablePayloadBytes + 1024
const durableIntentBytes = 2*durablePayloadBytes + 16*1024

type durableCommitIntent struct {
	Version uint32        `json:"version"`
	Target  string        `json:"target"`
	Before  *diskEnvelope `json:"before"`
	After   diskEnvelope  `json:"after"`
}

func readDurableBounded(path string, maximum int64, value any) error {
	if err := privatePath(path, false); err != nil {
		return err
	}
	before, err := os.Lstat(path)
	if err != nil || before.Size() > maximum {
		return errors.New("private commit file exceeds limit")
	}
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(descriptor), path)
	defer file.Close()
	actual, err := file.Stat()
	if err != nil || !os.SameFile(before, actual) || !actual.Mode().IsRegular() || actual.Mode().Perm()&0077 != 0 || actual.Size() > maximum {
		return errors.New("private commit file changed")
	}
	owner, owned := actual.Sys().(*syscall.Stat_t)
	if !owned || owner.Uid != uint32(os.Geteuid()) {
		return errors.New("private commit file ownership changed")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(encoded)) > maximum {
		return errors.New("private commit file unavailable or oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if sqlJSONValue(context.Background(), decoder, 0) != nil {
		return errors.New("invalid private commit JSON")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("invalid private commit JSON")
	}
	return strictDecode(encoded, value)
}

func validDurableEnvelope(envelope diskEnvelope) bool {
	return envelope.Version == 1 && len(envelope.Payload) > 0 && len(envelope.Payload) <= durablePayloadBytes && envelope.Checksum == digestHex(envelope.Payload)
}

func readDurableEnvelope(path string, envelope *diskEnvelope) error {
	return readDurableBounded(path, durableEnvelopeBytes, envelope)
}

func validateDurableCommit(intent durableCommitIntent, disk *durableFile) error {
	if intent.Version != 1 || intent.Target != disk.path || len(intent.Target) > 4096 || !validDurableEnvelope(intent.After) {
		return errors.New("invalid durable commit binding")
	}
	if intent.Before == nil {
		if intent.After.Revision != 0 {
			return errors.New("invalid durable initialization revision")
		}
	} else if !validDurableEnvelope(*intent.Before) || intent.Before.Revision == ^uint64(0) || intent.After.Revision != intent.Before.Revision+1 {
		return errors.New("durable commit must bind exact parent and next revision")
	}
	switch disk.prototype.(type) {
	case State:
		var after State
		if strictDecode(intent.After.Payload, &after) != nil || after.Revision != intent.After.Revision {
			return errors.New("invalid durable state intent")
		}
		if intent.Before == nil {
			initial, err := json.Marshal(disk.prototype)
			if err != nil || !bytes.Equal(initial, intent.After.Payload) {
				return errors.New("invalid durable initial state")
			}
		}
		var before *State
		if intent.Before != nil {
			before = &State{}
			if strictDecode(intent.Before.Payload, before) != nil || before.Revision != intent.Before.Revision {
				return errors.New("invalid durable parent state")
			}
			if err := validateState(*before, nil); err != nil {
				return err
			}
		}
		return validateState(after, before)
	case auditHead:
		var after auditHead
		if strictDecode(intent.After.Payload, &after) != nil || !validDurableAuditHead(after, intent.After.Revision) {
			return errors.New("invalid durable audit head intent")
		}
		if intent.Before == nil {
			if after.Hash != strings.Repeat("0", 64) {
				return errors.New("invalid audit initialization")
			}
		} else {
			var before auditHead
			if strictDecode(intent.Before.Payload, &before) != nil || !validDurableAuditHead(before, intent.Before.Revision) {
				return errors.New("invalid durable parent audit head")
			}
		}
	case keyRing:
		var after keyRing
		if strictDecode(intent.After.Payload, &after) != nil || !validDurableKeyRing(after, intent.After.Revision) {
			return errors.New("invalid durable keyring intent")
		}
		if intent.Before == nil && (after.Active != 1 || len(after.Keys) != 1 || after.Uses["1"] != 0) {
			return errors.New("invalid durable initial keyring")
		}
		if intent.Before != nil {
			var before keyRing
			if strictDecode(intent.Before.Payload, &before) != nil || !validDurableKeyRing(before, intent.Before.Revision) || after.Active < before.Active {
				return errors.New("invalid durable parent keyring")
			}
			for version, key := range before.Keys {
				if after.Keys[version] != key || after.Uses[version] < before.Uses[version] {
					return errors.New("durable keyring rollback forbidden")
				}
			}
		}
	}
	return nil
}

func validDurableAuditHead(head auditHead, revision uint64) bool {
	decoded, err := hex.DecodeString(head.Hash)
	return head.Version == 1 && head.Sequence == revision && err == nil && len(decoded) == 32
}

func validDurableKeyRing(ring keyRing, revision uint64) bool {
	if ring.Version != 1 || ring.Revision != revision || ring.Active == 0 || len(ring.Keys) == 0 || len(ring.Uses) != len(ring.Keys) {
		return false
	}
	if _, exists := ring.Keys[strconv.FormatUint(ring.Active, 10)]; !exists {
		return false
	}
	for version, key := range ring.Keys {
		decoded, err := hex.DecodeString(key)
		number, numberErr := strconv.ParseUint(version, 10, 64)
		uses, exists := ring.Uses[version]
		valid := err == nil && len(decoded) == 32 && numberErr == nil && number != 0 && strconv.FormatUint(number, 10) == version && number <= ring.Active && exists && uses <= 1000000
		clear(decoded)
		if !valid {
			return false
		}
	}
	return true
}

func writeDurableIntent(path string, intent durableCommitIntent) error {
	if _, err := os.Lstat(path + ".pending"); !errors.Is(err, os.ErrNotExist) {
		return errors.New("existing or unsafe durable intent")
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return errors.New("invalid durable intent")
	}
	encoded, err := json.Marshal(diskEnvelope{Version: 1, Revision: intent.After.Revision, Checksum: digestHex(payload), Payload: payload})
	if err != nil || len(encoded) > durableIntentBytes {
		return errors.New("durable intent exceeds physical limit")
	}
	return atomicPrivate(path+".pending", encoded)
}

func removeDurableIntent(path string) error {
	if err := os.Remove(path + ".pending"); err != nil {
		return err
	}
	return fsyncDirectory(filepath.Dir(path))
}

func durableEnvelopePosition(path string, intent durableCommitIntent) (bool, error) {
	var current diskEnvelope
	err := readDurableEnvelope(path, &current)
	if errors.Is(err, os.ErrNotExist) && intent.Before == nil {
		return false, nil
	}
	if err != nil || !validDurableEnvelope(current) {
		return false, errors.New("durable payload missing or invalid")
	}
	for _, candidate := range []*diskEnvelope{&intent.After, intent.Before} {
		if candidate != nil && current.Revision == candidate.Revision && current.Checksum == candidate.Checksum && bytes.Equal(current.Payload, candidate.Payload) {
			return candidate == &intent.After, nil
		}
	}
	return false, errors.New("durable payload does not match intent")
}

func durableFloorPosition(path string, intent durableCommitIntent) (bool, error) {
	var current diskFloor
	err := readDurableBounded(path+".floor", 1024, &current)
	if errors.Is(err, os.ErrNotExist) && intent.Before == nil {
		return false, nil
	}
	if err != nil || current.Version != 1 {
		return false, errors.New("durable floor missing or invalid")
	}
	for _, candidate := range []*diskEnvelope{&intent.After, intent.Before} {
		if candidate != nil && current.Revision == candidate.Revision && current.Checksum == candidate.Checksum {
			return candidate == &intent.After, nil
		}
	}
	return false, errors.New("durable floor does not match intent")
}

func recoverDurableCommit(disk *durableFile) error {
	if _, err := os.Lstat(disk.path + ".pending"); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var wrapped diskEnvelope
	if err := readDurableBounded(disk.path+".pending", durableIntentBytes, &wrapped); err != nil {
		return err
	}
	if wrapped.Version != 1 || wrapped.Checksum != digestHex(wrapped.Payload) {
		return errors.New("durable intent checksum mismatch")
	}
	var intent durableCommitIntent
	if strictDecode(wrapped.Payload, &intent) != nil || wrapped.Revision != intent.After.Revision {
		return errors.New("invalid durable intent envelope")
	}
	if err := validateDurableCommit(intent, disk); err != nil {
		return err
	}
	payloadNext, err := durableEnvelopePosition(disk.path, intent)
	if err != nil {
		return err
	}
	floorNext, err := durableFloorPosition(disk.path, intent)
	if err != nil {
		return err
	}
	if payloadNext && !floorNext {
		return errors.New("impossible durable commit order")
	}
	if !floorNext {
		encoded, _ := json.Marshal(diskFloor{Version: 1, Revision: intent.After.Revision, Checksum: intent.After.Checksum})
		if err := atomicPrivate(disk.path+".floor", encoded); err != nil {
			return err
		}
	}
	if !payloadNext {
		encoded, _ := json.Marshal(intent.After)
		if err := atomicPrivate(disk.path, encoded); err != nil {
			return err
		}
	}
	if err := removeDurableIntent(disk.path); err != nil {
		return err
	}
	disk.fresh = intent.Before == nil
	return nil
}
