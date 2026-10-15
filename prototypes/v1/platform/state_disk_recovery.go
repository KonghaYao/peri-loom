package platform

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const durablePayloadBytes = 16 << 20
const durableEnvelopeBytes = durablePayloadBytes + 1024

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

// durableRevisionStep bounds how far a durable payload may lead its rollback
// floor. Persistence writes the payload first, so the gap can only come from a
// crash inside one writer call: an audit batch may cover auditBatchMaximum
// records, every other commit advances exactly one revision.
func durableRevisionStep(prototype any) uint64 {
	if _, isAudit := prototype.(auditHead); isAudit {
		return auditBatchMaximum
	}
	return 1
}

// validateDurableTransition rejects a write that is not a legal successor of the
// in-memory payload. Persistence order is payload first and floor second, so the
// only states a crash can leave behind are "previous revision" and "next
// revision"; recovery never replays an intent file.
func validateDurableTransition(disk *durableFile, after diskEnvelope) error {
	if disk.path == "" || len(disk.path) > 4096 || !validDurableEnvelope(after) {
		return errors.New("invalid durable commit binding")
	}
	if disk.payload == nil {
		if after.Revision != 0 {
			return errors.New("invalid durable initialization revision")
		}
	} else if disk.revision == ^uint64(0) {
		return errors.New("durable commit must bind a valid parent")
	} else if _, isAudit := disk.prototype.(auditHead); isAudit {
		if after.Revision <= disk.revision || after.Revision-disk.revision > auditBatchMaximum {
			return errors.New("audit batch must advance by 1..32 records")
		}
	} else if after.Revision != disk.revision+1 {
		return errors.New("durable commit must bind exact parent and next revision")
	}
	switch disk.prototype.(type) {
	case State:
		var next State
		if strictDecode(after.Payload, &next) != nil || next.Revision != after.Revision {
			return errors.New("invalid durable state intent")
		}
		if disk.payload == nil {
			initial, err := json.Marshal(disk.prototype)
			if err != nil || !bytes.Equal(initial, after.Payload) {
				return errors.New("invalid durable initial state")
			}
		}
		var before *State
		if disk.payload != nil {
			before = &State{}
			if strictDecode(disk.payload, before) != nil || before.Revision != disk.revision {
				return errors.New("invalid durable parent state")
			}
			if err := validateState(*before, nil); err != nil {
				return err
			}
			if disk.parentOperations != nil {
				// Recovery joins the durable operation sidecar into this payload,
				// so the successor state must be compared against that same view.
				disk.parentOperations(before)
				if before.Revision != disk.revision {
					return errors.New("invalid durable parent state")
				}
				if err := validateState(*before, nil); err != nil {
					return err
				}
			}
		}
		return validateState(next, before)
	case auditHead:
		var next auditHead
		if strictDecode(after.Payload, &next) != nil || !validDurableAuditHead(next, after.Revision) {
			return errors.New("invalid durable audit head intent")
		}
		if disk.payload == nil {
			if next.Hash != strings.Repeat("0", 64) {
				return errors.New("invalid audit initialization")
			}
		} else {
			var before auditHead
			if strictDecode(disk.payload, &before) != nil || !validDurableAuditHead(before, disk.revision) {
				return errors.New("invalid durable parent audit head")
			}
		}
	case keyRing:
		var next keyRing
		if strictDecode(after.Payload, &next) != nil || !validDurableKeyRing(next, after.Revision) {
			return errors.New("invalid durable keyring intent")
		}
		if disk.payload == nil && (next.Active != 1 || len(next.Keys) != 1 || next.Uses["1"] != 0) {
			return errors.New("invalid durable initial keyring")
		}
		if disk.payload != nil {
			var before keyRing
			if strictDecode(disk.payload, &before) != nil || !validDurableKeyRing(before, disk.revision) || next.Active < before.Active {
				return errors.New("invalid durable parent keyring")
			}
			for version, key := range before.Keys {
				if next.Keys[version] != key || next.Uses[version] < before.Uses[version] {
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
