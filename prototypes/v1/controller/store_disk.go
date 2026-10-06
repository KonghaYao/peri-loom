package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type stateEnvelope struct {
	Version  uint32          `json:"version"`
	Revision uint64          `json:"revision"`
	Checksum string          `json:"checksum"`
	Payload  json.RawMessage `json:"payload"`
}

func createPrivateDirectory(directory string) error {
	var missing []string
	for current := directory; ; current = filepath.Dir(current) {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := syncDirectory(filepath.Dir(missing[index])); err != nil {
			return err
		}
	}
	return nil
}

func privateFile(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&077 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("metadata path must be private and not a symlink (mode %o)", info.Mode().Perm())
	}
	if directory != info.IsDir() || (!directory && !info.Mode().IsRegular()) {
		return errors.New("unexpected metadata path type")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("metadata path must be owned by current user")
	}
	return nil
}

func openLock(directory string) (*os.File, error) {
	path := filepath.Join(directory, "metadata.lock")
	descriptor, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(descriptor), path)
	if err := privateFile(path, false); err != nil {
		lock.Close()
		return nil, err
	}
	if err := syscall.Flock(descriptor, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrStoreLocked
		}
		return nil, err
	}
	return lock, nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func atomicFile(directory, name string, data []byte) error {
	temporary, err := os.CreateTemp(directory, ".state-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func encodeEnvelope(revision uint64, payload any) ([]byte, string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(encoded)
	checksum := hex.EncodeToString(digest[:])
	wrapped, err := json.Marshal(stateEnvelope{Version: 1, Revision: revision, Checksum: checksum, Payload: encoded})
	if err != nil {
		return nil, "", err
	}
	if len(wrapped) > 64<<20 {
		return nil, "", errors.New("metadata envelope exceeds size limit")
	}
	return wrapped, checksum, nil
}

func strictJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("invalid metadata JSON")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return errors.New("metadata JSON has trailing content")
	}
	return nil
}

func readEnvelope(directory, name string, destination any) (stateEnvelope, error) {
	path := filepath.Join(directory, name)
	if err := privateFile(path, false); err != nil {
		return stateEnvelope{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return stateEnvelope{}, err
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, (64<<20)+1))
	if err != nil {
		return stateEnvelope{}, err
	}
	if len(encoded) > 64<<20 {
		return stateEnvelope{}, errors.New("metadata envelope exceeds size limit")
	}
	var envelope stateEnvelope
	if err := strictJSON(encoded, &envelope); err != nil {
		return envelope, err
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.Version != 1 || envelope.Checksum != hex.EncodeToString(digest[:]) {
		return envelope, errors.New("metadata envelope checksum/version mismatch")
	}
	if err := strictJSON(envelope.Payload, destination); err != nil {
		return envelope, err
	}
	return envelope, nil
}

func loadState(directory string) (Metadata, epochFloor, error) {
	var metadata Metadata
	envelope, err := readEnvelope(directory, "metadata.json", &metadata)
	if err != nil {
		return metadata, epochFloor{}, fmt.Errorf("metadata unavailable: %w", err)
	}
	var floor epochFloor
	floorEnvelope, err := readEnvelope(directory, "epoch-floor.json", &floor)
	if err != nil {
		return metadata, floor, fmt.Errorf("epoch floor unavailable: %w", err)
	}
	if envelope.Revision != metadata.Revision || floorEnvelope.Revision != floor.Revision || floor.MetadataChecksum != envelope.Checksum {
		return metadata, floor, errors.New("metadata rollback or incomplete persistence")
	}
	return metadata, floor, nil
}

func validateDirectory(entries []os.DirEntry) error {
	for _, entry := range entries {
		name := entry.Name()
		if name == "metadata.json" || name == "epoch-floor.json" || name == "metadata.lock" {
			continue
		}
		if strings.HasPrefix(name, ".state-") && strings.HasSuffix(name, ".tmp") && entry.Type().IsRegular() {
			continue
		}
		return errors.New("unexpected file in private metadata directory")
	}
	return nil
}
