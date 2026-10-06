package stress

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SerialLock struct{ paths []string }

func acquireLocks(output, global string) (*SerialLock, error) {
	if err := os.MkdirAll(output, 0700); err != nil {
		return nil, errors.New("output directory unavailable")
	}
	info, err := os.Lstat(output)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("output directory must be private and not a symlink")
	}
	lock := &SerialLock{}
	for _, path := range []string{global, filepath.Join(output, ".stress.lock")} {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			lock.Close()
			return nil, errors.New("another serial stress runner holds the lock; never automatically remove stale locks")
		}
		lock.paths = append(lock.paths, path)
		_, writeErr := fmt.Fprintf(file, "pid=%d observed=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			lock.Close()
			return nil, errors.New("cannot persist runner lock")
		}
	}
	return lock, nil
}

func (lock *SerialLock) Close() {
	for index := len(lock.paths) - 1; index >= 0; index-- {
		_ = os.Remove(lock.paths[index])
	}
	lock.paths = nil
}

type Artifacts struct {
	mu        sync.Mutex
	file      *os.File
	maximum   int64
	written   int64
	secrets   []string
	directory string
	failed    atomic.Bool
}

func NewArtifacts(directory string, maximum int64, secrets []string) (*Artifacts, error) {
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, errors.New("run directory already exists or unavailable")
	}
	file, err := os.OpenFile(filepath.Join(directory, "operations.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.New("raw artifact unavailable")
	}
	return &Artifacts{file: file, maximum: maximum, secrets: secrets, directory: directory}, nil
}

func (artifacts *Artifacts) clean(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New("artifact encoding failed")
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if decoder.Decode(&decoded) != nil {
		return nil, errors.New("artifact encoding invalid")
	}
	redact := func(text string) string {
		for _, secret := range artifacts.secrets {
			if secret != "" {
				text = strings.ReplaceAll(text, secret, "[REDACTED]")
			}
		}
		return text
	}
	var walk func(any) any
	walk = func(input any) any {
		switch typed := input.(type) {
		case string:
			return redact(typed)
		case []any:
			for index := range typed {
				typed[index] = walk(typed[index])
			}
			return typed
		case map[string]any:
			output := make(map[string]any, len(typed))
			for key, field := range typed {
				lower := strings.ReplaceAll(strings.ToLower(key), "_", "")
				if strings.Contains(lower, "credential") || strings.Contains(lower, "token") || strings.Contains(lower, "apikey") || strings.Contains(lower, "managementkey") || strings.Contains(lower, "privatekey") || strings.Contains(lower, "authorization") || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "dek") {
					output[redact(key)] = "[REDACTED]"
				} else {
					output[redact(key)] = walk(field)
				}
			}
			return output
		default:
			return input
		}
	}
	return json.Marshal(walk(decoded))
}

func (artifacts *Artifacts) Record(value any) error {
	if artifacts.failed.Load() {
		return errors.New("artifact recording previously failed")
	}
	encoded, err := artifacts.clean(value)
	if err != nil {
		artifacts.failed.Store(true)
		return err
	}
	artifacts.mu.Lock()
	defer artifacts.mu.Unlock()
	if int64(len(encoded)+1) > artifacts.maximum-artifacts.written {
		artifacts.failed.Store(true)
		return errors.New("raw artifact budget exceeded")
	}
	encoded = append(encoded, '\n')
	written, err := artifacts.file.Write(encoded)
	artifacts.written += int64(written)
	if err != nil {
		artifacts.failed.Store(true)
		return errors.New("raw artifact write failed")
	}
	return nil
}

func (artifacts *Artifacts) Summary(value any) error {
	encoded, err := artifacts.clean(value)
	if err != nil || len(encoded) > 4<<20 {
		return errors.New("summary encoding failed or oversized")
	}
	file, err := os.OpenFile(filepath.Join(artifacts.directory, "run.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("summary file unavailable")
	}
	defer file.Close()
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return errors.New("summary write failed")
	}
	return file.Sync()
}

func (artifacts *Artifacts) Close() error {
	artifacts.mu.Lock()
	defer artifacts.mu.Unlock()
	return artifacts.file.Close()
}
