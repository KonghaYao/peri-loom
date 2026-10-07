package faultprobe

import (
	"context"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var ErrProcessGone = errors.New("process gone")
var ErrProcessZombie = errors.New("process remains a zombie")
var ErrIdentityIncomplete = errors.New("incomplete process identity")
var ErrIdentityMismatch = errors.New("process identity mismatch")
var ErrInspectionUnavailable = errors.New("process inspection unavailable")

type ProcessIdentity struct {
	PID   int
	UID   int
	Start string
	Args  []string
}

type Host interface {
	Inspect(context.Context, int) (ProcessIdentity, error)
	SignalKill(int) error
}

type LocalHost struct{}

func (LocalHost) Inspect(ctx context.Context, pid int) (ProcessIdentity, error) {
	return inspectProcess(ctx, pid, func(ctx context.Context, pid int) ([]byte, error) {
		command := exec.CommandContext(ctx, "ps", "-ww", "-p", strconv.Itoa(pid), "-o", "pid=,uid=,stat=,lstart=,command=")
		command.WaitDelay = 100 * time.Millisecond
		return command.Output()
	}, func(pid int) error { return syscall.Kill(pid, 0) })
}

func inspectProcess(ctx context.Context, pid int, observe func(context.Context, int) ([]byte, error), probe func(int) error) (ProcessIdentity, error) {
	if pid <= 1 {
		return ProcessIdentity{}, ErrIdentityMismatch
	}
	if ctx.Err() != nil {
		return ProcessIdentity{}, ctx.Err()
	}
	encoded, observationErr := observe(ctx, pid)
	if ctx.Err() != nil {
		return ProcessIdentity{}, ctx.Err()
	}
	identity, parseErr := parseIdentity(string(encoded), pid)
	if errors.Is(parseErr, ErrIdentityMismatch) {
		return identity, parseErr
	}
	if observationErr != nil || parseErr != nil {
		absent := errors.Is(probe(pid), syscall.ESRCH)
		if ctx.Err() != nil {
			return identity, ctx.Err()
		}
		if absent {
			return identity, ErrProcessGone
		}
		if observationErr != nil {
			return identity, ErrInspectionUnavailable
		}
		return identity, parseErr
	}
	return identity, nil
}

func parseIdentity(encoded string, expected int) (ProcessIdentity, error) {
	fields := strings.Fields(encoded)
	identity := ProcessIdentity{UID: -1}
	if len(fields) == 0 {
		return identity, ErrIdentityIncomplete
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return identity, ErrIdentityIncomplete
	}
	identity.PID = pid
	if pid != expected {
		return identity, ErrIdentityMismatch
	}
	if len(fields) < 2 {
		return identity, ErrIdentityIncomplete
	}
	uid, err := strconv.Atoi(fields[1])
	if err != nil || uid < 0 {
		return identity, ErrIdentityIncomplete
	}
	identity.UID = uid
	if len(fields) < 8 {
		return identity, ErrIdentityIncomplete
	}
	identity.Start = strings.Join(fields[3:8], " ")
	if strings.Contains(fields[2], "Z") {
		return identity, ErrProcessZombie
	}
	identity.Args = fields[8:]
	if len(identity.Args) == 0 {
		return identity, ErrIdentityIncomplete
	}
	return identity, nil
}

func (LocalHost) SignalKill(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }

func safeDirectory(path string) error {
	info, err := os.Lstat(path)
	canonical, resolveErr := filepath.EvalSymlinks(path)
	if err != nil || resolveErr != nil || canonical != path || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("canonical private directory required")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return errors.New("fixture must belong to caller")
	}
	return nil
}

func workerCache(config Config, worker string) string {
	if worker == "worker-a" {
		return filepath.Join(config.RunDir, "cache-a")
	}
	if worker == "worker-b" {
		return filepath.Join(config.RunDir, "cache-b")
	}
	return ""
}

func validateWorker(config Config, topology Topology, worker string, actual ProcessIdentity) error {
	cache := workerCache(config, worker)
	endpoint, err := url.Parse(topology.Workers[worker])
	if cache == "" || err != nil || actual.PID <= 1 || actual.PID == os.Getpid() || actual.UID != os.Getuid() || actual.PID != topology.Processes[worker].PID || actual.Start == "" {
		return errors.New("worker identity not in topology")
	}
	expected := []string{config.RustBinary, "owned-worker", topology.Authority, cache, worker, endpoint.Host}
	if !reflect.DeepEqual(actual.Args, expected) {
		return errors.New("exact Rust owned-worker/cache argv mismatch")
	}
	return safeDirectory(cache)
}

func validateCache(path string) (os.FileInfo, error) {
	if safeDirectory(path) != nil {
		return nil, errors.New("unsafe cache path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("cache identity unavailable")
	}
	files, total := 0, int64(0)
	err = filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.Type()&os.ModeSymlink != 0 {
			return errors.New("cache traversal or symlink rejected")
		}
		files++
		actual, err := entry.Info()
		if err != nil || !actual.IsDir() && !actual.Mode().IsRegular() {
			return errors.New("cache special file rejected")
		}
		total += actual.Size()
		if files > 10000 || total > 256<<20 {
			return errors.New("cache deletion bound exceeded")
		}
		return nil
	})
	return info, err
}

func killOwned(ctx context.Context, config Config, topology Topology, worker string, original ProcessIdentity, host Host, log *Log) error {
	if !config.AllowWorkerKill || !cleanAbsolute(config.RunDir) || !strings.HasPrefix(filepath.Base(config.RunDir), "peri-stress-") {
		return errors.New("explicit worker SIGKILL authorization absent")
	}
	for _, other := range []string{"authority", "platform", opposite(worker)} {
		if original.PID == topology.Processes[other].PID {
			return errors.New("non-owner process is not a signal target")
		}
	}
	if original.PID == topology.LauncherPID {
		return errors.New("launcher is not a signal target")
	}
	actual, err := host.Inspect(ctx, original.PID)
	if err != nil || actual.Start != original.Start || validateWorker(config, topology, worker, actual) != nil {
		return errors.New("worker identity changed before signal")
	}
	cache := workerCache(config, worker)
	cacheIdentity, err := validateCache(cache)
	if err != nil {
		return err
	}
	if err := log.Record(map[string]any{"type": "fault_intent", "worker": worker, "pid": actual.PID, "cache": cache, "process_start": actual.Start, "signal": "SIGKILL"}); err != nil {
		return err
	}
	actual, err = host.Inspect(ctx, original.PID)
	if err != nil || actual.Start != original.Start || validateWorker(config, topology, worker, actual) != nil {
		return errors.New("worker identity changed at signal boundary")
	}
	if ctx.Err() != nil {
		return errors.New("fault canceled before signal")
	}
	if err := host.SignalKill(actual.PID); err != nil {
		return errors.New("owned worker signal failed")
	}
	if err := log.Record(map[string]any{"type": "fault_signal_sent", "worker": worker, "pid": actual.PID, "signal": "SIGKILL"}); err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := waitOwnedExit(wait, worker, actual, host, log); err != nil {
		return err
	}
	if err := safeDirectory(config.RunDir); err != nil {
		return err
	}
	current, err := validateCache(cache)
	if err != nil || !os.SameFile(current, cacheIdentity) {
		return errors.New("cache identity changed before deletion")
	}
	if wait.Err() != nil {
		return uncertainExit(log, worker, actual.PID, "deadline_or_cancel_before_deletion")
	}
	if err := os.RemoveAll(cache); err != nil {
		return errors.New("owned fixture cache deletion failed")
	}
	return log.Record(map[string]any{"type": "fault_completed", "worker": worker, "pid": actual.PID, "deleted_cache": cache, "authority_signaled": false, "launcher_signaled": false})
}

func observationStatus(expected, current ProcessIdentity, failure error) (string, error) {
	if errors.Is(failure, ErrIdentityMismatch) || current.PID != 0 && current.PID != expected.PID ||
		current.PID != 0 && current.UID >= 0 && current.UID != expected.UID ||
		current.Start != "" && current.Start != expected.Start {
		return "identity_mismatch", ErrIdentityMismatch
	}
	if errors.Is(failure, ErrProcessZombie) {
		return "zombie", nil
	}
	if len(current.Args) > len(expected.Args) {
		return "argv_mismatch", ErrIdentityMismatch
	}
	for index, argument := range current.Args {
		if argument != expected.Args[index] {
			return "argv_mismatch", ErrIdentityMismatch
		}
	}
	if errors.Is(failure, ErrProcessGone) {
		return "gone", nil
	}
	if errors.Is(failure, ErrIdentityIncomplete) {
		return "identity_incomplete", nil
	}
	if failure != nil {
		return "inspection_unavailable", nil
	}
	if current.PID != expected.PID || current.UID != expected.UID || current.Start == "" || len(current.Args) != len(expected.Args) {
		return "identity_incomplete", nil
	}
	return "same_identity", nil
}

func uncertainExit(log *Log, worker string, pid int, reason string) error {
	if err := log.Record(map[string]any{"type": "fault_exit_uncertain", "worker": worker, "pid": pid, "reason": reason}); err != nil {
		return err
	}
	return errors.New("worker exit unproven: " + reason)
}

func waitOwnedExit(ctx context.Context, worker string, expected ProcessIdentity, host Host, log *Log) error {
	started := time.Now()
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return uncertainExit(log, worker, expected.PID, "deadline_or_cancel")
		}
		current, inspectErr := host.Inspect(ctx, expected.PID)
		if ctx.Err() != nil {
			return uncertainExit(log, worker, expected.PID, "deadline_or_cancel")
		}
		status, mismatch := observationStatus(expected, current, inspectErr)
		if mismatch != nil {
			return uncertainExit(log, worker, expected.PID, status)
		}
		if status == "gone" {
			return log.Record(map[string]any{"type": "fault_process_gone", "worker": worker, "pid": expected.PID, "proof": "host_esrch", "wait_ms": time.Since(started).Milliseconds()})
		}
		if err := log.Record(map[string]any{"type": "fault_process_wait", "worker": worker, "pid": expected.PID, "observation": status, "attempt": attempt, "wait_ms": time.Since(started).Milliseconds()}); err != nil {
			return err
		}
		delay := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			delay.Stop()
			return uncertainExit(log, worker, expected.PID, "deadline_or_cancel")
		case <-delay.C:
		}
	}
}
