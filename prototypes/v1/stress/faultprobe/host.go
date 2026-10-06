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
	encoded, err := exec.CommandContext(ctx, "ps", "-ww", "-p", strconv.Itoa(pid), "-o", "pid=,uid=,stat=,lstart=,command=").Output()
	if err != nil {
		if syscall.Kill(pid, 0) == syscall.ESRCH {
			return ProcessIdentity{}, ErrProcessGone
		}
		return ProcessIdentity{}, errors.New("process inspection failed")
	}
	return parseIdentity(string(encoded), pid)
}

func parseIdentity(encoded string, expected int) (ProcessIdentity, error) {
	fields := strings.Fields(encoded)
	if len(fields) < 9 {
		return ProcessIdentity{}, errors.New("incomplete process identity")
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid != expected {
		return ProcessIdentity{}, errors.New("PID mismatch")
	}
	uid, err := strconv.Atoi(fields[1])
	if err != nil || uid < 0 {
		return ProcessIdentity{}, errors.New("process UID unavailable")
	}
	if strings.Contains(fields[2], "Z") {
		return ProcessIdentity{}, ErrProcessGone
	}
	return ProcessIdentity{PID: pid, UID: uid, Start: strings.Join(fields[3:8], " "), Args: fields[8:]}, nil
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
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		current, inspectErr := host.Inspect(wait, actual.PID)
		if errors.Is(inspectErr, ErrProcessGone) {
			break
		}
		if inspectErr != nil || current.Start != actual.Start {
			return errors.New("process disappearance not proven or PID reused")
		}
		select {
		case <-wait.Done():
			return errors.New("worker exit unproven")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if err := safeDirectory(config.RunDir); err != nil {
		return err
	}
	current, err := validateCache(cache)
	if err != nil || !os.SameFile(current, cacheIdentity) {
		return errors.New("cache identity changed before deletion")
	}
	if err := os.RemoveAll(cache); err != nil {
		return errors.New("owned fixture cache deletion failed")
	}
	return log.Record(map[string]any{"type": "fault_completed", "worker": worker, "pid": actual.PID, "deleted_cache": cache, "authority_signaled": false, "launcher_signaled": false})
}
