package platform

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const durabilityObservationEnvironment = "PERI_DURABILITY_OBSERVER"
const durabilityObservationFile = "durability-observation.jsonl"
const durabilityObservationBytes = 64 << 20

type serviceDurabilityObservation struct {
	observer *durabilityObserver
	stream   string
	limit    int64
	stopping chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	failed   atomic.Bool
	unjoined atomic.Bool
}

// Failure is diagnostic-only. The caller logs its absence and serves SQL with
// the exact original store and audit; it never repairs or replaces a sink path.
func startServiceDurabilityObservation(directory, enabled string) (*serviceDurabilityObservation, error) {
	if enabled == "" || enabled == "0" {
		return nil, nil
	}
	if enabled != "1" {
		return nil, errors.New("invalid durability observer profile")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("durability observer private directory unavailable")
	}
	identity, ok := info.Sys().(*syscall.Stat_t)
	if !ok || identity.Uid != uint32(os.Getuid()) {
		return nil, errors.New("durability observer private directory unavailable")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, errors.New("durability observer identity unavailable")
	}
	file, err := os.OpenFile(filepath.Join(directory, durabilityObservationFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, errors.New("durability observer file unavailable")
	}
	actual, statErr := file.Stat()
	parent, parentErr := os.Lstat(directory)
	if statErr != nil || parentErr != nil || !os.SameFile(info, parent) || !actual.Mode().IsRegular() || actual.Mode().Perm() != 0600 {
		file.Close()
		return nil, errors.New("durability observer file identity unavailable")
	}
	sink := &serviceDurabilityObservation{observer: newDurabilityObserver(durabilityObservationCapacity), stream: hex.EncodeToString(random[:]), limit: durabilityObservationBytes, stopping: make(chan struct{}), done: make(chan struct{})}
	go sink.consume(file)
	return sink, nil
}

// Binding occurs once before the service accepts requests. No persistent state
// or public StateStore interface is involved, including in the disabled path.
func bindDurabilityObservation(state StateStore, audit Audit, sink *serviceDurabilityObservation) {
	if sink == nil {
		return
	}
	if store, ok := state.(*stateStore); ok {
		store.operations.observation.Store(sink.observer)
	}
	if file, ok := audit.(*fileAudit); ok {
		file.observation.Store(sink.observer)
	}
}

// A shutdown budget can expire while a producer's Close goroutine is still
// running. Keep that uncertainty in the footer without extending its budget.
func (sink *serviceDurabilityObservation) serviceResult(result error) {
	if sink != nil && errors.Is(result, ErrShutdownIncomplete) {
		sink.unjoined.Store(true)
	}
}

// Service calls this after its bounded teardown attempts. Sealing makes a
// late event a counted loss rather than a panic; an unjoined producer is never
// reported as a complete capture even if it has not emitted that event yet.
func (sink *serviceDurabilityObservation) stop() bool {
	if sink == nil {
		return true
	}
	sink.stopOnce.Do(func() {
		sink.observer.seal()
		close(sink.stopping)
	})
	select {
	case <-sink.done:
		return !sink.failed.Load() && !sink.unjoined.Load()
	case <-time.After(100 * time.Millisecond):
		sink.failed.Store(true)
		return false
	}
}

func (sink *serviceDurabilityObservation) consume(output io.WriteCloser) {
	defer close(sink.done)
	defer func() {
		if output.Close() != nil {
			sink.failed.Store(true)
		}
	}()
	var written, skipped uint64
	var bytes int64
	capped := false
	write := func(value any, footer bool) bool {
		if sink.failed.Load() {
			return false
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			sink.failed.Store(true)
			return false
		}
		encoded = append(encoded, '\n')
		reserve := int64(2048)
		if footer {
			reserve = 0
		}
		if bytes+int64(len(encoded))+reserve > sink.limit {
			capped = true
			return false
		}
		count, err := output.Write(encoded)
		bytes += int64(count)
		if err != nil || count != len(encoded) {
			sink.failed.Store(true)
			return false
		}
		return true
	}
	write(map[string]any{"schema": "peri-go-durability-v1", "stream": sink.stream, "kind": "header", "pid": os.Getpid(), "mode": "boundaries", "capacity": cap(sink.observer.queue), "max_bytes": sink.limit, "coverage_complete": false, "clock": "local_monotonic_duration_wall_bounds", "scope": "post_bind_journal_audit_boundaries"}, false)
	consume := func(event durabilityEvent) {
		event.Schema, event.Stream = "peri-go-durability-v1", sink.stream
		if !capped && write(event, false) {
			written++
		} else {
			skipped++
		}
	}
	for {
		select {
		case event := <-sink.observer.queue:
			consume(event)
		case <-sink.stopping:
			for {
				select {
				case event := <-sink.observer.queue:
					consume(event)
				default:
					produced, dropped := sink.observer.counts()
					complete := !sink.failed.Load() && !sink.unjoined.Load() && !capped && dropped == 0 && skipped == 0 && written == produced
					write(map[string]any{"schema": "peri-go-durability-v1", "stream": sink.stream, "kind": "footer", "stream_complete": complete, "coverage_complete": false, "producers_joined": !sink.unjoined.Load(), "produced": produced, "written": written, "dropped": dropped, "skipped": skipped, "capped": capped, "write_failed": sink.failed.Load()}, true)
					if !complete || sink.failed.Load() {
						log.Printf("durability observer incomplete")
					}
					return
				}
			}
		}
	}
}
