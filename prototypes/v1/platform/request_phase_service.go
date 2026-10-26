package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// The phase stream is a second sink of the same private diagnostic profile that already owns
// the SQL open observer: one switch, the same private-directory validation, an exclusive 0600
// file, a bounded queue, and a structured line per request. It is a separate file so the open
// observer's own artifact keeps exactly the bytes its consumers already read.
const requestPhaseFileName = "request-phase-observation.jsonl"

// One drain writes a bounded batch so a producer that keeps the ring full cannot starve the
// stop/context branch; the stop path repeats the batch until the ring is empty.
const requestPhaseDrainBatch = 64

type serviceRequestPhaseRecord struct {
	Stream         string                `json:"stream"`
	RequestID      string                `json:"request_id"`
	OuterRequestID string                `json:"outer_request_id,omitempty"`
	HranaStep      uint64                `json:"hrana_step,omitempty"`
	Action         string                `json:"action"`
	Outcome        string                `json:"outcome"`
	Status         int                   `json:"status"`
	Window         uint64                `json:"window"`
	TotalNS        uint64                `json:"total_ns"`
	AccountedNS    uint64                `json:"accounted_ns"`
	UnaccountedNS  uint64                `json:"unaccounted_ns"`
	Truncated      bool                  `json:"truncated"`
	Spans          []requestPhaseSegment `json:"spans"`
	// A bounded observer that stopped early says so instead of looking complete.
	Complete bool `json:"complete"`
}

// serviceRequestPhaseClose is the closing line. Filtered is always present so a complete
// stream cannot be mistaken for a capture of every request. Dropped separates the sampler
// from the ring so a reader can tell a designed sample from lost evidence.
type serviceRequestPhaseClose struct {
	Stream                 string `json:"stream"`
	Action                 string `json:"action"`
	Outcome                string `json:"outcome"`
	Complete               bool   `json:"complete"`
	Accepted               uint64 `json:"accepted"`
	Sampled                uint64 `json:"sampled"`
	Filtered               uint64 `json:"filtered"`
	KeptSlow               uint64 `json:"kept_slow"`
	KeptFailure            uint64 `json:"kept_failure"`
	Dropped                uint64 `json:"dropped"`
	DroppedSampledOut      uint64 `json:"dropped_sampled_out"`
	DroppedRingOverwritten uint64 `json:"dropped_ring_overwritten"`
	Windows                uint64 `json:"windows"`
	WindowMS               int64  `json:"window_ms"`
	SamplePermille         uint64 `json:"sample_permille"`
	SlowThresholdMS        int64  `json:"slow_threshold_ms"`
	Capacity               int    `json:"capacity"`
}

type serviceRequestPhaseSink struct {
	observer *requestPhaseObserver
	stream   string
	stopping chan struct{}
	done     chan struct{}
}

// phaseSink opens the phase stream for the profile. It returns nil when the observer is nil,
// which is the default-off path, and otherwise validates the destination exactly like the
// existing observer: a private directory owned by this user, an exclusive regular 0600 file,
// and a path that is still the same file after it was opened.
func phaseSink(ctx context.Context, observer *requestPhaseObserver, directory string) (*serviceRequestPhaseSink, error) {
	if observer == nil {
		return nil, nil
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("request phase observer private directory unavailable")
	}
	identity, ok := info.Sys().(*syscall.Stat_t)
	if !ok || identity.Uid != uint32(os.Getuid()) {
		return nil, errors.New("request phase observer private directory unavailable")
	}
	streamBytes := make([]byte, 16)
	if _, err := rand.Read(streamBytes); err != nil {
		return nil, errors.New("request phase observer stream unavailable")
	}
	path := filepath.Join(directory, requestPhaseFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, errors.New("request phase observer file unavailable")
	}
	actual, statError := file.Stat()
	parent, parentError := os.Lstat(directory)
	if statError != nil || parentError != nil || !os.SameFile(info, parent) || !actual.Mode().IsRegular() || actual.Mode().Perm() != 0600 {
		file.Close()
		return nil, errors.New("request phase observer file identity unavailable")
	}
	sink := &serviceRequestPhaseSink{observer: observer, stream: hex.EncodeToString(streamBytes), stopping: make(chan struct{}), done: make(chan struct{})}
	go sink.consume(ctx, file)
	return sink, nil
}

func (sink *serviceRequestPhaseSink) stop() bool {
	if sink == nil {
		return true
	}
	close(sink.stopping)
	select {
	case <-sink.done:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (sink *serviceRequestPhaseSink) consume(ctx context.Context, output io.WriteCloser) {
	defer close(sink.done)
	defer output.Close()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	encoder := json.NewEncoder(output)
	var failure error
	// The ring is the memory bound. A reader that falls behind lets the producer overwrite the
	// oldest records; that loss is reported by the closing line instead of being hidden or
	// turning into an unbounded queue.
	drain := func(limit int) int {
		processed := 0
		for processed < limit {
			record, found := sink.observer.TryReceive()
			if !found {
				break
			}
			processed++
			if failure != nil {
				continue
			}
			projected := serviceRequestPhaseRecord{Stream: sink.stream, RequestID: record.RequestID,
				OuterRequestID: record.OuterRequestID, HranaStep: record.HranaStep,
				Action: record.Action, Outcome: record.Outcome, Status: record.Status, Window: record.Window,
				TotalNS: record.TotalNS, AccountedNS: record.AccountedNS, UnaccountedNS: record.UnaccountedNS,
				Truncated: record.Truncated, Spans: record.Spans}
			if failure = encoder.Encode(projected); failure != nil {
				break
			}
		}
		return processed
	}
	// The stop path drains at most one ring, so a producer that is still running cannot keep
	// the shutdown waiting on records it is adding.
	drainAll := func() {
		for remaining := sink.observer.capacity; remaining > 0; {
			processed := drain(requestPhaseDrainBatch)
			if processed == 0 {
				return
			}
			remaining -= processed
		}
	}
	for {
		select {
		case <-ctx.Done():
			drainAll()
			sink.finish(encoder, failure)
			return
		case <-sink.stopping:
			drainAll()
			sink.finish(encoder, failure)
			return
		case <-ticker.C:
			drain(requestPhaseDrainBatch)
		}
	}
}

// finish writes the closing line that states whether the stream is complete, so a reader never
// mistakes an overwritten ring for a full capture.
func (sink *serviceRequestPhaseSink) finish(encoder *json.Encoder, failure error) {
	if failure != nil {
		return
	}
	stats := sink.observer.stats()
	complete := stats.Overwritten == 0
	outcome := "stopped"
	if !complete {
		outcome = "bounded"
	}
	_ = encoder.Encode(serviceRequestPhaseClose{
		Stream:                 sink.stream,
		Action:                 "observer",
		Outcome:                outcome,
		Complete:               complete,
		Accepted:               stats.Accepted,
		Sampled:                stats.Sampled,
		Filtered:               stats.Filtered,
		KeptSlow:               stats.KeptSlow,
		KeptFailure:            stats.KeptFailure,
		Dropped:                stats.Dropped,
		DroppedSampledOut:      stats.Filtered,
		DroppedRingOverwritten: stats.Overwritten,
		Windows:                stats.Windows,
		WindowMS:               sink.observer.window.Milliseconds(),
		SamplePermille:         sink.observer.samplePermille,
		SlowThresholdMS:        sink.observer.slowThreshold.Milliseconds(),
		Capacity:               sink.observer.capacity,
	})
}
