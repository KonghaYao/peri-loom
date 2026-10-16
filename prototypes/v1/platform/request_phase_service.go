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

type serviceRequestPhaseRecord struct {
	Stream        string                `json:"stream"`
	RequestID     string                `json:"request_id"`
	Action        string                `json:"action"`
	Outcome       string                `json:"outcome"`
	Status        int                   `json:"status"`
	TotalNS       uint64                `json:"total_ns"`
	AccountedNS   uint64                `json:"accounted_ns"`
	UnaccountedNS uint64                `json:"unaccounted_ns"`
	Truncated     bool                  `json:"truncated"`
	Spans         []requestPhaseSegment `json:"spans"`
	// A bounded observer that stopped early says so instead of looking complete.
	Complete bool `json:"complete"`
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
	count := 0
	var failure error
	// The stream is bounded, so the consumer stops with the profile instead of following a
	// request burst forever; what it wrote stays truthful because it is written in order.
	drain := func() {
		for count < requestPhaseLimit {
			record, found := sink.observer.TryReceive()
			if !found {
				return
			}
			count++
			if failure != nil {
				continue
			}
			projected := serviceRequestPhaseRecord{Stream: sink.stream, RequestID: record.RequestID, Action: record.Action, Outcome: record.Outcome, Status: record.Status, TotalNS: record.TotalNS, AccountedNS: record.AccountedNS, UnaccountedNS: record.UnaccountedNS, Truncated: record.Truncated, Spans: record.Spans}
			if failure = encoder.Encode(projected); failure != nil {
				return
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			drain()
			sink.finish(drain, encoder, count, failure)
			return
		case <-sink.stopping:
			drain()
			sink.finish(drain, encoder, count, failure)
			return
		case <-ticker.C:
			drain()
		}
	}
}

// finish writes the closing line that states whether the stream is complete, so a reader never
// mistakes a bounded stop for a full capture.
func (sink *serviceRequestPhaseSink) finish(drain func(), encoder *json.Encoder, count int, failure error) {
	if failure != nil {
		return
	}
	if count < requestPhaseLimit {
		_ = encoder.Encode(serviceRequestPhaseRecord{Stream: sink.stream, Action: "observer", Outcome: "stopped", Complete: true})
		return
	}
	_ = encoder.Encode(serviceRequestPhaseRecord{Stream: sink.stream, Action: "observer", Outcome: "bounded", Complete: false})
}
