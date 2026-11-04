package platform

// R50 one-shot upper-bound probe for the Hrana protobuf pipeline path.
// The file exists only in the probe worktree: it is not part of a release
// build, and every hook is nil when PERI_R50_PROBE_LOG is unset.
//
// The log line is space separated key=value pairs. Durations are nanoseconds,
// byte counts are bytes.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

type r50ProbeContextKey struct{}

var (
	r50Once      sync.Once
	r50LogPath   string
	r50Active    bool
	r50LogMu     sync.Mutex
	r50LogFile   *os.File
	r50LogBroken bool
)

func r50Load() {
	r50LogPath = os.Getenv("PERI_R50_PROBE_LOG")
	if r50LogPath == "" {
		return
	}
	file, err := os.OpenFile(r50LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	r50LogFile = file
	r50Active = true
}

func r50Enabled() bool {
	r50Once.Do(r50Load)
	return r50Active
}

type r50Probe struct {
	start       time.Time
	openNS      int64
	execNS      int64
	encodeNS    int64
	writeNS     int64
	totalNS     int64
	callNS      int64
	decodeNS    int64
	convertNS   int64
	metricsNS   int64
	resultBytes int64
	rows        int64
	cols        int64
	cells       int64
}

func newR50Probe() *r50Probe {
	if !r50Enabled() {
		return nil
	}
	return &r50Probe{start: time.Now(), rows: -1, cols: -1, cells: -1}
}

func withR50Probe(ctx context.Context, probe *r50Probe) context.Context {
	if probe == nil {
		return ctx
	}
	return context.WithValue(ctx, r50ProbeContextKey{}, probe)
}

func r50ProbeFromContext(ctx context.Context) *r50Probe {
	if !r50Enabled() {
		return nil
	}
	probe, _ := ctx.Value(r50ProbeContextKey{}).(*r50Probe)
	return probe
}

func (probe *r50Probe) log() {
	if probe == nil || !r50Active || r50LogBroken {
		return
	}
	probe.totalNS = time.Since(probe.start).Nanoseconds()
	line := fmt.Sprintf(
		"r50 open_ns=%d exec_ns=%d encode_ns=%d write_ns=%d total_ns=%d call_ns=%d decode_ns=%d convert_ns=%d metrics_ns=%d result_bytes=%d rows=%d cols=%d cells=%d\n",
		probe.openNS, probe.execNS, probe.encodeNS, probe.writeNS, probe.totalNS,
		probe.callNS, probe.decodeNS, probe.convertNS, probe.metricsNS,
		probe.resultBytes, probe.rows, probe.cols, probe.cells)
	r50LogMu.Lock()
	defer r50LogMu.Unlock()
	if r50LogFile == nil {
		r50LogBroken = true
		return
	}
	if _, err := r50LogFile.WriteString(line); err != nil {
		r50LogBroken = true
	}
}
