package platform

// R48 one-shot probe. It records where one Hrana JSON pipeline execute reply
// spends its time when the result is large. The file exists only in the probe
// worktree: it is never part of a release build, and every hook is a nil check
// from the caller's point of view when PERI_R48_PROBE_LOG is unset.
//
// The log line is space separated key=value pairs. Durations are nanoseconds,
// byte counts are bytes, and -1 means the site did not run for that request.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// r48LargeResultBytes is the sampling threshold. A request whose statement
// result is smaller never reaches the log.
const r48LargeResultBytes = 64 << 10

type r48ProbeContextKey struct{}

type r48Settings struct {
	logPath  string
	dumpPath string
	proxy    bool
}

var (
	r48Once      sync.Once
	r48Config    r48Settings
	r48Active    bool
	r48DumpOnce  sync.Once
	r48LogMu     sync.Mutex
	r48LogFile   *os.File
	r48LogBroken bool
)

func r48Load() {
	path := os.Getenv("PERI_R48_PROBE_LOG")
	if path == "" {
		return
	}
	r48Active = true
	r48Config = r48Settings{
		logPath:  path,
		dumpPath: os.Getenv("PERI_R48_PROBE_DUMP"),
		proxy:    os.Getenv("PERI_R48_PROBE_PROXY") == "1",
	}
}

func r48Enabled() bool {
	r48Once.Do(r48Load)
	return r48Active
}

// r48Probe is per request. Every site below writes it from the request's own
// call stack, so no lock is needed.
type r48Probe struct {
	parseNS         int64
	validateNS      int64
	adapterDecodeNS int64
	budgetMarshalNS int64
	budgetBytes     int64
	finalMarshalNS  int64
	finalBytes      int64
	compactNS       int64
	escapeNS        int64
	validNS         int64
	compactOnlyNS   int64
	resultBytes     int64
	rows            int64
}

func newR48Probe() *r48Probe {
	if !r48Enabled() {
		return nil
	}
	return &r48Probe{
		parseNS:         -1,
		validateNS:      -1,
		adapterDecodeNS: -1,
		budgetMarshalNS: -1,
		budgetBytes:     -1,
		finalMarshalNS:  -1,
		finalBytes:      -1,
		compactNS:       -1,
		escapeNS:        -1,
		validNS:         -1,
		compactOnlyNS:   -1,
		resultBytes:     -1,
		rows:            -1,
	}
}

func withR48Probe(ctx context.Context, probe *r48Probe) context.Context {
	if probe == nil {
		return ctx
	}
	return context.WithValue(ctx, r48ProbeContextKey{}, probe)
}

func r48ProbeFromContext(ctx context.Context) *r48Probe {
	if !r48Enabled() {
		return nil
	}
	probe, _ := ctx.Value(r48ProbeContextKey{}).(*r48Probe)
	return probe
}

// r48ValidStatementResult is the timed front door for validStatementResult.
// Callers without a probe keep the original call and its cost.
func r48ValidStatementResult(encoded json.RawMessage, probe *r48Probe) bool {
	if probe == nil {
		return validStatementResult(encoded)
	}
	start := time.Now()
	valid := validStatementResult(encoded)
	probe.validateNS = time.Since(start).Nanoseconds()
	return valid
}

// measureProxy runs the two lower-bound scans over the same entry bytes the
// budget check just encoded. It only runs under PERI_R48_PROBE_PROXY=1.
func (probe *r48Probe) measureProxy(encoded []byte) {
	if probe == nil || !r48Config.proxy {
		return
	}
	start := time.Now()
	_ = json.Valid(encoded)
	probe.validNS = time.Since(start).Nanoseconds()
	start = time.Now()
	var compacted bytes.Buffer
	_ = json.Compact(&compacted, encoded)
	probe.compactOnlyNS = time.Since(start).Nanoseconds()
}

// dump writes the first sampled statement result to PERI_R48_PROBE_DUMP when
// the file does not exist yet. Results below the sampling threshold are skipped
// so a warm-up query cannot consume the one dump.
func (probe *r48Probe) dump(raw []byte) {
	if probe == nil || r48Config.dumpPath == "" || len(raw) < r48LargeResultBytes {
		return
	}
	r48DumpOnce.Do(func() {
		file, err := os.OpenFile(r48Config.dumpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return
		}
		_, _ = file.Write(raw)
		_ = file.Close()
	})
}

// finish appends one log line for this request when its statement result met
// the sampling threshold. Every failure is silent: the probe must never change
// the answer the client receives.
func (probe *r48Probe) finish() {
	if probe == nil || probe.resultBytes < r48LargeResultBytes {
		return
	}
	line := fmt.Sprintf(
		"parse_ns=%d validate_ns=%d adapter_decode_ns=%d budget_marshal_ns=%d budget_bytes=%d final_marshal_ns=%d final_bytes=%d compact_ns=%d escape_ns=%d valid_ns=%d compact_only_ns=%d result_bytes=%d rows=%d\n",
		probe.parseNS, probe.validateNS, probe.adapterDecodeNS,
		probe.budgetMarshalNS, probe.budgetBytes,
		probe.finalMarshalNS, probe.finalBytes,
		probe.compactNS, probe.escapeNS,
		probe.validNS, probe.compactOnlyNS,
		probe.resultBytes, probe.rows,
	)
	r48Append([]byte(line))
}

func r48Append(line []byte) {
	r48LogMu.Lock()
	defer r48LogMu.Unlock()
	if r48LogBroken {
		return
	}
	if r48LogFile == nil {
		file, err := os.OpenFile(r48Config.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			r48LogBroken = true
			return
		}
		r48LogFile = file
	}
	if _, err := r48LogFile.Write(line); err != nil {
		r48LogBroken = true
	}
}
