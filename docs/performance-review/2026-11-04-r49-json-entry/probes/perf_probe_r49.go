package platform

// R49 one-shot upper-bound probe for the Hrana JSON pipeline response path.
// The file exists only in the probe worktree: it is not part of a release
// build, and every hook is nil when PERI_R49_PROBE_LOG is unset.
//
// The log line is space separated key=value pairs. Durations are nanoseconds,
// byte counts are bytes. Only large responses are sampled.

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"
)

const r49LargeBytes = 64 << 10

type r49ProbeContextKey struct{}

var (
	r49Once      sync.Once
	r49LogPath   string
	r49Active    bool
	r49LogMu     sync.Mutex
	r49LogFile   *os.File
	r49LogBroken bool
)

func r49Load() {
	r49LogPath = os.Getenv("PERI_R49_PROBE_LOG")
	if r49LogPath == "" {
		return
	}
	r49Active = true
}

func r49Enabled() bool {
	r49Once.Do(r49Load)
	return r49Active
}

// r49Probe is written from the request's own call stack only.
type r49Probe struct {
	entryMarshalNS int64
	budgetBytes    int64
	entries        [][]byte
	finalMarshalNS int64
	finalBytes     int64
	writeNS        int64
	assembleNS     int64
	assembleBytes  int64
	decodeNS       int64
	convertNS      int64
	rows           int64
}

func newR49Probe() *r49Probe {
	if !r49Enabled() {
		return nil
	}
	return &r49Probe{rows: -1}
}

func withR49Probe(ctx context.Context, probe *r49Probe) context.Context {
	if probe == nil {
		return ctx
	}
	return context.WithValue(ctx, r49ProbeContextKey{}, probe)
}

func r49ProbeFromContext(ctx context.Context) *r49Probe {
	if !r49Enabled() {
		return nil
	}
	probe, _ := ctx.Value(r49ProbeContextKey{}).(*r49Probe)
	return probe
}

// r49Assemble is the proxy for the next round's encoding: it joins the entry
// bytes the budget check already produced into the final response envelope.
func r49Assemble(baton *string, entries [][]byte) ([]byte, error) {
	batonBytes, err := json.Marshal(baton)
	if err != nil {
		return nil, err
	}
	size := len(`{"baton":`) + len(batonBytes) + len(`,"results":[`) + len(`]}`)
	for index, entry := range entries {
		size += len(entry)
		if index > 0 {
			size++
		}
	}
	out := make([]byte, 0, size)
	out = append(out, `{"baton":`...)
	out = append(out, batonBytes...)
	out = append(out, `,"results":[`...)
	for index, entry := range entries {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, entry...)
	}
	out = append(out, `]}`...)
	return out, nil
}

func (probe *r49Probe) sampleAssembly(baton *string) {
	if probe == nil {
		return
	}
	start := time.Now()
	assembled, err := r49Assemble(baton, probe.entries)
	probe.assembleNS = time.Since(start).Nanoseconds()
	if err == nil {
		probe.assembleBytes = int64(len(assembled))
	} else {
		probe.assembleBytes = -1
	}
}

func (probe *r49Probe) finish() {
	if probe == nil || !r49Enabled() {
		return
	}
	if probe.finalBytes < r49LargeBytes && probe.budgetBytes < r49LargeBytes {
		return
	}
	r49LogMu.Lock()
	defer r49LogMu.Unlock()
	if r49LogFile == nil && !r49LogBroken {
		file, err := os.OpenFile(r49LogPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			r49LogBroken = true
			return
		}
		r49LogFile = file
	}
	if r49LogFile == nil {
		return
	}
	line := "entry_marshal_ns=" + r49Itoa(probe.entryMarshalNS) +
		" budget_bytes=" + r49Itoa(probe.budgetBytes) +
		" final_marshal_ns=" + r49Itoa(probe.finalMarshalNS) +
		" final_bytes=" + r49Itoa(probe.finalBytes) +
		" write_ns=" + r49Itoa(probe.writeNS) +
		" assemble_ns=" + r49Itoa(probe.assembleNS) +
		" assemble_bytes=" + r49Itoa(probe.assembleBytes) +
		" decode_ns=" + r49Itoa(probe.decodeNS) +
		" convert_ns=" + r49Itoa(probe.convertNS) +
		" rows=" + r49Itoa(probe.rows) + "\n"
	if _, err := r49LogFile.WriteString(line); err != nil {
		r49LogBroken = true
	}
}

func r49Itoa(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		position--
		digits[position] = '-'
	}
	return string(digits[position:])
}
