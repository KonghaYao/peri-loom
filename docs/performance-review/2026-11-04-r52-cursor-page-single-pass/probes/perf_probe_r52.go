package platform

// R52 upper-bound probe: measures the passes a single-pass worker-reply decode
// and span hand-off would remove (decode validation, page re-scans, capture
// re-scans). This file exists only in the probe worktree and is never part of
// a measured binary. Set PERI_R52_PROBE_LOG to a writable path to enable it.

import (
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type r52ProbeCounter struct {
	calls atomic.Int64
	ns    atomic.Int64
	bytes atomic.Int64
}

func (counter *r52ProbeCounter) add(elapsed time.Duration, size int) {
	counter.calls.Add(1)
	counter.ns.Add(elapsed.Nanoseconds())
	counter.bytes.Add(int64(size))
}

var (
	r52ProbeOnce    sync.Once
	r52ProbeOn      bool
	r52ProbeLogPath string

	r52UTF8           r52ProbeCounter
	r52JSONValid      r52ProbeCounter
	r52Fields         r52ProbeCounter
	r52ResultCopy     r52ProbeCounter
	r52GatewayFetch   r52ProbeCounter
	r52GatewayFirst   r52ProbeCounter
	r52AdapterScan    r52ProbeCounter
	r52CaptureCompact r52ProbeCounter
	r52CaptureEscape  r52ProbeCounter
)

func r52ProbeEnabled() bool {
	r52ProbeOnce.Do(func() {
		r52ProbeLogPath = os.Getenv("PERI_R52_PROBE_LOG")
		if r52ProbeLogPath == "" {
			return
		}
		r52ProbeOn = true
		go r52ProbePulse()
	})
	return r52ProbeOn
}

func r52ProbePulse() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		r52ProbeDump()
	}
}

func r52ProbeDump() {
	line := "r52probe"
	appendCounter := func(name string, counter *r52ProbeCounter) {
		line += " " + name + "_calls=" + strconv.FormatInt(counter.calls.Load(), 10)
		line += " " + name + "_ms=" + strconv.FormatInt(counter.ns.Load()/1e6, 10)
		line += " " + name + "_us=" + strconv.FormatInt(counter.ns.Load()/1e3, 10)
		line += " " + name + "_bytes=" + strconv.FormatInt(counter.bytes.Load(), 10)
	}
	appendCounter("utf8", &r52UTF8)
	appendCounter("jsonvalid", &r52JSONValid)
	appendCounter("fields", &r52Fields)
	appendCounter("copy", &r52ResultCopy)
	appendCounter("gwfetch", &r52GatewayFetch)
	appendCounter("gwfirst", &r52GatewayFirst)
	appendCounter("adapter", &r52AdapterScan)
	appendCounter("compact", &r52CaptureCompact)
	appendCounter("escape", &r52CaptureEscape)
	file, err := os.OpenFile(r52ProbeLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		log.Printf("r52 probe: %v", err)
		return
	}
	defer file.Close()
	_, _ = file.WriteString(line + "\n")
}
