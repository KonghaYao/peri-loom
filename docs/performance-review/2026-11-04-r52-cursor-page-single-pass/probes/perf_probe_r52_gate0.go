package platform

// R52 Gate 0 probe: proves the read cursor page takes the single-pass decode
// and that the adapter reuses the scanned page. Probe-only; never measured.

import (
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type r52GateCounter struct {
	calls atomic.Int64
	bytes atomic.Int64
}

func (counter *r52GateCounter) add(size int) {
	counter.calls.Add(1)
	counter.bytes.Add(int64(size))
}

var (
	r52GateOnce    sync.Once
	r52GateOn      bool
	r52GateLogPath string

	r52GatePageDecode      r52GateCounter
	r52GatePageScan        r52GateCounter
	r52GateAdapterFallback r52GateCounter
	r52GateJSONValid       r52GateCounter
	r52GateSkeleton        r52GateCounter
)

func r52GateEnabled() bool {
	r52GateOnce.Do(func() {
		r52GateLogPath = os.Getenv("PERI_R52_GATE0_LOG")
		if r52GateLogPath == "" {
			return
		}
		r52GateOn = true
		hranaCursorPageScanHook = func() { r52GatePageScan.add(0) }
		go r52GatePulse()
	})
	return r52GateOn
}

func r52GatePulse() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		r52GateDump()
	}
}

func r52GateDump() {
	line := "r52gate0"
	appendCounter := func(name string, counter *r52GateCounter) {
		line += " " + name + "_calls=" + strconv.FormatInt(counter.calls.Load(), 10)
		line += " " + name + "_bytes=" + strconv.FormatInt(counter.bytes.Load(), 10)
	}
	appendCounter("pagedecode", &r52GatePageDecode)
	appendCounter("pagescan", &r52GatePageScan)
	appendCounter("fallback", &r52GateAdapterFallback)
	appendCounter("jsonvalid", &r52GateJSONValid)
	appendCounter("skeleton", &r52GateSkeleton)
	file, err := os.OpenFile(r52GateLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		log.Printf("r52 gate0: %v", err)
		return
	}
	defer file.Close()
	_, _ = file.WriteString(line + "\n")
}
