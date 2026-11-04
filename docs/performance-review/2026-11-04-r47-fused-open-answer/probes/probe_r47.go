package platform

// Temporary R47 attribution probe. Never merged: it only records per-stage
// wall time on the real request path so the upper bound of the planned span
// transcode can be compared with the acceptance bar before writing it.

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

var (
	probeR47DecodeNanos  atomic.Int64
	probeR47DecodeCalls  atomic.Int64
	probeR47ConvertNanos atomic.Int64
	probeR47ConvertCalls atomic.Int64
	probeR47BudgetNanos  atomic.Int64
	probeR47BudgetCalls  atomic.Int64
	probeR47BudgetBytes  atomic.Int64
	probeR47ResultNanos  atomic.Int64
	probeR47ResultCalls  atomic.Int64
	probeR47ResultBytes  atomic.Int64
	probeR47FinalNanos   atomic.Int64
	probeR47FinalCalls   atomic.Int64
	probeR47FinalBytes   atomic.Int64

	probeR47ValidateNanos      atomic.Int64
	probeR47ValidateCalls      atomic.Int64
	probeR47ValidateBytes      atomic.Int64
	probeR47ParseNanos         atomic.Int64
	probeR47ParseCalls         atomic.Int64
	probeR47ParseBytes         atomic.Int64
	probeR47OpenUnmarshalNanos atomic.Int64
	probeR47OpenUnmarshalCalls atomic.Int64
	probeR47OpenUnmarshalBytes atomic.Int64
)

func probeR47Record(label string, total, calls, bytes *atomic.Int64, started time.Time, size int) {
	total.Add(time.Since(started).Nanoseconds())
	n := calls.Add(1)
	if bytes != nil {
		bytes.Add(int64(size))
	}
	var meanBytes float64
	if bytes != nil {
		meanBytes = float64(bytes.Load()) / float64(n)
	}
	fmt.Fprintf(os.Stderr, "probe=r47 stage=%s calls=%d mean_ms=%.4f mean_bytes=%.0f\n", label, n, float64(total.Load())/float64(n)/1e6, meanBytes)
}
