package main

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// The gateway allocates hundreds of kilobytes per request (request decode, response
// re-encode, audit event) while its live heap stays at a few megabytes. The default
// collector target therefore cycles many times per second to reclaim almost nothing and
// charges the allocation path for assists it cannot avoid. A wider target cuts the cycle
// rate; the heap cap derived from the container's own memory promise keeps that wider
// target bounded. An explicit GOGC or GOMEMLIMIT wins, because the deployment configures
// the collector for reasons startup cannot observe.
const (
	collectorTargetPercent = 400
	collectorLimitShare    = 7 // tenths of the container limit
)

func tuneCollector() {
	if os.Getenv("GOGC") != "" || os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	if limit, ok := cgroupMemoryLimit(); ok {
		debug.SetMemoryLimit(limit / 10 * collectorLimitShare)
	}
	debug.SetGCPercent(collectorTargetPercent)
}

// cgroupMemoryLimit reports the memory the container is allowed to use. A container
// started without a memory limit reports "max" or a near-integer-max sentinel, and a
// host without cgroups reports nothing; both cases leave the collector target unbounded
// by us, as before.
func cgroupMemoryLimit() (int64, bool) {
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if limit, ok := parseCgroupLimit(string(raw)); ok {
			return limit, true
		}
	}
	return 0, false
}

func parseCgroupLimit(raw string) (int64, bool) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value <= 0 || value >= 1<<50 {
		return 0, false
	}
	return value, true
}
