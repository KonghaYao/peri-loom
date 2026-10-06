//go:build darwin && cgo

package platform

/*
#include <mach/mach_time.h>
#include <stdint.h>
static int peri_continuous_clock(uint64_t *ticks, uint32_t *numer, uint32_t *denom) {
    mach_timebase_info_data_t info;
    int status = mach_timebase_info(&info);
    if (status != 0) return status;
    *ticks = mach_continuous_time();
    *numer = info.numer;
    *denom = info.denom;
    return 0;
}
*/
import "C"

import "errors"

func continuousNanos() (uint64, error) {
	var ticks C.uint64_t
	var numerator, denominator C.uint32_t
	if C.peri_continuous_clock(&ticks, &numerator, &denominator) != 0 {
		return 0, errors.New("continuous clock unavailable")
	}
	return clockScale(uint64(ticks), uint32(numerator), uint32(denominator))
}
