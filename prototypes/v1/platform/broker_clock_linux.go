package platform

import (
	"errors"
	"syscall"
	"unsafe"
)

func continuousNanos() (uint64, error) {
	var value syscall.Timespec
	_, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, 7, uintptr(unsafe.Pointer(&value)), 0)
	if errno != 0 || value.Sec < 0 || value.Nsec < 0 || value.Nsec >= 1000000000 || uint64(value.Sec) > ^uint64(0)/1000000000 {
		return 0, errors.New("CLOCK_BOOTTIME unavailable")
	}
	nanoseconds := uint64(value.Sec) * 1000000000
	if ^uint64(0)-nanoseconds < uint64(value.Nsec) {
		return 0, errors.New("CLOCK_BOOTTIME overflow")
	}
	return nanoseconds + uint64(value.Nsec), nil
}
