package platform

import (
	"errors"
	"math/bits"
	"sync"
)

type brokerClock struct {
	mutex       sync.Mutex
	sample      func() (uint64, error)
	last        uint64
	initialized bool
	failed      bool
}

func (clock *brokerClock) now() (uint64, error) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	if clock.failed {
		return 0, errors.New("continuous clock unavailable")
	}
	now, err := clock.sample()
	if err != nil || (clock.initialized && now < clock.last) {
		clock.failed = true
		return 0, errors.New("continuous clock unavailable or regressed")
	}
	clock.last = now
	clock.initialized = true
	return now, nil
}
func clockScale(ticks uint64, numerator, denominator uint32) (uint64, error) {
	if denominator == 0 || numerator == 0 {
		return 0, errors.New("continuous clock timebase invalid")
	}
	high, low := bits.Mul64(ticks, uint64(numerator))
	if high >= uint64(denominator) {
		return 0, errors.New("continuous clock overflow")
	}
	value, _ := bits.Div64(high, low, uint64(denominator))
	return value, nil
}
