package platform

import (
	"log"
	"sync"
)

// deferredDurability is the fail-closed account of completion barriers that a
// non-committing operation pays after its reply. The answer that owned such a
// barrier is already written when the barrier returns, so its failure can no
// longer change that answer: the account refuses every later admission instead
// of leaving a gap in the durable record unreported. A nil account pays the
// barrier inline, which is what every committing step and every outcome_unknown
// keeps.
type deferredDurability struct {
	mutex   sync.Mutex
	failure error
	pending sync.WaitGroup
}

// settle pays the barrier now, or hands it to a goroutine that reports a failure
// through the account. The label names the record in the one log line the first
// failure writes; later failures are the same outage seen again.
func (latch *deferredDurability) settle(label string, wait func() error) error {
	if latch == nil {
		return wait()
	}
	latch.pending.Add(1)
	go func() {
		defer latch.pending.Done()
		if err := wait(); err != nil {
			latch.fail(label, err)
		}
	}()
	return nil
}

func (latch *deferredDurability) fail(label string, failure error) {
	latch.mutex.Lock()
	first := latch.failure == nil
	if first {
		latch.failure = failure
	}
	latch.mutex.Unlock()
	if first {
		log.Printf("ERROR deferred durability failed: %s failure=%v", label, failure)
	}
}

// refusal reports the first barrier that failed after its reply, nil while every
// deferred record is still one the durable log will hold.
func (latch *deferredDurability) refusal() error {
	if latch == nil {
		return nil
	}
	latch.mutex.Lock()
	defer latch.mutex.Unlock()
	return latch.failure
}

// wait blocks until every barrier handed to the account has returned. Tests use
// it to inspect what the deferred writes left behind; admission never waits here.
func (latch *deferredDurability) wait() {
	if latch == nil {
		return
	}
	latch.pending.Wait()
}
