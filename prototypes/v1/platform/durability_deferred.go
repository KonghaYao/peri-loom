package platform

import (
	"log"
	"sync"
)

const (
	// deferredDurabilityMaximum bounds the records whose barrier is paid after
	// their reply. A saturated account waits inline, so the bound is a latency
	// limit, never a refusal: the reply keeps the barrier in front of it exactly
	// as it did before the deferral existed.
	deferredDurabilityMaximum = 256
	deferredDurabilityWorkers = 8
)

type deferredWait struct {
	label string
	wait  func() error
}

// deferredDurability is the fail-closed account of completion barriers that a
// non-committing operation pays after its reply. The answer that owned such a
// barrier is already written when the barrier returns, so its failure can no
// longer change that answer: the account refuses every later admission instead
// of leaving a gap in the durable record unreported.
//
// The account is a fixed worker pool, not one goroutine per record: under
// concurrency the per-record goroutines were the only unbounded resource in the
// deferred path, and scheduler churn is the one cost a barrier deferral is not
// allowed to add. A nil account pays every barrier inline, which is what every
// committing step and every outcome_unknown keeps.
type deferredDurability struct {
	mutex   sync.Mutex
	failure error
	pending int
	closed  bool
	maximum int
	workers int
	running int
	jobs    chan deferredWait
	group   sync.WaitGroup
	done    sync.WaitGroup
}

func newDeferredDurability() *deferredDurability {
	return newDeferredDurabilityOf(deferredDurabilityMaximum, deferredDurabilityWorkers)
}

func newDeferredDurabilityOf(maximum, workers int) *deferredDurability {
	if maximum < 1 {
		maximum = 1
	}
	if workers < 1 {
		workers = 1
	}
	return &deferredDurability{maximum: maximum, workers: workers, jobs: make(chan deferredWait, maximum)}
}

// settle pays the barrier now, or hands it to the worker pool that reports a
// failure through the account. A full pool, a stopped account and a nil account
// all pay it inline: the barrier becomes latency here, which is the backpressure
// a reply must not turn into a refusal. The label names the record in the one
// log line the first failure writes; later failures are the same outage seen
// again.
func (latch *deferredDurability) settle(label string, wait func() error) error {
	if latch == nil {
		return wait()
	}
	latch.mutex.Lock()
	if latch.closed || latch.pending >= latch.maximum {
		latch.mutex.Unlock()
		return wait()
	}
	latch.pending++
	latch.done.Add(1)
	latch.startLocked()
	select {
	case latch.jobs <- deferredWait{label: label, wait: wait}:
		latch.mutex.Unlock()
		return nil
	default:
		// The pending count proves a free slot, so this is only reachable if the
		// bookkeeping and the channel ever disagree; waiting inline is the safe
		// answer either way.
		latch.pending--
		latch.done.Done()
		latch.mutex.Unlock()
		return wait()
	}
}

func (latch *deferredDurability) startLocked() {
	if latch.running > 0 || latch.closed {
		return
	}
	latch.running = latch.workers
	latch.group.Add(latch.workers)
	for index := 0; index < latch.workers; index++ {
		go latch.run()
	}
}

func (latch *deferredDurability) run() {
	defer latch.group.Done()
	for job := range latch.jobs {
		if err := job.wait(); err != nil {
			latch.fail(job.label, err)
		}
		latch.mutex.Lock()
		latch.pending--
		latch.mutex.Unlock()
		latch.done.Done()
	}
}

// stop drains the queued barriers and retires the pool. A settle that arrives
// after it waits inline, so shutdown needs no second signal. The caller closes
// the audit and state stores afterwards: the queued barriers then return their
// own result instead of waiting on a writer that stopped accepting.
func (latch *deferredDurability) stop() {
	if latch == nil {
		return
	}
	latch.mutex.Lock()
	if !latch.closed {
		latch.closed = true
		close(latch.jobs)
	}
	latch.mutex.Unlock()
	latch.group.Wait()
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
	latch.done.Wait()
}
