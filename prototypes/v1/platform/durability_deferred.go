package platform

import (
	"log"
	"sync"
	"sync/atomic"
)

const (
	// deferredDurabilityMaximum bounds the records whose barrier is paid after
	// their reply. A saturated account waits inline, so the bound is a latency
	// limit, never a refusal: the reply keeps the barrier in front of it exactly
	// as it did before the deferral existed.
	//
	// The bound matches the audit queue's own record limit, because that queue is
	// what a deferred barrier waits on: a tighter bound turns a burst the queue
	// still accepts into reply latency. Two records per request fit roughly two
	// thousand concurrent replies, far above any admission limit the gateway
	// enforces, so the inline path stays the guard against a stopped account
	// rather than the normal answer to a busy one.
	deferredDurabilityMaximum = 4096
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
	mutex    sync.Mutex
	failure  error
	pending  int
	closed   bool
	maximum  int
	workers  int
	running  int
	jobs     chan deferredWait
	group    sync.WaitGroup
	done     sync.WaitGroup
	enqueued atomic.Int64
	inline   atomic.Int64
	peak     atomic.Int64
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
		latch.inline.Add(1)
		return wait()
	}
	latch.pending++
	latch.observePeakLocked()
	latch.enqueued.Add(1)
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
		latch.inline.Add(1)
		return wait()
	}
}

// observePeakLocked records how deep the pending set has ever been, so an
// operator can tell a small buffer from an account that is permanently against
// its bound without reading a live gauge that is zero between bursts.
func (latch *deferredDurability) observePeakLocked() {
	pending := int64(latch.pending)
	for {
		seen := latch.peak.Load()
		if pending <= seen || latch.peak.CompareAndSwap(seen, pending) {
			return
		}
	}
}

// Status reports the deferral account's backlog. A record that the account has
// to pay inline delays the reply it belongs to, so the inline total is the
// direct measure of how often a saturated account became client latency.
func (latch *deferredDurability) Status() map[string]any {
	if latch == nil {
		return nil
	}
	latch.mutex.Lock()
	pending, maximum := latch.pending, latch.maximum
	latch.mutex.Unlock()
	return map[string]any{
		"pending":        pending,
		"pending_max":    latch.peak.Load(),
		"maximum":        maximum,
		"enqueued_total": latch.enqueued.Load(),
		"inline_total":   latch.inline.Load(),
		"semantics":      "completion barriers paid after the reply; a record beyond the bound pays its barrier inline before the reply is flushed",
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
