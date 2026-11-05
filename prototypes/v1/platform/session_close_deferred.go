package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	controller "peri-loom/controller"
)

const (
	// deferredSessionClosePerDatabaseMaximum bounds the worker closes one
	// database leaves outstanding. The worker holds 16 sessions per database, so
	// a bound of 4 keeps the closing set well inside that ceiling while still
	// absorbing the close that follows each pipeline in an 8-way burst. A
	// saturated bound becomes reply latency, never a refusal.
	deferredSessionClosePerDatabaseMaximum = 4

	// deferredSessionCloseMaximum matches the completion account: one session
	// close per committed reply, bounded far above any admission limit the
	// gateway enforces.
	deferredSessionCloseMaximum = 256

	deferredSessionCloseWorkers = 8

	// deferredSessionCloseTimeout bounds one attempt. A post-reply close is not
	// allowed to occupy a worker slot for the life of the process, so a stuck
	// worker is retried from the cleanup account instead.
	deferredSessionCloseTimeout = 30 * time.Second
)

var errDeferredSessionCloseNotProven = errors.New("worker did not prove the session close")
var errDeferredSessionCloseStopped = errors.New("deferred session close stopped before its retry window")

// deferredSessionCloseJob is one worker close that was queued after its reply.
// The session is kept because its cleanup account is the retry budget; attempt
// performs one worker call and returns the error that decides the next retry.
type deferredSessionCloseJob struct {
	database string
	session  *gatewaySession
	attempt  func() error
}

// deferredSessionClose is the account of worker close_session calls dispatched
// after the reply. Only a session whose whole life is a proven read-only
// autocommit one is handed here; every other close keeps the synchronous path.
//
// The account is a fixed worker pool with a per-database bound. A full pool, a
// stopped account and a nil account all close inline: saturation changes when
// the worker wait is paid, never whether the client is answered. A failed
// attempt is retried through the session's cleanup ledger, so an unreachable
// worker cannot grow the account without bound.
type deferredSessionClose struct {
	mutex              sync.Mutex
	closed             bool
	failure            error
	pending            int
	databases          map[string]int
	waiters            map[string]chan struct{}
	maximum            int
	perDatabaseMaximum int
	workerCount        int
	jobs               chan deferredSessionCloseJob
	stopCh             chan struct{}
	workers            sync.WaitGroup
	running            int
	done               sync.WaitGroup
	enqueued           atomic.Int64
	inline             atomic.Int64
	failed             atomic.Int64
	retried            atomic.Int64
	unresolved         atomic.Int64
	flushed            atomic.Int64
	peak               atomic.Int64
}

func newDeferredSessionClose() *deferredSessionClose {
	return newDeferredSessionCloseOf(deferredSessionCloseMaximum, deferredSessionClosePerDatabaseMaximum, deferredSessionCloseWorkers)
}

func newDeferredSessionCloseOf(maximum, perDatabase, workers int) *deferredSessionClose {
	if maximum < 1 {
		maximum = 1
	}
	if perDatabase < 1 {
		perDatabase = 1
	}
	if workers < 1 {
		workers = 1
	}
	return &deferredSessionClose{
		maximum:            maximum,
		perDatabaseMaximum: perDatabase,
		workerCount:        workers,
		jobs:               make(chan deferredSessionCloseJob, maximum),
		stopCh:             make(chan struct{}),
		databases:          map[string]int{},
		waiters:            map[string]chan struct{}{},
	}
}

// enqueue hands one worker close to the account and reports whether the account
// took it. A refusal is the caller's signal to pay the close inline before the
// reply: the bound is a latency limit, never an error the client can see.
func (latch *deferredSessionClose) enqueue(session *gatewaySession, attempt func() error) bool {
	if latch == nil {
		return false
	}
	key := session.database.Key()
	latch.mutex.Lock()
	if latch.closed || latch.pending >= latch.maximum || latch.databases[key] >= latch.perDatabaseMaximum {
		latch.mutex.Unlock()
		latch.inline.Add(1)
		return false
	}
	latch.pending++
	latch.databases[key]++
	if latch.databases[key] == 1 {
		latch.waiters[key] = make(chan struct{})
	}
	latch.observePeakLocked()
	latch.enqueued.Add(1)
	latch.done.Add(1)
	latch.startLocked()
	job := deferredSessionCloseJob{database: key, session: session, attempt: attempt}
	select {
	case latch.jobs <- job:
		latch.mutex.Unlock()
		return true
	default:
		// The pending count proves a free slot, so this is only reachable if the
		// bookkeeping and the channel ever disagree; paying inline is the safe
		// answer either way.
		latch.pending--
		latch.databases[key]--
		if latch.databases[key] == 0 {
			close(latch.waiters[key])
			delete(latch.waiters, key)
		}
		latch.done.Done()
		latch.mutex.Unlock()
		latch.inline.Add(1)
		return false
	}
}

// flush waits until the closes this database still owes have returned, bounded
// by the caller's context. The capacity refusal it answers proved the open never
// ran, so freeing the sessions the account is still retiring is the only thing
// that can make the one retry succeed.
func (latch *deferredSessionClose) flush(ctx context.Context, database string) {
	if latch == nil {
		return
	}
	latch.mutex.Lock()
	waiter := latch.waiters[database]
	pending := latch.databases[database]
	latch.mutex.Unlock()
	if waiter == nil || pending == 0 {
		return
	}
	latch.flushed.Add(1)
	select {
	case <-waiter:
	case <-ctx.Done():
	}
}

func (latch *deferredSessionClose) observePeakLocked() {
	pending := int64(latch.pending)
	for {
		seen := latch.peak.Load()
		if pending <= seen || latch.peak.CompareAndSwap(seen, pending) {
			return
		}
	}
}

func (latch *deferredSessionClose) startLocked() {
	if latch.running > 0 || latch.closed {
		return
	}
	latch.running = latch.workerCount
	latch.workers.Add(latch.workerCount)
	for index := 0; index < latch.workerCount; index++ {
		go latch.run()
	}
}

func (latch *deferredSessionClose) run() {
	defer latch.workers.Done()
	for job := range latch.jobs {
		latch.execute(job)
		latch.finish(job)
	}
}

func (latch *deferredSessionClose) execute(job deferredSessionCloseJob) {
	for {
		now := time.Now()
		noteCleanupDue(job.session, now)
		if cleanupExhausted(job.session.cleanup, now) {
			latch.abandon(job, errSessionCleanupExhausted)
			return
		}
		if wait := job.session.cleanup.nextAttempt.Sub(now); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-latch.stopCh:
				timer.Stop()
				latch.abandon(job, errDeferredSessionCloseStopped)
				return
			case <-timer.C:
			}
		}
		noteCleanupAttempt(job.session, time.Now())
		err := job.attempt()
		if err == nil {
			return
		}
		latch.failed.Add(1)
		noteCleanupFailure(job.session, err)
		if job.session.cleanup.attempts == 1 {
			log.Printf("deferred session close failed; retrying: database=%s session=%s failure=%s", job.session.database.Key(), job.session.public, cleanupFailureCode(err))
		}
		if cleanupExhausted(job.session.cleanup, time.Now()) {
			latch.abandon(job, err)
			return
		}
		latch.retried.Add(1)
	}
}

func (latch *deferredSessionClose) finish(job deferredSessionCloseJob) {
	latch.mutex.Lock()
	latch.pending--
	latch.databases[job.database]--
	if latch.databases[job.database] == 0 {
		if waiter := latch.waiters[job.database]; waiter != nil {
			close(waiter)
			delete(latch.waiters, job.database)
		}
	}
	latch.mutex.Unlock()
	latch.done.Done()
}

// abandon records a close the account can no longer finish. The answer that
// owned it is already written, so the only honest report is the log line and
// the counter; a clean shutdown turns the count into its error.
func (latch *deferredSessionClose) abandon(job deferredSessionCloseJob, failure error) {
	latch.mutex.Lock()
	if latch.failure == nil {
		latch.failure = failure
	}
	latch.mutex.Unlock()
	latch.unresolved.Add(1)
	log.Printf("ERROR deferred session close unresolved: database=%s session=%s attempts=%d failure=%s", job.session.database.Key(), job.session.public, job.session.cleanup.attempts, cleanupFailureCode(failure))
}

// stop retires the pool after draining every queued close. A close that cannot
// be finished stops at its retry window instead of holding shutdown for the
// whole cleanup budget; the unresolved count is what makes that visible.
func (latch *deferredSessionClose) stop() error {
	if latch == nil {
		return nil
	}
	latch.mutex.Lock()
	if !latch.closed {
		latch.closed = true
		close(latch.stopCh)
		close(latch.jobs)
	}
	latch.mutex.Unlock()
	latch.workers.Wait()
	latch.done.Wait()
	latch.mutex.Lock()
	pending, failure := latch.pending, latch.failure
	latch.mutex.Unlock()
	unresolved := latch.unresolved.Load()
	if unresolved > 0 {
		return fmt.Errorf("deferred session close unresolved: %d sessions: %w", unresolved, failure)
	}
	if pending != 0 {
		return fmt.Errorf("deferred session close left %d pending", pending)
	}
	return nil
}

// wait blocks until every close handed to the account has finished. Tests use it
// to inspect the worker calls a deferred close left behind; admission never
// waits here.
func (latch *deferredSessionClose) wait() {
	if latch == nil {
		return
	}
	latch.done.Wait()
}

// Status reports the deferral account's backlog. inline_total is the direct
// measure of how often the bound became client latency; failed_total counts
// failed worker attempts, while unresolved_total counts sessions the cleanup
// budget could not finish.
func (latch *deferredSessionClose) Status() map[string]any {
	if latch == nil {
		return nil
	}
	latch.mutex.Lock()
	pending, maximum, perDatabase := latch.pending, latch.maximum, latch.perDatabaseMaximum
	latch.mutex.Unlock()
	return map[string]any{
		"pending":              pending,
		"pending_max":          latch.peak.Load(),
		"maximum":              maximum,
		"per_database_maximum": perDatabase,
		"enqueued_total":       latch.enqueued.Load(),
		"inline_total":         latch.inline.Load(),
		"failed_total":         latch.failed.Load(),
		"retried_total":        latch.retried.Load(),
		"unresolved_total":     latch.unresolved.Load(),
		"flushed_total":        latch.flushed.Load(),
		"semantics":            "read-only stateless session worker closes dispatched after the reply; a session beyond the bound closes inline before the reply, a failed close retries through the cleanup ledger",
	}
}

// deferredSessionCloseEnabled reads the kill switch. Both unset and "0" keep
// today's synchronous close byte for byte: no account, no goroutines, no
// counters.
func deferredSessionCloseEnabled() (bool, error) {
	switch strings.TrimSpace(os.Getenv("PERI_DEFERRED_SESSION_CLOSE")) {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, errors.New("PERI_DEFERRED_SESSION_CLOSE must be 0 or 1")
	}
}

// deferredCloseEligible reports whether this session's worker close may be
// dispatched after the reply. The proof is conservative: the session never ran
// anything that could commit or hold a transaction/cursor, and its last
// statement (if any) said autocommit=true. A session that never ran SQL is
// stateless by definition.
func (session *gatewaySession) deferredCloseEligible() bool {
	if session == nil || !session.pristine || session.poisoned || session.writerProtocol {
		return false
	}
	if session.readCursorOpen || session.writeCursorOpen {
		return false
	}
	return !session.statementAutocommitKnown || session.statementAutocommit
}

// noteStatementAutocommit records the authoritative autocommit flag of a
// statement result. A missing flag is not evidence and keeps the session on the
// synchronous close path.
func (session *gatewaySession) noteStatementAutocommit(value, known bool) {
	if !known {
		session.pristine = false
		return
	}
	session.statementAutocommitKnown = true
	session.statementAutocommit = value
	if !value {
		session.pristine = false
	}
}

// sessionCapacityRefusal reports whether a worker reply proves an open was
// refused for session capacity before it created a session. The worker wraps
// open_session with not_executed evidence (worker_rpc::dispatch), so this is a
// refusal, not an unknown outcome: retrying the open is legal and is not a SQL
// replay. Any other failure keeps the ordinary answer.
func sessionCapacityRefusal(err error) bool {
	var failure *SQLRPCError
	if !errors.As(err, &failure) || failure.Ambiguous || !failure.evidence.Rejected() {
		return false
	}
	return strings.Contains(failure.workerFailure, "database session capacity exceeded") ||
		strings.Contains(failure.workerFailure, "worker session capacity exceeded")
}

// callOpenSession sends the open command and, when the worker proves it refused
// open_session for session capacity before creating a session, flushes the
// closes this database still owes and asks exactly once more. Only this command
// and this provably unexecuted refusal qualify: a statement is never replayed,
// and a refusal that proves nothing keeps the ordinary failure path.
func (gateway *Gateway) callOpenSession(ctx context.Context, database controller.DatabaseID, worker controller.Worker, requestID string, command frozenCommand, reply any) error {
	err := gateway.config.Upstream.Call(ctx, worker, requestID, command, reply)
	if err == nil || gateway.deferredClose == nil || command.operationName() != "open_session" || !sessionCapacityRefusal(err) {
		return err
	}
	gateway.deferredClose.flush(ctx, database.Key())
	return gateway.config.Upstream.Call(ctx, worker, requestID, command, reply)
}

func (gateway *Gateway) deferredSessionCloseAttemptTimeout() time.Duration {
	timeout := deferredSessionCloseTimeout
	if gateway.config.RequestTimeout > 0 && gateway.config.RequestTimeout < timeout {
		timeout = gateway.config.RequestTimeout
	}
	return timeout
}
