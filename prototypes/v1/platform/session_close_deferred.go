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
	// database leaves outstanding. It is not a capacity guarantee: the gateway
	// returns a session's seat as soon as the reply is written, while the worker
	// keeps that session until the close lands, so a database held at the
	// gateway's MaxSessions can have MaxSessions+4 sessions in the worker. The
	// worker refuses past its own per-database ceiling, and both open paths
	// (open_session and the fused open_guarded_statement) answer that provably
	// unexecuted refusal by flushing this database's pending closes and asking
	// once more. A saturated bound becomes reply latency, never a refusal.
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
	// attempt takes the context its worker call is derived from, so a shutdown
	// that spends its budget cancels a call that already started instead of
	// waiting for the attempt's own timeout.
	attempt func(context.Context) error
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
	// lifetime is the parent of every worker attempt. stop ties it to the
	// shutdown budget, so an attempt already in flight is cancelled when the
	// budget expires rather than holding the process for its own timeout.
	lifetime context.Context
	expire   context.CancelFunc
	// stopCtx is the shutdown budget handed to stop. A queued close consults it
	// before each attempt so the queue stops paying attempts once the budget is
	// gone; before stop it is nil and the account still owns its retries.
	stopCtx    context.Context
	workers    sync.WaitGroup
	running    int
	done       sync.WaitGroup
	enqueued   atomic.Int64
	inline     atomic.Int64
	failed     atomic.Int64
	retried    atomic.Int64
	unresolved atomic.Int64
	flushed    atomic.Int64
	peak       atomic.Int64
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
	lifetime, expire := context.WithCancel(context.Background())
	return &deferredSessionClose{
		maximum:            maximum,
		perDatabaseMaximum: perDatabase,
		workerCount:        workers,
		jobs:               make(chan deferredSessionCloseJob, maximum),
		stopCh:             make(chan struct{}),
		lifetime:           lifetime,
		expire:             expire,
		databases:          map[string]int{},
		waiters:            map[string]chan struct{}{},
	}
}

// enqueue hands one worker close to the account and reports whether the account
// took it. A refusal is the caller's signal to pay the close inline before the
// reply: the bound is a latency limit, never an error the client can see.
func (latch *deferredSessionClose) enqueue(session *gatewaySession, attempt func(context.Context) error) bool {
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
		// A close still queued when the shutdown budget is spent must not open
		// another worker attempt: the stop is already reported incomplete, and
		// every such attempt would hold the process past its deadline.
		if err := latch.budgetError(); err != nil {
			latch.abandon(job, err)
			return
		}
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
			case <-latch.lifetime.Done():
				// The shutdown budget ended while this close was backing off, so
				// its retry window is gone with it.
				timer.Stop()
				failure := latch.budgetError()
				if failure == nil {
					failure = errDeferredSessionCloseStopped
				}
				latch.abandon(job, failure)
				return
			case <-timer.C:
			}
		}
		noteCleanupAttempt(job.session, time.Now())
		err := job.attempt(latch.attemptParent())
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

// attemptParent is the context one worker attempt is derived from. After the
// shutdown began it is the shutdown budget itself, so the attempt inherits its
// deadline; an attempt that started before the shutdown runs on the account's
// lifetime, which stop cancels when that budget expires.
func (latch *deferredSessionClose) attemptParent() context.Context {
	latch.mutex.Lock()
	defer latch.mutex.Unlock()
	if latch.stopCtx != nil {
		return latch.stopCtx
	}
	return latch.lifetime
}

// budgetError names why a queued close may no longer start an attempt. It is
// nil while the account is running, nil during a graceful stop that still holds
// budget, and the shutdown context's own error once that budget is spent.
func (latch *deferredSessionClose) budgetError() error {
	latch.mutex.Lock()
	defer latch.mutex.Unlock()
	if !latch.closed {
		return nil
	}
	if latch.stopCtx == nil {
		return errDeferredSessionCloseStopped
	}
	return latch.stopCtx.Err()
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

// stop retires the pool under the caller's shutdown budget. Queued closes keep
// draining while the budget holds; once it expires the account abandons them
// without another worker attempt, and an attempt already in flight is cancelled
// through the lifetime it was derived from. The returned error reports what the
// budget could not finish, so Gateway.Close can refuse a clean terminal.
func (latch *deferredSessionClose) stop(ctx context.Context) error {
	if latch == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("invalid deferred session close shutdown context")
	}
	latch.mutex.Lock()
	if !latch.closed {
		latch.closed = true
		latch.stopCtx = ctx
		// The budget can expire while an attempt is already on the wire, so the
		// cancellation has to reach that attempt too, not only the queue.
		context.AfterFunc(ctx, latch.expire)
		close(latch.stopCh)
		close(latch.jobs)
	}
	latch.mutex.Unlock()
	drained := make(chan struct{})
	go func() {
		latch.workers.Wait()
		latch.done.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-ctx.Done():
		// Waiting here would let each queued close spend its own attempt timeout
		// past the shutdown deadline, which is what makes the teardown miss the
		// audit tail and the operation checkpoint. Report the unfinished work
		// instead; the workers abandon the queue on their own.
	}
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
// both open_session and the fused open_guarded_statement's session creation with
// not_executed evidence (worker_rpc::dispatch), so this is a refusal, not an
// unknown outcome: retrying the open is legal and is not a SQL replay. Any other
// failure keeps the ordinary answer.
func sessionCapacityRefusal(err error) bool {
	var failure *SQLRPCError
	if !errors.As(err, &failure) || failure.Ambiguous || !failure.evidence.Rejected() {
		return false
	}
	return strings.Contains(failure.workerFailure, "database session capacity exceeded") ||
		strings.Contains(failure.workerFailure, "worker session capacity exceeded")
}

// openCapacityRetryable reports whether this open command's capacity refusal
// may be retried after the pending closes are flushed. The worker wraps both the
// plain open_session and the fused open_guarded_statement's session creation
// with not_executed evidence (worker_rpc::dispatch), so the refusal proves no
// session exists. The fused statement only runs after that open succeeds, so a
// retry creates a session; it never replays SQL.
func openCapacityRetryable(command frozenCommand) bool {
	switch command.operationName() {
	case "open_session", "open_guarded_statement":
		return true
	default:
		return false
	}
}

// callOpenSession sends the open command and, when the worker proves it refused
// the session creation for capacity before any session existed, flushes the
// closes this database still owes and asks exactly once more. Both open commands
// qualify, because the worker wraps both their session creations with
// not_executed evidence; every other command and every refusal that proves
// nothing keeps the ordinary failure path, so a statement is never replayed.
func (gateway *Gateway) callOpenSession(ctx context.Context, database controller.DatabaseID, worker controller.Worker, requestID string, command frozenCommand, reply any) error {
	err := gateway.config.Upstream.Call(ctx, worker, requestID, command, reply)
	if err == nil || gateway.deferredClose == nil || !openCapacityRetryable(command) || !sessionCapacityRefusal(err) {
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
