package platform

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	controller "peri-loom/controller"
)

// A close_session is the platform's own retirement command for a session it
// already registered: it carries no SQL, it never replays an operation, and the
// worker validates the session identity before it acts. Isolation therefore
// does not refuse it, and a failed attempt may be retried — but the retry has to
// be bounded, or an unreachable worker would grow the durable operation log
// forever. These bounds are the account that keeps that growth finite.
const (
	sessionCleanupMaxAttempts = 12
	sessionCleanupDeadline    = 10 * time.Minute
	sessionCleanupMaxBackoff  = 60 * time.Second
)

var errSessionCleanupAdmission = errors.New("session cleanup admission unavailable")
var errSessionCleanupPending = errors.New("session cleanup is waiting for its retry window")
var errSessionCleanupExhausted = errors.New("session cleanup retry budget exhausted; operator retirement required")

// sessionCleanup is one session's retirement account, owned by the session
// because the session is the cleanup identity: database, worker and upstream
// identifier are fixed at open, so the frozen close command is a function of the
// same fields on every attempt.
type sessionCleanup struct {
	attempts    int
	attemptedAt time.Time
	nextAttempt time.Time
	lastFailure string
}

// sessionCleanupGate decides whether this session may attempt another close
// now. Every caller applies this one policy instead of inventing its own: nil
// means attempt, pending means the retry window has not elapsed, and exhausted
// means only the operator retirement path can finish the session.
func sessionCleanupGate(session *gatewaySession, now time.Time) error {
	if cleanupExhausted(session.cleanup, now) {
		return errSessionCleanupExhausted
	}
	if !session.cleanup.nextAttempt.IsZero() && now.Before(session.cleanup.nextAttempt) {
		return errSessionCleanupPending
	}
	return nil
}

// cleanupExhausted is the spent-account rule of the gate, on an account the
// caller has already copied under the session lock: the reporting path reads an
// account it must not keep the session locked for.
func cleanupExhausted(account sessionCleanup, now time.Time) bool {
	return !account.attemptedAt.IsZero() && (account.attempts >= sessionCleanupMaxAttempts || now.Sub(account.attemptedAt) >= sessionCleanupDeadline)
}

// noteCleanupDue starts the account at the first moment cleanup was owed, so a
// session that never reaches the worker still has a bounded, visible age.
func noteCleanupDue(session *gatewaySession, now time.Time) {
	if session.cleanup.attemptedAt.IsZero() {
		session.cleanup.attemptedAt = now
	}
}

// noteCleanupAttempt advances the account for one attempt that reaches a worker
// call. A local refusal dispatches nothing, so it must not spend the budget of a
// remediation that has not been tried yet; the caller's own cadence keeps that
// case from becoming a busy loop.
func noteCleanupAttempt(session *gatewaySession, now time.Time) {
	account := &session.cleanup
	account.attempts++
	backoff := time.Duration(1<<min(account.attempts, 5)) * time.Second
	account.nextAttempt = now.Add(min(backoff, sessionCleanupMaxBackoff))
}

// noteCleanupFailure keeps the reason of the last failed attempt in the account,
// as a code rather than worker text: the cleanup ledger is a metrics surface.
func noteCleanupFailure(session *gatewaySession, failure error) {
	session.cleanup.lastFailure = cleanupFailureCode(failure)
}

// noteUnowned records the moment a session stopped being addressable by its
// owner. Only an unproven outcome does that: the owner's next dispatch is
// refused, so the session survives on the platform's cleanup or an operator.
func noteUnowned(session *gatewaySession, now time.Time) {
	if session.unownedAt.IsZero() {
		session.unownedAt = now
	}
}

// retireSession performs one retirement attempt for a registered session the
// caller has locked and found due: it resolves the quota, reserves the control
// lane the close needs, dispatches the frozen close_session command under its
// own durable intent, and settles it. The idle sweepers share this path so one
// session keeps one cleanup identity and one retry account.
func (gateway *Gateway) retireSession(ctx context.Context, session *gatewaySession, now time.Time) error {
	noteCleanupDue(session, now)
	quota, err := sessionCleanupQuota(gateway.config.State, session)
	if err != nil {
		noteCleanupFailure(session, err)
		return err
	}
	permit, ok := gateway.admission.reserve(session.project, quota, false, true)
	if !ok {
		noteCleanupFailure(session, errSessionCleanupAdmission)
		return errSessionCleanupAdmission
	}
	defer permit.finish()
	requestID, err := controller.RandomID()
	if err != nil {
		noteCleanupFailure(session, err)
		return err
	}
	noteCleanupAttempt(session, now)
	if err = gateway.closeTrackedSession(ctx, session, requestID, permit); err != nil {
		var failure *SQLRPCError
		if !errors.As(err, &failure) || failure.Ambiguous {
			session.poisoned = true
			noteUnowned(session, now)
			permit.retain()
		}
		noteCleanupFailure(session, err)
		logCleanupFailure(session, err, now)
		return err
	}
	gateway.removeSession(session)
	return nil
}

// logCleanupFailure keeps a retirement the platform still owes visible in the
// process log, not only in the cleanup ledger: the retry account bounds the
// attempts, so this is a bounded number of lines, and the line that reports the
// spent account is the operator's notice that only the retirement path can
// finish this session. The caller holds the session lock.
func logCleanupFailure(session *gatewaySession, failure error, now time.Time) {
	log.Printf("session cleanup failed: database=%s session=%s attempts=%d failure=%s exhausted=%t",
		session.database.Key(), session.public, session.cleanup.attempts, cleanupFailureCode(failure), cleanupExhausted(session.cleanup, now))
}

// cleanupFailureCode names one failed retirement attempt for the ledger and the
// logs, without copying worker text onto a metrics surface.
func cleanupFailureCode(failure error) string {
	var rpc *SQLRPCError
	switch {
	case failure == nil:
		return ""
	case errors.Is(failure, ErrDatabaseQuarantined):
		return "database_isolated"
	case errors.Is(failure, errSessionProjectUnavailable):
		return "session_project_unavailable"
	case errors.Is(failure, errSessionCleanupAdmission):
		return "cleanup_admission_unavailable"
	case errors.Is(failure, errSessionCleanupExhausted):
		return "cleanup_retry_exhausted"
	case errors.Is(failure, context.DeadlineExceeded), errors.Is(failure, context.Canceled):
		return "cleanup_timeout"
	case errors.As(failure, &rpc) && rpc.Ambiguous:
		return "outcome_unknown"
	case errors.As(failure, &rpc):
		return "upstream_unavailable"
	default:
		return "upstream_failure"
	}
}

// cleanupLedger is the in-process age account for cleanup facts whose durable
// record carries no timestamp: the operation record format is frozen, so an age
// here means how long this process has watched the fact stay unresolved, and a
// restart restarts the clock. Entries are pruned to the facts that are still
// unresolved, so the account cannot become the invisible growth it exists to
// expose.
type cleanupLedger struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// observe reconciles the live set with the account: a fact watched for the first
// time starts its age now, and a fact that is gone drops its entry, which is
// also what keeps the map bounded by the facts that still exist.
func (ledger *cleanupLedger) observe(live map[string]bool, now time.Time) (int, int64) {
	if ledger == nil {
		return len(live), 0
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.seen == nil {
		ledger.seen = map[string]time.Time{}
	}
	oldest := time.Time{}
	for key := range live {
		since, exists := ledger.seen[key]
		if !exists {
			since = now
			ledger.seen[key] = since
		}
		if oldest.IsZero() || since.Before(oldest) {
			oldest = since
		}
	}
	for key := range ledger.seen {
		if !live[key] {
			delete(ledger.seen, key)
		}
	}
	return len(live), ageMilliseconds(now, oldest)
}

// noteIsolation seeds one unresolved isolation record the moment the platform
// creates it, so its age does not wait for the first scrape.
func (ledger *cleanupLedger) noteIsolation(key string, now time.Time) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.seen == nil {
		ledger.seen = map[string]time.Time{}
	}
	if _, exists := ledger.seen[key]; !exists {
		ledger.seen[key] = now
	}
}

// cleanupLedger reports the accounts the exit condition of the lifecycle batch
// needs: what is still owed, what has no owner, and what keeps a namespace
// isolated, each with a count and an age.
func (gateway *Gateway) cleanupLedger() map[string]any {
	now := time.Now()
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		// An unreadable account must not look like a complete one: a count of
		// minus one is the operator's signal that nothing was observed.
		unreadable := map[string]any{"count": -1, "oldest_age_ms": int64(0), "state_read": "unavailable"}
		pending, orphan := gateway.sessionCleanupLedger(now)
		return map[string]any{"pending_close": pending, "orphan_sessions": orphan, "isolated_records": unreadable}
	}
	live := map[string]bool{}
	for key, record := range state.Operations {
		// A released quarantine no longer blocks admission, so it is not an open
		// isolation; the record itself stays for history.
		if record.State == "quarantined" && record.ReleaseRequestID == "" {
			live[key] = true
		}
	}
	count, oldest := gateway.cleanup.observe(live, now)
	pending, orphan := gateway.sessionCleanupLedger(now)
	return map[string]any{
		"pending_close":    pending,
		"orphan_sessions":  orphan,
		"isolated_records": map[string]any{"count": count, "oldest_age_ms": oldest, "state_read": "ok"},
	}
}

// sessionCleanupLedger counts the sessions the platform still holds and cannot
// account for. A session whose retirement was attempted and not proven is a
// pending close; a session its owner can no longer address is an orphan, because
// only the platform's cleanup or an operator can finish it. A locked session is
// skipped the way the sweepers skip it: reporting must not queue behind the
// dispatch it would describe.
func (gateway *Gateway) sessionCleanupLedger(now time.Time) (map[string]any, map[string]any) {
	gateway.mu.Lock()
	sessions := make([]*gatewaySession, 0, len(gateway.sessions))
	for _, session := range gateway.sessions {
		sessions = append(sessions, session)
	}
	gateway.mu.Unlock()
	var pendingCount, orphanCount, exhausted int
	oldestPending, oldestOrphan := time.Time{}, time.Time{}
	lastFailure := ""
	for _, session := range sessions {
		if !session.mu.TryLock() {
			continue
		}
		if session.closed {
			session.mu.Unlock()
			continue
		}
		account, unowned := session.cleanup, session.unownedAt
		session.mu.Unlock()
		if !account.attemptedAt.IsZero() {
			pendingCount++
			if oldestPending.IsZero() || account.attemptedAt.Before(oldestPending) {
				oldestPending, lastFailure = account.attemptedAt, account.lastFailure
			}
			if cleanupExhausted(account, now) {
				exhausted++
			}
		}
		if !unowned.IsZero() {
			orphanCount++
			if oldestOrphan.IsZero() || unowned.Before(oldestOrphan) {
				oldestOrphan = unowned
			}
		}
	}
	pending := map[string]any{"count": pendingCount, "oldest_age_ms": ageMilliseconds(now, oldestPending), "last_failure": lastFailure, "exhausted": exhausted}
	orphan := map[string]any{"count": orphanCount, "oldest_age_ms": ageMilliseconds(now, oldestOrphan)}
	return pending, orphan
}

func ageMilliseconds(now, since time.Time) int64 {
	if since.IsZero() || !since.Before(now) {
		return 0
	}
	return now.Sub(since).Milliseconds()
}
