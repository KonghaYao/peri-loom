package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"

	controller "peri-loom/controller"
)

// A settlement append that meets a full operation journal was refused before
// anything was written: ErrOperationQueueFull is the one submit refusal that
// changes no durable state. The worker call that produced the outcome has already
// happened and must never be repeated, but the record that makes it durable can
// simply be queued again once the committer drained a batch.
//
// The budget is short on purpose. The queue drains at disk speed, so a request
// held longer than this is better answered as an unknown outcome than kept waiting
// behind a stalled device; 300ms is roughly a hundred barriers on the slowest disk
// the platform targets and stays well below any client timeout.
const (
	operationSettleRetryBudget  = 300 * time.Millisecond
	operationSettleRetryInitial = 250 * time.Microsecond
	operationSettleRetryMaximum = 20 * time.Millisecond
)

// settleRefusal names why a settlement record could not be written. The names are
// a closed vocabulary published in /v1/metrics; they never carry error text.
type settleRefusal int

const (
	settleRefusedQueueFull settleRefusal = iota
	settleRefusedStateClosed
	settleRefusedStateUncertain
	settleRefusedStateFrozen
	settleRefusedOperation
	settleRefusedOther
	settleRefusalCount
)

var settleRefusalNames = [settleRefusalCount]string{"queue_full", "state_closed", "state_uncertain", "state_frozen", "operation_refused", "other"}

func settleRefusalOf(err error) settleRefusal {
	switch {
	case errors.Is(err, ErrOperationQueueFull):
		return settleRefusedQueueFull
	case errors.Is(err, ErrStateClosed):
		return settleRefusedStateClosed
	case errors.Is(err, ErrStateUncertain):
		return settleRefusedStateUncertain
	case errors.Is(err, ErrStateFrozen):
		return settleRefusedStateFrozen
	case errors.Is(err, ErrOperationExists), errors.Is(err, ErrOperationCapacity), errors.Is(err, ErrDatabaseQuarantined):
		return settleRefusedOperation
	}
	return settleRefusedOther
}

// settleTarget is the diagnostic identity of one settlement. It is only read when
// a refusal is logged or counted, so the success path never formats it.
type settleTarget struct {
	action, stage, operation, request, database string
}

// operationSettleAccount is the process-wide account of settlement refusals and
// the bounded retries that recover from a local queue refusal. Its zero value is
// ready; a nil account disables retries and accounting, which is what the
// store-level helpers use when no gateway is involved.
type operationSettleAccount struct {
	retries   atomic.Uint64
	recovered atomic.Uint64
	exhausted atomic.Uint64
	refusals  [settleRefusalCount]atomic.Uint64
	// testRetry runs after a queue refusal released the namespace fence and before
	// the first backoff, so a test can reopen the queue at a known point.
	testRetry func()
}

// settleSubmit queues one settlement edit and hands back its barrier. It is called
// without any lock: every attempt takes the namespace fence itself.
type settleSubmit func(func(*operationEdit) error) (func() error, error)

// submit queues one settlement and retries only a local queue refusal. The fence
// covers exactly one submit: a refusal is answered after the fence is released, so
// no backoff ever runs while holding the namespace fence or the operation state
// lock. That is what keeps a full queue from deadlocking against the committer (or
// against a completion drainer that is holding a batch).
//
// Every attempt re-enters submit, which re-checks the store's closed/poisoned
// state, so a store that failed while the request waited is refused instead of
// retried blindly. The SQL itself is never replayed here: submit only appends the
// record of a call that already returned.
func (account *operationSettleAccount) submit(ctx context.Context, namespace controller.Namespace, caller *durabilityCaller, submit settleSubmit, change func(*operationEdit) error) (func() error, error) {
	unlock := lockObservedOperationNamespace(namespace, caller)
	wait, err := submit(change)
	unlock()
	if !errors.Is(err, ErrOperationQueueFull) || account == nil {
		return wait, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(operationSettleRetryBudget)
	delay := operationSettleRetryInitial
	attempts := 1
	for {
		if account.testRetry != nil {
			account.testRetry()
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			account.exhausted.Add(1)
			return nil, &settleExhaustedError{attempts: attempts, cause: err}
		}
		sleep := min(operationSettleRetryDelay(delay), remaining)
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			account.exhausted.Add(1)
			return nil, &settleExhaustedError{attempts: attempts, cause: fmt.Errorf("%w (context: %v)", err, ctx.Err())}
		case <-timer.C:
		}
		unlock = lockObservedOperationNamespace(namespace, caller)
		wait, err = submit(change)
		unlock()
		attempts++
		account.retries.Add(1)
		if err == nil {
			account.recovered.Add(1)
			return wait, nil
		}
		if !errors.Is(err, ErrOperationQueueFull) {
			return wait, err
		}
		delay = min(delay*2, operationSettleRetryMaximum)
	}
}

// operationSettleRetryDelay spreads one backoff over 50-100% of its slot so a burst
// of refused settlements does not wake in lockstep.
func operationSettleRetryDelay(delay time.Duration) time.Duration {
	slot := int64(delay)
	if slot <= 0 {
		return time.Millisecond
	}
	return time.Duration(slot/2 + rand.Int64N(slot/2+1))
}

// settleExhaustedError reports that the queue stayed full for the whole retry
// budget. It unwraps to ErrOperationQueueFull so callers keep treating the
// settlement as a local refusal, never as an unknown write.
type settleExhaustedError struct {
	attempts int
	cause    error
}

func (err *settleExhaustedError) Error() string {
	return fmt.Sprintf("operation settlement retry budget exhausted after %d attempts: %v", err.attempts, err.cause)
}

func (err *settleExhaustedError) Unwrap() error { return ErrOperationQueueFull }

// recordRefusal counts and logs the terminal refusal of one settlement, at the
// point where the request's answer is rewritten to outcome_unknown. This is the
// only line that says the durable record was never written: the later unknown-outcome
// log knows nothing about why.
func (account *operationSettleAccount) recordRefusal(err error, target settleTarget) {
	if account == nil || err == nil {
		return
	}
	reason := settleRefusalOf(err)
	account.refusals[reason].Add(1)
	log.Printf("operation settlement refused: reason=%s action=%s stage=%s operation=%s request_id=%s database=%s error=%v",
		settleRefusalNames[reason], target.action, target.stage, target.operation, target.request, target.database, err)
}

func (account *operationSettleAccount) Status() map[string]any {
	if account == nil {
		return nil
	}
	refused := map[string]uint64{}
	for index, name := range settleRefusalNames {
		if count := account.refusals[index].Load(); count > 0 {
			refused[name] = count
		}
	}
	return map[string]any{
		"retry_total":         account.retries.Load(),
		"retry_success_total": account.recovered.Load(),
		"exhausted_total":     account.exhausted.Load(),
		"refused_total":       refused,
		"refusal_values":      append([]string(nil), settleRefusalNames[:]...),
		"retry_budget_ms":     operationSettleRetryBudget.Milliseconds(),
		"semantics":           "only a local journal queue refusal before any write is retried; the SQL call is never replayed and no lock is held across a backoff",
	}
}
