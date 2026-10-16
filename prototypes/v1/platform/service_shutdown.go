package platform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// defaultServiceShutdownTimeout is the whole teardown budget. The HTTP servers,
// the gateway drain, the operation checkpoint and the audit tail share this one
// deadline: the orchestrator stops the process with its own per-stage cap, and a
// step that waits on a context of its own can outlive that cap, get killed
// mid-teardown and turn a stop that could have been clean into a dirty one.
const defaultServiceShutdownTimeout = 10 * time.Second

// maxServiceShutdownTimeout is the bound on a declared budget. Past it the value
// is no longer a teardown budget but an unbounded wait on the launcher's cap.
const maxServiceShutdownTimeout = 60 * time.Second

// ErrShutdownIncomplete marks a stop whose teardown did not finish inside the
// budget. The service reports it instead of waiting forever, and the caller's
// exit status makes the launcher refuse to record a clean terminal.
var ErrShutdownIncomplete = errors.New("platform shutdown incomplete")

// serviceShutdownTimeout reads the budget the deployment allows this process and
// keeps it inside the cap the launcher holds it to. The launcher owns the outer
// deadline, so a budget above the default is only accepted next to a declared
// per-stage cap that stays strictly above it: a process still tearing down when
// the cap expires is killed mid-stop, which turns a stop that could have been
// clean into a dirty one. Lowering the budget is always accepted, because a stop
// that finishes earlier fits under any cap, and an unusable value refuses to
// start rather than silently stopping on a different budget than the one asked
// for.
func serviceShutdownTimeout() (time.Duration, error) {
	grace, err := declaredShutdownOuterGrace()
	if err != nil {
		return 0, err
	}
	stageCap, err := declaredShutdownStageCap()
	if err != nil {
		return 0, err
	}
	// A cap is only evidence about the real kill if it is smaller than the grace
	// the orchestrator enforces. The process cannot observe that grace, so the
	// launcher declares it and a cap that reaches it refuses to start: a
	// self-declared cap larger than the outer grace is a check that cannot fail,
	// not a covered stop.
	if grace > 0 && stageCap > 0 && stageCap >= grace {
		return 0, errors.New("declared shutdown stage cap must stay below the declared outer grace")
	}
	setting := os.Getenv("PERI_SHUTDOWN_TIMEOUT_MS")
	if setting == "" {
		if stageCap > 0 && defaultServiceShutdownTimeout >= stageCap {
			return 0, errors.New("declared shutdown stage cap does not exceed the default shutdown timeout")
		}
		if grace > 0 && defaultServiceShutdownTimeout >= grace {
			return 0, errors.New("declared outer grace does not cover the default shutdown timeout")
		}
		return defaultServiceShutdownTimeout, nil
	}
	milliseconds, err := strconv.Atoi(setting)
	if err != nil || milliseconds < 1000 || milliseconds > int(maxServiceShutdownTimeout.Milliseconds()) {
		return 0, errors.New("invalid shutdown timeout")
	}
	timeout := time.Duration(milliseconds) * time.Millisecond
	if stageCap == 0 {
		if timeout > defaultServiceShutdownTimeout {
			return 0, errors.New("shutdown timeout above the default requires PERI_SHUTDOWN_STAGE_CAP_MS")
		}
		if grace > 0 && timeout >= grace {
			return 0, errors.New("shutdown timeout must stay below the declared outer grace")
		}
		return timeout, nil
	}
	if timeout >= stageCap {
		return 0, errors.New("shutdown timeout must stay below the declared shutdown stage cap")
	}
	return timeout, nil
}

// declaredShutdownStageCap reads the per-stage stop cap the launcher holds this
// process to. It is the one statement of the outer deadline the process can check
// its own budget against, so an unusable value refuses to start instead of
// silently dropping the check.
func declaredShutdownStageCap() (time.Duration, error) {
	setting := os.Getenv("PERI_SHUTDOWN_STAGE_CAP_MS")
	if setting == "" {
		return 0, nil
	}
	milliseconds, err := strconv.Atoi(setting)
	if err != nil || milliseconds < 1000 || milliseconds > 300000 {
		return 0, errors.New("invalid shutdown stage cap")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// declaredShutdownOuterGrace reads the stop grace the orchestrator enforces
// around this process. The cap above is the launcher's own statement about the
// stage it will stop; this is the deployment's statement about the whole stop,
// and it is what keeps a declared cap from being larger than the real kill:
// without it a cap of any size is accepted next to a budget just below it, and a
// process that stops itself after the orchestrator already sent SIGKILL never
// gets to report the unclean exit. An unusable value refuses to start.
func declaredShutdownOuterGrace() (time.Duration, error) {
	setting := os.Getenv("PERI_SHUTDOWN_OUTER_GRACE_MS")
	if setting == "" {
		return 0, nil
	}
	milliseconds, err := strconv.Atoi(setting)
	if err != nil || milliseconds < 1000 || milliseconds > 300000 {
		return 0, errors.New("invalid shutdown outer grace")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// shutdownBudget hands every teardown step the same deadline. The first caller
// starts the clock: a later step cannot buy a fresh budget and let the tail grow
// past the orchestrator's stop cap.
type shutdownBudget struct {
	mu      sync.Mutex
	timeout time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
}

func newShutdownBudget(timeout time.Duration) *shutdownBudget {
	return &shutdownBudget{timeout: timeout}
}

func (budget *shutdownBudget) begin() context.Context {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.ctx == nil {
		budget.ctx, budget.cancel = context.WithTimeout(context.Background(), budget.timeout)
	}
	return budget.ctx
}

func (budget *shutdownBudget) stop() {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.cancel != nil {
		budget.cancel()
		budget.cancel = nil
	}
}

// stopStep names one teardown action, so an unclean stop can report which part
// of the shutdown did not finish.
type stopStep struct {
	name string
	run  func(context.Context) error
}

// runServiceTeardown runs the teardown steps in order under one deadline and
// reports every step that did not finish. A step still running when the budget
// expires is abandoned and reported: the process must not wait forever, and it
// must not claim a clean stop either. Once the budget is spent the remaining
// steps are reported as not attempted rather than started, because nothing is
// left to give them.
func runServiceTeardown(budget *shutdownBudget, steps []stopStep) error {
	var failure error
	for _, step := range steps {
		ctx := budget.begin()
		if err := ctx.Err(); err != nil {
			failure = errors.Join(failure, fmt.Errorf("%w: %s not attempted", ErrShutdownIncomplete, step.name))
			continue
		}
		// The bound is enforced here, not by the step: a teardown that ignores its
		// context would otherwise hold the process open past the deadline.
		done := make(chan error, 1)
		go func(run func(context.Context) error) { done <- run(ctx) }(step.run)
		select {
		case err := <-done:
			if err != nil {
				// Name the step: the launcher only sees the exit status, so the
				// operator has to learn which part of the teardown failed.
				failure = errors.Join(failure, fmt.Errorf("%s: %w", step.name, err))
			}
		case <-ctx.Done():
			failure = errors.Join(failure, fmt.Errorf("%w: %s exceeded the shutdown budget", ErrShutdownIncomplete, step.name))
		}
	}
	return failure
}

// closeAuditWithin bounds the audit tail with the shutdown budget. The tail
// waits for the writes that already backed an acknowledged request, so an
// unbounded wait here is exactly what let an orchestrator kill the process
// mid-tail and record a stop that could have been clean as a dirty one.
func closeAuditWithin(budget *shutdownBudget, audit Audit) error {
	return runServiceTeardown(budget, []stopStep{{name: "audit tail", run: func(context.Context) error { return audit.Close() }}})
}

// joinServiceResult keeps a clean result clean and a failed one failed: joining
// a single nil error would replace the identity callers compare against.
func joinServiceResult(current, failure error) error {
	if failure == nil {
		return current
	}
	if current == nil {
		return failure
	}
	return errors.Join(current, failure)
}
