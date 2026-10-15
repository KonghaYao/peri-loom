package platform

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	controller "peri-loom/controller"
)

const nativeSessionIdleTimeout = 5 * time.Minute

// Only the Hrana adapter can attach this context value. A public HTTP header
// cannot turn a native session into a Hrana session and bypass native expiry.
type hranaSessionContextKey struct{}

func (gateway *Gateway) expireNativeSessions(ctx context.Context, idle time.Duration) error {
	if ctx == nil || idle <= 0 {
		return errors.New("invalid native session expiry request")
	}
	gateway.mu.Lock()
	if gateway.closing {
		gateway.mu.Unlock()
		return nil
	}
	sessions := make([]*gatewaySession, 0, len(gateway.sessions))
	for _, session := range gateway.sessions {
		sessions = append(sessions, session)
	}
	gateway.mu.Unlock()
	now := time.Now()
	due := make([]*gatewaySession, 0)
	for _, session := range sessions {
		if !session.mu.TryLock() {
			continue
		}
		expired := !session.closed && !session.hrana && !session.poisoned && !session.lastUsed.IsZero() && now.Sub(session.lastUsed) >= idle
		session.mu.Unlock()
		if expired {
			due = append(due, session)
		}
	}
	if len(due) == 0 {
		return ctx.Err()
	}
	const maxConcurrentNativeCleanup = 16
	jobs := make(chan *gatewaySession)
	failures := make(chan error, len(due))
	var group sync.WaitGroup
	for range min(len(due), maxConcurrentNativeCleanup) {
		group.Add(1)
		go func() {
			defer group.Done()
			for session := range jobs {
				if err := gateway.expireNativeSession(ctx, session, now, idle); err != nil {
					failures <- err
				}
			}
		}()
	}
dispatch:
	for _, session := range due {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- session:
		}
	}
	close(jobs)
	group.Wait()
	close(failures)
	var all []error
	for err := range failures {
		all = append(all, err)
	}
	return errors.Join(append(all, ctx.Err())...)
}

func (gateway *Gateway) expireNativeSession(ctx context.Context, session *gatewaySession, now time.Time, idle time.Duration) error {
	if !session.mu.TryLock() {
		return nil
	}
	defer session.mu.Unlock()
	if session.closed || session.hrana || session.poisoned || session.lastUsed.IsZero() || now.Sub(session.lastUsed) < idle {
		return nil
	}
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		return err
	}
	if _, blocked := isolatedOperation(state, session.database, true); blocked {
		return ErrDatabaseQuarantined
	}
	project, exists := state.Projects[session.project]
	if !exists {
		return errors.New("native session project unavailable")
	}
	permit, ok := gateway.admission.reserve(session.project, project.Quotas, false, true)
	if !ok {
		return errors.New("native session cleanup admission unavailable")
	}
	defer permit.finish()
	requestID, err := controller.RandomID()
	if err != nil {
		return err
	}
	if err = gateway.closeTrackedSession(ctx, session, requestID, permit); err != nil {
		var failure *SQLRPCError
		if !errors.As(err, &failure) || failure.Ambiguous {
			session.poisoned = true
			permit.retain()
		}
		return err
	}
	gateway.removeSession(session)
	return nil
}

func (gateway *Gateway) runNativeSessionExpiry(ctx context.Context) error {
	return gateway.runNativeSessionExpiryWith(ctx, nativeSessionIdleTimeout, time.Second)
}

func (gateway *Gateway) runNativeSessionExpiryWith(ctx context.Context, idle, interval time.Duration) error {
	if ctx == nil || idle <= 0 || interval <= 0 {
		return errors.New("invalid native session expiry schedule")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastReport time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			cleanup, cancel := context.WithTimeout(ctx, hranaCleanupTimeout(gateway.config.RequestTimeout))
			err := gateway.expireNativeSessions(cleanup, idle)
			cancel()
			if err != nil && ctx.Err() == nil && time.Since(lastReport) >= 30*time.Second {
				log.Print("native session cleanup not proven; retry or operator recovery required")
				lastReport = time.Now()
			}
		}
	}
}
