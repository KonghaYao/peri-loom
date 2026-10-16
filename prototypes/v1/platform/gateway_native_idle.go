package platform

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
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
		// A session whose outcome is unknown is still due: its owner can no
		// longer address it, and the close below is the only platform path that
		// can take it and its quota to an accounted end.
		expired := !session.closed && !session.hrana && !session.lastUsed.IsZero() && now.Sub(session.lastUsed) >= idle
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
	if session.closed || session.hrana || session.lastUsed.IsZero() || now.Sub(session.lastUsed) < idle {
		return nil
	}
	if err := sessionCleanupGate(session, now); err != nil {
		// A spent account stays loud: the cleanup ledger reports the count, the
		// age and the reason, and the sweeper's periodic log is the operator's
		// trace that only the retirement path can finish this session.
		if errors.Is(err, errSessionCleanupExhausted) {
			return err
		}
		return nil
	}
	return gateway.retireSession(ctx, session, now)
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
