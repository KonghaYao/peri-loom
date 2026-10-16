package platform

import (
	"context"
	"errors"
	"log"
	"time"
)

func (handler *hranaHTTP) refreshIdleStream(baton string, stream *hranaStream) {
	if stream.timer == nil {
		stream.expiresAt = time.Now().Add(hranaIdleTimeout)
		stream.timer = time.AfterFunc(hranaIdleTimeout, func() { handler.expireStream(baton, stream) })
		return
	}
	stream.expiresAt = time.Now().Add(hranaIdleTimeout)
	stream.timer.Reset(hranaIdleTimeout)
}

func (handler *hranaHTTP) expireStream(baton string, stream *hranaStream) {
	if !stream.mu.TryLock() {
		time.AfterFunc(time.Second, func() { handler.expireStream(baton, stream) })
		return
	}
	defer stream.mu.Unlock()
	handler.mu.Lock()
	current := handler.streams[baton] == stream
	handler.mu.Unlock()
	if !current {
		return
	}
	if time.Now().Before(stream.expiresAt) {
		stream.timer.Reset(time.Until(stream.expiresAt))
		return
	}
	handler.expireStreamLocked(baton, stream)
}

func (handler *hranaHTTP) expireStreamLocked(baton string, stream *hranaStream) {
	handler.mu.Lock()
	if handler.streams[baton] != stream {
		handler.mu.Unlock()
		return
	}
	delete(handler.streams, baton)
	handler.mu.Unlock()
	if stream.timer != nil {
		stream.timer.Stop()
	}
	if stream.session == "" {
		return
	}
	handler.cleanupRetiredSession(stream.session, 0)
}

// cleanupRetiredSession drives the retirement of one stream's session to its
// accounted end. The session's own ledger decides whether an attempt is due and
// when the bounded account is spent, so this timer only wakes that account up:
// a close_session carries no SQL, and abandoning it after one failure would
// leave the session, its quota and its stream owed forever.
func (handler *hranaHTTP) cleanupRetiredSession(session string, attempt int) {
	cleanup, cancel := context.WithTimeout(context.Background(), hranaCleanupTimeout(handler.gateway.config.RequestTimeout))
	defer cancel()
	if err := handler.gateway.expireHranaSession(cleanup, session); err != nil {
		log.Printf("Hrana session cleanup not proven: %v", err)
		if errors.Is(err, errSessionCleanupExhausted) {
			return
		}
		handler.gateway.mu.Lock()
		closing := handler.gateway.closing
		handler.gateway.mu.Unlock()
		if !closing {
			delay := time.Duration(1<<min(attempt, 5)) * time.Second
			time.AfterFunc(delay, func() { handler.cleanupRetiredSession(session, attempt+1) })
		}
	}
}

func hranaCleanupTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout < 20*time.Second {
		return 20 * time.Second
	}
	return requestTimeout
}
