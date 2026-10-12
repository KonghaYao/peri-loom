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

func (handler *hranaHTTP) cleanupRetiredSession(session string, attempt int) {
	cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := handler.gateway.expireHranaSession(cleanup, session); err != nil {
		log.Printf("Hrana session cleanup not proven: %v", err)
		var failure *SQLRPCError
		if errors.Is(err, errHranaSessionUnknown) || errors.Is(err, ErrDatabaseQuarantined) || errors.As(err, &failure) && failure.Ambiguous {
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
