package platform

import (
	"net/http"
	"time"
)

// The inner Gateway spans stop at an in-memory recorder, not at the public
// Hrana handler. This envelope exposes adapter time without attributing the
// remaining client/server gap to storage. It deliberately does not wrap the
// writer: preserving Flusher/Hijacker behaviour is part of the observed path.
func observeHranaHTTPBoundary(request *http.Request, observer *requestPhaseObserver) func() {
	if observer == nil || !observer.httpBoundary {
		return noRequestPhaseSpan
	}
	correlation, _ := request.Context().Value(hranaCorrelationKey{}).(*hranaCorrelation)
	if correlation == nil || correlation.started.IsZero() {
		return noRequestPhaseSpan
	}
	started := correlation.started
	return func() {
		finished := time.Now()
		total := uint64(max(finished.Sub(started), 0))
		window := requestPhaseWindowIndex(finished, observer.window)
		observer.observeWindow(window)
		// This is a duration sample, not an HTTP outcome. Actual inner failures
		// retain their existing unconditional sampling; the envelope makes no
		// status claim about an unwrapped writer or a hijacked connection.
		if !observer.keep(total, "ok", http.StatusOK) {
			return
		}
		observer.record(requestPhaseRecord{
			Scope: "hrana_http", RequestID: correlation.outer, OuterRequestID: correlation.outer,
			Action: "hrana_http", Outcome: "handler_returned", Window: window,
			StartedUnixNS: started.UnixNano(), FinishedUnixNS: finished.UnixNano(),
			TotalNS: total, UnaccountedNS: total,
		})
	}
}
