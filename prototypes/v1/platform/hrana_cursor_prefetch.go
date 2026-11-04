package platform

import (
	"context"
	"net/http"
	"time"
)

// cursorFetchAheadTimeout bounds a prefetched page fetch that the client can no
// longer answer for. It is a backstop only: the gateway's own upstream timeout
// ends the worker call, and a prefetch normally returns in about a page's
// production time.
const cursorFetchAheadTimeout = 30 * time.Second

// cursorFetchAhead issues the next read-cursor page fetch while the current page
// is still being transcoded and written. The worker produces page N+1 during
// the adapter's local work, which is the only part of the page loop that does
// not need the worker.
//
// The fetch keeps the caller's request values (the authorization scope attached
// by the protocol adapter) but not its cancellation: the worker executes the
// page even after a disconnect, and a cancelled context would turn that
// already-issued fetch into an audited transport failure. The caller joins the
// channel before it closes the cursor or session, and the channel is buffered so
// an abandoned fetch never blocks its goroutine.
func (handler *hranaHTTP) cursorFetchAhead(request *http.Request, credential string, session string) <-chan hranaReply {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(request.Context()), cursorFetchAheadTimeout)
	prefetch := request.WithContext(ctx)
	input := map[string]any{"session": session, "max_rows": 256, "write_cursor": false}
	done := make(chan hranaReply, 1)
	go func() {
		defer cancel()
		done <- handler.call(prefetch, credential, "/v1/sql/cursor/fetch", input)
	}()
	return done
}
