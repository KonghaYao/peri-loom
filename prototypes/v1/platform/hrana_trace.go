package platform

import (
	"context"
	"net/http"
	"sync/atomic"

	controller "peri-loom/controller"
)

type hranaCorrelationKey struct{}
type hranaStepKey struct{}

type hranaCorrelation struct {
	outer string
	next  atomic.Uint64
}

type hranaStep struct {
	outer string
	index uint64
}

// The outer ID is diagnostic only. Each inner Gateway call keeps its own
// operation/request ID: a multi-step pipeline must never reuse one intent ID.
func beginHranaCorrelation(request *http.Request, observer *requestPhaseObserver) *http.Request {
	if observer == nil {
		return request
	}
	identifier, _ := controller.RandomID()
	values := request.Header.Values("X-Request-ID")
	if len(values) == 1 && sqlLabel(values[0]) {
		identifier = values[0]
	}
	return request.WithContext(context.WithValue(request.Context(), hranaCorrelationKey{}, &hranaCorrelation{outer: identifier}))
}

func nextHranaStep(ctx context.Context) context.Context {
	correlation, _ := ctx.Value(hranaCorrelationKey{}).(*hranaCorrelation)
	if correlation == nil {
		return ctx
	}
	return context.WithValue(ctx, hranaStepKey{}, hranaStep{outer: correlation.outer, index: correlation.next.Add(1)})
}
