package stress

import (
	"context"
	"sync"
	"time"
)

type phaseDispatch struct {
	mutex     sync.Mutex
	context   context.Context
	cancel    context.CancelFunc
	offerStop <-chan struct{}
}

func newPhaseDispatch(parent context.Context, offerStop <-chan struct{}) *phaseDispatch {
	ctx, cancel := context.WithCancel(parent)
	return &phaseDispatch{context: ctx, cancel: cancel, offerStop: offerStop}
}

func (dispatch *phaseDispatch) admit() bool {
	dispatch.mutex.Lock()
	defer dispatch.mutex.Unlock()
	return dispatch.context.Err() == nil && !offerStopRequested(dispatch.offerStop)
}

func offerStopRequested(offerStop <-chan struct{}) bool {
	select {
	case <-offerStop:
		return true
	default:
		return false
	}
}

func (dispatch *phaseDispatch) stopOffers() {
	dispatch.mutex.Lock()
	defer dispatch.mutex.Unlock()
	dispatch.cancel()
}

func phaseSQL(parent context.Context, session *Session, timeout time.Duration, query bool, sql string) ([][]any, string, *Failure) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return session.SQL(ctx, query, sql)
}
