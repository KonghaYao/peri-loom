package platform

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// A request phase trace answers one question: where did one request spend its time. It is a
// private diagnostic, off unless the observer profile is enabled, and it never carries a
// credential, SQL text, or anything a client could observe. The server path is identical with
// the profile off: every entry point below is a nil check, and the spans are only recorded when
// an observer was bound into the request context.
//
// A span measures its own work, not its children: the stack subtracts the time a nested span
// spent, so the recorded spans partition the request and never double count. What the spans do
// not cover stays visible as unaccounted time instead of being folded into a neighbour.
const (
	requestPhaseMaxDepth = 8
	requestPhaseMaxSpans = 16
	// The bound covers one import-and-query slice so the phases of a data load and the
	// phases of the reads against that data come from the same process. It stays a
	// bound: past it the stream drops records and says so instead of growing.
	requestPhaseLimit = 8192
)

type requestPhase string

const (
	requestPhaseEntry     requestPhase = "entry"      // handler entry to the first span
	requestPhaseAuth      requestPhase = "auth"       // credential verification and principal
	requestPhaseDecode    requestPhase = "decode"     // body read and request validation
	requestPhaseState     requestPhase = "state_read" // narrow metadata read; may settle an ordered append
	requestPhaseAdmission requestPhase = "admission"  // quota admission and classification
	requestPhaseSession   requestPhase = "session"    // session lookup
	requestPhaseIntent    requestPhase = "intent"     // durable intent append, including its barrier
	requestPhaseWorker    requestPhase = "worker"     // worker call
	requestPhaseTerminal  requestPhase = "terminal"   // durable terminal append, including its barrier
	requestPhaseAudit     requestPhase = "audit"      // audit append, including its barrier
	requestPhaseRespond   requestPhase = "respond"    // response encoding and write
)

// requestPhaseRecord is one line of the phase stream. Nothing here identifies a tenant: the
// request id correlates with the client's own timestamps, and the action names the operation.
type requestPhaseRecord struct {
	Stream        string                `json:"stream"`
	RequestID     string                `json:"request_id"`
	Action        string                `json:"action"`
	Outcome       string                `json:"outcome"`
	Status        int                   `json:"status"`
	TotalNS       uint64                `json:"total_ns"`
	AccountedNS   uint64                `json:"accounted_ns"`
	UnaccountedNS uint64                `json:"unaccounted_ns"`
	Truncated     bool                  `json:"truncated"`
	Spans         []requestPhaseSegment `json:"spans"`
}

type requestPhaseSegment struct {
	Phase requestPhase `json:"phase"`
	NS    uint64       `json:"ns"`
}

type requestPhaseObserver struct {
	sequence atomic.Uint64
	queue    chan requestPhaseRecord
}

type requestPhaseFrame struct {
	phase   requestPhase
	started time.Time
	childNS uint64
}

type requestPhaseTrace struct {
	observer *requestPhaseObserver
	request  string
	action   string
	started  time.Time

	mutex     sync.Mutex
	frames    []requestPhaseFrame
	spans     []requestPhaseSegment
	first     time.Time
	truncated bool
}

type requestPhaseKey struct{}

// noRequestPhaseSpan is what every span call returns when the profile is off, so the disabled
// path allocates nothing and cannot change the request's behaviour.
var noRequestPhaseSpan = func() {}

func newRequestPhaseObserver() *requestPhaseObserver {
	return &requestPhaseObserver{queue: make(chan requestPhaseRecord, requestPhaseLimit)}
}

func (observer *requestPhaseObserver) TryReceive() (requestPhaseRecord, bool) {
	if observer != nil {
		select {
		case record := <-observer.queue:
			return record, true
		default:
		}
	}
	return requestPhaseRecord{}, false
}

func (observer *requestPhaseObserver) record(record requestPhaseRecord) {
	if observer == nil {
		return
	}
	// The stream is bounded and never blocks a request: a diagnostic that drops records is
	// better than one that slows the path it measures.
	if observer.sequence.Add(1) > requestPhaseLimit {
		return
	}
	select {
	case observer.queue <- record:
	default:
	}
}

// beginRequestPhases binds a trace for this request when the profile is on, and returns the
// context unchanged when it is off.
func beginRequestPhases(ctx context.Context, observer *requestPhaseObserver, requestID, action string, started time.Time) (context.Context, *requestPhaseTrace) {
	if ctx == nil || observer == nil {
		return ctx, nil
	}
	trace := &requestPhaseTrace{observer: observer, request: requestID, action: action, started: started}
	return context.WithValue(ctx, requestPhaseKey{}, trace), trace
}

// requestPhaseSpan opens one span and returns the function that closes it. With the profile
// off there is no trace in the context and this is a nil check plus a shared no-op function.
func requestPhaseSpan(ctx context.Context, phase requestPhase) func() {
	if ctx == nil {
		return noRequestPhaseSpan
	}
	trace, _ := ctx.Value(requestPhaseKey{}).(*requestPhaseTrace)
	if trace == nil {
		return noRequestPhaseSpan
	}
	return trace.enter(phase)
}

func (trace *requestPhaseTrace) enter(phase requestPhase) func() {
	trace.mutex.Lock()
	if len(trace.frames) >= requestPhaseMaxDepth || len(trace.spans) >= requestPhaseMaxSpans {
		trace.truncated = true
		trace.mutex.Unlock()
		return noRequestPhaseSpan
	}
	started := time.Now()
	if trace.first.IsZero() {
		trace.first = started
	}
	trace.frames = append(trace.frames, requestPhaseFrame{phase: phase, started: started})
	trace.mutex.Unlock()
	return func() {
		trace.mutex.Lock()
		trace.leaveLocked()
		trace.mutex.Unlock()
	}
}

// leaveLocked closes the innermost frame, charges its elapsed time to its parent's children,
// and records only the time the frame itself spent.
func (trace *requestPhaseTrace) leaveLocked() {
	if len(trace.frames) == 0 {
		return
	}
	frame := trace.frames[len(trace.frames)-1]
	trace.frames = trace.frames[:len(trace.frames)-1]
	elapsed := time.Since(frame.started)
	own := elapsed - time.Duration(frame.childNS)
	if own < 0 {
		own = 0
	}
	if len(trace.frames) > 0 {
		trace.frames[len(trace.frames)-1].childNS += uint64(elapsed)
	}
	trace.spans = append(trace.spans, requestPhaseSegment{Phase: frame.phase, NS: uint64(own)})
}

// finish records the request's own totals. Spans are reported as they were measured, and the
// time no span covered is reported separately instead of being attributed to a phase.
func (trace *requestPhaseTrace) finish(requestID, outcome string, status int) {
	if trace == nil {
		return
	}
	trace.mutex.Lock()
	for len(trace.frames) > 0 {
		trace.leaveLocked()
	}
	elapsed := time.Since(trace.started)
	// Entry is what happened before the first span opened: connection handling, the request
	// context, correlation parsing and route selection.
	entry := elapsed
	if !trace.first.IsZero() {
		entry = trace.first.Sub(trace.started)
	}
	if entry < 0 {
		entry = 0
	}
	spans := append([]requestPhaseSegment{{Phase: requestPhaseEntry, NS: uint64(entry)}}, trace.spans...)
	truncated := trace.truncated
	trace.mutex.Unlock()
	var accounted uint64
	for _, span := range spans {
		accounted += span.NS
	}
	total := uint64(elapsed)
	if accounted > total {
		accounted = total
	}
	if requestID == "" {
		requestID = trace.request
	}
	trace.observer.record(requestPhaseRecord{
		Stream:        "",
		RequestID:     requestID,
		Action:        trace.action,
		Outcome:       outcome,
		Status:        status,
		TotalNS:       total,
		AccountedNS:   accounted,
		UnaccountedNS: total - accounted,
		Truncated:     truncated,
		Spans:         spans,
	})
}
