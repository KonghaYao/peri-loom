package platform

import (
	"context"
	"fmt"
	"os"
	"strconv"
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
	// requestPhaseLimit is the default capacity of the ring. The ring overwrites its oldest
	// record instead of refusing new ones, so the profile keeps observing past the bound; the
	// overwrite count in the closing line makes the loss visible. The capacity is a bound, not
	// a cumulative stop.
	requestPhaseLimit = 8192
	// Only slow requests, failures and a deterministic sample of fast successes are recorded.
	// The S1-S4 stalls this profile exists to explain are 1-2 s, so 100 ms keeps an order of
	// magnitude of headroom while normal successes are covered by sampling. Errors bypass both
	// filters: an Unknown is evidence even when it returns quickly.
	requestPhaseSlowThreshold = 100 * time.Millisecond
	// Fast successes are kept when the deterministic sampler fires. One hundred per mille
	// keeps one fast success in ten, enough to explain the 2-30 ms bulk in a controlled run;
	// an operator measuring a high-rate load can lower it without losing slow evidence.
	requestPhaseSamplePermille = 100
	// Samples are tagged with the window they completed in, and window rotation is reported
	// in the closing line. The window is an observation grouping; the ring, not the window,
	// bounds memory.
	requestPhaseWindow = time.Minute
	// A larger ring is allowed, but the process should not accept an unbounded allocation
	// through an environment variable.
	requestPhaseCapacityMaximum = 65536
	// Duration knobs are capped so an absurd millisecond value cannot overflow
	// time.Duration*time.Millisecond and silently become a different setting.
	requestPhaseDurationMaximum = 24 * time.Hour
)

// The phase knobs are only read when the existing PERI_SQL_OPEN_OBSERVER profile is on. They
// are documented in request-phase-observation.md; an invalid value fails the profile start
// instead of silently changing what a measurement means.
const (
	requestPhaseThresholdEnvironment = "PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS"
	requestPhaseSampleEnvironment    = "PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE"
	requestPhaseWindowEnvironment    = "PERI_SQL_OPEN_OBSERVER_PHASE_WINDOW_MS"
	requestPhaseCapacityEnvironment  = "PERI_SQL_OPEN_OBSERVER_PHASE_CAPACITY"
	requestPhaseHTTPEnvironment      = "PERI_SQL_OPEN_OBSERVER_HTTP_BOUNDARY"
)

type requestPhase string

const (
	requestPhaseEntry     requestPhase = "entry"      // handler entry to the first span
	requestPhaseAuth      requestPhase = "auth"       // credential verification and principal
	requestPhaseDecode    requestPhase = "decode"     // body read and request validation
	requestPhaseState     requestPhase = "state_read" // narrow metadata read; may settle an ordered append
	requestPhaseAdmission requestPhase = "admission"  // quota admission and classification
	requestPhaseSession   requestPhase = "session"    // session lookup
	requestPhaseClassify  requestPhase = "classify"   // worker describe that proves a statement read-only
	requestPhaseIntent    requestPhase = "intent"     // durable intent append, including its barrier
	requestPhaseWorker    requestPhase = "worker"     // worker call
	// requestPhaseNative is the cross-process boundary to the worker. It is deliberately a
	// wait, not CPU: the gateway can measure how long the call took, not how the native side
	// spent it. The gateway request path has no Authority RPC of its own (workers hold that
	// boundary), so requestPhaseAuthority remains a named boundary for the same accounting
	// rather than a segment this path can emit honestly.
	requestPhaseNative    requestPhase = "native"
	requestPhaseAuthority requestPhase = "authority"
	requestPhaseTerminal  requestPhase = "terminal" // durable terminal append, including its barrier
	requestPhaseAudit     requestPhase = "audit"    // audit append, including its barrier
	requestPhaseRespond   requestPhase = "respond"  // response encoding and write
)

// requestPhaseKind separates waiting on another process from work this process did and from
// durable I/O. The split is static per phase: a span that crosses a process boundary is a
// wait even though encoding around it is CPU.
type requestPhaseKind string

const (
	requestPhaseCPU  requestPhaseKind = "cpu"
	requestPhaseWait requestPhaseKind = "wait"
	requestPhaseIO   requestPhaseKind = "io"
)

func (phase requestPhase) kind() requestPhaseKind {
	switch phase {
	case requestPhaseClassify, requestPhaseWorker, requestPhaseNative, requestPhaseAuthority:
		return requestPhaseWait
	case requestPhaseEntry, requestPhaseAuth, requestPhaseDecode, requestPhaseAdmission, requestPhaseSession:
		return requestPhaseCPU
	case requestPhaseState, requestPhaseIntent, requestPhaseTerminal, requestPhaseAudit, requestPhaseRespond:
		return requestPhaseIO
	default:
		return requestPhaseCPU
	}
}

// requestPhaseRecord is one line of the phase stream. Nothing here identifies a tenant: the
// request id correlates with the client's own timestamps, and the action names the operation.
type requestPhaseRecord struct {
	Stream         string                `json:"stream"`
	Scope          string                `json:"scope,omitempty"`
	StartedUnixNS  int64                 `json:"started_unix_ns,omitempty"`
	FinishedUnixNS int64                 `json:"finished_unix_ns,omitempty"`
	RequestID      string                `json:"request_id"`
	OuterRequestID string                `json:"outer_request_id,omitempty"`
	HranaStep      uint64                `json:"hrana_step,omitempty"`
	Action         string                `json:"action"`
	Outcome        string                `json:"outcome"`
	Status         int                   `json:"status,omitempty"`
	Window         uint64                `json:"window"`
	TotalNS        uint64                `json:"total_ns"`
	AccountedNS    uint64                `json:"accounted_ns"`
	UnaccountedNS  uint64                `json:"unaccounted_ns"`
	Truncated      bool                  `json:"truncated"`
	Spans          []requestPhaseSegment `json:"spans"`
}

type requestPhaseSegment struct {
	Phase requestPhase     `json:"phase"`
	Kind  requestPhaseKind `json:"kind"`
	NS    uint64           `json:"ns"`
}

// requestPhaseConfig is immutable after the observer is built, so readers never take a lock to
// ask what the current threshold or rate is.
type requestPhaseConfig struct {
	SlowThreshold  time.Duration
	SamplePermille uint64
	Window         time.Duration
	Capacity       int
	HTTPBoundary   bool
}

func defaultRequestPhaseConfig() requestPhaseConfig {
	return requestPhaseConfig{
		SlowThreshold:  requestPhaseSlowThreshold,
		SamplePermille: requestPhaseSamplePermille,
		Window:         requestPhaseWindow,
		Capacity:       requestPhaseLimit,
	}
}

// requestPhaseConfigFromEnvironment reads the knobs of an already-enabled profile. Unset names
// keep their defaults; invalid values fail the start instead of quietly changing the capture.
func requestPhaseConfigFromEnvironment() (requestPhaseConfig, error) {
	config := defaultRequestPhaseConfig()
	if raw, present := os.LookupEnv(requestPhaseHTTPEnvironment); present {
		if raw != "0" && raw != "1" {
			return requestPhaseConfig{}, fmt.Errorf("%s must be 0 or 1", requestPhaseHTTPEnvironment)
		}
		config.HTTPBoundary = raw == "1"
	}
	if raw, present := os.LookupEnv(requestPhaseThresholdEnvironment); present {
		value, err := requestPhaseMilliseconds(requestPhaseThresholdEnvironment, raw, true)
		if err != nil {
			return requestPhaseConfig{}, err
		}
		config.SlowThreshold = value
	}
	if raw, present := os.LookupEnv(requestPhaseSampleEnvironment); present {
		value, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || value > 1000 {
			return requestPhaseConfig{}, fmt.Errorf("%s must be between 0 and 1000", requestPhaseSampleEnvironment)
		}
		config.SamplePermille = value
	}
	if raw, present := os.LookupEnv(requestPhaseWindowEnvironment); present {
		value, err := requestPhaseMilliseconds(requestPhaseWindowEnvironment, raw, false)
		if err != nil {
			return requestPhaseConfig{}, err
		}
		config.Window = value
	}
	if raw, present := os.LookupEnv(requestPhaseCapacityEnvironment); present {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > requestPhaseCapacityMaximum {
			return requestPhaseConfig{}, fmt.Errorf("%s must be between 1 and %d", requestPhaseCapacityEnvironment, requestPhaseCapacityMaximum)
		}
		config.Capacity = value
	}
	return config, nil
}

func requestPhaseMilliseconds(name, raw string, allowZero bool) (time.Duration, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	minimum := int64(1)
	if allowZero {
		minimum = 0
	}
	maximum := int64(requestPhaseDurationMaximum / time.Millisecond)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d milliseconds", name, minimum, maximum)
	}
	return time.Duration(value) * time.Millisecond, nil
}

type requestPhaseObserver struct {
	slowThreshold  time.Duration
	samplePermille uint64
	window         time.Duration
	capacity       int
	httpBoundary   bool

	sequence    atomic.Uint64 // records accepted into the ring
	filtered    atomic.Uint64 // fast successes removed by the sampler
	overwritten atomic.Uint64 // records the ring overwrote before a reader took them
	dropped     atomic.Uint64 // filtered + overwritten, the total presented for lost samples
	sampled     atomic.Uint64 // fast successes the sampler considered
	keptSlow    atomic.Uint64
	keptFailure atomic.Uint64
	windows     atomic.Uint64 // window boundaries crossed since the observer started
	lastWindow  atomic.Uint64

	mutex   sync.Mutex
	records []requestPhaseRecord
	head    int
	size    int
}

type requestPhaseStats struct {
	Accepted    uint64
	Sampled     uint64
	Filtered    uint64
	Overwritten uint64
	Dropped     uint64
	Windows     uint64
	KeptSlow    uint64
	KeptFailure uint64
}

type requestPhaseFrame struct {
	phase   requestPhase
	started time.Time
	childNS uint64
}

type requestPhaseTrace struct {
	observer *requestPhaseObserver
	request  string
	outer    hranaStep
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
	return newRequestPhaseObserverWithConfig(defaultRequestPhaseConfig())
}

func newRequestPhaseObserverWithConfig(config requestPhaseConfig) *requestPhaseObserver {
	if config.Capacity < 1 {
		config.Capacity = requestPhaseLimit
	}
	if config.Window <= 0 {
		config.Window = requestPhaseWindow
	}
	if config.SamplePermille > 1000 {
		config.SamplePermille = 1000
	}
	observer := &requestPhaseObserver{
		slowThreshold:  config.SlowThreshold,
		samplePermille: config.SamplePermille,
		window:         config.Window,
		capacity:       config.Capacity,
		httpBoundary:   config.HTTPBoundary,
		records:        make([]requestPhaseRecord, config.Capacity),
	}
	// Baselines the rotation counter at the window the observer started in. Without this,
	// the first record would report every window since the Unix epoch as a rotation.
	observer.lastWindow.Store(requestPhaseWindowIndex(time.Now(), config.Window))
	return observer
}

func (observer *requestPhaseObserver) TryReceive() (requestPhaseRecord, bool) {
	if observer == nil {
		return requestPhaseRecord{}, false
	}
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	if observer.size == 0 {
		return requestPhaseRecord{}, false
	}
	record := observer.records[observer.head]
	observer.records[observer.head] = requestPhaseRecord{}
	observer.head = (observer.head + 1) % observer.capacity
	observer.size--
	return record, true
}

func (observer *requestPhaseObserver) stats() requestPhaseStats {
	if observer == nil {
		return requestPhaseStats{}
	}
	return requestPhaseStats{
		Accepted:    observer.sequence.Load(),
		Sampled:     observer.sampled.Load(),
		Filtered:    observer.filtered.Load(),
		Overwritten: observer.overwritten.Load(),
		Dropped:     observer.dropped.Load(),
		Windows:     observer.windows.Load(),
		KeptSlow:    observer.keptSlow.Load(),
		KeptFailure: observer.keptFailure.Load(),
	}
}

// keep decides whether a finished request belongs in the ring. A failure always stays so a
// fast Unknown cannot disappear from the evidence. Slow requests stay regardless of the
// sampler. A fast success is kept by the deterministic sampler, which makes the configured
// rate exact over a run instead of unpredictable per request.
func (observer *requestPhaseObserver) keep(total uint64, outcome string, status int) bool {
	if outcome != "ok" || status < 200 || status >= 300 {
		observer.keptFailure.Add(1)
		return true
	}
	if total >= uint64(observer.slowThreshold) {
		observer.keptSlow.Add(1)
		return true
	}
	count := observer.sampled.Add(1)
	if requestPhaseSampled(count, observer.samplePermille) {
		return true
	}
	observer.filtered.Add(1)
	observer.dropped.Add(1)
	return false
}

// requestPhaseSampled reports whether the count-th fast success is kept. The remainder changes
// exactly permille times per thousand candidates, so a run of 1000 fast successes keeps
// permille of them and the first kept sample arrives at the configured stride. The modulo keeps
// the arithmetic bounded for a long-running process.
func requestPhaseSampled(count, permille uint64) bool {
	if permille == 0 || count == 0 {
		return false
	}
	if permille >= 1000 {
		return true
	}
	return (count%1000)*permille%1000 < permille
}

func (observer *requestPhaseObserver) record(record requestPhaseRecord) {
	if observer == nil {
		return
	}
	observer.sequence.Add(1)
	// The ring is bounded and never blocks a request: when a reader falls behind, the oldest
	// record is replaced and the loss is counted instead of stalling the path being measured.
	observer.mutex.Lock()
	if observer.size == observer.capacity {
		observer.records[observer.head] = record
		observer.head = (observer.head + 1) % observer.capacity
		observer.overwritten.Add(1)
		observer.dropped.Add(1)
	} else {
		index := (observer.head + observer.size) % observer.capacity
		observer.records[index] = record
		observer.size++
	}
	observer.mutex.Unlock()
}

func requestPhaseWindowIndex(now time.Time, window time.Duration) uint64 {
	if window <= 0 {
		return 0
	}
	nanos := now.UnixNano()
	if nanos < 0 {
		return 0
	}
	return uint64(nanos / int64(window))
}

// observeWindow counts window boundaries as records complete. Out-of-order completions only
// move the mark forward, so the count is a rotation count, not a per-request counter.
func (observer *requestPhaseObserver) observeWindow(index uint64) {
	for {
		last := observer.lastWindow.Load()
		if index <= last {
			return
		}
		if observer.lastWindow.CompareAndSwap(last, index) {
			observer.windows.Add(index - last)
			return
		}
	}
}

// beginRequestPhases binds a trace for this request when the profile is on, and returns the
// context unchanged when it is off.
func beginRequestPhases(ctx context.Context, observer *requestPhaseObserver, requestID, action string, started time.Time) (context.Context, *requestPhaseTrace) {
	if ctx == nil || observer == nil {
		return ctx, nil
	}
	trace := &requestPhaseTrace{observer: observer, request: requestID, action: action, started: started}
	trace.outer, _ = ctx.Value(hranaStepKey{}).(hranaStep)
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
	trace.spans = append(trace.spans, requestPhaseSegment{Phase: frame.phase, Kind: frame.phase.kind(), NS: uint64(own)})
}

// finish records the request's own totals. Spans are reported as they were measured, and the
// time no span covered is reported separately instead of being attributed to a phase.
func (trace *requestPhaseTrace) finish(requestID, outcome string, status int) {
	if trace == nil {
		return
	}
	now := time.Now()
	elapsed := now.Sub(trace.started)
	if elapsed < 0 {
		elapsed = 0
	}
	window := requestPhaseWindowIndex(now, trace.observer.window)
	trace.observer.observeWindow(window)
	// Sample before the record is built: a fast success costs one clock read and never takes
	// a slot in the ring unless the sampler keeps it. Failures and slow requests keep their
	// evidence.
	if !trace.observer.keep(uint64(elapsed), outcome, status) {
		return
	}
	trace.mutex.Lock()
	for len(trace.frames) > 0 {
		trace.leaveLocked()
	}
	// Entry is what happened before the first span opened: connection handling, the request
	// context, correlation parsing and route selection.
	entry := elapsed
	if !trace.first.IsZero() {
		entry = trace.first.Sub(trace.started)
	}
	if entry < 0 {
		entry = 0
	}
	spans := append([]requestPhaseSegment{{Phase: requestPhaseEntry, Kind: requestPhaseEntry.kind(), NS: uint64(entry)}}, trace.spans...)
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
		Stream:         "",
		StartedUnixNS:  trace.started.UnixNano(),
		FinishedUnixNS: now.UnixNano(),
		RequestID:      requestID,
		OuterRequestID: trace.outer.outer,
		HranaStep:      trace.outer.index,
		Action:         trace.action,
		Outcome:        outcome,
		Status:         status,
		Window:         window,
		TotalNS:        total,
		AccountedNS:    accounted,
		UnaccountedNS:  total - accounted,
		Truncated:      truncated,
		Spans:          spans,
	})
}
