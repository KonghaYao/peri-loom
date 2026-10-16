package platform

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	controller "peri-loom/controller"
)

type Gateway struct {
	config         GatewayConfig
	mu             sync.Mutex
	closing        bool
	hrana          *hranaHTTP
	sessions       map[string]*gatewaySession
	admission      *gatewayAdmission
	counters       *gatewayCounters
	operationHolds map[string]*admissionPermit
	operationMu    sync.Mutex
	// cleanup is the in-process age account of the cleanup facts whose durable
	// record carries no timestamp.
	cleanup *cleanupLedger
}

type gatewaySession struct {
	mu             sync.Mutex
	public         string
	upstream       string
	worker         controller.Worker
	binding        controller.Binding
	database       controller.DatabaseID
	project        string
	subject        string
	keyID          string
	poisoned       bool
	moved          bool
	closed         bool
	writerProtocol bool
	writerKnown    bool
	writerOwner    bool
	writerSequence uint64
	hrana          bool
	lastUsed       time.Time
	// cleanup is this session's bounded retirement account, and unownedAt marks
	// when an unproven outcome stopped its owner from addressing it.
	cleanup   sessionCleanup
	unownedAt time.Time
}

type gatewayResult struct {
	status                  int
	code                    string
	failure                 *sqlFailure
	value                   any
	dispatched              bool
	session                 *gatewaySession
	finish                  func()
	stream                  []byte
	permit                  *admissionPermit
	rpc                     *SQLRPCError
	operationID             string
	operationStage          string
	operationDatabase       controller.DatabaseID
	operationPending        bool
	operationDeferred       *pendingOperation
	operationObservation    *OperationStatus
	auditExecutionCompleted bool
	committing              bool
}

// pendingOperation carries everything the single terminal record of an
// unregistered operation needs. Non-committing operations skip the durable
// intent, so their record is written once, after the worker call.
type pendingOperation struct {
	database  controller.DatabaseID
	requestID string
	action    string
	stage     string
	worker    string
	session   string
	digest    string
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	if config.State == nil || config.Placements == nil || config.Identity == nil || config.Upstream == nil || config.Audit == nil {
		return nil, errors.New("missing gateway dependency")
	}
	if config.WriterAdmission {
		if _, ok := config.Upstream.(WriterSQLUpstream); !ok {
			return nil, errors.New("writer admission requires versioned SQL upstream")
		}
	}
	state, err := config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: config.WriterAdmission}
	retainedSessions := map[string]bool{}
	holds := map[string]*admissionPermit{}
	// A quarantine that survived a restart was first watched when this process
	// started, which is the age the cleanup account can honestly report.
	isolated := map[string]bool{}
	for key, record := range state.Operations {
		if record.State == "quarantined" && record.ReleaseRequestID == "" {
			isolated[key] = true
		}
	}
	for _, record := range state.Operations {
		if record.State == "terminal" || record.LocalResourcesRetired {
			continue
		}
		project := gate.projects[record.Project]
		if project == nil {
			project = &projectAdmission{}
			gate.projects[record.Project] = project
		}
		project.unknown++
		// A durable unresolved close keeps its unknown and its session but not the
		// project's session-close lane, exactly as the running process leaves it:
		// the cleanup that resolves the unknown has to be able to reserve that
		// lane after a restart too.
		laneReleased := record.Action == "close"
		if !laneReleased {
			project.active++
			project.ownerActive++
		}
		resource := record.Project + "/" + record.Worker + "/" + record.Session
		if record.Session == "" {
			resource += record.OperationID
		}
		countSession := !retainedSessions[resource]
		if countSession {
			retainedSessions[resource] = true
			project.sessions++
		}
		holds[operationKey(record.Database, record.OperationID)] = &admissionPermit{gate: gate, project: record.Project, control: record.Action == "close", owner: record.Action != "close", session: countSession, retained: true, laneReleased: laneReleased}
	}
	ledger := &cleanupLedger{}
	ledger.observe(isolated, time.Now())
	return &Gateway{config: config, sessions: map[string]*gatewaySession{}, admission: gate, operationHolds: holds, cleanup: ledger, counters: &gatewayCounters{actions: map[string]uint64{}, errors: map[string]uint64{}, latency: map[string]uint64{}}}, nil
}

func gatewayFailure(status int, code string) gatewayResult {
	return gatewayResult{status: status, code: code}
}

func (gateway *Gateway) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("invalid shutdown context")
	}
	// The drain loops while a session stays locked by an in-flight handler or
	// while its control lane is occupied, so an unbounded context here is a
	// process that never exits and gets killed mid-teardown. Bound the loop even
	// when the caller hands over a context without a deadline.
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultServiceShutdownTimeout)
		defer cancel()
	}
	gateway.mu.Lock()
	gateway.closing = true
	hrana := gateway.hrana
	gateway.mu.Unlock()
	if hrana != nil {
		if err := hrana.closeWebSockets(ctx); err != nil {
			return err
		}
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, active, unknown := gateway.admission.status()
		if unknown > 0 {
			return errors.New("gateway has unresolved upstream outcomes; terminal proof required")
		}
		gateway.mu.Lock()
		sessions := make([]*gatewaySession, 0, len(gateway.sessions))
		for _, session := range gateway.sessions {
			sessions = append(sessions, session)
		}
		gateway.mu.Unlock()
		if len(sessions) == 0 && active == 0 {
			return nil
		}
		for _, session := range sessions {
			if !session.mu.TryLock() {
				continue
			}
			if session.poisoned {
				session.mu.Unlock()
				return errors.New("gateway has unresolved session outcome")
			}
			if session.closed {
				session.mu.Unlock()
				continue
			}
			state, err := gateway.config.State.Snapshot()
			if err != nil {
				session.mu.Unlock()
				return errors.New("gateway shutdown metadata unavailable")
			}
			// Isolation refuses new SQL, not the close of a session that is
			// already registered: the drain has to be able to retire the
			// sessions it still holds, or the process ends while a namespace it
			// could have cleaned stays owed.
			permit, ok := gateway.admission.reserve(session.project, state.Projects[session.project].Quotas, false, true)
			if !ok {
				session.mu.Unlock()
				continue
			}
			identifier, err := controller.RandomID()
			if err == nil {
				err = gateway.closeTrackedSession(ctx, session, identifier, permit)
			}
			if err != nil {
				var failure *SQLRPCError
				if errors.As(err, &failure) && !failure.Ambiguous {
					permit.finish()
					session.mu.Unlock()
					return errors.New("gateway shutdown upstream unavailable before SQL close")
				}
				session.poisoned = true
				noteUnowned(session, time.Now())
				permit.retain()
				session.mu.Unlock()
				return errors.New("gateway shutdown outcome unknown")
			}
			gateway.removeSession(session)
			permit.finish()
			session.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (gateway *Gateway) removeSession(session *gatewaySession) {
	session.closed = true
	gateway.mu.Lock()
	delete(gateway.sessions, session.public)
	gateway.mu.Unlock()
	gateway.admission.removeSession(session.project)
}

var _ http.Handler = (*Gateway)(nil)
