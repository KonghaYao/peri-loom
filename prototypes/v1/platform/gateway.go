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
	poisoned       bool
	moved          bool
	closed         bool
	writerProtocol bool
	writerKnown    bool
	writerOwner    bool
	writerSequence uint64
}

type gatewayResult struct {
	status                  int
	code                    string
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
	operationObservation    *OperationStatus
	auditExecutionCompleted bool
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
		if record.Action == "close" {
			project.control++
		} else {
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
		holds[operationKey(record.Database, record.OperationID)] = &admissionPermit{gate: gate, project: record.Project, control: record.Action == "close", owner: record.Action != "close", session: countSession, retained: true}
	}
	return &Gateway{config: config, sessions: map[string]*gatewaySession{}, admission: gate, operationHolds: holds, counters: &gatewayCounters{actions: map[string]uint64{}, errors: map[string]uint64{}, latency: map[string]uint64{}}}, nil
}

func gatewayFailure(status int, code string) gatewayResult {
	return gatewayResult{status: status, code: code}
}

func (gateway *Gateway) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("invalid shutdown context")
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
			if _, blocked := isolatedOperation(state, session.database, true); blocked {
				session.mu.Unlock()
				return ErrDatabaseQuarantined
			}
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
