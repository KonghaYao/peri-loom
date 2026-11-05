package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	controller "peri-loom/controller"
)

// placementSettleTimeout bounds how long an open waits for the controller to
// converge a placement it owns. It is a variable so a test can shorten the wait;
// production keeps it well below the public request deadline.
var placementSettleTimeout = 5 * time.Second

// placementSettlePoll is the re-route interval while a placement settles. The
// metadata read is cheap and the window is short, so polling beats a second
// notification channel that could disagree with the store.
const placementSettlePoll = 50 * time.Millisecond

// settlePlacement routes once and, while the refusal says the controller is
// expected to converge, retries the route until the wait budget or the request
// context ends. A non-transient refusal, a gateway without a controller, and an
// exhausted budget all answer with the last refusal instead of guessing.
func (gateway *Gateway) settlePlacement(ctx context.Context, database controller.DatabaseID) (controller.Worker, controller.Binding, error) {
	worker, binding, err := gateway.route(database)
	if gateway.config.Controller == nil {
		return worker, binding, err
	}
	deadline := time.Now().Add(placementSettleTimeout)
	if contextDeadline, bounded := ctx.Deadline(); bounded && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	for {
		var refusal *placementRefusal
		if err == nil || !errors.As(err, &refusal) || !refusal.transient {
			return worker, binding, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return worker, binding, err
		}
		timer := time.NewTimer(min(placementSettlePoll, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return worker, binding, err
		case <-timer.C:
		}
		worker, binding, err = gateway.route(database)
	}
}

// beginSessionOpen prepares the record an open keeps. An open submits no SQL, so it
// needs no durable intent: a crash between the call and the reply leaves nothing to
// reconcile, and the worker session it may have created is reclaimed by the worker's
// own idle sweep. Its record is written once, after the call, and is the cleanup the
// later close discharges. Only the admission rule still runs before dispatch, so an
// isolated database or an exhausted operation capacity refuses the request before any
// SQL reaches the worker.
func (gateway *Gateway) beginSessionOpen(database controller.DatabaseID, requestID, worker string, command frozenCommand) (string, *pendingOperation, error) {
	if err := verifyNonCommittingAdmission(gateway.config.State, database, "gateway", "open", "open_session"); err != nil {
		return "", nil, err
	}
	return "", pendingOutcomeOperation(database, worker, "", "open", "open_session", requestID, command), nil
}

func (gateway *Gateway) open(ctx context.Context, principal Principal, database controller.DatabaseID, requestID string, permit *admissionPermit, quota Quotas) (result gatewayResult) {
	if gateway.authorizeIn(ctx, principal, database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	// 持久隔离的原因优先于 placement 故障，避免客户端把 Unknown 误当成可重试的路由问题。
	if err := readOperationState(gateway.config.State, readGatewayAdmission, func(state State) error {
		if _, isolated := isolatedOperation(state, database, false); isolated {
			return ErrDatabaseQuarantined
		}
		return nil
	}); err != nil {
		return gateway.admissionFailure(database, err)
	}
	worker, binding, err := gateway.route(database)
	if err != nil {
		// Authorization above is the gate: only an authorized request may spend
		// activation budget, and only an idle release (never an administrator
		// suspension) is eligible.
		if waiting := gateway.awaitActivation(ctx, database); waiting != nil {
			return *waiting
		}
		// The wait may have outlasted a revocation, so the check below reads state again.
		invalidateAuthorizationScope(ctx)
		worker, binding, err = gateway.settlePlacement(ctx, database)
	}
	if err != nil {
		return gateway.placementFailure(err)
	}
	if !principalBindingValid(principal, binding) {
		return gatewayFailure(403, "token_binding_mismatch")
	}
	if principal.Role == "read_only" && binding.Role != "read_only" {
		return gatewayFailure(403, "readonly_placement_required")
	}
	if gateway.authorizeIn(ctx, principal, database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	writerProtocol := gateway.admission.eligible(quota)
	// The Hrana adapter may ask for the first statement to ride along with this
	// open. A remembered committing statement is never fused: the worker would
	// run it read-only before the platform's durable intent exists, and the
	// committing path would then send it a second time.
	fusedStatement, fusedRequested := fusedOpenStatementFrom(ctx)
	fusedRequested = fusedRequested && fusedStatement.SQL != "" && !writerProtocol &&
		quota.MaxSQLBytes > 0 && len(fusedStatement.SQL) <= quota.MaxSQLBytes &&
		!gateway.committingStatements.remembered(committingStatementKey{worker: worker.ID, digest: digestHex([]byte(fusedStatement.SQL))})
	fusedLevel := 0
	if fusedRequested {
		fusedLevel = gateway.fusedOpenLevel(ctx, worker)
	}
	fusedAttempt := fusedLevel >= 1
	// The close joins the open only when the adapter knows it is the next entry
	// and the worker advertises that level. A level-1 worker keeps the first
	// fusion and answers the close separately.
	closeAfter := fusedAttempt && fusedStatement.CloseAfter && fusedLevel >= 2
	fusedStatement.CloseAfter = closeAfter
	plainOpenCommand := map[string]any{"command": "open_session", "database": database}
	openFields := plainOpenCommand
	if fusedAttempt {
		openFields = fusedOpenCommand(database, fusedStatement)
	}
	openCommand, err := freezeCommand(openFields)
	if err != nil {
		return gateway.admissionFailure(database, err)
	}
	leaveIntent := requestPhaseSpan(ctx, requestPhaseIntent)
	operationID, pending, err := gateway.beginSessionOpen(database, requestID, worker.ID, openCommand)
	leaveIntent()
	if err != nil {
		return gateway.admissionFailure(database, err)
	}
	defer func() {
		result.operationID, result.operationStage, result.operationDatabase = operationID, "open_session", database
		result.operationPending, result.operationDeferred = operationID != "", pending
	}()
	var reply struct {
		Session   string          `json:"session"`
		Readonly  *bool           `json:"readonly"`
		Statement json.RawMessage `json:"statement"`
		Close     json.RawMessage `json:"close"`
	}
	// The worker span covers the open call on both protocol branches; it is deferred so every
	// refusal below still attributes its time to the worker.
	var writerState *SessionWriterState
	leaveWorker := requestPhaseSpan(ctx, requestPhaseWorker)
	defer leaveWorker()
	if writerProtocol {
		probeID, probeErr := controller.RandomID()
		if probeErr != nil {
			return gatewayFailure(503, "upstream_unavailable")
		}
		if probeErr = gateway.config.Upstream.(WriterSQLUpstream).WriterProfile(ctx, worker, probeID); probeErr != nil {
			return gatewayFailure(503, "writer_profile_unavailable")
		}
		currentWorker, currentBinding, routeErr := gateway.route(database)
		if routeErr != nil {
			return gateway.placementFailure(routeErr)
		}
		if !sameGatewayBinding(binding, currentBinding) || worker.Address != currentWorker.Address {
			// The binding moved between routing and the open call, so no SQL was
			// dispatched and a retry is safe; the caller must open a new session
			// against the binding the platform now routes.
			return gatewayRetryablePlacementFailure()
		}
		// The profile call is an upstream RPC, so it can outlast a revocation.
		invalidateAuthorizationScope(ctx)
		if !principalBindingValid(principal, currentBinding) || gateway.authorizeIn(ctx, principal, database) != nil {
			return gatewayFailure(403, "forbidden")
		}
		binding = currentBinding
		ctx = bindSQLOpenObservation(ctx, gateway.config.OpenObserver, requestID)
		leaveNative := requestPhaseSpan(ctx, requestPhaseNative)
		writerState, err = gateway.config.Upstream.(WriterSQLUpstream).CallWriter(ctx, worker, requestID, openCommand, &reply)
		leaveNative()
	} else {
		ctx = bindSQLOpenObservation(ctx, gateway.config.OpenObserver, requestID)
		leaveNative := requestPhaseSpan(ctx, requestPhaseNative)
		err = gateway.callOpenSession(ctx, database, worker, requestID, openCommand, &reply)
		leaveNative()
		if err != nil && fusedAttempt {
			var failure *SQLRPCError
			if errors.As(err, &failure) && failure.unsupportedCommand {
				// An older worker rejects the private command before it reads a
				// session, so nothing opened and no statement ran. Rebuild the
				// plain open (and its pending record) and send that instead.
				gateway.markFusedOpenUnsupported(worker)
				fusedAttempt = false
				closeAfter = false
				openCommand, err = freezeCommand(plainOpenCommand)
				if err == nil {
					leaveIntent := requestPhaseSpan(ctx, requestPhaseIntent)
					operationID, pending, err = gateway.beginSessionOpen(database, requestID, worker.ID, openCommand)
					leaveIntent()
				}
				if err != nil {
					return gateway.admissionFailure(database, err)
				}
				leaveNative := requestPhaseSpan(ctx, requestPhaseNative)
				err = gateway.callOpenSession(ctx, database, worker, requestID, openCommand, &reply)
				leaveNative()
			}
		}
	}
	if err != nil {
		return observeSQLOpenResult(ctx, requestID, sqlOpenUpstreamResult, gateway.upstreamFailure(err, nil, permit), err)
	}
	if !upstreamSessionID(reply.Session) || reply.Readonly == nil || *reply.Readonly != (binding.Role == "read_only") {
		return observeSQLOpenResult(ctx, requestID, sqlOpenReply, gateway.unknown(nil, permit), nil)
	}
	var fusedReply json.RawMessage
	fusedClose := false
	var fusedAnswer fusedStatementAnswer
	fusedAnswerValid := false
	if fusedAttempt {
		// The statement already ran, so an answer this platform cannot read is
		// not a reason to send it again. It is also not a reason to isolate the
		// namespace: the worker only runs statements its engine proved read-only
		// in this request, so the lost answer cannot hide a commit.
		answer, valid := parseFusedStatementAnswer(reply.Statement)
		if !valid {
			return observeSQLOpenResult(ctx, requestID, sqlOpenReply, gateway.unknown(nil, permit), nil)
		}
		fusedReply = reply.Statement
		fusedAnswer, fusedAnswerValid = answer, true
		// A close is only usable next to a settled statement answer, and an
		// answer this platform cannot read leaves the session for the ordinary
		// close entry instead of guessing that the worker retired it.
		if closeAfter && reply.Close != nil && fusedCloseEligible(answer) {
			if closed, valid := parseFusedCloseAnswer(reply.Close); valid && closed {
				fusedClose = true
			}
		}
	}
	identifier, err := controller.RandomID()
	if err != nil {
		return observeSQLOpenResult(ctx, requestID, sqlOpenPublicID, gateway.unknown(nil, permit), nil)
	}
	session := &gatewaySession{public: identifier, upstream: reply.Session, worker: worker, binding: binding, database: database, project: principal.Project, subject: principal.Subject, keyID: principal.KeyID, writerProtocol: writerProtocol, hrana: ctx.Value(hranaSessionContextKey{}) != nil, lastUsed: time.Now(), fusedClose: fusedClose, pristine: true}
	if fusedAnswerValid {
		// A fused statement ran inside the open, so its own autocommit flag is
		// the session's first transaction fact. A refusal or a failure carries
		// no such proof and keeps the close synchronous.
		switch fusedAnswer.kind {
		case fusedStatementOK:
			session.noteStatementAutocommit(fusedAnswer.autocommit, fusedAnswer.autocommitKnown)
		default:
			session.pristine = false
		}
	}
	if writerProtocol && (!session.acceptWriterState(writerState, binding) || writerState.Owner) {
		return observeSQLOpenResult(ctx, requestID, sqlOpenWriterBinding, gateway.unknown(nil, permit), nil)
	}
	session.mu.Lock()
	gateway.mu.Lock()
	if _, exists := gateway.sessions[identifier]; exists {
		gateway.mu.Unlock()
		session.mu.Unlock()
		return observeSQLOpenResult(ctx, requestID, sqlOpenCollision, gateway.unknown(nil, permit), nil)
	}
	gateway.sessions[identifier] = session
	gateway.mu.Unlock()
	permit.keepSession = true
	value := map[string]any{"session": identifier, "readonly": *reply.Readonly}
	if fusedReply != nil {
		// Private to the in-process adapter that asked for the fused open; the
		// public open result still carries exactly session and readonly.
		value["fused_statement"] = fusedReply
	}
	if fusedClose {
		// The adapter still sends its close entry for the platform's own records,
		// but it knows the worker session is already gone.
		value["fused_close"] = true
	}
	// Only the fused-open request carries a typed consumer on the other side:
	// the pipeline adapter asked for the statement to ride along and consumes
	// this answer directly instead of decoding the envelope it would otherwise
	// have re-encoded. The cursor and websocket adapters still decode the
	// envelope, so they keep the encoded reply.
	var hranaOpen *hranaOpenReply
	if _, requested := fusedOpenStatementFrom(ctx); requested {
		hranaOpen = &hranaOpenReply{Session: identifier, FusedStatement: fusedReply, FusedClose: fusedClose, Answer: fusedAnswer, AnswerValid: fusedAnswerValid}
	}
	return gatewayResult{value: value, hranaOpen: hranaOpen, dispatched: true, session: session, finish: func() {
		session.lastUsed = time.Now()
		session.mu.Unlock()
	}}
}

func (gateway *Gateway) dispatch(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool) gatewayResult {
	leaveSession := requestPhaseSpan(ctx, requestPhaseSession)
	gateway.mu.Lock()
	session := gateway.sessions[request.Session]
	gateway.mu.Unlock()
	leaveSession()
	if session == nil || session.subject != principal.Subject || session.project != principal.Project || session.keyID != principal.KeyID {
		return gatewayFailure(403, "invalid_session")
	}
	if !session.mu.TryLock() {
		return gatewayFailure(409, "session_busy")
	}
	if permit == nil {
		if failure := gateway.writerDispatchGuard(ctx, principal, session, action); failure.code != "" {
			session.mu.Unlock()
			return failure
		}
		var admissionReason rejectionReason
		if action == "close" {
			permit, admissionReason = gateway.admission.reserve(principal.Project, quota, false, true)
		} else {
			permit, admissionReason = gateway.admission.reserveWriter(principal.Project, quota, session.writerProtocol && session.writerKnown && session.writerOwner)
		}
		if admissionReason != rejectionNone {
			session.mu.Unlock()
			return gatewayRefusal(429, "quota_exceeded", admissionReason)
		}
	}
	result := gateway.dispatchLocked(ctx, principal, request, requestID, action, quota, permit, ndjson, session)
	result.permit = permit
	result.session = session
	result.finish = func() {
		session.lastUsed = time.Now()
		session.mu.Unlock()
	}
	return result
}

func (gateway *Gateway) dispatchLocked(ctx context.Context, principal Principal, request gatewayRequest, requestID, action string, quota Quotas, permit *admissionPermit, ndjson bool, session *gatewaySession) (result gatewayResult) {
	currentBinding := session.binding
	// Authorization is verified again after routing, before the worker call.
	if gateway.authorizeIn(ctx, principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	if session.closed {
		return gatewayFailure(403, "invalid_session")
	}
	if session.poisoned {
		return gateway.isolatedSessionFailure(session.database)
	}
	// The placement store is read once per dispatch and the same snapshot backs
	// every binding check below: re-reading it only queues the request behind
	// placement writers, and both checks must agree on one snapshot to be
	// meaningful.
	var (
		worker   controller.Worker
		binding  controller.Binding
		routed   bool
		routeErr error
	)
	lookup := func() error {
		if !routed {
			routed = true
			worker, binding, routeErr = gateway.route(session.database)
		}
		return routeErr
	}
	if principal.Database != nil {
		if err := lookup(); err != nil {
			return gateway.placementFailure(err)
		}
		if !principalBindingValid(principal, binding) {
			return gatewayFailure(403, "token_binding_mismatch")
		}
	}
	if action != "close" {
		if session.moved {
			return gatewayFailure(409, "session_moved")
		}
		if err := lookup(); err != nil {
			return gateway.placementFailure(err)
		}
		if !sameGatewayBinding(session.binding, binding) || worker.Address != session.worker.Address {
			session.moved = true
			return gatewayFailure(409, "session_moved")
		}
		currentBinding = binding
		if principal.Role == "read_only" && binding.Role != "read_only" {
			return gatewayFailure(403, "readonly_placement_required")
		}
	}
	if gateway.authorizeIn(ctx, principal, session.database) != nil {
		return gatewayFailure(403, "forbidden")
	}
	// The worker command of this dispatch is assembled here, completely, before
	// anything durable names it: the record registered below binds the bytes this
	// switch produces, so no later branch may rewrite a field the digest covers.
	command := map[string]any{"command": action, "session": session.upstream}
	switch action {
	case "cursor_open", "cursor_start":
		command["command"] = "open_read_cursor"
		if request.WriteCursor {
			command["command"] = "open_write_cursor"
		}
		if action == "cursor_start" {
			command["command"] = command["command"].(string) + "_first"
			command["max_rows"] = request.MaxRows
		}
		args := request.Args
		if args == nil {
			args = []sqlArgument{}
		}
		namedArgs := request.NamedArgs
		if namedArgs == nil {
			namedArgs = []namedSQLArgument{}
		}
		command["args"] = args
		command["named_args"] = namedArgs
	case "cursor_fetch":
		command["command"] = "fetch_read_cursor"
		if request.WriteCursor {
			command["command"] = "fetch_write_cursor"
		}
		command["max_rows"] = request.MaxRows
	case "cursor_close":
		command["command"] = "close_read_cursor"
		if request.WriteCursor {
			command["command"] = "close_write_cursor"
		}
	case "close":
		command["command"] = "close_session"
	case "session_state":
		command["command"] = "session_autocommit"
	case "statement":
		args := request.Args
		if args == nil {
			args = []sqlArgument{}
		}
		namedArgs := request.NamedArgs
		if namedArgs == nil {
			namedArgs = []namedSQLArgument{}
		}
		command["args"] = args
		command["named_args"] = namedArgs
		// The row projection is always materialized as its effective value: the
		// worker reads an absent want_rows as true, so omitting it would let one
		// logical command carry two digests.
		command["want_rows"] = request.WantRows == nil || *request.WantRows
	}
	stage := "tool_rpc"
	if action == "close" {
		stage = "close_session"
	} else if action == "execute" || action == "query" || action == "statement" || action == "describe" || action == "cursor_open" || action == "cursor_start" {
		command["sql"] = request.SQL
	}
	frozen, err := freezeCommand(command)
	if err != nil {
		return operationAdmissionFailure(err)
	}
	// A remembered write only adds an intent; it never grants permission to
	// skip one. Every other statement still proves read-only admission on the
	// same prepared statement before each step, including automatic reprepare.
	if action == "statement" {
		key := committingStatementKey{worker: session.worker.ID, digest: digestHex([]byte(request.SQL))}
		// The successful open may already carry this entry's answer. The marker
		// names the public session and the statement it belongs to, so a fused
		// answer can never be spent on a different entry.
		if fused, ok := fusedStatementResultFrom(ctx); ok && fused.Session == session.public && fused.SQL == request.SQL {
			return gateway.fusedGuardedStatement(ctx, session, currentBinding, requestID, stage, key, frozen, fused, permit, quota)
		}
		// A planned block already decided this entry when its first entry sent
		// the block: consuming that item avoids a second worker call, while the
		// authorization, placement, admission, permit and completion checks
		// above and after this point still run for the entry. An entry the
		// block did not reach runs the single-statement path from here.
		if step, planned := guardedBatchStepFrom(ctx); planned {
			if result, handled := gateway.guardedBatchStatement(ctx, step, key, session, currentBinding, requestID, stage, request.SQL, frozen, permit, quota); handled {
				return result
			}
		}
		if gateway.committingStatements.remembered(key) {
			return gateway.committingStatement(ctx, session, currentBinding, requestID, stage, frozen, permit, quota)
		}
		return gateway.guardedStatement(ctx, session, currentBinding, requestID, stage, key, command, frozen, permit, quota)
	}
	// A platform probe cannot commit and is not a client action, so it takes no
	// operation record; the client request that needed the answer records its
	// own. The read-only admission check still refuses an isolated database.
	if hranaProbeRequest(ctx, action) {
		if err := verifyNonCommittingAdmission(gateway.config.State, session.database, "gateway", action, stage); err != nil {
			return gateway.admissionFailure(session.database, err)
		}
	} else {
		// A statement the worker proves read-only cannot commit, so it needs no
		// durable intent; the terminal record of the dispatch is then its only record.
		var classify func() bool
		if readonlyStatementAction(action) && !request.WriteCursor {
			classify = func() bool { return gateway.readonlyStatement(ctx, session, currentBinding, request.SQL) }
		}
		leaveIntent := requestPhaseSpan(ctx, requestPhaseIntent)
		operationID, pending, err := gateway.beginDispatchOperation(session, action, stage, requestID, request.WriteCursor, frozen, classify)
		leaveIntent()
		if err != nil {
			return gateway.admissionFailure(session.database, err)
		}
		defer func() {
			result.operationID, result.operationStage, result.operationDatabase = operationID, stage, session.database
			result.operationPending, result.operationDeferred = operationID != "", pending
		}()
	}
	if action == "close" {
		if session.fusedClose {
			// The fused open already retired the worker session. The close entry
			// still runs every platform-side step - admission, the close record,
			// quota and retirement - but the worker call it would make names a
			// session that no longer exists, so it is skipped.
			session.fusedClose = false
			gateway.removeSession(session)
			return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
		}
		deferred := gateway.deferredClose != nil && session.deferredCloseEligible()
		removed := false
		if deferred {
			// A proven stateless session gives its public identity and admission
			// seat back before the worker is told: the client cannot address it
			// again, and a session that never ran anything that could commit has
			// no worker-side result the reply has to wait for. The account retries
			// a failed worker close through the cleanup ledger; the worker's own
			// idle sweep is the final backstop.
			gateway.removeSession(session)
			removed = true
			attempt := func(parent context.Context) error {
				attemptCtx, cancel := context.WithTimeout(parent, gateway.deferredSessionCloseAttemptTimeout())
				defer cancel()
				var answered struct {
					Closed bool `json:"closed"`
				}
				if err := gateway.callSession(attemptCtx, session, currentBinding, requestID, frozen, &answered); err != nil {
					return err
				}
				if !answered.Closed {
					return errDeferredSessionCloseNotProven
				}
				return nil
			}
			if gateway.deferredClose.enqueue(session, attempt) {
				return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
			}
			// The account is full or stopped, so the reply pays the worker wait
			// inline exactly as it did before the deferral. A bound is latency,
			// never a refusal.
		}
		var reply struct {
			Closed bool `json:"closed"`
		}
		err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Closed {
			return gateway.unknown(session, permit)
		}
		if !removed {
			gateway.removeSession(session)
		}
		return gatewayResult{value: map[string]bool{"closed": true}, dispatched: true}
	}
	if action == "cursor_open" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			session.pristine = false
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes || !validCursorOpenResult(reply) {
			return gateway.unknown(session, permit)
		}
		if request.WriteCursor {
			// A write cursor may commit, so the session keeps the synchronous
			// close regardless of how the cursor ends.
			session.pristine = false
			session.writeCursorOpen, session.writeCursorCommitted = true, false
		} else {
			session.readCursorOpen = true
		}
		return gatewayResult{value: reply, dispatched: true, validatedResult: true}
	}
	if action == "cursor_fetch" || action == "cursor_start" {
		if !request.WriteCursor {
			// A read page is decoded and structurally indexed once, by sqlDecode;
			// the result is the page itself, so no second validation scan follows.
			var pageReply hranaCursorPageReply
			pageReply.allowMetadata = action == "cursor_start"
			if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &pageReply); err != nil {
				return gateway.upstreamFailure(err, session, permit)
			}
			if len(pageReply.page.source) > quota.MaxResultBytes || pageReply.page.rows > request.MaxRows || action == "cursor_start" && !validCursorFirstPage(pageReply.page) {
				return gateway.unknown(session, permit)
			}
			// A done page leaves no cursor behind; an unfinished page does, and
			// the session keeps its synchronous close until it is closed.
			session.readCursorOpen = !pageReply.page.done
			return gatewayResult{value: json.RawMessage(pageReply.page.source), dispatched: true, validatedResult: true, cursorPage: &pageReply.page}
		}
		var reply json.RawMessage
		// A write cursor action can commit, so the session keeps the synchronous
		// close even when the reply turns out to be a refusal.
		session.pristine = false
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			result := gateway.upstreamFailure(err, session, permit)
			if request.WriteCursor && result.code == "outcome_unknown" {
				result.cursorNativeOutcome = cursorOutcomeUnknown
			}
			return result
		}
		valid := validCursorFetchResponse(reply, request.MaxRows, request.WriteCursor)
		if action == "cursor_start" {
			valid = validCursorFirstResponse(reply, request.MaxRows, request.WriteCursor)
		}
		if len(reply) > quota.MaxResultBytes || !valid {
			failure := gateway.unknown(session, permit)
			if request.WriteCursor {
				failure.cursorNativeOutcome = cursorOutcomeUnknown
			}
			return failure
		}
		if request.WriteCursor {
			if action == "cursor_start" {
				session.writeCursorOpen, session.writeCursorCommitted = true, false
			}
			var page struct {
				Done bool `json:"done"`
			}
			if json.Unmarshal(reply, &page) != nil {
				failure := gateway.unknown(session, permit)
				failure.cursorNativeOutcome = cursorOutcomeUnknown
				return failure
			}
			if page.Done {
				// The worker's done page is the commit proof. Record it before the
				// adapter writes any row, so a disconnect during the final page
				// cannot be mistaken for an unproven write.
				session.writeCursorOpen, session.writeCursorCommitted = false, true
				return gatewayResult{value: reply, dispatched: true, validatedResult: true, cursorNativeOutcome: cursorOutcomeCommitted}
			}
		}
		return gatewayResult{value: reply, dispatched: true, validatedResult: true}
	}
	if action == "cursor_close" {
		lifecycle := gatewayResult{cursorClientStopped: request.CursorClientStopped, cursorDispatchStopped: request.CursorDispatchStopped}
		var reply struct {
			Closed      bool  `json:"closed"`
			AbortProven *bool `json:"abort_proven"`
		}
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			result := gateway.upstreamFailure(err, session, permit)
			if request.WriteCursor && result.code == "outcome_unknown" {
				result.cursorNativeOutcome = cursorOutcomeUnknown
			}
			result.cursorClientStopped, result.cursorDispatchStopped = lifecycle.cursorClientStopped, lifecycle.cursorDispatchStopped
			return result
		}
		if !reply.Closed {
			failure := gateway.unknown(session, permit)
			if request.WriteCursor {
				failure.cursorNativeOutcome = cursorOutcomeUnknown
			}
			failure.cursorClientStopped, failure.cursorDispatchStopped = lifecycle.cursorClientStopped, lifecycle.cursorDispatchStopped
			return failure
		}
		if request.WriteCursor {
			switch {
			case session.writeCursorCommitted:
				session.writeCursorOpen = false
				lifecycle.cursorNativeOutcome = cursorOutcomeCommitted
			case session.writeCursorOpen && reply.AbortProven != nil && *reply.AbortProven:
				session.writeCursorOpen = false
				lifecycle.cursorNativeOutcome = cursorOutcomeRolledBack
			case session.writeCursorOpen:
				// The worker had no cursor left to abort and no done page was ever
				// delivered. A lost fetch reply may have committed, so a successful
				// close is not a rollback proof; keep the write unknown and isolated.
				failure := gateway.unknown(session, permit)
				failure.cursorNativeOutcome = cursorOutcomeUnknown
				failure.cursorClientStopped, failure.cursorDispatchStopped = lifecycle.cursorClientStopped, lifecycle.cursorDispatchStopped
				return failure
			}
		} else {
			session.readCursorOpen = false
		}
		lifecycle.value, lifecycle.dispatched = map[string]bool{"closed": true}, true
		return lifecycle
	}
	if action == "describe" {
		var reply json.RawMessage
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if len(reply) > quota.MaxResultBytes {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget}
		}
		if !validDescribeResult(reply) {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: reply, dispatched: true, validatedResult: true}
	}
	if action == "session_state" {
		var reply struct {
			Autocommit *bool `json:"autocommit"`
		}
		if err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply); err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if reply.Autocommit == nil {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"autocommit": *reply.Autocommit}, dispatched: true}
	}
	if action == "statement" {
		return gateway.dispatchStatement(ctx, session, currentBinding, requestID, frozen, quota, permit)
	}
	if action == "execute" {
		var reply struct {
			Executed bool `json:"executed"`
		}
		err := gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
		// The execute answer carries no autocommit flag, so this session can no
		// longer prove it is stateless; the close keeps its worker wait.
		session.pristine = false
		if err != nil {
			return gateway.upstreamFailure(err, session, permit)
		}
		if !reply.Executed {
			return gateway.unknown(session, permit)
		}
		return gatewayResult{value: map[string]bool{"executed": true}, dispatched: true}
	}
	var reply json.RawMessage
	err = gateway.callSession(ctx, session, currentBinding, requestID, frozen, &reply)
	// A query answer carries no autocommit flag either.
	session.pristine = false
	if err != nil {
		return gateway.upstreamFailure(err, session, permit)
	}
	if len(reply) > quota.MaxResultBytes {
		return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget}
	}
	var rows [][]json.RawMessage
	if len(reply) == 0 || reply[0] != '[' || json.Unmarshal(reply, &rows) != nil {
		return gateway.unknown(session, permit)
	}
	if ndjson {
		stream, err := gatewayRows(requestID, rows, quota.MaxResultBytes)
		if err != nil {
			return gatewayResult{status: 413, code: "result_limit", dispatched: true, rejection: rejectionMemoryBudget}
		}
		return gatewayResult{dispatched: true, stream: stream}
	}
	return gatewayResult{value: reply, dispatched: true, validatedResult: true}
}

func (gateway *Gateway) unknown(session *gatewaySession, permit *admissionPermit) gatewayResult {
	permit.retain()
	if session != nil {
		session.poisoned = true
		noteUnowned(session, time.Now())
	}
	return gatewayResult{status: 502, code: "outcome_unknown", dispatched: true, session: session}
}

// logUnknownOutcome records the transition that poisons a session and isolates
// its namespace. The 502 the caller sees lives for one request, and the refusals
// that follow are the same isolation seen again, so without this line a single
// unproven reply leaves an isolated namespace and no trace in the process log.
// The caller holds the session lock that the result's finish releases.
func logUnknownOutcome(requestID, action string, result *gatewayResult) {
	database, session, worker := "", "", ""
	switch {
	case result.session != nil:
		database, session, worker = result.session.database.Key(), result.session.public, result.session.worker.ID
	case result.operationDatabase.Namespace.Tenant != "":
		database = result.operationDatabase.Key()
	}
	// The worker's own verdict is what separates "it never entered execution" from
	// "it may have committed", and its failure text is the only account of why the
	// reply could not be used: the line that outlives the request is where an
	// operator reconstructs both, so an absent verdict says so instead of leaving
	// the fields empty.
	rpc, observation := "none", "none"
	if result.rpc != nil {
		rpc = fmt.Sprintf("ambiguous=%t,rejected=%t,execution_completed=%t", result.rpc.Ambiguous, result.rpc.evidence.Rejected(), result.rpc.evidence.Executed())
		switch {
		case result.rpc.workerFailure != "":
			observation = workerFailureLog(result.rpc.workerFailure)
		case result.rpc.cause != nil:
			observation = workerFailureLog(result.rpc.cause.Error())
		}
	}
	log.Printf("SQL outcome unknown: request_id=%s action=%s stage=%s database=%s session=%s worker=%s rpc=%s observation=%s",
		requestID, action, result.operationStage, database, session, worker, rpc, observation)
}

func (gateway *Gateway) upstreamFailure(err error, session *gatewaySession, permit *admissionPermit) (result gatewayResult) {
	var failure *SQLRPCError
	defer func() { result.rpc = failure }()
	if !errors.As(err, &failure) || failure.Ambiguous {
		return gateway.unknown(session, permit)
	}
	worker, found := sqlWorkerFailure(failure)
	if !found {
		if failure.evidence.Rejected() {
			return gatewayFailure(502, "upstream_rejected")
		}
		return gatewayFailure(503, "upstream_unavailable")
	}
	// The worker text is the operator's evidence; the caller only sees the
	// classified code and message.
	log.Printf("SQL worker failure: code=%s status=%d executed=%t rejected=%t worker_error=%s", worker.code, worker.status, worker.executed, worker.rejected, workerFailureLog(failure.workerFailure))
	return gatewayResult{status: worker.status, code: worker.code, failure: &worker, dispatched: true, session: session, rejection: workerRejection(worker)}
}

// workerRejection names the layer behind a worker failure whose own reply proves
// the statement was turned away. The platform cannot see inside the worker, so it
// only names a layer where the proof is unambiguous: a deadline is a refusal only
// when the worker proved the statement never entered execution, because a
// deadline after execution is a settled statement and must not be reported as
// something the platform refused. A refused statement with no recognised cause
// keeps its code and stays unnamed rather than being filed under a layer the
// evidence does not support.
func workerRejection(worker sqlFailure) rejectionReason {
	switch worker.code {
	case "sql_timeout":
		if worker.rejected {
			return rejectionDeadline
		}
	case "result_limit":
		return rejectionMemoryBudget
	case "sql_limit":
		return rejectionSQLBytes
	case "sql_conflict":
		if worker.rejected {
			return rejectionWriterBusy
		}
	}
	return rejectionNone
}

func upstreamSessionID(identifier string) bool {
	if len(identifier) != 64 {
		return false
	}
	for _, character := range identifier {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
