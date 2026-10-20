package platform

import (
	"errors"
	"reflect"

	controller "peri-loom/controller"
)

const operationCapacity = 4096
const operationKnownRetention = 256

// Cursor lifecycle outcomes. Only a native reply or the gateway's own delivered
// page may produce rolled_back/committed; a missing reply is unknown.
const (
	cursorOutcomeRolledBack = "rolled_back"
	cursorOutcomeCommitted  = "committed"
	cursorOutcomeUnknown    = "unknown"
)

var ErrDatabaseQuarantined = errors.New("database isolated; new database or fixture required")
var ErrOperationExists = errors.New("operation already recorded; replay forbidden")
var ErrOperationCapacity = errors.New("durable operation capacity exhausted")

type OperationStatus struct {
	OperationID           string                `json:"operation_id"`
	RequestID             string                `json:"request_id"`
	Database              controller.DatabaseID `json:"database"`
	Project               string                `json:"project"`
	Worker                string                `json:"worker_id"`
	Source                string                `json:"source"`
	Action                string                `json:"action"`
	Stage                 string                `json:"stage"`
	State                 string                `json:"state"`
	BusinessOutcome       string                `json:"business_outcome"`
	CleanupOutcome        string                `json:"cleanup_outcome"`
	Ambiguous             bool                  `json:"ambiguous"`
	Rejected              bool                  `json:"rejected"`
	ExecutionCompleted    bool                  `json:"execution_completed"`
	ExecutionProven       bool                  `json:"execution_proven"`
	MayHaveCommitted      bool                  `json:"may_have_committed"`
	RetryAllowed          bool                  `json:"retry_allowed"`
	Decision              string                `json:"decision,omitempty"`
	DecisionRequestID     string                `json:"decision_request_id,omitempty"`
	LocalResourcesRetired bool                  `json:"local_resources_retired"`
	RetirementRequestID   string                `json:"retirement_request_id,omitempty"`
	// A recorded release keeps the operation truthful (its outcome is still unknown
	// and may have committed) while ending the namespace isolation under durability
	// proof: every affected worker's owned stop plus the data plane's own attestation
	// that no worker holds the branch. ReleaseEpoch is the epoch the authority
	// reported at release time, kept for audit.
	ReleaseRequestID     string `json:"release_request_id,omitempty"`
	ReleaseAuthorityMode string `json:"release_authority_mode,omitempty"`
	ReleaseEpoch         uint64 `json:"release_epoch,omitempty"`
	// CursorClientStopped and CursorDispatchStopped are the Hrana adapter's own
	// report that it stopped reading and stopped dispatching fetches. They are
	// descriptive, never proof of a write outcome. CursorNativeOutcome is the
	// proven fate of a write cursor: rolled_back only when the worker
	// acknowledged an abort, committed only when the gateway delivered a done
	// page, unknown when neither fact was established.
	CursorClientStopped   bool   `json:"cursor_client_stopped,omitempty"`
	CursorDispatchStopped bool   `json:"cursor_dispatch_stopped,omitempty"`
	CursorNativeOutcome   string `json:"cursor_native_outcome,omitempty"`
}

type OperationRecord struct {
	OperationStatus
	// Digest is the identity of the frozen worker command this record authorizes,
	// written as logical_command_v1:<sha256>. A bare 64-hex value is the legacy
	// domain: the platform cannot prove which bytes it covered, so it is kept for
	// history and must never be compared with a recomputed digest.
	Digest               string `json:"digest"`
	Session              string `json:"session,omitempty"`
	RetirementPending    bool   `json:"retirement_pending,omitempty"`
	RegistrationSequence uint64 `json:"registration_sequence,omitempty"`
}

type OperationRetention struct {
	KnownTerminalLimit             int    `json:"known_terminal_limit"`
	UnresolvedLimit                int    `json:"unresolved_limit"`
	Order                          string `json:"order"`
	HistoryComplete                bool   `json:"history_complete"`
	MissingRecordProvesNotExecuted bool   `json:"missing_record_proves_not_executed"`
	MissingRecordAllowsReplay      bool   `json:"missing_record_allows_replay"`
	RetryAllowed                   bool   `json:"retry_allowed"`
}

func operationRetentionContract() OperationRetention {
	return OperationRetention{KnownTerminalLimit: operationKnownRetention, UnresolvedLimit: operationCapacity, Order: "registration_sequence_legacy_key_order"}
}

func operationKnownTerminal(record OperationRecord) bool {
	if record.State != "terminal" || record.Ambiguous || record.RetirementPending || record.LocalResourcesRetired || record.Decision != "" {
		return false
	}
	if record.CleanupOutcome != "completed" && record.CleanupOutcome != "not_applicable" {
		return false
	}
	return record.BusinessOutcome == "completed" && record.ExecutionProven || record.BusinessOutcome == "not_executed" || record.Source == "gateway" && record.Action == "close" && record.BusinessOutcome == "not_applicable" && record.CleanupOutcome == "completed"
}

func operationCounts(state State) (int, int) {
	known, unresolved := 0, 0
	for _, record := range state.Operations {
		if operationKnownTerminal(record) {
			known++
		} else {
			unresolved++
		}
	}
	return known, unresolved
}

func knownDatabaseDeletionPermitted(state State, database controller.DatabaseID) error {
	if _, blocked := isolatedOperation(state, database, true); blocked {
		return ErrDatabaseQuarantined
	}
	for _, record := range state.Operations {
		if record.Database == database && !operationKnownTerminal(record) {
			return ErrDatabaseQuarantined
		}
	}
	return nil
}

func removeKnownDatabaseOperations(state *State, database controller.DatabaseID) error {
	if err := knownDatabaseDeletionPermitted(*state, database); err != nil {
		return err
	}
	remove := []string{}
	for key, record := range state.Operations {
		if record.Database == database {
			remove = append(remove, key)
		}
	}
	if state.OperationEvictions > ^uint64(0)-uint64(len(remove)) {
		return ErrOperationCapacity
	}
	for _, key := range remove {
		delete(state.Operations, key)
	}
	state.OperationEvictions += uint64(len(remove))
	return nil
}

func updateOperationRecords(store StateStore, change func(*operationEdit) error) error {
	if change == nil {
		change = func(*operationEdit) error { return nil }
	}
	if narrow, ok := store.(interface {
		updateOperations(func(*operationEdit) error) error
	}); ok {
		return narrow.updateOperations(change)
	}
	return store.Update(func(state *State) error {
		edit := newOperationEdit(state, newOperationIndex(state.Operations))
		if err := change(edit); err != nil {
			edit.revert()
			return err
		}
		if err := edit.settleRetention(); err != nil {
			edit.revert()
			return err
		}
		// Test stores may not validate State, so enforce the same online checks.
		if err := edit.validate(); err != nil {
			edit.revert()
			return err
		}
		return nil
	})
}

func operationKey(database controller.DatabaseID, requestID string) string {
	return database.Key() + "/" + requestID
}

func isolatedOperation(state State, database controller.DatabaseID, includePending bool) (OperationStatus, bool) {
	var selected OperationStatus
	found := false
	for _, record := range state.Operations {
		// A released quarantine no longer blocks admission: the release was recorded
		// only after the owned workers stopped and the data plane attested that no
		// worker holds the branch, so the next activation runs on a fresh epoch. The
		// record itself stays quarantined and may_have_committed, so the history is
		// still visible.
		if record.Database.Namespace == database.Namespace && (record.State == "quarantined" && record.ReleaseRequestID == "" || record.State == "in_flight" && (includePending || record.Source == "management")) {
			if !found || record.OperationID < selected.OperationID {
				selected, found = record.OperationStatus, true
			}
		}
	}
	return selected, found
}

func operationNeedsRecovery(record OperationRecord) bool {
	return record.State == "in_flight" || record.State == "terminal" && record.Source == "gateway" && record.Action == "open" && record.CleanupOutcome == "pending"
}

// beginOperation records an operation under a caller-chosen identity. It is the
// entry point of hand-built fixtures, which pass whatever command shape they
// staged, and it freezes that shape through the same encoder dispatch uses.
func beginOperation(store StateStore, database controller.DatabaseID, requestID, source, action, stage, worker, session string, command any) error {
	frozen, err := freezeCommandValue(command)
	if err != nil {
		return errors.New("operation registration unavailable")
	}
	return beginCorrelatedOperation(store, database, requestID, requestID, source, action, stage, worker, session, frozen)
}

func registerOperation(store StateStore, database controller.DatabaseID, requestID, source, action, stage, worker, session string, command frozenCommand) (string, error) {
	operationID, err := controller.RandomID()
	if err != nil {
		return "", err
	}
	err = beginCorrelatedOperation(store, database, operationID, requestID, source, action, stage, worker, session, command)
	return operationID, err
}

// seedOperationRecord stores the in-flight record of a new operation and returns
// it, so a caller either leaves it open for settlement or settles it at once.
func seedOperationRecord(edit *operationEdit, database controller.DatabaseID, operationID, requestID, source, action, stage, worker, session, digest string) OperationRecord {
	registered, _ := edit.database(database.Key())
	return OperationRecord{OperationStatus: OperationStatus{OperationID: operationID, RequestID: requestID, Database: database, Project: registered.Project, Worker: worker, Source: source, Action: action, Stage: stage, State: "in_flight", BusinessOutcome: "not_proven", CleanupOutcome: "not_applicable"}, Digest: digest, Session: session, RegistrationSequence: edit.advance()}
}

// nonCommittingOperation reports whether a gateway action provably cannot
// modify data. Describe only prepares a statement, and the worker refuses
// non-read-only SQL on read cursors, so a read cursor can never commit.
// Operations in this set need no durable intent before dispatch: their single
// terminal record is the acknowledgement.
func nonCommittingOperation(action string, writeCursor bool) bool {
	switch action {
	case "describe", "session_state":
		return true
	case "cursor_open", "cursor_fetch", "cursor_close":
		return !writeCursor
	}
	return false
}

// readonlyStatementAction reports whether an action still sends a statement a
// worker describe could classify. A statement action is classified and executed
// by the guarded read-only RPC, so only the older execute/query paths ask.
func readonlyStatementAction(action string) bool {
	switch action {
	case "execute", "query":
		return true
	}
	return false
}

// operationAdmissionView is the projection the admission rule needs, so the committed
// read path and an in-flight edit run the same rule instead of two copies of it.
type operationAdmissionView interface {
	isolation(database controller.DatabaseID, includePending bool) (OperationStatus, bool)
	counts() (known, unresolved int)
	database(key string) (Database, bool)
	sequence() uint64
}

// stateAdmissionView adapts a committed view to the admission projection.
type stateAdmissionView struct{ state State }

func (view stateAdmissionView) isolation(database controller.DatabaseID, includePending bool) (OperationStatus, bool) {
	return isolatedOperation(view.state, database, includePending)
}

func (view stateAdmissionView) counts() (known, unresolved int) { return operationCounts(view.state) }

func (view stateAdmissionView) database(key string) (Database, bool) {
	database, exists := view.state.Databases[key]
	return database, exists
}

func (view stateAdmissionView) sequence() uint64 { return view.state.OperationSequence }

// retirementOperation reports the operations that only retire a session the
// platform already registered. Isolation stops new work from touching a branch
// whose outcome is unknown, but it must not stop the close of a session that is
// already open: that close is the platform's own cleanup, it carries no SQL, and
// refusing it would leave the session, its quota and its storage in place
// forever — the isolation would block the only path that resolves it.
func retirementOperation(action, stage string) bool {
	return action == "close" && stage == "close_session"
}

// operationAdmission is the admission rule shared by registration and by the
// pre-dispatch check of an unregistered operation: an isolated database and an
// exhausted capacity refuse every new record. A retirement operation is exempt
// from the isolation refusal only; every other action, and the capacity bound,
// keeps refusing.
func operationAdmission(view operationAdmissionView, database controller.DatabaseID, source string, retirement bool) error {
	if !retirement {
		if _, blocked := view.isolation(database, source == "management"); blocked {
			return ErrDatabaseQuarantined
		}
	}
	_, unresolved := view.counts()
	if _, exists := view.database(database.Key()); !exists || unresolved >= operationCapacity || view.sequence() == ^uint64(0) {
		return ErrOperationCapacity
	}
	return nil
}

func readOperationState(store StateStore, visit func(State) error) error {
	if narrow, ok := store.(interface {
		readOperations(func(State) error) error
	}); ok {
		return narrow.readOperations(visit)
	}
	state, err := store.Snapshot()
	if err != nil {
		return err
	}
	return visit(state)
}

// verifyNonCommittingAdmission refuses an unregistered operation the durable
// writer would reject, before SQL reaches the worker. The writer repeats every
// check inside its commit.
func verifyNonCommittingAdmission(store StateStore, database controller.DatabaseID, source string) error {
	if store == nil || database.Validate() != nil {
		return errors.New("operation registration unavailable")
	}
	return readOperationState(store, func(state State) error {
		return operationAdmission(stateAdmissionView{state: state}, database, source, false)
	})
}

// settleNonCommittingOperation writes the only durable record of an operation
// that cannot commit. The record is created already settled, so the request
// pays one sidecar commit instead of a registration plus a settlement.
func settleNonCommittingOperation(store StateStore, database controller.DatabaseID, requestID, source, action, stage, worker, session, digest string, change func(*OperationRecord)) (string, error) {
	operationID, err := controller.RandomID()
	if err != nil {
		return "", err
	}
	if store == nil || database.Validate() != nil || !sqlLabel(requestID) || !taggedCommandDigest(digest) {
		return operationID, errors.New("operation registration unavailable")
	}
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	err = updateOperationRecords(store, func(edit *operationEdit) error {
		if edit.has(operationKey(database, operationID)) {
			return ErrOperationExists
		}
		if err := operationAdmission(edit, database, source, false); err != nil {
			return err
		}
		record := seedOperationRecord(edit, database, operationID, requestID, source, action, stage, worker, session, digest)
		if change != nil {
			change(&record)
		}
		edit.put(operationKey(database, operationID), record)
		return nil
	})
	return operationID, err
}

// beginCorrelatedOperation registers the durable intent of one operation and
// binds it to the frozen command it authorizes. The digest comes from the frozen
// bytes, so the record cannot describe a command other than the one that will be
// sent after this returns.
func beginCorrelatedOperation(store StateStore, database controller.DatabaseID, operationID, requestID, source, action, stage, worker, session string, command frozenCommand) error {
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	digest := command.digest()
	if store == nil || database.Validate() != nil || !sqlLabel(requestID) || source == "management" && !labelPattern.MatchString(requestID) {
		return errors.New("operation registration unavailable")
	}
	return updateOperationRecords(store, func(edit *operationEdit) error {
		if edit.has(operationKey(database, operationID)) {
			return ErrOperationExists
		}
		if err := operationAdmission(edit, database, source, retirementOperation(action, stage)); err != nil {
			return err
		}
		edit.put(operationKey(database, operationID), seedOperationRecord(edit, database, operationID, requestID, source, action, stage, worker, session, digest))
		return nil
	})
}

func updateOperation(store StateStore, database controller.DatabaseID, requestID string, change func(*OperationRecord)) error {
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	return updateOperationRecords(store, func(edit *operationEdit) error {
		key := operationKey(database, requestID)
		record, exists := edit.record(key)
		if !exists || record.State != "in_flight" {
			return ErrOperationExists
		}
		change(&record)
		edit.put(key, record)
		return nil
	})
}

func finishOperation(store StateStore, database controller.DatabaseID, requestID string, failure error, businessCompleted bool) error {
	return updateOperation(store, database, requestID, func(record *OperationRecord) {
		if businessCompleted {
			record.BusinessOutcome = "completed"
			record.ExecutionProven = true
		}
		if failure == nil {
			record.State = "terminal"
			if record.CleanupOutcome != "not_applicable" {
				record.CleanupOutcome = "completed"
			}
			return
		}
		record.State = "quarantined"
		var rpc *SQLRPCError
		if errors.As(failure, &rpc) {
			record.Ambiguous, record.Rejected, record.ExecutionCompleted = rpc.Ambiguous, rpc.evidence.Rejected(), rpc.evidence.Executed()
		}
		record.MayHaveCommitted = true
		if record.Stage == "close_session" {
			record.CleanupOutcome = "unknown"
		} else {
			record.BusinessOutcome = "unknown"
		}
	})
}

func validateOperationRecord(record OperationRecord, key string, databases map[string]Database) error {
	validStage := record.Stage == "open_session" || record.Stage == "tool_rpc" || record.Stage == "close_session"
	validAction := record.Source == "management" && nativeDiagnosticAction("/v1/databases/"+record.Action) != "" || record.Source == "gateway" && (record.Action == "open" || record.Action == "execute" || record.Action == "query" || record.Action == "statement" || record.Action == "describe" || record.Action == "session_state" || record.Action == "cursor_open" || record.Action == "cursor_fetch" || record.Action == "cursor_close" || record.Action == "close")
	validBusiness := record.BusinessOutcome == "not_proven" || record.BusinessOutcome == "unknown" || record.BusinessOutcome == "completed" || record.BusinessOutcome == "not_executed" || record.BusinessOutcome == "not_applicable"
	validCleanup := record.CleanupOutcome == "not_applicable" || record.CleanupOutcome == "pending" || record.CleanupOutcome == "unknown" || record.CleanupOutcome == "completed"
	cursorAction := record.Source == "gateway" && (record.Action == "cursor_fetch" || record.Action == "cursor_close")
	validCursorOutcome := record.CursorNativeOutcome == "" || record.CursorNativeOutcome == cursorOutcomeRolledBack || record.CursorNativeOutcome == cursorOutcomeCommitted || record.CursorNativeOutcome == cursorOutcomeUnknown
	validCursorStop := !record.CursorClientStopped && !record.CursorDispatchStopped || record.Source == "gateway" && record.Action == "cursor_close" && (!record.CursorClientStopped || record.CursorDispatchStopped)
	validCursorState := record.CursorNativeOutcome != cursorOutcomeUnknown || record.State == "quarantined"
	if record.CursorNativeOutcome == cursorOutcomeRolledBack || record.CursorNativeOutcome == cursorOutcomeCommitted {
		validCursorState = validCursorState && record.State == "terminal"
	}
	if !validStage || !validAction || !validBusiness || !validCleanup || !validCursorOutcome || !validCursorStop || !validCursorState || record.CursorNativeOutcome != "" && !cursorAction || !identifierPattern.MatchString(record.Worker) || record.Session != "" && !upstreamSessionID(record.Session) {
		return errors.New("invalid operation observation")
	}
	database, exists := databases[record.Database.Key()]
	if !exists || database.Project != record.Project || key != operationKey(record.Database, record.OperationID) || !sqlLabel(record.OperationID) || !validCommandDigest(record.Digest) || record.RetryAllowed || (record.Source != "management" && record.Source != "gateway") || (record.State != "in_flight" && record.State != "terminal" && record.State != "quarantined") {
		return errors.New("invalid durable operation")
	}
	if record.Decision != "" && (record.Decision != "retain_isolation_new_database_required" || record.State != "quarantined" || !labelPattern.MatchString(record.DecisionRequestID)) {
		return errors.New("unsafe operator decision")
	}
	if record.LocalResourcesRetired && (record.State != "quarantined" || record.Decision == "" || !labelPattern.MatchString(record.RetirementRequestID)) {
		return errors.New("unsafe local retirement")
	}
	if record.RetirementPending && (record.State != "quarantined" || record.Decision == "" || record.LocalResourcesRetired) {
		return errors.New("unsafe pending retirement")
	}
	if record.ReleaseRequestID != "" && (record.State != "quarantined" || record.Decision == "" || !record.LocalResourcesRetired || record.RetirementPending || !labelPattern.MatchString(record.ReleaseRequestID) || !fencedAuthorityMode(record.ReleaseAuthorityMode)) {
		return errors.New("unsafe isolation release")
	}
	return nil
}

func validateOperationTransition(old, record OperationRecord) error {
	if !old.LocalResourcesRetired && record.LocalResourcesRetired && !old.RetirementPending {
		return errors.New("retirement must follow durable pending intent")
	}
	if record.RegistrationSequence != old.RegistrationSequence || record.Database != old.Database || record.Project != old.Project || record.OperationID != old.OperationID || record.RequestID != old.RequestID || record.Source != old.Source || record.Action != old.Action || record.Digest != old.Digest || record.Worker != old.Worker {
		return errors.New("operation identity and history immutable")
	}
	if old.State != "in_flight" {
		normalized := record
		normalized.Decision, normalized.DecisionRequestID = old.Decision, old.DecisionRequestID
		normalized.LocalResourcesRetired, normalized.RetirementRequestID = old.LocalResourcesRetired, old.RetirementRequestID
		normalized.RetirementPending = old.RetirementPending
		normalized.ReleaseRequestID, normalized.ReleaseAuthorityMode, normalized.ReleaseEpoch = old.ReleaseRequestID, old.ReleaseAuthorityMode, old.ReleaseEpoch
		if old.State == "terminal" && old.Source == "gateway" && old.Action == "open" && old.CleanupOutcome == "pending" && (record.State == "terminal" && record.CleanupOutcome == "completed" || record.State == "quarantined" && record.CleanupOutcome == "unknown") {
			normalized.State, normalized.CleanupOutcome, normalized.MayHaveCommitted = old.State, old.CleanupOutcome, old.MayHaveCommitted
		}
		if old.ReleaseRequestID != "" {
			// A recorded release is immutable: no path may rewrite or drop it.
			if record.ReleaseRequestID != old.ReleaseRequestID || record.ReleaseAuthorityMode != old.ReleaseAuthorityMode || record.ReleaseEpoch != old.ReleaseEpoch {
				return errors.New("isolation release immutable")
			}
		} else if record.ReleaseRequestID != "" {
			// A release may only be recorded once, on a quarantine whose local
			// retirement already completed, and it never coexists with a pending
			// retirement intent.
			if !old.LocalResourcesRetired || old.RetirementPending || record.RetirementPending || record.LocalResourcesRetired != old.LocalResourcesRetired {
				return errors.New("isolation release must follow completed local retirement")
			}
			normalized.ReleaseRequestID, normalized.ReleaseAuthorityMode, normalized.ReleaseEpoch = "", "", 0
		}
		if !reflect.DeepEqual(normalized, old) || old.Decision != "" && (record.Decision != old.Decision || record.DecisionRequestID != old.DecisionRequestID) || old.LocalResourcesRetired && (!record.LocalResourcesRetired || record.RetirementRequestID != old.RetirementRequestID) {
			return errors.New("terminal outcome or quarantine immutable")
		}
	}
	return nil
}

func validateOperations(state State, previous *State) error {
	known, unresolved := operationCounts(state)
	if unresolved > operationCapacity || known > operationKnownRetention && (previous != nil || state.OperationSequence != 0) || len(state.Operations) > operationCapacity+operationKnownRetention {
		return ErrOperationCapacity
	}
	if previous != nil && (state.OperationSequence < previous.OperationSequence || state.OperationEvictions < previous.OperationEvictions) {
		return errors.New("operation retention counters immutable")
	}
	sequences := map[uint64]bool{}
	for key, record := range state.Operations {
		if record.RegistrationSequence > state.OperationSequence || record.RegistrationSequence != 0 && sequences[record.RegistrationSequence] {
			return errors.New("invalid operation registration order")
		}
		if record.RegistrationSequence != 0 {
			sequences[record.RegistrationSequence] = true
		}
		if err := validateOperationRecord(record, key, state.Databases); err != nil {
			return err
		}
	}
	if previous != nil {
		for key, old := range previous.Operations {
			record, exists := state.Operations[key]
			if !exists {
				if operationKnownTerminal(old) {
					continue
				}
				return errors.New("unresolved operation and isolation history immutable")
			}
			if err := validateOperationTransition(old, record); err != nil {
				return err
			}
		}
	}
	return nil
}
