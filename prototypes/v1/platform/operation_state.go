package platform

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"

	controller "peri-loom/controller"
)

const operationCapacity = 4096
const operationKnownRetention = 256

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
}

type OperationRecord struct {
	OperationStatus
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

func compactKnownOperations(state *State, eligible map[string]bool) error {
	keys := []string{}
	known := 0
	for key, record := range state.Operations {
		if operationKnownTerminal(record) {
			known++
			if eligible[key] {
				keys = append(keys, key)
			}
		}
	}
	if known <= operationKnownRetention {
		return nil
	}
	sort.Slice(keys, func(first, second int) bool {
		firstSequence, secondSequence := state.Operations[keys[first]].RegistrationSequence, state.Operations[keys[second]].RegistrationSequence
		if firstSequence == secondSequence {
			return keys[first] < keys[second]
		}
		return firstSequence < secondSequence
	})
	remove := known - operationKnownRetention
	if remove > len(keys) || state.OperationEvictions > ^uint64(0)-uint64(remove) {
		return ErrOperationCapacity
	}
	for _, key := range keys[:remove] {
		delete(state.Operations, key)
	}
	state.OperationEvictions += uint64(remove)
	return nil
}

func updateOperationRecords(store StateStore, change func(*State) error) error {
	return store.Update(func(state *State) error {
		eligible := map[string]bool{}
		for key, record := range state.Operations {
			if operationKnownTerminal(record) {
				eligible[key] = true
			}
		}
		if change != nil {
			if err := change(state); err != nil {
				return err
			}
		}
		return compactKnownOperations(state, eligible)
	})
}

func operationKey(database controller.DatabaseID, requestID string) string {
	return database.Key() + "/" + requestID
}

func isolatedOperation(state State, database controller.DatabaseID, includePending bool) (OperationStatus, bool) {
	var selected OperationStatus
	found := false
	for _, record := range state.Operations {
		if record.Database.Namespace == database.Namespace && (record.State == "quarantined" || record.State == "in_flight" && (includePending || record.Source == "management")) {
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

func beginOperation(store StateStore, database controller.DatabaseID, requestID, source, action, stage, worker, session string, command any) error {
	return beginCorrelatedOperation(store, database, requestID, requestID, source, action, stage, worker, session, command)
}

func registerOperation(store StateStore, database controller.DatabaseID, requestID, source, action, stage, worker, session string, command any) (string, error) {
	operationID, err := controller.RandomID()
	if err != nil {
		return "", err
	}
	err = beginCorrelatedOperation(store, database, operationID, requestID, source, action, stage, worker, session, command)
	return operationID, err
}

func beginCorrelatedOperation(store StateStore, database controller.DatabaseID, operationID, requestID, source, action, stage, worker, session string, command any) error {
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	encoded, err := json.Marshal(command)
	if err != nil || store == nil || database.Validate() != nil || !sqlLabel(requestID) || source == "management" && !labelPattern.MatchString(requestID) {
		return errors.New("operation registration unavailable")
	}
	digest := sha256.Sum256(encoded)
	return updateOperationRecords(store, func(state *State) error {
		if _, exists := state.Operations[operationKey(database, operationID)]; exists {
			return ErrOperationExists
		}
		if _, blocked := isolatedOperation(*state, database, source == "management"); blocked {
			return ErrDatabaseQuarantined
		}
		descriptor, exists := state.Databases[database.Key()]
		_, unresolved := operationCounts(*state)
		if !exists || unresolved >= operationCapacity || state.OperationSequence == ^uint64(0) {
			return ErrOperationCapacity
		}
		if state.Operations == nil {
			state.Operations = map[string]OperationRecord{}
		}
		state.OperationSequence++
		state.Operations[operationKey(database, operationID)] = OperationRecord{OperationStatus: OperationStatus{OperationID: operationID, RequestID: requestID, Database: database, Project: descriptor.Project, Worker: worker, Source: source, Action: action, Stage: stage, State: "in_flight", BusinessOutcome: "not_proven", CleanupOutcome: "not_applicable"}, Digest: hex.EncodeToString(digest[:]), Session: session, RegistrationSequence: state.OperationSequence}
		return nil
	})
}

func updateOperation(store StateStore, database controller.DatabaseID, requestID string, change func(*OperationRecord)) error {
	unlock := lockOperationNamespace(database.Namespace)
	defer unlock()
	return updateOperationRecords(store, func(state *State) error {
		key := operationKey(database, requestID)
		record, exists := state.Operations[key]
		if !exists || record.State != "in_flight" {
			return ErrOperationExists
		}
		change(&record)
		state.Operations[key] = record
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
			record.Ambiguous, record.Rejected, record.ExecutionCompleted = rpc.Ambiguous, rpc.Rejected, rpc.ExecutionCompleted
		}
		record.MayHaveCommitted = true
		if record.Stage == "close_session" {
			record.CleanupOutcome = "unknown"
		} else {
			record.BusinessOutcome = "unknown"
		}
	})
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
		validStage := record.Stage == "open_session" || record.Stage == "tool_rpc" || record.Stage == "close_session"
		validAction := record.Source == "management" && nativeDiagnosticAction("/v1/databases/"+record.Action) != "" || record.Source == "gateway" && (record.Action == "open" || record.Action == "execute" || record.Action == "query" || record.Action == "statement" || record.Action == "describe" || record.Action == "cursor_open" || record.Action == "cursor_fetch" || record.Action == "cursor_close" || record.Action == "close")
		validBusiness := record.BusinessOutcome == "not_proven" || record.BusinessOutcome == "unknown" || record.BusinessOutcome == "completed" || record.BusinessOutcome == "not_executed" || record.BusinessOutcome == "not_applicable"
		validCleanup := record.CleanupOutcome == "not_applicable" || record.CleanupOutcome == "pending" || record.CleanupOutcome == "unknown" || record.CleanupOutcome == "completed"
		if !validStage || !validAction || !validBusiness || !validCleanup || !identifierPattern.MatchString(record.Worker) || record.Session != "" && !upstreamSessionID(record.Session) {
			return errors.New("invalid operation observation")
		}
		database, exists := state.Databases[record.Database.Key()]
		digest, err := hex.DecodeString(record.Digest)
		if !exists || database.Project != record.Project || key != operationKey(record.Database, record.OperationID) || !sqlLabel(record.OperationID) || err != nil || len(digest) != 32 || record.RetryAllowed || (record.Source != "management" && record.Source != "gateway") || (record.State != "in_flight" && record.State != "terminal" && record.State != "quarantined") {
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
				if old.State == "terminal" && old.Source == "gateway" && old.Action == "open" && old.CleanupOutcome == "pending" && (record.State == "terminal" && record.CleanupOutcome == "completed" || record.State == "quarantined" && record.CleanupOutcome == "unknown") {
					normalized.State, normalized.CleanupOutcome, normalized.MayHaveCommitted = old.State, old.CleanupOutcome, old.MayHaveCommitted
				}
				if !reflect.DeepEqual(normalized, old) || old.Decision != "" && (record.Decision != old.Decision || record.DecisionRequestID != old.DecisionRequestID) || old.LocalResourcesRetired && (!record.LocalResourcesRetired || record.RetirementRequestID != old.RetirementRequestID) {
					return errors.New("terminal outcome or quarantine immutable")
				}
			}
		}
	}
	return nil
}
