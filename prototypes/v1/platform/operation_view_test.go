package platform

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"testing"
)

// Freeze the old full-view algorithms so incremental edits cannot redefine the oracle.
func referenceOperationDelta(previous, next map[string]OperationRecord) operationDelta {
	delta := operationDelta{}
	for key, record := range next {
		if before, exists := previous[key]; !exists || before != record {
			if delta.Set == nil {
				delta.Set = map[string]OperationRecord{}
			}
			delta.Set[key] = record
		}
	}
	for key := range previous {
		if _, exists := next[key]; !exists {
			delta.Remove = append(delta.Remove, key)
		}
	}
	sort.Strings(delta.Remove)
	return delta
}

func referenceOperationRetention(state *State, previous map[string]OperationRecord) error {
	eligible := map[string]bool{}
	for key, record := range previous {
		if operationKnownTerminal(record) {
			eligible[key] = true
		}
	}
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

func assertOperationIndex(test *testing.T, state State, index *operationIndex) {
	test.Helper()
	if reflect.ValueOf(state.Operations).Pointer() != reflect.ValueOf(index.records).Pointer() {
		test.Fatal("operation map copied")
	}
	known, unresolved := index.counts()
	wantKnown, wantUnresolved := operationCounts(state)
	if known != wantKnown || unresolved != wantUnresolved {
		test.Fatalf("counts = %d/%d, want %d/%d", known, unresolved, wantKnown, wantUnresolved)
	}
	for _, database := range state.Databases {
		for _, includePending := range []bool{false, true} {
			got, found := index.isolated(database.ID.Namespace, includePending)
			want, wantFound := isolatedOperation(state, database.ID, includePending)
			if got != want || found != wantFound {
				test.Fatalf("isolation differs for %s pending=%v", database.ID.Key(), includePending)
			}
		}
	}
	if !reflect.DeepEqual(index, newOperationIndex(state.Operations)) {
		test.Fatal("incremental index differs from full rebuild")
	}
}

func differentialOperationEdit(test *testing.T, state *State, index *operationIndex, change func(*operationEdit)) operationDelta {
	test.Helper()
	previous := maps.Clone(state.Operations)
	identity := reflect.ValueOf(state.Operations).Pointer()
	edit := newOperationEdit(state, index)
	change(edit)
	reference := *state
	reference.Operations = maps.Clone(state.Operations)
	wantErr := referenceOperationRetention(&reference, previous)
	if err := edit.settleRetention(); err != wantErr {
		test.Fatalf("retention error = %v, want %v", err, wantErr)
	}
	if !reflect.DeepEqual(state.Operations, reference.Operations) || state.OperationEvictions != reference.OperationEvictions {
		test.Fatal("retention differs from frozen reference")
	}
	got, want := edit.delta(), referenceOperationDelta(previous, reference.Operations)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(got, operationDeltaBetween(previous, reference.Operations)) {
		test.Fatalf("delta differs: got %+v, want %+v", got, want)
	}
	if err := edit.validate(); err != nil {
		test.Fatal(err)
	}
	if reflect.ValueOf(state.Operations).Pointer() != identity {
		test.Fatal("commit replaced operation map")
	}
	assertOperationIndex(test, *state, index)
	return got
}

func operationViewState() State {
	database := securityDatabase()
	return State{Databases: map[string]Database{database.Key(): {ID: database, Project: "alpha"}}, Operations: map[string]OperationRecord{}}
}

func operationViewRecord(identifier string, sequence uint64) OperationRecord {
	digest, _ := operationCommandDigest(nil)
	return OperationRecord{OperationStatus: OperationStatus{OperationID: identifier, RequestID: identifier, Database: securityDatabase(), Project: "alpha", Worker: "worker-a", Source: "gateway", Action: "execute", Stage: "tool_rpc", State: "in_flight", BusinessOutcome: "not_proven", CleanupOutcome: "not_applicable"}, Digest: digest, RegistrationSequence: sequence}
}

func TestOperationEditDifferential(test *testing.T) {
	state := operationViewState()
	index := newOperationIndex(state.Operations)
	for number := 0; number < operationKnownRetention+2; number++ {
		identifier := fmt.Sprintf("record_%04d", number)
		key := operationKey(securityDatabase(), identifier)
		differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
			edit.put(key, operationViewRecord(identifier, edit.advance()))
		})
		differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
			record, _ := edit.record(key)
			record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
			edit.put(key, record)
		})
	}
	key := index.order[0]
	delta := differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
		record, _ := edit.record(key)
		changed := record
		changed.CleanupOutcome = "pending"
		edit.put(key, changed)
		edit.put(key, record)
		edit.remove("absent")
		transient := operationViewRecord("transient", edit.sequence())
		edit.put("transient", transient)
		edit.remove("transient")
	})
	if !delta.empty() {
		test.Fatal("restoring original values produced a delta")
	}
	differentialOperationEdit(test, &state, index, func(edit *operationEdit) { edit.remove(key) })
}

func TestOperationEditRetentionDifferential(test *testing.T) {
	for _, legacy := range []bool{false, true} {
		test.Run(fmt.Sprintf("legacy_%v", legacy), func(test *testing.T) {
			state := operationViewState()
			state.OperationSequence = 1000
			for number := 0; number < operationKnownRetention+3; number++ {
				identifier := fmt.Sprintf("record_%04d", number)
				record := operationViewRecord(identifier, uint64(number+1))
				if legacy {
					record.RegistrationSequence = 0
				}
				if number >= 3 {
					record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
				}
				state.Operations[operationKey(record.Database, identifier)] = record
			}
			quarantine := operationViewRecord("quarantine", 999)
			quarantine.State, quarantine.BusinessOutcome, quarantine.MayHaveCommitted = "quarantined", "unknown", true
			quarantineKey := operationKey(quarantine.Database, quarantine.OperationID)
			state.Operations[quarantineKey] = quarantine
			index := newOperationIndex(state.Operations)
			delta := differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
				for number := 0; number < 3; number++ {
					key := operationKey(securityDatabase(), fmt.Sprintf("record_%04d", number))
					record, _ := edit.record(key)
					record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
					edit.put(key, record)
				}
			})
			for number, key := range delta.Remove {
				if key != operationKey(securityDatabase(), fmt.Sprintf("record_%04d", number+3)) {
					test.Fatal("newly settled record evicted or ordering changed")
				}
			}
			if len(delta.Remove) != 3 || state.OperationEvictions != 3 || !index.records[quarantineKey].MayHaveCommitted {
				test.Fatal("batch eviction changed quarantine or eviction count")
			}
		})
	}
}

func TestOperationIndexIsolationDifferential(test *testing.T) {
	state := operationViewState()
	other := securityDatabase()
	other.Namespace.Database = "other"
	state.Databases[other.Key()] = Database{ID: other, Project: "alpha"}
	for number, identifier := range []string{"z_gateway", "m_management", "a_quarantine", "b_other"} {
		record := operationViewRecord(identifier, uint64(number+1))
		switch identifier {
		case "m_management":
			record.Source, record.Action = "management", "checkpoint"
		case "a_quarantine":
			record.State, record.BusinessOutcome = "quarantined", "unknown"
			record.Decision, record.DecisionRequestID = "retain_isolation_new_database_required", "decision"
			record.LocalResourcesRetired, record.RetirementRequestID = true, "retirement"
		case "b_other":
			record.Database = other
		}
		state.Operations[operationKey(record.Database, identifier)] = record
	}
	state.OperationSequence = 4
	index := newOperationIndex(state.Operations)
	assertOperationIndex(test, state, index)
	key := operationKey(securityDatabase(), "a_quarantine")
	differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
		record, _ := edit.record(key)
		record.ReleaseRequestID, record.ReleaseAuthorityMode = "release", "detached"
		edit.put(key, record)
	})
	if _, exists := index.blocking[namespaceKey(securityDatabase().Namespace)][key]; exists {
		test.Fatal("released quarantine still blocks")
	}
	for _, identifier := range []string{"m_management", "z_gateway"} {
		key := operationKey(securityDatabase(), identifier)
		differentialOperationEdit(test, &state, index, func(edit *operationEdit) {
			record, _ := edit.record(key)
			record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
			edit.put(key, record)
		})
	}
	if _, blocked := index.isolated(securityDatabase().Namespace, true); blocked {
		test.Fatal("another namespace or released quarantine blocks admission")
	}
}

func TestOperationEditRollback(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	defer store.Close()
	journalCommit(test, store, "known")
	if err := beginOperation(store, securityDatabase(), "pending", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	before := maps.Clone(store.state.Operations)
	beforeIndex := newOperationIndex(before)
	sequence, evictions := store.state.OperationSequence, store.state.OperationEvictions
	identity := reflect.ValueOf(store.state.Operations).Pointer()
	bytes := journalBytes(test, store)
	failure := errors.New("callback failed")
	var attempted *operationEdit
	err := store.updateOperations(func(edit *operationEdit) error {
		attempted = edit
		key := operationKey(securityDatabase(), "pending")
		record, _ := edit.record(key)
		record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
		edit.put(key, record)
		edit.remove(operationKey(securityDatabase(), "known"))
		edit.put(operationKey(securityDatabase(), "new"), operationViewRecord("new", edit.advance()))
		edit.state.OperationEvictions++
		return failure
	})
	if !errors.Is(err, failure) || !reflect.DeepEqual(before, store.state.Operations) || !reflect.DeepEqual(beforeIndex, store.operationsIndex) || sequence != store.state.OperationSequence || evictions != store.state.OperationEvictions {
		test.Fatal("callback failure did not restore records, counters and index")
	}
	if journalBytes(test, store) != bytes || store.disk.poisoned || len(attempted.prior) != 0 || len(attempted.touched) != 0 || !attempted.delta().empty() {
		test.Fatal("callback failure wrote the journal or retained touches")
	}
	if reflect.ValueOf(store.state.Operations).Pointer() != identity {
		test.Fatal("rollback replaced operation map")
	}
	assertOperationIndex(test, store.state, store.operationsIndex)
}

func TestOperationEditRetentionFailure(test *testing.T) {
	for _, overflow := range []bool{false, true} {
		test.Run(fmt.Sprintf("overflow_%v", overflow), func(test *testing.T) {
			state := operationViewState()
			for number := 0; number <= operationKnownRetention; number++ {
				record := operationViewRecord(fmt.Sprintf("record_%04d", number), uint64(number+1))
				if overflow {
					record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
				}
				state.Operations[operationKey(record.Database, record.OperationID)] = record
			}
			state.OperationSequence = operationKnownRetention + 1
			if overflow {
				state.OperationEvictions = ^uint64(0)
			}
			previous := maps.Clone(state.Operations)
			index := newOperationIndex(state.Operations)
			edit := newOperationEdit(&state, index)
			for _, key := range edit.matching(func(_ string, record OperationRecord) bool { return record.State == "in_flight" }) {
				record, _ := edit.record(key)
				record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
				edit.put(key, record)
			}
			reference := state
			reference.Operations = maps.Clone(state.Operations)
			if edit.settleRetention() != ErrOperationCapacity || referenceOperationRetention(&reference, previous) != ErrOperationCapacity {
				test.Fatal("ineligible eviction or counter overflow accepted")
			}
			edit.revert()
			if !reflect.DeepEqual(previous, state.Operations) {
				test.Fatal("failed retention did not roll back")
			}
			assertOperationIndex(test, state, index)
		})
	}
}

func TestOperationEditValidationAndAppendRollback(test *testing.T) {
	for _, failure := range []string{"validation", "append"} {
		test.Run(failure, func(test *testing.T) {
			store := operationDurableStore(test).(*stateStore)
			defer store.Close()
			journalCommit(test, store, "known")
			previous := maps.Clone(store.state.Operations)
			sequence, evictions := store.state.OperationSequence, store.state.OperationEvictions
			beforeIndex := newOperationIndex(previous)
			bytes := journalBytes(test, store)
			if failure == "append" {
				if err := store.operations.close(); err != nil {
					test.Fatal(err)
				}
			}
			err := store.updateOperations(func(edit *operationEdit) error {
				key := operationKey(securityDatabase(), "known")
				if failure == "validation" {
					record, _ := edit.record(key)
					record.Digest = "invalid"
					edit.put(key, record)
				} else {
					edit.remove(key)
				}
				edit.advance()
				return nil
			})
			if err == nil || !reflect.DeepEqual(previous, store.state.Operations) || !reflect.DeepEqual(beforeIndex, store.operationsIndex) || sequence != store.state.OperationSequence || evictions != store.state.OperationEvictions {
				test.Fatal("failed commit did not restore records, counters and index")
			}
			if store.disk.poisoned != (failure == "append") || journalBytes(test, store) != bytes {
				test.Fatal("wrong poison or journal behavior on commit failure")
			}
			assertOperationIndex(test, store.state, store.operationsIndex)
		})
	}
}

func TestOperationIndexReplayOrder(test *testing.T) {
	state := operationViewState()
	for number, identifier := range []string{"a", "b", "c"} {
		record := operationViewRecord(identifier, uint64(number+1))
		record.State, record.BusinessOutcome, record.ExecutionProven = "terminal", "completed", true
		state.Operations[operationKey(record.Database, identifier)] = record
	}
	index := newOperationIndex(state.Operations)
	key := operationKey(securityDatabase(), "a")
	record := state.Operations[key]
	record.RegistrationSequence = 4
	state.Operations[key] = record
	index.track(key, record)
	index.track(key, record)
	if index.order[len(index.order)-1] != key {
		test.Fatal("replayed key kept its stale position")
	}
	assertOperationIndex(test, state, index)
	index.release(key, record)
	index.release(key, record)
	delete(state.Operations, key)
	assertOperationIndex(test, state, index)
}

func TestOperationStoreIndexInvalidation(test *testing.T) {
	store := operationDurableStore(test).(*stateStore)
	defer store.Close()
	journalCommit(test, store, "known")
	old := store.operationsIndex
	if err := store.Update(func(*State) error { return nil }); err != nil {
		test.Fatal(err)
	}
	if store.operationsIndex != nil {
		test.Fatal("registry replacement retained stale index")
	}
	identity := reflect.ValueOf(store.state.Operations).Pointer()
	if err := store.updateOperations(func(edit *operationEdit) error {
		if reflect.ValueOf(edit.index.records).Pointer() != identity {
			test.Fatal("commit copied the map before invoking callback")
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if store.operationsIndex == old {
		test.Fatal("registry replacement did not rebuild index")
	}
	journalCommit(test, store, "next")
	if reflect.ValueOf(store.state.Operations).Pointer() != identity {
		test.Fatal("registration or settlement copied the map")
	}
	assertOperationIndex(test, store.state, store.operationsIndex)
}
