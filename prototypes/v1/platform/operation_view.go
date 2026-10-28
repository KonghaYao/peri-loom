package platform

import (
	"errors"
	"sort"

	controller "peri-loom/controller"
)

type operationIndex struct {
	records  map[string]OperationRecord
	terminal map[string]bool
	order    []string
	blocking map[string]map[string]bool
}

func newOperationIndex(records map[string]OperationRecord) *operationIndex {
	if records == nil {
		records = map[string]OperationRecord{}
	}
	index := &operationIndex{records: records, terminal: map[string]bool{}, order: []string{}, blocking: map[string]map[string]bool{}}
	for key, record := range records {
		index.track(key, record)
	}
	return index
}

func (index *operationIndex) track(key string, record OperationRecord) {
	if index.terminal[key] {
		for position, existing := range index.order {
			if existing == key {
				index.order = append(index.order[:position], index.order[position+1:]...)
				break
			}
		}
		delete(index.terminal, key)
	}
	if operationKnownTerminal(record) {
		position := sort.Search(len(index.order), func(position int) bool {
			other := index.order[position]
			sequence := index.records[other].RegistrationSequence
			return sequence > record.RegistrationSequence || sequence == record.RegistrationSequence && other >= key
		})
		index.order = append(index.order, "")
		copy(index.order[position+1:], index.order[position:])
		index.order[position] = key
		index.terminal[key] = true
	}
	namespace := namespaceKey(record.Database.Namespace)
	delete(index.blocking[namespace], key)
	if record.State == "quarantined" && record.ReleaseRequestID == "" || record.State == "in_flight" {
		if index.blocking[namespace] == nil {
			index.blocking[namespace] = map[string]bool{}
		}
		index.blocking[namespace][key] = record.State == "in_flight" && record.Source != "management"
	} else if len(index.blocking[namespace]) == 0 {
		delete(index.blocking, namespace)
	}
}

func (index *operationIndex) release(key string, record OperationRecord) {
	if index.terminal[key] {
		for position, existing := range index.order {
			if existing == key {
				index.order = append(index.order[:position], index.order[position+1:]...)
				break
			}
		}
		delete(index.terminal, key)
	}
	namespace := namespaceKey(record.Database.Namespace)
	delete(index.blocking[namespace], key)
	if len(index.blocking[namespace]) == 0 {
		delete(index.blocking, namespace)
	}
}

func (index *operationIndex) counts() (known, unresolved int) {
	return len(index.order), len(index.records) - len(index.order)
}

func (index *operationIndex) isolated(namespace controller.Namespace, includePending bool) (OperationStatus, bool) {
	var selected OperationStatus
	found := false
	for key, pendingOnly := range index.blocking[namespaceKey(namespace)] {
		if pendingOnly && !includePending {
			continue
		}
		record := index.records[key]
		if !found || record.OperationID < selected.OperationID {
			selected, found = record.OperationStatus, true
		}
	}
	return selected, found
}

type operationTouch struct {
	key     string
	record  OperationRecord
	existed bool
}

type operationEdit struct {
	// Diagnostic metadata only; never part of State, journalMutation or rollback.
	caller        *durabilityCaller
	state         *State
	index         *operationIndex
	priorSequence uint64
	evictions     uint64
	touched       map[string]int
	prior         []operationTouch
}

func newOperationEdit(state *State, index *operationIndex) *operationEdit {
	if state.Operations == nil {
		state.Operations = index.records
	}
	return &operationEdit{state: state, index: index, priorSequence: state.OperationSequence, evictions: state.OperationEvictions, touched: map[string]int{}}
}

func (edit *operationEdit) has(key string) bool {
	_, exists := edit.index.records[key]
	return exists
}

func (edit *operationEdit) record(key string) (OperationRecord, bool) {
	record, exists := edit.index.records[key]
	return record, exists
}

func (edit *operationEdit) put(key string, record OperationRecord) {
	old, exists := edit.record(key)
	if _, touched := edit.touched[key]; !touched {
		edit.touched[key] = len(edit.prior)
		edit.prior = append(edit.prior, operationTouch{key: key, record: old, existed: exists})
	}
	if exists {
		edit.index.release(key, old)
	}
	edit.index.records[key] = record
	edit.index.track(key, record)
}

func (edit *operationEdit) remove(key string) {
	old, exists := edit.record(key)
	if !exists {
		return
	}
	if _, touched := edit.touched[key]; !touched {
		edit.touched[key] = len(edit.prior)
		edit.prior = append(edit.prior, operationTouch{key: key, record: old, existed: true})
	}
	edit.index.release(key, old)
	delete(edit.index.records, key)
}

func (edit *operationEdit) advance() uint64 {
	edit.state.OperationSequence++
	return edit.state.OperationSequence
}

func (edit *operationEdit) sequence() uint64 { return edit.state.OperationSequence }

func (edit *operationEdit) isolation(database controller.DatabaseID, includePending bool) (OperationStatus, bool) {
	return edit.index.isolated(database.Namespace, includePending)
}

func (edit *operationEdit) counts() (known, unresolved int) { return edit.index.counts() }

func (edit *operationEdit) database(key string) (Database, bool) {
	database, exists := edit.state.Databases[key]
	return database, exists
}

func (edit *operationEdit) matching(match func(string, OperationRecord) bool) []string {
	var keys []string
	for key, record := range edit.index.records {
		if match(key, record) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (edit *operationEdit) revert() {
	for position := len(edit.prior) - 1; position >= 0; position-- {
		touch := edit.prior[position]
		if current, exists := edit.record(touch.key); exists {
			edit.index.release(touch.key, current)
		}
		if touch.existed {
			edit.index.records[touch.key] = touch.record
			edit.index.track(touch.key, touch.record)
		} else {
			delete(edit.index.records, touch.key)
		}
	}
	edit.state.OperationSequence = edit.priorSequence
	edit.state.OperationEvictions = edit.evictions
	clear(edit.touched)
	edit.prior = nil
}

func (edit *operationEdit) delta() operationDelta {
	delta := operationDelta{}
	for _, touch := range edit.prior {
		record, exists := edit.record(touch.key)
		if exists && (!touch.existed || touch.record != record) {
			if delta.Set == nil {
				delta.Set = map[string]OperationRecord{}
			}
			delta.Set[touch.key] = record
		} else if !exists && touch.existed {
			delta.Remove = append(delta.Remove, touch.key)
		}
	}
	sort.Strings(delta.Remove)
	return delta
}

func (edit *operationEdit) settleRetention() error {
	known := len(edit.index.order)
	if known <= operationKnownRetention {
		return nil
	}
	remove := known - operationKnownRetention
	keys := make([]string, 0, remove)
	for _, key := range edit.index.order {
		if position, touched := edit.touched[key]; touched && (!edit.prior[position].existed || !operationKnownTerminal(edit.prior[position].record)) {
			continue
		}
		keys = append(keys, key)
		if len(keys) == remove {
			break
		}
	}
	if len(keys) < remove || edit.state.OperationEvictions > ^uint64(0)-uint64(remove) {
		return ErrOperationCapacity
	}
	for _, key := range keys {
		edit.remove(key)
	}
	edit.state.OperationEvictions += uint64(remove)
	return nil
}

func (edit *operationEdit) validate() error {
	known, unresolved := edit.index.counts()
	if unresolved > operationCapacity || known > operationKnownRetention || len(edit.index.records) > operationCapacity+operationKnownRetention {
		return ErrOperationCapacity
	}
	if edit.state.OperationSequence < edit.priorSequence || edit.state.OperationEvictions < edit.evictions {
		return errors.New("operation retention counters immutable")
	}
	// Recovery and registry commits validate global uniqueness and cross-record
	// invariants with validateOperations. Online edits preserve them inductively: a
	// retained sequence is at most the counter this commit started from, and an
	// insertion takes its sequence from that counter, so it can collide with neither a
	// retained record nor another insertion in the same commit.
	inserted := map[uint64]bool{}
	for _, touch := range edit.prior {
		next, exists := edit.record(touch.key)
		if !exists {
			if touch.existed && !operationKnownTerminal(touch.record) {
				return errors.New("unresolved operation and isolation history immutable")
			}
			continue
		}
		if next.RegistrationSequence > edit.state.OperationSequence {
			return errors.New("invalid operation registration order")
		}
		if !touch.existed {
			if next.RegistrationSequence == 0 || next.RegistrationSequence <= edit.priorSequence || inserted[next.RegistrationSequence] {
				return errors.New("invalid operation registration order")
			}
			inserted[next.RegistrationSequence] = true
		}
		if err := validateOperationRecord(next, touch.key, edit.state.Databases); err != nil {
			return err
		}
		if touch.existed {
			if err := validateOperationTransition(touch.record, next); err != nil {
				return err
			}
		}
	}
	return nil
}
