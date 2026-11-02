package platform

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	controller "peri-loom/controller"
)

type stateStore struct {
	disk            *durableFile
	operations      *operationJournal
	operationsIndex *operationIndex
	cost            stateCost
	state           State
	// credentials is an immutable digest -> key view published under the state lock.
	// Authentication is the first lock a request would otherwise queue for, and it only
	// needs one lookup, so readers load the pointer instead of taking the lock. A stale
	// view cannot authenticate a revoked key: the authorization step still reads the
	// locked view and refuses revoked descriptors.
	credentials atomic.Pointer[map[string]APIKey]
	// checkpointed freezes the store once the shutdown checkpoint has materialized
	// the operation view: a write that landed afterwards would leave the payload
	// claiming a final view it no longer carries. Guarded by the state lock.
	checkpointed bool
	// The edits applied to the view whose appended deltas are not durable yet, oldest
	// first. Only the view lock touches it, so it is exactly the journal's queue and
	// in-flight batches in the order they were applied.
	unconfirmed        []operationPendingEdit
	maintenance        sync.WaitGroup
	maintenanceClosing bool
	// The state lock owns the v1 compaction gate and its shutdown error latch.
	operationCompacting        chan struct{}
	operationCompactionFailure error
	// testOperationLock observes each operation state lock acquisition. Tests use
	// it to pin how many times a path may enter the shared view lock.
	testOperationLock func()
}

func OpenStateStore(directory string) (StateStore, error) {
	if directory == "" {
		return nil, errors.New("private state directory required")
	}
	initial := State{Version: 1, Organizations: map[string]Organization{}, Projects: map[string]Project{}, Databases: map[string]Database{}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{}}
	disk, err := openDurable(filepath.Join(directory, "state.json"), initial)
	if err != nil {
		return nil, err
	}
	var state State
	if err := strictDecode(disk.payload, &state); err != nil {
		disk.close()
		return nil, err
	}
	if state.Revision != disk.revision {
		disk.close()
		return nil, errors.New("state revision mismatch")
	}
	if err := validateState(state, nil); err != nil {
		disk.close()
		return nil, err
	}
	store := &stateStore{disk: disk, state: state}
	operations, err := openOperationDisk(directory, disk, &store.state)
	if err != nil {
		disk.close()
		return nil, err
	}
	store.operations = operations
	disk.parentOperations = registryParentOperations(operations)
	state = store.state
	store.publishCredentialsLocked()
	known, _ := operationCounts(state)
	if known > operationKnownRetention {
		if err := updateOperationRecords(store, nil); err != nil {
			store.Close()
			return nil, err
		}
	}
	for _, record := range state.Operations {
		if operationNeedsRecovery(record) {
			if err := store.Update(func(candidate *State) error {
				for key, pending := range candidate.Operations {
					if operationNeedsRecovery(pending) {
						openedSession := pending.State == "terminal"
						pending.State = "quarantined"
						pending.MayHaveCommitted = true
						if pending.Stage == "close_session" || openedSession {
							pending.CleanupOutcome = "unknown"
						} else {
							pending.BusinessOutcome = "unknown"
						}
						candidate.Operations[key] = pending
					}
				}
				return nil
			}); err != nil {
				store.Close()
				return nil, err
			}
			break
		}
	}
	return store, nil
}

func (store *stateStore) Snapshot() (State, error) {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return State{}, err
	}
	// The snapshot carries operations, so it must not report an edit whose append was
	// never confirmed: settle first, then answer the same uncertainty that append had.
	if err := store.settleOperations(); err != nil {
		return State{}, err
	}
	return cloneState(store.state)
}

// CheckpointOperations commits the operation view this store has joined into the
// registry payload, so a reader of state.json alone sees the durable truth
// without replaying the sidecar, and freezes both write paths afterwards so no
// later mutation can invalidate that view.
//
// The payload only carries an operation view when a registry commit happens to
// write one, and between commits the sidecar is the only writer, so a payload
// read on its own can trail the durable log by an arbitrary number of
// operations. The checkpoint names its own revision, and only this call writes a
// nonzero value, so a payload whose operations_checkpoint equals its revision is
// the materialized view and every other payload is not.
//
// It fails closed: an error here leaves the payload without a checkpoint and the
// caller reports an unclean stop.
func CheckpointOperations(store StateStore) error {
	narrow, ok := store.(interface{ checkpointOperations() error })
	if !ok {
		return errors.New("operation checkpoint requires the durable state store")
	}
	return narrow.checkpointOperations()
}

// operationsCheckpointMaterialized reports whether this state is the payload a
// shutdown checkpoint wrote: the checkpoint names the revision that commit
// wrote, and any later commit leaves it behind. Absent or stale never counts as
// materialized, so an older payload cannot be read as a final view.
func operationsCheckpointMaterialized(state State) bool {
	return state.OperationsCheckpoint != 0 && state.OperationsCheckpoint == state.Revision
}

func (store *stateStore) checkpointOperations() error {
	defer store.lockOperationMutationState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if store.checkpointed {
		return errors.New("operation checkpoint already recorded")
	}
	if store.state.OperationStorageVersion != 1 && store.state.OperationStorageVersion != completionJournalVersion {
		return errors.New("operation storage identity unavailable")
	}
	// The view can carry edits whose appended delta is not durable yet: the commit queue
	// applies them under this lock and only then waits for its barrier outside it, so a
	// clone taken now could freeze the payload on a record the log has not accepted.
	// Joining the in-flight commits first makes the clone exactly what the durable log
	// holds; an append whose barrier refused it fails this checkpoint closed instead.
	if err := store.settleOperations(); err != nil {
		return err
	}
	if err := store.operations.checkpointCompletionTail(); err != nil {
		store.disk.poisoned.Store(true)
		return err
	}
	candidate, err := cloneState(store.state)
	if err != nil {
		return err
	}
	// commitLocked derives the committed revision by incrementing once, so the
	// checkpoint can name the revision this commit lands on.
	candidate.OperationsCheckpoint = candidate.Revision + 1
	if err := store.commitLocked(candidate); err != nil {
		return err
	}
	// The whole point of the checkpoint is that a reader may trust the payload
	// without the sidecar, so freeze the store and verify the fact it claims
	// instead of assuming it: a later writer would silently take it back.
	store.checkpointed = true
	if !operationsCheckpointMaterialized(store.state) {
		return errors.New("operation checkpoint did not materialize the operation view")
	}
	return nil
}

// publishCredentialsLocked republishes the digest -> key view. Callers hold the state
// lock, so the snapshot a reader loads is always a whole committed view.
func (store *stateStore) publishCredentialsLocked() {
	keys := make(map[string]APIKey, len(store.state.APIKeys))
	for _, key := range store.state.APIKeys {
		if key.Digest != "" {
			keys[key.Digest] = key
		}
	}
	store.credentials.Store(&keys)
}

func (store *stateStore) credentialByDigest(digest string) (APIKey, bool, error) {
	// Lock-free by design: the snapshot is immutable once published, and the health
	// check reads atomics, so no request serializes here behind a commit's fsync.
	if err := store.disk.available(); err != nil {
		return APIKey{}, false, err
	}
	keys := store.credentials.Load()
	if keys == nil {
		return APIKey{}, false, errors.New("credential view unavailable")
	}
	key, found := (*keys)[digest]
	return key, found, nil
}

func (store *stateStore) authorizationState(keyID, projectID string, database controller.DatabaseID) (State, error) {
	return store.requestState(keyID, projectID, &database)
}

// requestState is the one narrow read a gateway request needs: the project quotas and the
// database descriptor its admission checks look at, plus the credential and descriptor
// authorization decides from. Reading those separately queued the same request for the
// same state lock twice, and the lock is where the request path serializes.
func (store *stateStore) requestState(keyID, projectID string, database *controller.DatabaseID) (State, error) {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return State{}, err
	}
	view := State{
		APIKeys: map[string]APIKey{}, Projects: map[string]Project{},
		Databases: map[string]Database{}, Keys: map[string]KeyDescriptor{},
	}
	if key, found := store.state.APIKeys[keyID]; found {
		view.APIKeys[keyID] = key
	}
	if database != nil {
		if record, found := store.state.Databases[database.Key()]; found {
			view.Databases[database.Key()] = record
			if project, found := store.state.Projects[record.Project]; found {
				view.Projects[record.Project] = project
			}
		}
		if descriptor, found := store.state.Keys[namespaceKey(database.Namespace)]; found {
			descriptor.Envelope = nil
			view.Keys[namespaceKey(database.Namespace)] = descriptor
		}
	}
	if project, found := store.state.Projects[projectID]; found {
		view.Projects[projectID] = project
	}
	return view, nil
}

func readRequestState(store StateStore, keyID, projectID string, database *controller.DatabaseID) (State, error) {
	if narrow, ok := store.(interface {
		requestState(string, string, *controller.DatabaseID) (State, error)
	}); ok {
		return narrow.requestState(keyID, projectID, database)
	}
	return store.Snapshot()
}

func readAuthorizationState(store StateStore, keyID, projectID string, database controller.DatabaseID) (State, error) {
	if narrow, ok := store.(interface {
		authorizationState(string, string, controller.DatabaseID) (State, error)
	}); ok {
		return narrow.authorizationState(keyID, projectID, database)
	}
	return store.Snapshot()
}

func (store *stateStore) gatewayRequestState(projectID string, database *controller.DatabaseID) (State, error) {
	return store.requestState("", projectID, database)
}

func readGatewayRequestState(store StateStore, projectID string, database *controller.DatabaseID) (State, error) {
	if narrow, ok := store.(interface {
		gatewayRequestState(string, *controller.DatabaseID) (State, error)
	}); ok {
		return narrow.gatewayRequestState(projectID, database)
	}
	return store.Snapshot()
}

func (store *stateStore) Update(mutation func(*State) error) error {
	defer store.lockOperationMutationState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if store.checkpointed {
		return ErrStateFrozen
	}
	if mutation == nil {
		return errors.New("state mutation required")
	}
	// The registry payload carries the complete operation view and becomes the base the
	// log's next deltas bind to, so every ordered append has to be durable and published
	// before the payload is cloned: an edit that only lives in the view would otherwise be
	// recorded as though the log had acknowledged it.
	if err := store.settleOperations(); err != nil {
		return err
	}
	candidate, err := cloneState(store.state)
	if err != nil {
		return err
	}
	if err := mutation(&candidate); err != nil {
		return err
	}
	return store.commitLocked(candidate)
}

// commitLocked validates and commits one candidate as the next revision. The
// caller holds the state lock and has already decided the candidate's contents.
func (store *stateStore) commitLocked(candidate State) error {
	if candidate.Version != 1 || candidate.Revision != store.state.Revision || candidate.Revision == ^uint64(0) {
		return errors.New("state revision/version cannot be changed or exhausted")
	}
	candidate.Revision++
	committed, err := cloneState(candidate)
	if err != nil {
		return err
	}
	if err := validateState(committed, &store.state); err != nil {
		return err
	}
	if err := store.disk.commit(committed.Revision, committed); err != nil {
		return err
	}
	// The registry payload already carries the complete operation view, so the
	// operation log only has to record the deltas that follow this revision; the
	// next operation mutation binds the new one.
	store.state = committed
	store.publishCredentialsLocked()
	// Only Update and opening replace the whole view; only those paths invalidate
	// the derived index. Operation-only commits retain the same map.
	store.operationsIndex = nil
	return nil
}

// Close stops the commit queue first: a queued append that was never written fails, the
// barrier already in flight still reports its own result, and only then does the registry
// file close, so no acknowledged append can be left without its durable record. Closing also
// settles what is left, so the final view keeps every confirmed record and drops only the
// appends whose barrier refused them.
func (store *stateStore) Close() error {
	unlock := store.lockState()
	store.maintenanceClosing = true
	unlock()
	store.maintenance.Wait()
	operations := store.operations.close()
	unlock = store.lockState()
	settled := store.settleOperations()
	compactionFailure := store.operationCompactionFailure
	unlock()
	return errors.Join(operations, settled, compactionFailure, store.disk.close())
}

func cloneState(state State) (State, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return State{}, errors.New("invalid state JSON")
	}
	var cloned State
	err = json.Unmarshal(encoded, &cloned)
	return cloned, err
}
