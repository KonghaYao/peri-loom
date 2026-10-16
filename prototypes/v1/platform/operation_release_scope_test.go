package platform

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	controller "peri-loom/controller"
)

// releaseScopeBranch names a sibling branch of the security namespace, which is what
// makes a release responsible for more than the branch whose quarantine it ends.
func releaseScopeBranch(branch string) controller.DatabaseID {
	database := securityDatabase()
	database.Branch = branch
	return database
}

// releaseScopeFence answers per branch, so a test can tell whether the release
// collected one attestation for every branch in its scope or reused a single one.
// An unconfigured branch refuses, which keeps an unexpected scope from passing.
type releaseScopeFence struct {
	fences map[string]IsolationFence
	errs   map[string]error
	calls  []string
}

func (verifier *releaseScopeFence) VerifyIsolationFence(_ context.Context, database controller.DatabaseID) (IsolationFence, error) {
	verifier.calls = append(verifier.calls, database.Key())
	if err := verifier.errs[database.Key()]; err != nil {
		return IsolationFence{}, err
	}
	fence, found := verifier.fences[database.Key()]
	if !found {
		return IsolationFence{}, errors.New("authority fence unavailable")
	}
	return fence, nil
}

type releaseScopeFixture struct {
	management *Management
	store      *stateStore
	proof      *releaseTestStopProof
	fence      *releaseScopeFence
	kms        KMS
}

// registerBranch adds one branch to the platform's own desired state. A serviceable
// branch is one the platform could put back in service the moment the isolation ends,
// which is what makes the release responsible for fencing it.
func (fixture *releaseScopeFixture) registerBranch(test *testing.T, database controller.DatabaseID, serviceable bool) {
	test.Helper()
	entry := Database{ID: database, Project: "alpha", State: "active"}
	if serviceable {
		fixture.registerNamespaceKey(test, database.Namespace)
		entry.Encrypted, entry.RootIncarnation, entry.BranchIncarnation = true, "root-a", 1
	}
	if err := fixture.store.Update(func(state *State) error {
		state.Databases[database.Key()] = entry
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

func (fixture *releaseScopeFixture) descriptor(test *testing.T, namespace controller.Namespace) (KeyDescriptor, bool) {
	test.Helper()
	state, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	descriptor, exists := state.Keys[namespaceKey(namespace)]
	return descriptor, exists
}

// registerNamespaceKey stores the ready key descriptor an encrypted active branch
// needs before the platform can serve it again.
func (fixture *releaseScopeFixture) registerNamespaceKey(test *testing.T, namespace controller.Namespace) {
	test.Helper()
	if _, exists := fixture.descriptor(test, namespace); exists {
		return
	}
	descriptor := KeyDescriptor{ID: "descriptor", ProvisionID: "provision", Revision: 2, Namespace: namespace, RootIncarnation: "root-a", DomainID: "domain", DEKID: "dek", Cipher: "aegis256", State: "ready"}
	var dek [32]byte
	for index := range dek {
		dek[index] = byte(index)
	}
	envelope, err := fixture.kms.Seal(dek[:], descriptorAAD(descriptor))
	if err != nil {
		test.Fatal(err)
	}
	descriptor.Envelope = envelope
	if err := fixture.store.Update(func(state *State) error {
		state.Keys[namespaceKey(namespace)] = descriptor
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

// quarantineBranches drives every named branch to a durable quarantine whose local
// retirement already completed. Admission is namespace-wide, so every intent is
// registered before any of them turns terminal.
func quarantineBranches(test *testing.T, store StateStore, databases ...controller.DatabaseID) {
	test.Helper()
	for _, database := range databases {
		if err := beginOperation(store, database, "unknown", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
	}
	for _, database := range databases {
		if err := finishOperation(store, database, "unknown", testRPCError(true, false, SQLRPCError{Ambiguous: true}), false); err != nil {
			test.Fatal(err)
		}
	}
	for _, database := range databases {
		if err := store.Update(func(state *State) error {
			key := operationKey(database, "unknown")
			record, exists := state.Operations[key]
			if !exists {
				return errors.New("fixture quarantine missing")
			}
			record.Decision, record.DecisionRequestID = "retain_isolation_new_database_required", "retain_unknown"
			state.Operations[key] = record
			return nil
		}); err != nil {
			test.Fatal(err)
		}
		if err := store.Update(func(state *State) error {
			key := operationKey(database, "unknown")
			record := state.Operations[key]
			record.RetirementPending = true
			state.Operations[key] = record
			return nil
		}); err != nil {
			test.Fatal(err)
		}
		if err := store.Update(func(state *State) error {
			key := operationKey(database, "unknown")
			record := state.Operations[key]
			record.LocalResourcesRetired, record.RetirementRequestID, record.RetirementPending = true, "retire_unknown", false
			state.Operations[key] = record
			return nil
		}); err != nil {
			test.Fatal(err)
		}
	}
}

func newReleaseScopeFixture(test *testing.T) *releaseScopeFixture {
	test.Helper()
	store := operationDurableStore(test).(*stateStore)
	proof := &releaseTestStopProof{failures: map[string]error{}}
	fence := &releaseScopeFence{fences: map[string]IsolationFence{}, errs: map[string]error{}}
	management := &Management{config: ManagementConfig{State: store, Identity: releaseTestIdentity{}, StopProof: proof, Fence: fence}}
	return &releaseScopeFixture{management: management, store: store, proof: proof, fence: fence, kms: securityKMS(test)}
}

func (fixture *releaseScopeFixture) release(test *testing.T, requestID string, database controller.DatabaseID) error {
	test.Helper()
	_, err := fixture.management.releaseIsolation(context.Background(), Principal{Subject: "operator", Project: "alpha", Role: "admin"}, managementRequest{RequestID: requestID, Database: database})
	return err
}

func (fixture *releaseScopeFixture) record(test *testing.T, database controller.DatabaseID, identifier string) OperationRecord {
	test.Helper()
	state, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	return state.Operations[operationKey(database, identifier)]
}

func (fixture *releaseScopeFixture) isolated(test *testing.T, database controller.DatabaseID) bool {
	test.Helper()
	state, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	_, found := isolatedOperation(state, database, false)
	return found
}

// One branch's attestation is not another branch's proof. A sibling quarantine that
// still has an owner keeps the namespace isolated, and when both are fenced each
// record carries the epoch the authority reported for its own branch.
func TestIsolationReleaseFencesEveryQuarantinedBranch(test *testing.T) {
	main, feature := securityDatabase(), releaseScopeBranch("feature")
	fixture := newReleaseScopeFixture(test)
	fixture.registerBranch(test, feature, false)
	quarantineBranches(test, fixture.store, main, feature)
	fixture.fence.fences[main.Key()] = IsolationFence{Mode: "detached", Epoch: 12}
	fixture.fence.fences[feature.Key()] = IsolationFence{Mode: "writer", Epoch: 9, Bound: true}

	if err := fixture.release(test, "release_partial", main); !errors.Is(err, errReleaseFenceUnavailable) {
		test.Fatalf("release accepted a still-owned sibling branch: %v", err)
	}
	if record := fixture.record(test, main, "unknown"); record.ReleaseRequestID != "" {
		test.Fatal("refused release mutated the named branch")
	}
	if record := fixture.record(test, feature, "unknown"); record.ReleaseRequestID != "" {
		test.Fatal("refused release mutated the sibling branch")
	}
	if !fixture.isolated(test, main) {
		test.Fatal("refused release cleared namespace isolation")
	}
	if len(fixture.fence.calls) != 1 || fixture.fence.calls[0] != feature.Key() {
		test.Fatalf("refused release proved %v, want the unfenced branch it stopped at", fixture.fence.calls)
	}
	fixture.fence.calls = nil

	fixture.fence.fences[feature.Key()] = IsolationFence{Mode: "detached", Epoch: 13}
	if err := fixture.release(test, "release_proven", main); err != nil {
		test.Fatalf("release with both branches fenced refused: %v", err)
	}
	if len(fixture.fence.calls) != 2 {
		test.Fatalf("proven release collected %v, want one attestation per branch", fixture.fence.calls)
	}
	for _, key := range []string{main.Key(), feature.Key()} {
		if !slices.Contains(fixture.fence.calls, key) {
			test.Fatalf("proven release never read %s: %v", key, fixture.fence.calls)
		}
	}
	mainRecord := fixture.record(test, main, "unknown")
	featureRecord := fixture.record(test, feature, "unknown")
	if mainRecord.ReleaseRequestID != "release_proven" || mainRecord.ReleaseAuthorityMode != "detached" || mainRecord.ReleaseEpoch != 12 {
		test.Fatalf("named branch recorded the wrong proof: %+v", mainRecord.OperationStatus)
	}
	if featureRecord.ReleaseRequestID != "release_proven" || featureRecord.ReleaseAuthorityMode != "detached" || featureRecord.ReleaseEpoch != 13 {
		test.Fatalf("sibling branch recorded another branch's epoch: %+v", featureRecord.OperationStatus)
	}
	if fixture.isolated(test, main) {
		test.Fatal("proven release kept namespace isolation")
	}
}

// The gate the release opens is namespace-wide, so a sibling branch with no
// quarantine record at all still has to be fenced: it would otherwise resume on the
// very epoch the unproven write happened in, and no proof would mention it.
func TestIsolationReleaseFencesUnquarantinedSiblingBranches(test *testing.T) {
	main, feature := securityDatabase(), releaseScopeBranch("feature")
	fixture := newReleaseScopeFixture(test)
	fixture.registerBranch(test, feature, true)
	quarantineForRelease(test, fixture.store, main, "unknown")
	fixture.fence.fences[main.Key()] = IsolationFence{Mode: "detached", Epoch: 12}
	fixture.fence.fences[feature.Key()] = IsolationFence{Mode: "writer", Epoch: 9, Bound: true}

	if err := fixture.release(test, "release_sibling_owned", main); !errors.Is(err, errReleaseFenceUnavailable) {
		test.Fatalf("release ignored a serving sibling branch: %v", err)
	}
	if record := fixture.record(test, main, "unknown"); record.ReleaseRequestID != "" {
		test.Fatal("refused release mutated the quarantine")
	}
	if !fixture.isolated(test, main) {
		test.Fatal("refused release cleared namespace isolation")
	}

	fixture.fence.fences[feature.Key()] = IsolationFence{Mode: "revoked", Epoch: 13}
	if err := fixture.release(test, "release_sibling_fenced", main); err != nil {
		test.Fatalf("release with the sibling fenced refused: %v", err)
	}
	if record := fixture.record(test, main, "unknown"); record.ReleaseRequestID != "release_sibling_fenced" {
		test.Fatalf("release did not record the proof: %+v", record.OperationStatus)
	}
	if state, err := fixture.store.Snapshot(); err != nil {
		test.Fatal(err)
	} else if len(state.Operations) != 1 {
		test.Fatalf("fencing a sibling branch wrote %d records, want the single quarantine", len(state.Operations))
	}
}

// A branch that cannot serve again needs no fencing proof: a suspended or never
// activated sibling is behind a fresh activation, which is a new grant on a new
// epoch.
func TestIsolationReleaseIgnoresBranchesThatCannotServe(test *testing.T) {
	main, suspended, provisioned := securityDatabase(), releaseScopeBranch("feature"), releaseScopeBranch("reporting")
	fixture := newReleaseScopeFixture(test)
	if err := fixture.store.Update(func(state *State) error {
		state.Databases[suspended.Key()] = Database{ID: suspended, Project: "alpha", State: "suspended", Encrypted: true, RootIncarnation: "root-a", BranchIncarnation: 1}
		state.Databases[provisioned.Key()] = Database{ID: provisioned, Project: "alpha", State: "provisioning", Encrypted: true, RootIncarnation: "root-a", BranchIncarnation: 1}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	quarantineForRelease(test, fixture.store, main, "unknown")
	fixture.fence.fences[main.Key()] = IsolationFence{Mode: "detached", Epoch: 12}

	if err := fixture.release(test, "release_active_only", main); err != nil {
		test.Fatalf("branches that cannot serve blocked the release: %v", err)
	}
	if len(fixture.fence.calls) != 1 || fixture.fence.calls[0] != main.Key() {
		test.Fatalf("release proved %v, want only the branch it ends a quarantine for", fixture.fence.calls)
	}
}

// The request names the database whose quarantine it ends. Naming a branch that
// carries none of them would record that request as the proof of branches the
// operator never asked about, and would read as a release of a database that is
// still isolated.
func TestIsolationReleaseRefusesARequestThatNamesNoQuarantine(test *testing.T) {
	main, feature := securityDatabase(), releaseScopeBranch("feature")
	fixture := newReleaseScopeFixture(test)
	fixture.registerBranch(test, feature, true)
	quarantineForRelease(test, fixture.store, feature, "unknown")
	fixture.fence.fences[main.Key()] = IsolationFence{Mode: "detached", Epoch: 12}
	fixture.fence.fences[feature.Key()] = IsolationFence{Mode: "detached", Epoch: 13}

	if err := fixture.release(test, "release_wrong_branch", main); !errors.Is(err, errReleaseScopeMismatch) {
		test.Fatalf("release of an unrelated branch accepted: %v", err)
	}
	if len(fixture.fence.calls) != 0 || len(fixture.proof.calls) != 0 {
		test.Fatalf("scope refusal still collected proofs: %v %v", fixture.fence.calls, fixture.proof.calls)
	}
	if record := fixture.record(test, feature, "unknown"); record.ReleaseRequestID != "" {
		test.Fatal("scope refusal released the quarantine anyway")
	}
	if !fixture.isolated(test, main) {
		test.Fatal("scope refusal cleared namespace isolation")
	}

	if err := fixture.release(test, "release_named_branch", feature); err != nil {
		test.Fatalf("release of the named branch refused: %v", err)
	}
	if record := fixture.record(test, feature, "unknown"); record.ReleaseRequestID != "release_named_branch" || record.ReleaseEpoch != 13 {
		test.Fatalf("named branch release not recorded: %+v", record.OperationStatus)
	}
}

// The refusal reaches the API as its own code instead of collapsing into "not
// retired" or a silent success.
func TestIsolationReleaseScopeRefusalIsPublished(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	management.config.Identity, management.config.Audit = fixture.identity, fixture.audit
	_, token, err := NewAPIKey(fixture.state, "operator", "project-a", "admin", false)
	if err != nil {
		test.Fatal(err)
	}
	sibling := fixture.first
	sibling.Branch = "feature"
	if err := fixture.state.Update(func(state *State) error {
		state.Databases[sibling.Key()] = Database{ID: sibling, Project: "project-a", State: "active", Encrypted: true, RootIncarnation: "root-a", BranchIncarnation: 1}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	quarantineForRelease(test, fixture.state, fixture.first, "unknown")
	proof := &releaseTestStopProof{failures: map[string]error{}}
	management.config.StopProof = proof
	fence := &releaseTestFence{fence: IsolationFence{Mode: "detached", Epoch: 4}}
	management.config.Fence = fence
	code := func(test *testing.T, database controller.DatabaseID) (int, string) {
		test.Helper()
		response := operationHTTP(test, management, token, "/v1/databases/isolation/release", managementRequest{RequestID: "release_scope", Database: database})
		var reply struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(response.Body.Bytes(), &reply)
		return response.Code, reply.Error.Code
	}
	if status, observed := code(test, sibling); status != 409 || observed != "release_scope_mismatch" {
		test.Fatalf("scope refusal published as %d %q", status, observed)
	}
	if fence.calls != 0 {
		test.Fatal("scope refusal collected a fencing proof")
	}
	if status, observed := code(test, fixture.first); status != 200 || observed != "" {
		test.Fatalf("named branch release refused: %d %q", status, observed)
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	if released := state.Operations[operationKey(fixture.first, "unknown")].ReleaseRequestID; released != "release_scope" {
		test.Fatalf("named branch release not recorded: %q", released)
	}
}
