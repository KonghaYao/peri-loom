package platform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// These cases pin the R02 fix: a snapshot binds the chain position it was taken at, and
// that binding says nothing about the records stored there. Every fixture here rewrites
// the durable snapshot the way a stale copy, a partial restore or a hand repair does —
// the JSON stays schema-legal and every position field is kept.

// snapshotJournalFixture compacts a view that still holds an unresolved operation, so the
// snapshot carries an isolation fact whose log prefix is already gone: after the
// truncation nothing but the snapshot can restate that the operation is unsolved.
type snapshotJournalFixture struct {
	directory    string
	journalPath  string
	snapshotPath string
	unresolved   string
	preTruncate  [][]byte
	compacted    uint64
	compactedSum string
	records      int
}

func snapshotJournalSetup(test *testing.T) snapshotJournalFixture {
	test.Helper()
	store, journalPath := journalFixture(test)
	if err := beginOperation(store, securityDatabase(), "lost", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	journalCommit(test, store, "settled")
	fixture := snapshotJournalFixture{
		directory:    filepath.Dir(store.disk.path),
		journalPath:  journalPath,
		snapshotPath: store.operations.snapshotPath(),
		unresolved:   operationKey(securityDatabase(), "lost"),
		preTruncate:  journalLines(test, journalPath),
	}
	if err := store.operations.compact(store.state); err != nil {
		test.Fatal(err)
	}
	fixture.compacted, fixture.compactedSum = store.operations.revision, store.operations.digest
	fixture.records = len(store.state.Operations)
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	return fixture
}

func snapshotJournalBytes(test *testing.T, path string) []byte {
	test.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	return encoded
}

func snapshotJournalDecode(test *testing.T, encoded []byte) operationSnapshot {
	test.Helper()
	var snapshot operationSnapshot
	if err := strictDecode(encoded, &snapshot); err != nil {
		test.Fatal(err)
	}
	return snapshot
}

func snapshotJournalRead(test *testing.T, path string) operationSnapshot {
	test.Helper()
	return snapshotJournalDecode(test, snapshotJournalBytes(test, path))
}

// snapshotJournalWrite stores bytes only after proving a decoder would still accept them:
// a tamper case that broke the JSON shape would say nothing about the content binding.
func snapshotJournalWrite(test *testing.T, path string, encoded []byte) {
	test.Helper()
	snapshotJournalDecode(test, encoded)
	if err := atomicPrivate(path, encoded); err != nil {
		test.Fatal(err)
	}
}

func snapshotJournalRewrite(test *testing.T, path string, change func(*operationSnapshot)) {
	test.Helper()
	snapshot := snapshotJournalRead(test, path)
	change(&snapshot)
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		test.Fatal(err)
	}
	snapshotJournalWrite(test, path, encoded)
}

// snapshotJournalReseal also recomputes the content digest, which is what a writer able to
// replace the snapshot can do; the chain such a writer cannot rewrite still has to refuse
// what it produced. The file keeps the origin it already claimed, because only the content
// binding is being recomputed here.
func snapshotJournalReseal(test *testing.T, path string, change func(*operationSnapshot)) {
	test.Helper()
	snapshotJournalRewrite(test, path, func(snapshot *operationSnapshot) {
		change(snapshot)
		if err := sealSnapshot(snapshot, snapshot.Origin); err != nil {
			test.Fatal(err)
		}
	})
}

// legacyOperationSnapshot is the pre-envelope on-disk shape: it binds the chain position
// and nothing else, which is what every state directory written before the envelope holds.
type legacyOperationSnapshot struct {
	Version      uint32                     `json:"version"`
	BaseRevision uint64                     `json:"base_revision"`
	Revision     uint64                     `json:"revision"`
	Digest       string                     `json:"digest"`
	Sequence     uint64                     `json:"sequence"`
	Evictions    uint64                     `json:"evictions"`
	Records      map[string]OperationRecord `json:"records"`
}

func legacySnapshotShape(snapshot operationSnapshot) legacyOperationSnapshot {
	return legacyOperationSnapshot{Version: snapshot.Version, BaseRevision: snapshot.BaseRevision, Revision: snapshot.Revision, Digest: snapshot.Digest, Sequence: snapshot.Sequence, Evictions: snapshot.Evictions, Records: snapshot.Records}
}

// snapshotJournalDowngrade rewrites the durable snapshot in the legacy shape and returns
// the content the reseal has to keep unchanged.
func snapshotJournalDowngrade(test *testing.T, path string) legacyOperationSnapshot {
	test.Helper()
	legacy := legacySnapshotShape(snapshotJournalRead(test, path))
	encoded, err := json.Marshal(legacy)
	if err != nil {
		test.Fatal(err)
	}
	if err := atomicPrivate(path, encoded); err != nil {
		test.Fatal(err)
	}
	return legacy
}

func snapshotJournalExpectSealed(test *testing.T, path string, legacy legacyOperationSnapshot) {
	test.Helper()
	snapshot := snapshotJournalRead(test, path)
	if snapshot.EnvelopeVersion != operationSnapshotEnvelopeVersion || snapshot.Scope != operationSnapshotScope {
		test.Fatalf("snapshot carries no current envelope: %+v", snapshot)
	}
	digest, err := snapshot.contentDigest()
	if err != nil || len(snapshot.ContentDigest) != 64 || digest != snapshot.ContentDigest {
		test.Fatalf("snapshot content digest does not verify: %v", err)
	}
	if snapshot.Version != legacy.Version || snapshot.BaseRevision != legacy.BaseRevision || snapshot.Revision != legacy.Revision || snapshot.Digest != legacy.Digest || snapshot.Sequence != legacy.Sequence || snapshot.Evictions != legacy.Evictions || !reflect.DeepEqual(snapshot.Records, legacy.Records) {
		test.Fatal("resealing rewrote the snapshot instead of only binding it")
	}
	if snapshot.Origin != operationSnapshotOriginReseal {
		test.Fatalf("resealed snapshot does not record that a legacy acceptance happened: %q", snapshot.Origin)
	}
}

// Recovery turns an unresolved intent into an isolated Unknown: it may have committed,
// it is never retryable, and it never settles itself.
func snapshotJournalExpectQuarantined(test *testing.T, state State, key string) {
	test.Helper()
	record, exists := state.Operations[key]
	if !exists {
		test.Fatalf("recovery lost the unresolved intent %s", key)
	}
	if record.State != "quarantined" || !record.MayHaveCommitted || record.RetryAllowed {
		test.Fatalf("recovered intent is not isolated: %+v", record.OperationStatus)
	}
}

// The counterexample from the review: remove an unresolved record, keep every position
// field and every schema rule, and recovery used to apply a view in which the isolation
// fact had silently disappeared. validateState runs with no parent after compaction, so it
// cannot see a deleted record; only the content binding can refuse it.
func TestOperationJournalSnapshotRefusesContentTampering(test *testing.T) {
	for _, scenario := range []string{"unresolved_removed", "record_identity_changed", "sequence_changed", "evictions_changed"} {
		test.Run(scenario, func(test *testing.T) {
			fixture := snapshotJournalSetup(test)
			snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
				switch scenario {
				case "unresolved_removed":
					delete(snapshot.Records, fixture.unresolved)
				case "record_identity_changed":
					record := snapshot.Records[fixture.unresolved]
					record.Digest = strings.Repeat("a", 64)
					snapshot.Records[fixture.unresolved] = record
				case "sequence_changed":
					snapshot.Sequence++
				case "evictions_changed":
					snapshot.Evictions++
				}
			})
			reopened, err := journalReopen(test, fixture.directory)
			if err == nil {
				state, _ := reopened.Snapshot()
				reopened.Close()
				test.Fatalf("%s was accepted and recovered %d records", scenario, len(state.Operations))
			}
			if !strings.Contains(err.Error(), "content digest") {
				test.Fatalf("%s was refused by another layer than the content binding: %v", scenario, err)
			}
		})
	}
}

// The content digest and the chain position guard different things, so the assertions name
// the layer instead of only demanding an error. Recomputing the digest defeats the content
// binding; moving the snapshot, or replacing the journal hash it claims, is then still
// refused by the position the surviving chain and the witness prove.
func TestOperationJournalSnapshotLayersRefuseDifferentTampering(test *testing.T) {
	test.Run("content_changed_without_resealing", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		if _, err := journalReopen(test, fixture.directory); err == nil || !strings.Contains(err.Error(), "content digest") {
			test.Fatalf("content binding did not refuse a rewritten record set: %v", err)
		}
	})
	test.Run("position_moved_with_a_resealed_digest", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalReseal(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			snapshot.Revision, snapshot.Digest = fixture.compacted+4, strings.Repeat("b", 64)
		})
		if _, err := journalReopen(test, fixture.directory); err == nil || !strings.Contains(err.Error(), "chain") {
			test.Fatalf("chain position binding did not refuse a moved snapshot: %v", err)
		}
	})
	test.Run("journal_digest_replaced_with_a_resealed_digest", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalReseal(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			snapshot.Digest = strings.Repeat("c", 64)
		})
		if _, err := journalReopen(test, fixture.directory); err == nil || !strings.Contains(err.Error(), "chain") {
			test.Fatalf("the witness did not refuse a replaced journal digest: %v", err)
		}
	})
	// This is the boundary of an in-file digest, not an oversight: whoever replaces the
	// snapshot can recompute the digest over the replacement, and nothing in the same
	// trust domain can tell that apart from a legitimate compaction. Refusing it needs an
	// independent trusted anchor, which this format does not claim to have. If such an
	// anchor is added, this case has to be inverted together with the new proof.
	test.Run("resealed_content_is_not_detectable_locally", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalReseal(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatalf("a self-consistent snapshot was refused: %v", err)
		}
		defer reopened.Close()
		state, err := reopened.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		if _, exists := state.Operations[fixture.unresolved]; exists {
			test.Fatal("the fixture did not drop the unresolved record")
		}
	})
}

// The legacy acceptance path is also a downgrade channel, and this case names it instead of
// leaving it implicit. Dropping the envelope fields from a sealed snapshot makes it decode
// as a pre-envelope file, so a content change the content binding refuses above goes
// through once the envelope is gone — and it goes through once, because the reseal turns the
// result into an ordinary bound snapshot.
//
// It is accepted deliberately. A stripped file and a genuinely pre-envelope file are
// byte-identical, so no check can separate them, and the only policy that would refuse the
// stripped one is refusing every snapshot whose log prefix was already compacted away —
// exactly the clean restart this migration exists to keep. The downgrade is also not weaker
// than the alternative it replaces: dropping three keys and recomputing a digest are both
// available to whoever can rewrite the snapshot, and neither is detectable without an anchor
// outside this directory. What the format refuses to do is forget: the reseal stamps the
// content as legacy-accepted and that stamp outlives later compactions.
func TestOperationJournalSnapshotStrippedEnvelopeIsADocumentedDowngrade(test *testing.T) {
	test.Run("with_the_envelope_present_the_same_change_is_refused", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		if _, err := journalReopen(test, fixture.directory); err == nil || !strings.Contains(err.Error(), "content digest") {
			test.Fatalf("the content binding did not refuse the intact envelope: %v", err)
		}
	})
	test.Run("stripped_it_is_accepted_and_the_loss_becomes_durable", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		stripped := snapshotJournalDowngrade(test, fixture.snapshotPath)
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatalf("a stripped envelope is indistinguishable from a pre-envelope snapshot: %v", err)
		}
		state, err := reopened.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		if _, exists := state.Operations[fixture.unresolved]; exists {
			test.Fatal("the fixture did not drop the unresolved record")
		}
		if len(state.Operations) != fixture.records-1 {
			test.Fatalf("stripped snapshot recovered %d records, want %d", len(state.Operations), fixture.records-1)
		}
		if err := reopened.Close(); err != nil {
			test.Fatal(err)
		}
		// The loss is now durable and the file is bound again, so a later reader cannot tell
		// the record ever existed; only the origin records that it was not written natively.
		snapshotJournalExpectSealed(test, fixture.snapshotPath, stripped)
	})
	test.Run("the_origin_survives_a_later_compaction", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		snapshotJournalDowngrade(test, fixture.snapshotPath)
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		journalCommit(test, reopened, "after")
		if err := reopened.operations.compact(reopened.state); err != nil {
			test.Fatal(err)
		}
		if err := reopened.Close(); err != nil {
			test.Fatal(err)
		}
		materialized := snapshotJournalRead(test, fixture.snapshotPath)
		if materialized.Origin != operationSnapshotOriginReseal {
			test.Fatalf("a later compaction relabelled migrated content as natively bound: %q", materialized.Origin)
		}
		digest, err := materialized.contentDigest()
		if err != nil || digest != materialized.ContentDigest {
			test.Fatalf("re-materialized migrated content does not verify: %v", err)
		}
		if _, exists := materialized.Records[fixture.unresolved]; exists {
			test.Fatal("a record deleted before the migration reappeared after compaction")
		}
	})
}

// Compaction is durable in two steps, so recovery has to be correct on both sides of the
// truncation: while the prefix survives the chain still covers the snapshot, and once it is
// truncated the content binding is the only guard left. A rewritten payload is refused in
// both, and a crash between the steps still reconstructs the compacted view.
func TestOperationJournalSnapshotCrashPoints(test *testing.T) {
	test.Run("snapshot_durable_prefix_not_truncated", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		journalWriteState(test, fixture.journalPath, fixture.compacted, fixture.compactedSum, fixture.preTruncate)
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		defer reopened.Close()
		state, err := reopened.Snapshot()
		if err != nil || len(state.Operations) != fixture.records {
			test.Fatalf("crash before the truncation recovered %d records, want %d: %v", len(state.Operations), fixture.records, err)
		}
	})
	test.Run("snapshot_rewritten_before_truncation", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		journalWriteState(test, fixture.journalPath, fixture.compacted, fixture.compactedSum, fixture.preTruncate)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			delete(snapshot.Records, fixture.unresolved)
		})
		if _, err := journalReopen(test, fixture.directory); err == nil || !strings.Contains(err.Error(), "content digest") {
			test.Fatalf("a rewritten snapshot survived an intact chain: %v", err)
		}
	})
	test.Run("snapshot_durable_and_truncated", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		state, err := reopened.Snapshot()
		if err != nil {
			test.Fatal(err)
		}
		if len(state.Operations) != fixture.records {
			test.Fatalf("compact-only recovery returned %d records, want %d", len(state.Operations), fixture.records)
		}
		snapshotJournalExpectQuarantined(test, state, fixture.unresolved)
		reopened.Close()
	})
}

// A directory written before the envelope must still open: clean restart is a promise. The
// snapshot is accepted on the position the surviving chain proves, and it is immediately
// re-sealed with a content digest, which shrinks the window in which an unverifiable view
// is trusted from every startup to exactly this one.
func TestOperationJournalLegacySnapshotIsResealedOnOpen(test *testing.T) {
	test.Run("truncated_at_its_own_position", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		legacy := snapshotJournalDowngrade(test, fixture.snapshotPath)
		journalBefore := snapshotJournalBytes(test, fixture.journalPath)
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		state, err := reopened.Snapshot()
		if err != nil || len(state.Operations) != fixture.records {
			test.Fatalf("legacy snapshot recovered %d records, want %d: %v", len(state.Operations), fixture.records, err)
		}
		if err := reopened.Close(); err != nil {
			test.Fatal(err)
		}
		snapshotJournalExpectSealed(test, fixture.snapshotPath, legacy)
		if !reflect.DeepEqual(snapshotJournalBytes(test, fixture.journalPath), journalBefore) {
			test.Fatal("resealing touched the journal instead of only the snapshot")
		}
		sealed := snapshotJournalBytes(test, fixture.snapshotPath)
		second, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		if !reflect.DeepEqual(snapshotJournalBytes(test, fixture.snapshotPath), sealed) {
			test.Fatal("an already sealed snapshot was rewritten again")
		}
		second.Close()
	})
	test.Run("with_a_continuing_log", func(test *testing.T) {
		store, journalPath := journalFixture(test)
		if err := beginOperation(store, securityDatabase(), "lost", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
			test.Fatal(err)
		}
		if err := store.operations.compact(store.state); err != nil {
			test.Fatal(err)
		}
		journalCommit(test, store, "later")
		directory, snapshotPath := filepath.Dir(store.disk.path), store.operations.snapshotPath()
		records := len(store.state.Operations)
		if err := store.Close(); err != nil {
			test.Fatal(err)
		}
		if len(journalLines(test, journalPath)) != 2 {
			test.Fatal("fixture did not leave a continuing log")
		}
		legacy := snapshotJournalDowngrade(test, snapshotPath)
		reopened, err := journalReopen(test, directory)
		if err != nil {
			test.Fatal(err)
		}
		state, err := reopened.Snapshot()
		if err != nil || len(state.Operations) != records {
			test.Fatalf("legacy snapshot with a continuing log recovered %d records, want %d: %v", len(state.Operations), records, err)
		}
		snapshotJournalExpectQuarantined(test, state, operationKey(securityDatabase(), "lost"))
		if err := reopened.Close(); err != nil {
			test.Fatal(err)
		}
		snapshotJournalExpectSealed(test, snapshotPath, legacy)
	})
	// Unproven legacy content is never stamped as bound content: the chain has to prove the
	// position first, and a file that fails that proof stays exactly as it was found.
	test.Run("unprovable_position_is_refused_and_left_unsealed", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		snapshotJournalRewrite(test, fixture.snapshotPath, func(snapshot *operationSnapshot) {
			snapshot.Revision, snapshot.Digest = fixture.compacted+3, strings.Repeat("d", 64)
		})
		snapshotJournalDowngrade(test, fixture.snapshotPath)
		before := snapshotJournalBytes(test, fixture.snapshotPath)
		if reopened, err := journalReopen(test, fixture.directory); err == nil {
			reopened.Close()
			test.Fatal("a legacy snapshot outside the chain was accepted")
		}
		if !reflect.DeepEqual(snapshotJournalBytes(test, fixture.snapshotPath), before) {
			test.Fatal("an unproven legacy snapshot was re-sealed")
		}
	})
	test.Run("reseal_failure_refuses_the_open", func(test *testing.T) {
		fixture := snapshotJournalSetup(test)
		legacy := snapshotJournalDowngrade(test, fixture.snapshotPath)
		operations := filepath.Dir(fixture.snapshotPath)
		if err := os.Chmod(operations, 0500); err != nil {
			test.Fatal(err)
		}
		defer os.Chmod(operations, 0700)
		refused, openErr := journalReopen(test, fixture.directory)
		if err := os.Chmod(operations, 0700); err != nil {
			test.Fatal(err)
		}
		if openErr == nil {
			refused.Close()
			test.Fatal("a legacy snapshot was accepted without being re-sealed")
		}
		reopened, err := journalReopen(test, fixture.directory)
		if err != nil {
			test.Fatal(err)
		}
		reopened.Close()
		snapshotJournalExpectSealed(test, fixture.snapshotPath, legacy)
	})
}
