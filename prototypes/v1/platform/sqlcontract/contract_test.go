package sqlcontract

import (
	"encoding/json"
	"testing"
)

// TestEvidenceDecodesEveryProduciblePair pins what the two published fields
// mean. The contract review that produced this package found four consumers
// reading the pair as "completed == committed", which calls the platform's own
// proof for a describe or a read cursor — settled, and proven not to commit —
// "no evidence" and turns a definite answer into an unknown outcome.
func TestEvidenceDecodesEveryProduciblePair(t *testing.T) {
	for _, scenario := range []struct {
		name                   string
		completed, committed   *bool
		proof                  Proof
		executed, rejected     bool
		mayHaveCommitted, sure bool
	}{
		{"absent", nil, nil, Unproven, false, false, false, false},
		{"committed-only", nil, flag(true), Unproven, false, false, true, false},
		{"settled-committing", flag(true), flag(true), Settled, true, false, true, true},
		{"settled-non-committing", flag(true), flag(false), Settled, true, false, false, true},
		{"rejected", flag(false), flag(false), Rejected, false, true, false, true},
		// A commit warning the platform published survives even a pairing that
		// proves nothing about execution: dropping it would read as "safe to
		// replay" for a write the platform warned may have landed.
		{"contradictory", flag(false), flag(true), Unproven, false, false, true, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			evidence := EvidenceFrom(scenario.completed, scenario.committed)
			if evidence.Proof() != scenario.proof || evidence.Executed() != scenario.executed ||
				evidence.Rejected() != scenario.rejected || evidence.Proven() != scenario.sure ||
				evidence.CommitRisk() != scenario.mayHaveCommitted {
				t.Fatalf("evidence decoded as %+v", evidence)
			}
		})
	}
}

// TestVerdictNeverHidesACommitRisk checks the producer side: a settled statement
// keeps the commit warning of its operation, and only an operation that cannot
// commit may report a settled statement without one.
func TestVerdictNeverHidesACommitRisk(t *testing.T) {
	for _, scenario := range []struct {
		executed, committing        bool
		completed, mayHaveCommitted bool
	}{
		{false, false, false, false},
		{false, true, false, false},
		{true, false, true, false},
		{true, true, true, true},
	} {
		completed, mayHaveCommitted := Verdict(scenario.executed, scenario.committing)
		if completed != scenario.completed || mayHaveCommitted != scenario.mayHaveCommitted {
			t.Fatalf("verdict(%t,%t) = %t,%t", scenario.executed, scenario.committing, completed, mayHaveCommitted)
		}
	}
}

// TestUnknownFollowsProofAndNotTheCode is the rule three rounds of review
// disagreed about. The expectation below states the client contract in full,
// independently of the implementation: the platform's own unresolved answer is
// always unknown, a proved failure never is, a statement-level answer that
// proves nothing always is, and anything else follows the platform's own
// failure status.
func TestUnknownFollowsProofAndNotTheCode(t *testing.T) {
	for _, vector := range Vectors() {
		failure := vector.Failure()
		want := false
		switch {
		case failure.Code() == "outcome_unknown":
			want = true
		case failure.Proof() != Unproven:
			want = false
		case failure.Class() == Statement:
			want = true
		default:
			want = vector.Status >= 500
		}
		if failure.Unknown(vector.Status) != want {
			t.Fatalf("%s unknown=%t want %t", vector.Name, failure.Unknown(vector.Status), want)
		}
		// The two invariants the contract exists for: a proved failure is never
		// an unknown outcome, and an unknown outcome is never a proved one.
		if failure.Proof() != Unproven && failure.Class() != Uncertain && failure.Unknown(vector.Status) {
			t.Fatalf("%s: a proven failure became an unknown outcome", vector.Name)
		}
	}
}

// TestEveryVectorAgreesWithTheVocabulary checks that the conformance kit covers
// the whole contract and that no vector is silently unclassified.
func TestEveryVectorAgreesWithTheVocabulary(t *testing.T) {
	seen := map[string]bool{}
	for _, vector := range Vectors() {
		failure := vector.Failure()
		if failure.Class() == Unrecognized {
			t.Fatalf("%s: vector outside the vocabulary", vector.Name)
		}
		if failure.Code() != vector.Code {
			t.Fatalf("%s: code lost", vector.Name)
		}
		seen[vector.Code] = true
	}
	for _, code := range Codes() {
		if !seen[code] {
			t.Fatalf("code %s has no conformance vector", code)
		}
	}
}

// TestFailureObjectShapeIsClosed checks that a reply the platform can never
// publish cannot become an accepted answer: a failure without a code, or with a
// non-boolean evidence flag, is malformed rather than a definite refusal.
func TestFailureObjectShapeIsClosed(t *testing.T) {
	for _, body := range []string{
		`{}`, `null`, `[]`, `"code"`, `{"code":42}`, `{"code":""}`,
		`{"code":"sql_error","execution_completed":"true"}`,
		`{"code":"sql_error","possible_commit":"yes"}`,
		`{"code":"sql_error","rpc_observation":{"ambiguous":true}}`,
	} {
		var failure Failure
		if err := json.Unmarshal([]byte(body), &failure); err == nil {
			t.Fatalf("malformed failure object accepted: %s", body)
		}
	}
	// An explicit null is read as an absent field, exactly as the pointer-based
	// decoders that preceded this package read it.
	var absent Failure
	if err := json.Unmarshal([]byte(`{"code":"sql_error","may_have_committed":null}`), &absent); err != nil || absent.Proven() {
		t.Fatalf("null evidence was not read as absent: %+v %v", absent, err)
	}
	var failure Failure
	if err := json.Unmarshal([]byte(`{"code":"sql_error","execution_completed":true,"may_have_committed":true,"operation_id":"op","possible_commit":true,"rpc_observation":{"ambiguous":false,"rejected":false,"execution_completed":true}}`), &failure); err != nil {
		t.Fatal(err)
	}
	if len(failure.Unexpected()) != 0 || !failure.MayHaveCommitted() || failure.OperationID() != "op" {
		t.Fatalf("published failure object rejected: %+v", failure)
	}
	observation, found := failure.Observation()
	if !found || !observation.Executed() || observation.Ambiguous() || observation.Rejected() {
		t.Fatalf("observation lost: %+v", observation)
	}
	// possible_commit is a warning, not proof: it must not promote an unproven
	// reply into a proven one.
	var warned Failure
	if err := json.Unmarshal([]byte(`{"code":"outcome_unknown","possible_commit":true}`), &warned); err != nil {
		t.Fatal(err)
	}
	if warned.Proven() || !warned.MayHaveCommitted() || !warned.Unknown(502) {
		t.Fatalf("possible_commit changed the proof: %+v", warned)
	}
}

// TestUnexpectedKeysAreReported keeps the strict consumers strict after their
// envelope decodes a failure through this package.
func TestUnexpectedKeysAreReported(t *testing.T) {
	var failure Failure
	if err := json.Unmarshal([]byte(`{"code":"sql_error","invented":1,"another":2}`), &failure); err != nil {
		t.Fatal(err)
	}
	unexpected := failure.Unexpected()
	if len(unexpected) != 2 || unexpected[0] != "another" || unexpected[1] != "invented" {
		t.Fatalf("unexpected keys = %v", unexpected)
	}
}
