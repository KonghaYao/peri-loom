// Package sqlcontract is the single authority for the gateway's SQL failure
// contract: which failure codes the platform publishes, what a reply proves
// about the statement it answers, and whether that answer leaves the statement's
// fate unknown.
//
// Every consumer — the Hrana adapters, the Go SDK, the TypeScript SDK, the
// browser and console clients and the stress harnesses — must decide through
// this package. The evidence fields are unexported and have no raw accessors, so
// a consumer can neither read them off the wire nor reconstruct the rule: it
// receives a decoded Failure and asks it. Consumers outside this module import
// it through a go.mod replace, which is what keeps a new consumer from writing
// its own predicate. Failures the platform answers with a code this package does
// not publish are deliberately not "safe": they count as an unresolved outcome
// rather than as a definite answer.
//
// Two decisions are deliberately separate. Uncertain is the semantic answer a
// harness reports: the reply says nothing about a statement it answers, or the
// platform itself reports an unresolved outcome. Unknown is the conservative
// client contract, which also refuses to call any platform-side failure (a 5xx)
// a definite answer even when nothing was dispatched. See
// .peri/plans/errmap-fix3-report.md for why the two are not the same question.
package sqlcontract

import (
	"encoding/json"
	"errors"
	"sort"
)

// Proof is what a failure reply proves about the statement it names.
type Proof int

const (
	// Unproven: the reply carries no usable evidence, so the statement's fate
	// stays unknown. Missing, contradictory and unreadable evidence are all
	// Unproven: a reply that cannot be decoded never becomes a definite answer.
	Unproven Proof = iota
	// Rejected: the reply proves the statement never entered execution.
	Rejected
	// Settled: the reply proves the statement entered execution and settled.
	// Settling is not a rollback, so a settled statement keeps its commit risk.
	Settled
)

// Evidence is the platform's proof about the statement a failure names. Both
// fields of the client reply are decoded into it here and nowhere else: they are
// unexported so that no consumer can read a raw evidence flag and write a second
// predicate over it.
type Evidence struct {
	proof     Proof
	committed bool
}

// EvidenceFrom decodes the two fields the gateway publishes about a statement. A
// missing field proves nothing, and the legal pairs are exactly (settled,
// committed), (settled, not committed) for an operation that cannot commit, and
// (rejected, not committed). Any other pairing is contradictory or partial, so
// it proves nothing rather than half of a fact — but a commit warning the
// platform did publish is kept even then, because dropping it would tell a
// caller that a write it cannot account for is safe to replay.
func EvidenceFrom(completed, committed *bool) Evidence {
	if completed == nil || committed == nil {
		// Half an answer proves nothing about execution, and the commit warning
		// the platform published is kept even then.
		return Evidence{committed: committed != nil && *committed}
	}
	switch {
	case *completed && *committed:
		return Evidence{proof: Settled, committed: true}
	case *completed:
		return Evidence{proof: Settled}
	case *committed:
		return Evidence{committed: true}
	default:
		return Evidence{proof: Rejected}
	}
}

// EvidenceFromDirections decodes evidence that is kept as two directions rather
// than as the wire pair: a worker's own execution flag and a durable operation
// record both state "settled" or "never entered execution" outright. It is
// decoded here rather than reinterpreted by each reader, and a source that
// claims both directions proves nothing.
func EvidenceFromDirections(executed, rejected, mayHaveCommitted bool) Evidence {
	switch {
	case executed && !rejected:
		return Evidence{proof: Settled, committed: mayHaveCommitted}
	case rejected && !executed:
		return Evidence{proof: Rejected}
	default:
		return Evidence{}
	}
}

// Verdict is the platform's answer about a statement that failed, and the only
// place the two evidence fields of a client reply are produced. A statement that
// entered execution keeps the commit risk of its operation, and only an
// operation that cannot commit — a describe or a cursor bound to a read — may
// report a settled statement without one.
func Verdict(executed, committing bool) (executionCompleted, mayHaveCommitted bool) {
	return executed, executed && committing
}

func (evidence Evidence) Proof() Proof   { return evidence.proof }
func (evidence Evidence) Executed() bool { return evidence.proof == Settled }
func (evidence Evidence) Rejected() bool { return evidence.proof == Rejected }

// Proven reports whether the reply states what happened to the statement. It is
// the whole condition for a proven failure, and a proven failure is never an
// unknown outcome and never aborts a pipeline.
func (evidence Evidence) Proven() bool { return evidence.proof != Unproven }

// CommitRisk reports whether the statement may have taken effect. It is false
// only when the reply says the statement did not settle while committing, so a
// consumer never reads an unproven statement as rolled back and never reads a
// settled one as safe to replay.
func (evidence Evidence) CommitRisk() bool { return evidence.committed }

// Class is the part of the platform that answers a failure. Consumers switch on
// the class rather than on code names, so a code added to the vocabulary is
// classified everywhere at once instead of falling through every closed table.
type Class int

const (
	// Unrecognized: the code is not part of the published contract.
	Unrecognized Class = iota
	// Statement: the failure answers a statement the platform dispatched.
	Statement
	// Uncertain: the platform itself reports an unresolved outcome.
	Uncertain
	// Request: the request was refused before dispatch because it is malformed.
	Request
	// Limit: refused before dispatch by a size limit.
	Limit
	// Access: refused before dispatch by a credential or permission rule.
	Access
	// Session: refused before dispatch by the session's own lifecycle.
	Session
	// Capacity: refused before dispatch by admission control or shutdown.
	Capacity
	// Placement: refused before dispatch because no worker holds the placement.
	Placement
	// Platform: the platform could not answer; nothing was dispatched.
	Platform
	// Unsupported: no such endpoint, method or profile.
	Unsupported
)

// vocabulary is the published failure contract. A code is added here — and
// nowhere else — when the platform gains a new answer, which is what extends
// every consumer and every conformance test at the same time.
var vocabulary = []struct {
	code  string
	class Class
}{
	// Statement failures. The worker or the platform answered a dispatch, so the
	// reply must carry proof of what happened to the statement.
	{"sql_error", Statement},
	{"sql_timeout", Statement},
	{"sql_budget_exceeded", Statement},
	{"sql_conflict", Statement},
	{"sql_unavailable", Statement},
	{"readonly_session", Statement},
	{"upstream_rejected", Statement},
	{"result_limit", Statement},
	// The platform's own unresolved outcomes.
	{"outcome_unknown", Uncertain},
	{"audit_unavailable", Uncertain},
	{"operation_incomplete", Uncertain},
	{"database_isolated", Uncertain},
	// Refusals raised before anything was dispatched.
	{"invalid_request", Request},
	{"invalid_session", Request},
	{"session_busy", Session},
	{"session_moved", Session},
	{"session_unusable", Session},
	{"gateway_closing", Capacity},
	{"quota_exceeded", Capacity},
	{"unauthenticated", Access},
	{"unauthorized", Access},
	{"forbidden", Access},
	{"token_binding_mismatch", Access},
	{"readonly_placement_required", Access},
	{"sql_limit", Limit},
	{"request_limit", Limit},
	{"placement_unavailable", Placement},
	{"metadata_unavailable", Platform},
	{"upstream_unavailable", Platform},
	{"operation_recorded", Platform},
	{"operation_registration_unavailable", Platform},
	{"release_fencing_proof_unavailable", Platform},
	{"release_not_retired", Platform},
	{"release_scope_mismatch", Platform},
	{"release_stop_proof_missing", Platform},
	{"recovery_restricted", Platform},
	{"unavailable", Platform},
	{"not_found", Unsupported},
	{"method_not_allowed", Unsupported},
	{"writer_profile_unavailable", Unsupported},
}

// Codes returns every published failure code. Tests and consumers enumerate the
// contract through this call, so adding a code to the vocabulary adds it to
// every coverage matrix without a second edit.
func Codes() []string {
	codes := make([]string, 0, len(vocabulary))
	for _, entry := range vocabulary {
		codes = append(codes, entry.code)
	}
	return codes
}

func classOf(code string) Class {
	for _, entry := range vocabulary {
		if entry.code == code {
			return entry.class
		}
	}
	return Unrecognized
}

// publishedFields is the key set the gateway publishes on a failure object. A
// strict consumer keeps its own acceptance rule through Unexpected.
var publishedFields = map[string]bool{
	"code": true, "message": true, "execution_completed": true, "may_have_committed": true,
	"operation_id": true, "stage": true, "business_outcome": true, "cleanup_outcome": true,
	"execution_proven": true, "possible_commit": true, "ambiguous": true, "rejected": true,
	"retry_allowed": true, "rpc_observation": true,
}

// Observation is the platform's own diagnostic view of the RPC that produced a
// failure: whether the exchange was ambiguous, and what the worker's reply
// proved. It is decoded here so that the nested object is not read field by
// field either, and it is diagnostics only: the decision a client makes comes
// from the failure's own evidence.
type Observation struct {
	ambiguous bool
	rejected  bool
	executed  bool
}

func (observation Observation) Ambiguous() bool { return observation.ambiguous }
func (observation Observation) Rejected() bool  { return observation.rejected }
func (observation Observation) Executed() bool  { return observation.executed }

// Failure is the platform's answer to a failed request, as its clients receive
// it. Every field is unexported, so a consumer decides only through the methods
// below and cannot read an evidence flag or a code name off the wire.
type Failure struct {
	code        string
	message     string
	evidence    Evidence
	commitWarn  bool
	ambiguous   bool
	proven      bool
	retry       bool
	operation   string
	stage       string
	business    string
	cleanup     string
	observation Observation
	observed    bool
	unexpected  []string
}

var errMalformedFailure = errors.New("malformed failure object")

// UnmarshalJSON decodes the failure object. It reports malformed input rather
// than accepting a partial object, and it rejects a code that is not a string or
// evidence that is not a boolean, so a value the platform can never publish
// cannot become an accepted answer.
func (failure *Failure) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return errMalformedFailure
	}
	if json.Unmarshal(fields["code"], &failure.code) != nil || failure.code == "" {
		return errMalformedFailure
	}
	if raw, present := fields["message"]; present {
		if json.Unmarshal(raw, &failure.message) != nil {
			return errMalformedFailure
		}
	}
	var completed, committed *bool
	if raw, present := fields["execution_completed"]; present {
		if json.Unmarshal(raw, &completed) != nil {
			return errMalformedFailure
		}
	}
	if raw, present := fields["may_have_committed"]; present {
		if json.Unmarshal(raw, &committed) != nil {
			return errMalformedFailure
		}
	}
	// possible_commit is the platform's warning that an unresolved outcome may
	// still have committed. It is not evidence of execution, so it never turns an
	// unproven reply into a proven one, but it is part of the commit risk a
	// client renders.
	failure.commitWarn = false
	if raw, present := fields["possible_commit"]; present {
		if json.Unmarshal(raw, &failure.commitWarn) != nil {
			return errMalformedFailure
		}
	}
	failure.evidence = EvidenceFrom(completed, committed)
	failure.ambiguous, failure.proven, failure.retry = false, false, false
	failure.operation, failure.stage, failure.business, failure.cleanup = "", "", "", ""
	failure.observed = false
	for key, target := range map[string]any{
		"ambiguous": &failure.ambiguous, "execution_proven": &failure.proven, "retry_allowed": &failure.retry,
		"operation_id": &failure.operation, "stage": &failure.stage,
		"business_outcome": &failure.business, "cleanup_outcome": &failure.cleanup,
	} {
		if raw, present := fields[key]; present {
			if json.Unmarshal(raw, target) != nil {
				return errMalformedFailure
			}
		}
	}
	if raw, present := fields["rpc_observation"]; present {
		var observation struct {
			Ambiguous          *bool `json:"ambiguous"`
			Rejected           *bool `json:"rejected"`
			ExecutionCompleted *bool `json:"execution_completed"`
		}
		if json.Unmarshal(raw, &observation) != nil || observation.Ambiguous == nil || observation.Rejected == nil || observation.ExecutionCompleted == nil {
			return errMalformedFailure
		}
		failure.observation = Observation{ambiguous: *observation.Ambiguous, rejected: *observation.Rejected, executed: *observation.ExecutionCompleted}
		failure.observed = true
	}
	failure.unexpected = failure.unexpected[:0]
	for key := range fields {
		if !publishedFields[key] {
			failure.unexpected = append(failure.unexpected, key)
		}
	}
	sort.Strings(failure.unexpected)
	return nil
}

// Code is the platform's name for the failure, for pass-through to the caller.
func (failure Failure) Code() string { return failure.code }

// Message is the platform's own text, never the worker's.
func (failure Failure) Message() string { return failure.message }

// Class reports which part of the platform answered.
func (failure Failure) Class() Class { return classOf(failure.code) }

// Proof reports what the reply proves about the statement.
func (failure Failure) Proof() Proof { return failure.evidence.proof }

// Executed reports whether the reply proves the statement entered execution and
// settled.
func (failure Failure) Executed() bool { return failure.evidence.Executed() }

// Rejected reports whether the reply proves the statement never entered
// execution.
func (failure Failure) Rejected() bool { return failure.evidence.Rejected() }

// Proven reports whether the reply states what happened to the statement.
func (failure Failure) Proven() bool { return failure.evidence.Proven() }

// MayHaveCommitted reports whether the answer leaves a commit unaccounted for: a
// settled statement on an operation that can commit, or the platform's own
// warning that an unresolved statement may still have committed. It is the flag
// a client renders, and it is never false for a statement the reply proved
// settled while committing.
func (failure Failure) MayHaveCommitted() bool {
	return failure.evidence.CommitRisk() || failure.commitWarn
}

// Unexpected lists the failure keys the platform does not publish, sorted. A
// consumer that validates the reply's shape reads this instead of decoding the
// object a second time.
func (failure Failure) Unexpected() []string { return failure.unexpected }

// Ambiguous reports whether the platform marked the RPC itself as ambiguous.
func (failure Failure) Ambiguous() bool { return failure.ambiguous }

// ExecutionProven reports whether the platform proved the operation behind the
// failure executed.
func (failure Failure) ExecutionProven() bool { return failure.proven }

// RetryAllowed reports whether the platform allows the caller to retry.
func (failure Failure) RetryAllowed() bool { return failure.retry }

// OperationID, Stage, BusinessOutcome and CleanupOutcome are the platform's own
// correlation and durable-state labels for an unknown outcome.
func (failure Failure) OperationID() string     { return failure.operation }
func (failure Failure) Stage() string           { return failure.stage }
func (failure Failure) BusinessOutcome() string { return failure.business }
func (failure Failure) CleanupOutcome() string  { return failure.cleanup }

// Observation reports the platform's diagnostic view of the RPC, when the
// platform published one.
func (failure Failure) Observation() (Observation, bool) {
	return failure.observation, failure.observed
}

// ReadOnlyCandidate reports whether a statement-level failure is one the Hrana
// adapter re-probes with a describe before answering, so that a statement the
// worker refused only because it was dispatched as a writer can still be served
// as a read. The set is exactly the pre-existing trigger set — the worker's own
// statement error and the platform's result budget — and it lives here so the
// adapter does not compare a code name itself.
func (failure Failure) ReadOnlyCandidate() bool {
	switch failure.code {
	case "sql_error", "result_limit":
		return true
	}
	return false
}

// Uncertain reports whether the answer leaves a statement's fate unresolved: the
// platform says so through a diagnostic that means "unresolved", a statement
// level answer proves nothing, or the code is not part of the published contract.
// It is the semantic question a harness asks about a reply, independent of the
// transport status.
func (failure Failure) Uncertain() bool {
	switch failure.Class() {
	case Uncertain, Unrecognized:
		return true
	case Statement:
		return !failure.evidence.Proven()
	}
	return false
}

// Unknown reports whether a client must treat the answer as an unknown outcome —
// the only condition that may abort a pipeline, drop a connection or raise an
// unknown result. It is the conservative client contract rather than the
// harness's semantic question, and it is decided by proof, not by the code name:
//
//   - the platform's own unresolved answer is always unknown;
//   - a reply that proved what happened to the statement never is, whatever code
//     or status carries it, because a proved failure is a definite answer, and
//     that includes the diagnostics that report a durable record rather than a
//     statement;
//   - a failure about a dispatched statement that proves nothing is unknown;
//   - any other platform failure (a 5xx) is unknown, because the platform did not
//     establish whether the statement was dispatched.
func (failure Failure) Unknown(status int) bool {
	if failure.code == "outcome_unknown" {
		return true
	}
	if failure.evidence.Proven() {
		return false
	}
	if failure.Class() == Statement {
		return true
	}
	return status >= 500
}
