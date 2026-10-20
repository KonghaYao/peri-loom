// The browser clients' single authority for the gateway SQL failure contract.
//
// It is the JavaScript counterpart of peri-loom/platform/sqlcontract, and it
// exists for the same reason: the console, the legacy rpc client and the
// TypeScript SDK must not each carry their own copy of "does this reply prove
// what happened to the statement". A reply that proves the statement settled —
// including one the platform proved cannot have committed, such as a describe or
// a read cursor — is a definite answer whatever code it carries; only a reply
// without that proof is an unknown outcome.
//
// The Go side asserts that CODES below matches sqlcontract.Codes(), so a code
// added to the platform fails this file's portability test until it is listed
// here, and every consumer that imports this module is extended at once.

// CODES is the published failure vocabulary, with the class of each code.
export const CODES = {
  sql_error: "statement",
  sql_timeout: "statement",
  sql_budget_exceeded: "statement",
  sql_conflict: "statement",
  sql_unavailable: "statement",
  readonly_session: "statement",
  upstream_rejected: "statement",
  result_limit: "statement",
  outcome_unknown: "uncertain",
  audit_unavailable: "uncertain",
  operation_incomplete: "uncertain",
  database_isolated: "uncertain",
  invalid_request: "request",
  invalid_session: "request",
  session_busy: "session",
  session_moved: "session",
  session_unusable: "session",
  gateway_closing: "capacity",
  quota_exceeded: "capacity",
  unauthenticated: "access",
  unauthorized: "access",
  forbidden: "access",
  token_binding_mismatch: "access",
  readonly_placement_required: "access",
  sql_limit: "limit",
  request_limit: "limit",
  placement_unavailable: "placement",
  metadata_unavailable: "platform",
  upstream_unavailable: "platform",
  operation_recorded: "platform",
  operation_registration_unavailable: "platform",
  release_fencing_proof_unavailable: "platform",
  release_not_retired: "platform",
  release_scope_mismatch: "platform",
  release_stop_proof_missing: "platform",
  recovery_restricted: "platform",
  unavailable: "platform",
  not_found: "unsupported",
  method_not_allowed: "unsupported",
  writer_profile_unavailable: "unsupported",
};

export const classOf = code => CODES[code] ?? "unrecognized";

// proofOf decodes the two evidence fields the gateway publishes. A missing field
// proves nothing, and only (settled, committed), (settled, not committed) and
// (rejected, not committed) are pairs the platform produces; anything else is
// contradictory and proves nothing rather than half a fact.
export function proofOf(completed, committed) {
  if (typeof completed !== "boolean" || typeof committed !== "boolean") return "unproven";
  if (completed) return "settled";
  return committed ? "unproven" : "rejected";
}

// uncertain reports whether the answer leaves the statement's fate unresolved:
// the platform says so, a statement-level answer proves nothing, or the code is
// not part of the published contract.
export function uncertain(failure) {
  const proof = proofOf(failure.execution_completed, failure.may_have_committed);
  switch (classOf(failure.code)) {
    case "uncertain":
    case "unrecognized":
      return true;
    case "statement":
      return proof === "unproven";
    default:
      return false;
  }
}

// unknown reports whether a client must treat the answer as an unknown outcome.
// A proven failure never is, whatever code or status the platform answered it
// with; an unproven one is unknown when the platform itself failed.
export function unknown(failure, status) {
  if (failure.code === "outcome_unknown") return true;
  if (proofOf(failure.execution_completed, failure.may_have_committed) !== "unproven") return false;
  if (classOf(failure.code) === "statement") return true;
  return typeof status === "number" && status >= 500;
}

// executed and commitRisk are what a client renders about the statement.
export const executed = failure => proofOf(failure.execution_completed, failure.may_have_committed) === "settled";
export const commitRisk = failure =>
  proofOf(failure.execution_completed, failure.may_have_committed) === "settled" && failure.may_have_committed === true ||
  failure.possible_commit === true;
