package platform

import "strings"

// The worker reports SQL failures as prose: the evidence context first, then the
// causal chain, for example
//
//	SQL execution settled: writer admission deadline: query duration budget exceeded
//	SQL execution not entered: prepare remote query: Parse error: no such table: t
//
// The response body is the platform's contract, so every worker failure is
// classified into the closed vocabulary below and answered with a platform
// message: the worker text stays in the gateway log. Unrecognised text keeps the
// conservative answer instead of guessing, and never becomes success.
//
// The phrase table mirrors worker_rpc::diagnostics and the budget, admission and
// ownership literals in prototypes/v1/src/session. Remove this classification once
// the worker ships its own cause class on the SQL reply (see
// .peri/plans/error-mapping-report.md).
type sqlFailureClass int

const (
	sqlFailureUnknown sqlFailureClass = iota
	sqlFailureSyntax
	sqlFailureConstraint
	sqlFailureReadOnly
	sqlFailureDeadline
	sqlFailureComputeBudget
	sqlFailureResultBudget
	sqlFailureTextBudget
	sqlFailureConflict
	sqlFailureRefused
)

var sqlFailurePhrases = []struct {
	phrase string
	class  sqlFailureClass
}{
	// Preparation runs before execution, so a statement that cannot be prepared
	// is a client mistake even when a later phrase would say otherwise.
	{"prepare remote query", sqlFailureSyntax},
	{"prepare remote SQL statement", sqlFailureSyntax},
	{"Parse error", sqlFailureSyntax},
	{"syntax error", sqlFailureSyntax},
	{"no such table", sqlFailureSyntax},
	{"no such column", sqlFailureSyntax},
	{"multiple SQL statements not supported", sqlFailureSyntax},
	{"empty remote SQL statement", sqlFailureSyntax},
	{"constraint failed", sqlFailureConstraint},
	{"attempt to write a readonly database", sqlFailureReadOnly},
	{"query duration budget exceeded", sqlFailureDeadline},
	{"writer admission deadline", sqlFailureDeadline},
	// A cold load has its own budget, so it must not answer with a statement deadline:
	// the statement never entered execution, and the caller may retry it unchanged.
	{"database load budget exceeded", sqlFailureRefused},
	{"VM step budget exceeded", sqlFailureComputeBudget},
	{"result row budget exceeded", sqlFailureResultBudget},
	{"result byte budget exceeded", sqlFailureResultBudget},
	{"SQL byte budget exceeded", sqlFailureTextBudget},
	{"writer admission conflict", sqlFailureConflict},
	{"writer admission queue full", sqlFailureRefused},
	{"writer admission closing", sqlFailureRefused},
	{"writer admission quarantine", sqlFailureRefused},
	// The admission refusals the worker encodes as a bare rejection body
	// (worker_rpc/protocol.rs AdmissionRejection): the request was turned away
	// before it entered execution or any queue, so the statement is refused
	// rather than unknown and the caller may resend it.
	{"worker execution lane saturated", sqlFailureRefused},
	{"worker connection limit reached", sqlFailureRefused},
	{"worker saturated", sqlFailureRefused},
	{"local ownership revoked or detached", sqlFailureRefused},
	{"local ownership epoch mismatch", sqlFailureRefused},
	{"scoped store generation revoked", sqlFailureRefused},
	{"Fenced lease unavailable", sqlFailureRefused},
	{"Fenced lease identity", sqlFailureRefused},
	{"Fenced root identity", sqlFailureRefused},
	{"Fenced ownership identity", sqlFailureRefused},
	{"LeaseExpired", sqlFailureRefused},
	{"lease clock moved backwards", sqlFailureRefused},
	{"draining rejects new transactions", sqlFailureRefused},
	// Weakest token last: an authority role check reports the bare word.
	{"ReadOnly", sqlFailureReadOnly},
}

// sqlFailure is the platform answer for a worker-reported SQL failure. The
// evidence flags repeat the worker's own proof: executed means the statement
// entered execution and settled (not that it rolled back), rejected means it
// never entered execution.
type sqlFailure struct {
	status   int
	code     string
	message  string
	executed bool
	rejected bool
}

func classifySQLFailure(text string) sqlFailureClass {
	for _, candidate := range sqlFailurePhrases {
		if strings.Contains(text, candidate.phrase) {
			return candidate.class
		}
	}
	return sqlFailureUnknown
}

// sqlWorkerFailure maps a non-ambiguous worker failure onto the client contract.
// It reports false when the reply proves nothing about execution, which stays an
// unknown outcome rather than becoming a definite answer.
func sqlWorkerFailure(reply *SQLRPCError) (sqlFailure, bool) {
	if reply == nil || reply.Ambiguous {
		return sqlFailure{}, false
	}
	if !reply.evidence.Proven() {
		// Unproven, or a reply that claimed both directions at once, states
		// nothing about the statement and must not become a definite answer.
		return sqlFailure{}, false
	}
	executed, rejected := reply.evidence.Executed(), reply.evidence.Rejected()
	outcome := func(status int, code, message string) (sqlFailure, bool) {
		return sqlFailure{status: status, code: code, message: message, executed: executed, rejected: rejected}, true
	}
	switch classifySQLFailure(reply.workerFailure) {
	case sqlFailureDeadline:
		return outcome(504, "sql_timeout", "statement did not finish within the worker per-statement deadline (PERI_SQL_MAX_DURATION_MS)")
	case sqlFailureComputeBudget:
		return outcome(400, "sql_budget_exceeded", "statement exceeded the worker per-statement compute budget (PERI_SQL_MAX_VM_STEPS)")
	case sqlFailureResultBudget:
		return outcome(413, "result_limit", "statement result exceeded the worker per-statement result budget (PERI_SQL_MAX_ROWS or PERI_SQL_MAX_RESULT_BYTES)")
	case sqlFailureTextBudget:
		return outcome(413, "sql_limit", "statement text exceeded the worker SQL size limit (PERI_SQL_MAX_BYTES)")
	case sqlFailureConflict:
		return outcome(409, "sql_conflict", "another writer holds the writer admission of this database")
	case sqlFailureRefused:
		return outcome(503, "sql_unavailable", "worker refused the statement before execution (admission, ownership or shutdown)")
	case sqlFailureSyntax:
		return outcome(400, "sql_error", "statement could not be prepared (SQL syntax, schema or statement shape)")
	case sqlFailureConstraint:
		return outcome(400, "sql_error", "statement violated a SQL constraint")
	case sqlFailureReadOnly:
		return outcome(403, "readonly_session", "worker refused a write on a session bound to a read-only placement")
	}
	if executed {
		return outcome(400, "sql_error", "statement failed during execution")
	}
	// A rejected failure without a recognised cause stays an upstream rejection:
	// the platform knows the statement never ran, but not who to blame.
	return outcome(502, "upstream_rejected", "worker rejected the statement before execution")
}

// sqlExecutedOutcome reports whether the result proves the statement reached the
// worker and settled. Only these outcomes may be recorded as executed, so a
// statement cancelled by a deadline must not be confused with an unrun one.
func sqlExecutedOutcome(result gatewayResult) bool {
	if result.code == "" || result.code == "result_limit" {
		return true
	}
	return result.rpc != nil && result.rpc.evidence.Executed()
}

// workerFailureLog bounds the worker text for the gateway log and keeps it on one
// line, so client-visible characters inside a failure cannot forge log entries.
func workerFailureLog(text string) string {
	var rendered strings.Builder
	for offset, character := range text {
		if offset >= 512 {
			rendered.WriteString("...")
			break
		}
		if character < ' ' || character == 0x7f {
			character = ' '
		}
		rendered.WriteRune(character)
	}
	return rendered.String()
}
