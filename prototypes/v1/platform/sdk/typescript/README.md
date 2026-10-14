# SDK error metadata

`request_id` (`PeriError.requestID`, Rust `SdkError.request_id`) is correlation
only. It is not a deduplication key. The SDK does not automatically retry SQL or
management requests, even when a response contains `retry_allowed: true`.

F03 errors may expose `operationID`, `stage`, `businessOutcome`, `cleanupOutcome`,
`executionProven`, `ambiguous`, `rejected`, and `retryAllowed`. Rust exposes the
equivalent snake_case fields as `Option` values. Missing metadata remains absent;
the SDK never substitutes the request ID for a server-issued operation ID.

The two response forms intentionally remain distinct:

- Management errors carry flat `execution_completed`, `may_have_committed`,
  `ambiguous`, and `rejected` observations.
- Gateway errors carry `possible_commit` and an optional `rpc_observation` object.
  TypeScript exposes `possibleCommit` and `rpcObservation`; Rust exposes
  `possible_commit` and `rpc_observation`. Nested completion observations do not
  become the legacy flat SQL completion flags.

`outcome_unknown` stays unknown even if execution was observed as completed.
Business completion with unknown cleanup does not make a session reusable.
Legacy `sql_error` still requires both flat completion flags to be `true` before
explicit rollback/reuse is permitted.

All new fields are optional for compatibility with code-only errors, but fields
present with wrong types, nulls, unsupported stage/outcome enums, or unknown keys
fail closed as `outcome_unknown`. A present RPC observation requires exactly
three boolean fields: `ambiguous`, `rejected`, and `execution_completed`.

Management supports the defined `operations`, `operations/decision`,
`operations/clear`, `operations/retire-local`, and `diagnostics` paths, including
their `databases/`-prefixed forms. Pass `operation_id` explicitly for record
selection. The only supported `decision` value is
`retain_isolation_new_database_required`; this does not authorize replay or
clearing isolation.

Local checks: `npm run build`, `npm run typecheck`, and `npm test` in this
directory; `cargo test --test p6_sdk` from `prototypes/v1` for the Rust SDK.
