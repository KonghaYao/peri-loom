# Gateway SDKs and management CLI

Go: `peri-loom/platform/sdk/go`. TypeScript: `sdk/typescript/client.ts`, compiled with
the installed TypeScript compiler (`tsc -p sdk/typescript`). Rust: `peri_loom_v1::sdk`.
All three clients target actual `/v1/sql/open`, `/execute`, `/query`, `/close` endpoints
and correlate version-1 envelopes with a bounded request ID (1–64 alphanumeric,
underscore or hyphen characters). Default body limits are 1 MiB request / 8 MiB reply.

## No replay contract

Both **execute and query may write**. Clients dispatch once; they never retry SQL,
reopen sessions, restore transactions, or replay `COMMIT`. A transport failure,
malformed ACK, or oversized response is typed `outcome_unknown` with the request ID.
Such a session, or a `session_moved` session, rejects further SQL locally. Resolve an
uncertain operation using application-level evidence before explicitly opening a new
session. A new session does not resume the previous transaction. Cancellation after
dispatch is also uncertain. Closing is explicit; no destructor performs network I/O.

An acknowledged terminal native SQL error is `sql_error` with
`execution_completed=true` and `may_have_committed=true`. It preserves the opaque
session for an **explicit** caller-issued `ROLLBACK` or subsequent statement. These
flags never prove rollback or absence of a committed multi-statement prefix. Missing
or malformed terminal flags are uncertain and poison the session. No SDK performs
automatic rollback or retry.

A statement the Worker proved it never entered execution is `sql_error` with
`execution_completed=false` and `may_have_committed=false`. The Gateway also reports
`sql_timeout`, `sql_budget_exceeded`, `sql_conflict`, `sql_unavailable`, and
`readonly_session` for the failure classes a caller must act on differently, each
with an `Error.Message`. Legacy `result_limit`, `sql_limit`, `upstream_rejected`,
and `upstream_unavailable` keep their meanings.

Management accepts the server whitelist: database create/delete/branch/clone/snapshot/
restore/restore-point/pitr/drop-snapshot/retain-after/suspend/resume/migrate/status/
refs/schema/checkpoint/compact/gc/gc-inspect/rewrap, plus canonical organization,
project, key, and token endpoints. Bare database operation names and `databases/name`
are supported; arbitrary paths and invented commands are not. **`pitr` resolves a
recovery point; `restore-point` explicitly restores it.** A new namespace requires a
writer initialization; existing encrypted branches can open native read-only.

## Authentication and TLS

HTTP is permitted only for literal loopback IP addresses, never `localhost` or an
external hostname. External access requires normally verified HTTPS. Go accepts an
explicit root CA pool; Rust accepts a PEM trust anchor through `ClientOptions`;
TypeScript uses normal browser/Node trust (configure Node's extra CA file before
startup if needed). No insecure TLS flag, credential-in-URL, redirects, or API-key
logging is supported. Request errors and debug representations redact credentials.

```go
client, err := sdk.NewClient(endpoint, privateCredential, sdk.Options{})
if err != nil { return err }
session, err := client.Open(ctx, database)
if err != nil { return err }
defer session.Close(ctx)
rows, err := session.Query(ctx, "SELECT id FROM records", "read-1")
```

Go's generic row decoder uses `Decoder.UseNumber()`: numeric cells are `json.Number`,
not implicit `float64`. Type-assert `json.Number` before an explicit `Int64()` or
other application-selected conversion; calling `Float64()` can lose integer precision.
Re-marshaling unchanged rows retains numeric wire literals. Rust's
`serde_json::Value` retains signed `i64` and unsigned `u64` integers.

TypeScript exposes recursive `SqlValue`, including `bigint`. Its raw JSON parser
constructs values directly: **integer lexical tokens** outside the safe-number range
become `bigint`; safe integers and decimal/exponent tokens remain `number`. Quoted
digits and escaped SQL text remain strings, with no placeholder collisions. Nested
values and management replies use the same lossless parser; large epochs can be
`bigint` in the otherwise `unknown` management result. Wire numbers remain ordinary
JSON literals: there is no schema tag change. Duplicate fields and malformed JSON
still fail closed without replaying SQL. Management request numeric fields continue
to require safe integer `number` values; unsafe input is rejected before dispatch.

```ts
const rows = await session.query("SELECT id FROM records", "read-1");
const display = JSON.stringify(rows, (_key, value) =>
  typeof value === "bigint" ? value.toString() : value);
```

This display conversion renders big integers as decimal strings without rounding;
plain `JSON.stringify` does not accept `bigint`. It is not a request replay or a wire
schema change. Replies remain buffered JSON with explicit bounds; SDKs do not
advertise an unimplemented NDJSON parser.

## CLI

Build `go build -o peri ./cmd/peri` from the platform module. Credentials come only
from `PERI_API_KEY` or an owner-controlled regular mode-0600 `--credential-file` /
`PERI_CREDENTIAL_FILE`; symlinks and public files are rejected. Never supply API keys
as CLI arguments. `PERI_ENDPOINT` and `PERI_CA_FILE` are supported.

```sh
peri status --database local_default/demo/main
peri sql --database local_default/demo/main --query 'SELECT count(*) FROM records'
peri sql --database local_default/demo/main --execute 'CREATE TABLE records(id INTEGER)'
peri shell --database local_default/demo/main
peri gc --database local_default/demo/main
peri gc --database local_default/demo/main --collect --safety-window-ms 60000
```

The shell retains one opaque native Gateway session. Commands are `:execute SQL`,
`:query SQL`, `:reconnect`, and `:quit`; there is no inferred read-only SQL mode.
After migration or uncertain outcome it asks for explicit reconnection instead of
replaying statements. GC defaults to inspection; deletion requires `--collect`.
CLI operations include create/delete/snapshot/branch/clone/restore/restore-point/pitr/
suspend/resume/migrate/status/checkpoint/schema/compact/refs/gc.

Unit tests use local HTTP mocks to verify wire schemas, bounds, TLS trust, lost ACKs,
and no-replay behavior. They are **not** native SQL durability or deployment acceptance;
the actual authority/worker/gateway restart and S3 gates remain separate.
