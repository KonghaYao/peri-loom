# P6 native cloud platform

This is the Go management, SQL gateway, key broker, and ownership controller for
the native Rust database services. It is a standalone Go module with a local
dependency on `../controller`. Initialize the pinned Turso submodule before building.

## Run the whole local project

From the repository root:

```sh
git submodule update --init prototypes/p0/turso
python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-local --tls
```

The launcher builds the React/TypeScript web console, native Rust service, and
Go platform in that order, then starts one
authority, two encrypted workers, and the platform, and prints a readiness
record only after authenticated provisioning. It seeds an organization, a
project, and an encrypted demo database in a new private directory. The
`connection.json` file contains synthetic API keys and a development CA path;
never publish it. Open its gateway endpoint in a browser and trust the disposable
development CA only for this fixture. Do not disable TLS verification in clients.
The browser console has project/database navigation, SQL editor, refs, operations,
access, and observability; see `frontend/README.md` for direct Vite development
and the required frontend-before-Go build order. Project API keys remain in
page memory only; if a user explicitly opts in, the console verifies the
global management API key and saves it in this browser's localStorage until
cleared. Such a key is readable by same-origin scripts/extensions and anyone
using the browser profile; do not enable this on shared devices. The global
key can open project-scoped native SQL sessions and preview rows; project keys remain available for application access.

The listener defaults to `127.0.0.1:9443`. `--listen` changes the endpoint;
`--tls` generates a disposable local certificate unless an existing private
certificate/key pair is supplied via `PERI_TLS_CERT` and `PERI_TLS_KEY`.
External listeners require TLS. SIGTERM performs joined shutdown; an uncertain
gateway operation makes process termination unsuccessful rather than proving
clean cancellation. A fully acknowledged, joined worker shutdown writes a
signed stop proof for that exact worker incarnation. The launcher refuses to
reuse any existing runtime, including one with no proofs or fully verified
proofs. The standalone proof verifier checks both workers' signed incarnation
and joined-exit records for Local and S3; it does not authorize restart. The
proofs do not currently attest the shutdown RPC acknowledgement or establish
old-process quiescence. The launcher holds a private, nonblocking exclusive
run-directory lock across startup and joined shutdown; this prevents competing
launchers, but does not authorize reuse. In-place restart still needs Authority
clean-marker verification, lease fencing, old-cache lifecycle
handling, and crash-atomic generation/configuration rotation. Reuse also requires
the platform payload to be the final view a shutdown checkpoint materialized
(`operations_checkpoint == revision`); operation records otherwise commit to a
sidecar that the launcher does not read, so a payload that may trail that sidecar
is refused as not materialized rather than trusted. Use a new fixture;
never erase proof records or ownership dirty markers to force a restart.

`--recover` reopens an existing, unclean run directory only to end an
isolation; it is not a second restart gate. It requires the owned worker stop
proofs and the clean Authority shutdown marker the previous stop left behind,
refuses a directory that already passes the unchanged clean gate, and archives
the failing `launcher-stop.json` as `launcher-stop.pre-recovery.json` before
starting the recovery instance. The recovery instance starts only the Authority and the platform
with `PERI_RECOVERY_ONLY=1`: no workers, no reconcile loop, no Hrana, and
`/v1/sql/`, `/db/` and `/` answer 503 `sql_unavailable` with `recovery:true`.
Only `operations`, `operations/decision`, `operations/retire-local`,
`isolation/release` and `status` are routed; every other management path answers
409 `recovery_restricted`, and capabilities report `recovery:true` and
`sql_enabled:false`. `isolation/release` still requires the stop proof, the local
retirement and the Authority's fencing attestation, and never clears the
ownership marker. The run becomes reusable only after a clean stop of that
instance followed by a normal start that passes the unchanged clean gate.
For a fresh fixture only, `PERI_BOOTSTRAP_KEY` can reuse a previously protected global key;
other fixture credentials and database contents are new. Never put the key in a command argument or log.

Private state, KMS, and audit heads use a bounded complete commit intent before
the floor/payload renames. Under the exclusive file lock, restart only rolls
forward exact validated parent/next-revision prefixes. Missing or unsafe files
on an established store, mismatched intent, and rollback without intent remain
fail-closed. Each business payload remains limited to 16 MiB; the temporary
two-payload intent has a separate physical limit of 32 MiB plus 16 KiB.
Audit-head recovery never creates or replays an append: the existing append
chain must still match the recovered sequence/hash, or opening the audit fails.
Persistence uncertainty remains an error until verified reopen; recovering a
file commit does not prove SQL execution, permit replay, or clear quarantine.

For an existing S3-compatible service, create a **dedicated bucket** first and
inject `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`, and
`S3_SECRET_ACCESS_KEY` through a private environment mechanism, then add
`--backend s3`. Only the Rust authority receives these variables. Workers and
the Go platform receive no raw S3 credentials. Never save real credentials in
source, argv, run configuration, or diagnostic logs.

## API and credentials

- `/v1/sql/open`, `/execute`, `/query`, `/close` manage opaque, principal-bound sessions.
- `GET /v1/platform/capabilities` reports the public gateway origin and whether experimental Hrana HTTP and WebSocket are actually enabled. With `PERI_HRANA_EXPERIMENTAL=1`, project SQL API keys can use `https://<host>/db/<tenant>/<database>/<branch>/` through the tested JS client's Hrana HTTP v2 transport, the Rust `turso_serverless` 0.1.3 client's v3 JSON pipeline/cursor transport, or `wss://<host>/db/<tenant>/<database>/<branch>/` through the low-level JS client's WebSocket v1/v2 JSON and v3 protobuf. The adapter defaults off; HTTP v3 JSON read cursors, and v3 protobuf read-only and write cursors on HTTP and WebSocket, fetch bounded native pages. A write cursor no longer requires `RETURNING`: without it the reply carries no columns or rows, and `step_end` reports `affected_row_count` and `last_insert_rowid`. HTTP v3 JSON read cursors have a 10,001-row regression; Rust SDK reads and a disposable transactional write batch passed against an isolated loopback HTTP gateway. TLS development certificates must be trusted by the client independently; the Rust SDK does not accept a custom CA through its public builder. Full protocol and remote session-store schema compatibility, as well as crash-restart recovery, are not yet proven.
- Query may execute SQL writes: native read-only mode, not SQL text filtering, enforces reader roles.
- `/v1/databases/*` exposes provisioning, lifecycle, refs, snapshots, clone, restore, PITR, checkpoint, and GC.
- `/v1/organizations/create` and `/v1/projects/create` require the global management key.
- Project keys manage only their project; the global management key may execute native SQL against any existing database using that database's project quotas, but cannot mint database tokens.
- `/v1/tokens` issues bounded-lifetime tokens tied to issuer, project, root, branch incarnation, and encryption domain.
- `/v1/keys/revoke` invalidates issuing keys and their database tokens.
- `/v1/metrics` requires a global management key and combines gateway and actual native process observations.

SQL clients should send optional `X-Request-ID` alongside the same JSON
`request_id`. The header must occur once and contain 1–128 ASCII letters,
digits, `-`, `_`, or `.`. It is transport correlation only, not authentication
or an idempotency guarantee. Valid headers correlate even early authentication
or quota denials without reading the body; malformed or duplicate headers fail
closed with a safe generated ID. A nonempty body ID must match the header or
the request fails before SQL dispatch. Headerless body-ID success remains
compatible, but early denials before body reading use a generated fallback ID
and cannot guarantee echoing a body-only ID. Admission still precedes body
parsing; clients must not weaken response-ID checks or retry SQL automatically.

SQL requests are never automatically retried. A lost reply may have committed;
use business idempotency keys and inspect durable state before reconnecting.
Worker SQL failures are answered with a machine-readable `error.code` and a
sanitized `error.message`; the Worker text stays in the Gateway log. `sql_error`
(400) is the statement itself failing, `sql_timeout` (504) the Worker
per-statement deadline, `sql_budget_exceeded` (400) the per-statement compute
budget, `result_limit`/`sql_limit` (413) the Worker result or SQL size budget,
`sql_conflict` (409) writer admission held by another writer, `sql_unavailable`
(503) a refused admission or lost ownership, and `readonly_session` (403) a write
on a read-only session. Settled failures carry
`execution_completed=true` plus `may_have_committed`; a statement the Worker
proved it never entered execution carries `execution_completed=false` and
`may_have_committed=false`. Only the Worker's own SQL reply states which of the
two happened: an answer the Gateway could not decode as that reply, including any
non-200 status on the Worker `/sql` call, is answered as `outcome_unknown` with
`possible_commit` rather than as a proven rejection. The session can issue an
explicit rollback, but a multi-statement committed prefix is not rolled back
automatically. Unknown
outcomes retain admission permits and poison the session instead of treating
timeout, close, or readback as cancellation proof; that transition is logged with
the request id, stage, database and the session it had, and with the Worker's own
verdict (`rpc=ambiguous=…,rejected=…,execution_completed=…`, or `rpc=none` when
the platform holds no verdict) and its failure text or the transport error
(`observation=`, `none` when neither exists), while the isolated-namespace
refusals it causes are audited without a second log line. Hrana pipeline errors
use that same contract: an unproven session or autocommit probe failure is
`outcome_unknown` with `outcome_unknown: true`, `retry_allowed: false`,
`possible_commit`, `execution_proven` and whatever operation id and stage the
platform holds, never a transport-specific code that hides the platform's
verdict.
Every client — the Hrana
adapters, the Go and TypeScript SDKs, the browser console and the stress
harnesses — decides from `peri-loom/platform/sqlcontract`, which owns the code
vocabulary, the meaning of the two evidence fields and the rule that a failure
proving what happened to a statement is never an unknown outcome. The two fields
are unexported and decoded by that package alone, and
`TestHranaAgreesWithTheFailureContract` in the platform package drives each
consumer over every code, evidence shape and status, so a code added to the
vocabulary cannot fall out of a consumer's table. Migration invalidates old
session bindings; reopen explicitly against the current placement.

Native SQL sessions idle for 5 minutes are closed by the Gateway's background
cleanup loop. Each completed request refreshes the idle deadline. Cleanup waits
for a confirmed Worker close before returning the session quota; an uncertain
close retains the session and quota for explicit recovery instead of growing the
durable operation log: a retained session is retried on a bounded account of
twelve attempts or ten minutes, with backoff. Every failed attempt is logged with
the session identity and counted in the cleanup ledger. A spent account has no
in-process exit — the session keeps its seat and its quota until an operator
retires it, and while it exists the launcher refuses a clean stop rather than
hiding the retained session. Clients should close sessions when finished and must
not rely on an idle session to keep an open transaction alive. Hrana streams keep
their separate idle timer.

JSON retains native integer literals. Go generic results use `json.Number`;
TypeScript and the browser console use `bigint` for unsafe integer literals.
NDJSON streams bounded, already materialized native rows; it is not an
incremental native-execution cursor.

## Security and persistence boundaries

The authority owns the only raw storage path. Private authenticated loopback
HTTP carries control, SQL, and key-broker traffic with distinct credentials.
New databases use native AEGIS-256 page encryption, including WAL images.
Every native session connects with the resolved key. Wrapped DEKs are bound to
namespace, immutable root incarnation, encryption domain, and key revision;
the broker checks fresh Rust ownership before and after unwrapping. There is
no plaintext fallback and no retroactive encryption claim for old plaintext databases.

The local KMS wraps DEKs with AES-GCM and stores its KEKs in a private,
file-backed development keyring, **not an HSM or external cloud KMS**. The KEK
file itself is not encrypted; host disk/root access can recover the KEKs and
unwrap DEKs. Database encryption protects stored page/WAL images without
promising protection from compromise of the key-management host.
Rotate a KEK and rewrap descriptors without changing
DEKs or the database domain. Keep old KEK versions required by retained envelopes.
DEKs remain available for reachable historical snapshots; deleting a branch is
not cryptographic erasure. Keys can exist in process memory; swap and hostile
container administrators are outside this prototype's threat boundary.

Controller/platform metadata use separately locked, checksummed, fsynced
atomic local stores with anti-rollback floors. Audit records exclude SQL text
and credentials and use a fsynced hash-chain head. Corruption, missing state,
and unresolved persistence outcomes fail closed. This is a single-authority,
single-platform prototype, not a consensus-backed or replicated HA deployment.
Coordinated rollback of every persistence file is not prevented by local floors.

Admission and rate limits are per gateway process/project. Native SQL has
cooperative VM/time/row/result budgets. Result accounting is not an RSS cap;
a large native scalar can allocate before a result check, and cooperative
timeouts cannot preempt a blocked storage operation or a single native function.
Audit storage rotates at 64 MiB per segment, retaining older segments on the
same volume. Startup verifies the full sequence and hash chain, including
archived segments, and resumes only validated interrupted rotations. Disk space
remains finite: there is no safe pre-dispatch audit-capacity reservation or
operator retention/backup policy yet, so disk exhaustion still fails closed.
After a trusted native SQL completion, an audit append failure returns
`audit_unavailable` without reclassifying the durable operation as Unknown or
poisoning its session. The reply includes `execution_completed` and forbids
retries; the audit outage itself remains a fail-closed service error. A genuinely
ambiguous native outcome remains quarantined. This does not solve disk-space
exhaustion or prove that the client received an earlier reply.

Two durable operation formats upgrade one way, and both refuse before serving
rather than after: a compacted operation snapshot now carries an envelope version
and a digest of its own content, and durable command digests are written as
`logical_command_v1:<hex>`. Binaries released before v1.1.4 do not know either
field vocabulary and refuse to open such a state directory at startup. Rolling
back therefore needs an operator who removes the operation snapshot (which drops
the operations it materialized) or restores a pre-upgrade copy. In the other
direction the upgrade is one way: a snapshot written by an older release stays
readable and is re-sealed in place on its first open, while earlier binaries
refuse this field vocabulary at open. Because the snapshot has no anchor outside
its own directory, an edited snapshot that keeps a legal chain position is only
detectable through its content digest: a writer who can rewrite the directory can
also recompute that digest.

A clean stop materializes the operation view. Operation commits append to the
sidecar log without rewriting the registry payload, so a reader of `state.json`
alone can trail the durable log by an arbitrary number of operations. Shutdown
therefore commits the joined view into the payload and freezes both write paths,
and that commit stamps `payload.operations_checkpoint` with its own revision. A
reader may treat `payload.operations` as the durable operation view exactly when
`operations_checkpoint != 0 && operations_checkpoint == payload.revision`
(equivalently `== envelope.revision`); an absent or older value means the payload
may lag the sidecar, and a clean-stop judgment must not be based on it. A
checkpoint that cannot commit leaves the marker behind, keeps both write paths
open and makes the process exit non-zero, so no clean-terminal proof can be
recorded for that stop.

v1.1.5 adds `payload.operations_checkpoint`, and it is additive in both
directions: binaries released before it ignore the field, and a payload without
it stays readable with the marker at 0. The launcher's reuse gate reads that 0 as
"not materialized" and refuses the stop, so a run directory written before this
release cannot be reused by the new launcher and is restarted with a fresh
directory instead of being judged by the older criteria. The owned-session
lifecycle and shutdown budget protocols are additive RPC as well: the Worker and
the Authority advertise `lifecycle_protocol` and `shutdown_protocol` on
`get_metrics`, a caller that does not send the new commands keeps the legacy
`open_session` path, and the platform's own consumption of both is not wired yet.

The whole teardown — HTTP shutdown, gateway drain, operation checkpoint and audit
tail — shares one deadline, reported as `platform shutdown incomplete` when a step
does not finish inside it, instead of waiting for a step that may never return.
`PERI_SHUTDOWN_TIMEOUT_MS` lowers that deadline (1s to 60s, default 10s) and an
unusable value refuses to start. A budget above the default only starts next to
`PERI_SHUTDOWN_STAGE_CAP_MS`, the per-stage stop cap the launcher holds this
process to, and it has to stay strictly below it: a process still tearing down
when the cap expires is killed mid-stop, which is the dirty stop this shared
deadline exists to prevent. Raising without that declaration refuses to start,
and so does a declared cap the default budget does not fit under.
`PERI_SHUTDOWN_OUTER_GRACE_MS` is the deployment's statement about the whole
stop, the grace the orchestrator enforces around the process, and a declared cap
that reaches it refuses to start as well: a cap larger than the real kill is a
check that cannot fail, not a covered stop. The process cannot observe either
deadline, so both stay declarations by the launcher, and an unusable value
refuses to start rather than dropping the comparison. The operation
checkpoint is only half of a reuse judgment: it proves the sidecar view reached
the payload, never that the stop itself was clean, and a checkpoint step the
deadline abandoned can still commit after the stop was reported unclean.

v1.1.6 changes how the Authority RPC carries object bytes: `Put` bodies,
`UploadSegments` batches and `Get` replies use base64 strings instead of JSON
integer arrays, and a new `put_group` session operation makes a group of
immutable segment bodies durable, so a 4 MiB object takes about 5.6 MB on the
wire instead of about 15 MB; the 32 MiB message cap is unchanged. The new
decoder still accepts the integer-array form and can read a message produced by
an older peer, but an older decoder cannot read base64 and an older Authority
rejects `put_group` before dispatch. `PROTOCOL_VERSION` remains 1, so this
release's Authority and Worker binaries must be deployed together: a mixed pair
is not rejected by a version check, and fails at the first object the older side
has to decode from the new encoder. A decode failure is an RPC failure, never
permission to replay SQL.

The ownership root advances to schema version 5, which adds the paged command
index, and the WAL index gains its receipt index and entry bounds on the first
append or truncate. Boot rewrites a version 1, 2 or 4 root to version 5, and
v1.1.5 and earlier reject these documents at open: they accept only root
versions 1, 2 and 4 and deny unknown fields. Opening a database with v1.1.6 is
therefore a one-way disk upgrade; rolling back to an earlier release needs the
pre-upgrade object store, not just the earlier binary. Command receipts beyond
the inline window and WAL receipt pages written by truncation are
content-addressed and verified on read, with a missing or altered page failing
closed. Legacy WAL receipts without a digest stay inline and are never archived;
once their chunks are truncated, a retry is refused with "WAL receipt content
unavailable" instead of being served from the baseline.

The maintenance scheduler is still off unless `PERI_MAINTENANCE_SCHEDULE=1`;
with the switch unset, maintenance cadence is unchanged and a tick is one static
read per loaded database. Enabling it opts into checkpoint and compaction lanes,
watermarks, cooldowns and budgets whose defaults are experiment inputs, not
measured production thresholds. One maintenance change is unconditional: a
passive checkpoint no longer takes the writer admission or the operations gate,
so admin, scheduled and pre-compaction checkpoints can run beside foreground
SQL; compaction still takes exclusive admission, and maintenance units remain
mutually exclusive. Per-database diagnostics always expose the maintenance
ledger and named block reasons, and a failed scheduled unit is logged to stderr.
`PERI_WORK_METRICS=1` adds WAL/index, lock, execution, result-size, maintenance
and authority publication/snapshot counters; with the switch off the field is
omitted rather than reported as zero. The `/v1/metrics` endpoint always reports
admitted, successful and rejected totals, named rejection reasons, a
successful-only latency account and operation-journal stage timings;
`action_latency_ns_total` still includes refusals. The existing default-off
diagnostics remain diagnostics: `PERI_SQL_OPEN_OBSERVER=1` streams phases for
requests of at least 100 ms or failures, `PERI_DIAG_SLOW=1` requires
`PERI_DIAG_DIR`, and `PERI_ADMISSION_TRACE=1` or `PERI_DIAG_GROUP_PHASE=1` adds
trace detail. None of these switches changes SQL, WAL, publication, audit or
recovery decisions.

The Go SDK and CLI add `Options.ConnectionReuse` and `--connection-reuse`, both
default off. Reuse cuts connection churn. A request whose cancellation or write
fails before the request body was written is answered as a proven non-dispatch
rather than an unknown outcome; a request that completed the write and then lost
its reply remains `outcome_unknown`, so callers must resolve that state with
durable operation records or business idempotency. Shutdown retries the worker
retirement sample within its bounded budget, and a failed launcher restart
preserves prior clean evidence and failure records instead of overwriting them.
The restart gate itself is not relaxed: it still requires the signed clean stop
record, the worker stop proofs, a clean terminal proof, a materialized operation
view and no unresolved operations (a quarantine ended by `isolation/release`
counts as resolved history, not as an open isolation). Experimental Hrana remains default-off with
its documented gaps, and this release claims neither an SP01 pass nor a capacity
result.

## Build, test, and SDKs

```sh
cd prototypes/v1/platform/frontend
bun install --frozen-lockfile
bun run build
cd ..
go build ./cmd/peri-platform ./cmd/peri
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 go test ./...
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 PERI_P6_VERIFY_RACE_CLI=1 go test -race ./...
node --test tests/ui-json.test.mjs
cd sdk/typescript
PATH="../../frontend/node_modules/.bin:$PATH" bun run build
bun run test
```

The TypeScript build expects `tsc` on PATH; generated `dist/` is not committed.
See `sdk/README.md` for Go/TypeScript and `../src/sdk.rs` for the Rust SDK.
For a live tenant/role/quota/revocation API check, run
`python3 tests/user-api-live.py --connection /private/fresh/connection.json --output /private/fresh/user-api-results.json`
from this directory. The input must be the launcher's private TLS connection file;
the output must not exist. This creates two test projects and databases, never
replays an uncertain request, and leaves first-failure state for inspection.
The management CLI reads credentials from a private configuration file or
environment, never command-line secret arguments. SQL shell reconnection is
explicit and does not replay transactions. Deployment manifests live in
`../deploy`; their one-container/private-loopback topology is intentional.
