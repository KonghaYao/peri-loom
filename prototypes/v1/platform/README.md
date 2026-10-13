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
key cannot open SQL sessions: table-row previews need a project API key.

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
handling, and crash-atomic generation/configuration rotation. Use a new fixture;
never erase proof records or ownership dirty markers to force a restart.

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
- `GET /v1/platform/capabilities` reports the public gateway origin and whether experimental Hrana HTTP and WebSocket are actually enabled. With `PERI_HRANA_EXPERIMENTAL=1`, project SQL API keys can use `https://<host>/db/<tenant>/<database>/<branch>/` through the tested JS client's Hrana HTTP v2 transport, the Rust `turso_serverless` 0.1.3 client's v3 JSON pipeline/cursor transport, or `wss://<host>/db/<tenant>/<database>/<branch>/` through the low-level JS client's WebSocket v1/v2 JSON and v3 protobuf. The adapter defaults off; HTTP v3 JSON read cursors, and v3 protobuf read-only and write-RETURNING cursors on HTTP and WebSocket, fetch bounded native pages. HTTP v3 JSON read cursors have a 10,001-row regression; Rust SDK reads and a disposable transactional write batch passed against an isolated loopback HTTP gateway. TLS development certificates must be trusted by the client independently; the Rust SDK does not accept a custom CA through its public builder. Full protocol and remote session-store schema compatibility, as well as crash-restart recovery, are not yet proven.
- Query may execute SQL writes: native read-only mode, not SQL text filtering, enforces reader roles.
- `/v1/databases/*` exposes provisioning, lifecycle, refs, snapshots, clone, restore, PITR, checkpoint, and GC.
- `/v1/organizations/create` and `/v1/projects/create` require the global management key.
- Project keys manage only their project; global management keys cannot open SQL sessions or mint database tokens.
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
Native terminal `sql_error` replies explicitly indicate `execution_completed`
and `may_have_committed`: the session can issue an explicit rollback, but a
multi-statement committed prefix is not rolled back automatically. Unknown
outcomes retain admission permits and poison the session instead of treating
timeout, close, or readback as cancellation proof. Migration invalidates old
session bindings; reopen explicitly against the current placement.

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
Audit storage has an explicit 64 MiB cap and requires an operator archival plan.
After a trusted native SQL completion, an audit append failure returns
`audit_unavailable` without reclassifying the durable operation as Unknown or
poisoning its session. The reply includes `execution_completed` and forbids
retries; the audit outage itself remains a fail-closed service error. A genuinely
ambiguous native outcome remains quarantined. This does not solve the audit
capacity limit or prove that the client received an earlier reply.

## Build, test, and SDKs

```sh
cd prototypes/v1/platform/frontend
npm ci
npm run build
cd ..
go build ./cmd/peri-platform ./cmd/peri
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 go test ./...
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 PERI_P6_VERIFY_RACE_CLI=1 go test -race ./...
node --test tests/ui-json.test.mjs
cd sdk/typescript
npm run build
npm test
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
