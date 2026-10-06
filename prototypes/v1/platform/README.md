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

The launcher builds the native Rust service and the Go platform, starts one
authority, two encrypted workers, and the platform, and prints a readiness
record only after authenticated provisioning. It seeds an organization, a
project, and an encrypted demo database in a new private directory. The
`connection.json` file contains synthetic API keys and a development CA path;
never publish it. Open its gateway endpoint in a browser and trust the disposable
development CA only for this fixture. Do not disable TLS verification in clients.

The listener defaults to `127.0.0.1:9443`. `--listen` changes the endpoint;
`--tls` generates a disposable local certificate unless an existing private
certificate/key pair is supplied via `PERI_TLS_CERT` and `PERI_TLS_KEY`.
External listeners require TLS. SIGTERM performs joined shutdown; an uncertain
gateway operation makes process termination unsuccessful rather than proving
clean cancellation. Reuse the same run directory to restart a clean deployment.
Never erase ownership dirty markers to force an unproven restart.

For an existing S3-compatible service, create a **dedicated bucket** first and
inject `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`, and
`S3_SECRET_ACCESS_KEY` through a private environment mechanism, then add
`--backend s3`. Only the Rust authority receives these variables. Workers and
the Go platform receive no raw S3 credentials. Never save real credentials in
source, argv, run configuration, or diagnostic logs.

## API and credentials

- `/v1/sql/open`, `/execute`, `/query`, `/close` manage opaque, principal-bound sessions.
- Query may execute SQL writes: native read-only mode, not SQL text filtering, enforces reader roles.
- `/v1/databases/*` exposes provisioning, lifecycle, refs, snapshots, clone, restore, PITR, checkpoint, and GC.
- `/v1/organizations/create` and `/v1/projects/create` require the global management key.
- Project keys manage only their project; global management keys cannot open SQL sessions or mint database tokens.
- `/v1/tokens` issues bounded-lifetime tokens tied to issuer, project, root, branch incarnation, and encryption domain.
- `/v1/keys/revoke` invalidates issuing keys and their database tokens.
- `/v1/metrics` requires a global management key and combines gateway and actual native process observations.

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

The local KMS is a private, encrypted-at-rest development keyring, **not an HSM
or external cloud KMS**. Rotate a KEK and rewrap descriptors without changing
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

## Build, test, and SDKs

```sh
cd prototypes/v1/platform
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
The management CLI reads credentials from a private configuration file or
environment, never command-line secret arguments. SQL shell reconnection is
explicit and does not replay transactions. Deployment manifests live in
`../deploy`; their one-container/private-loopback topology is intentional.
