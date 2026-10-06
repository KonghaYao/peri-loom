# P6 security decision — architecture sidecar, requested Astra model (`gpt-6-astra`)

Status: proposed, source-reviewed; no implementation, tests or benchmarks run. Scope is independent of P4 final verification and P5 integration. Repository HEAD observed: `51191870dc903327688fc3672ed8dde31932e016`; local Turso HEAD: `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`. Working-tree source is the evidence, not a claim that either HEAD is a frozen verification result. No calendar date inferred. Provenance, as clarified by the requester: this architecture sidecar was explicitly requested through spawn with `model=gpt-6-astra`. This records the requested model, not independently verified runtime model identity; no additional tool-based Astra consultation is claimed.

## Decision

Use Go for Gateway authentication/RBAC, routing, admission policy, key provisioning and KMS envelope management. Keep SQL, native encryption, IO, durability and fencing in Rust. Go must never issue page/frame/manifest commands or implement a pager. First encrypted profile is **new databases only**, using Turso built-in `aegis256` encryption with a random 32-byte DEK and an external local KMS envelope. Do not introduce an authority `ObjectStore` AES-GCM wrapper as the primary SQL encryption solution. Do not build a custom `PageCodec` in this slice.

This protects SQL page payloads in remote main segments, WAL page bodies and worker SSD segment caches, subject to the integration tests below. It does not encrypt catalog metadata, WAL headers, object names, RAM, host swap, logs or all temporary files. Native code is explicitly opt-in/experimental; source inspection establishes an integration path, not a production security certification. No claim of malicious-storage rollback resistance or overlapping authority HA.

## Source evidence and exact engine seam

Paths below are relative to `/Users/konghayao/code/ai/peri-loom`.

| Evidence | Consequence |
| --- | --- |
| `docs/v1/P4-DESIGN.md`; `docs/v1/07-p6-cloud-platform.md` | Preserve P4 final-dispatch fencing, unknown-outcome blocking, single authority and Go coarse control. P6 routing retries cannot weaken these contracts. |
| `prototypes/p0/turso/core/database.rs:185`, `:217`, `:277`, `:282` | Actual Rust APIs are `EncryptionOpts { cipher: String, hexkey: String }`, `OpenOptions::encryption(...)`, `OpenOptions::page_codec(...)`. These options are mutually exclusive. |
| `prototypes/p0/turso/core/database.rs:108`, `:2387`, `:2394`, `:2402` | Enable `DatabaseOpts::with_encryption(true)`. Every connection needs `connect_with_encryption(Some(EncryptionKey))`; plain `connect()` does not retain the open-time key. Custom codecs instead require `connect_with_page_codec(Arc<dyn PageCodec>)` on every connection. |
| `prototypes/v1/Cargo.toml`; `prototypes/p0/turso/core/Cargo.toml:52` | v1 disables default features and currently enables only `fs`, `uuid`, `json`. Add Turso `encryption`; crypto dependencies already exist in Turso. |
| `prototypes/v1/src/database.rs:43`, `:107`; `prototypes/v1/src/runtime.rs:144` | Both initial and independent-session connects currently use plain `connect()`. `WorkerRuntime::load` is the key-provider injection seam, before `RemoteDatabase` open. |
| `prototypes/p0/turso/core/storage/page_transform.rs:93`, `:128`; `core/storage/wal.rs:3206` under the same Turso directory | Built-in encryption is adapted to `PageCodec`; WAL transforms page bodies before computing frame checksums. External codecs expose `codec_id`, `bootstrap_page_info`, `required_reserved_bytes`, `encode_page`, `decode_page`; context contains page number and Database/Wal location, not an epoch or nonce allocator. |
| `prototypes/v1/src/io/files.rs:17`; `prototypes/v1/src/io.rs:51`; `prototypes/v1/src/cache.rs:120` | RemoteFile stores the bytes delivered by native IO; WAL recovery reconstructs those bytes; SegmentCache persists segment bytes. Encrypting above IO therefore preserves ciphertext through these paths. |
| `prototypes/p0/turso/core/storage/encryption.rs:729`, `:940`, `:1050` | Fresh OS-random nonces per encryption; AEGIS-256 uses 32-byte nonce plus 16-byte tag, reserving 48 bytes/page. Ordinary pages use empty AAD; page 1 authenticates visible header. Page identity/namespace and freshness are not cryptographically bound by this built-in format. |

Concrete engine construction, inside a new explicitly encrypted open path:

```rust
let options = OpenOptions::new(Arc::new(SqliteDialect))
    .db_opts(DatabaseOpts::new().with_attach(false).with_encryption(true))
    .encryption(EncryptionOpts {
        cipher: "aegis256".to_owned(),
        hexkey: encoded_dek,
    });
let options = if readonly { options.flags(OpenFlags::ReadOnly) } else { options };
let engine = Database::open(io.clone(), &io.database_path(), options)?;
let connection = engine.connect_with_encryption(Some(EncryptionKey::new_256(dek)))?;
```

Repeat the last operation for **every** `RemoteDatabase::session()`. Hold key material in a private, non-Debug/non-Serialize per-open configuration; do not retain/log `EncryptionOpts` (it derives Debug and contains the hex key). Minimize/zeroize adapter copies, but do not claim full zeroization: native `EncryptionKey` is Clone and the inspected type has no zeroizing Drop. Never send keys in SQL, URLs, argv or audit fields.

Choose AEGIS-256 for this bounded prototype because the native implementation supplies 256-bit random nonces across rewrites/restarts without a new persistent counter. Native `aes256gcm` also exists but uses random 96-bit nonces; do not silently describe it as unlimited safe reuse of one DEK. An AES-mandated deployment needs an explicit per-key invocation budget covering all branches/restarts or a separately reviewed scheme. This choice makes no FIPS claim.

For custom codecs, fixed-size output, reserved bytes, page-1 bootstrap, registry identity and connection installation become our responsibility. Multiprocess WAL is rejected with external codecs; MVCC integration also has restrictions in `database.rs`. Keep the existing native WAL mode and reject SQL attempts to switch to unsupported encryption/journal modes. Do not modify the Turso pager/WAL implementation.

## Why the raw authority wrapper is not the minimal answer

`ObjectStore::{get,put,delete,list}` returns `StoredObject { bytes, etag }`; conditional operations use `PutCondition::{Any,Absent,Match(String)}` (`prototypes/v1/src/object_store.rs`). A wrapper beneath the credential-bearing authority can encrypt persisted roots/objects, but decrypts them before sending data to workers. Their main-page IO and `.seg` files remain plaintext. It also creates ciphertext length/accounting, immutable-object comparison and conditional-root complications; catalog/bootstrap keys need decryption before DB key lookup. Physical keys must be resolved by authority, not guessed from a logical tenant prefix or supplied by clients.

If separately added later for metadata defense, preserve raw backend ETags unchanged through GET/PUT/DELETE; never derive CAS versions from plaintext. Bind version, key ID and canonical physical key into AAD; use a reviewed unique-nonce scheme, authenticate before releasing plaintext, preserve exact unknown/precondition outcomes and all fencing checks, and do not add retries. No magic-header fallback to plaintext on decryption failure. Existing roots need an explicit offline/new-prefix migration, not read-time rewrite. This is an additional project, not a shortcut that covers SSD caches.

Native encryption changes page bytes but preserves IO sizes and the existing physical key layout: content hashes naturally identify encrypted segments; root CAS, reservations, history and GC remain Rust authority responsibilities. Visible main header fields and WAL/frame headers are necessary for current bootstrap/recovery. Existing P4 checksums/fencing are not a cryptographic defense against an attacker rewriting storage and catalog together; page swaps/replay within a shared DEK domain remain outside this prototype's threat model.

## Provisioning, KMS and lifecycle contract

Proposed interfaces below are **new**, not current RPCs:

* Go `KeyProvisioner.CreateDomain(ctx, requestID, databaseIdentity) -> KeyDescriptor`; `ResolveForWorker(ctx, authenticatedWorker, liveBinding, descriptorID) -> short-lived DEK delivery`; `Rewrap(ctx, descriptorID, newKEKVersion)`. No API accepts a caller-chosen physical object path.
* `KeyDescriptor = {version, domainID, tenantID, databaseID, rootIncarnation, cipher, dekID, kmsKeyID, kekVersion, wrappedDEK, envelopeVersion}`. Branch-incarnation/epoch are delivery authorization, not part of immutable page encryption identity. Branches/snapshots sharing encrypted segments share a DEK domain. Cross-tenant/shared-key clones are refused in the first profile.
* Rust `DatabaseKeyProvider::resolve(&DatabaseId, epoch) -> Result<ResolvedEncryption>` injected into WorkerRuntime; the provider must validate exact authority binding/root incarnation and key descriptor, not authorize using epoch alone. `ResolvedEncryption` is a private trusted configuration, never caller SQL input. A default legacy provider may serve explicitly legacy deployments; the P6 encrypted service fails closed on missing descriptor/key.

Local prototype KMS: a separate Go key broker over an owner-only Unix socket (or authenticated TLS), OS-CSPRNG DEKs, a persisted 32-byte KEK in a 0600 file in a 0700 directory outside S3/cache/source. Envelope-wrap the 32-byte DEK with Go standard `crypto/aes` + `cipher.NewGCM`, OS-random 12-byte nonce, and a canonical length-delimited encoding of descriptor identity/cipher/DEK ID/KEK version as AAD. Store nonce and authenticated ciphertext in the descriptor. This AES-GCM use wraps small keys, not every SQL object; cap at 1,000,000 envelopes per KEK version with durable accounting, then require a fresh KEK. Refuse counter rollback/uncertain state; do not reuse restored broker state as an active writer. No hand-written cipher.

Provision with a durable idempotent state machine: reserve never-reused database/domain identity → generate/persist envelope and fsync → create fresh empty namespace → pin the returned authority root incarnation in the ready descriptor and durably persist → only then enable placement/token issuance. If binding fields change between pending/ready, create a new authenticated envelope with a fresh nonce. Never allocate a replacement DEK merely because an acknowledgement was lost. Ambiguous creation stays pending for operator reconciliation; an unused envelope is harmless. At this stage prohibit out-of-band restore/delete/name reuse that could detach the sidecar descriptor from authority identity. Ready descriptors are immutable except versioned rewrap; validate mapping before each cold open. This staged visibility avoids pretending the Go metadata store and Rust root CAS are one transaction.

Broker restart loads the same KEK and envelopes; missing/wrong versions fail closed. Back up KEKs separately from envelope metadata and data; losing them loses access. Existing opened workers can hold plaintext DEKs; KMS outage blocks new opens, and KMS revocation does not erase already delivered keys. To revoke service access, stop admissions and revoke/drain leases/sessions. The broker authenticates worker identity and checks an authoritative live lease, delivers only that domain's DEK, and rechecks after slow unwrap; worker revalidates its lease before native open. Raw S3 credentials remain exclusive to Rust authority. A local file KEK is a functional KMS prototype, not HSM isolation or protection from host root.

KEK rotation rewraps the **same** DEK using durable versioned envelopes; atomically replace metadata, retain old KEKs until all descriptors/backups are accounted for. It does not rewrite SQL data. Online DEK/cipher rotation is unsupported: pages do not carry our DEK IDs and retained snapshots/WAL may need the old key. DEK rotation or plaintext migration requires offline logical export/import into a fresh encrypted identity with a fresh cache, verification and controlled routing cutover. No in-place PRAGMA conversion, no mixed encrypted/plaintext branch, no changing DEK under a live cached Database. Reuse of an old cache path requires complete teardown; preferably key paths by domain/DEK ID. Old plaintext snapshots/caches/backups stay plaintext until separately retired; do not claim secure erasure.

`RemoteIO::open_file` explicitly delegates recognized temporary files to host PlatformIO (`prototypes/v1/src/io.rs`). Therefore production “all data at rest” additionally needs encrypted ephemeral volumes or bounded RAM-backed temporary storage, disabled core dumps, swap policy, and redacted logs. The first profile must deploy these protections or label the remaining exposure. SQL RAM and responses are necessarily plaintext in trusted worker memory/TLS endpoints.

## Gateway token/RBAC versus NativeReadonly

Current `WorkerCommand::OpenSession { database }` has **no per-client role** (`prototypes/v1/src/worker_rpc/protocol.rs:16`). `Sessions::open` opens the role of the loaded ownership runtime and reports `readonly`. Do not infer that authority's ability to narrow a scoped store is already exposed as a native per-session feature. `Query` is not read-only authorization: it prepares SQL and can include writes returning rows. `PRAGMA query_only` or verb/regex classification is not a security boundary.

Minimal Go Gateway: verify signed short-lived DB tokens with fixed algorithm/key IDs (e.g. Ed25519), issuer, audience, expiry/not-before, subject/service account, tenant/project, exact DB/branch identity and incarnation, allowed operations and authorization revision. Suggested token lifetime: 5 minutes, explicitly bounded clock skew. Tenant/DB must be derived from claims plus server mapping and match the request, never accepted solely from its path. API-key hashes live in management metadata; mint short-lived DB tokens after RBAC evaluation. Check token/revocation on **each** request and terminate expired sessions; short-lived tokens alone do not revoke long-lived sessions.

Roles for the first profile: `db.connect`, `db.sql`, `db.admin` (named maintenance), and management permissions. A writer DB's arbitrary SQL permission is full SQL within that DB/branch, not table/row RBAC. “Reader” arbitrary SQL is accepted only when controller/authority granted the **whole loaded database/branch** ReadOnly and OpenSession confirms native readonly; otherwise reject it as unsupported. No simultaneous writer plus token-downgraded reader guarantee on the same engine. A separately authorized read-only snapshot branch is a possible product path, not a claim of live read replicas. Management admin does not automatically imply SQL access.

Gateway uses its separate SQL worker credential; tokens never reach `/control` or raw authority paths. Bind an opaque public session handle to subject, tenant/DB/branch/incarnation, upstream worker/session, placement epoch, permissions and expiry; never forward a client-supplied upstream session ID. Serialize requests on a session and reauthorize at dispatch. Worker/control/authority endpoints remain private; use TLS with peer verification for non-loopback transport. Gateway prevents SQL callers from invoking Checkpoint/Compact/Shutdown as generic forwarded commands. Audit identities, operation, outcome and correlation IDs, not SQL text/keys/tokens by default.

Routing can refresh stale placement before dispatch. After any upstream bytes may have been sent, disconnect/timeout/worker loss yields `outcome_unknown`, with a correlation ID and no automatic write/batch/COMMIT retry. Existing `request_id` is correlation, not transactional SQL deduplication. A batch may partially commit. Do not replay transactions on reconnect or retry `Query` assuming it cannot write. Cancellation is not proof of rollback; a completed COMMIT remains committed even if the response was lost. First profile performs no automatic SQL replay at all. Caller business idempotency must be implemented transactionally in their database.

## Actual admission and noisy-neighbor bounds

Starting prototype limits, configuration rather than performance promises:

| Enforcement point | Bound and behavior |
| --- | --- |
| Go Gateway | Hierarchical token buckets, per tenant 20 requests/s burst 40, per DB 10/s burst 20; charge every SQL dispatch and separately cap token mint/login failures. Shared tenant accounting prevents evasion by minting tokens. Single Gateway process owns counters. |
| Gateway sessions/work | Tenant 16 sessions/4 in-flight statements, DB 8 sessions/2 in-flight, 1 in-flight per session; tenant queue 8, wait ≤100 ms, then pre-dispatch 429. Worker saturation returns pre-dispatch 503. Bound body 256 KiB and session idle 60 s, transaction lifetime 30 s. Reserve before dispatch and release on actual completion, not merely client disconnect. |
| Rust worker | Existing WorkerLimits caps loaded DBs and sessions and per-DB segment caches; explicitly configure totals so sum fits worker budget. Add statement execution deadline 5 s and result cap 10,000 rows/4 MiB before accumulation/serialization. A Gateway response cap alone cannot stop `run_collect_rows()` allocating inside Rust. |
| SQL execution | Public `Connection::set_progress_handler(ops, Some(Box::new(...)))` and `interrupt()` exist in `core/connection.rs:4846`. Install per request with a monotonic deadline, clear/reset safely, and consume rows using `Statement::step()` / `row()` instead of unbounded collect. Callback limits VM work, not blocking backend IO; keep backend timeouts and P4 uncertain-outcome handling. |
| Memory/SSD/CPU | Existing `ResourceUsage` reports WAL/pending WAL/dirty segments but does not bound them. For this slice, dedicate a worker process per tenant, enforce OS/container memory 512 MiB, CPU 1 core, cache+temp filesystem 1 GiB; reserve small control-plane capacity separately. This gives coarse containment, not fair scheduling between DBs. On hosts without enforceable process/container limits, no hard isolation claim. |
| Authority/backend | Bound per-tenant queued requests, bytes/request and concurrent backend operations before admission; e.g. 2 active operations and 8 waiting per tenant, global 16 active, with a separate bounded control lane. Configure byte ceilings against actual legal segment/WAL request sizes. Attribute usage from authenticated namespace, not user headers. Hold permits until backend outcome settles; keep unknown operations charged/fenced. Never pause an admitted mutation past its lease then silently replay it. |

Enforce storage quota conservatively: proposed tenant retained-data budget 1 GiB includes snapshots, branches, WAL, reservations and pending GC; refuse new mutation reservations that would exceed it. Exact storage accounting belongs in Rust authority at its existing reservation/root-CAS boundary, not a Go read-then-check. An independent filesystem/backend hard quota supplies a physical upper bound but may turn failed writes into unknown outcomes. Until reservation accounting is implemented, advertise only the physical bound, not an exact logical per-tenant quota. Early P6 can ship rate/session/process bounds with this limitation explicit. Shared-authority root locks/IO remain a noisy-neighbor bottleneck even with per-tenant workers.

Gateway-only throttling cannot limit an already running recursive query, huge WAL, or materialized result. HTTP cancellation does not release Rust execution capacity; track it to terminal completion or quarantine that worker. Do not apply quota policy by rewriting pager algorithms.

## Bounded, disjoint future patch ownership

These are implementation assignments, **not files changed by this review**. Stage them in isolated P6 work after P4 is frozen; no P5 controller file is needed for this sidecar design.

1. **Rust encryption adapter:** new `src/security/keys.rs`, `src/security.rs`; own only `src/database.rs`, `src/runtime.rs`, `src/lib.rs`, `Cargo.toml` and its dependency lock update. Add an optional injected key provider and an explicit encrypted open API; preserve old public constructors for legacy tests. All initial/session connects use one internal key-aware helper. No edits to Turso, ObjectStore, authority catalog or WAL formats.
2. **Rust worker execution bounds:** new `src/session/budget.rs`; own `src/session.rs`, `src/worker_rpc/sessions.rs`, `src/worker_rpc/server.rs`. Fixed trusted worker limits initially avoid a shared protocol change. Bound row collection, track deadlines/cancellation and active permits. Encryption work does not edit these files.
3. **Rust authority admission, separate follow-up:** new `src/authority_rpc/quota.rs`; own `src/authority_rpc/server.rs` for principal-bound request/byte/concurrency admission. Exact retained-byte quota needs a separately reviewed reservation/CAS change, explicitly outside this small RPC patch. Do not claim the server-only patch enforces durable storage quotas.
4. **One integration owner:** `src/service_cli.rs` wires a key-provider endpoint and limits; keep it out of the three patch sets above. New integration tests get separate files (`tests/p6_encryption.rs`, `tests/p6_worker_budget.rs`, `tests/p6_admission.rs`). Go Gateway/broker use a new sidecar module/directory, not `/tmp/peri-loom-p5-dev/controller` edits.

Compile-time dependencies: Turso encryption feature; adapter key-buffer hygiene dependency if used; Go standard-library crypto for local envelope/signature primitives. Runtime dependencies: persisted KEK/envelopes, identity-authenticated broker, fresh namespace/cache provisioning, TLS/private service transport, enforceable host isolation. The architecture does not require a cloud KMS SDK for the local prototype. Actual cloud KMS integration requires provider-specific primary-document review later.

## Required correctness tests; no benchmarks

* New encrypted DB: create distinctive text/blob payloads, COMMIT with no checkpoint, kill worker, cold-open on another cache and query; checkpoint and repeat. Inspect decoded object payloads (not just JSON/base64 string scans), reconstructed WAL and SSD `.seg` files for absence of markers. Assert native decrypt/query returns them. Exercise initial connection plus multiple sessions, readonly open and wrong/missing key failures. Tamper ciphertext to require rejection; separately document that replay/page relocation is not promised protection.
* Header/page sizes, overflow pages, repeated update/checkpoint/truncate, native WAL checksums and recovery positions, branch/snapshot/restore with shared domain, cache eviction/cold recovery and key mismatch. Force spill/temp behavior and verify the declared host-storage protection. Confirm no plaintext downgrade and refusal of unsupported journal/encryption reconfiguration.
* Provisioning fault injection before/after every fsync/namespace creation/ready publication; broker restart, wrong KEK/AAD, missing descriptor, name/incarnation mismatch, duplicate create, unknown outcome. Rewrap restart at each boundary retains at least one working envelope; old DEK never changes. KMS outage and revoke with already-open workers must match the stated behavior.
* Gateway token tenant/DB/branch mismatch, expiry and revocation during session, forged algorithm/kid, stolen upstream handle, admin/control denial. A reader token on writer placement is refused; ReadOnly whole-DB grant rejects writes through Execute and Query, multi-statements, PRAGMAs, ATTACH and extension loading.
* Drop response after successful COMMIT, partial batch, migration/disconnect mid-transaction: no automatic replay, report ambiguity. Simulate late execution completion after client cancellation; permits stay occupied and COMMIT acknowledgement semantics remain intact.
* Saturate tenant A with queue floods, recursive SQL, large result/WAL and slow storage while tenant B makes progress within configured concurrency bounds. Verify pre-allocation row/body limits, process memory/CPU/disk containment, control-lane availability, and zero quota-triggered retries after dispatch. Test accounting cleanup on error and unknown outcomes.
* On the eventual isolated implementation, run targeted new correctness tests and required P4 native readonly, fencing, cold-recovery and unknown-outcome regressions. Do not run benches or mutate P4's active verification artifacts to certify P6.

## Explicit availability limits

One exclusive credential-bearing authority, one durable Go controller, one Gateway/broker accounting owner. P4 dirty-marker restart refusal and operator `--reconciled` requirements remain unchanged. A second state directory/host does not create exclusivity; key broker persistence is not consensus, and distributed Gateway instances would multiply local rate limits without shared allocation. Worker failure/replacement is supported only through existing lease/takeover barriers; controller/authority/broker failure can stop new placement or opens. Go desired placement metadata cannot override a durable authority term, and a key descriptor cannot authorize a stale lease. None of this blocks ongoing P5 integration.

## Addendum: selected minimal online key broker contract

This refinement supersedes the abstract provisioning/provider seam above where more specific. **Select a separate Go broker with in-memory DEK delivery, not a plaintext worker keyring or launcher-written DEK file.** A 0600 DEK file limits Unix access but still persists an unwrapped data key; it is not the chosen KMS-backed at-rest profile. Workers persist only their separate broker authentication secret, not DEKs. Local KMS still has a file-backed KEK, with the previously stated host-root limitation. Gateway is an authenticated provisioning caller, not the holder of worker validation credentials or DEKs.

### Ownership and existing authority capability

* Broker owns an exclusive, durable metadata directory: provision receipts, a permanent namespace-to-domain mapping, versioned namespace key descriptors and their status. Descriptor IDs/revisions are broker-generated; Gateway/worker never choose an envelope, KEK version, cipher or physical object key on resolve. One DEK per namespace/root-incarnation domain; all branches in that domain share it. No independent per-branch confidentiality or retroactive key revocation is promised.
* Broker stores `{worker_id -> hash(worker_key_token), authority_worker_token}` in a separate operator-provisioned private credential configuration. `worker_key_token` is independently random, never the authority, SQL Gateway or controller token. Match it in constant time and derive worker identity from it. An operator supplies only registered workers' entries from the authority credential source (`PERI_AUTHORITY_WORKERS_FILE`), preferably as a restricted copy, not a shared writable file. Rotation is operator-coordinated in this prototype; do not invent hot reload in current authority code.
* Existing Rust names are `AuthorityClient` and `RemoteAuthorityProvider::validate_lease`, not `WorkerAuthorityClient`. The broker sends an ordinary `AuthorityCommand::ValidateLease` using the authenticated worker's **server-selected** authority token. In `authority_rpc/server.rs`, `worker()` rejects Control and checks the binding worker; `allowed()` requires a grant in this authority term. Therefore the controller token or client-provided “validation succeeded” assertion is insufficient.
* No S3 credentials, raw store directory, authority control token, grant/renew/revoke operation or catalog/root mutation is added to the broker. However the copied authority worker tokens are **not validate-only capabilities**: the existing authority also permits scoped OpenSession/storage operations with them. Broker compromise inherits those workers' existing live-grant storage permissions as well as KMS access. This trusted broker is the bounded tradeoff for using P4 unchanged. Actual least-privilege validate-only credentials would require a later authority authentication/API change; a client wrapper alone cannot narrow a stolen token.
* Deploy the local broker on the authority host: current `AuthorityClient`/authority HTTP transport is loopback-only. Broker-to-authority uses fixed configured loopback `/`, never an endpoint from a request. Worker-to-broker uses HTTPS with a pinned/private trusted server CA and the separate bearer token; the local prototype may keep this on loopback too. No plaintext non-loopback hop, redirects, proxy environment routing or automatic HTTP retries. Multi-host authority transport is separate work, not implicitly solved here.

### Exact coarse key RPC (new P6 endpoint)

`POST /internal/key`, `Authorization: Bearer <worker_key_token>`, `Content-Type: application/json`; request/response body bound 8 KiB, one object, reject duplicate/unknown fields and trailing JSON. JSON integers map to checked Rust `u64` / Go `uint64`, never float64. HTTPS response includes `Cache-Control: no-store`; disable body logging/tracing and proxy caching. No public Gateway forwarding route exposes this endpoint.

Request (illustrative identifiers; all field names and enum spellings are normative):

```json
{
  "version": 1,
  "request_id": "key-open-001",
  "database": {
    "namespace": {"tenant": "tenant_a", "database": "db_a"},
    "branch": "main"
  },
  "binding": {
    "database": {
      "namespace": {"tenant": "tenant_a", "database": "db_a"},
      "branch": "main"
    },
    "root_incarnation": "root_a",
    "authority_term": 7,
    "branch_incarnation": 1,
    "epoch": 12,
    "worker_id": "worker_a",
    "lease_id": "lease_a",
    "grant_id": "grant_a",
    "renewal_sequence": 3,
    "role": "writer"
  },
  "access_role": "writer"
}
```

`database` must exactly equal `binding.database`; `binding.worker_id` must equal the token principal. `access_role` is the locally installed narrowed role (`writer` or `read_only`), distinct from binding's durable grant role. It may narrow Writer, never elevate ReadOnly. The broker resolves descriptor solely by authenticated namespace plus validated root incarnation, after checking that namespace was provisioned. Request contains no DEK ID, physical path, KMS ciphertext or URL.

The actual authority request is the existing wire shape, using `binding` from above and the worker token selected from server configuration:

```json
{
  "version": 1,
  "request_id": "key-validate-001",
  "command": {
    "command": "validate_lease",
    "binding": {
      "database": {
        "namespace": {"tenant": "tenant_a", "database": "db_a"},
        "branch": "main"
      },
      "root_incarnation": "root_a",
      "authority_term": 7,
      "branch_incarnation": 1,
      "epoch": 12,
      "worker_id": "worker_a",
      "lease_id": "lease_a",
      "grant_id": "grant_a",
      "renewal_sequence": 3,
      "role": "writer"
    },
    "role": "writer"
  }
}
```

Consume existing `RpcResponse {version, request_id, ok, result, error, precondition_failed}` and deserialize `result` as `GrantReply {binding, remaining_ttl_ms}`. Check all stable binding fields; renewal sequence may advance, never regress. The broker performs validation before unwrap and again after unwrap/any descriptor binding persistence; no registry mutex is held across either RPC. If validation, persistence or deadline checking fails, discard key material and return no DEK.

Successful broker response, HTTP 200:

```json
{
  "version": 1,
  "request_id": "key-open-001",
  "ok": true,
  "result": {
    "descriptor_id": "kd_a",
    "descriptor_revision": 2,
    "domain_id": "domain_a",
    "dek_id": "dek_a",
    "cipher": "aegis256",
    "dek_hex": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    "validated_binding": {
      "database": {
        "namespace": {"tenant": "tenant_a", "database": "db_a"},
        "branch": "main"
      },
      "root_incarnation": "root_a",
      "authority_term": 7,
      "branch_incarnation": 1,
      "epoch": 12,
      "worker_id": "worker_a",
      "lease_id": "lease_a",
      "grant_id": "grant_a",
      "renewal_sequence": 3,
      "role": "writer"
    },
    "access_role": "writer",
    "valid_for_ms": 1000
  },
  "error": null
}
```

The sample hex is test data, never a default key. Require exactly 64 hex digits/32 decoded bytes. `valid_for_ms` is a conservative **open/delivery freshness hint**, not a cryptographic key expiry or lease extension: at broker send time take `min(1000, remaining_ttl_ms - elapsed_since_final_validation_request_start - 5)` and require >0. Use suspend-inclusive monotonic elapsed time; if Go's ordinary clock cannot provide that guarantee on the host, add a platform clock shim and fail closed on failure/regression. Worker subtracts its entire key-RPC elapsed time and 5 ms again, and performs its own fresh authority validation. No wall-clock lease timestamps, no use of delivery TTL to bypass P4 LeaseContext.

Failure envelope: `{"version":1,"request_id":"key-open-001","ok":false,"result":null,"error":{"code":"LEASE_INVALID"}}`. Codes/status: `INVALID_REQUEST` 400, `UNAUTHENTICATED` 401, `FORBIDDEN` 403, `LEASE_INVALID` 409, `KEY_NOT_READY` 409, `RATE_LIMITED` 429, `UNAVAILABLE` 503. Unknown namespace is hidden behind FORBIDDEN to unauthorized callers; never echo credentials/DEK/KMS ciphertext. No cached success response contains a reusable plaintext key. A fresh caller retry must redo validation, even for the same request ID; request ID correlates delivery, not SQL deduplication.

### Rust injection and the required binding accessor

New adapter API, using existing `DatabaseId`, `LeaseBinding`, `AccessRole` types:

```rust
pub struct InstalledKeyLease {
    pub binding: LeaseBinding,
    pub access_role: AccessRole,
}

impl RemoteAuthorityProvider {
    pub fn installed_key_lease(
        &self,
        database: &DatabaseId,
        epoch: u64,
    ) -> anyhow::Result<InstalledKeyLease>;
}

pub trait DatabaseKeyProvider: Send + Sync {
    fn resolve(
        &self,
        database: &DatabaseId,
        epoch: u64,
    ) -> anyhow::Result<ResolvedDatabaseKey>;
}
```

These are signature specifications, not directly compilable method declarations/patches. `ResolvedDatabaseKey` has private fields: `descriptor_id: String`, `descriptor_revision: u64`, `domain_id: String`, `dek_id: String`, `binding: LeaseBinding`, `access_role: AccessRole`, `dek: [u8;32]`; only the validated broker client constructs it, cipher fixed to AEGIS-256 for v1. No Debug/Serialize, and zeroize adapter-owned key storage on Drop.

The accessor is necessary because `RemoteAuthorityProvider.leases` is private and neither `DatabaseProvider::open_store` nor `ObjectStore` exposes binding. Under its current lease mutex it validates database/epoch and clones `(LeaseBinding, AccessRole)`, then releases the mutex. It does **not** mark the snapshot fresh, mint leases, return a token, or introduce root writes. `BrokerDatabaseKeyProvider` holds `Arc<RemoteAuthorityProvider>`, the fixed broker endpoint, its separate secret and a monotonic clock. Its resolve method:

1. Snapshot installed binding/role; call existing `remote.validate_lease(&binding, role)` using the worker's own AuthorityClient; reject elapsed/expired replies. Never derive identity from caller JSON.
2. Send `/internal/key` with that binding/role. Broker independently validates using its configured authority credential; the worker's validation is not evidence trusted by broker.
3. Require response stable identity and role to match, nonregressing renewal, exact cipher/key size, positive conservatively elapsed freshness and expected namespace/domain mapping. Snapshot the installed binding again; require same stable identity and narrowed role, allowing a higher renewal sequence only. Revalidate with authority. Any replacement/revoke/timeout drops the received key.
4. Return the key for this native open only. Runtime retains it only for the corresponding loaded engine and its independent connections; no disk cache or broker-plaintext response cache. Existing ownership generation/lease checks continue on every statement and IO path.

Keep `ContextProvider` in the load chain: `WorkerRuntime::load` opens its guarded store first, resolves key via the injected provider, calls `store.validate_transaction(false)` again, opens the encrypted engine, and checks the guarded store again before publishing the loaded instance. A revoke between snapshots and engine construction therefore cannot publish a usable new instance; `OwnershipRuntime::acquire` already checks generation/deadline after `worker.open`. Decryption/key delivery is not atomically revocable: a worker valid at validation can retain the key after losing its lease. The guarantee is authorized release plus fenced SQL/storage use, not that a malicious formerly authorized worker forgets a shared namespace key.

Add `WorkerRuntime::new_with_key_provider(...)` and `OwnershipRuntime::new_with_key_provider(...)`, preserving legacy constructors; the latter still constructs the same ContextProvider and passes the key provider to WorkerRuntime. The construction chain in `ownership/runtime.rs:26` makes this extra wiring necessary. Do not bypass ContextProvider just to reach RemoteAuthorityProvider. `RemoteDatabase::open_with_encryption_and_limits(...)` consumes a resolved configuration; its initial connect and session helper both call native `connect_with_encryption`.

Revised disjoint patch assignment: encryption adapter owner additionally owns `src/authority_rpc/provider.rs` (the snapshot accessor only) and `src/ownership/runtime.rs` (constructor wiring only). No authority server, protocol, root/catalog, pager, IO or P5 controller change is needed for key delivery. `src/service_cli.rs` remains the single integration owner's wiring file. This is an explicit small expansion of the earlier encryption patch list.

### Namespace descriptor activation without authority root changes

Before asking the existing controller to place/acquire a **fresh, never-used namespace**, a separately authenticated management caller asks the broker to provision it with an idempotency key. Broker durably generates/seals the DEK and records state `sealed_unbound`, immutable tenant/database/domain/DEK/cipher and revision 1; only after fsync does provisioning return success. Place is not called on failure/uncertainty. A worker cannot invoke provisioning. This management ordering belongs in the new P6 orchestrating sidecar, not a change to P5 internals.

The first key request after a real authority grant may bind the descriptor to that authority's root incarnation: broker validates the grant, unwraps the already persisted DEK, reseals the **same DEK** with root incarnation included in AAD, and under a broker-local compare-and-update persists `ready`, revision 2, before any key response. Then it performs the second authority validation. This first-bind path replaces the earlier assumption that management already has a root incarnation before Place; it needs no new authority RPC or root field. Place may synchronously acquire/open: the sealed descriptor already exists, so first-bind breaks that startup dependency without exposing an unprovisioned key.

The one-time binding trusts the existing authority's authenticated controller to issue grants only for legitimate new namespaces. Namespace names are permanently reserved/tombstoned in broker metadata; existing/plaintext imports and namespace reuse are refused. A broker without backend access cannot prove a namespace is empty: fresh-namespace allocation by the management/operator path is a prerequisite, not something ValidateLease proves. If that prerequisite cannot be guaranteed, first-bind activation must be refused pending an explicit operator-approved fresh namespace. No opportunistic encryption of a discovered old database.

Concurrent first requests must agree on root incarnation; persist via a revision comparison and return only the winning same-DEK descriptor. A different root is rejected, never auto-rebound. Crash after sealing/binding retries returns the same DEK, not a regenerated one; unknown filesystem persistence fails closed. Same-root branch restores use the existing shared domain; changed-root recovery requires explicit descriptor reconciliation. No second live broker or restored metadata snapshot is allowed to overwrite authoritative revisions.

Rewrap increments descriptor revision/envelope version but preserves domain/DEK/root binding; renewal/worker migration changes no descriptor. The descriptor is broker-owned durable metadata, not embedded in SQL pages or authority root. Readonly workers receive the namespace DEK only after a valid readonly grant; once released it decrypts other ciphertext in that same key domain if obtained, so P4 scoped storage access remains essential.

Acceptance additions: worker-A key token plus worker-B binding is denied; forged/missing/replayed/old-term grants fail broker's own ValidateLease; controller token cannot validate; no-worker-authority-credential configuration fails startup. Revoke during unwrap, renewal during resolve, concurrent first bind with conflicting roots, crash after bind before reply, failed Place then retry, broker restart and KEK rewrap all preserve one DEK/namespace. Verify no DEK files/logs/HTTP caches and no S3 credential in Go broker/Gateway. Only this review file is changed; these interfaces remain proposed until the isolated P6 implementation passes tests.
