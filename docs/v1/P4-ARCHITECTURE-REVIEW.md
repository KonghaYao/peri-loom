# P4 architecture review — implementation contract

Review only. Baseline: P2 `2bf3c1e` (124 tests reported by main,
not rerun here); P3 is concurrently changing sessions, resources and compaction.
This document proposes P4 interfaces, not implemented or verified guarantees.
Scope: `prototypes/v1`. Read against `05`, `06`, `08`, `P3-DESIGN.md`, current
authority, ObjectStore, IO, database, WAL, storage, capabilities and GC sources.

## Blocking decisions before implementation

1. **Checksum migration first.** `authority.rs::decode` hashes a typed root
   reserialization. Adding `serde(default)` ownership fields changes that byte
   stream and rejects valid P2 roots. Verify the original root bytes before
   decoding the new type; defaults alone are not a migration.
2. **The authority must be the actual storage access boundary.** Only the
   authority process receives S3 credentials or a writable local object directory.
   SQL workers receive scoped authority sessions. The P3 local provider is a
   development adapter, not evidence of storage fencing. Checking an epoch in
   SQL ingress, `validate_session`, or a worker-side wrapper is insufficient.
3. **Root CAS cannot fence already dispatched physical operations.** A conditional
   root PUT orders publications; it does not make blob PUT/DELETE atomic with an
   epoch or lease check. Freeze the barrier and ambiguous-operation behavior below
   before claiming `05`'s invariant. A generic S3 backend cannot establish literal
   “no physical byte changes after lease deadline” with this design alone.
4. **Restart and HA are separate from worker failover.** An in-process mutex only
   protects one authority instance. P4 can deliver worker failover behind one
   exclusive authority, with fail-closed authority restart. It cannot claim
   overlapping authority failover or strict deadline-time backend cancellation.
5. **ReadOnly is a native connection role.** `query()` can execute writes and
   `PRAGMA query_only` can be reset by SQL. Neither a method name nor SQL regex is
   enforcement. Freeze P3's final native-session interface before wiring P4.

## Boundary and durable identity

Topology: Go/control principal → six coarse worker commands → Rust ownership
runtime → authenticated Rust storage protocol → Authority → raw ObjectStore.
The storage protocol is Rust-to-Rust; it is not a Go page/WAL API. Retain
`WorkerRuntime` as the P3 name; “RustRuntime” describes its P4 ownership layer.

Use namespace-wide serialization initially: one catalog root contains shared
objects, refs, pins and GC state across branches. Leases are branch scoped;
namespace mutations additionally require a control/maintenance permission.
Do not use one branch writer's lease to authorize cross-branch destructive GC.

Add versioned root metadata:

- `authority_term`: durable, checked-increment boot term, distinct from permanent
  root incarnation; changes only after exclusive-startup reconciliation.
- `ownership[branch]`: epoch high-water mark, branch incarnation, worker identity,
  lease ID, grant request ID, renewal sequence, and Active/Draining/Revoked state.
  Keep epoch tombstones after branch deletion; recreation cannot reuse an epoch.
- Durable command receipts: principal + request ID + payload digest + result.
  Reusing an ID with different contents is an error. Existing root receipts alone
  are insufficient: `transact_inner` currently generates a fresh ID per call.

An opaque worker capability binds namespace, branch, permanent root incarnation,
authority term, branch incarnation, epoch, worker identity, lease ID, role and
session ID. Match all fields exactly, including `epoch == current_epoch`, on
every mutation; `epoch >= wal.epoch` is not permission to mint ownership.
Go proposes a higher epoch under policy; authority CAS accepts it only against
the expected current ownership. Workers cannot allocate epochs. Epoch exhaustion
fails closed. Restore changes branch incarnation/timeline and revokes its grant;
never restore ownership metadata from a historical recovery point.

## Single-authority admission and the precise fence

Use one namespace admission gate shared by ownership commands, engine mutations,
pin changes, refs, upload reservations, retries and physical GC. Reserve operation
membership under the gate, perform network IO outside its mutex, retain an
in-flight ticket until the backend outcome is terminal. Never hold P3's global
registry mutex during this work. A namespace transition closes old admission
before waiting for tickets; queued requests must revalidate when admitted.

Each durable stage checks current capability/state/deadline immediately before
dispatch. Each root-CAS retry reloads the root and repeats permission and semantic
checks; no closure validated once may silently retry under a newer epoch.
Ownership transitions do not reopen admission until their durable root CAS and
receipt are confirmed. Recovering from an ambiguous CAS is readback of that same
operation, never a new blind mutation. Confirmation of an earlier committed
operation is historical success, not renewed permission to write.

For takeover/revoke: close old admission → settle dispatched old operations →
CAS ownership transition → admit the new owner. Undispatched stages are canceled.
If a physical request's outcome is ambiguous, retain its ticket and durable
intent; return Pending/Unknown, keep the transition blocked. A timeout, canceled
future, dropped socket, or one GET reporting absence does not prove that the
server cannot execute a delayed request. Do not advance the epoch and report a
completed strict fence while such an operation remains possible.

Exact guarantee ledger:

| Situation | P4 guarantee / limitation |
| --- | --- |
| Request admitted after stale epoch, revoke or expiration | Reject before any durable dispatch, including orphan upload or GC delete. |
| Old request already dispatched during valid lease | May complete; no claim that transport cancellation retracts it. Revalidate before any following stage. |
| Successful barrier and epoch transition | No old physical request remains unresolved; no later old publication or physical dispatch is allowed. |
| Root CAS from old snapshot races newer ownership CAS | At most one matches that ETag; stale candidate cannot publish after the newer CAS. |
| Lease expires with CAS/PUT/DELETE in flight | Admission stops at deadline; an earlier dispatched operation may finish after it. This is not strict physical deadline fencing. |
| Lost publication ACK | Resolve receipt; do not discard an acknowledged tail or report definite rollback without evidence. |
| Authority crash / unresolved transport | No automatic restart-and-resume claim. Reconcile or remain unavailable as described below. |

If product acceptance interprets “stale epoch cannot produce a durable mutation”
as physical completion after the epoch transition, the barrier is mandatory.
If it also means no completion after the wall/lease deadline, P4 is blocked until
the backend supports an atomic credential/term/deadline-conditioned mutation or
an equivalent proven sequencer. Local prechecks and root CAS cannot prove that.

An alternative that advances ownership while old immutable PUTs may arrive can
protect published state using root CAS, permanent unique IDs and retained upload
intents. It permits late orphan bytes and therefore is a **weaker contract**, not
the strict P4 acceptance path. Never delete unresolved intents to disguise this.
Late DELETE is tolerable only for a permanent tombstoned exact physical instance;
that preservation property likewise does not prove absence of stale mutation.

Restart requires external exclusive authority ownership (P4 local process lock
on one host is sufficient only for that deployment), no surviving old issuer,
and resolution/quiescence of its outstanding backend operations. Killing the
process alone does not establish backend quiescence. If this cannot be proven,
remain closed. Once established, CAS a new authority term, invalidate all prior
leases/sessions, preserve epoch high-water marks, and require fresh higher-epoch
grants. Never reconstruct a live monotonic lease from stored wall timestamps.
Root term CAS alone does not fence a second credential-bearing authority's raw
blob operations. HA requires backend enforcement or a separately proven exclusive
sequencer; document it as outside P4 rather than implicit in P6 TLS work.

## Lease protocol and runtime states

Authority keeps authoritative monotonic deadlines in memory; worker deadlines
are conservative secondary guards. Wall timestamps are diagnostic only. Use an
injectable monotonic clock with documented suspend behavior; suspend must count
against the lease, or resume must revoke before any dispatch. Clock regression,
overflow, unavailable clock, or uncertainty invalidates the lease. System clock
rollback must not lengthen ownership. Historical commit timestamps remain P2
ordering metadata and must not be reused for lease decisions.

Acquire/Renew use an authority-issued, single-use nonce bound to principal,
target, expected ownership and an authority monotonic expiration. Thus a delayed
first delivery cannot become an arbitrarily late new grant. A recorded grant
retry returns its original result/status, never a new duration. An expired or
revoked grant cannot be resurrected by Acquire replay or Renew.

Renew requires exact lease/term/epoch, next renewal sequence, valid nonce and
`now < old_deadline` both at admission and before persistence. Reject out-of-order
or expired renewals; duplicates return the original deadline. A renewal confirmed
only after the old deadline must not reopen write admission: invalidate it and
require a fresh grant. Persist renewal identity before reporting success, but
never use that record to reconstruct a deadline after restart.

For worker secondary timing, authority returns remaining TTL computed at reply
construction. Worker sets its conservative deadline from the monotonic **request
start**, not reply receipt, plus returned TTL (with safety margin / bounded clock
rate assumption). If already elapsed, reject. Authority remains the final check;
the worker cannot extend a lease using a controller-supplied wall deadline.

| State | Behavior and transition |
| --- | --- |
| Detached | No native engine/session. Acquire reserves loading capacity → Acquiring. |
| Acquiring | Resolve durable grant, bind store, cold recover acknowledged state. Recheck generation token and deadline before Writer. Late completion cannot undo revoke. |
| Writer | Accept sessions only with live grant; guard native write admission and every IO durability path. |
| Draining | Stop new sessions/transactions. Existing transactions may finish only within the still-valid lease and bounded drain policy; final checkpoint/compaction also requires it. |
| Revoked | Immediate local write rejection; interrupt/abort sessions, never flush uncommitted state as cleanup. Only fresh Acquire can leave this state. |
| ReadOnly | Separate native read-only engine/session on an authority-pinned view. No implicit reuse/downgrade of a writer connection with an active transaction. |

Revoke invalidates local admission immediately; its completed durable response
requires the authority barrier. Drain is not revoke: after transactions finish
and optional safe compact succeeds, release ownership through the same barrier.
Deadline/revoke during drain aborts further durability work; it cannot flush under
an expired grant. Report drain failure/unknown explicitly. Close/evict releases
native sessions first; late callbacks are checked against a runtime generation
token and cannot install a Writer into a replaced registry entry.

Wire native OpenOptions/OpenFlags read-only semantics through P3's final session
API and verify the engine reports that role. `RemoteFile.readonly` alone currently
does not guard all sync/checkpoint paths. Enforce role in native access and remote
authorization as well as IO write/truncate/sync/reset/checkpoint callbacks. If
native read-only open/recovery cannot be supported, return Unsupported; never
emulate it with a SQL classifier. Disable unauthorized ATTACH/extension/host-file
escape paths through native policy/IO, including statements executed via query.

## Closed interfaces and mutation coverage

Freeze these semantic interfaces before distributing files; precise serialization
types and errors belong to main. No endpoint accepts a root value, closure,
physical key, caller-selected pin, or arbitrary reference graph.

| Interface | Bounded contract |
| --- | --- |
| Worker ownership commands | `AcquireDatabase`, `RenewLease`, `RevokeDatabase`, `DrainDatabase`, `GetDatabaseState`, `GetWorkerState`; versioned tagged enum with strict payloads, command ID and expected ownership. |
| Authority control | Grant/Renew/Revoke and named P2 ref operations under control identity; expected epoch/incarnation/revision as appropriate, durable idempotent result. Drain transitions use the same ownership record. |
| Authority engine session | `OpenBranch(role, grant)`, `Validate`, `Get`, `List`, `PutImmutable(kind, digest, bytes)`, `PublishWal(expected_etag, bytes)`, `PublishHead(expected_etag, expected_generation, bytes)`, `CloseSession`. |
| Worker ObjectStore client | Implements existing trait over an opaque session; translates only known logical paths. `authority/*`, unknown paths, arbitrary delete and unsupported PutCondition combinations fail closed. Transport failure never falls back to raw storage. |
| Provider seam | Keep P3 `open_store(id, epoch)` by resolving a previously installed opaque grant in the provider; bare epoch cannot authorize. Main may introduce a grant-based seam after P3 merges, with all callers migrated together. |
| RootCAS internal | Expected root ETag/revision + typed authorized operation + command receipt; preserve prepare/semantic validation. Keep `transact` authority-internal, never RPC. |
| Maintenance | Named pin, refs, mark and exact-tombstone sweep operations under separate roles/term; no SQL-worker physical delete privilege. |

Bound frame/body sizes, identifiers and list responses; stable errors distinguish
Fenced, LeaseExpired, ReadOnly, Conflict, Unauthorized, Unavailable and
UnknownOutcome. Authenticate before parsing large bodies or resolving namespace.
An RPC retry keeps its operation ID and payload; the service retains the same
session view and logical ETags across calls. Restart invalidates it, not silently
reopens an unpinned view. Reader pin release is authority-owned cleanup, scoped
to the issued session and independently authorized even after writer revocation.

Every durability mutation goes through the authority gate **and** durable RootCAS:

- Grant, renewal/revoke state and authority term: root identity/CAS validation.
- Upload reservation: root CAS before raw PUT; physical dispatch ticket through
  outcome; logical publication settles the original intent in a later root CAS.
- WAL chunks and index/commit: immutable hash validation, exact granted epoch,
  expected index ETag, existing commit callback, history/current counters in the
  same CAS. Old index/chunk epochs remain valid as historical recovery data.
- Segments/manifests/generation/HEAD: immutable dependencies, expected HEAD ETag
  and generation/parent checked server-side against current branch, lease and
  incarnation; no server trusting merely worker-side generation arithmetic.
- Branch/snapshot/clone/restore/retention/delete: named capability handlers retain
  P2 invariants. Restore/delete quiesce affected ownership; invalidate grants in
  the same root publication. Clones receive fresh ownership, not copied grants.
- Pin creation/extension/release: explicit reader/cleanup authorization and CAS;
  read-only SQL still needs authority-owned pin metadata writes. No automatic
  pin expiry on writer-lease expiry. Retain crash pins conservatively in P4.
- GC mark: exact revision/reachability CAS with namespace maintenance authority.
  Sweep: persisted exact-instance tombstone, no upload intent or reachable refs,
  gated raw DELETE, then completion CAS. Even repeat deletion of a late
  reappearance needs the gate. Preserve permanent negative IDs and ambiguous
  intent retention. Clock anomalies postpone GC rather than shorten safety waits.

## P2 checksum compatibility and rollout

Current `Envelope` is `{ root: CatalogRoot, checksum }`. P2 encode serializes the
typed root compactly for hashing, then embeds that same serialization. For
unmodified P2 output, the exact embedded root JSON bytes are the checksum input.

Implement a codec using borrowed `&serde_json::value::RawValue` (enable serde_json
`raw_value`) or boxed RawValue: parse envelope → hash `root.get().as_bytes()` →
compare checksum → inspect version → deserialize version-specific typed root →
validate semantics. Do not hash `Value` reserialization: ordering/spacing/numeric
representation can change. New encode serializes the typed root once, hashes it,
and embeds precisely those bytes as RawValue. Reject duplicate envelope fields,
unknown versions and malformed roots; never reset on checksum failure.

If compatibility includes externally reformatted envelopes accepted by the old
decoder, raw-byte verification alone changes that behavior. Support that only
through an explicit frozen V1 decoder that reproduces **V1** serialization and
validation; never checksum a V1 root using an extended V2 type. Default P4 scope
is exact P2 writer output, with the limitation documented and tested.

Introduce V2 ownership metadata via an explicit migration CAS under exclusive
authority startup. Update `prepare`/`validate_root`, which currently require
unchanged version / version 1; only the dedicated migration may advance it.
Initialize branches unowned, derive epoch floors at least from retained WAL
metadata, preserve root incarnation and every P2 ref/blob/pin/tombstone/receipt.
Never initialize a missing existing root or reset counters. Existing
`current_sequence/current_commit_ms` are present at `2bf3c1e`: preserve them
exactly, including restored values below high-water marks. Earlier formats that
lack them need a separate validated migration from retained recovery evidence;
do not guess zero or high-water values after restore.

Rollout: capture immutable P2 byte fixtures → land codec compatibility → establish
exclusive authority/no legacy raw writers → migrate CAS → start remote clients.
No mixed P2/P4 writable deployment and no automatic downgrade after V2 publication.
Schema version rejection is not a substitute for removing old raw credentials.

## Three developer scopes plus main

These are future write allocations, not agent dispatch instructions. Wait for P3
integration; no P4 agent rewrites an actively owned P3 file. Main freezes shared
types first. Tests must exercise behavior, not simply duplicate checks.

| Owner | Exclusive files and deliverable |
| --- | --- |
| Main | `src/authority.rs`, `src/authority/types.rs`, new `src/authority/codec.rs`, new `src/ownership/types.rs`, new `src/authority_rpc/protocol.rs`; root codec/migration and frozen contracts. Also `src/lib.rs`, Cargo files, `src/main.rs`, configuration, `src/runtime/types.rs`, `src/runtime.rs`, `src/runtime/registry.rs`, `src/runtime/handle.rs` integration, and `tests/p4_compat.rs` + `tests/fixtures/p4/`. Review shared API patches centrally. |
| Developer A: storage authority | New `src/authority/ownership.rs`, `src/authority/admission.rs`; existing `src/authority/branch.rs`, `catalog.rs`, `semantic.rs`, `src/capabilities.rs`, `src/gc.rs`; `tests/p4_authority.rs`. Implement gate, leases, permission-aware root operations, ref/GC fencing. Request main's root API changes instead of editing its files. |
| Developer B: remote boundary | New `src/authority_rpc.rs`, `src/authority_rpc/server.rs`, `client.rs`, `auth.rs`, `provider.rs`; `tests/p4_remote.rs`. Use main's protocol; implement authenticated service, session-bound ObjectStore/provider, strict key translation, idempotent retries. No raw credentials in worker construction. |
| Developer C: runtime/native enforcement | New `src/ownership.rs`, `src/ownership/runtime.rs`, `clock.rs`; existing `src/database.rs`, P3 native session module if created (main assigns its exact path after merge), `src/io.rs`, `src/io/files.rs`, `src/wal.rs`, `src/storage.rs`; `tests/p4_runtime.rs`. State machine, native role, stage guards, drain/revoke callbacks, preserve P3 compact. No registry-file edits; supply integration calls to main. |

Integration order: codec fixtures/contracts → authority gate and ownership →
remote client/auth → runtime/native → process acceptance. No temporary production
bypass to unblock another developer. Existing P3 tests remain intact; main owns
combined acceptance and resolves interface changes before merging workers.

## Required acceptance evidence

1. **Real boundary:** separate authority and two worker processes, workers with
   no raw credentials/writable backend path. Missing/wrong principal, forged role,
   other namespace, guessed session, root paths and physical deletes are rejected.
   Endpoint is loopback-only for P4, but explicit internal authentication and
   control/worker/maintenance separation are still mandatory. TLS, external
   identity lifecycle and broader P6 security are not P4 claims.
2. **All mutation stages:** pause old writer before reservation, physical PUT,
   WAL publication, segment/manifest/HEAD CAS, pin extension, refs, GC mark/delete/
   completion and CAS retry. Take over; stale dispatched stages follow the
   barrier contract, all newly attempted stages fail, new writer recovers every
   previously acknowledged commit. Audit raw backend calls, not only root values.
3. **In-flight counterexample:** delay PUT/DELETE server-side past client timeout.
   Assert strict takeover remains Pending until terminal/quiescent; separately
   demonstrate why permitting late orphan PUTs cannot pass strict fencing. Delay
   root CAS across expiration and record the admitted-in-flight limitation.
4. **Lease adversaries:** fake monotonic/wall clocks, backward/forward wall jumps,
   suspend/resume, exact deadline, overflow, authority/worker restart, lost grant
   ACK, duplicate IDs with changed payload, delayed Acquire/Renew, out-of-order
   renewal, revoke during recovery/drain/checkpoint. No callback resurrects Writer.
5. **Native sessions:** writes via both execute/query, prepared statements,
   triggers, CTEs, multi-statements, query_only toggling and ATTACH cannot modify
   a read-only database. Independent transactions remain isolated. Expiry during
   COMMIT/checkpoint surfaces error/unknown accurately, never invented success.
6. **P2 golden data:** immutable root/envelope and referenced blob fixtures emitted
   by `2bf3c1e`, not regenerated using new structs. Verify checksum, migrate, reopen,
   reject corruption and test lost migration ACK. Include current commit below
   historical maximum after restore, branches, clone, snapshots, PITR/time lookup,
   crash pins, unresolved uploads and permanent tombstones.
7. **P3/P2 preservation:** acknowledged uncheckpointed tail survives worker kill
   and empty-cache reopen; safe native compaction bounds current replay while old
   PITR/snapshot WAL remains reachable. GC after migration/restore/compaction must
   preserve all retained points and reader views; stale handles cannot publish.
   Run existing authority/capabilities/recovery/GC/P3 suites, then full suite;
   report actual counts and backend used, without reusing the reported P2 count
   as new P4 evidence.

P4 is acceptable only with the guarantee ledger explicitly attached to results.
Do not label loopback authentication as production TLS security, a local mutex as
HA, or publication safety with late orphan writes as strict physical fencing.
