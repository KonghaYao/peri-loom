# P2 implementation contract

This document specifies implementation work, not acceptance evidence. P2 uses
one sharing domain (`Namespace`) with named branches. Cross-domain cloning is
not claimed: it would need an additional durable pin/transfer protocol.

## Publication authority

A trusted Rust `Authority` owns the raw `ObjectStore`. SQL engines use a
branch-bound adapter, never construct a replacement root. One durable root CAS
serializes logical object publication, refs, recovery history, read pins and GC.
P4 adds ownership to this same root, not a separate epoch record.

Immutable physical blobs use unique instance IDs, even when logical segment
names are BLAKE3 hashes. Delayed deletion must not delete a later upload of the
same content. Logical entry ETags are independent of the physical ETag and root
revision. Root initialization is `Absent`; its identity is never reset.

An upload intent is persisted before a physical PUT. Unresolved intents are
conservatively retained, rather than being declared absent after a timeout.
Ambiguous PUT/CAS acknowledgements require readback of operation identity and
content. A CAS retry reruns conditions and semantic dependency validation.

## Consistent recovery and references

A branch session pins one root revision and its compatible HEAD/WAL view.
Native lazy segment reads remain protected for the lifetime of retained handles.
Durable explicit-release pins may conservatively leak after a crash; expiry is
not assumed safe. A restored or deleted/recreated branch changes incarnation,
invalidating old sessions. Restore requires reopening the SQL engine.

Snapshot/fork/clone copy only logical references within the sharing domain.
They do not GET/PUT segment bodies. Snapshots are immutable recovery points,
including the WAL view needed for acknowledged commits after the base generation.
Branch publication is copy-on-write and does not mutate snapshot references.

## Commit history and PITR

At native WAL durability sync, detect a complete final frame with a nonzero
database-size commit marker. Publish the marker with the WAL index; the root CAS
atomically records a recovery point with authority-assigned time and increasing
commit sequence. Do not infer commits from SQL text or `execute()` returning.
Retained recovery points pin their entire immutable WAL index and dependencies,
so WAL header resets and compaction do not invalidate older points.

Time selection returns the latest retained committed recovery point at or before
the requested time. Sequence selection resolves same-time commits. Requests
before retained history fail explicitly; idle time after the last commit is
valid through the greater of authority wall time and the branch's monotonic
commit-time high-water mark. Later future times fail. These logical monotonic
timestamps are not a proof of precise UTC ordering under wall-clock regression.
Retention removal is metadata-only and
must not discard dependencies of surviving snapshots or reader pins.

## Garbage collection

Mark from branch HEAD/WAL, snapshots, retained recovery points, session pins and
upload intents. Traverse manifests to segments and WAL indices to chunks.
`Manifest.parent` is a lineage edge, not unconditional retention of every past
generation. The collector may reclaim history only after retention policy drops
the corresponding recovery points.

Tombstone unreachable objects with CAS against the marked root revision. Remove
their logical mappings atomically; metadata publication rejects missing or
tombstoned dependencies. On a revision conflict, mark again. Wait the configured
safety window before deleting exact unique physical blob IDs. Retry deletion
idempotently after crashes. Never resurrect a tombstoned physical ID.

## Batch ownership

- Authority worker: `src/authority/`, `src/authority.rs`, branch adapter, durable
  root CAS/intent/pin semantics and authority tests.
- Recovery worker: native commit marker, WAL metadata/append integration,
  `src/recovery.rs`, recovery tests; existing P1 signatures remain compatible.
- Capability worker: `src/capabilities.rs` and capability tests; only authority
  APIs, never raw physical deletion.
- Main: GC planner/executor, composition exports, CLI and integration acceptance.
- Independent verifier: its own adversarial tests and P2 verification report.

P2 remains a trusted storage prototype, not a distributed owner protocol. P4
must close authorization, lease and stale physical-operation admission at the
authority boundary. No P2 test can substitute for that later safety proof.
