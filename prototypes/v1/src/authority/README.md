# P2 publication authority

`Authority` privately owns the physical `ObjectStore`. Its root lives at
`namespace/authority/root`; unique physical instances live at
`namespace/authority/blobs/<blob-id>`. Engines receive an incarnation-bound
`Arc<BranchStore>` using unchanged P1 namespace keys. No raw-store accessor or
arbitrary physical-delete API is exposed.

## Publication and receipts

Root initialization uses `Absent`, starts at revision 1 with branch `main`,
and checks its envelope digest, namespace and permanent root incarnation.
Existing malformed or missing roots never get reset by a live authority.
`transact(Some(revision), ...)` returns a downcastable `PreconditionFailed` on
revision mismatch; it does not retry an obsolete GC candidate. Transactions
without a supplied revision retry typed root-CAS conflicts, rerunning the
mutation and semantic dependency validation against the latest root.

Every root publication includes a durable operation receipt. Lost root ACKs
are resolved by reading that operation's receipt, even if another root CAS
has already advanced the revision. Non-CAS failures without a confirmed receipt
remain errors. Receipt metadata is currently retained without compaction.

Uploads first reserve an instance in both `blobs` and `uploads`, then issue the
physical conditional PUT. An ambiguous upload ACK requires exact-content
readback. Publication settles its intent in the same root CAS that installs
logical objects or branch/recovery references. `blobs` keeps all known physical
instances, including overwritten HEAD/WAL blobs and unreachable catalog slots.

Failed or unresolved publication intents remain conservatively retained. Even
when PUT appeared complete, this implementation does not expose an abandonment
operation claiming that no dispatched retry can arrive later. Known catalog
objects whose intents were settled can become GC candidates after references
and pins disappear. Never-published intents may therefore leak indefinitely.

## Views and recovery

A new branch session atomically pins its consistent HEAD/WAL view, all catalog
objects, branch references, history references and snapshot references.
Publications extend that pin before returning. Immutable old views remain
readable through the final retained adapter/native handle. Only final
`Arc<BranchStore>` destruction releases the pin; process crashes may leave
durable pins, with no unsafe expiry assumption.

Mutable reads use the session view; write conditions compare logical ETags
against the latest branch reference. Every publication rechecks the bound
incarnation. `validate_session` additionally checks the latest incarnation and
pin, allowing SQL ingress to reject cached queries after restore/recreation.
Pin release is housekeeping and remains possible after incarnation changes.

WAL index publication uses the frozen recovery callback to validate committed
chunks from the candidate catalog. Only a new valid marker relative to the
branch's previous index creates history, atomically with its WAL reference and
current HEAD. Sequence/time high-water marks advance monotonically; current
sequence/time record the actual restored state. Markerless appends do not
invent history, and restored timelines do not compare against old maximum LSNs.

## Semantic validation and GC

Every root CAS validates all catalog manifests and referenced HEAD/WAL metadata,
including logical digests and registered, non-tombstoned dependency identities.
Validation loads metadata, not segment bodies. Reachability starts only at
branch/recovery/snapshot references, pins and unresolved uploads, not all catalog
entries. `Manifest.parent` is lineage, not unconditional retention.

GC can tombstone only exact previously registered instances. Negative IDs are
permanent, and public transactions cannot fabricate a deletion acknowledgment.
`delete_tombstone` requires a persisted tombstone, no active upload intent and
no reachable reference, deletes only its exact instance, then atomically marks
completion and removes the registry entry. An already-completed tombstone still
checks for and deletes a late physical reappearance without changing the root.
The caller supplies the safety window; this is not an ownership/lease protocol.

## Scoped evidence

```sh
cargo test --locked --test authority
rustfmt --edition 2021 --check --config skip_children=true src/authority.rs src/authority/*.rs tests/authority.rs
```

The current authority suite passes 13 tests: typed strict conflicts, latest-root
retries, lost-ACK receipts through intervening CAS, no reset after root failure,
durable intent ordering/readback, stale publication rejection, final-Arc pin
lifetime, disk reopen/crash pins, unique-instance late deletion/reappearance,
narrow deletion guards, metadata-only dependency validation, envelope corruption
and concurrent updates. This suite uses fault-injected memory and local stores;
it does not independently claim live S3 acceptance or a distributed P4 fencing
proof. Live provider, full joint and independent-verifier gates remain separate.
