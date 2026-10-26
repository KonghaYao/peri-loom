# Local immutable object packs

This is an opt-in physical layout for the Authority's local object store. It
retains logical keys, values, etags, conditional writes and the final root CAS.
It does not put a durable WAL on a Worker, change S3 acknowledgement semantics,
or implement a new global key/value index.

## Enable and reopen

Use a new empty store with `PERI_LOCAL_OBJECT_PACK=1`, or call
`LocalObjectStore::open_packed`. The admission check runs under the actual
store's `.lock`, probes hard links/rename/directory sync on that filesystem, and
durably publishes `.peri-local-object-format` containing `pack-v1\n`.
Existing non-empty legacy stores are refused; there is no online migration.
Existing marked volumes reopen as pack-v1 even without the environment opt-in.
Unknown marker versions and symlink staging directories are rejected.

The launcher persists this choice for its owned Authority process. Do not
downgrade a volume that has accepted packed writes to an older binary. Small
single puts, including the Authority seal, use the new container and are not
readable by the old JSON-only reader. Large objects deliberately retain their
standalone JSON envelope; this mixed layout is part of pack-v1, not an online
migration mechanism.

Only Unix filesystems that pass the capability probe are admitted. A probe is
not proof of power-loss behavior: production qualification still needs the
supported storage failure model, crash tests and real durable-volume evidence.

## Publication and durability

Small immutable metadata pages in the same namespace/family share bounded pack
files. Mutable heads, recovery pages, receipt pages and opaque physical blobs
are kept in separate groups. Opaque blobs are deliberately not classified by
guessed payload contents. Objects above 32 KiB remain separate files.

A pack has at most 32 entries, 256 KiB of binary payload and 64 KiB of directory
JSON. Each live logical key keeps its existing `hash(key).obj` filename as a
hard link to the pack inode. There is no global key-to-offset map. Reads open
the real alias and fetch only the matching slice after validating its checksum.

Publication stages and syncs pack bodies, then acquires the actual STORE lock
and rechecks every condition against the current objects. Dependency aliases
are published and their directory synced before the final root alias rename.
This dependency barrier also runs when a retry adopts every preceding alias:
an alias visible after an earlier failed attempt does not prove its directory
entry has reached durable storage.
An entirely adopted immutable group also completes a directory sync and barrier
before success, without rewriting bodies or changing etags.
An idempotent delete of an already absent alias similarly completes directory
durability, covering retries after unlink succeeded but its sync failed.
The root directory is then synced before returning success. macOS also keeps
the required hardware barriers. A one-pack publication with root in the pack
therefore needs one body sync plus two directory syncs; extra packs, large
objects, housekeeping and maintenance are counted separately.

The ordinary grouping policy isolates a conditional root, so a publication
with many small index pages commonly has two body syncs plus two directory
syncs. The counter is physical calls, not the number of logical objects.

Independent packs and standalone large bodies are staged by at most four scoped
workers per publication. Every worker completes before final conditions are
checked or any logical alias is published. Workers own temporary-name cleanup;
a failed or panicked worker cancels pending jobs, and all workers are joined
before the attempt returns. Phase timings are accumulated on the caller after
joining, counting each physical pack body once.

Group `Any` keeps immutable adoption/conflict semantics; single `put(Any)`
keeps mutable overwrite semantics. `Absent`, `Match`, duplicate-key ordering,
and etags are not replaced by content equality. The store lock is shared across
processes and is independent of the Authority's STATE_DIR startup lock.

A non-precondition error after publication starts remains uncertain. It must
not be interpreted as proof that SQL did not execute. The storage layer never
re-executes SQL.

## Bounded reads and reclamation

The decoded-directory cache has both a 128-pack count ceiling and a 2 MiB
conservative allocation estimate. It caches no payload. Each access first
opens the actual alias and binds a cache entry to the descriptor's device,
inode, size, timestamps and full header. Every payload is still checksummed;
a deleted alias cannot be resolved from a cached directory.

Before removing/replacing an alias, the store inspects only that pack's bounded
directory. If losing the alias would leave it mostly dead, the other *currently
live* aliases are copied to a smaller durable pack first, preserving bytes and
etags. Inspection and alias replacement stay under the STORE lock. A crash
during relocation leaves each alias referring to valid old or new bytes.
The final unlink frees the old inode once no open reader retains it.
If structural/checksum corruption is found before relocation starts, optional
compaction is skipped and counted. This permits an unconditional overwrite to
repair its target and prevents a corrupt peer from blocking a healthy key's
removal. It does not make that peer readable or suppress I/O, permission, or
publication failures.

`list` emits the object represented by each actual alias filename, not every
entry present inside its pack. Deleted entries are never resurrected by
listing or compaction. Packing does not reduce filesystem directory-entry
counts and is not a claim about million-database density.

Temporary links live in a dedicated staging directory. Active attempts hold
inode locks; bounded cleanup skips locked attempts and removes abandoned
names. It does not scan all live database objects or maintain a persistent
unbounded cleanup queue.

Backups must preserve hard links or export logical objects. A plain copy that
duplicates the complete pack for every alias can amplify backup size.

## Verification and diagnostics

`local_storage_metrics()` reports physical body/directory sync calls, pack
creation, actual payload/directory bytes read, cache occupancy/hits, compaction
bytes and cleaned orphan names. Counts include foreground maintenance but
exclude one-time capability-probe calls.

Tests cover conditional/duplicate writes, cross-process CAS, root/dependency
publication failures, corruption, dead aliases, half-live reclamation, large
legacy objects, cache identity/invalidation, byte/count budgets and format
admission. Failure injection is not equivalent to a power-loss test.

The 32 KiB pack/legacy boundary is tested with real file-read call counts.
Legacy envelopes must use bulk file reads before JSON slice parsing; passing an
unbuffered file directly to serde JSON parsing caused a measured full-stack
regression that the small-page microbenchmark did not expose.

An ignored ABBA microbenchmark is available:

```bash
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --release --lib \
  packed_publication_microbenchmark -- --ignored --nocapture --test-threads=1
```

It reports object-store publication latency and point-read cost. It is not the
Hrana end-to-end acceptance gate; that gate must include the original separate
write/read requests, the default quotas, long-running maintenance and recovery.
