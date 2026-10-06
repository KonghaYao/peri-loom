# P0 Turso fork boundary

## Direct Git fork development

The dependency is a Git submodule at `turso/`, with `origin` set to
`https://github.com/KonghaYao/turso.git` and `upstream` pointing to the official
repository. Development uses branch `codex/p0-core-prototype`, based on
`8549c16595d2faf1bdd6ee24aee0be8bfabb3d4a`. The original registry
package's `.cargo_vcs_info.json` identified the same commit and `path_in_vcs:
core`. The checkout preserves
upstream's MIT license at `turso/LICENSE.md` and all upstream notices.

`Cargo.toml` uses `turso/core` and excludes `turso` from the prototype workspace
so upstream workspace inheritance resolves against its own root. Core, parser,
macros, and extension sources are local workspace path dependencies from the
same pinned Git checkout, not independently resolved registry patch releases.
No dependency-version manifest modification is required in the fork.

Edit the upstream source files directly in this Git checkout. There is no vendor
snapshot, separate patch file, or replay script. Review changes with
`git -C turso status --short` and `git -C turso diff`. The parent repository
tracks `.gitmodules` and a mode-160000 gitlink rather than ignoring the checkout.
The existing four source edits were preserved byte-for-byte when moving the Git
directory into the parent's `.git/modules/turso`. The source changes are committed
as `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`, which the parent gitlink records.
No push has been performed. The configured submodule branch and this commit are
not yet published; publish the child commit before sharing the parent revision.
Ordinary submodule initialization uses the gitlink rather than a branch name.

## Exact patch boundary

The final fork changes exactly four upstream source files, with 29 insertions
and four deletions relative to the pinned commit:

- `core/io/mod.rs`: add the object-safe default no-op
  `File::checkpoint_wal_position(salts: [u32; 2], frame_count: u64) -> Result<()>`.
- `core/storage/database.rs`: add the same default hook to `DatabaseStorage` and
  forward it through `DatabaseFile` to the underlying `File`.
- `core/storage/wal.rs`: add `CheckpointResult::checkpoint_wal_salts`, initialized by
  the existing constructor; fill it from the coordination backend WAL header in
  both the no-work return and completed-backfill result. Capture completed-backfill
  salts **before** `restart_log()`: RESTART/TRUNCATE can advance the in-memory
  generation before pager receives the checkpoint result.
- `core/storage/pager.rs`: pass the saved salts and this checkpoint result's
  `wal_total_backfilled` immediately before checkpoint DB sync. No hook or publish
  is performed before DB truncate; the following sync owns the durable barrier.

There are no SQL VM, parser, B-tree algorithm, WAL recovery, codec, checkpoint
locking, proof, or nbackfill-ordering changes. Existing File/DatabaseStorage
implementations inherit a no-op hook. The added result field is a source-level
addition: external exhaustive struct literals must account for it; existing
constructor users and `..` destructuring are unaffected.

## Adapter contract

`SegmentedIO::open(root)` creates/canonicalizes the directory, acquires a
nonblocking OS exclusive lock on `.peri-loom.lock`, then opens segmented storage.
It rejects an existing physical `root/database`. The canonical UTF-8
`root/database` is a virtual name only: no complete `.db` or `database` is created.
Main file identity is a synthetic hash of the canonical virtual name.
Main/WAL path matching also canonicalizes the parent directory for path aliases
(including macOS `/var` versus `/private/var`), so an alias cannot accidentally
create a physical main database or bypass the WAL reset wrapper.

The directory lock is retained by all main/WAL handles, not just SegmentedIO.
Main `unlock_file` cannot release it. Duplicate same-process and cross-process
owners are rejected, regardless of native Turso lock-disable environment variables.
The lock file is never removed, avoiding replacement-inode races. This uses
standard-library file locking (Rust 1.89+); the local toolchain is Rust 1.98.1.

Main reads/writes use `SegmentedStorage`. Completions report actual read counts,
full successful write/vectored counts, and zero for sync/truncate. Reads clear the
unused EOF suffix. Read-only handles reject writes/truncate. Storage/publication
failures complete with an error and return an error instead of leaving callbacks
pending. The vectored override avoids the upstream default's unfinished aggregate
completion on child errors. Temporary files pass directly through PlatformIO;
WAL files use a synchronous PlatformIO wrapper with publication/reset guards.
Clock and `step` delegate to PlatformIO. Host shared-WAL coordination is not
advertised; native in-process coordination remains authoritative.

## Durability and exact position

The File hook stores a pending exact `(salts, frame_count)` supplied by the native
checkpoint result. Main sync synchronizes the existing local WAL first, then
calls `storage.publish(pending_position)`. Successful publication clears pending,
dirty, and failed state. Initialization or standalone sync without a framed WAL
preserves the old manifest watermark and still publishes. Main truncate only
updates storage and marks dirty; it is not durable until the next successful
sync. This applies to initialization and standalone callers as well as checkpoint
shrinking: **truncate must be followed by sync**.

The native durable sequence remains WAL sync, backfill writes, optional DB
truncate, DB sync/publication, durable backfill proof, then nbackfill advancement.
WAL truncate remains after successful DB publication. The extra adapter WAL
`sync_all` is a conservative local durability barrier, not a parser. Native
checkpoint WAL `FullFsync` remains upstream's responsibility; this additional
standard-library barrier does not independently implement Apple F_FULLFSYNC.

Partial/PASSIVE checkpoint publication uses `result.wal_total_backfilled`, never
the latest commit or physical WAL length. Reader-pinned partial checkpoints do
not record unbackfilled committed tails. There is no WAL scan, checksum
reconstruction, or per-page image comparison in this adapter: native recovery
handles partial/invalid crash tails. Saved pre-restart salts retain the applied
generation even when TRUNCATE later zeroes result counters or restart rewrites
the WAL header.

WAL header rewrite, truncate, and removal fail closed while main data is dirty,
a checkpoint position is pending, or the last publication/WAL sync failed. A
successful publication retry is required before reset. Dirty main sync with WAL
length greater than its 32-byte header requires the exact hook. The guard does
not parse committed tails: native Turso must decide when WAL reset is legal.
Raw callers must not arbitrarily truncate a clean WAL containing new commits;
this is not a standalone general-purpose WAL-management API.

## Limits and verification

- P0 has one process owner with native in-process connection coordination, not
  distributed/shared-WAL support. OS locks are advisory; network filesystem
  locking and durability are not claimed. Do not unlink/replace the lock file.
- Use native durable sync modes. `synchronous=OFF`, direct mutation via the
  returned `Arc<SegmentedStorage>`, external WAL manipulation, MVCC/VACUUM variants,
  and asynchronous replacement backends are not supported safety contracts.
  PlatformIO is synchronous on the validated macOS target.
- The gate supplements, not replaces, Turso transaction/checkpoint locks.
  Synchronous passthrough callbacks must not reenter this IO adapter.
- The real Git checkout passed all 23 unit tests and eight acceptance tests via
  `cargo test --offline --locked`, including the independent-process child probe.
  Existing upstream warnings remain; the adapter test module introduces no warnings.
  Adapter unit tests in `src/io.rs` cover byte counts, EOF clearing,
  truncate-then-sync durability, exact partial watermark without tail parsing,
  fail-closed reset/retry, read-only/vectored errors, clocks, virtual identity,
  same-process lock lifetime, and independent-process exclusion.
- SQL checkpoint/crash integration and replay are tested against the real fork.
  Run
  `cargo test --offline --locked --lib io::tests -- --nocapture` for the focused
  adapter suite, then the SQL integration suite. Acceptance tests verify actual
  pre-restart salts and frame counts across a native TRUNCATE checkpoint.
  Reader-pinned partial checkpoints and native experimental VACUUM shrinking
  remain outside the public single-connection prototype's acceptance scope;
  synthetic hook tests alone do not prove those native paths.
