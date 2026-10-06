# P3 batch implementation contract

This is a work contract, not acceptance evidence. P3 uses the P2 authority path;
the old P1 raw-layout compatibility CLI is not the new worker's storage model.
Distributed writer selection, leases and epochs remain P4/P5 work.

## Runtime and provider boundary

`DatabaseId` is `{ namespace: Namespace, branch: String }`, validated at every
public boundary. A `DatabaseProvider: Send + Sync` supplies
`open_store(&DatabaseId, epoch: u64) -> Result<Arc<dyn ObjectStore>>`.
The P3 local provider opens the authority and a branch-bound store. P4 replaces
this provider with an authorized storage-service client; SQL workers must not
retain unrestricted raw object-store credentials in that deployment.

`WorkerRuntime` owns a dynamic registry and cache root. Its public surface is
`new(provider, cache_root, limits)`, `open(id, epoch)`, `close(id)`,
`drain(id)`, `evict_idle(duration)` and `status()`. Open returns a scoped
`DatabaseHandle`, not an untracked raw engine/IO escape hatch. Handle operations
are execute/query/checkpoint, resource status and transaction-state inspection.
The exact Rust return types are frozen before dispatch.

Reserve opening capacity under the registry mutex, perform remote/engine IO
outside that global mutex, and coalesce same-key opens. Failed opens release
capacity and wake waiters. Different DBs must not block on another DB's slow
storage operation. Capacity includes loading instances, not just loaded ones.

Close/eviction reject active handles. Drain stops new admission while existing
sessions finish or abort; the final handle releases its connection before the
instance can unload. Idle timing uses a monotonic clock. An unloaded instance
has no live engine, WAL image or attached DB volume. Its remaining segment cache
is optional and disposable. A force-close must not silently invalidate active
transactions or acknowledge uncommitted state.

## SQL sessions and accounting

Different handles use different native Turso connections to one engine. BEGIN,
COMMIT and ROLLBACK may span calls within a handle, not leak into another
client's transaction. Native sync/auto-action protections and branch validation
apply to each connection. Dropping the last session aborts its unfinished
transaction and releases native handles; it cannot retain a hidden writer lock.

Account loaded/loading DBs, active sessions, activity/idle age, segment RAM/SSD,
dirty segment payload and in-memory WAL/pending payload. Keep metrics honest:
cache payload accounting is not total process RSS or all native engine memory.
Limits include open DBs, total/per-DB sessions and per-DB cache bytes. Admission
and cache caps must have real behavior, not only configuration fields.

## Cold recovery and migration

An empty-cache worker opens remote HEAD/WAL and fetches segments lazily. Native
startup may need header pages; do not equate engine open with zero segment GETs.
No local main DB or persistent WAL is allowed. Migration moves no full DB file.
Tests distinguish required uncheckpointed WAL tail from old history that need
not be transferred during a checkpointed cold open.

Add a safe native truncate-checkpoint and current-WAL compaction operation:
checkpointed state is in immutable generations, while the current WAL index
keeps a replayable small baseline. Old immutable index views remain pinned by
P2 recovery history/snapshots. Never discard native headers, uncommitted dirty
state or acknowledged tail; busy/failed durability operations fail explicitly.
Use this bounded current stream to prove large checkpointed DB cold open does
not replay all archived WAL. Graceful migration can quiesce and compact; abrupt
worker loss must still recover acknowledged tail without that optimization.

## Batch write scopes

- Registry worker: `src/runtime.rs`, `src/runtime/`, registry/lifecycle tests.
- Engine/accounting worker: native session module, `src/database.rs`, IO/storage
  resource accessors and safe compaction, session/resource tests.
- Acceptance worker: process CLI/worker-disposal and migration acceptance tests,
  fixtures and documented reproducible commands; no registry/engine edits.
- Main: frozen shared types/provider composition, exports/config and integration.
- Independent verifier: adversarial acceptance and P3 report, no production edits.

Required counterexamples include concurrent same-key open, failed-open rollback,
capacity races, slow DB versus another DB, idle eviction with a live transaction,
separate-session rollback isolation, drain admission, worker SIGKILL with ACKed
tail, full cache removal, large-DB lazy migration and old P2 PITR after compaction.
