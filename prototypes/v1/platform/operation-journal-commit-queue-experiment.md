# Operation journal ordered commit queue and lock boundary

Date: 2026-10-16. Status: implementation + directed correctness tests passed; performance
**NOT MEASURED**.

## 1. Isolation and base

| Item | Value |
| --- | --- |
| Base commit | `997abbdeeb5cbd6b827677a3e39046acc6acf86e` (tag `v1.1.4`), only committed content |
| Worktree | `/Users/konghayao/code/ai/peri-loom-worktrees/opjournal-ordered-commit-d98e6c1e` |
| Branch | `perf/opjournal-ordered-commit-d98e6c1e` |
| Created by | `git worktree add -b <branch> <path> 997abbd` |

`997abbd` is an ancestor of `main`; the five commits after it touch only
`.claude/skills/e01-benchmark` and `docs/`, so the platform Go sources of this base are
identical to the current committed `main`. Nothing from the parent tree's uncommitted work was
copied, staged, reset or cleaned. The earlier `perf/gateway-fixed-cost-…` worktree and its
uncommitted map/struct candidate are untouched and remain a secondary candidate.

## 2. What the review asked for, against the code at this base

| Review claim | Evidence in the base |
| --- | --- |
| An ordinary query/statement persists intent, then calls the worker, then persists the terminal record, then audits, then answers | For non-committing actions `beginDispatchOperation` registers no intent and returns a `pendingOperation`; `settlePendingOperation` writes the single terminal record and is the acknowledgement. A committing action registers through `registerOperation` before dispatch. |
| `journal append` `Sync` holds the shared state lock, so authentication blocks | `stateStore.lockState()` takes `disk.mutex`; `updateOperations` held it across `operationJournal.appendOperations`, which did `WriteAt` + `file.Sync` (+ witness, + periodic `compact`) inside that hold. Every narrow read (`credentialByDigest`, `authorizationState`, `gatewayRequestState`) takes the same lock. |
| Audit already group-commits; do not duplicate it | `fileAudit.drain` merges only already-waiting requests and never delays a lone one, and its head advances in batches bounded by `auditBatchMaximum`. Left as is. |

Additional facts that shaped the design and bound the win:

- A per-namespace fence (`lockOperationNamespace`, 64 stripes) wraps every operation commit
  (`begin…`, `settle…`, `settlePendingOperation`, retirement, release, transport). It is held
  across the barrier too, so **two commits for the same namespace never overlap** and cannot
  share a barrier. Coalescing therefore only covers appends that genuinely overlap: different
  namespaces, and any internal append that does not take the fence. Moving the barrier out of
  the fence changes fencing semantics and is explicitly out of scope this round.
- `compact` rewrites the log and truncates it to the witness, and its snapshot content is the
  view at the revision it materializes, so it needs the confirmed view.

## 3. Design

State machine, in the order a commit passes through it:

1. **Queue ownership.** `operationJournal` owns an ordered queue (`commits`) of
   `operationCommit`. Its chain position is fixed at submit time - `revision` and `previous`
   follow the last queued append - so the order the log records is the order submitters were
   serialized in and the digest still covers the bytes actually written. Two locks: a queue
   lock for bookkeeping only, and a write lock that only the committer (and a compaction)
   holds. A submitter therefore never waits for a barrier.
2. **Ordering and the confirmation barrier.** The caller holds the view lock while it applies
   its edit and submits, so queue order and view order are one order. The barrier runs with the
   view lock released; the caller then reacquires the view lock and *settles*: every edit whose
   delta is ordered but not durable is either confirmed or rolled back. `unconfirmed` is the
   FIFO that ties the two together, oldest first.
3. **In-flight read visibility.** Readers never observe an unconfirmed record: `Snapshot`,
   `readOperations` and the gateway's operation reads settle first and answer the same
   uncertainty the append had (or proceed normally when nothing is outstanding). Narrow reads
   that do not touch operations (`credentialByDigest`, `authorizationState`,
   `gatewayRequestState`) do not settle and are no longer blocked by a flush. A reader that
   needs operations can wait at most one outstanding batch, and that wait is counted.
4. **Failure fan-out.** A failed `WriteAt`, `Sync`, witness or path identity poisons the log
   and is reported to **every** waiter in the batch with the same error value; nothing from a
   failed batch is published. A failure rolls the affected edits back in reverse order and
   poisons the store, so a failed barrier can never leave a confirmed-looking record. A queue
   refusal (`ErrOperationQueueFull`, bounded at 256 entries, before any write) poisons nothing.
   Batch coalescing is bounded (32 entries or 1 MiB) and never delays a lone request.
5. **Registry barrier.** `Update` settles before it clones: the registry payload carries the
   complete operation view and becomes the base the next deltas bind to, so an edit that only
   lived in the view must not be recorded as though the log had acknowledged it.
6. **Close drain.** `Close` stops the queue first: appends that were never written fail with
   `ErrStateClosed`, the batch already in flight still reports its own result, and the file is
   released only after the committer exits, under the write lock.
7. **Recovery.** Unchanged and re-verified: the log is one ordered chain, a torn tail is
   truncated to the verified prefix, and the chain continues from that prefix. No durable
   format, file name or schema changed; the witness, snapshot envelope and mutation encoding
   are exactly the ones this base writes.

Remaining boundaries, deliberately not changed in this round:

- The namespace fence still holds across the barrier, so same-namespace coalescing stays
  bounded by it (cross-boundary change, deferred).
- `compact` still runs under the view lock (once per 256 appends or 8 MiB), as it did before;
  the barrier of an ordinary append is what moved out.
- A read that needs operations still waits for an outstanding batch while holding the view
  lock, so unrelated narrow reads share that one wait. This is bounded by one batch and is
  visible in `operation_journal_wait_ns_total`.
- The close-versus-in-flight window (a caller may still see success after `Close` began) is
  unchanged from the base; joining in-flight requests remains the service's job.

## 4. Implementation boundary

| File | Change |
| --- | --- |
| `prototypes/v1/platform/operation_commit.go` (new) | Queue, committer, batch writer, bounds, idle/compaction guards, wait counters |
| `prototypes/v1/platform/operation_journal.go` | Queue fields; `appendOperations` becomes the synchronous single-commit wrapper over `submit`+`await`; `compactConfirmed` quiescence guard; write lock; `close` drains |
| `prototypes/v1/platform/operation_store.go` | `operationPendingEdit`, `settleOperations`, `compactOperationsLocked`; `readOperations` settles; `updateOperations` rewritten (edit under the lock, barrier outside, settle after) |
| `prototypes/v1/platform/state.go` | `unconfirmed` field; `Snapshot` settles; `Update` settles before cloning; `Close` drains the queue before closing the registry |
| `prototypes/v1/platform/state_cost.go` | New counters: ordered, coalesced, batch/queue maxima and limits, queue depth, wait total/max |
| `prototypes/v1/platform/state_cost_test.go` | Commit cost expectation: two lock acquisitions per commit (edit, settle), barrier outside both |
| `prototypes/v1/platform/operation_commit_queue_test.go` (new) | The seven tests in section 5 |

Not touched: TLS, audit and its group commit, admission/quota, public URL and token handling,
serverless/diskless paths, fencing order, SQL retry rules, Unknown semantics, the namespace
fence, Rust, the E01 skill, SDK clients and any root document.

## 5. Unsettled-operation audit

Every path in this base that can hand an operation record to a reader or to a durable
materialization, and how it is covered now:

| Path | Reads / materializes operations | Covered by |
| --- | --- | --- |
| `Snapshot` | yes (whole registry) | settles first; fails closed if a barrier failed |
| `readOperations` (operation reads: open admission, quarantine check, management) | yes | settles first |
| `Update` (registry payload = the base the next deltas bind to) | yes | settles before cloning |
| `compactOperationsLocked` -> `compactConfirmed` | yes (snapshot content) | requires no unconfirmed edit, and the journal refuses unless it is quiescent |
| `Close` | no reader, but the final view | closes the queue, then settles, then releases the file |
| `credentialByDigest`, `authorizationState`, `gatewayRequestState` | no (narrow views without operations) | never wait on a barrier; never expose a pending record |
| `openOperationDisk` / recovery | yes | no runtime appends exist at that point |

Failure scope: only an append whose barrier failed is rolled back. An append whose barrier
returned is durable, and the view keeps it, so a poison pass can never erase acknowledged
history; the store stays fail-closed either way. A closed log is a refusal that proves the
append never happened, so it does not mark persistence uncertain.

The queue capacity and the batch capacity are both enforced and evidenced by
`TestOperationCommitQueueBoundedRefusal` (refusals at the limit, no poisoning, no edit left
behind) and `TestOperationCommitQueueKeepsBatchBounded` (a burst larger than one batch splits,
no batch exceeds the limit, every append is durable). The namespace fence is untouched and
`TestOperationNamespaceFenceStillSerializesCommits` shows two same-namespace appends never
share a barrier.

## 6. Correctness tests and results

Environment: Go 1.23.4 darwin/arm64, offline (`GOPROXY=off`), `GOMAXPROCS=2`, `-p 1`, no Rust
build and no full stack. The Rust-backed `*Native*` tests fail with "PERI_RUST_BINARY must be
an actual built executable"; that is the documented missing-binary fixture, not a code failure,
and every non-native test in those families passes.

```sh
GOMAXPROCS=2 GOPROXY=off GOTOOLCHAIN=local \
PERI_RUST_BINARY=/nonexistent/opjournal-unit-tests-no-rust \
go test -buildvcs=false -p 1 -mod=readonly -count=1 -timeout 100s \
  -run '^(TestOperationCommitQueue|TestOperationNarrowRead|TestOperationJournal|TestOperationEdit|TestOperationIndex|TestStateOperationCost|TestOperationStore|TestOperationView|TestOperationDurable|TestOperationRetention|TestOperationRelease|TestOperationInterleaving|TestOperationState|TestCommandDigest)' .
```

| Test | Property |
| --- | --- |
| `TestOperationCommitQueueCoalescesWaitingAppends` | Three appends waiting on one barrier pay exactly one `Sync`; `coalesced`, batch/queue maxima, wait time and chain head are all recorded |
| `TestOperationCommitQueueFailureFansOutToBatchWaiters` | A failed write gives every waiter in the batch the same error value, poisons the store, leaves no unconfirmed record and refuses later appends |
| `TestOperationCommitQueueBoundedRefusal` | A stalled barrier refuses with `ErrOperationQueueFull` instead of growing; the queue never exceeds its limit, no refusal poisons the store, and every refused edit is gone |
| `TestOperationNarrowReadIsNotBlockedByASlowBarrier` | A narrow read returns while a barrier is outstanding and carries no pending operation; an operations read waits for it and then reports the record |
| `TestOperationCommitQueueCloseDrainsOrderedAppends` | `Close` blocks on the in-flight batch, fails the queued append with `ErrStateClosed`, and only the appended batch is durable on reopen |
| `TestOperationCommitQueueRecoversTornBatchTail` | A torn tail recovers as the verified prefix, the file is truncated to it, and the chain continues at the next revision |
| `TestOperationCommitQueueKeepsConcurrentOrder` | 24 concurrent registrations plus settlements all succeed, the chain head counts every append, recovery equals the confirmed view, and registration sequences are exactly 1..24 |
| `TestOperationSnapshotWaitsForItsAppend` | A snapshot does not return while an append's barrier is outstanding, and then carries the record exactly once |
| `TestOperationRegistryCommitRefusesAnUnconfirmedAppend` | A registry commit waits for the barrier and, when it failed, fails closed instead of recording the append in the payload |
| `TestOperationSettleRollsBackOnlyFailedAppends` | A confirmed append stays in the view while a failed one is rolled back; the failure poisons the store |
| `TestOperationCommitQueueKeepsBatchBounded` | A burst larger than one batch splits into two barriers, no batch exceeds the limit, and every append is durable on reopen |
| `TestOperationNamespaceFenceStillSerializesCommits` | Two same-namespace appends are never ordered concurrently and never share a barrier (the fence is unchanged) |

Also passing: the existing operation journal, view, store, retention, release and interleaving
tests, `TestStateOperationCostIsObservable` with the updated expectation, and the
gateway/management/audit/admission/state families without the Rust-backed names.

Race detector on the queue, journal, cost and narrow-read tests: `ok` (the linker prints a
known macOS `LC_DYSYMTAB` warning; exit status is 0).

Test durations are correctness evidence, not performance results.

## 7. Measurement preparation (needs a slot from the parent)

`go test` timing must not be quoted as a benefit. The counters are what make the candidate
measurable without a benchmark harness:

- `operation_journal_ordered_total`, `operation_journal_appends_total`,
  `operation_journal_syncs_total` - barriers per commit is `syncs/appends` (1.0 before this
  change); `operation_journal_coalesced_total` is what the queue merged.
- `operation_journal_queued`, `..._queue_max`, `..._batch_max`, `..._queue_limit`,
  `..._batch_limit` - bounds actually reached.
- `operation_journal_wait_ns_total`, `..._wait_max_ns` - what callers and settling readers
  spent waiting for a barrier.
- `state_lock_waits_total`, `state_lock_wait_ns_total`, `..._wait_max_ns` - shared-lock
  contention, now excluding the barrier.
- `operation_commits_total`, `operation_index_rebuilds_total` - work per commit.

Baseline and candidate must be compared through the existing E01 stack with identical
connection policy, SQL/manifest, TLS, public URL and token handling, audit and durability
settings, and with `state_operation_cost` and journal counters captured before and after. The
Rust binary and the frontend build are unchanged by this work, so a candidate run reuses them
and only the Go platform binary is rebuilt. **No performance claim can be made until that run
happens; nothing here is measured.**

## 8. Risks

1. **Integration prerequisite for the main tree's checkpoint (read-only review).** The
   uncommitted `checkpointOperations` clones `store.state` and commits it with
   `OperationsCheckpoint = revision+1`, then freezes the store. It has no settle step, so an
   append that is ordered but unresolved can be materialized: `commitLocked` validates the
   candidate against a parent view that recovery rebuilds from the log itself
   (`disk.parentOperations` -> `journal.applyView`), and the payload becomes the base the next
   deltas bind to. A failed append would then be recorded as a final view, and an append that
   was still ordered at clone time would be missing from a payload that later becomes
   authoritative via `operationsCheckpointMaterialized`. Required shape of the change, in the
   rewritten code: keep `if store.checkpointed { return ErrStateFrozen }` immediately after
   `store.disk.available()`, then `if err := store.settleOperations(); err != nil { return err }`
   *before* `cloneState`, and only then build the checkpoint candidate. `commitLocked` itself
   needs no change, and the settle path must stay exempt from the freeze so an append ordered
   before the freeze can still resolve. This worktree cannot test that code: the checkpoint
   lives only in the parent tree's uncommitted work.
2. **Integration conflicts (high).** The parent tree has uncommitted edits in files this change
   also edits: `operation_store.go` (a `store.checkpointed` → `ErrStateFrozen` guard at the top
   of `updateOperations`), `state.go` (`checkpointed` field, `CheckpointOperations`,
   `commitLocked`, the same guard in `Update`), plus `operation_state.go`,
   `gateway_operations.go`, `state_disk.go`. The queue must be ported as a diff, never as a
   file copy, and the `checkpointed`/`ErrStateFrozen` guard must stay ahead of the edit in the
   rewritten `updateOperations` and ahead of the settle in `Update`.
3. **Same-namespace coalescing is bounded by the namespace fence.** A future measurement that
   shows no barrier reduction on a single-database workload is expected as long as the fence
   holds across the barrier; that change is deferred and is not a defect this round.
4. **Reading operations during a flush serializes on one barrier.** Bounded and counted, but a
   future step is to publish a confirmed view to readers without any wait (a second, confirmed
   operations view) if the measurement shows it matters.
5. **Loss of coalescing is invisible in correctness tests.** The queue merges whatever is
   waiting; under a device that never stalls, batches of one are correct but no faster. The
   counters, not the tests, are the instrument.
6. **Not run:** the full Go suite (native tests need a real Rust binary), the race suite for
   the whole package, SDK matrix, TLS end-to-end, and any performance measurement.
