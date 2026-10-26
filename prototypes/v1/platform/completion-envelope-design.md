# Completion envelope: one durable barrier for terminal result and audit

> Historical v1 proposal. The opt-in implementation and current format contract
> are documented in [completion-journal-v2.md](completion-journal-v2.md).

Date: 2026-10-20. Status: **design only; no production durable format changed**.
Base: `913a20b`, branch `fix/v1.1.10-audit-20261020`. Directed fault-matrix
tests and a local cost fixture are implemented; the frozen-stack attribution and the
S05 recovery-matrix extension are **NOT MEASURED/RUN yet**.

## 1. Current completion path and what each barrier means

A committing public request (`execute`, `query`, `statement`, `open`, `close`, cursor
operations) passes through these durable points:

| Step | Code | Durable write | Why the barrier exists |
| --- | --- | --- | --- |
| Intent | `gateway_http.go` -> `dispatch` / `open` -> `registerOperation` -> `updateOperationRecords` -> `stateStore.updateOperations` -> `operationJournal.submit` + await | one journal record + `file.Sync` | The frozen command must be durable before it can reach a worker. Without it, a crash would permit execution with no intent record and no way to classify the outcome. |
| Worker | `callSession` -> `SQLUpstream.Call` | not owned by this host | The worker/data plane owns its own durability. |
| Terminal | `serveHTTP` defer -> `settleOperation` / `settlePendingOperation` -> `updateOperations` | one journal record + `file.Sync` | A success reply is only sent after the terminal record is durable. This is what makes "operation query after Unknown" possible. |
| Audit | `recordAuditEvent` -> `fileAudit.Record`/`Reservation.Record` -> `recordBatch` | one audit record + `audit.file.Sync`; plus `audit.head.commitAuditBatch` every `auditBatchMaximum` (32) records and on clean close | The audit row is durable before the reply. A failing audit changes the reply (`audit_unavailable`) except for an already-committed `outcome_unknown`. |

A non-committing request (`describe`, `session_state`, read cursors) writes no intent;
its only operation record is the terminal one (`settlePendingOperation`), so it pays
two barriers (terminal + audit) instead of three.

Current coalescing already merges **waiters that overlap**:

- The operation journal commits a batch of at most 32 records / 1 MiB behind one
  `file.Sync`; each caller waits for its own commit result.
- The audit writer drains at most `auditBatchMaximum` records behind one
  `file.Sync`; each caller waits for its own result.

Neither queue delays a lone request to manufacture a batch. The per-namespace fence
(`lockOperationNamespace`, 64 stripes) is held across the barrier, so two commits for
the same namespace never overlap and can never share a barrier. Consequently:

- concurrency reduces cost only across different namespaces (and internals that do
  not take the fence);
- an isolated committing request always pays **three barriers** (intent, terminal,
  audit), and no amount of group commit removes one of them.

The only barrier that can be removed without weakening the two hard ordering rules
(intent before dispatch, terminal before success ACK) is the terminal/audit pair.
That is the scope of this design. The intent barrier stays.

## 2. Cost attribution method

### 2.1 Sources, not estimates

| Segment | Evidence |
| --- | --- |
| Intent / terminal / audit wall time | request phase observer (`request_phase.go`), phases `intent`, `worker`, `terminal`, `audit`, `respond`; or the direct fixture in `completion_envelope_cost_test.go` |
| Journal barrier count and time | `operationJournal.stats()`: `operation_journal_syncs_total`, `operation_journal_sync_ns_total`, plus `encode`, `verify`, `assemble`, `write`, `witness`, `wait` counters |
| Audit barrier count and time | `fileAudit.AuditStatus()`: `fsync_count`, `fsync_ns_total`, `fsync_ns_max`, queue age/depth |
| Worker RPC | request phase `worker` / `native`; **only the frozen stack can attribute this** |
| Object-store staging | worker/native metrics; **not measured by the local fixture** |

The local fixture calls the real `stateStore` and `fileAudit` with real fsync enabled.
It deliberately has an in-process upstream and no object store, so its `worker` and
staging shares do not exist. Any number for those must come from the frozen stack and
must not be inferred from this fixture.

### 2.2 Procedure

1. Build the Go binary and run the frozen stack with the phase observer enabled
   (`dev.py --sql-open-observer`), then run the same lone-write and mixed-namespace
   sets used by E01/C1. Read the request-phase stream for per-request p50/p95 of
   `intent`, `worker`, `terminal`, `audit`, `respond`, `unaccounted`.
2. Read `operationJournal.stats()` and `fileAudit.AuditStatus()` deltas around the run
   to get barriers per request and barrier time per request. Compare the sum of
   phase durations with `total_ns`; do not fold `unaccounted` into a neighbour.
3. Attribute:
   - `completion_barrier_share = (terminal_sync_ns + audit_sync_ns) / total_ns`
     for the same requests;
   - `worker_storage_share = (worker_ns + native_ns + object_staging_ns) / total_ns`;
   - a lone committing write pays 3 barriers; the envelope candidate pays 2.
4. Measure mixed writes with at least two namespaces so the fence does not serialize
   them; report p50 and p95 for lone and mixed separately. A change that helps only
   the merged case is not a win for the isolated C1 request.
5. Never disable fsync, weaken TLS/audit, or relax fencing to obtain a number.

### 2.3 Local fixture measurement (real, but not capacity evidence)

Machine: macOS 26.5.1, Apple M5 Pro arm64, Go 1.23.4, machine under high concurrent
load. Command (from `prototypes/v1/platform`):

```sh
go test -buildvcs=false -o /tmp/q04.test -run 'TestCompletionPathSegmentCost' -v -count=1 .
go test -buildvcs=false -o /tmp/q04bench.test -run '^$' -bench '^BenchmarkCompletion' -benchtime=20x -count=1 .
```

Two runs of the 20-sample sequential segment test (each sample: intent, then terminal,
then audit, all with real fsync):

| Run | intent p50 | terminal p50 | audit p50 | journal barriers | journal sync total | audit barriers | audit sync total |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A (lower load) | 4.20 ms | 4.19 ms | 4.08 ms | 40 (2.0/request) | 190.7 ms | 20 (1.0/request) | 96.2 ms |
| B (higher load) | 12.00 ms | 7.55 ms | 7.96 ms | 40 (2.0/request) | 437.4 ms | 20 (1.0/request) | 183.3 ms |

Journal stage counters, run B: encode 2.48 ms, verify 2.50 ms, assemble 0.06 ms,
write 42.31 ms, sync 437.43 ms, witness 8.95 ms, wait 0.013 ms (totals over 40
appends). Run A: encode 0.71 ms, verify 1.31 ms, assemble 0.09 ms, write 3.55 ms,
sync 190.72 ms, witness 1.20 ms, wait 0.004 ms.

Benchmarks (`-benchtime=20x`, one process):

| Benchmark | ns/op | barriers/op |
| --- | --- | --- |
| `BenchmarkCompletionIntent` | 14.10 ms | 1.0 journal |
| `BenchmarkCompletionIntentTerminalPair` | 16.69 ms | 2.0 journal |
| `BenchmarkCompletionTerminalOnly` | 9.22 ms | 1.0 journal |
| `BenchmarkCompletionAudit` | 10.85 ms | 1.0 audit |
| `BenchmarkCompletionMixed` | 20.26 ms | 2.0 journal + 1.0 audit |

The benchmarks ran as separate loops on the same loaded machine; they are not an
exact decomposition (the pair is cheaper than intent + terminal measured in different
loops because the load changed). Use the segment test for p50 attribution and the
benchmarks for barrier counts and order of magnitude.

Interpretation, bounded by the evidence:

- fsync dominates the measured completion path on this device; encoding, assembling
  and verification are one to two orders of magnitude smaller.
- A committing write pays 3 barriers, a non-committing write 2. The proposed envelope
  removes exactly one (terminal + audit share one barrier): a ceiling of **33% fewer
  barriers** for committing writes and **50%** for non-committing writes. It does not
  promise a fixed latency multiple: the gain is bounded by the fsync share.
- Worker RPC and object staging are **not measured here**; if the frozen stack shows
  they dominate, optimize those layers first and treat this design as no-go for now.
- Variance between runs A and B is load/device noise, not a product difference.

## 3. Completion envelope format draft (version 2)

One append-only log owns the operation view and the audit projection. A record
carries the operation delta and, when the request is auditable, the audit event. One
fsync of this log makes both durable. After migration the log carries both intent and
completion envelopes, so the operation view still has a single total order; the v1
journal becomes a read-only migration prefix and is never appended to in v2 mode.

Proposed location: `operations/completion/segment.<start>` plus a witness file. A new
subdirectory keeps v1 binaries fail-closed: `openOperationDisk` rejects unexpected
entries in `operations/`.

Record draft (JSON line, strict decoding, unknown fields rejected):

```json
{
  "payload": {
    "version": 2,
    "revision": 42,
    "previous": "<seal of envelope 41>",
    "operation": {
      "base_revision": 41,
      "sequence": 912,
      "evictions": 0,
      "set": { "<database-key>/<operation-id>": { "...": "OperationRecord" } },
      "remove": []
    },
    "audit": {
      "sequence": 77,
      "previous": "<hash of audit record 76>",
      "event": { "...": "AuditEvent" }
    }
  },
  "seal": "<sha256 of the exact payload JSON bytes>"
}
```

Rules:

- `revision` is the total envelope order. `previous` is the previous envelope seal;
  the genesis seal is 64 zeros.
- `seal` covers the exact bytes of `payload` produced by the writer's struct encoding,
  not a normalized/JCS form. This is the current journal/audit digest convention and
  avoids canonicalization ambiguity.
- `operation` and `audit` are independently optional, but at least one is present.
  Intent envelopes carry `operation` only. Completion envelopes carry `operation` plus
  `audit` for public requests; internal completions (no audit row) carry `operation`
  only.
- `operation` keeps the current `journalMutation` semantics: `base_revision` binds the
  delta to the confirmed view revision, `sequence`/`evictions` preserve retention
  counters, and `set`/`remove` are the same delta encoding.
- `audit.sequence` and `audit.previous` are the projected audit chain position. The
  audit record hash is derived with the **current** algorithm:
  `sha256(json({version:1, sequence, previous, event}))`. The materialized audit file
  therefore keeps its existing byte format, sequence and hash chain.
- One envelope is one record. Several envelopes may share one write+fsync through the
  existing bounded commit queue, but each envelope remains independently verifiable.
- Segment rotation uses an intent record that binds the last revision/seal and the
  next start, following the audit rotation protocol (`.rotate` intent, rename, fsync
  directory, clear intent). No segment is overwritten.
- A periodic witness binds the last verified revision/seal. Recovery accepts a lagging
  witness within a fixed bound but refuses a chain that cannot cover it, exactly as
  the current journal witness does.

### 3.1 The two views

Both views are pure functions of the verified envelope prefix:

1. **Operation view.** Start from the registry payload (which contains the operation
   snapshot at a bound envelope revision) and apply envelopes with
   `revision > bound`, validating `base_revision`, sequence and retention counters.
   An intent without a completion envelope stays in flight; recovery quarantines it
   exactly as today.
2. **Audit view.** For each envelope with `audit`, compute the current-format audit
   record from `audit.sequence`/`audit.previous`/`event`, append it to the materialized
   audit segments, and verify the existing chain. The audit sequence is a projection:
   envelopes without an audit event do not consume it. Envelopes are still ordered by
   `revision`, so the audit chain never reorders.

Invariant: for every envelope, its operation delta and its audit event become durable
together or not at all. There is no state in which the audit view reports a completion
whose operation view cannot reconcile, or vice versa.

### 3.2 Audit materialization policy

The envelope fsync is the durability point. `audit.jsonl` becomes a materialized view:

- The writer may append the audit record synchronously (a plain `write`, no fsync) or
  asynchronously; either way the record is already durable in the envelope.
- `audit.head` remains a witness for the materialized view and may lag; repair from
  the envelope is idempotent.
- On restart, the materialized view is verified to its last valid record. If it is
  behind, missing envelopes are replayed and fsynced before serving. A torn tail is
  truncated only to a record the envelope prefix still covers; a head that names an
  earlier position is repaired from the envelope. If the view or its head is ahead of
  the envelope, or diverges from it, the audit is refused.
- Rotation keeps the existing `.rotate` protocol for the materialized view.

This is the semantic change the go/no-go must accept: the durable audit record lives
in the envelope, and the audit file is rebuildable. If an external contract requires
the audit file itself to be fsynced before the reply, there are two independent files
and no single-barrier design can satisfy it; in that case this design is no-go.

### 3.3 Admission and reply semantics

- A completion slot is still reserved before dispatch (as `ReserveCompletion` does
  today). The reservation converts to an envelope queue entry without re-checking
  capacity, so traffic that arrives after the reservation cannot make an accepted
  SQL unrecordable.
- The reply is released only after the envelope fsync returns.
- Envelope fsync failure keeps the current meaning: a dispatched operation becomes
  `outcome_unknown` and is not retried; a pre-dispatch refusal keeps its original
  refusal code. Audit loss is never silently reported as success.
- Queue bounds and refusal codes (`operation_registration_unavailable`) are preserved;
  an envelope queue rejection before any write poisons nothing, as today.

## 4. Migration and version refusal

- The registry gains `OperationStorageVersion = 2`. The current validator rejects
  values above 1, so an old binary opening a migrated directory **fails closed before
  reading any operation log**. That is the required "old version rejects unsupported
  format" behavior; it is not a compatibility shim.
- Migration is explicit (operator flag), not automatic. Steps:
  1. Open and replay the v1 journal and audit; verify both chains. Capture journal
     `revision/digest` and audit `sequence/hash`.
  2. Write `operations/completion.migrate` intent and a genesis envelope in
     `operations/completion/` that binds both captured heads and format 2; fsync the
     segment and the directory.
  3. Commit the registry with `OperationStorageVersion = 2` (fsynced).
  4. Remove the intent. The v1 journal and audit segments stay as the read-only
     prefix/snapshot; new appends go to the envelope.
- Crash before step 3: the registry is still 1. A v1 binary fails closed on the
  unknown `completion` directory, so rollback requires restoring the pre-migration
  snapshot. A v2 binary completes the migration from the intent. This availability
  cost is deliberate: silently ignoring a half-written newer log is worse.
- Crash after step 3 before step 4: the v2 binary completes cleanup; the old binary
  already refuses at registry validation.
- A new binary reading a v1 directory keeps the v1 path. A v1 envelope/segment read
  by a v2 binary is verified against its declared version; an unknown version or
  unknown field is refused, never skipped.
- `operations/completion.migrate` and the v2 genesis envelope are the only new
  formats; no existing record is rewritten.

## 5. Crash consistency and recovery protocol

Recovery order, before any public request is served:

1. Open the registry. If version < 2, use the current v1 recovery unchanged.
2. If version = 2: open the envelope segments, verify revision/previous/seal, and
   truncate an unverifiable tail to the last verified envelope. A later segment after
   a gap, a rewritten seal, or a chain shorter than the witness is refused.
3. Apply the verified operation prefix to the confirmed view; validate `base_revision`,
   sequence and retention. An intent without a completion is quarantined (Unknown).
4. Open the materialized audit, verify its chain. If behind, replay the missing
   envelope audit records and fsync. A torn tail is truncated only to a record the
   envelope still covers; a head ahead of the envelope or a divergent record is
   refused.
5. Both views must reach the same envelope head; otherwise fail closed. Only then
   admit requests.

Crash table (current semantics, to be preserved):

| Crash point | Envelope on disk | Operation view after restart | Audit view after restart | Client-visible outcome |
| --- | --- | --- | --- | --- |
| before intent write | none | nothing executed; dispatch never started | no row | definite refusal |
| after intent write, before intent fsync | maybe (power loss) | if present: in-flight -> quarantined; if absent: no record | no row | no dispatch; no success |
| after intent fsync, before dispatch | intent | in-flight -> quarantined | no row | no success; Unknown not "not executed" |
| during worker | intent | in-flight -> quarantined, `may_have_committed` | no row | `outcome_unknown` |
| after completion write, before fsync | maybe (process kill keeps it; power loss may not) | if present: terminal completed; if absent: intent -> quarantined | row present iff envelope present | no ACK; operation query resolves completed or Unknown |
| after envelope fsync, before ACK | completion | terminal completed | row in envelope; materialized view repaired if needed | client may retry; same operation ID refused |
| after ACK, before audit materialization | completion | terminal completed | envelope has the row; view lags then repaired | success, audit durable in envelope |
| during audit materialization | completion | terminal completed | torn view tail truncated, replayed from envelope | success unless envelope fsync failed |
| during envelope rotation | rotation intent + segments | verified prefix; rotation completed or refused | same | success only if the envelope fsync returned |
| during migration | intent and/or genesis envelope | v1 path if registry is 1; v2 completion if registry is 2; otherwise refuse | verify/repair as above | fail closed, never a silent mixed read |

Equivalence argument:

- An ACK is emitted only after the envelope fsync returns, so ACK implies the
  envelope and both projections are durable.
- No ACK means the envelope may or may not be in the verified prefix. Absence never
  proves `not_executed`; the surviving intent is quarantined with
  `may_have_committed=true` and `retry_allowed=false`, preserving the current
  `MissingRecordProvesNotExecuted=false` / `MissingRecordAllowsReplay=false` contract.
- The audit view is a deterministic projection of the same prefix, so it can neither
  contain an unaudited completion nor omit an audited one after repair.
- The namespace fence, operation identity, digest, retention and snapshot bindings are
  unchanged; the envelope only changes where the terminal record and audit event are
  persisted together.

## 5.1 Dependencies and concurrent work

- **Q01/Q02** provide the request-phase observer used for intent/worker/terminal/audit
  attribution. The local fixture is the fallback when the observer profile is off; it
  cannot substitute for a frozen-stack run.
- **S06** is concurrently changing audit admission and the gateway response path in this
  worktree. This design depends on the contracts (`ReserveCompletion`, the audit error
  semantics, the reply-after-durable rule), not on the current line numbers; the
  implementation must rebase on S06 before editing `audit.go` or `gateway_http.go`.
- **S05** owns the recovery matrix. Section 7.1 is the requested extension; the kill
  points, assertions and old/new-binary cases must land there before any format switch.
- This round adds only this document and the two test files. It does not modify the
  production completion path, `audit.go`, `gateway_http.go`, or any durable format.

## 6. Directed tests delivered with this design

`completion_envelope_fault_test.go`:

- `TestCompletionCrashMatrixOperationJournal`: intent durable before dispatch;
  completion write never started; completion written but not fsynced, surviving and
  torn; completion fsynced before ACK with replay refusal.
- `TestCompletionCrashMatrixAudit`: audit write never started; written before fsync;
  fsynced before head materialization; after head materialization; torn tail refused;
  rewritten record refused. It also recomputes sequence/previous/hash for every
  recovered record.
- `TestCompletionEnvelopeModelCrashPoints`: the reference reducer for the draft
  format, with the crash points before write, after write before fsync (present and
  lost), after fsync before ACK, and after ACK. It asserts that the audit sequence is
  a projection and that a missing completion can never reduce to `not_executed`.

`completion_envelope_cost_test.go`: the segment measurement and the five benchmarks
quoted in section 2.3. The test asserts the barrier counts (`2 * samples` journal,
`samples` audit) so a run cannot report plausible timings for the wrong number of
fsyncs.

## 7. Go/no-go

Proceed to a guarded prototype (format behind a flag, v1 unchanged by default) only
if all hold:

1. Frozen-stack attribution shows the terminal+audit barrier share is material for
   lone writes (proposed threshold: >= 20% of p50, or >= 1 barrier removal with no
   p50/p95 regression). If worker/storage dominates, optimize those first.
2. The audit contract accepts "durable in the envelope, audit file rebuildable". If
   the audit file itself must be fsynced before the reply, no-go.
3. The S05 recovery matrix passes every crash point above, including migration,
   rotation, repair and old/new-binary refusal.
4. Envelope p50 is at least 20% lower than the current 3-barrier p50 for isolated
   committing writes, and mixed p50/p95 regress by no more than 5%, on the same frozen
   stack and device. No fixed multiple is promised.
5. Audit verification and operator tooling can consume the envelope or force
   materialization; a lagging view must never be read as complete.

No-go if any condition fails, if the two-view equivalence cannot be proven under
crash/tamper, or if the added encode/write cost offsets the removed barrier on the
target device.

### 7.1 S05 recovery-matrix extension points

Add these kill points to the existing recovery matrix (each with a directory copy as
the crash artifact):

- before/after intent envelope write and fsync;
- after intent fsync, before dispatch;
- during worker (response loss);
- before/after completion envelope write and fsync;
- after completion fsync, before ACK;
- after ACK, before/during audit materialization;
- during envelope rotation (`completion.rotate`);
- during migration steps 1-4;
- during snapshot/compaction and during audit-view repair;
- old binary on a v2 directory; new binary on a v1 directory; v2 binary with a
  removed/rewritten/torn envelope segment; audit view ahead of, behind, and divergent
  from the envelope.

Each point asserts: the ACK set matches the durable prefix; a success reply implies a
terminal operation and its audit event; an Unknown never reads as `not_executed`;
same-operation replay is refused; audit sequence/hash continuity and rotation survive;
the namespace fence and queue refusal semantics are unchanged.

## 8. Explicitly out of scope

- Changing the intent barrier or allowing dispatch before intent durability.
- Removing or weakening the audit requirement, hash chain, sequence or rotation.
- Relaxing TLS, fencing, or fsync to obtain a result.
- Moving the barrier outside the per-namespace fence (changes fencing semantics).
- Object-store per-batch thread/encoding micro-optimizations. These get a separate
  subtask only if a profile proves they matter; no fixed multiple is promised.
