# Optional Go durability boundaries

This diagnostic profile adds no performance optimization and does not establish that issue #15 is fixed. It records a bounded subset of the Go journal and audit path while preserving the existing persistence, error, recovery and acknowledgement sequence. No public `StateStore` interface, durable record, JSON tag or RPC payload is changed.

## Enable and collect

Direct Go service startup accepts `PERI_DURABILITY_OBSERVER=1`. The default, empty value and `0` leave the observer unbound and create no diagnostic file. Recovery mode always leaves it disabled. The development launcher forwards `1` only when `--sql-open-observer` is also present and the environment value is exactly `1`.

The output is `ServiceConfig.StateDirectory/durability-observation.jsonl`, independently of the older SQL-open observer output. The service requires its existing private, owned state directory. It creates an exclusive, no-follow, mode-0600 file; it does not overwrite a previous capture. An existing file, unsafe directory, invalid setting or other setup failure logs `durability observer unavailable; diagnostics incomplete` and leaves SQL running without this observer. Preserve the old file and use a fresh diagnostic run directory for a new capture.

One asynchronous consumer serializes events. Producers only submit fixed-shape, bounded metadata to a 4096-event queue without waiting for its consumer. The file is capped at 64 MiB, with room reserved for a footer. There is no diagnostic `Sync`, durable sink requirement or observer callback in the journal/audit critical path. Enabling the observer adds allocations, local clocks and queue arbitration, so its overhead must be measured separately; formal performance comparisons keep it off.

## Records and interpretation

All records use schema `peri-go-durability-v1`, with a random process-local `stream`. Event IDs, member IDs and batch IDs are only unique within that stream. `event_sequence` orders accepted producer attempts; gaps, drops or a missing footer make the capture incomplete. Request IDs are existing bounded identifiers, not unique business samples: several intent, terminal and audit records may share one. Correlate by stream, member ID, batch ID and journal revision/audit sequence before aggregating.

| Kind | Meaning |
| --- | --- |
| `journal_member_wait` | One admitted mutation: admission-to-selection queue time, caller wait and the existing mutation encode interval; phase is `intent`, `terminal` or `completion` only where the current in-memory record proves it. |
| `journal_batch` | Once per physical batch: writer-lock wait, identity verification, assembly, write, sync and witness-write intervals. The member list links each waiter to the shared cost. |
| `audit_member_wait` | One admitted audit record's queue and caller-wait intervals, with batch membership. |
| `audit_batch` | Once per audit batch: writer-lock wait, encoding, optional rotation/pre-batch checkpoint, write and sync. `legacy_v1` is the original audit file; `completion_v2_delegated` delegates persistence to the existing completion journal, with no invented audit-file sync. |
| `audit_checkpoint` | The existing head/floor update, including its file/directory persistence work. Mode distinguishes `pre_batch`, `post_batch` and `shutdown`; an enclosing pre-batch operation has `batch_id`. |
| `journal_compaction` | A captured revision, snapshot encoding/persistence, journal sync and truncate. The event reports failure without fabricating later phases. Mode identifies legacy or completion format. |

`owner=request` is emitted only when an existing request ID safely identifies an ordinary Gateway intent/terminal or current audit/completion event. Multi-record changes are `multiple`; maintenance and ambiguous historical owners are `background`. These are association labels, not proof that a background operation has no request waiting for it. The record contains no SQL, payload, subject, database name, credential or error text. Known credential prefixes are also rejected in the request-ID field.

Each batch's cost occurs once; do not multiply it by its member count. Member waits, queue waits, batch spans and maintenance spans overlap. In particular `audit_batch.timings.checkpoint_ns` contains the nested `audit_checkpoint` interval and must not be added to it. Queue wait can overlap the writer-lock wait, and caller wait overlaps persistence. `previous_maintenance_id` links the preceding maintenance operation, not proof that a member was blocked for its whole duration. Use local intervals to test overlap, preserve unresolved time, and do not sum percentiles.

Durations come from the same process's monotonic clock. Wall timestamps locate intervals approximately; they must not be subtracted across processes to create exact latency or causal links.

## Deliberately incomplete coverage

Header and footer always contain `coverage_complete:false`. This profile begins after stores are opened and bound. It does not cover all startup/recovery work, admission/view-lock delays before a mutation exists, every compaction gate, all completion-v2 maintenance, network/SDK work or a complete server request. It cannot relabel write dispatch-to-headers as fsync. Revisions and explicit request IDs permit precise local associations; missing ownership remains missing.

`stream_complete` means the sealed capture interval had no observed queue loss, cap, writer failure or unfinished producer teardown. It is not end-to-end request coverage and is not a promise of fsynced diagnostic data. A complete stream still has `coverage_complete:false`. On shutdown-budget exhaustion, `producers_joined:false` forces `stream_complete:false` before any late event can be omitted from the final counts. Late events after sealing are dropped; the already-written footer is not rewritten.

Consumer failure never changes an SQL, persistence or shutdown result. The sink's shutdown wait is bounded to 100 ms. A write failure may leave no usable footer; a `Close` failure can occur after footer serialization, so also retain the service's incomplete-capture logs and validate the complete JSONL file. Do not treat an absent file, malformed/missing footer or explicit incomplete capture as zero latency.

## Validation

`go test -run '^TestDurabilityObservation'` covers actual journal/audit Sync barriers, journal failure fan-out, concurrent shared batches, audit checkpoint and Close ordering, journal/snapshot/audit/head/floor byte equality with the observer on and off, compaction failure phases, bounded queue/file loss, sink failure isolation, identifier filtering and unjoined-producer shutdown. The tests retain the existing durable writes rather than substituting a diagnostic callback for them.

Campaign-specific commands, retained failure logs and independent review are under `performance-council-round2/implementation/go/` in the delivery. The observer is evidence-gathering work; a new product candidate and formal ABBA remain separate gates.
