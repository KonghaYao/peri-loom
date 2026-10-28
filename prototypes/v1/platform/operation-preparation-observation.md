# Operation preparation diagnostics

The existing `PERI_DURABILITY_OBSERVER=1` opt-in profile now records the
pre-enqueue part of an operation mutation. It does not change a durable encoding,
barrier, admission result, SQL dispatch, retry policy, or response boundary. The
profile remains off by default. Disabled calls allocate no observer/caller object
and take no additional timestamps or locks.

`journal_prepare` is emitted only for a mutation admitted to the journal queue.
Its `id` equals the member ID in `journal_batch` and `journal_member_wait`.
`outcome=enqueued` says nothing about the later barrier result; use the batch/wait
outcome for that. Refusals and no-op edits do not emit a complete preparation.
The cutoff is the existing member's queue-admission timestamp, immediately before
its slice append, not an exact kernel enqueue timestamp. Emitting the bounded
diagnostic events occurs after that timestamp, so its own overhead belongs to the
subsequent member queue interval, not the preparation interval.

The `preparation` object partitions the event's monotonic `total_ns` exactly:

| Field | Interval |
| --- | --- |
| `state_lock_ns` | All state-lock acquisitions, including existing lock-accounting bookkeeping |
| `compaction_gate_ns` | Waiting on a prior operation compaction's completion channel |
| `state_gate_other_ns` | Remaining mutation-gate bookkeeping |
| `entry_checks_ns` | Store availability, shutdown, format and argument checks |
| `edit_setup_ns` | Index initialization if needed and construction of the edit |
| `edit_ns` | Mutation callback, including operation admission and close's related-open edits |
| `retention_ns` | Known-operation retention settlement |
| `validate_ns` | Online edit validation |
| `delta_ns` | Construction of the changed-record delta |
| `submit_entry_ns` | No-op check, caller handoff and entry into journal submission |
| `queue_lock_ns` | Acquisition of the journal bookkeeping mutex |
| `order_ns` | Queue bounds, chain position and mutation setup |
| `encode_ns` | Audit/mutation encoding and hashing, including their validation |
| `enqueue_other_ns` | Remaining checks, allocation, member validation and timestamping before queue admission |

`journal_namespace_wait` is a separate interval ending before `journal_prepare`
starts. It includes namespace stripe selection and lock acquisition; it is not a
kernel-only blocked-time measurement. It uses the same member ID and is emitted
only when the current caller was verified against exactly one primary changed
record. It does not contain the namespace or operation key.

The Gateway supplies the current request ID and primary operation identity as
in-memory metadata. A matching request/operation must appear exactly once in the
delta, must have the expected Gateway phase, and must not carry an administrative
retirement/release/decision owner. Completion audit must agree on request ID.
`correlation=caller_verified` marks this check. A successful close can therefore
identify its current caller even when it also updates a prior open record.
Missing, conflicting or duplicate primary matches preserve the old conservative
fallback; administrative multi-record changes are not guessed to belong to one
historical request. IDs with credential-shaped prefixes or invalid labels remain
excluded. SQL, command payload, database/namespace, session and operation key are
never copied into these new diagnostic fields.

Do not add preparation durations to the containing request `intent`/`terminal`
phase, or sum request spans with their journal batches. The pre-existing
`journal_member_wait.timings.encode_ns` overlaps preparation encoding and is not
an extra disjoint cost. Repeated wait events may describe the same member because
several state readers can join its barrier: use member ID, not event count, for
operation counts. Missing events, queue drops or sink truncation remain missing
evidence. Wall timestamps are correlation aids; durations use monotonic clocks.

Coverage is deliberately partial: the gap between namespace acquisition and store
mutation entry, refusals/no-op edits, and post-barrier settlement are not partitioned
by these two new events. They remain part of their containing request span (or its
unaccounted time). This is not an end-to-end trace completeness claim.
