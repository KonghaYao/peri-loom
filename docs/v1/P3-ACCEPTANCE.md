# P3 Stateless Compute — Acceptance

## Scope and trust boundary

These tests exercise the standalone line-JSON worker and the public runtime with
the P2 authority-backed local provider. Worker storage is an independent object
directory; each worker's cache is disposable. All providers and metadata callers
are trusted local prototype participants. This is **not** proof of distributed
writer fencing, ownership, SDK authorization or network security; those belong
to P4/P5. No secrets or operating-system service changes are required.

## Reproduce

Run from the repository root with the P3 runtime/provider/worker CLI integrated:

```sh
cargo +1.98.1 test --manifest-path prototypes/v1/Cargo.toml --test stateless_acceptance -- --nocapture
```

The fixture invokes the Cargo-built executable as:

```text
peri-loom-v1 worker STORE CACHE
```

Each request is one JSON line with a unique request ID. Responses must echo that
ID and explicitly acknowledge success or report an error. SQL handles are worker
session IDs, not raw engine references. The fixture drains stdout and stderr
concurrently, bounds individual responses to 120 seconds and graceful EOF exit
to 20 seconds, and kills/reaps a failed worker. Stderr retains at most 64 KiB.

## Acceptance cases

| Case | Required evidence |
| --- | --- |
| Dynamic multi-DB and scale-to-zero | Startup registry and object namespace are empty; two DBs isolate SQL; failed/invalid opens do not leave loading entries; close/unload/idle eviction remove all registry instances. |
| Independent sessions and drain | BEGIN spans protocol calls without exposing uncommitted rows to another connection; rollback/drop releases the writer; busy unload fails; drain denies new admission, permits existing work and unloads on final close. |
| Resource admission | Default per-DB session capacity is actually enforced; rejected admission leaves counts unchanged; closing a session frees capacity. Segment RAM/disk payload accounting respects configured limits. |
| EOF cleanup | Closing worker stdin with a live transaction aborts it, exits successfully and releases authority pins/native handles; a new worker can reuse the same cache and write. |
| SIGKILL disposal | SQL COMMIT is acknowledged with a durable native recovery point; the parent verifies termination by signal 9, removes the entire worker-A cache and recovers all acknowledged rows in fresh worker B. |
| Large lazy migration and old PITR | 1,000 rows with 2,048-byte payloads are created in one transaction; checkpoint/compact bounds the current stream; A stops serving before B opens with an empty cache; retained snapshot/PITR still restore 1,000 rows after the current branch advances to 1,001. |

The SIGKILL case is Unix-only and checks the actual exit signal, not a simulated
exception or normal shutdown. The pre-kill WAL is deliberately not compacted:
fresh recovery must replay its acknowledged tail. The graceful large-DB migration
case has a different purpose and compacts before handoff.

## Lazy migration measurements

The current manifest must contain more than 16 immutable 128-KiB segments. A fresh
process may fetch header/schema segments during native startup; zero segment GETs
are **not** required. The process assertions allow at most four segment loads at
open and at most eight after a primary-key query, still fewer than the manifest's
total segment count. They run before any full-table count or integrity scan.
The current in-memory WAL image must be at most 64 KiB, with no pending WAL payload
after compaction. Old snapshot/PITR reads are intentionally allowed to replay
their larger historical WAL views and are not subject to the current-stream bound.

A supplementary public-runtime probe uses another empty cache and a counting
wrapper around the real local ObjectStore. Physical keys are classified from the
authority's actual immutable entries and current WAL index, not inferred from
SQL text or fabricated hooks. The probe requires:

- At most four actual segment-body GETs at cold open and eight after two PK reads.
- Zero GETs of archived WAL chunk bodies while opening the current branch.
- At most 64 KiB of current WAL chunk payload returned by the backend.
- A one-segment RAM cache and two-segment disk cache remaining within their caps.

Both paths print their observed counters under `--nocapture`. These counters
describe segment/WAL payload operations, **not** total network traffic, native
engine memory, process RSS, latency, throughput or a general performance benchmark.
Authority metadata reads are not mislabeled as segment-body GETs.

Cache-tree assertions permit only content-addressed segment cache files and local
coordination locks, not authoritative main DB/WAL mirrors. Backend checks require
`authority/root` and reject raw P1 `HEAD` / `wal/index` publication.

## Execution status

The focused `stateless_acceptance` target completed with **6 passed, 0 failed**.
The recorded debug-profile run took 15.01 seconds for all six cases; this is an
execution record, not a throughput/latency benchmark or a performance promise.
The actual `--nocapture` counters from its large-DB case were:

| Observed counter | Value |
| --- | ---: |
| Current manifest segments | 32 |
| Fresh process B segment loads at open | 1 |
| Process B segment loads after one PK query | 3 |
| Process B current WAL image bytes | 0 |
| Counting probe segment-body GETs at open | 1 |
| Counting probe total segment-body GETs after two PK reads | 4 |
| Counting probe returned segment payload bytes | 524,288 |
| Counting probe archived WAL chunk-body GETs | 0 |
| Counting probe returned current WAL chunk payload bytes | 233 |
| Counting probe resident segment-cache RAM bytes | 131,072 |
| Counting probe segment-cache disk bytes | 262,144 |

The SIGKILL case verified signal 9 before deleting the entire first worker's
cache. Both acknowledged tail rows were recovered in the second worker, and its
native integrity check returned `ok`. Graceful EOF with an unfinished transaction
left only previously committed data and released authority pins. After compaction,
the current branch contained 1,001 rows while both the restored retained PITR point
and the old snapshot clone contained 1,000 rows.

Only acceptance tests/fixtures and this document are owned by this work item.
Formatting and focused tests are checked separately from the main worker's final
integration gates and independent adversarial verification. These six passing
cases do **not** establish full-project/P0–P6 joint acceptance or P4 ownership.
