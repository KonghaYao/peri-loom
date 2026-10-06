# v1 whole-project stress and performance plan

## Status and prerequisites

This is an execution plan, **not a benchmark result**. First complete P0–P6,
the independent per-stage gates and the joint verification. Then start the
actual complete project: gateway/management, controller, Rust workers, storage
authority and the configured S3-compatible backend. Do not substitute the P1
CLI, isolated engine timings or mocked services for full-project evidence.

The existing local RustFS may supply S3 storage. Use isolated generated test
buckets and namespaces, environment-only credentials, and unique run IDs. Never
change the user's existing container, bucket contents, port configuration,
security services or unrelated application workloads.

## Research questions and success criteria

1. Which architecture limits become visible before SQL-engine limits: shared
   root CAS, metadata growth, synchronous remote calls, worker contention,
   replay, ownership, reconciliation, gateway queues and tenant interference?
2. Which ordinary database operations expose throughput/latency cliffs or
   unbounded memory, disk-cache, WAL or object-storage growth?
3. Can a targeted change improve the measured bottleneck while preserving
   acknowledged-commit recovery, fencing, isolation and reference/GC safety?

Correctness failures have zero tolerance: lost acknowledged commits, duplicate
applied writes, stale-writer publication, tenant leakage and unsafe GC fail the
round. Performance success is an independently reproduced improvement against
the frozen baseline, not a guessed absolute throughput target. Record uncertainty
and limitations; do not claim production scalability from a local experiment.

## Phase A: launch and architecture-first stress

Smoke-test the real client-to-gateway-to-worker-to-durable-storage path first,
including authentication, lifecycle operations and metrics. Record exact service
commands, versions, topology, source commits, configuration excluding secrets,
readiness probes and stop/cleanup commands.

Exercise the following before tuning SQL:

- Many independently active DBs versus one hot DB; dynamic cold open, idle
  eviction and scale-to-zero, with repeated create/open/close cycles.
- Writer contention, lease expiry, controller loss, delayed requests and stale
  worker revival; zero stale authoritative mutations.
- Graceful migration and abrupt worker loss under acknowledged writes; cold
  recovery from a new empty cache without copying a full DB volume.
- Concurrent append/checkpoint, growing generations and WAL, snapshots/forks,
  retention changes and GC, including reader pins and delayed deletion.
- Slow/unavailable storage, ambiguous acknowledgements and retry storms; bounded
  admission/backpressure, explicit errors and no false commit success.
- Hot-tenant interference, request fan-out, gateway/controller saturation and
  resource accounting; compare unaffected tenants to their isolated baselines.

Capture failure oracles and the first saturation knee, not merely maximum QPS.
Preserve the exact failing run as a reproducible regression scenario.

## Phase B: common SQL workloads

Use deterministic seeds, documented schema/indexes and independently verified
expected rows/checksums. Separate cold and warm cache, read-only and read/write,
single-request/autocommit and batched transactions.

- Primary-key point reads and inserts; indexed range reads and pagination.
- Sequential and random updates/deletes, hot-key contention and mixed OLTP.
- Bulk loading with varied transaction size; compare equal durability semantics.
- Full scans, filtering, ORDER BY/LIMIT, aggregation, joins and index creation.
- Rollback, multi-statement transactions, constraint failures and large values.
- Checkpoint-heavy sustained writes, WAL backlog, reopen/replay and large
  segmented-database cold starts.
- Snapshot/branch/clone/PITR operations during SQL traffic and after migration.

Initial cardinalities are 1k, 10k and 100k rows, growing only within the resource
budget. Sweep concurrency 1, 2, 4 and 8 before considering higher values. Report
offered load separately from achieved throughput so overload cannot hide behind
closed-loop client pacing.

## Measurements and resource safety

Record attempted/completed/error operations, verified committed rows, offered
rate, throughput, latency distributions (p50/p95/p99/max), timeouts/retries,
queue/admission delay and end-to-end versus service-level timings. Include CPU,
RSS, worker/cache accounting, disk usage, WAL backlog/replay, object GET/PUT/CAS
counts/bytes, cache ratio, cold start, checkpoint, recovery and failover latency.
Unavailable metrics must be identified, not invented or silently set to zero.

Start with one measured scenario at a time, short smoke runs, small data and two
workers. Default smoke duration is 10 seconds; established measurement duration
is 30 seconds with separate warmup. Start with a 256 MiB generated-data budget
and conservative explicit limits on clients, workers, requests and storage bytes.
Record actual machine capacity before establishing CPU/RSS abort thresholds.
Any correctness failure, unexpected fixture escape, runaway resource growth or
loss of control/cleanup stops the run. Never share simultaneous competing load
experiments on this host; parallelism applies to implementation and analysis.

## Multi-agent rounds and evidence

The main agent supervises disjoint development scopes: architecture/fault
harness, SQL workload generator, metrics/raw-result collection and targeted
runtime fixes. Freeze executable interfaces before dispatch. Fix workers change
code directly; independent verification workers do not alter production code.

At least two experimental rounds are required:

1. **Baseline:** freeze commits/config, run architecture scenarios then SQL
   workloads, retain raw results and independently validate correctness and
   methodology. Diagnose bottlenecks with timing/profile evidence.
2. **Targeted correction:** implement narrow fixes with regression tests, run
   correctness gates, repeat identical workload/config and independently compare.
   Interleave baseline/fix repetitions where practical to limit host drift.

Additional rounds follow verified findings. Do not run simultaneous benchmarks
or select only favorable repetitions. Correctness gates precede every performance
comparison. Commit only after independent verification; do not push automatically.

Store reports here using `01_experiment_report.md`, `01_verification.md`,
`02_experiment_report.md`, `02_verification.md`, then `conclusion.md` and a
separate `adversarial_review.md`. Keep raw per-operation/results and commands in
run-ID-specific artifact directories, with credential redaction. A final
adversarial verifier challenges alternative explanations, coverage, durability
equivalence, load accounting, reproducibility and the limits of local conclusions.
