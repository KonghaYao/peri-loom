# Whole-project gateway stress harness

Status: implementation and unit-validation only. No real SQL load, stress run,
microbenchmark or performance experiment has been executed by this sidecar.
The Rust/P5/P6 implementation and the user's existing RustFS remain untouched.

This is a standalone Go 1.23 standard-library module. Main can integrate its
source into `prototypes/v1/stress`. Do not copy `.gocache`, `.test-tmp`, compiled
binaries or validation logs as source. There are no external Go dependencies.

## Safety and execution gate

Actual execution requires a main-confirmed P0–P6 **joint PASS** evidence file
matching the Git HEAD read from the configured source root. Exploratory package
results, compiling this tool and its unit tests are not that gate. This evidence
is an operator attestation, not a cryptographic signature or independent proof
that every stage passed. Main must issue it only after joint verification.

Example shape, deliberately not enabling execution:

```json
{
  "version": 1,
  "joint_pass": false,
  "confirmed_by": "main",
  "source_git": "REPLACE_WITH_ACTUAL_VERIFIED_GIT_HEAD",
  "stages": ["P0", "P1", "P2", "P3", "P4", "P5", "P6"],
  "observed_at": "REPLACE_WITH_ACTUAL_RFC3339_CONFIRMATION_TIME"
}
```

The runner acquires `/tmp/peri-loom-stress.global.lock` and an exclusive
`.stress.lock` in the private output root. It never automatically removes
pre-existing or stale locks. An operator must first prove the previous runner
has stopped before clearing a stale lock. Output roots must have mode 0700;
new run directories use 0700 and JSON/JSONL files use 0600.

There are no signals to topology PIDs, shell fault commands, direct S3 calls,
bucket deletion, native engine CLI execution or SQL retries. A supplied
`fault_endpoint` is reserved metadata only: it is never called without a future
approved, fixture-scoped injection contract. Unknown outcomes stop subsequent
load; poisoned sessions are not blindly closed or reused. Closing a known
session is not claimed to atomically cancel native execution.

Main must provision dedicated fixture databases, using a database name starting
with `stress_`, before execution. The `fixture_only` assertion and name prefix
are safeguards, not proof of ownership. Never point this configuration at a
user database. Each run creates uniquely named tables and preserves them for
inspection. Provision a fresh isolated namespace for comparable repetitions;
accumulated schemas, WAL and cache state in reused databases are confounders.
Cleanup of retained fixtures belongs to main's approved management workflow.

## Parent fixture provisioning

`provision.py` prepares this module's private `ConnectionConfig` from a **new,
independent release-mode S3 launcher fixture**, never the user's existing databases.
Main must first confirm the joint gate and explicitly authorize `--execute`.
The provisioner does not certify the release binary or joint gate itself. No real
provisioning has been performed as part of its static/unit validation.

From the repository root, validate without network access:

```sh
python3 prototypes/v1/stress/provision.py --self-test
python3 prototypes/v1/stress/provision.py
python3 prototypes/v1/stress/provision.py --plan \
  --run-dir /private/tmp/peri-stress-release-NEW_PRIVATE_FIXTURE \
  --output /private/tmp/peri-stress-release-NEW_PRIVATE_FIXTURE/parent-baseline.json \
  --label baseline
```

No arguments display help; the default mode is also plan. Plan reads only the
fixture's local private files and validates its CA; it neither contacts services
nor reserves labels or writes output. Paths must be absolute/canonical, the run
directory must be an owned 0700 `peri-stress-...` directory (name length at least
24), and the output parent must be owned 0700. Output must not already exist.
Inputs are bounded, owned, non-symlink private JSON files; the regular CA file
must reside inside this fixture and not be writable by other users. Labels are
lowercase safe IDs of at most 24 characters, such as `baseline` or `fixed`.
Reads validate type, ownership, link count, size and permissions before opening,
use `O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC`, and repeat those checks with `fstat` plus
device/inode identity before any bounded read. FIFOs cannot block this preflight.
Offline self-tests live separately in `provision_tests.py`.

Only after main's explicit authorization, the execution command is:

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-release-NEW_PRIVATE_FIXTURE \
  --output /private/tmp/peri-stress-release-NEW_PRIVATE_FIXTURE/parent-baseline.json \
  --label baseline
```

Execution first checks the declared service PIDs with signal 0, exclusively
reserves `.stress-provision-<label>.json` and a 0600 output, then reads the global
admin `/v1/catalog` to reject existing `stress_<label>` or `noisy_<label>` projects.
The reservation records safe mutation request IDs before any mutation and is
never removed or reused. Incomplete output is deliberately not a valid parent
connection. On any failure or uncertain outcome, stop: retain these artifacts,
inspect them privately, and use a newly authorized label/fixture, never retry or
delete/recreate the reservation. Successful output contains credentials and must
remain private; stdout contains only plan identities or the output path.

Nine one-shot mutations create two projects under existing organization `local`,
two independent `service_account:true` admin keys, four primary-project databases
(`stress_<label>_main`, `stress_<label>_many_1` through `_many_3`) in tenant
`local_stress_<label>`, and `stress_noisy_<label>` in the separate `noisy_<label>`
project/tenant `local_noisy_<label>`. Exact existing management fields are used:
projects `id/organization/region`; keys `subject/project/role/service_account`;
databases `project/database/role`; every POST also has a unique `request_id`.
Quotas are omitted so the real defaults stay 8 databases, 16 sessions,
**4 concurrent requests**, 100 requests/second, 65536 SQL bytes and 4MiB results.
Concurrent admission limits are measured, not increased by provisioning.

The output preserves the verified CA, source root, global bootstrap management
key, actual worker/authority/platform PIDs and worker IDs. The migration target
is the other declared worker relative to the confirmed primary DB placement.
Additional DB identities and the independently authorized noisy target are included.
Networking uses new direct HTTPS connections to a literal loopback IP, this CA,
no proxy, no redirect, no automatic reconnect/retry, 30-second socket timeouts and
an absolute read deadline,
and at most 1MiB response bodies. Version/request ID/boolean success/error and
project/key/native-placement confirmations are validated before publishing output.

## Private connection file

The connection JSON must be a regular, non-symlink file with private permissions
(normally 0600), at most 64KiB. Credentials only belong in this file, never CLI
flags, environment dumps or command examples containing real keys.

```json
{
  "endpoint": "https://gateway.example.test",
  "project": "stress_project",
  "api_key": "REPLACE_WITH_PRIVATE_PROJECT_API_KEY",
  "database": {
    "namespace": {"tenant": "stress_tenant", "database": "stress_fixture"},
    "branch": "main"
  },
  "ca_file": "/private/fixture-ca.pem",
  "fixture_only": true,
  "source_root": "/path/to/frozen/source/git/root",
  "management_endpoint": "https://management.example.test",
  "management_key": "REPLACE_WITH_PRIVATE_GLOBAL_ADMIN_KEY",
  "databases": [],
  "processes": [],
  "worker_ids": ["worker_a", "worker_b"],
  "migration_worker": "worker_b"
}
```

Management credentials, endpoints, CA and process IDs are optional. The
management metrics route requires an actual global admin key (`project="*"`
server-side), not a database-scoped credential. Optional process entries look
like `{"role":"worker_a","pid":12345}` and use actual local host PIDs, not
container-internal IDs. At most two declared workers and eight sampled processes
are accepted. Additional many-DB fixtures belong to the same project. A
`noisy_tenant` object has its own `endpoint`, `project`, `api_key` and `database`;
its project and DB must differ from the primary fixture.

TLS verification is always enabled for HTTPS, using system roots plus the
optional CA. Plain HTTP is allowed only for a literal loopback IP. Redirects,
proxies, transparent reused-connection retries and request-body replay are
disabled. Every load goroutine owns one opaque gateway session for its phase.

## Actual public APIs

SQL uses POST `/v1/sql/open`, `/v1/sql/query`, `/v1/sql/execute`, and
`/v1/sql/close`, with the existing gateway request/response envelopes. No pager
or worker SQL RPC is introduced. Management uses POST
`/v1/databases/status`, `/suspend`, `/resume`, `/migrate`, `/checkpoint`, and
optional GET `/v1/metrics`. Each mutation is sent once. Bounded placement polls
are new read-only management observations, not SQL or migration retries.

Main reports the management native-tool path now admits checkpoint commands.
A present HTTP route alone is not proof of a successful native checkpoint;
the harness still requires the actual response to contain `checkpointed=true`.
Ambiguous management errors are unresolved, never converted into a PASS.

HTTP 400 `sql_error` is a known terminal SQL failure only when both
`execution_completed=true` and `may_have_committed=true` are present. The session
remains usable, but no implicit rollback or committed-prefix guarantee is
assumed. Missing/false flags, wrong HTTP status, EOF, cancellation and lost
replies remain uncertain; poisoned sessions stop without rollback, close or
SQL retry. Other unexpected known failures stop the workload without retry.

## Workloads and capabilities

`Scenarios()` and the network-free CLI `-list` report the capability catalog.
Architecture cases are ordered before SQL cases even if the caller lists SQL
first. Unsupported and unconfigured cases explicitly report SKIP with reasons.

| Case | Implemented behavior / prerequisite |
|---|---|
| `session_cycles` | Eight real gateway open/read/close cycles; no cold-cache claim |
| `many_db` | Supplied isolated DB IDs, bounded persistent sessions; reports actual active DB count |
| `noisy_tenant` | Independently authorized tenant, isolated baselines and combined phases; concurrency >=2 |
| `saturation_admission` | Open-loop offerings, client queue drops and actual gateway admission responses |
| `idle_suspend_resume` | Management suspend/active placement and durable-row checks; not scale-to-zero proof |
| `graceful_migrate` | Quiescent migration to a distinct explicit worker and post-migration ledger check |
| `point_read`, `index_range`, `pagination` | Seeded independent expected rows and hashes |
| `mixed_oltp` | 70% reads; single transactional write ledger/account updates otherwise |
| `hot_keys` | Four contended account keys |
| `bulk_tx` | Eight ledger/account updates per business transaction |
| `sort_aggregate_join` | Independent group counts and score sums over a dimension join |
| `index_plan` | Actual EXPLAIN result must name the generated index |
| `rollback` | Rolled-back unique probe must remain absent |
| `constraint_error_reuse` | One bounded correctness proof: explicit BEGIN/update/duplicate PK, typed terminal error, explicit same-session ROLLBACK, unchanged seed balance, ledger write/query reuse; not a throughput phase |
| `large_values` | Deterministic 16KiB text, safely within SQL/result limits |
| `sustained_wal` | Real acknowledged durable writes; no invented WAL backlog |
| `checkpoint_reopen` | Real management checkpoint and fresh gateway session, not empty-cache recovery |

Explicit SKIPs: proven cold-vs-warm native cache reset, abrupt migration/worker
faults, controller loss/lease-expiry injection, storage faults/GC reader pins,
and snapshot/branch/PITR-under-load schedules. Their safe hooks and independent
oracles are not implemented. Constraint-error reuse is supported, not skipped;
an unknown response never counts as an expected constraint success.
Unexpected native errors may stop write-contention sweeps;
this is evidence of the outcome contract, not a hidden retry or false success.

## Correctness, load and measurements

Initial datasets are only 1,000 or 10,000 rows, with concurrency 1/2/4/8. Default
phases are separate 5s warmup, 10s smoke and 30s measure, offering 50 business
operations/second globally. Shorter accepted durations are explicitly marked
`short_gate_not_benchmark`. A seeded account dataset, independent row hashes,
account sums and a primary-keyed `(business_op_id,ordinal)` ledger are checked
before load. Each smoke's durable ledger/rows are checked before measurement;
all acknowledged mutations are checked again at the end of each scenario.

The open-loop scheduler offers into one bounded queue per session. It separately
records planned/emitted offers, missed scheduling opportunities, client queue
drops/expiry, attempted/completed client workflows, acknowledged workflows,
errors, timeouts and unresolved outcomes. A request timeout is not a native
completion acknowledgement. Per-project offers/acks are retained for interference
analysis. No admission rejection means saturation was **not** observed.

Latencies use monotonic elapsed times, nearest-rank p50/p95/p99/max, separate
all/acknowledged workflow distributions and client queue timing. Client task
preparation is separate from queue and HTTP-workflow time. Observed wall times
are RFC3339 UTC labels, never inputs to elapsed-time calculations. Throughput
counts acknowledged business workflows including request drain, not assumed
native commits, scheduled capacity or retries.

Generated serialized request bytes are conservatively charged against 256MiB
per run. There are at most 50,000 offers per phase, eight clients, two declared
workers, 512KiB request envelopes and bounded response reads (default 8MiB).
Raw artifact bytes are capped at 64MiB. These are safety bounds, not hard native
RSS limits. Run one measured case at a time for initial rounds; a large `all`
selection can legitimately stop at a safety cap and must not be called complete.

CPU/RSS samples come from local `ps`, never native cache-accounting counters.
PS CPU percent has OS lifetime/window semantics; cumulative CPU seconds and
process-start identity are also recorded. Missing/reused sampled PIDs or an
explicit RSS threshold breach stop load. No arbitrary RSS/CPU abort threshold
is invented: actual host capacity is sampled first and an optional threshold
must be chosen by the operator. Optional management metrics preserve the whole
actual reported map, including `native.authority`, `native.workers` and explicit
`measurement_scopes`. A native-map presence marker does not claim every worker
or counter is available. Missing native/cache/service-queue/disk measurements are
unavailable/null, never fabricated zeros. No cache ratio is synthesized.

## Raw evidence and future invocation

Each actual run retains `run.json` and `operations.jsonl`: Git HEAD, dirty/diff
hashes, harness binary hash, redacted config, synthetic seed, observed wall
times, monotonic elapsed, individual request IDs, business operations, row/ledger
hashes, acknowledged mutations, resources and actual counters. Untracked source
content is not proven by a Git-status hash; main must retain a frozen source
manifest for performance comparisons. Credential fields and matching private
key strings are recursively redacted, including raw management snapshots.

Build and unit validation only:

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o peri-stress ./cmd/peri-stress
```

Future operator invocation **only after main's joint PASS confirmation**, not
an instruction to run now:

```sh
./peri-stress -connection /private/connection.json -joint-pass /private/joint-pass.json \
  -output /private/stress-results -cases architecture -rows 1000 -concurrency 1,2,4,8
```

Use targeted case runs, retained raw evidence and independent verification for
baseline and targeted-correction rounds. This tool does not create experimental
conclusions or an adversarial verification report from compilation/unit tests.
