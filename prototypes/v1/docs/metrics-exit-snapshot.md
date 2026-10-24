# Worker metrics exit snapshot

Opt-in diagnostic export for the stdio worker (`peri-loom-v1 worker STORE CACHE`). It lets a
benchmark runner collect the counters that already exist in `work_metrics` and the object-store
metrics without adding a request to the worker protocol.

## Contract

| Variable | Meaning |
| --- | --- |
| `PERI_WORK_METRICS=1` | Records the workload counters at their existing work points. Unchanged by this export. |
| `PERI_METRICS_SNAPSHOT_PATH=/abs/path/snapshot.json` | Where the process writes one JSON document when `worker` exits. Unset or empty is the off state. |

The two switches are independent. With only the path set, the process writes the document and
reports `"work_metrics_enabled": false`; the `phases` entries are zero and `counters` is empty
because nothing was measured. Diagnostic runs should set both.

## Write timing

The snapshot is written once, after the worker's stdio loop ends and before `run` returns. This
covers a clean EOF, every error exit from the loop, and a failure to construct the runtime. It
does not cover process startup failures before `runtime_cli::run` (argument parsing, backend
open), and it does not cover death by signal (`SIGKILL`, `SIGTERM`, `SIGINT`): those have no
graceful exit. A runner that needs the document must close the worker's stdin and wait for it
to exit.

## Atomic write and cost

The document is serialized in memory, written to `.<name>.<pid>.tmp` in the target directory,
and renamed over the target. A reader never sees a half-written file, and a failed write leaves
no temporary behind. The file is deliberately **not** fsynced: it is a diagnostic artifact and
must not put durable-write cost on the disk under measurement. Nothing is allocated or read on
the exit path while the variable is unset; while it is set, the whole cost is paid once, after
the workload has finished.

## Schema (`schema_version` = 1)

```json
{
  "schema_version": 1,
  "generated_at_unix_ms": 1761210000000,
  "process": {"pid": 12345, "version": "1.1.11"},
  "work_metrics_enabled": true,
  "phases": [
    {"name": "snapshot", "count": 3, "total_ns": 410000, "max_ns": 190000},
    {"name": "clone", "count": 3, "total_ns": 12000, "max_ns": 6000}
  ],
  "counters": [
    {"name": "object_store.get.total", "value": 41},
    {"name": "object_store.put.total", "value": 120},
    {"name": "work.metadata.root_publish_attempts", "value": 7},
    {"name": "work.metadata.publish_origin.append.calls", "value": 3},
    {"name": "work.metadata.publish_origin.pin_extend.calls", "value": 4},
    {"name": "work.metadata.publish_outside_transact.migration.calls", "value": 0},
    {"name": "work.group.calls", "value": 7},
    {"name": "work.group.writes.5_16", "value": 7},
    {"name": "work.wal_workload.append_calls", "value": 19}
  ],
  "group_phases": {
    "available": true,
    "calls": 7,
    "writes": {"1": 0, "2_4": 0, "5_16": 7, "17_plus": 0},
    "class": {"wal": 7, "receipt": 0, "authority": 0, "other": 0},
    "barrier_ns": 1200000,
    "body_fsync_ns": 900000,
    "dir_flush_ns": 50000,
    "total_ns": 3100000,
    "total_max_ns": 900000
  }
}
```

* `phases` always lists the eleven `PublishStage` steps in declaration order. `count` is the
  number of publication attempts that reached the stage, not the number that completed it;
  `total_ns` and `max_ns` are wall time spent in the stage. The names and meanings come from
  `PublishStage::ALL`; this export restates neither.
* `counters` names are the serialized paths of the existing snapshots, only prefixed:
  `work.*` for `WorkMetricsSnapshot` and `object_store.*` for the `Metrics` operation counters
  (`total`, `errors`, `bytes`, `latency_*`). No counter is synthesized here. `object_store.*`
  is populated because the worker wraps its store in `MetricsObjectStore` while both switches
  are on; with only `PERI_WORK_METRICS=1` the wrapper is not installed and the export omits
  the object-store counters entirely.
* `work.metadata.publish_origin.<origin>.calls` attributes every authority publication
  attempt to exactly one origin. The origins are `append`, `wal_pages`, `pin_extend`,
  `branch_open`, `pin_release`, `put`, `put_group`, `delete`, `list`, `gc`, `capability`,
  `lease`, `migration`, `pin_upgrade`, `recovery` and `other`. The counter is charged at the
  same point as `root_publish_attempts`, so the sixteen values sum to that counter exactly.
* `work.metadata.publish_outside_transact.<kind>.calls` covers publications whose durability
  group is assembled without `transact_inner`; the kinds are `migration` and `other`. A
  reconciliation is therefore `publications_total = root_publish_attempts +
  publish_outside_transact.migration.calls + publish_outside_transact.other.calls`.
* `work.group.*` and the top-level `group_phases` object report the same successful local
  `put_group` aggregate. `calls` counts groups, `writes` buckets them by body count
  (`1`, `2_4`, `5_16`, `17_plus`), and `class` splits the first-write namespace
  (`wal`, `receipt`, `authority`, `other`). The stage fields are process sums except
  `total_max_ns`, which is the longest single group. Group aggregation is armed by
  `PERI_WORK_METRICS=1`; `PERI_DIAG_GROUP_PHASE=1` remains the independent stderr line.
* `group_phases.available` is `true` while the counters are on and `false` otherwise. When it
  is `false` the object carries a `reason` and no aggregate fields, so a reader can tell
  "not measured" from "measured, no group ran".
* Within one `schema_version`, existing field meanings do not change. A new field may be added;
  a semantic change requires a new `schema_version`.

## Failure semantics

A write failure (missing/unwritable directory, no file name, serialization error) is logged on
stderr as `peri-loom-v1: metrics exit snapshot not written: ...` and is otherwise ignored. It
never changes a successful close into an error, never changes the protocol response, and never
panics. The runner should treat a missing document as a diagnostics failure, not as a workload
failure.
