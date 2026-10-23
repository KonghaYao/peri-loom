# W0-A1 performance harness

The gate recomputes native thresholds from raw `operations.jsonl` evidence. It
does not trust summaries, campaign-declared denominators, injected profiles, or
self-declared mounts.

## Gate commands

```bash
python3 scripts/perf-reliability/gate.py --evidence-root <dir> --tier G1 --profile quick
python3 scripts/perf-reliability/gate.py --evidence-root <dir> --tier G2 --profile formal \
  --trusted-artifacts <outside-evidence-dir>/trusted-artifacts.json
```

`--trusted-artifacts` is the **operator trust root** for the candidate and
denominator binaries. The gate enforces that the manifest path lies outside the
evidence root, but it cannot prove the manifest itself is trustworthy: whoever
can write the evidence can also write an external file, and a wrong binary
paired with a matching wrong external manifest remains a residual trust
boundary. The manifest should come from CI or another operator-controlled
channel; the gate makes no signature claim.

## Exit codes

The gate follows `issue8/acceptance.json:status_rules.any_not_pass_exit_nonzero`:
**only an overall `PASS` exits 0**. `PHASE_PASS`, `INCOMPLETE`, `NOT_RUN` and
`FAIL` all exit 1 (missing evidence and contract errors also exit 1).
`PHASE_PASS` remains a distinct status for quick profiles, the attribution
waiver and foreign-environment review; it is never a formal `PASS`. G1 is a
phase gate: even when every G1 threshold passes, its highest possible status is
`PHASE_PASS` (nonzero exit, `"formal_pass": false`). Only a complete tier-G2
evaluation can produce an overall `PASS` and exit 0. The JSON summary therefore
carries `"formal_pass"`, `"phase_only"` and a `status_note`, and every waiver is
written to `waivers`.

## Contract file set

The required file set is fixed by the frozen `issue8/SHA256SUMS.json` and the
files actually copied into `issue8/`; `issue8_derived.json:required_files` may
only restate it. Pruning, extending or duplicating that list is a contract
error, and so is swapping the manifest. The three generator/validator copies
under `issue8/reference/` keep their own `reference-manifest.json` and are
verified separately; adding or removing files in the contract directory is
rejected.

## Attribution

Metrics snapshots are mandatory by default. The gate parses each snapshot and
requires the documented schema: `schema_version == 1`,
`work_metrics_enabled == true`, a plausible `generated_at_unix_ms`, all eleven
`PublishStage` phases with non-negative `count/total_ns/max_ns`, and a non-empty
`counters` list containing `work.wal_workload.append_calls`,
`work.metadata.root_publish_attempts`, `object_store.put.total` and
`object_store.wal_durability.total`. `group_phases.available == false` is the
documented state and is accepted; no aggregate is required. A snapshot whose
WAL append count is below the run's timed write ACKs is rejected. Missing,
unparseable or structurally invalid snapshots make attribution `NOT_RUN`, which
keeps the overall exit code nonzero.

Each snapshot is also bound to the run that claims it: `process.pid` must be one
of the worker pids recorded for that lane in `resources.json` (`processes[].lane`
maps usage to the lane), and `generated_at_unix_ms` must fall inside the run's
wall-clock window (`run_started_at_unix_ms` .. `run_completed_at_unix_ms`,
±5 s) and must not precede `timed_started_at_unix_ms`. A pid from another run, a
missing pid, or a timestamp from another run's window makes attribution
`NOT_RUN`. This is an internal-consistency binding, not provenance: the evidence
producer still writes every timestamp and pid.

## Environment contract

Each run must declare `host_environment` (`os`, `arch`, `kernel`, `machine`) and
`roots_validation`. The gate rejects tmpfs/ramfs/devtmpfs/overlay roots,
unverified persistence, store/cache paths under `/tmp`, `/var/tmp`, `/dev/shm`
or inside the evidence root, and (by default) evidence recorded on a different
os/arch. Offline review of foreign evidence requires
`--allow-foreign-environment`; the overall status is then capped at
`PHASE_PASS`, never `PASS`.

## Tier selection

The effective tier is the strictest of the CLI `--tier`, `campaign.json:tier`
and `campaign-identity.json:tier` (G2 over G1). A lower CLI tier never drops
runs the producer declared: a `G2` campaign reviewed with `--tier G1` is
evaluated at G2 and reported `NOT_RUN` with `tier_downgrade_rejected`; a
campaign/identity disagreement is reported `NOT_RUN` with
`tier_declaration_mismatch`. Integrity is audited for every loaded run, so runs
outside a lower tier's required set cannot hide broken raw evidence. A campaign
that declares G1 and never runs S2 is a legitimate phase check, but remains
`PHASE_PASS`; asking for G2 fails coverage because the G2 runs are absent.

## Failure markers and resource accounting

Any non-empty `failure` on a run (`environment.json` or `summary.json`), the
campaign or an arm makes the result `NOT_RUN`; a missing/null marker means no
failure. Resource evidence must account for every concurrent lane: at least
`concurrency` worker processes, each with a unique positive integer `pid`,
`group_joined == true`, `exit_code == 0`, empty `cleanup_failure` /
`group_proof_failure`, and empty-or-zero `group_live_remaining`. Every raw
sample must carry a non-negative integer `rss_bytes` with no `failure` and no
`missing_roots`; one failed sample fails the resources section even when
`sampler.failures` is empty. Density and coverage only count samples whose
`monotonic_ns` falls inside `[timed_started_at_ns, timed_completed_at_ns]`;
samples taken during setup or teardown may be present but do not substitute for
in-window measurements. The in-window samples must be strictly increasing on
the monotonic clock, number at least `ceil(timed_seconds / interval) - 1` (one
sample of scheduling tolerance, never zero), contain no adjacent gap larger
than two intervals, and place the first/last sample within one interval of the
timed start/end. The interval comes from `sampler.interval_seconds`, capped at
a conservative 1 Hz floor when missing or larger.

Each in-window sample must also carry a non-empty `pids` list and a non-empty
`roots` list. In the real sampler `roots` is the process-root pid list the
sampler was constructed with (the initial worker pids); store/cache filesystem
paths are validated through `roots_validation`. Every lane's worker pid must
appear in at least one in-window sample, at least one in-window sample must
observe all worker pids together, and each in-window sample's `roots` must
equal the lane worker pid set; otherwise the resources section is `NOT_RUN`.

Warmup and timed phases must agree in time, not only in file order. Every
warmup record's `completed_at` must not exceed the lane's first timed
`issued_at` (and not the declared `timed_started_at_ns`); warmup records may not
follow timed records for the same lane; and each lane's timed `issued_at`
values must be non-decreasing in file order.

## S1/S2 schedules

S1 writes every row. S2 follows the frozen workload: reads cover even rows,
writes cover odd rows, and the per-lane/per-phase read+write union covers all
64 rows with the contract's 32/32 split per round. The gate rejects parity
inversions, reordered records, and incomplete coverage.

## Read-only reference materials

`issue8/reference/` contains byte-identical copies of the kit's
`generate_fixture.py`, `validate_sql.py`, and `sql-validation.json`, plus an
independent `reference-manifest.json`. The harness never executes these files;
`validate_sql.py` and `generate_fixture.py` are reference material only.
