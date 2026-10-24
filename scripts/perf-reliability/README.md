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
`object_store.wal_durability.total`. `group_phases.available` may be either
boolean: `true` carries the local group aggregate documented in
`prototypes/v1/docs/metrics-exit-snapshot.md`, `false` is accepted without an
aggregate. A snapshot whose
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

## Issue 8 CI container

`ci/Dockerfile` builds a pinned `rust:1.98.1-bookworm` toolchain image with
Python 3.11, `git`, `procps`, `util-linux`, CA certificates and C/C++ build
tools. It contains only `/opt/perf/entrypoint.py`; the repository, `.git`,
Turso, cargo caches, evidence and credentials are never copied into the image.
The default user is UID/GID 10001 and the default command is `test`.

### Build and identity

```bash
docker buildx build --platform linux/amd64 --load \
  -f scripts/perf-reliability/ci/Dockerfile \
  -t peri-loom:issue8-ci scripts/perf-reliability/ci
docker image inspect peri-loom:issue8-ci \
  --format '{{.Id}} user={{.Config.User}} {{.Os}}/{{.Architecture}} {{.Size}}'
docker run --rm --entrypoint python3 peri-loom:issue8-ci \
  -c 'import os; print(os.getuid())'          # must print 10001
docker run --rm peri-loom:issue8-ci --help
```

Build natively for the architecture under test (`linux/amd64` or
`linux/arm64`). Do not use emulated binaries for performance evidence. The
base tag is pinned by the repository toolchain; a registry digest is recorded
by the consumer when one is available, never invented here.

### Deterministic tests

```bash
SOURCE=$PWD
RUN_ROOT=$(mktemp -d)
mkdir -p "$RUN_ROOT"/{cargo-cache,cargo-target,build,reports}
chmod -R 0777 "$RUN_ROOT"       # container UID 10001; ephemeral host dirs

docker run --rm --init --read-only \
  --cap-drop=ALL --security-opt no-new-privileges --pids-limit=512 \
  --cpus=2 --memory=8g --memory-swap=8g \
  --tmpfs /tmp:rw,nosuid,nodev,size=512m \
  --mount "type=bind,src=$SOURCE,dst=/workspace,readonly" \
  --mount "type=bind,src=$RUN_ROOT/cargo-cache,dst=/cargo-cache" \
  --mount "type=bind,src=$RUN_ROOT/cargo-target,dst=/cargo-target" \
  --mount "type=bind,src=$RUN_ROOT/build,dst=/build" \
  --mount "type=bind,src=$RUN_ROOT/reports,dst=/reports" \
  peri-loom:issue8-ci test
```

The command above is complete: `test` needs no writable rustup volume, no
`/private/tmp` mount and no other hidden mount. The only writable paths are the
four bind mounts and the `/tmp` tmpfs, all shown here.

`test` runs, in order: the Python harness unittest discovery, `cargo check
--locked`, `cargo test --locked --lib -- --test-threads=1`, and
`cargo test --locked --test local_provider_worker --test statement_worker`
against `prototypes/v1/Cargo.toml`. It stops at the first failure. The summary
at `/reports/test-summary.json` records the executed/passed/failed/skipped
counts and the harness snapshot hash. This is not the P0 or SDK gate and does
not run all integrations.

The image installs `rustfmt` and `clippy` for 1.98.1 and sets
`RUSTUP_TOOLCHAIN=1.98.1`; the entrypoint also pins it in every subprocess.
The root filesystem can therefore stay read-only without a writable rustup
volume, and rustup never follows the checkout's `rust-toolchain.toml`.
`--test-threads=1` runs lib tests serially: the suite's lease tests use a
20-second wall-clock window and still expired in 2 of 7 full runs with four
threads on an emulated, CPU-capped runner. Serial execution changes test
concurrency only; production lease and timeout checks are untouched. The cost
is a longer deterministic job, not a weaker authority check.

The Python suite runs from a hash-verified writable snapshot of
`scripts/perf-reliability` under `/build`, not from the read-only mount: one
contract test intentionally creates a temporary directory under the harness
ROOT, and another test executes fake worker scripts. `/workspace` remains
read-only; the snapshot copy is compared byte-for-byte before any test runs.
The snapshot links `prototypes` and `.git` back to the read-only checkout so
`runner.source_inventory` and the Git identity checks still see the real
sources; process-scoped `safe.directory` entries cover both paths.
`TMPDIR` points at `/build/tmp`, outside the snapshot root, so the harness
temporary-root guard still exercises its real rule.

### Quick and formal

The `quick` and `formal` modes require the campaign/environment inputs, the
operator trusted-artifacts manifest, the prebuilt binaries and (for formal) the
host attestation. The container invokes the existing `campaign_cli.py` and
`gate.py`; it does not implement SQL rules or alter the gate.

Mount contract:

| Path | Mode | Contents |
|---|---|---|
| `/workspace` | ro | full checkout including `.git` and the fixed Turso gitlink |
| `/cargo-cache`, `/cargo-target` | rw | test mode only; never part of evidence |
| `/build` | rw | temp files and logs |
| `/perf/store` | rw | authoritative local object backend |
| `/perf/cache` | rw | disposable worker cache |
| `/perf/evidence` | rw | must be empty at start |
| `/perf/metrics` | rw | must be empty at start; raw worker snapshots |
| `/reports` | rw | gate JSON and entrypoint summary |
| `/inputs` | ro | campaign, environment, host attestation |
| `/artifacts` | ro | candidate/denominator release binaries |
| `/trust` | ro | `trusted-artifacts.json` outside the evidence root |

The environment JSON must set `store_root`, `cache_root`, `evidence_root` and
`metrics_snapshot` to the container paths above and `tier` to `G1`/`G2`; a
non-empty `profile_overrides` object is rejected so CI cannot shorten the fixed
sampling. The campaign must match the quick (1 pair, `AB`, S1/C2) or formal
(3 pairs, `AB,BA,AB`, S1/C2+C1, S2/C2) shape, and both arm binaries must hash
to the operator trust manifest.

Use `--network=none` for quick/formal; build and dependency download happen
before the measurement window. The deterministic test run needs registry
access and therefore keeps the default bridge network. Never mount the Docker
socket, never add capabilities, and never pass host environment files.

Example quick invocation (inside the hardened `docker run` above):

```bash
"$IMAGE" quick \
  --campaign /inputs/quick-campaign.json \
  --environment /inputs/quick-environment.json \
  --trusted-artifacts /trust/trusted-artifacts.json \
  --artifacts /artifacts
```

`formal` additionally requires `--host-attestation /inputs/host-attestation.json`
and an exclusive lock on `/perf/store/.peri-perf.lock`; the entrypoint refuses
to start without a flock, without an ext4/xfs mount for every performance root,
without a native-Linux attestation, or when evidence/metrics are not empty.
The attestation is an operator statement, not a cryptographic proof:

```json
{
  "schema": 1,
  "native_linux": true,
  "arch": "amd64",
  "persistent_disk": true,
  "exclusive_window": true,
  "provisioner": "operator-controlled perf profile",
  "device": "nvme0n1"
}
```

`--dry-run` prints the planned argument vectors without validating mounts or
executing anything; it exists for unit tests and operator previews.

### Exit codes and status semantics

- `0`: the requested mode was accepted. For `test`, every step passed. For
  `quick`, the gate reported `PHASE_PASS` with all four sections `PASS`, no
  waivers, no evidence errors, a native/G1/quick identity and two complete
  runs. For `formal`, the gate reported `PASS`/exit 0 with `formal_pass: true`
  and eighteen complete runs.
- `1`: a test, campaign or gate result was rejected. Raw output is preserved.
- `2`: preflight/input validation failed (`NOT_RUN`); nothing was measured.

`quick` can only ever return `PHASE_PASS`, and the gate exits 1 for it. The
entrypoint accepts that exit code only together with `formal_pass: false`,
`phase_only: true`, four `PASS` sections and an empty waiver list. A plain
`PASS`/0 is accepted only by `formal`. Do not use `|| true`, do not translate a
gate failure into a formal performance pass, and do not add
`--allow-missing-attribution` or `--allow-foreign-environment` to either mode.

### Known blocker: frozen denominator metrics

The frozen denominator `v1.1.11 /
f3c75622924d84189fa74f3069c209761ceeec31` has no W1 exit-snapshot export, so
its required metrics snapshot cannot be produced without changing the
denominator. The gate therefore reports attribution as `NOT_RUN` (and the
overall status as `NOT_RUN`) for evidence containing that arm. Strict quick and
formal acceptance is blocked by that attribution gap, not by the container.
The entrypoint reports the blocker and returns nonzero; it does not fabricate
snapshots, relax the gate, or claim a performance PASS. Fixing this requires a
separately authorized denominator/observability decision.

### CI workflow

`.github/workflows/issue8-perf.yml` is independent of the release gates:

- `deterministic-container` runs on PRs, pushes to `main` and manual dispatch
  on a GitHub-hosted runner. It builds the image without pushing, runs `test`,
  and uploads only the sanitized reports. Fork PRs get no secrets and never run
  on the persistent self-hosted runners.
- `quick-native` and `formal-native` are manual-only and require the protected
  `peri-perf-native` / `peri-perf-formal` environments and native runners with
  persistent disks. They do not publish images. Formal uploads only reports,
  evidence and metrics; store, cache, build logs and binaries stay on the
  runner.
