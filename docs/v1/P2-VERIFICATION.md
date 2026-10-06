# P2 independent verification

## Status: PASS — frozen P2 independent verification

Main explicitly announced that all P2 source was frozen. The independent
verifier then completed the full locked test, formatting and scoped strict
Clippy gates with exit 0, verified unchanged source hashes, and independently
read the completed live RustFS/S3 log. P2 passes within the contract and
limitations below. Main owns any commit; this verifier does not commit or push.
Only `prototypes/v1/tests/p2_verify.rs` and this report are owned by this
verifier. No production, Cargo, export, P1 or agent changes are made here.

Contract: `03-p2-storage-capabilities.md` and `P2-DESIGN.md`. P4 authorization,
distributed ownership/leases and stale physical-operation admission remain
outside P2; branch incarnation checks here are not a P4 fencing proof.

## Final frozen evidence

### Stable source

SHA-256 manifests cover **49 files**: every file under `prototypes/v1/src/`
and `prototypes/v1/tests/`, `prototypes/v1/Cargo.toml`,
`prototypes/v1/Cargo.lock`, `docs/v1/P2-DESIGN.md`, and
`docs/v1/03-p2-storage-capabilities.md`. Paths are sorted, each file is hashed,
and the resulting manifests are compared byte-for-byte before and after all
three gates. Verification-report edits are intentionally outside that manifest.

- Before: `/tmp/peri-p2-final-source-before.sha256`.
- After: `/tmp/peri-p2-final-source-after.sha256`.
- `cmp` exit **0**, with identical aggregate SHA-256:
  `36bd0c4dfa91c006c60131453b46f5edb71e4a783810bff2b14330af0ff1b76a`.
- Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`;
  `cargo 1.98.1 (797e8a9bc 2026-08-05)`.

### Gate results

| Gate | Exact command | Exit | Evidence |
| --- | --- | --- | --- |
| All tests | `cargo test --locked --manifest-path prototypes/v1/Cargo.toml` | **0** | `/tmp/peri-p2-final-cargo-test.log` |
| Formatting | `cargo fmt --manifest-path prototypes/v1/Cargo.toml --all -- --check` | **0** | `/tmp/peri-p2-final-fmt.log`; empty output |
| Scoped strict Clippy | `cargo clippy --locked --manifest-path prototypes/v1/Cargo.toml --all-targets --no-deps -- -D warnings` | **0** | `/tmp/peri-p2-final-clippy.log` |

Exit record: `/tmp/peri-p2-final-gate-exits.txt`. Clippy uses the explicitly
requested P1-compatible `--no-deps` scope. The Git-submodule Turso fork emits **9
existing dependency warnings**; they were not fixed, hidden, or represented as
an upstream warning-free gate. The `peri-loom-v1` all-target gate passes.

### Actual test counts

These are primary-harness counts from the final original Cargo log, not the
earlier focused runs or an estimated total. All primary suites have **0 failed,
0 measured and 0 filtered out**.

| Suite | Passed | Ignored |
| --- | ---: | ---: |
| Library unit tests | 5 | 0 |
| Binary unit tests | 0 | 0 |
| Authority | 13 | 0 |
| Capabilities | 14 | 0 |
| Diskless acceptance | 1 | 0 |
| GC | 6 | 0 |
| S3 test module, non-live tests | 3 | 3 |
| Object store | 20 | 0 |
| P1 independent verifier | 10 | 0 |
| P2 independent verifier | 14 | 0 |
| Recovery | 10 | 0 |
| Remote database | 12 | 0 |
| Remote storage | 13 | 0 |
| Documentation tests | 0 | 0 |
| **Primary-harness total** | **121** | **3** |

The remote-database cross-process cache-lock test also launches a nested
child harness: **1 passed, 0 failed, 0 ignored, 0 measured, 11 filtered out**.
That worker invocation is real evidence but is not added as an extra unique
test to the 121 primary-harness total. Process-helper cases remain Cargo test
counts, not separate claims of additional product features.

### Live RustFS/S3 evidence

The verifier independently read `/tmp/peri-p2-rustfs-resumed.log` and inspected
the corresponding live tests and fresh-bucket fixture. The log contains real
S3 assertion completion messages and **3 passed, 0 failed, 0 ignored, 0
measured, 3 filtered out**, completed in **5.29s**:

1. `minio_network_fresh_cache_sql_recovery`: cache deletion, remote WAL replay,
   checkpoint, second cache deletion, cold query and integrity check.
2. `minio_network_object_store_conformance`: signed network requests, 16-way
   conditional CAS, conditional deletion, 1005-key pagination, checksum and
   namespace isolation.
3. `p2_s3_authority_refs_pitr_and_gc_with_empty_worker_cache`: authority root
   publication, immutable snapshot/clone with COW writes, native commit
   recovery point, restore, branch/snapshot deletion, GC, authority restart
   and empty-cache SQL recovery/integrity check.

The `minio_*` names are historical names of generic S3 tests, **not evidence
that the backend is MinIO**. Main identifies the live endpoint as RustFS at
localhost:9000; the verifier independently confirmed the local published-port
container uses image `rustfs/rustfs:latest`. An unauthenticated GET to that
endpoint returned HTTP 403; that alone is not a product-identification or
durability proof. The live log proves completed network test execution; it
does not expose endpoint configuration or credentials.

The fixture requires explicit live opt-in and performs fresh-bucket preflight;
it does not return early and silently claim a live PASS. Main supplied
credentials through private PTY environment and owns this live execution. The
verifier did not request, print, or copy credentials. The **3 ignored** tests
in the ordinary Cargo run are exactly these live tests; their separate live
execution gives **124 passing distinct registered cases across the normal and
live runs**, not 124 passes in the ordinary Cargo invocation.

### Decision

**PASS for P2**: all required frozen gates are green, the 14 independent
adversarial cases actually ran, live RustFS/S3 execution is evidenced, and the
49-file manifest remained unchanged. No P2 blocker remains in these gates.
Any later source/Cargo/contract change invalidates the affected frozen evidence
and requires rerunning gates. P4 ownership/fencing and later P0–P6 joint-startup,
architecture, stress and performance work remain separate; none substitutes
for the P2 gate.

## Historical development evidence

The following sections preserve the earlier pre-freeze failures, cancellation
and recovery. Statements about pending work describe that earlier stage, not
the final status above.

## Previous blocker: idle-time PITR, now resolved in focused run

The earlier `Capabilities::resolve_time` implementation rejected a request
later than the latest retained commit (`requested time is outside retained
history`). The contract says to
return the latest retained committed point **at or before** the requested time;
only a request before retained history must fail. The independently written
initial test requested `u64::MAX` and failed on the current implementation.
That request alone is ambiguous if the intended policy rejects true future
requests. The oracle is therefore sharpened to
`pitr_time_selects_latest_commit_during_idle_time_after_last_commit`: request
one millisecond after the last commit, after independently waiting until the
wall clock has reached that time. This is a real idle-time request, not a
request for a future commit. Do not restrict ordinary idle time to accommodate
the old upper bound.

Independent recommendation: allow times through
`max(authority_now_ms, branch.last_commit_ms)` and return the latest retained
point at or before the target; reject true future requests if that policy is
made explicit in the design. The existing contract does not explicitly define
future rejection. Authority time can be logically monotonic under wall-clock
regression; it is not proof of exact UTC commit time. The verifier's idle-time
test uses a bounded wait and reports a clock/environment failure rather than
silently passing if that timestamp cannot be reached.

After host recovery, the verifier read the updated source: the upper bound is
now `max(now_ms, reference.last_commit_ms)`. The refined idle-time regression
actually executed and passed in the resumed 14-test run. This closes the
previous static blocker on the tested development revision; no current failure
was observed in the independently run 14-test suite or the 5-test GC suite.
Source is still explicitly **not frozen**, so this is not final acceptance.

## Executed evidence

These are provisional development runs, not freeze gates:

| Run | Command / log | Actual result |
| --- | --- | --- |
| Initial independent suite | `cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test p2_verify`; `/tmp/peri-loom-p2-verifier-initial.log` | **10 passed, 1 failed, 0 ignored, 0 filtered out**; failed oracle requested `u64::MAX` (true-future policy ambiguous) |
| Expanded independent suite | Same command; `/tmp/peri-loom-p2-verifier-expanded.log` | **12 passed, 1 failed, 0 ignored, 0 filtered out**; same original future-time oracle; original Cargo invocation completed without cancellation after launch delay |
| 14-test independent launch | Same command; `/tmp/peri-loom-p2-verifier-14.log` | Compiled, never entered harness; own child PID 33900 terminated with SIGTERM at main's request; Cargo **exit 101**; **no tests executed or counted** |
| Independent GC launch | `cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test gc`; `/tmp/peri-loom-p2-verifier-gc.log` | Compiled, never entered harness; own child PID 31301 terminated with SIGTERM at main's request; Cargo **exit 101**; **none of the 5 tests executed or counted** |
| Resumed independent suite after host recovery | `cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test p2_verify`; `/tmp/peri-loom-p2-verifier-resume-14.log` | **14 passed, 0 failed, 0 ignored, 0 measured, 0 filtered out**; Cargo **exit 0**; harness runtime **0.26s** |
| Resumed independent GC suite after host recovery | `cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test gc`; `/tmp/peri-loom-p2-verifier-resume-gc.log` | **5 passed, 0 failed, 0 ignored, 0 measured, 0 filtered out**; Cargo **exit 0**; harness runtime **0.10s** |
| Verifier-file formatting | `rustfmt --edition 2021 prototypes/v1/tests/p2_verify.rs` | Completed; not the all-target formatting gate |

The expanded binary was sampled separately: macOS reported only `_dyld_start`
before test harness startup (`/tmp/peri-loom-p2-verifier-sample.txt`). It later
entered the harness and finished 13 tests in 0.49s, without cancellation. This
corroborates main's reported process-launch delay, rather than an in-test
deadlock. The current file additionally includes the deterministic concurrent
HEAD publication test (14 total); only completed logged counts are evidence.

The refined idle-time oracle and concurrent HEAD test were not executed before
the OS-related cancellation. Both now actually executed and passed in the
resumed suite. All 14 tests still require rerunning after source freeze.

Only the two owned pre-harness test children were terminated. Original logs
are preserved, and the termination is neither PASS nor a test assertion
failure. Main reports `/tmp/peri-macos-policy-log.txt` with repeated syspolicyd
driver errors and `/tmp/peri-p2-gc-process-sample.txt` with `_dyld_start` only.
This verifier makes no OS/security daemon or policy changes. Main subsequently
reported host recovery without OS service/settings changes, and both resumed
invocations entered their harnesses and completed. The canceled invocations
remain historical evidence, not successful test runs. No unverified P2 commit
is approved.

### Exact suites requiring frozen rerun

All 14 tests in the current verifier file require frozen-source execution.
The resumed suite includes completed, passing execution of
`gc_mark_cas_conflict_recomputes_new_head_transitive_dependencies` and
`pitr_time_selects_latest_commit_during_idle_time_after_last_commit`.
The current file's test names are:

```text
metadata_fork_snapshot_clone_share_exact_physical_instances
native_history_ignores_reads_rollback_and_uncommitted_execute
restore_reopen_keeps_snapshot_identity_but_advances_commit_high_water
pitr_snapshot_wal_tail_survives_reset_retention_gc_and_restart
pitr_time_selects_latest_commit_during_idle_time_after_last_commit
lost_root_ack_is_one_publication_and_survives_restart
gc_recomputes_after_concurrent_new_read_pin_and_rejects_resurrection
gc_delays_exact_instance_deletion_and_never_touches_foreign_namespace
unresolved_upload_intent_cannot_be_released_tombstoned_or_deleted
corrupted_root_and_metadata_blob_fail_closed_before_deletion
deleted_recreated_branch_rejects_old_incarnation_and_root_counter_rewind
last_reader_handle_protects_lazy_segment_and_corruption_is_not_hidden
retained_history_pins_dependencies_but_manifest_parent_is_not_retention
gc_mark_cas_conflict_recomputes_new_head_transitive_dependencies
```

The earlier canceled GC invocation executed none of these 5 existing tests;
the resumed invocation executed and passed all 5. Frozen rerun remains needed:

```text
orphan_segments_are_tombstoned_and_physically_deleted
reader_pin_protects_lazy_reads_until_last_session_handle_drops
delayed_sweep_does_not_delete_new_instance_of_same_content
safety_window_survives_authority_restart
snapshot_and_retained_history_survive_ref_deletion_and_gc
```

Main reports authority compilation, capability/recovery focused results, and
pending GC/RustFS runs. Those are not independently relabelled as this
verifier's results. Main logs: `/tmp/peri-p2-gc-main.log` and
`/tmp/peri-p2-rustfs-live.log`.

Main retains ownership of GC/live S3 work and has not announced freeze while
authority/recovery/capability test updates finish. The verifier does not claim
those other suites green from compile success or earlier worker reports. The
later P0–P6/joint-startup architecture and database stress/performance work is
outside this P2 verification scope and does not replace or bypass the P2 gates.

## Independent adversarial coverage

`prototypes/v1/tests/p2_verify.rs` contains the following scenarios. Coverage
description is not a PASS assertion for a test that has not finished running.

| Scenario | Independent oracle |
| --- | --- |
| Metadata-only branch/snapshot/clone | Exact logical object and unique physical registry equality; segment-size GET/PUT counters unchanged; cold clone recovers WAL tail |
| Native commit authority | Read-only SQL, successful uncommitted `execute`, and rollback create no recovery points; one native transaction adds exactly one point |
| Restore/reopen | Old engine/session rejects access; incarnation/timeline advance; commit high-water counters do not rewind; current sequence/time match restored state; cold reopen sees old rows and next commit exceeds old high-water |
| Immutable snapshots | Duplicate name rejected; subsequent commit and restore do not mutate saved point; snapshot taken immediately after restore reflects restored sequence/time and HEAD/WAL |
| PITR across reset | Snapshot includes acknowledged WAL tail; truncate/reset and later commits do not alter old view; retention removes sequence selection but snapshot still permits cold clone after GC and authority restart |
| Time selection | Latest retained commit at/before valid idle time after last commit; before-history request rejected; same-time tie uses sequence/timeline |
| Lost root CAS acknowledgment | Root is physically published then PUT returns error; one revision/receipt/ref change only; reopening preserves identity and durable result |
| Concurrent GC/read pin | Deterministic root-CAS interleaving publishes a new durable pin; stale mark conflicts and recomputes; later tombstoned-reference resurrection rejected |
| Concurrent GC/new HEAD | Deterministic root-CAS interleaving publishes HEAD referencing previously unreachable manifest/segment; GC must recompute and preserve transitive dependencies or fail safely |
| Delayed deletion | Tombstones survive reopen and positive safety window; identical content has a different physical ID; sweeping old instance does not remove replacement; repeated sweep idempotent |
| Namespace isolation | Foreign-tenant sentinel survives GC; collector is namespace-bound |
| Unresolved upload | Failed physical PUT leaves durable intent; explicit intent removal/tombstoning/deletion rejected; restart/GC retain it without physical DELETE |
| Corrupt state | Corrupt root prevents open, publication and GC; corrupt HEAD blob prevents clone/reopen/GC; corrupted segment fails lazy read with no cached substitute |
| Reader lifetime | Cloned retained handle protects unread segment until last drop; GC then reclaims it |
| Retained history vs lineage | History alone pins old HEAD/WAL dependencies and cold PITR; after policy removal old instances are reclaimed despite current manifest lineage; current branch still cold-reopens correctly |
| Stale recreation/counters | Delete/recreate invalidates old session; root revision and incarnation mutation rejected without advancing root |

Fault injection wraps the strict in-memory ObjectStore. Root hooks run once
immediately before a conditional root PUT, with their mutex released before
reentry. Thus CAS conflict tests exercise an actual intervening durable root
publication rather than assuming a failed request represents a committed one.

## Historical final-gate plan (now completed above)

After main announces full freeze, capture stable-source evidence and run:

```sh
cargo test --locked --manifest-path prototypes/v1/Cargo.toml
cargo fmt --manifest-path prototypes/v1/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path prototypes/v1/Cargo.toml --all-targets --no-deps -- -D warnings
```

Record every suite's actual passed/failed/ignored/filtered counts, process exit
codes, log paths, and any changed source while gates run. A successful build,
`execute()` returning, or a launched test binary is not a committed-history
oracle and does not establish PASS. Re-run affected gates if source changes.

Real RustFS/S3 at localhost:9000 is available according to main. Main may run
the credential-safe opt-in live test; this verifier neither requests nor logs
secrets. Environment-disabled S3 tests returning early must be identified as
not live-executed, even if Cargo counts them as passing. Final PASS requires
finished frozen gates and explicit live-test evidence/limitations, not these
provisional runs.

## Final limitations

- Clippy is strict for this prototype's all-target `--no-deps` scope, not an
  upstream warning-free audit. The source-hash manifest covers the stated P2
  files and contracts, not the entire Git-submodule Turso fork dependency tree.
- In-memory deterministic root CAS tests do not independently establish remote
  S3 concurrency, durability or error semantics; the separate live RustFS run
  supplies concrete network evidence but not a proof for every S3 provider,
  server crash/power-loss case or distributed schedule.
- Segment-operation instrumentation identifies fixed-size P2 segment bodies;
  exact object registry comparison separately guards against new blob copies.
- Clock tie coverage uses real retained commit times if ties occur; no injected
  clock or claim of exhaustive scheduling/model checking is made.
- Unresolved intents and explicit-release read pins may conservatively leak;
  tests do not assume expiry permits unsafe deletion.
- Metadata validation/read corruption checks are not a malicious-owner or P4
  authority-admission proof.
