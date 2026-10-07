# Independent conditional Native TEMP safety verification

Date: 2026-10-07. Verifier: F. Decision: **ACCEPT CONDITIONAL NATIVE API SUB-BATCH ONLY**.

This decision covers the frozen nine-path TEMP extension to the accepted child `3264185e511befc95c8750751e588b18709cd996`, with independent execution of 37 selected Native tests and the six external P0 tests. It does not accept the production provider, parent writer candidate, shutdown integration, whole SQL scenario coverage, S3, deployment, pressure, or performance. The caller's real exclusion and drain proof remains a prerequisite, not something established by the Native guard trait.

## Source identity and private workspace

- Authoritative manifest: `/tmp/peri-native-temp14-source.sha256`, SHA256 `638f1fb194b614ecabaf6b4fc9e3ec2b8f0a19fdf9eb2ca4e53283b486f02728`; all 12 entries independently match production and private sources.
- Nine patched paths: `core/connection.rs`, `core/connection_safety.rs`, `core/statement.rs`, `core/statement_safety.rs`, `core/vdbe/mod.rs`, `core/vdbe/execute.rs`, `core/vdbe/admission.rs`, plus new `core/statement_temp_tests.rs` and `core/statement_temp_io.rs`.
- The three additional unchanged entries are `core/statement_safety_tests.rs`, `core/statement_profile_tests.rs`, and `core/lib.rs`. New test files are 482 and 128 lines, respectively.
- Snapshot: `/private/tmp/peri-core-native-temp14.GStL9PTa/snapshot`; source manifest SHA256 `d7d857ad2e5c87253cbb836586bfa54e9ae4cf8f50f0db14f4e568f376a765a2`; inventory has 338 unique files.
- Every inventory entry was parsed and hashed independently against both its production origin and private copy. Only private root `Cargo.toml` and `Cargo.lock` are declared derivatives; the remaining 336 copies are exact.
- Effective members: `core`, `extensions/core`, `macros`, `sqlite/parser`; default member: `core`. Parsed root manifests differ only in members, default-members, and exclusions. All other parsed fields match.
- Private derivative lock SHA256 `8923b12683d91532da43578cfad34a2e557e445171b123e1d229cac939856fa2`; original lock SHA256 `55f187e3aea044cbd5571c61a03d3f15a99ece0688e0749f13af32a8687b28f4`.
- Independently parsed locks: 849 original packages, 438 retained, 411 removed, zero added; retained package metadata, including versions, sources and checksums, matches exactly. Both lock byte identities match the prior independently reviewed pair: 54 raw dependency-list differences, 42 representation-only differences, 12 normalized differences removing 23 edges and adding none. This is a limited feature/workspace derivative, not full-workspace or all-feature lock equivalence.
- P0 external test SHA256 remains `1af823f89d87a50cff48a773a498b903eb86b81651cd41c36a45a43ed0771403`. P0 Cargo manifest, lock, all six `src` files and this test are included in the independent input inventory.
- Production and private inventory, P0 inputs, and verification manifests were checked after each command. Before/after JSON files are byte-identical, SHA256 `0b20c8771203191677066a848fc249f9de6d3f4a2ce240f5a0eb5d63bea4b42a`.
- This is a bounded Native/P0 identity proof. Mutable V1 provider/drive/worker work is outside it; no whole-repository clean or unchanged assertion is made.

## Independently executed gates

All commands ran sequentially, offline and locked, in debug mode. No author results were substituted for these executions. No source, production lock or snapshot file was modified by F.

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`, host `aarch64-apple-darwin`; `cargo 1.98.1 (797e8a9bc 2026-08-05)`.

Environment: `CARGO_TARGET_DIR=/private/tmp/peri-core-native-temp14.GStL9PTa/target`, `CARGO_NET_OFFLINE=true`, `CARGO_INCREMENTAL=0`.

Native command, run once for each listed module from the private workspace:

```text
cargo +1.98.1 test --offline --locked -p turso_core --lib --no-default-features --features fs,uuid,json MODULE -- --nocapture
```

| Module | Pass | Fail | Ignored | Filtered | Actual exit |
| --- | ---: | ---: | ---: | ---: | ---: |
| `statement::temp_tests::` | 14 | 0 | 0 | 2489 | 0 |
| `statement::safety::tests::` | 12 | 0 | 0 | 2491 | 0 |
| `statement::safety::profile_tests::` | 9 | 0 | 0 | 2494 | 0 |
| `connection::safety::tests::` | 2 | 0 | 0 | 2501 | 0 |

The 37 observed passing names exactly match the frozen expected-name set. The remaining 2466 unique library tests were not selected; they are not PASS or SKIP results from this gate.

External P0 command, run from the root repository:

```text
cargo +1.98.1 test --offline --locked --manifest-path /Users/konghayao/code/ai/peri-loom/prototypes/p0/Cargo.toml --test statement_safety -- --nocapture
```

Default-parallel: 6 PASS, 0 FAIL, 0 ignored, exit0. A second execution with `--test-threads=1`: the same six names, 6 PASS, 0 FAIL, 0 ignored, exit0. **43 unique tests total; 49 passing executions are not 49 unique tests.**

Owned check-only formatting: all 12 manifest Rust files plus P0's external test, using `rustfmt +1.98.1 --check --edition 2021 --config skip_children=true`, exit0. Scoped Native and P0 `git diff --check` each exit0. Existing compiler warnings remain in raw logs; this was not a strict-clippy gate and does not claim warning-free compilation.

## Static safety and executable oracle scope

- BEGIN certification requires an exact six-instruction root program: fixed Init/Goto targets, MAIN Write, TEMP Write with schema cookie zero, non-rollback AutoCommit false, and successful empty-description Halt. Cursors and read/write database sets must be empty. This is bytecode certification, not SQL-prefix matching or a blanket TEMP allowance; child/helper programs do not inherit the TEMP exception.
- The admission callback is surrounded by profile validation. Explicit Memory selection is required before entry; the certificate binds the exact prepared-program Arc and execution generation. Transaction handling rechecks it before MAIN or TEMP transaction work.
- Auxiliary creation uses a fresh typed `Arc<MemoryIO>`. Private provenance records the specific database, pager, baseline schema and generation; validation checks both db/pager IO identity and database/pager object identity. An ordinary memory TEMP database or unchecked legacy BEGIN cannot be adopted solely because its selector says Memory.
- Setter calls, unchecked root execution, schema mutation and reprepare revoke execution authorization. Cleanup deliberately retains the physical certified-memory provenance after execution generation/schema authorization becomes invalid. Unknown outcome is not converted to a completed statement or successful Close.
- Retirement checks the guard against MAIN database/pager IO. TEMP checkpoint cleanup and rollback are included; the existing attached-pager enumeration explicitly includes TEMP. `verify_noncommitting_retired` requires closed state, no transaction, correct native activity counts, and absence of read/write locks and checkpoint state on BOTH MAIN and TEMP pagers.
- The 14 new tests cover Immediate/Exclusive commit and rollback, RETURNING with explicit-prefix preservation, pause/reprepare and selector mutation, Default/File/ordinary TEMP denial, user TEMP/schema/vtab and altered scaffold rejection, setter/legacy invalidation, legacy BEGIN non-adoption, attached/MVCC denial, object/schema identity changes, promotion with real native BusySnapshot, and four pending-read/late-commit success/failure cases.
- The four held-IO cases use actual submitted MAIN completion objects. Their test IO counts entry points, including file opens/reads/writes/sync, locks, stepping and completion operations; after exclusion they assert zero new counted MAIN calls. They verify both pagers, wrong-guard rejection, connection count zero/idempotence and Weak reclamation. A late failed commit remains `RetiredUnsettled`; local release is not a remote durability ACK.
- The test guard's getter and Arc matching are NOT a production exclusion proof. The caller must actually stop new MAIN IO, drain submitted work and hold database/connection exclusion through abort, retirement and final drop. These tests neither certify A's production provider nor establish a production actor's lifecycle/shutdown join.
- The old 12 safety and nine profile tests are byte-identical to child3264185. The old two connection-test module bodies are also byte-identical. Historical acceptance29 is separate; current acceptance43 is based on this new independent execution, not inherited green counts.

## Failure history and exclusions

The first historical default-parallel P0 run's 1 PASS / 5 FAIL, exit101 is preserved at `/tmp/peri-native-final23-independent/production-six.log`, SHA256 `dacd4e33010a7b30649c2c1c1d7b748188fbf564c47f44fba7b596cf049fe105`. The subsequently accepted fixture uses unique AtomicU64 filelike names without changing SQL/assertions; this gate reran both default and serial modes on that exact fixture. Earlier rejected-profile and root-reset REDs are not erased by current results.

Parent author busy_tuning failures, Worker fixture failure, TEMP metadata-expectation failure and their later author results are outside this gate. No parent regression, Go/RPC, SDK/UI, real S3, fixture API, load, benchmark, release build or signal operation was executed here. No old runtime binary was replaced. This report cannot identify historical stress Busy holders or claim FIFO fairness, starvation resolution or a performance improvement.

## Raw evidence and freeze

Fresh evidence directory: `/tmp/peri-native-temp14-independent`. `results.json` records exact commands, cwd, actual exits, elapsed times and raw log hashes; `counts.json` contains observed names and summaries. Runner session32927 completed exit0; all F builders stopped and the Cargo window was released to main before this report was written.

- Native14 log SHA256: `8a44599f46f98032bc7dfcfaea6ce30f845077cdb5083900443eb6291a7ef869`.
- P0 default-six log SHA256: `17e845122d2b389222f8228ec3db39f74d297e21eeb7dfbd56f6ce6dda9a2413`.
- P0 serial-six log SHA256: `ff9e891f090704f48ef8cd1fe7a0dd2c21a9a58c99eeb5f2f8ff0c7d8ff2bb43`.
- Results SHA256: `56c1eedb567d6a26173708ea717dff04b03538998a92b3bfb29b2eaa9e66a704`; counts SHA256: `a08d34970c6882a51942f90453664d1aa6fbb7c4e082c2139a97de269a77012a`.
- Core debug test executable SHA256: `8d9e3d4989e0d714edb96ac0ea637036b8598e49614aa345b086bfc5ab053c33`; P0 debug test executable SHA256: `bace1812bb789d7a7e69b8b8f7318e2e0957c869d71e6ef0ec3d2e8d119236a5`. Absolute paths are recorded in `test-executables.json`; these are test artifacts, not deployable runtime approvals.

The frozen Native sub-batch is eligible for main's limited child/gitlink packaging decision under the caller-proof contract. **Production provider, full parent writer and pressure approval remain HOLD.** F did not stage, commit or push.
