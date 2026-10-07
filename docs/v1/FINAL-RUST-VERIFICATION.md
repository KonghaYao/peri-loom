# Final Rust independent gate: HOLD

- Decision: **FAIL/HOLD for the current full Rust gate.** One real regression gate failure is preserved; no automatic rerun, source repair, or whole-project acceptance.
- Executor F, non-author; Root manifest `prototypes/v1/Cargo.toml`, Rust/Cargo +1.98.1, aarch64-apple-darwin; hot `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target`, locked/offline.
- Actual fmt --all --check exited 0; clippy --all-targets --no-deps -- -D warnings exited 0. These successes do not override the subsequent full-test failure.
- Actual `cargo +1.98.1 test --locked --offline --release --no-fail-fast --manifest-path prototypes/v1/Cargo.toml` exited 101, elapsed 95.072319792s, default parallel test execution.
- Across 37 separately launched harnesses: **392 unique PASS, 1 FAIL, 5 ignored =398 cases**. One nested selfspawn summary was excluded; repeated execution is not an additional unique case.
- Failed harness `tests/p4_runtime.rs`: 19 PASS/1 FAIL, `scenarios::concurrent_watchdog_cannot_retire_a_newly_acquired_generation`.
- Actual fixed-context error: `watchdog race: next grant` → `next grant: grant RPC` → `authority operation rejected`. The log does not expose a more precise authority rejection cause; none is inferred.
- This reproduces the previously unrootcaused watchdog test failure category. Earlier exact/parallel/full successes remain historical facts, not proof of a race repair; no claim that this occurrence shares a proven underlying cause.
- All other harnesses completed because --no-fail-fast was used. Raw release log SHA256 `58629882687ddd4585776a8624478ef6424164a82942f8650663a2ba0714646f`.
- The five ignored cases are actual S3 opt-in tests in tests/minio.rs: object-store conformance, fresh-cache SQL recovery, P2 refs/PITR/GC, P3 multi-DB recovery, and P4 fencing/restored snapshot. They were not run; no historical real-S3 PASS is substituted.
- PERI_S3_TEST and AWS credential variables were removed from the test environment. Only test-local fixtures were exercised; no authorized live fixture API, secret retrieval, or old Unknown DB operation.
- Dependency identity: all 1026 Rust/Cargo inputs matched the accepted maintenance baseline before execution and after every stage; before/after manifest SHA256 both `fa625159720fff07c13462fbe5c644d35991e978dba1e93c3f4bb7709f7ca0c3`.
- Measured gate start/end HEAD `775bc629a3e8cbc0148243bebe7cddb954c8a28b`; no committed HEAD change during the measured gate. Earlier launch announcement preceded this captured start; only the measured scope.json identity is authoritative.
- This is the Root package gate, not an independent full-feature Turso workspace/core matrix. Go/server/web/Linux/SDK results from other executors are not counted here.
- Native release-bin build and SDK example build were **NOT RUN** after test exit101; no new immutable artifact was issued and accepted Native64ec was not overwritten.
- Separately reported SDK helper Unknown/Close problems are static blockers, not an actual API failure from this run. No old unsafe example was handed off as latest validated SDK; helper correction requires a separate source delta gate/build.
- Fresh evidence `/tmp/peri-rust-final-independent/`: scope.json, commands/results.json, per-stage logs/exits, counts.json, before/after.json and head_after.json. Counts SHA256 `01d28ca90c3fcfc71030773bec413101b10fa45295f2e71e4f8c6df7a2a4317d`.
- Builders stopped and the measured source closure was RELEASED to main for separately authorized investigation/helper work. No production/tests were modified by F; future acceptance requires explicit freeze and gate authorization, not retry washing this failure.
