# Native busy-wait independent verification

Verifier: Fermat, 2026-10-07. **Current adapter safety/regression gates PASS; release artifact build-usable, not a performance or deployment PASS.** PhaseDrain's separately approved semantic change is assessed in `PHASE-DRAIN-VERIFICATION.md`. No real stress was run by this verifier.

## Exact scope and identity

| A-owned ROOT file | SHA256 |
| --- | --- |
| `prototypes/v1/src/session.rs` | `142e10dc701c3b1b9f4617ab4aa3a0073046df6e2d9c38cdf6770fb48b82fddd` |
| `prototypes/v1/src/session/drive.rs` | `68bb18a5fd669fc35c6649c0cce4d956dbbf188b8ae2cbc798da4a7f9a8bfd39` |
| `prototypes/v1/tests/concurrent_sessions.rs` | `f2e4b44a449235bcb2a232c2f65a4661608ecf95fd0fe6e8823ec31a363a3979` |
| `prototypes/v1/tests/busy_wait.rs` | `535339a3ccc4b5bd874795d55013d62aad0ff34769bfe5c13b3314bea86e708a` |
| `docs/v1/NATIVE-BUSY-WAIT.md` | `b65020b885c2227a1add67eace800a9aab6372400cbff99ab2f9e229359e23ae` |

All five match A's freeze; each owned code file is below 500 lines. Relative to the prior 314-file source freeze, existing runtime changes are only native `session.rs` and E's `stress/phase.go`; the new native driver/tests and drain files are additions. No verifier production/test edits occurred. Cargo files, core/ownership/encryption/S3 guard source and P0 remain unchanged. Independent baseline comparison matches P0's ten native files and all 4,115 tracked Turso files, including symlink identity.

The current 300-file gated-source before/after manifests both hash to `073db37e76bba86da5c7ea3d5035db50451d632182e15dce0e37dbf68aa3ccad`: `/tmp/peri-native-busy-f/gated-source-{before,after}.json`. C/James's actively developing nested faultprobe scope was explicitly excluded, 19 pre-gate files; it is **not accepted or counted by this gate**. User `CLAUDE.md` was not modified. This is frozen runtime content in a pre-commit dirty working tree, not a completely clean Git checkout.

## Driver safety review

`RemoteSession::new` sets a one-second native busy timeout exactly once on its newly connected session. `RemoteDatabase::session` creates an independent connection and constructs that session once. The busy setter's existing core implementation bumps prepare-context generation; static inspection finds the new setter only in the constructor, not `configure`, statement loops or sleep callbacks. No generation counter was instrumented at runtime: the init-once conclusion is source/call-path evidence, not a fabricated telemetry measurement.

The new driver repeatedly steps **the same Statement**. It does not invoke prepare/query-runner creation inside a wait, replay a statement/batch, retry a client SQL request, add a database-wide queue or edit the core fork. Existing native internal schema-reprepare behavior is not replaced or claimed nonexistent.

For native Sleep, the driver uses monotonic elapsed time to honor the requested duration without restarting it, requests sleep slices of at most 10 ms, and checks the original execution budget and local store predicate before/after every slice and before the next native step. OS scheduling and guard work can exceed a requested slice; this is not a hard-real-time guarantee. IO/Yield retain native IO advancement with budget/local checks on both sides. Row/Done/Busy/Interrupt remain distinct terminal states and native errors remain in the error source chain. The session's own connection mutex is retained; no new all-database lock was added.

Existing statement ingress, transaction validation, VM/row/result/deadline budgets, physical ownership/S3/WAL/root-CAS fencing and AEAD are unchanged. A shorter SQL budget still wins over the one-second busy handler. No error implies that prior committed statements in a batch were rolled back; explicit rollback applies only to the active uncommitted suffix.

## Current independent commands and counts

One unified Rust suite used ROOT `prototypes/v1`, Rust 1.98.1 and `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target`:

```text
cargo +1.98.1 fmt --all --check
cargo +1.98.1 clippy --locked --offline --all-targets --no-deps -- -D warnings
cargo +1.98.1 test --locked --offline --release --no-fail-fast
cargo +1.98.1 build --locked --offline --release --bin peri-loom-v1
```

All four exit 0. Full release package: **306 unique PASS, five ignored**, no failed target; a subprocess reruns one helper, excluded from unique counts. This includes six new busy-wait tests, two Memory/Local concurrent-session tests, all seven P6 budget tests, seven Rust SDK tests, and the existing complete P4/fencing suites. Logs/status/unique records: `/tmp/peri-native-busy-f/rust-{fmt,clippy,test,release-build,status}.log` and `rust-counts.json`.

The new tests establish holder commit and independent reader progress while one writer call waits; bounded timeout and no side effects for failed lock acquisition; a shorter 40 ms write-query budget; expired ingress; local predicate expiry/revocation during sleep with no subsequent physical IO; and an already committed batch prefix surviving a later constraint failure plus explicit rollback. Concurrent Memory/Local tests reconcile ledger/balance/integrity with actual ACKs, not a fixed presumed number of winners.

**The sleep expiry/revocation predicates are mocks, not an actual Authority grant revoked during a live busy wait.** Complete existing P4/ownership/fencing regressions and the real native integration suite remain in the gate, but they do not convert that new mock into a specific real-authority-during-busy proof. No live-load or owner-crash experiment is claimed.

Using the new release artifact, `PERI_RUST_BINARY=/tmp/peri-stress-native-busy-bin`, `PERI_P6_VERIFY_RACE_CLI=1`, and umask 022, full platform normal/race runs each pass **92 roots + 150 named subcases**, two skips; vet exits 0. All **seven real native P6 tests pass in each mode**, included rather than added to those counts. They exercise encrypted ACK/recovery, typed terminal SQL error with same-session explicit rollback, migration/failover, native ReadOnly/token scopes, bounded NDJSON/committed prefix, KEK/authenticated tamper, and reply-lost unknown retaining admission. Their signals apply only to isolated test-owned fixtures, not actual baseline services. Skips remain the unsupported-clock test and process-child helper. Logs: `/tmp/peri-native-busy-f/go-{normal,race}.{jsonl,stderr}`, `go-vet.log`, `go-status.log`.

UI, TS, Python, controller and unrelated faultprobe tests were **not rerun in this busy gate**. Prior accepted UI/SDK/Python/controller evidence may be inherited only through unchanged source identity and the separate earlier reports; it is not relabeled as a current execution. Go SDK and Rust SDK tests are included in the current platform/package runs.

## Real S3: failed wrapper retained, separate final invocation accepted

Main executed the already-built release minio test directly with environment-only credentials, explicit `PERI_S3_TEST=1`, and arguments `--ignored --test-threads=1`. F did not obtain credentials or execute network requests. Executable: `/tmp/peri-loom-p6-dev/prototypes/v1/target/release/deps/minio-458fbff260aff428`, independently hashed SHA256 **`2bcf1a4c2ab038c1bc26a646583bbc47687252feb8347ba605e453e98c9698b0`**; Cargo fingerprint JSON SHA256 `b0247f033b87a53a3ccaf18d8a92f9fdfb6974f695b0b05779b1addaa964ebae`.

- First invocation `/private/tmp/peri-stress-busy-s3-gate-VKd5qAEB/test.log` reports five tests PASS in 7.74 s, SHA256 `c9bf4f690cfc4526e4031e546d53f3b7f8370663a76f6ca690afe9118a0068de`. Main reports wrapper exit 1 from zsh's readonly `status` variable; **no child exit-code file exists**. This first whole invocation is not accepted as PASS merely from its test summary.
- Corrected fresh invocation `/private/tmp/peri-stress-busy-s3-final-rs5eM5XB/test.log` reports **five PASS, zero failures, three filtered**, 7.22 s, SHA256 `af93e524ce99cbe59c236d0188b846fbde777878eba775715ea482120ba0bc87`. Its recorded `exit-code` is **0**; main reports actual child/wrapper exit 0. Both directories are 0700, logs are 0600, and the final exit file is 0600.

Independent inspection verifies all five exact network test names, recorded counts/exit and current executable identity. Source requires explicit enablement rather than an inactive successful return, and creates PID/time/temp-salt unique buckets only after confirming HEAD absence; cleanup is scoped to those newly created buckets. Tests cover object conformance, fresh-cache native SQL recovery, P2 authority/PITR/GC, P3 multi-database recovery and P4 fencing/restored snapshots. These unchanged tests are regressions, not a new real-S3 busy-contention experiment.

The logs contain ordinary test results only, no credential-bearing fields. Exact argv, which enablement variable was set, env-only injection and live bucket identities are executor-reported, not captured by these minimal logs; F does not pretend to independently recover them. Cleanup completion and each physical HTTP retry are not audited by this aggregate record; existing bounded store retries remain configured. The second execution used separately generated test buckets, not a replay of historical failed business SQL. Derived readonly evidence is `/tmp/peri-native-busy-f/s3-verification.json`.

## Artifacts, retained failures and limits

New host **arm64 release** `/tmp/peri-stress-native-busy-bin`: SHA256 **`868962e6896fa166c1429aa149812955e9740e690118609ebff20d2f0ccc89b4`**. Preserved baseline native `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`, previous View native `b3e6d22dd0a16c967c56141f235122d79b7ef7bf25a8438916314d2f7fa6128a`, and old harness `c186fd905c34f9d68d2f899e162fdae2bc0539e1a290fffa0cf64341b0e54886` remain untouched. New drain harness identity is recorded in its separate report. All verifier builds/tests finished before the direct S3 invocation and measurement handoff.

A's original default-handler Busy reproduction and pre-fix failing wait-progress test remain in `/tmp/peri-concurrent-sessions-focused.log` and `/tmp/peri-native-busy-progress-red.log`; the successful gates do not erase them. Round 3's uncaptured underlying HTTP SQL-error cause is still unproved; an offline typed Busy reproduction does not retrospectively identify that cause. Historical unknown requests remain unknown, and baseline fixture termination's exit 1/no clean platform proof is not relabeled as a successful whole shutdown.

This candidate combines busy-adapter and previously accepted OwnershipView changes. No isolated optimization benefit, maximum capacity, starvation resolution, live paired result or deployment completeness follows from these safety gates. New actual arms require main's matching-content operator attestation, startup/provisioning and explicit runner authorization. No production source or historical gate report was modified by this verifier.
