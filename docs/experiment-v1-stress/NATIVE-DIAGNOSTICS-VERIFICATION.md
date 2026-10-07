# Native terminal diagnostics: independent gate

Date: 2026-10-07. Verifier: F (Fermat). **Current native regression gate PASS within the stated scope; diagnostic cause for Round 5 remains NOT DETERMINED. No stress/performance or current real-S3 PASS is asserted.**

## Frozen source and immutable artifact

A's three-file freeze was independently matched before testing and remained unchanged afterward:

- `prototypes/v1/src/worker_rpc.rs`, 4 lines: `f7668b43f275cef5ded7c5395aa6cd7d1172219cda22e5e3f0329b017d37e336`.
- `prototypes/v1/src/worker_rpc/server.rs`, 444 lines: `0090a4896b3d358acd51a8b0603b9f5206328d7f9f5fc12b1bbfb08927b81548`.
- `prototypes/v1/src/worker_rpc/diagnostics.rs`, 359 lines including unit tests: `1e0d2b8909f45ef91a1d0d994772c89ad302cf94b4e25ca48e119b1aae64dc01`.

The gate manifest uses the same previously accepted 300-file scope plus the new diagnostics module: **301 files**. Comparing the old 300-file accepted manifest finds changes only in `worker_rpc.rs` and `worker_rpc/server.rs`; every other scoped source/test/Cargo/Go/SDK/harness file matches. Current before/after manifests are identical, SHA256 `2c8bcd10166e581ad2d244c84d3f9b31c1b55ab5f63b4270a8ca6a83f0268d32`, at `/tmp/peri-native-diagnostics-f/gated-source-{before,after}.json`. This is a bounded content manifest, not a claim that every repository document or separately owned nested faultprobe file was gated.

Accepted P0 10-file and Turso 4115 tracked-file manifests were independently matched; Turso remains at accepted `1a548d14a5be41e32b05bc43cb559cfb8b44dae4` with clean status. No fork/core edit, busy-timeout, prepare-generation, budget, lease/fence, SQL replay or protocol-field change appears in this delta. Test/helper sources were not altered by F.

New executable: `/tmp/peri-stress-native-diagnostics-bin`, **Mach-O arm64 release**, mode 0500, SHA256 **`63779a5c020787201ca5639430294060b870b1d162fc6cca9ac5f40e64c3a2f3`**. It was copied only after the successful release gate to a previously absent destination. Preserved baseline 3c67, View b3e6, BUSY 8689, shared Go 8692 and drain harness 9aa8 hashes remain unchanged. The new binary is not the R5 measured 868 artifact.

Build/gate source HEAD was `3dad8b668b5a60b5582a7639ecffebabcd0b314b`, with the intended uncommitted three-file diagnostic delta, new reports and user `CLAUDE.md` dirty. This is a content-bound precommit build, **not a clean-HEAD build**; F did not alter CLAUDE or commit. Main must establish the final matching-content operator gate after its commits and separately authorize any fresh diagnostic experiment.

## Actual commands and counts

One serial Cargo gate ran in ROOT `prototypes/v1`, using `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target` and toolchain `+1.98.1`:

| Command | Actual exit | Scope/result |
|---|---:|---|
| `cargo +1.98.1 fmt --all --check` | 0 | Current package formatting |
| `cargo +1.98.1 clippy --locked --offline --all-targets --no-deps -- -D warnings` | 0 | All package targets, strict warnings; dependency diagnostics not promoted by this package-only gate |
| `cargo +1.98.1 test --locked --offline --release --no-fail-fast` | 0 | **312 unique PASS, 5 ignored, 0 failed** |
| `cargo +1.98.1 build --locked --offline --release --bin peri-loom-v1` | 0 | Actual new release executable |

There are 313 raw passing test-name occurrences because the existing cross-process cache-lock helper repeats one name; it is not an additional unique case. The six diagnostic unit tests all actually pass in this full gate, not merely in A's focused run. The five credential-dependent S3 tests remain ignored. Existing dependency warnings were retained in raw logs; no fork cleanup was attempted. A's focused pre-fix RED evidence `/tmp/peri-worker-diagnostics-red.log` (exit 101) remains separate from the later successful gate.

Full ROOT Go platform gates ran serially with **`PERI_RUST_BINARY=/tmp/peri-stress-native-diagnostics-bin`**, never a stale debug fallback:

- `go test -count=1 -json ./...`: exit 0; **92 root PASS, 150 subcase PASS, 2 root SKIP**.
- `go test -race -count=1 -json ./...`: exit 0; **92 root PASS, 150 subcase PASS, 2 root SKIP**.
- `go vet ./...`: exit 0.

Both normal/race runs execute all seven actual-native P6 roots: encrypted ACK; KEK rewrap/restart/tamper; constraint rollback/session reuse; encrypted migration/owned-test failover; scoped tokens/native readonly; bounded NDJSON/committed prefix; actual reply-lost EOF/held permit. Their unchanged public flags/correlation/unknown behavior is regression evidence, not live-pressure cause identification. Test-owned local fixtures/process actions are ordinary authorized integration tests, not use of main's pressure fixture or a production signal.

Raw command status/logs and independently reconciled Rust/Go counts are under `/tmp/peri-native-diagnostics-f/`: `rust-{fmt,clippy,test,release-build,status}.log`, `rust-counts.json`, `go-{normal,race}.jsonl`, corresponding stderr, `go-vet.log`, `go-status.log`, `go-counts.json`, executable/architecture evidence and manifests. Darwin race-linker warnings remain visible; they did not fail the gate. All gate processes completed before this handoff.

## Diagnostic safety and limits

Only Execute/Query errors returned by existing session handling call the logger; SQL-byte ingress rejection, missing session and other early dispatch failures need not do so. Existing worker response formation/HTTP flags and original returned error are unchanged. No statement is retried, re-prepared or replayed by this logger. Returned errors still follow the original guarded execution and wire paths.

The five-field JSON record permits only fixed event, action, validated request ID, fixed cause class and completion field. Typed native errors take precedence over message text; exact policy constants use the deepest cause rather than misleading outer context. Unit cases include Busy/BusySnapshot, constraint payloads carrying misleading lease/secret text, IO/completion, fixed budget/local-fence constants and unknown/decryption classes. Request IDs use the existing 128-byte restricted label validation; invalid IDs emit nothing and consume no capacity. Record metadata is not authorization or idempotency proof.

Unit tests independently cover Query/Execute field allowlists and payload exclusion, concurrent hard cap, invalid labels and a BrokenPipe writer. The atomic cap is **64 record attempts per worker state**, not 64 delivered records, reset per request or a durable counter. Ignored stderr failures do not mutate/propagate the SQL error, but there is no delivery acknowledgement or dropped-record metric. Standard synchronous stderr locking/writing is not proven nonblocking or time-bounded by this test. Main's required owned regular logfile is the planned sink; arbitrary blocked pipe/device behavior is not an accepted diagnostic timing guarantee. Early failure or missing records cannot mean zero native errors.

The logger prints no SQL/raw native message/session/namespace/credential payload. The existing trusted internal worker error wire is unchanged and is **not newly claimed redacted**; original public gateway redaction remains. Future cause attribution requires the actual originating worker, unique new request/action, recorded class and bounded private logfile identity/hash. Unknown/absent/ambiguous/cap-or-writer-loss evidence stays NOT_DETERMINED; cause classification and runner success are separate. Six unit cases and integration regressions do not establish a root cause for missing historical bodies.

## Inherited evidence and next boundary

UI/TypeScript/Python/controller suites were **not rerun** in this gate. Nineteen SDK files match the earlier accepted scope, and the 16-file final UI/SDK/Python freeze matches SHA manifest `dc740977e129d48306ace900e79e1f726f63bc94ea2b6dec0fe58b0fbf13ec9f`. Go/Rust SDK cases included in the current full gates are current runs; other client/controller results remain the previously accepted evidence, not newly executed totals. Separately owned faultprobe work is excluded from F's native gate.

Real S3 is inherited from main's previously reviewed BUSY invocation, not run with this new artifact: corrected direct-executable gate `/private/tmp/peri-stress-busy-s3-final-rs5eM5XB/test.log`, SHA256 `af93e524ce99cbe59c236d0188b846fbde777878eba775715ea482120ba0bc87`, actual child/wrapper exit 0, five PASS. Its executable was the earlier BUSY test binary, not today's recompiled minio executable. The first wrapper's five test PASS but bookkeeping exit 1/no child-exit record remain disclosed in `BUSY-WAIT-VERIFICATION.md`; this report does not erase them or claim current actual-S3 coverage. S3/store source identity is unchanged.

`06_PLAN_REVIEW.md` separately accepts only the declared fresh single diagnostic mixed scope conditionally. R5 independent report remains frozen at SHA `e384a5c2e40ec92a7f763ec423f6351385aa652460b71e3d4ea78163685d4c73`, whole PARTIAL/mixed FAIL. No S3 credentials, real pressure load, fixture API, new benchmark or post-failure SQL was used by this gate. Main's new gate and explicit runner authorization, after all builders stop, remain required; no prospective performance or retroactive cause PASS is issued here.
