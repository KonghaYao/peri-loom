# Busy4 candidate: independent native regression verification

Date: 2026-10-07. Verifier: F (Fermat). **Offline/native integration regression gate PASS within the stated scope; four-second candidate remains unmeasured in real pressure.** This report does not revise historical failures, confer a performance PASS or establish that the candidate eliminates native contention.

## Source freeze and compiled identity

The only production delta from the accepted diagnostic artifact is `RemoteSession::new` setting the native busy timeout from one to four seconds. The setting remains once per newly constructed connection, before any statements. There is one Rust-side setter occurrence; `configure`, statement stepping and per-statement execution do not reset it. Immutable Turso's setter bumps prepare-context generation, which is why moving it into repeated execution would be unsafe; this candidate does not do that.

Independent six-file final author freeze:

| Path | Lines | SHA256 |
|---|---:|---|
| `prototypes/v1/src/session.rs` | 185 | `92900e843667d19135069dd34eda5147c91e4f4595e1e3a12223e8b4152e5bae` |
| `prototypes/v1/tests/busy_wait.rs` | 385 | `8162aabed033ac051e92187871ebd8ffad068fb85abacc510d329b831b9f54c9` |
| `prototypes/v1/tests/concurrent_sessions.rs` | 264 | `ea02960734a97327b89c8d402956bd3108ca01cc082c1b685712d6b613742caa` |
| `prototypes/v1/tests/busy_tuning.rs` | 341 | `f3fc7709d530ea59ce8b3b38e9605d22e8a7749687468d739900c16a17286244` |
| `docs/v1/NATIVE-BUSY-WAIT.md` | 57 | `4e5a85ccb4f1baaa7d1d269a2f06d9c25b929d862bcbeb5745ce226f48e981f4` |
| `docs/v1/USER-PATH-CONCURRENCY.md` | 212 | `14355413fb4ebb92d1b10b764e91158c01f2e079a98345f08b1091515747f8e7` |

The native/platform/harness input manifest extends the accepted301 files with only new `busy_tuning.rs`: **302 files**, before/after SHA256 **`1d0e2f70765c671963d6a904284deb68b40fe2c17b021a1442d2f68dd241f356`**. Relative to the old301, only `session.rs`, `busy_wait.rs` and `concurrent_sessions.rs` change. All diagnostic modules, root/ownership/IO guards, budgets, Cargo files, SDK and Go/harness inputs in that scope remain byte-identical. This is a bounded manifest, not a claim that every separately owned nested faultprobe file/document was compiled.

During this gate, main authorized one documentation-only correction: the stale proposed output path in `NATIVE-BUSY-WAIT.md` was changed to explicitly preserve historical busy-bin and use only new busy4-bin. Its initial SHA `50d3fdca…` is retained in `owned-before.json`; the final SHA above is in `owned-final-author-freeze.json` and `owned-after.json`, aggregate **`0ca9f53801452cbed9e9f14ed59a674c9da70c7e676c3f0da892aef8094d7ce2`**. Compiled inputs did not change, so no duplicate Cargo run was required. F did not edit author source/tests/docs.

Build source HEAD before was `2e462776c9248ab4281c34bb16639fd061bbc5a5`; after main's historical-report/plan docs commit it was `a9815e84ea23903840d0c7e1639272940ee4ac15`. The intended Busy4 delta was uncommitted and user `CLAUDE.md` remained dirty. This is a source-content-bound precommit artifact, **not a clean-HEAD build**. Later matching-content operator attestation is main's responsibility; documentation commits do not turn old binaries into builds from those HEADs.

P0's10 files and Turso's4115 tracked files match their previously accepted manifests; no fork/core change was made. Preserved binaries 3c67 baseline, b3 View,868 BUSY,637 diagnostic and shared8692 Go/9aa drain were independently rehashed and remain unchanged.

New native: **`/tmp/peri-stress-native-busy4-bin`**, Mach-O arm64 release, mode **0500**, SHA256 **`237625635a15b5d2ca0044ed2538cf1d67968f40cf73541c1170acd4310196cb`**. It was installed only to a previously absent path after the successful Cargo gate; no preserved executable was overwritten.

The actual release Cargo bin fingerprint files under `target/release/.fingerprint/peri-loom-v1-f186625b6a904ff0/` have SHA256 `e37d52d0c30984282b3c41d6e852627dedcd53b692ac2745b451095016899fee` (`bin-peri-loom-v1`) and `cc0716c2f9ea25a17ce712771942987daeab44b5749a0e4548a86788b1a1cea4` (its JSON). Snapshots **after Cargo/before Go and after Go** are identical, aggregate **`3b74e36896405b8de5dda75865ec97ee2dc690aaa903e867e835c2e9a436f3c5`**. These are Cargo metadata fingerprints, not replacements for the executable/source-content hashes or a fabricated pre-build fingerprint comparison.

## Actual gates and counts

F alone ran one complete serial Rust gate in ROOT `prototypes/v1`, with toolchain `+1.98.1` and `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target`. Recorded rustc is1.98.1, host `aarch64-apple-darwin`, compiler commit `48a229ceaefd4985c50990b14116b6d856af0985`.

| Actual command | Exit | Result |
|---|---:|---|
| `cargo +1.98.1 fmt --all --check` | 0 | Current package formatting |
| `cargo +1.98.1 clippy --locked --offline --all-targets --no-deps -- -D warnings` | 0 | Strict package all-target lint; pre-existing dependency warnings remain visible |
| `cargo +1.98.1 test --locked --offline --release --no-fail-fast` | 0 | **317 unique PASS,5 ignored,0 failed** |
| `cargo +1.98.1 build --locked --offline --release --bin peri-loom-v1` | 0 | Actual independent Busy4 executable |

There are318 raw passing test-name occurrences because the existing cross-process cache-lock helper repeats one name; it is not an additional unique test. The focused suites appear in this actual full gate: busy_tuning5, busy_wait6, concurrent_sessions2 and p6_budget7, total20 PASS. The unchanged six diagnostic unit tests also pass. Five credential-dependent real-S3 cases remain ignored in this full package invocation; their separate actual execution below is not folded into the317-test count.

Full Go platform gates explicitly use **`PERI_RUST_BINARY=/tmp/peri-stress-native-busy4-bin`**, not stale debug/native fallback:

- `go test -count=1 -json ./...`: exit0; **92 root PASS,150 subcase PASS,2 root SKIP**.
- `go test -race -count=1 -json ./...`: exit0; **92 root PASS,150 subcase PASS,2 root SKIP**.
- `go vet ./...`: exit0.

Both normal/race runs pass all seven actual-native P6 roots: encrypted ACK; KEK rewrap/restart/tamper; constraint rollback/session reuse; migration/test-owned failover; tokens/native readonly; NDJSON/committed prefix; reply-lost EOF/unknown-held permit. Their protocol/security regressions are current runs, not proof of future pressure success. Test-owned local integration fixtures/actions do not access main's stress fixture or authorize production signals. Existing Darwin race-linker/dependency warnings remain in raw evidence and were not silently removed.

Evidence lives under `/tmp/peri-native-busy4-f/`: Rust logs/status/counts, Go normal/race JSONL+stderr/vet/status/counts, git before/after records,302-file input snapshots, six-file freeze snapshots, compiled fingerprint snapshots and executable/architecture/compiler identity. F's Cargo and Go sessions both exited; no F build/test remains active at handoff.

## Busy, budget and safety semantics

Four seconds is the native **cumulative backoff configuration**, not a hard whole-request wall deadline, guaranteed success, fairness/FIFO policy or new database-global queue. Existing same-statement `step` handles native Sleep using at most10ms chunks, with unchanged duration/VM budget and local-authority checks before/after sleeping and IO/Yield. Remote physical IO, ownership validation and root publication fences retain their original paths. The native driver and Turso core are byte-identical. No outer SQL replay, statement reprepare, new transaction/reopen or retry loop was introduced. Existing internal engine behavior is not falsely claimed absent.

The single batch still installs one `ExecutionBudget` before its QueryRunner; the original start/VM counter cover every statement. The default SQL duration budget remains **five seconds**, with original SQL bytes/result rows/result bytes/VM-step budgets unchanged. The native wait is bounded by the original remaining budget rather than granting each subsequent statement a new five-second window. Ten-second public RPC deadlines and normal ownership/IO guards were not increased.

Current tests verify: a holder released after1.5seconds lets the **same contender call** ACK while an independent reader sees no uncommitted holder rows; held-lock timeout produces typed Busy with no failed-batch write/physical object IO; an interactive stale Memory read transaction produces typed **BusySnapshot** promptly, keeps its original snapshot/active transaction until caller's explicit ROLLBACK and does not reopen/replay; a single batch's earlier COMMIT remains visible when a later locked BEGIN returns typed Busy. Fresh-reader ledger/integrity checks verify those bounded offline outcomes.

The250ms cumulative-budget test includes a150ms prefix hook pause; the default five-second test includes a1.5second committed-prefix pause, so the blocked suffix is cut off before receiving a fresh full four-second allowance. These real native tests use deterministic test-store scheduling hooks; they are not a measured S3/Authority contention distribution or a hard real-time scheduling promise. The short40ms budget regression, VM/row/byte/SQL limits and existing fencing regressions also pass.

Expiry/revocation-during-wait cases remain explicitly **test-local/mock lease checks**, not an actual Authority grant revoked during a Busy wait. Full existing P4/ownership/encryption regressions and actual-native Go cases are preserved, but they do not upgrade that mock into a real busy-plus-Authority-revocation scenario. Diagnostic64-attempt/best-effort logging and public wire flags are unchanged; no global zero-drop or nonblocking stderr guarantee is added.

A's old-one-second RED reproduction `/tmp/peri-native-busy4-red.log` is preserved alongside its focused GREEN evidence, not hidden by this full gate. R6's two precisely matched typed Busy negatives remain actual FAIL; the class evidence does not establish their specific lock-holder cause, prove BusySnapshot, or retroactively explain R3/R5. Candidate tests do not erase the101-versus39 R6 ledger/oracle boundary.

## S3, inherited scopes and next boundary

The built minio test executable is **`/tmp/peri-loom-p6-dev/prototypes/v1/target/release/deps/minio-458fbff260aff428`**, SHA256 **`6bf93ab2bb8e6739bb5af387e6cae0767ca3500d4fb094868ec0648fff9ee946`**. After F's Cargo/Go work stopped, **main separately executed this already-built test binary**, with environment-only credentials/explicit opt-in and `--ignored --test-threads=1`, without Cargo/recompilation. Main reports actual PTY36338 exit0 and use of only fresh unique test-owned buckets. Those argv/environment/executor details are main's attestation, not recovered secret values or independently reconstructed arguments from the raw test log. F obtained no credentials and performed only the following readonly audit.

The complete owned regular0600 log `/tmp/peri-native-busy4-s3.log` is448bytes, SHA256 **`ab86b451c6cb0dc0403aae5ec5d61aa878a4eccd038ac91668292f052b8f4576`**. It names exactly five successful real-S3 tests: fresh-cache SQL recovery, object-store conformance, P2 authority refs/PITR/GC, P3 multi-DB unload/fresh-cache recovery and P4 two-worker fencing/restored-snapshot preservation. Its summary is **5 PASS,0 failed,0 ignored,3 filtered,7.67seconds**. The owned regular0600 `/tmp/peri-native-busy4-s3.exit` contains0, SHA256 **`9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`**. Source opt-in checks fail rather than silently skip these ignored tests; fresh-bucket absence preflight refuses mutation/cleanup of an existing bucket.

F used nofollow/nonblocking readonly descriptors with pre-open/post-open regular-file owner/inode checks, read the complete bounded log/exit file, reconciled all five names/results and rehashed the minio executable. Its before-main-S3 and after-main-S3 SHA are identical to the built SHA above. No credential-key markers appeared in the reviewed log; this is not a general secret-scanner proof. Audit details are `/tmp/peri-native-busy4-f/s3-verification.json`. **Current Busy4 main-executed real-S3 regression PASS is accepted on that attributed execution and independently checked raw evidence**, separately from F's native/Go gates and from any unrun pressure measurement.

Prior accepted BUSY real-S3 evidence remains historical, distinct from this new Busy4 invocation; its initial wrapper bookkeeping exit1 remains disclosed. Other unchanged controller/UI/TypeScript/Python/client gates were not duplicated. Go/Rust SDK cases included in the current complete commands are current executions; unrelated previously accepted suites remain inherited evidence, not newly counted PASS. Separately owned faultprobe work is outside this native gate.

The only repository file F writes for this task is this new report; old verification/experiment reports, author tests/source and user CLAUDE are untouched. Main owns code/report commits, new matching-content gate, any actual S3 invocation and explicit fresh R7 load authorization after builders stop. R7 is a new conditional functional sequence, not an unrun performance comparison; missing/SKIP/FAIL cases and absent final oracles must remain explicit. **No pressure, S3, Docker, external fixture API or production signal was executed by F.**
