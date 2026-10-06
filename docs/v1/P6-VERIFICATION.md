# P6 independent verification

## Verdict and scope

**P6 ACCEPTED within the explicitly verified scopes: ROOT code/test gates, genuine native live S3, proper-CA encrypted S3/TLS stack, actual SDKs, browser UI, and an actual Linux encrypted Local/TLS deployment.** Main authorized retaining the byte-identical completed Go normal/race gates and rerunning the finally changed launcher/UI/SDK/example scope. Both the immutable Go scope and the fresh complete code manifest compare equal, with every requested command actually completed. Separately executed native, live SDK/S3/browser, and Linux runtime evidence is independently inspected and attributed below. This does not retroactively make earlier failed tests or changing-manifest rounds pass. This report and the owned tests are frozen for handoff; it does not accept an unexecuted P0–P6 joint verification, pressure run, MinIO deployment, Kubernetes cluster, or HA configuration.

The independent verifier owns only `/tmp/peri-loom-p6-dev/platform/p6_verify*_test.go`, their integrated ROOT counterparts with explicitly authorized diagnostic-only additions, and this report. No production, Cargo, other documentation, P3/P4/P5 verifier, user CLAUDE, daemon/security, commit, push, or subagent changes are part of this work. Main's live smoke resources are not modified or terminated. Every owned Go file is below 500 lines.

## Actual process method

Each case builds the actual Go platform CLI into its own private temporary directory and launches that CLI, a genuine Rust Authority, and two genuine encrypted OwnedWorkers. The Go CLI runs the real controller, gateway, and KMS broker; these are not mocked controller or database processes. Synthetic credentials, independent loopback ports, and isolated backend/cache/state directories are generated per fixture. KMS has its own dedicated persistence directory.

Child environments use an explicit allowlist plus service-specific credentials. Workers receive Authority/control/SQL/key tokens and the broker endpoint, but no raw Local backend path or S3 credentials. No real S3 credential is acquired or persisted by this verifier. The sidecar requires explicit `PERI_RUST_BINARY`; eventual in-repo execution can locate a built binary through a repository ancestor, without a machine-specific fallback or helper skip.

The race round instruments both the Go test harness and each actual Go CLI executable: `PERI_P6_VERIFY_RACE_CLI=1` adds `go build -race` to fixture CLI construction. Native Rust is not claimed to be Go-race instrumented.

## Latest completed candidate

Main's completed native build log is `/tmp/peri-p6-native-final-build.log`. It includes the native diagnostics implementation and ends with a successful dev-profile build. Dependency warnings in that build are not treated as a completed strict-clippy gate. After the status correction, the sidecar platform module uses the integrated root controller through `replace peri-loom/controller => /Users/konghayao/code/ai/peri-loom/prototypes/v1/controller`, including the controller's UseNumber correction.

Actual Rust executable:

```text
/tmp/peri-loom-p6-dev/prototypes/v1/target/debug/peri-loom-v1
SHA256 91268f8077d94de3bf8e71557b67ecdd2d72104f8bfdaf615d761e243c4dd6d1
```

Both commands run from `/tmp/peri-loom-p6-dev/platform`, sequentially rather than concurrently:

```sh
PERI_RUST_BINARY=/tmp/peri-loom-p6-dev/prototypes/v1/target/debug/peri-loom-v1 PERI_P6_VERIFY_RACE_CLI=0 go test -count=1 -parallel=1 -json -run '^TestP6Verify' . > /tmp/peri-p6-verifier-inspect-fixed-normal.jsonl 2>&1
PERI_RUST_BINARY=/tmp/peri-loom-p6-dev/prototypes/v1/target/debug/peri-loom-v1 PERI_P6_VERIFY_RACE_CLI=1 go test -race -count=1 -parallel=1 -json -run '^TestP6Verify' . > /tmp/peri-p6-verifier-inspect-fixed-race.jsonl 2>&1
```

Counts are parsed from unique top-level Go JSON test outcomes, excluding package summary events. Both commands actually complete: normal **7 PASS / 0 FAIL / 0 SKIP**, exit 0, **33.379s**; harness plus actual-CLI race **7 PASS / 0 FAIL / 0 SKIP**, exit 0, **42.789s**. No helper skip or mocked process is counted as a pass.

| Actual case | Normal seconds | Actual CLI race seconds | Outcome |
| --- | ---: | ---: | --- |
| KEK rewrap, restart, authenticated-envelope and audit tamper | 3.17 | 7.35 | PASS / PASS |
| Encrypted native payload roundtrip, ACK, integrity, main/WAL/SSD checks | 4.79 | 4.39 | PASS / PASS |
| UNIQUE failure and explicit rollback on the same session | 2.88 | 3.97 | PASS / PASS |
| Graceful migration followed by actual SIGKILL and cache deletion | 10.94 | 12.09 | PASS / PASS |
| Scoped tokens, root/domain claims, tenant isolation, native readonly | 5.55 | 6.71 | PASS / PASS |
| Committed SQL prefix, bounded JSON and NDJSON | 3.05 | 3.88 | PASS / PASS |
| Actual native reply lost through socket EOF | 2.66 | 2.99 | PASS / PASS with expected unclean exit |

The executable content compares byte-identically before and after both rounds: **binary cmp 0**. A scoped 116-file manifest covers sidecar platform Go files, module/README files, embedded web assets, and integrated root controller Go/module/README files: **source cmp 0**, SHA256 `ffafe1d9fe61ef785b1ec23b9e66ca0386675fed3408058b1a78a36e38d5e780`. All owned test bytes also compare unchanged against the previous failed seven-case round, manifest SHA256 `bbd519b703bfae69724067e6bad27485bb428f7b5e268ba67d99d9e23115ad96`. No mtime-only stability inference is used. This is not a complete integrated Rust/native/fixture source manifest or an integrated full-gate claim.

Evidence:

| Evidence | SHA256 or result |
| --- | --- |
| `/tmp/peri-p6-verifier-inspect-fixed-normal.jsonl` | `4d91322ec79e2fd755a985919ea72600c09676c0a302bc6746e6fac0a1e0e424` |
| `/tmp/peri-p6-verifier-inspect-fixed-race.jsonl` | `2d14fb880aac571a3974b59faf370b74a7e268f067db81aba885e57e3b3a361b` |
| `/tmp/peri-p6-verifier-inspect-fixed-sources-before.json` | `ffafe1d9fe61ef785b1ec23b9e66ca0386675fed3408058b1a78a36e38d5e780` |
| `/tmp/peri-p6-verifier-inspect-fixed-tests.sha256` | `bbd519b703bfae69724067e6bad27485bb428f7b5e268ba67d99d9e23115ad96` |
| `/tmp/peri-p6-verifier-inspect-fixed-exits.log` | `normal=0 race=0 sources_cmp=0 binary_cmp=0` |

## Resolved status regression

In the preceding candidate, `TestP6VerifyEncryptedMigrationActualKillFailover` failed at `p6_verify_ownership_test.go:70` in both rounds. After an active native transaction made migration return an error, the verifier requested the placement through `/v1/databases/status` to prove there was no premature epoch advance. That status request returned **HTTP 409 / `operation_incomplete`**.

The inspected production cause was the new diagnostics dependency: `management_tools.go`'s `inspect` unconditionally called `nativeTool` for diagnostics, while `nativeTool` required `placement.Phase == "active"`. A legitimate drain, transition, or failed-worker placement therefore prevented status from returning the placement itself. The private diagnostics transport whitelist had already been corrected; this was a separate failure.

Main corrected both sidecar and root production: refs, placement, and metadata remain actual control-plane reads; non-active placement returns diagnostics tagged `unavailable / placement_not_active`; failure of attempted active native inspection is tagged `unavailable / native_inspection_failed`. This is not a fabricated zero metric, successful native inspection, clean-drain acknowledgment, or completed physical barrier. The unchanged independent migration case now passes in both complete seven-case rounds, including drain observation, epoch checks, cleanup, takeover, and actual crash recovery. No test was skipped, no timeout was increased, and no production workaround was applied by the verifier.

## What the successful cases establish

- The encrypted payload case reads back the inserted unique plaintext marker through native SQL, checks the acknowledged row and `PRAGMA integrity_check`, and successfully uses the real management checkpoint endpoint. It decodes Local object byte arrays, follows catalog references, and separately decodes native WAL write bytes. The latest round observes two native main segments, three native WAL write payloads, and two SSD segments, all without the marker. The state/audit secret scan also passes. This is payload evidence, not an assertion that all metadata or headers are encrypted.
- A native UNIQUE error yields `sql_error`, `execution_completed:true`, and `may_have_committed:true`. With `MaxConcurrent=1`, the same session executes explicit `ROLLBACK`, confirms the uncommitted row is absent, and subsequently writes successfully. A separate multi-statement error leaves its already committed prefix visible: execution completion is not automatic rollback or statement-batch atomicity.
- Two actually provisioned encrypted tenant databases provide a positive control for cross-tenant denials. Valid-HMAC but incorrect root/domain claims, token expiry, backing issuer-key revocation, privileged management access, and restore-invalidated branch tokens are rejected. A native read-only clone rejects a query-path `INSERT ... RETURNING`; its acknowledged rows remain intact.
- The NDJSON case receives 17 ordered row records and a matching completion record. Both native-result and NDJSON-overhead bounds produce terminal result-limit errors; the session and its single SQL permit remain reusable. Native results are materialized before NDJSON output: this is not incremental engine execution.
- KEK rotation and rewrap advance the wrapping-key version while preserving DEK/domain identity and acknowledged row 41 across real Go CLI restarts. A correctly persisted state update containing a bit-mutated wrapped ciphertext reaches the real broker and returns 503 without a DEK. Audit-chain mutation is refused on reopen. This proves wrapped-key authentication and audit integrity, not native-page AEAD tamper detection.
- The EOF proxy first receives an actual terminal native SQL-error reply, then discards it and closes the downstream socket. The gateway reports unknown outcome without terminal SQL flags, retains the only SQL permit, and rejects poisoned-session close without dispatch. Actual SIGTERM exit 1 is required by this case and is explicitly **not** accepted as clean completion, cancellation, rollback, or a completed barrier.
- The migration case refuses migration while a real native transaction is active and verifies that the grant/epoch have not advanced. Closing that session aborts its uncommitted row; migration then advances epoch 1 to 2 and preserves acknowledged row 7. The old worker's broker binding is denied, and a project API key cannot use the privileged key endpoint. Actual SIGKILL of the new owner plus deletion of its entire cache triggers automatic epoch 3 recovery on the other worker, preserving acknowledged rows 7/8 and native integrity in both rounds.

## Preserved development evidence

The older combined sidecar log `/tmp/peri-p6-verifier-focused-all.jsonl` remains unchanged: **5 PASS / 2 FAIL**, 22.943s, SHA256 `20e9d1fd06cb8aa7e45a42389b99df358e7f761a656703c82dde20d211257465`. Both failures were status requests after diagnostics was added while its private transport whitelist was still missing. The later correction does not erase this failed round.

Additional retained exploration:

- `/tmp/peri-p6-verifier-seven-normal.jsonl` and `/tmp/peri-p6-verifier-seven-race.jsonl`: the native-diagnostics-capable but control-plane-coupled candidate failed **6 PASS / 1 FAIL** in both rounds, normal 29.450s and actual-CLI race 39.339s. The executable SHA256 was `c0fa18f6f67689ed8a49f512dc7a22e61ca68d25c0696aff82516d53f70a1d2c`, with binary/owned-test before-after cmp 0. Log SHA256 values remain `425329d7595bb1f2c46c247c51421b1d52782ec234d382eb63ee68b54af4abb3` and `9d835f453d42180de07d9fdeec802aaa314d35be406b1488862c1e2d099d82b3`. These failures are retained, not overwritten by corrected-round evidence.
- `/tmp/peri-p6-verifier-initial.log`: the initial real management checkpoint request failed because the private upstream whitelist rejected checkpoint/compact before dispatch; the corrected endpoint subsequently passed actual verification.
- `/tmp/peri-p6-verifier-payload-diagnostic.log`: the verifier initially classified catalog keys as namespace-prefixed rather than relative keys and correctly refused to accept a vacuous payload scan. Its owned classifier was corrected; the actual payload counts were then established.
- `/tmp/peri-p6-verifier-uncertain.log`: the first EOF fixture incorrectly expected a clean CLI shutdown. Its owned expectation was corrected to require the actual exit-1 refusal, not to ignore a failure or manufacture a barrier.
- `/tmp/peri-p6-verifier-ownership.log`: an earlier isolated candidate genuinely passed graceful epoch 1-to-2 migration, SIGKILL, whole-cache deletion, automatic epoch 3 recovery, acknowledged rows 7/8, and integrity in 13.05s. That is historical pre-diagnostics-regression evidence, not acceptance of the latest failed candidate.
- `/tmp/peri-p6-verifier-focused-race-five.jsonl`: five targeted cases passed with actual CLI race instrumentation in 23.742s. This was not a full seven-case or integrated acceptance gate.
- Main reported exploratory budget compatibility regressions corrected with an actual 31-target pass, and an initial launcher KMS shared-directory startup failure corrected with a dedicated KMS subdirectory. These are explicitly owner-provided development history, not independently repeated gate results in this report.

## Handoff and limits

The sidecar seven-case scope completed before the subsequently authorized formal ROOT rounds. The final anonymous-audit correction and ROOT code/test closure now have actual completed evidence, including immutable Go normal/race provenance and fresh finally changed-scope checks. No calendar review date is inferred from host-clock timestamps in logs.

This verifier's own genuine-process fixtures use Local storage, not a new private-secret P6 S3 fixture. Main's actual S3/TLS, SDK, browser, and Linux deployment evidence is separately attributed rather than represented as verifier-launched processes. Compose rendering and Helm lint/template evidence do not establish a deployed cluster; the Linux runtime acceptance uses the actual latest image/container, not an older successful build. There are no benchmark, native page-AEAD tamper rejection, incremental native streaming, RSS, P0–P6 joint-verification, real-pressure, deployed MinIO, Kubernetes, or HA claims. P4/P5's single-Authority fencing and dirty-restart limitations are not broadened into automatic dirty recovery, physical deadline cancellation, or cancellation-as-success claims. Crash pins and receipt growth remain limitations.

## Formal ROOT round: preserved counterexample

The first production/test/native source manifest covers 4,169 files, SHA256 `d1f1931b9ac0bbfb3e5b69da3cd6757b06eaeb6bd0d4244aea08da15df9392db`, under `prototypes/v1` and native Turso. Its policy excludes docs/deploy, Markdown documentation, generated targets/dist/node_modules/bytecode/runtime data, Git metadata, and compiled CLI outputs. **This manifest is not stable final-acceptance evidence:** main subsequently authorized launcher/test-tool corrections, and the verifier added the explicitly requested safe failure-capture helpers. A fresh final manifest is required.

The actual full root platform normal command was `PERI_P6_VERIFY_RACE_CLI=0 go test -count=1 -parallel=1 -json ./...`, with `PERI_RUST_BINARY` set to the recorded P6 executable. `/tmp/peri-p6-root-verifier/platform-normal.jsonl` records **77 unique top-level PASS / 2 intentional helper SKIP / 1 FAIL**, exit 1, 36.723s. Package counts are platform 68 PASS / 2 SKIP / 1 FAIL, Go SDK 6 PASS, and SDK CLI 3 PASS. The helper skips are not counted as passes. The pipeline stopped before the full platform race gate; no completed full-platform race result is invented.

The failed assertion is `p6_verify_ownership_test.go:111`: after actual SIGKILL and deletion of the killed worker's whole cache, polling `/v1/databases/status` returns HTTP 409 / `operation_incomplete`. This is distinct from the earlier non-active native-diagnostics dependency at line 70.

Diagnostic-only test additions preserve every original assertion and deadline. They read only the fixture's private metadata, issue a separate bounded read-only Authority refs diagnostic after failure, and optionally observe the original control-plane RPC through a no-fault proxy. The observer is opt-in through `PERI_P6_VERIFY_OBSERVE_AUTHORITY=1`; default formal tests remain direct. Requests are not retried, forwarded replies are not replaced, and only parsed error strings and phase/epoch fields are logged after synthetic-token redaction; request bodies, headers, credentials, result payloads, SQL, and DEKs are not echoed.

`/tmp/peri-p6-root-verifier/status-failure-capture.log` records 4 PASS / 1 FAIL in five diagnostic repetitions, 55.349s. The failed case's private placement is `grant_pending`, revision 34, global epoch 3, desired worker `worker-a`, epoch 3. A subsequent direct refs read succeeds. That later success alone does **not** identify the original failure and is not treated as a retry that makes the failed assertion pass.

The original failure is captured in `/tmp/peri-p6-root-verifier/status-original-race-capture.log`: two cases passed, then the third failed; fail-fast stops remaining requested repetitions without counting them as passed. Total execution is 37.607s. The original Authority maintenance refs reply for `verify-25` is **HTTP 200, `ok:false`, error `Retryable admission transition busy`**. Before the diagnostic the private placement is revision 34 / `grant_pending`; after a separate successful refs diagnostic it is revision 35 / `acquire_pending`, both at epoch 3. This establishes namespace admission-transition contention for that concrete failure, not namespace `UnknownOutcome`, blocked persistence, authentication failure, or a guessed CAS failure. The inspected `admission_gate.rs` transition compare-exchange produces this exact error when another transition already owns admission.

Passing repetitions, including five no-fault-observer repetitions in 55.790s, do not erase the failed full gate or establish a fix. Production correction remains main's responsibility; no verifier production patch, assertion relaxation, retry, or timeout extension is used.

Main subsequently corrected `/status` without changing SQL/fencing: controller placement and metadata are actual local reads; inability to inspect Authority refs is explicitly represented as `unavailable / authority_inspection_failed`. The separate `/refs` endpoint remains mandatory and fail-closed. Native diagnostics remain optional and explicitly unavailable when not obtainable. Status is not an ownership grant, a lease validation, a physical barrier, or permission to unblock a namespace.

The requested status-focused direct-process repeat then actually passes **5 of 5**, exit 0, **57.980s**, log `/tmp/peri-p6-root-verifier/status-fixed-five.log`. `PERI_P6_VERIFY_OBSERVE_AUTHORITY=0` preserves the direct path. Each repetition preserves the original migration/fencing assertions and deadlines, observes the pre-cleanup epoch unchanged, migrates after transaction-session close, actually SIGKILLs the new worker, deletes its whole cache, and recovers acknowledged rows 7/8 with integrity at epoch 3. Binary before/after content cmp is 0, SHA256 `91268f8077d94de3bf8e71557b67ecdd2d72104f8bfdaf615d761e243c4dd6d1`. This is targeted corrected-status evidence, not a fresh full-platform gate; the final audit fix was still under development, so no Go-source freeze is inferred from these repetitions.

## Other completed ROOT gates and attributed evidence

These results are completed evidence for their recorded candidate, not a substitute for the failed full-platform gate. Logs reside under `/tmp/peri-p6-root-verifier/` unless otherwise stated.

| Gate | Actual result |
| --- | --- |
| Root controller `go test -count=1 -parallel=1 -json ./...` | 63 top-level PASS / 1 helper SKIP, 57 subcase events, exit 0, 22.767s |
| Root controller `go test -race -count=1 -parallel=1 -json ./...` | 63 top-level PASS / 1 helper SKIP, 57 subcase events, exit 0, 27.357s |
| Platform and controller `go vet ./...` | Both exit 0 |
| Platform/controller Go source `gofmt -l` | Empty output, exit 0; no formatting writes |
| Actual platform, controller, and Go SDK CLI builds | All exit 0, separate verifier outputs outside frozen source |
| TypeScript SDK build and `--noEmit` typecheck | Both exit 0, cached offline TypeScript 7.0.2 |
| TypeScript SDK Node tests | 13 PASS / 0 FAIL / 0 SKIP |
| UI lossless JSON Node tests | 3 PASS / 0 FAIL / 0 SKIP |
| Updated Python launcher unit tests | 6 PASS, 0.020s; mocked cleanup tests are not whole-stack process proof |

Fermat's separately executed native logs `/tmp/peri-p6-root-rust-{fmt,clippy,test,build}.log` and `status.log` were independently inspected. All four recorded command exits are 0. Parsing the native test log yields **290 raw successful test lines, 289 unique successful names, 5 ignored**, with the duplicate `cache_directory_lock_is_cross_process_even_without_native_locks` child occurrence excluded from unique counts. Its 159-Rust-source before/after lists compare equal, SHA256 `dfd681b7f0efe33a953c93d4258ef37b689f018a82a96c3054e2f3f428f994d8`. Its broader before/after lists did change controller/platform/deployment/documentation files while other owners integrated them; that broader comparison is not represented as a global stable freeze. Dependency warnings in successful no-deps strict clippy are recorded, not silently transformed into a dependency-clean claim.

Main's `/tmp/peri-p6-root-real-s3.log` was independently inspected: the five genuine ignored live-network tests actually pass, exit-0 result, 4.32s. This is separately executed owner evidence, not a verifier-generated secret environment. Log SHA256 is `a3caace1209e63ae265fcca66b31fee801561e24b2f53c4beb601441aea4b3c0`.

Main's full-stack S3/TLS evidence `/tmp/peri-p6-s3-whole-smoke-final.log` and credential-free JSON was also independently inspected: certificate-verified TLS, external S3 fixture, and 11 reported functional checks pass, including encrypted SQL ACK, exact int64, explicit rollback, integrity, checkpoint, restore/token invalidation, native readonly clone, migration, suspend/resume, and actual native metrics. JSON SHA256 is `12817797ecc796edd62c788d0ab79a3b269eb742c75d984208b2b34d3c0a99cc`. Observed active permits and unknown-held permits are zero, but `errors_total` includes two `audit_unavailable` events; this evidence is **not** called zero-error. Main reported the first two smoke failures were test-tool wire-shape expectations, subsequently corrected, not production faults. SDK and deployment claims still require their own actual evidence rather than inference from this smoke helper.

Main identified the audit-error misclassification in source: unauthenticated requests supplied empty subject/project to file-audit validation, so a stale-token authentication rejection became `audit_unavailable`. B's separately owned root gateway correction and real-file-audit regression tests were frozen before the completed full ROOT normal/race gates. Safe anonymous identity is audit-event-only, not an authenticated principal; genuine audit failure remains 503 or post-dispatch unknown outcome. The historical smoke's two misclassified events are preserved rather than relabeled as passing authentication behavior.

## FINAL SOURCE READY round: commands pass, manifest fails

Evidence directory: `/tmp/peri-p6-root-verifier/final-source-ready/`. All tests run serially by module, with the same explicit native executable and original assertions/deadlines. The actual-CLI race fixture uses `go build -race`; formal fixtures use direct Authority transport, not the diagnostic observer. Unique top-level Go outcomes exclude package summaries, nested subcases, and child-process output duplicates.

| Completed gate | Actual result |
| --- | --- |
| ROOT platform normal | 80 PASS / 2 helper SKIP / 0 FAIL; platform package 38.591s |
| ROOT platform harness and actual CLI race | 80 PASS / 2 helper SKIP / 0 FAIL; platform package 55.778s |
| ROOT controller normal | 63 PASS / 1 helper SKIP / 0 FAIL; 31.104s |
| ROOT controller race | 63 PASS / 1 helper SKIP / 0 FAIL; 27.515s |
| Both Go modules vet; platform/controller/SDK CLI builds | All exit 0 |
| Both Go modules gofmt inspection | Empty output, exit 0; no source writes |
| TypeScript SDK build and typecheck | Both exit 0, cached offline compiler |
| TypeScript SDK tests | 13 PASS / 0 FAIL / 0 SKIP |
| UI JSON and refs tests | 5 PASS / 0 FAIL / 0 SKIP |
| Python launcher runtime tests | 6 PASS, 0.005s |
| Python launcher protocol tests | 10 PASS, 0.009s |
| Rust fmt including new example | Exit 0 |
| Locked Rust all-targets/no-deps clippy with `-D warnings` | Exit 0, including new example |

All seven owned genuine-process cases pass in both full platform rounds. Each platform round additionally records 81 successful nested subcase events; each controller round records 57. These are not added to unique top-level counts. B's frozen audit fix uses anonymous labels only in the audit event, not as an authenticated principal; genuine audit failure remains failure/unknown rather than false authentication success.

The 4,193-file source manifest changes from `296a0b85493132159515294a2792593b597b94a3ae01b2da25524a0427009d7c` to `b189131c55ebb395ee7c75e8a3194e8b92dfa76a3dcb4d2e95074ff410b1fc05`; `source-cmp` exits 1. The sole differing file is `prototypes/v1/examples/p6_sdk_smoke.rs`, from SHA256 `3b5ce00f7d828d26e6e41633c80cc89be14e8acc8e92d7682a5d218c01c81c27` to `46d7547e76ebc257a8971287eb397f2c637c1650375d237aac1b85db5e217b72`. This change is reported to main immediately; neither passing tests nor a semantic explanation converts this comparison into a stable final freeze.

The native executable remains byte-identical, SHA256 `91268f8077d94de3bf8e71557b67ecdd2d72104f8bfdaf615d761e243c4dd6d1`. Native submodule HEAD/tree remain identical. A supplemental ten-file module Markdown/README manifest also compares equal, SHA256 `424e187bc5ecb0ce5dbd4109d75e484baca1fc3a1caad24263b4ae34b001f4cc`. Docs/deploy and generated/runtime outputs are explicitly outside the code manifest. A final example refreeze and correctly scoped recertification remain required.

The immediately preceding pre-closing round is also retained under `/tmp/peri-p6-root-verifier/final-audit-fixed/`: all commands completed successfully, but twelve test/SDK-tool files changed and its 4,193-file manifest changed from `bd7c7a5bc4ab536c14ca79139164d57182e9b0dda6e09171bf1bb3ffc0e661cc` to `6863c8bc829a4164dd3a1bf6107b57fea8e890120112573a7078111deb49d59f`. That round tested UI 3 and launcher runtime 6, not the final UI 5/protocol 10. It is not substituted for final source stability.

## Final authorized split-scope closure

Main explicitly authorized not repeating completed full Go suites when their source bytes are unchanged, while rerunning the finally changed launcher, UI, SDK, and Rust-example scope. Evidence is `/tmp/peri-p6-root-verifier/closure/`.

The selected 118-file platform/controller Go scope includes all `.go` files, `go.mod`, `go.sum`, and every embedded platform web asset. `go-tested.json` from the completed full normal/race round and `go-current.json` from the final closure compare equal, SHA256 `9024035e4d32c861d2f948918426c5e757936ba449ffc4743dcb99727e260297`. Therefore the actual platform 80 PASS / 2 helper SKIP and controller 63 PASS / 1 helper SKIP in each normal/race round remain applicable to exactly the final Go bytes. There is no claim that the entire former 4,193-file candidate stayed unchanged.

The fresh 4,195-file complete code/test/native manifest compares byte-identically before and after all closure commands, SHA256 **`746615bd1637c44291238fc0562207948af1d872f42d627dd6a75ff8d3a00c54`**. It covers `prototypes/v1` including frozen stress/faultprobe source, and native Turso, under the previously documented generated/docs/deploy exclusions. The supplemental ten-file module Markdown/README manifest also compares equal at `424e187bc5ecb0ce5dbd4109d75e484baca1fc3a1caad24263b4ae34b001f4cc`. The native executable compares equal at the recorded `91268f80…` SHA256. These are byte/content comparisons, not mtime evidence.

| Fresh closure command | Actual result |
| --- | --- |
| Python launcher runtime | 8 PASS, 0.119s; includes actual OpenSSL CA/leaf verification and child session isolation |
| Python launcher bounded protocol | 10 PASS, 0.009s |
| UI JSON/refs tests | 5 PASS / 0 FAIL / 0 SKIP |
| TypeScript SDK tests | 13 PASS / 0 FAIL / 0 SKIP |
| TypeScript build and `--noEmit` typecheck | Both exit 0 |
| Rust fmt including final example | Exit 0 |
| Locked Rust all-targets/no-deps strict clippy, `-D warnings` | Exit 0, including final example |
| Source, binary, and immutable Go provenance comparisons | All exit 0 |

The root and sidecar owned verifier test files are frozen, ten per scope, all below 500 lines. Their exact combined file/hash records are `closure/owned-tests.json`, SHA256 `ecc9fef1f27fbfa36c8fa9f35e124f516556fd05fbd216e624c611dbadb1e634`.

## Actual externally executed closure evidence

These owner-run logs are independently read, but their processes and private S3 environment were not launched or modified by this verifier.

| Evidence | Actual inspected outcome | SHA256 |
| --- | --- | --- |
| `/tmp/peri-p6-s3-proper-ca-whole-smoke.json` | Certificate-verified proper-CA S3 whole stack, all 11 checks PASS; active/unknown-held permits zero | `87ceff78492e3db6b47f2ffb2510d4cdd62f4f334095c5a1777cc9fb142e95ac` |
| `/tmp/peri-p6-sdk-live-rust-proper-ca.log` | Actual Rust SDK verified-TLS status, native exact signed-int64 SELECT, explicit close PASS | `c4dbe8398e78fe1b66a53ed2464a5a5d8ca81be1f41f623857ebed8ee0bb2e82` |
| `/tmp/peri-p6-sdk-live-ts-proper-ca.log` | Actual TypeScript SDK verified-TLS status, native exact BigInt SELECT, explicit close PASS | `686b9b35ae1c22e23000ab50c697917515bcc4f15cbfbea277d4ee7a0d5f720d` |
| `/tmp/peri-p6-sdk-live-go-cli.log` | Actual Go CLI/SDK verified-TLS native exact signed-int64 SELECT and explicit close PASS | `ec7fab6a8a09e5e76f2e9c6ca784b979bb83a8d777f1d40315854fbf628b3fa1` |
| `/tmp/peri-p6-ui-final-smoke.log` | Actual headless Chromium authenticated catalog, opaque session, exact int64 JSON/NDJSON, real refs graph, explicit close, no credential browser storage PASS | `cbd02e19effcb8181cdfa55a34b35ba4fc34d5deedbfc41df65a87deae96e28d` |
| `/tmp/peri-p6-deploy-proper-ca-gates.log` | Compose local/S3/MinIO render, Helm CA-mount lint/template, invalid backend/TLS schema rejection PASS | `887fa558d4ff6e0901f73e56e4a8110f2a00b75ac6871dc35e4926146c093214` |

The latest S3 metrics report one expected `unauthenticated` event, with no `audit_unavailable` entry (zero), rather than a fabricated zero-error aggregate. The tightened smoke requires stale-token HTTP 401/`unauthenticated`. Proper CA/leaf generation replaces the earlier TLS-helper failure; no TLS-verification bypass is accepted. The earlier `/tmp/peri-p6-sdk-live-rust.log` actually fails readonly management status with `outcome_unknown` and is not erased or counted as a pass. Likewise an old group-delivered SIGINT with exit 0 is not accepted as joined-clean shutdown proof: the launcher now starts isolated sessions, and the final joined-shutdown claim uses the separate latest Linux SIGTERM probe below.

## Final Linux deployment and handoff freeze

The latest image is **`sha256:bab9892875497a976ae96022bfc3eea79daea02f17325f2889772461c565db10`, `linux/arm64`**. The verifier independently confirms both through read-only `docker image inspect peri-loom:p6-verify`. `/tmp/peri-p6-docker-build-offline.log` actually completes export of that image; an earlier successful image is not substituted for it.

The before-build, after-build, and after-runtime production manifests compare byte-identically. Their canonical compact 4,268-record list SHA256 is **`5b35e773cca212c0c3240244c0af3e69a3cba4c64c305241847ab84304bfb3c9`**; each full manifest file hashes to `7f49007b1488d22ebb5bc10d4190644172d53bdc53b67523533778bed03a0fa5`. The verifier recomputes that canonical digest and independently checks every current input: all 4,268 records match, including twelve symlinks represented by their literal link targets, not dereferenced content. Verification details are `closure/docker-input-verification.json`. This separately scoped deployment-input manifest is not mislabeled as the code/test manifest.

| Final actual Linux evidence | Inspected outcome | Log SHA256 |
| --- | --- | --- |
| `/tmp/peri-p6-docker-build-offline.log` | Latest Linux image build/export complete | `821fe96f848e9968f766d96730253fcc5cbf2db6b9c1521acc91eaf9bd81f263` |
| `/tmp/peri-p6-docker-live-cli.log` | In-container actual Go SDK/CLI verified-TLS exact integers, integrity, checkpoint, status PASS | `803128705b5b4e7dbd77ba5bad048cac5a3c00069babddec9b7b159fc6dd3b33` |
| `/tmp/peri-p6-docker-live-rust-sdk.log` | External actual Rust SDK verified-TLS exact-i64 SELECT/status/explicit-close PASS | `c4dbe8398e78fe1b66a53ed2464a5a5d8ca81be1f41f623857ebed8ee0bb2e82` |
| `/tmp/peri-p6-docker-live-ts-sdk.log` | External actual TypeScript SDK verified-TLS exact-BigInt SELECT/status/explicit-close PASS | `686b9b35ae1c22e23000ab50c697917515bcc4f15cbfbea277d4ee7a0d5f720d` |
| `/tmp/peri-p6-docker-live-shutdown.log` | Actual launcher SIGTERM joined exit 0, exact Authority `clean\n` marker, stable production input, cleanup only owned container/volume PASS | `504f1a99a901f37928db4dc770535654b797a858e0706ab2fca306256bd54417` |

Main reports the isolated port-19445 deployment ran actual encrypted Local storage with Authority, two workers, controller, gateway, broker, and proper TLS. Its actual application and shutdown outcomes above are independently inspected owner logs; the verifier did not launch, interrupt, or remove those resources. This deployment is distinct from the proper-CA real-S3 whole-stack proof and the five genuine native S3 tests.

The default online build failure is preserved in `/tmp/peri-p6-docker-build-final.log`: crates.io index transfer stalls and the Rust stage exits 101 after the reported 300.7s failure point. It is not called a pass. Main reports the successful offline path used 273 lock-checksum-verified host-cache archives and public sparse metadata, with optional local Cargo context and narrowed native inputs; it does not establish online registry availability. No image push or user RustFS modification is part of this verification.

The final code/test manifest is recaptured after deployment inspection as `closure/source-final-confirm.json` and still compares equal to the 4,195-file closure baseline at `746615bd1637c44291238fc0562207948af1d872f42d627dd6a75ff8d3a00c54`. Owned tests and this report are now frozen; main may stage the explicitly permitted P6 files while excluding user CLAUDE changes. P0–P6 joint verification and real pressure remain subsequent, separately authorized work, not claims in this acceptance.
