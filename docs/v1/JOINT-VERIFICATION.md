# Independent v1 joint verification

## Current verdict

**JOINT v1 FUNCTIONAL VERIFICATION PASS within the explicitly verified scopes.** B's newly rerun native gates yield 325 unique PASS / 5 ignored; unchanged accepted P6 Go, live S3, SDK, browser and Linux deployment evidence is reused with explicit provenance. Main's new actual release S3/TLS fault execution exits 0 and completes the entire recovery/fencing oracle. This verifier independently reads its evidence and verifies unchanged script/native/Go scopes. The initial failed attempt remains a failure. The later fault-fixture launcher shutdown exits 1 and is explicitly NOT clean; that separate conservative teardown outcome does not negate the completed recovery oracle. There is no pressure, benchmark, performance, HA, deployed MinIO/Kubernetes, or universal correctness claim. This report is final and frozen for handoff.

The independent verifier owns this report only. A owns `prototypes/v1/platform/tests/joint-s3-failover.py`. The frozen `P6-VERIFICATION.md` and `P6-LAUNCHER-REVIEW.md` are not edited. No production, test-script, credentials, user CLAUDE, daemon/security, commit, push, or subagent changes are part of this work. No pressure or benchmark is executed or inferred.

## Evidence and source matrix

“This joint round” identifies newly executed work; it does not mean every command was executed by this verifier. Owner-run logs are independently inspected and explicitly attributed. Reused completed gates are not counted as newly rerun tests, and normal/race repetitions are not summed into extra unique cases.

| Scope | Provenance | Actual outcome / current status |
| --- | --- | --- |
| P0 native complete locked/offline tests | B reran in this joint round; report and recorded exits inspected | 36 unique PASS, 0 FAIL, 0 ignored |
| v1 native complete locked/offline tests | B reran in this joint round; report and recorded exits inspected | 289 unique PASS, 0 FAIL, 5 ignored |
| Native formatting, all-target/no-deps strict clippy and dependency features | B reran both packages in this joint round | All recorded command exits 0; dependency warnings are not represented as dependency-wide lint success |
| ROOT platform ordinary and actual Go CLI race suites | Reused independent accepted P6 full-gate logs | Each 80 unique top-level PASS / 2 helper SKIP / 0 FAIL |
| ROOT controller ordinary and race suites | Reused independent accepted P6 full-gate logs | Each 63 unique top-level PASS / 1 helper SKIP / 0 FAIL |
| Go vet, formatting and actual CLI builds | Reused accepted P6 completed gates | Exit 0; no claim of a new joint rerun |
| Final Python launcher runtime/protocol, UI tests, TypeScript SDK tests/build/typecheck, Rust example fmt/strict clippy | Reused accepted P6 final closure | Python 8+10, UI 5, TS 13 PASS; toolchain gates exit 0 |
| Five genuine ignored native S3 tests | Reused owner-run accepted P6 live evidence | 5 PASS, 4.32s; not run by B's new default native suite |
| Proper-CA encrypted S3/TLS whole stack | Reused independently inspected main-run accepted P6 evidence | 11 functional checks PASS; expected unauthenticated event, zero audit-unavailable events |
| Actual Go/Rust/TypeScript SDKs and browser | Reused independently inspected accepted P6 owner evidence | Verified TLS and exact integer handling; browser real refs graph/no credential storage |
| Latest actual Linux Local/TLS deployment and joined shutdown | Reused independently inspected accepted P6 owner evidence | Latest image, real SDK/application checks, SIGTERM joined exit 0, exact Authority clean marker |
| New joint-script syntax and safety tests | This verifier, this joint round | Initial 4 self-tests PASS, 0.004s; alias-corrected 5 PASS, 0.002s; final bounded-wait 7 PASS, 0.005s; no real signal/deletion/RPC |
| Current private fixture read-only preflight | This verifier, this joint round | Initial alias FAIL preserved; corrected initial fixture and fresh release fixture preflights PASS |
| First actual S3/TLS fault attempt | Main executed after independent frozen-script review | Exit 1, `process_argv_unavailable` during post-kill wait; owner absent, cache not deleted; FAIL preserved |
| Fresh corrected actual S3/TLS fault and recovery | Main executed in this joint round; independent proof/source checks completed | Actual exit 0, full oracle PASS: owner-b to owner-a, epoch 2→3, cache deletion, ACK recovery/integrity/new ACK/stale-session denial |
| Fault-fixture teardown after successful recovery | Main's actual launcher SIGTERM | Exit 1, NOT clean; Authority clean marker and all four owned child PIDs gone separately reported |

Native totals are **325 unique PASS / 5 ignored**, not raw child-helper counts. B records P0 37 raw named entries with one extra helper occurrence and v1 295 raw named entries with one extra helper occurrence. Ignored network tests are not counted as newly passing ordinary tests. Go helper skips are not passes; nested subcases and child-process output are not additional unique top-level outcomes.

## Native joint evidence

The source report is `/tmp/peri-v1-joint-native.md`, SHA256 `ec51e66eb38d5a5e0ac22326328edebeb5b4cd89c935ba08658db42f19a78666`. Evidence directory: `/tmp/peri-v1-joint-native/`. Its eight test/fmt/clippy/features exit files are independently read and all contain 0. `before.json` and `after.json` compare byte-identically, SHA256 `6163740b8df5d409e8e6f16aaa9ec28091e8f516dbc81e325549e5d0c46179a9`.

B's snapshot covers 171 native files at scope SHA256 `839fe1f8126b0ec5c4cbabf8b77ffdc7aee9fa1a2c2efc6427eee1b5463a33eb`; the tracked fork scope is separately recorded. Native test HEAD is `1130bc57c8a904e9d0ad4d1a1c5c8dd76e691655`, with native bytes unchanged from accepted P6 `25069e10bf823fe0927639ae21c52f881bf2a9d1`. The subsequent read-only review observes HEAD `0425991fcef4930a73315a50c2ecb9ffe0365431`; no rerun is invented merely because later documentation changes the commit ID.

The Turso fork's reported clean HEAD is `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`, matching the parent gitlink, on `codex/p0-core-prototype`. Native compatibility evidence covers both P0 without encryption and v1 with encryption against this fork; it is not the entire upstream Turso workspace's test/lint coverage or an overlapping-Authority HA proof.

## Frozen P6 evidence reuse

The accepted frozen report remains SHA256 `4eb2a0ea74c60820659242fbab3f36674a00109f76d0cb0c7d1f92b8edefa5c0`. This joint review independently checks all 118 platform/controller Go/module/embed records in `/tmp/peri-p6-root-verifier/closure/go-current.json`: **zero current byte mismatches**. Its scope SHA256 is `9024035e4d32c861d2f948918426c5e757936ba449ffc4743dcb99727e260297`. This makes the recorded completed Go normal/race results applicable to those unchanged bytes; it does not claim the whole earlier P6 tree remained unchanged after adding a new joint script or documentation.

Primary reused evidence:

- `/tmp/peri-p6-root-verifier/final-source-ready/`: actual complete platform/controller normal/race and tools. Its historical whole-source comparison failure is preserved, not changed into a pass.
- `/tmp/peri-p6-root-verifier/closure/`: authorized split-scope closure, with completed changed-scope commands and stable 4,195-file P6 code/test manifest `746615bd1637c44291238fc0562207948af1d872f42d627dd6a75ff8d3a00c54`.
- `/tmp/peri-p6-root-real-s3.log`: actual five native live-network tests.
- `/tmp/peri-p6-s3-proper-ca-whole-smoke.{log,json}`: actual proper-CA S3/TLS eleven-check stack; credential-free JSON SHA256 `87ceff78492e3db6b47f2ffb2510d4cdd62f4f334095c5a1777cc9fb142e95ac`.
- `/tmp/peri-p6-sdk-live-{rust,ts}-proper-ca.log`, `/tmp/peri-p6-sdk-live-go-cli.log`, `/tmp/peri-p6-ui-final-smoke.log`: actual SDK and browser evidence.
- `/tmp/peri-p6-docker-final-equivalence.log` and `/tmp/peri-p6-docker-live-{cli,rust-sdk,ts-sdk,shutdown}.log`: actual Linux deployment equivalence, application checks and joined-clean proof.

Accepted Linux image: `sha256:bab9892875497a976ae96022bfc3eea79daea02f17325f2889772461c565db10`, linux/arm64. Its separately scoped 4,268-record compiled-input digest is `5b35e773cca212c0c3240244c0af3e69a3cba4c64c305241847ab84304bfb3c9`. This was independently checked during P6, not asserted as a newly built joint image. The default online crates.io build failure remains a failure; offline success does not establish online registry availability. Old group-SIGINT exit 0 is not substituted for the inspected actual joined shutdown.

## New joint script: safety review and preparation

The original candidate fixture is `/private/tmp/peri-p6-s3-leaf.DNSTWI5q`. After its failed attempt, the only newly authorized execution fixture is `/private/tmp/peri-stress-joint-xs2gO5jO`; this is not a wildcard permission for other temporary paths. This verifier performs review, not execution. No S3 administration, bucket/object cleanup, arbitrary process signaling, launcher termination, surviving-worker termination, or cache deletion outside the proved dead owner's `cache-a`/`cache-b` is permitted by the scoped fault review.

The initial script SHA256 is `e8ade7f5187d6efe1af2c3ad81dbc508250ca5a7daa3d2387d2e1adb18b50ccb`. Read-only fixture inspection constructs HTTP clients but performs no RPC; only private topology/configuration and process/path identities are read. It does not print tokens, raw configuration, environment, SQL credentials, or KMS material.

Confirmed protections in the reviewed script:

- Exact fixed fixture, canonical owned paths, private root/config modes, duplicate-key refusal, bounded configuration/replies, redirect/proxy refusal, proper TLS hostname/CA verification.
- Worker UID, launcher parent PID, exact argv, session-leader PID, configured address and cache binding; recheck immediately before signaling a single PID, not a process group.
- Reap original PID before cache deletion; refuse changed/reused process identity. Cache device/inode and fixed name are checked, recursive symlinks rejected, descriptor-safe deletion anchored to the private root.
- Actual current Authority binding must match placement before fault and after recovery; optional status refs/native diagnostics alone are not ownership proof.
- Unknown/failed mutation dispatch is not retried or relabeled completed. Only explicit read-only state polling repeats; the original twenty-second recovery deadline is not extended.

Initial real preflight fails `launcher_identity_mismatch`: UID and live launcher PID `38557` match, but argv contains `/tmp/peri-p6-s3-leaf.DNSTWI5q` while the script requires the `/private/tmp/...` spelling. The supplied path resolves to the exact authorized fixture. This is a real readiness blocker, not an actual failed fault/recovery experiment. Main/A were notified to accept only the two exact system aliases with canonical-target equality, retaining rejection of arbitrary paths. The initial failure remains in the evidence; it is not retroactively marked PASS.

Initial evidence: `/tmp/peri-v1-joint-independent/readonly-preflight.json` and `script-self-test.log`. The initial four self-tests use mocks and independent temporary directories; no actual owner PID is signaled. No failure is swallowed to allow execution.

A then freezes the 431-line corrected script at SHA256 **`b1a074f093d71c83e322df1a8075c8d807c39e07e38a91712450230fe8ee88a4`**. This verifier independently reads the alias helper, verifies that only the two exact system aliases are accepted with canonical-target equality, and runs its now-five self-tests: **5 PASS / 0 FAIL, 0.002s**. Traversal, doubled/trailing separators, relative/other-temp paths and duplicate flags remain rejected; both separated and `--run-dir=value` flag forms are supported. No broader arbitrary-path exception is introduced.

The repeated actual fixture preflight constructs clients without RPC and passes with unchanged script bytes. Evidence: `script-fixed-self-test.log` and `readonly-fixed-preflight.json` in the independent evidence directory. Launcher PID is `38557`, Authority PID `39645`; worker-a PID `39648` binds only `cache-a` device `16777231` / inode `186280548`, and worker-b PID `39649` binds only `cache-b` device `16777231` / inode `186280549`. Both worker session leaders equal their own PID, and UID/parent/exact argv/cache binding/private-file checks pass. Binary `/tmp/peri-loom-p6-dev/prototypes/v1/target/debug/peri-loom-v1` hashes to `91268f8077d94de3bf8e71557b67ecdd2d72104f8bfdaf615d761e243c4dd6d1`.

The verifier notifies main that the frozen script has no remaining readiness blocker for a single execution under the already specified scope. These PIDs/inodes are a review snapshot, not an exemption from the script's immediate identity checks. Main must select only the owner validated by the actual current Authority binding, signal only that one proved PID, wait for its disappearance, and delete only its proved cache. No other process or S3 administrative operation is authorized by this review.

Residual limits require explicit execution-scope approval: process-identity check and `kill(pid)` are not an atomic kernel capability, and a cache inode check before removal is not an absolute concurrent-replacement guarantee. The script itself records remaining TOCTOU. The fixture must not be concurrently renamed/replaced/restarted during the destructive window. These protections reduce risk but are not claimed to eliminate same-user adversarial races.

## First actual joint failure: retained evidence

Main performs the first actual attempt with the frozen `b1a074f…` script and reports command exit 1. Log `/tmp/peri-v1-joint-independent/actual-fault.log` hashes to `7a4830f4fa3d20eaed2067e0d80db14b825b07247743ccff6753da679895e57e`. Private credential-free evidence is `/private/tmp/peri-p6-s3-leaf.DNSTWI5q/joint-s3-failover-5aa4f4df4f6d4ce6.json`, SHA256 `05bfb03ed60ab138750bf1ff4c838d362601f2227810cca9e3672e777a910390`.

The evidence has `pass_result:false`, failure `process_argv_unavailable`, acknowledged durable commit count 2, acknowledged uncommitted insert, and selected worker-a PID `39648` at epoch 6. Its coarse stage remains `confirm_owner_and_kill`, which does not accurately distinguish the post-signal wait; it is not used to invent a completed wait or cache deletion. Main reports the SIGKILL was issued and the transient argv/sysctl failure occurred while waiting. This verifier independently observes `ps -p 39648` exit 1 and confirms cache-a remains a private owned directory with the same inode `186280548`. The script still hashes to the frozen SHA after this failure.

No `original_pid_gone`/`owner_cache_deleted` completion is recorded by the failed script, no fresh-session recovery/integrity/third COMMIT/stale-session denial is established, and no successful failover is inferred merely from eventual process absence. The summary is retained in `/tmp/peri-v1-joint-independent/first-actual-failure-summary.json`. This run counts as one failed actual attempt, not a passing partial case or a cancellation success.

Required correction scope is post-signal, bounded observation only: transient inability to read argv must never mean “PID gone”; before-signal identity failure must continue to refuse signaling, and changed UID/parent/start/argv identity must continue to refuse reused PID/cache deletion. A must add regression coverage and clearer signal/wait stage evidence. The correction must not retry uncertain SQL mutations, loosen the original recovery deadline, or suppress the original failure. Main plans to shut down this old fixture; any gateway exit 1 is not clean proof. The next attempt requires a newly specified exact private fixture, fresh source freeze/preflight, and independent complete evidence.

## Bounded wait correction and fresh release fixture

Main subsequently reports the old launcher actually terminates with exit 1, all old PIDs are gone, and the first failed cache remains intact. This is not a clean shutdown or successful first joint run. A first freezes the bounded-observation correction at `2829f60f…` for the old path, then changes only the exact fixture constant for the newly authorized deployment. The final 491-line source SHA256 is **`b071aa1745baafffd84fa30b1bff89ab84780c51ed9f00b6d3c927bbeb171717`**.

Independent source review confirms that unavailable/dying argv triggers a second metadata read: absent metadata yields `None`; unchanged metadata yields `PendingIdentity`, not process absence; changed identity fails closed. Immediately before signal, the original exact tuple is still mandatory, so pending identity refuses signaling. After signal, pending metadata must match the original, and the ten-second monotonic wait budget remains bounded by process-inspection timeouts. Cache deletion still requires process absence. The script now explicitly records `signal_dispatch`, `signal_sent_waiting_pid_gone`, `signal_sent`, and `waiting_uncertain`; only observed disappearance clears uncertainty. There is no mutation retry or false terminal-proof fallback.

This verifier actually runs all **7 self-tests PASS / 0 FAIL, 0.005s** on the new frozen source. Regression cases include dying argv with absent/unchanged/reused metadata, pending wait followed by absence/reuse/timeout, and the retained alias/private-path/symlink/no-retry safeguards. Evidence: `/tmp/peri-v1-joint-independent/new-fixture-self-test.log`. No real signal, cache deletion, SQL, or RPC is part of those tests.

Actual no-RPC preflight also passes with script bytes unchanged. The authorized root is `/private/tmp/peri-stress-joint-xs2gO5jO`; launcher PID `74688`, Authority PID `75163`, worker-a PID `75177` / cache-a inode `186322118`, worker-b PID `75178` / cache-b inode `186322119`, both on device `16777231`. UID/parent/exact argv/session leaders/private paths all match. The evidence is `new-fixture-preflight.json`, with the exact reviewed source copied as `frozen-new-fixture-script.py` in the independent evidence directory.

The actual native executable changes from the earlier debug deployment to `/tmp/peri-loom-p6-dev/prototypes/v1/target/release/peri-loom-v1`, SHA256 **`3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`**. Main reports unchanged Go production. This is a functional joint deployment variant, not a performance before/after measurement or an assertion that debug and release executable bytes are identical. Native ordinary test provenance remains B's completed joint suite; the new release runtime requires its own actual fault proof.

The independent verifier tells main there is no new readiness blocker for one execution on exactly this fixture with no concurrent fixture changes. Immediate Authority/identity/cache validation remains mandatory; only the owner selected by the actual authoritative binding may be signaled. The preflight PID snapshot is not an authorization to signal other PIDs or skip revalidation. Main then executes the complete fresh attempt, whose proof and source checks are recorded below.

Three additional independent mock-only guard probes pass: pending identity refuses a pre-signal kill with zero killer calls; pending identity refuses cache deletion before path access; a non-whitelisted inspection failure is not swallowed. Evidence is `pending-guards.json`, SHA256 `7641d9554e5571f2a3759b95af5247ed747e80efcc1403e05faad0e1aa58e835`. These are focused assertions, not extra full-suite or actual-signal counts.

## Actual fault oracle and completed proof

The frozen corrected script checks, on the authorized fixture only:

1. Create one uniquely isolated database and actually acknowledge commits for rows 1 and 2.
2. Acknowledge an insert of row 999 inside an uncommitted transaction, explicitly distinguishing statement success from durable COMMIT.
3. Match current authoritative writer binding, prove exact owner PID/cache, perform the authorized single SIGKILL, observe original PID gone, then delete only its proved cache.
4. Observe a different worker and strictly greater epoch through placement plus current Authority binding, without retries of uncertain mutations or fabricated optional-inspection success.
5. Open a fresh session, recover exactly rows 1 and 2 with no 999, pass native integrity, acknowledge a new commit for row 3, read rows 1/2/3, and explicitly close the fresh session.
6. Reject the old session with HTTP 409 / `session_moved`, without old-session close or retry. This is a gateway/session fence proof, not proof of a revived old native worker's physical write rejection.

Main's actual command is the frozen script's `--execute` mode with `--run-dir /private/tmp/peri-stress-joint-xs2gO5jO` and `--worker-binary /tmp/peri-loom-p6-dev/prototypes/v1/target/release/peri-loom-v1`. Main records actual exit 0; stdout is retained in `/tmp/peri-v1-joint-independent/release-actual-fault.log`, SHA256 **`8cf8f6801153fa33110d84628e6334fbaf433bb277e2820836d68c56d2395e77`**, containing `pass:true`, `stage:complete`, and the exact evidence path without credentials. No elapsed-time figure is invented where that log does not record one.

The private evidence is `/private/tmp/peri-stress-joint-xs2gO5jO/joint-s3-failover-a09d23ffe9e4cac9.json`, SHA256 **`ef444c47479494825d495dedef0006474190c9fb283161551363d226ebcc6751`**. An unchanged copy is retained as `/tmp/peri-v1-joint-independent/second-actual-evidence.json`. This verifier independently checks all twenty expected completion/scope fields against their expected values and reads the frozen code that requires exact rows and integrity before setting PASS.

| Actual recovered property | Inspected result |
| --- | --- |
| Selected owner and physical fault | Worker-b PID `75178`, signal sent, original PID gone, owner cache deleted |
| Takeover | Worker-a PID `75177`; epoch **2→3**, distinct owner, same root/branch incarnation, current Authority binding matches |
| Acknowledged durable recovery | Fresh native session reads exactly `[[1],[2]]`; uncommitted 999 absent |
| Native correctness and continued writes | Integrity `[["ok"]]`; new COMMIT acknowledged; exact final rows `[[1],[2],[3]]`; fresh session explicitly closed |
| Stale session | HTTP **409 / `session_moved`**; no old-session close or retry |
| Uncertainty and dispatch discipline | `waiting_uncertain:false`, `uncertain_operations_retried:false`; remaining TOCTOU is explicitly true, not hidden |

The verifier independently observes `ps -p 75178` exit 1, cache-b absent, and surviving cache-a still owned/private with its original inode `186322118`. It performs no kill, cache removal, HTTP mutation, S3 administrative action, or cleanup itself. Owner-run actual execution is not mislabeled verifier-run.

`joint-after.json` records script bytes equal to `frozen-new-fixture-script.py` at **`b071aa1745baafffd84fa30b1bff89ab84780c51ed9f00b6d3c927bbeb171717`**, unchanged release binary at **`3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`**, zero mismatches in the 171 native records and 118 accepted Go/module/embed records, and unchanged frozen P6 report SHA256. This is scoped immutable-source proof, not a claim that later pressure tooling or documentation made the entire root tree byte-identical. The observed attempt accounting is **one failed actual fault attempt followed by one successful fresh actual fault attempt**; the failed first run is not deleted from the verdict history.

## Conservative teardown, not clean success

After the successful oracle, main sends SIGTERM to launcher `74688`. Its actual exec session `91493` exits **1**, reporting `platform exited without a clean proof` and `worker-b exited before joined clean proof`. Main separately reports the Authority lock contains exact `clean\n` and all four owned child PIDs are gone. These distinguish native Authority cleanup from whole-platform/session teardown: a clean Authority marker or absence of PIDs does not convert launcher exit 1 into joined-clean success.

The fault deliberately preserves the stale old session rather than closing/retrying it to manufacture a clean result. Its conservative teardown limit is retained, not treated as a test cancellation pass or a recovery failure. The successful exact ACK/recovery/integrity/new-commit/409 oracle precedes teardown and remains a completed functional PASS. Accepted P6's separate healthy Linux deployment joined-clean proof remains separately attributed; it is not substituted for this fault-fixture exit.

Main is preparing a separate fresh healthy release fixture for potential performance work. At this report's freeze, no provisioning, pressure execution, throughput/latency/RSS measurement, or benchmark result from that future fixture is accepted or incorporated. Debug→release here is a deployment-profile change, not a measured optimization.

## Scope limits

This is functional joint correctness verification, not load, pressure, latency/throughput benchmarking, or a performance conclusion. It does not establish multi-Authority HA, automatic dirty-restart recovery, physical-I/O cancellation at deadline, a revived-worker native fence in this new scenario, native page-AEAD tamper rejection, engine-incremental streaming, all-metadata encryption, RSS caps, a deployed MinIO/Kubernetes cluster, or secure erasure of caches. Existing crash-pin and receipt-growth limits remain. No guessed review date is inferred from host-clock log timestamps.
