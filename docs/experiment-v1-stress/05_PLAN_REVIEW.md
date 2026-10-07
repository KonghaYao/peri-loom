# Round 5 predeclaration static review

Independent reviewer: Fermat, 2026-10-07. **Predeclared scope accepted after the mandatory evidence-gate clarifications. PLAN ONLY: no execution authorization, observed awake guarantee, native-cause finding or performance PASS.**

## Reviewed frozen identities

- E's `04_experiment_report.md`, 143 lines: SHA256 `1573d2d6317963fefd342d7a2bc3ee479ea6595fb1e9fca890073e0a497acadc`.
- F's unchanged `04_control_verification.md`, 75 lines: SHA256 `058a8c8013325818246baf96d4e37ec0a66b97a1abad6699d63f995b1755cf87`.
- Final `05_PLAN.md`, 141 lines: SHA256 **`e26efad821d724dcb04773de150d863a02849d4123db857e6dce4e66f47ea433`**. This supersedes the initial `2437ae…` and intermediate `df397a…` versions; only the final identity is accepted here.

The author Round 4 report agrees with the independent raw verification: actual control exit 1/FAIL, first Query `…4534` at C1 measure, 489 read ACKs/490 attempts, zero unknown/cleanup unknown, only preflight and post-smoke four-entry oracles, incomplete measure and no paired result. Its resource maxima, charged request bytes and evidence hashes also match raw records. Confirmed OS Idle Sleep/DarkWake and the approximately 256.594-second wall/monotonic discrepancy are not mislabeled as ordinary elapsed time or proof of a particular native error. Later maintenance sleep is outside that runner. Main's normal launcher termination is executor-reported, not an independently observed whole-resource cleanup proof.

## Mandatory symmetric awake evidence

Both fresh arms must use the same main-owned template, `caffeinate -i -t 3600 -w "$LAUNCHER_PID"`, bound to their own exact launcher PID/start identity. No user `pmset` configuration, lease TTL, SQL/RPC deadline, budget, fencing rule, TLS setting or quota is relaxed. Local installed `caffeinate(8)` documentation supports idle-sleep inhibition, a timeout and release on watched-process exit; this review did not launch that utility or test its live effectiveness. The template does not guarantee protection against forced sleep, lid closure or every power event.

The initially vague process-liveness check was tightened before acceptance. **Each arm startup and each runner preflight must capture a safe readonly `pmset -g assertions` subset proving an actual `PreventUserIdleSystemSleep` assertion owned by the exact main-owned caffeinate PID**, with observed timestamp and PID/start identity/exact-launcher binding. Aggregate assertion count, another process's assertion, a live PID or the command arguments alone are insufficient. Missing/type/owner/identity mismatch, expired window or lost launcher binding means STOP in either arm. Main records the real assertion lifecycle; E does not restart assertions, signal processes or alter OS settings.

Safe evidence excludes unrelated user process details, scheduled wake requests and secrets. Main's actual assertion proof and lifecycle are still **pending future execution**; accepting this text does not attest that an assertion currently exists.

## Immediate timing and outcome gate

After **each runner returns**, wrapper/main must audit that run, every phase and every RPC's observed wall interval against its original monotonic elapsed, together with safe OS events and actual awake evidence. This must pass **before the next label provision, runner or arm**; it cannot be deferred until the final author/F report.

Wall reversal, invalid elapsed, absolute wall-minus-monotonic difference **greater than one second**, confirmed suspend even below that threshold, or awake-identity failure invalidates the affected performance window and stops the remaining sequence. Unexpected SQL failure, unknown, non-COMPLETE outcomes, oracle failure and resource/artifact risk likewise STOP without retry or label substitution. One second is an operational screening threshold, not proof of clock synchronization or absence of shorter pauses. Wall observations are never substituted as throughput denominators or lease clocks.

## Comparison and safety boundaries

The final plan keeps the same Go `8692ec…` and drain harness `9aa86b…` in both arms, with preserved old native `3c67f7…` versus the frozen BUSY+View candidate. Main's future gate/provenance must fix the candidate's exact existing executable path/SHA; no source-derived binary identity or automatic rebuild is accepted. Candidate effects remain those of the entire bundle, not isolated View/BUSY attribution. Only complete, oracle-backed 30-second sat/point windows are paired; control mixed is explicitly out of scope, candidate mixed is new work, and architecture/SQL short phases are functional gates only.

Fresh fixtures/buckets/labels/business IDs, equal sat→point history and symmetric awake checks are required. The new seven-runner matrix is not a replay of Round 4 SQL or retroactive repair of its evidence. The approved bounded-drain contract still retains the initiating failure and each real unknown/poison outcome while other already admitted workflows can finish under unchanged finite deadlines; parent/resource/artifact hard cancellation remains. Unsupported snapshot-under-load and other declared SKIPs remain explicit noncoverage, not PASS. No disappearance of future sleep events would prove the original native `LeaseExpired` hypothesis.

A fixed-order single pair without repeats/randomization permits descriptive, appropriately qualified comparisons, not maximum capacity or strong isolated causal/statistical claims. Commit, matching-content operator attestation, actual main-owned awake proof, fresh startup/credentials and explicit per-arm runner authorization remain prerequisites. The gate is ordinary private JSON operator attestation, not a cryptographic signature proof.

This review performed only local reads and wrote this new report. No build/test, API/SQL, provisioning, load, power assertion or signal was executed. Round 4 reports/raw and the prior gate reports remain unchanged.
