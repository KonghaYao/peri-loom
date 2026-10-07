# PhaseDrain independent verification

Verifier: Fermat, 2026-10-07. **Safety/unit gate PASS for the explicitly approved bounded-drain contract; no actual load or performance result.** The native busy adapter is separately assessed in `BUSY-WAIT-VERIFICATION.md`.

## Approved semantic change

This is broader than a known-SQL-error fix. Main explicitly approved that a real unknown request keeps its unknown/poison classification and makes the phase fail or remain UNRESOLVED, while **other already logically admitted workflows may finish naturally under their original finite RPC deadlines and parent safety caps**. It is no longer correct to say that every unknown must hard-cancel its peers.

Known non-admission SQL failure, unknown, or a successful-but-incorrect read stops new offers/dequeue/logical admission. Already admitted peers can yield genuine ACK, typed error or unknown. No success of those peers turns the failed phase into COMPLETE. Lost COMMIT is not replayed or declared uncommitted; poisoned sessions are not rolled back, reopened or blindly closed. Parent cancellation and resource/artifact safety failures still hard-cancel.

Static review confirms `phaseDispatch.admit` and `stopOffers` serialize on the same mutex. Admission linearizes at the successful check, not at TCP dispatch: a call admitted before stop may reach the transport afterward and belongs to the permitted drain. A queued/prepared job checking admission after stop is rejected. Scheduler admission is checked too; any previously admitted offer not executed remains accounting residual rather than a new SQL attempt. No new logical admission can succeed after the serialized stop.

`phaseSQL` derives one finite `WithTimeout` context for each original SQL RPC from the RPC/parent context, not the canceled offer context. Settings reject nonpositive timeouts and values over 30 seconds. Drain neither recreates a failed call nor extends an in-flight deadline. An original rollback workflow's follow-up check belongs to that already admitted workflow, not a new business offer. Resource monitoring's existing parent cancellation remains unchanged; artifact failures cancel that same RPC context. Cleanup retains its existing five-second limit, and poison prevents an actual Close RPC.

## Frozen source and actual gates

Only these three E-owned files changed in this fix:

| ROOT path | SHA256 |
| --- | --- |
| `prototypes/v1/stress/phase.go` | `04b38f6c3c90f0e0b73f5824dfbe340c6f9757d76c5b62587062c1d53d35d5a6` |
| `prototypes/v1/stress/phase_dispatch.go` | `772bf016e702afcf13c86581ef31f90dd1dd0f1f0faa6db19f1b97119237a5d4` |
| `prototypes/v1/stress/phase_drain_test.go` | `96bfee38f07563732c674b9f830585778e843638d262bedc0f4622b7125336e5` |

Independent full parent-module `go test -count=1 -json ./...` and `go test -race -count=1 -json ./...` each pass **40 roots + 24 named subcases**, zero skips/failures. Vet and CLI build exit 0; formatting reports no differences. Each owned file is below 500 lines. Source before/after hashes match the author freeze. Commands used explicit umask 022. Logs, status, source identity and artifacts: `/tmp/peri-phase-drain-f/{normal.jsonl,race.jsonl,normal.stderr,race.stderr,vet.log,fmt.log,build.log,status.log,source-before.sha256,source-after.sha256,binaries.sha256}`.

The six new tests use an in-memory HTTP RoundTripper, not a live fixture. They verify:

- A typed first SQL error remains FAIL while two already admitted peers actually ACK and only their two mutations enter the ledger.
- A genuinely lost COMMIT remains unknown/poison with no replay or Close; two other admitted peers can ACK under the newly approved contract.
- Hard parent cancellation yields three unknown outcomes, no ACK ledger entries and no Close of poisoned sessions.
- A 40 ms original deadline still expires draining peers; no one-second extension or queued replacement work is allowed.
- Artifact failure retains hard cancellation and stops subsequent dispatch.
- A wrong read result remains a confirmed correctness failure even when peers drain; transport ACK and correctness PASS remain distinct.

Test transport invokes `GotConn` to mark dispatched requests, uses unique request IDs and asserts attempted counts, Close counts and ACK ledger membership. These deterministic tests establish adapter behavior, not cancellation guarantees or durable database state on a live network. The retained pre-fix RED evidence `/tmp/peri-phase-drain-red.log` shows the old path manufactured two canceled-peer unknowns; it is not deleted or retroactively substituted for Round 3 evidence.

## Runnable artifact and provenance

New host arm64 harness: `/tmp/peri-stress-phase-drain-bin`, SHA256 **`9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`**. The old `/tmp/peri-stress-correlation` remains **`c186fd905c34f9d68d2f899e162fdae2bc0539e1a290fffa0cf64341b0e54886`**, untouched; old raw results retain that identity. The new binary embeds pre-commit HEAD `175ee0429244ab41d196354a729553f7797b9a73`, `vcs.modified=true`; it is a frozen-content working-tree build, not a clean-HEAD build. Main must bind subsequent commits/gate to the matching runtime source content.

The unified current gate excludes C/James's actively developing nested `stress/faultprobe` scope: 19 pre-gate files were excluded, and that sidecar is **not independently accepted by this report**. Go's separate nested module is not included in parent `./...`. The remaining 300-file gate source snapshot is unchanged before/after; SHA256 `073db37e76bba86da5c7ea3d5035db50451d632182e15dce0e37dbf68aa3ccad`, `/tmp/peri-native-busy-f/gated-source-{before,after}.json`. Exclusion is not a PASS for any new faultprobe implementation.

## Historical evidence and next-plan limits

Independent hash checking matches all 14 historical raw/report files in `/tmp/peri-phase-drain-forensics-before.json`, including both Round 3 reports. The two historical mixed unknown requests remain unknown; no counterfactual unit behavior reclassifies them. The initiating HTTP SQL error still has no captured native error body. Whole baseline shutdown also remains executor-reported exit 1 without a clean platform proof; exact authority-lock cleanup is not whole-system clean shutdown.

Readonly static review of final `04_PLAN.md` at SHA256 `22515f4de8a641b4b84c1bc1ab7a56bcc3cf4f9a094946620c25979f3dab944f` accepts it **only as a predeclared plan**, not execution authorization. Its gate is a main-confirmed matching-HEAD operator attestation in ordinary private JSON, not a cryptographic signature proof; the final wording correction did not change runtime/drain scope. It requires the same new drain harness/Go in both arms, fresh equivalent fixtures and sat→point history, records BUSY+View as a composite candidate, excludes control mixed explicitly, separates short architecture/SQL gates from full 30-second observations, preserves unsupported SKIPs and all failure/unknown STOP rules, and requires durable oracles. A fixed-order single pair without repetitions/randomization permits descriptive paired observations only; it does not establish isolated View benefit, maximum capacity or strong causal/statistical performance claims. Any later plan revision requires checking its new identity.

All verifier build/test processes for this gate finished before handing artifacts to main. No real load, provisioning, fixture API, SQL replay or failure-injection signal was executed for this PhaseDrain verification.
