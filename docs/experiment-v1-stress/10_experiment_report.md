# R9 architecture — author execution record, independent F audit pending

- Scope: one `r9arch` provision and one architecture-only CLI. Common NOTRUN; pressure STOPPED pending F/main. Author is runner, not independent verifier.
- UTC actual window: 2026-10-07T17:38:43.717564Z–17:41:55.468218Z (2026-10-08 Asia/Shanghai); monotonic run191.749277542s.
- Root before/after `4fa6923977fb4e44d6f96218ef4c807c12a9f1ff`; user CLAUDE.md dirty preserved. No source/spec/HEAD/cap changes, build, Docker, bucket deletion, raw backend keys, or user-process signals.
- Fixture `/private/tmp/peri-stress-r9-owner-eoa8i69s`, proper-CA gateway19447, launcher72671 exact identity verified and retained live. Main/F whole-smoke11 SHA matched before run; TLS-only preflight added no HTTP workload.
- OUTPUT `/private/tmp/peri-stress-r9-architecture-results-hbvsofq9`0700; spec/gate/journal0600. Provision single execution exit0: Python3.12 `prototypes/v1/stress/provision.py --execute --run-dir /private/tmp/peri-stress-r9-owner-eoa8i69s --output /private/tmp/peri-stress-r9-owner-eoa8i69s/stress-r9arch.json --label r9arch`.
- Wrapper: Python3.12 `prototypes/v1/stress/run_guarded.py --spec /private/tmp/peri-stress-r9-architecture-results-hbvsofq9/operator-spec.json`; wrapper61464/CLI61488 both exit0, natural clean join, no retained child/unknown/softstop/hardstop/first STOP.
- Fixed architecture6 C1,2; warmup2s/smoke10s/measure10s/rate50; rows1000/seed20261007/request10s; RSS536870912/generated268435456/artifact67108864 bytes. Functional-only record-confound, hard1200s/grace5s, guard interval/probe2s.
- Run `fd95f5cd5a18a970` COMPLETE. Six scenario records COMPLETE does not mean skipped subconfiguration PASS; lifecycle scenarios do not use duration phases.

| Scenario | Executed duration phases | Attempted/ACK | Recorded scenario oracles | Explicit skip |
|---|---:|---:|---:|---|
| session_cycles | 0; lifecycle1 | lifecycle failure=null | 1 PASS | none |
| many_db | 6 COMPLETE | 1665/1665 | 12 PASS | none |
| noisy_tenant | 9 COMPLETE | 2891/2891 | 6 PASS | C1 SKIP: combined interference needs two independent sessions |
| saturation_admission | 6 COMPLETE | 1661/1661 | 3 PASS | none |
| idle_suspend_resume | 0; lifecycle1 | lifecycle failure=null | 1 PASS | none |
| graceful_migrate | 0; lifecycle1 | lifecycle failure=null | 1 PASS | none; migrate RPC actual HTTP200 |

- Totals: 21 executed duration phases COMPLETE plus one explicit SKIP; offered7700, attempted/completed6217=ACK6217, admission negatives0; client admission drops1473/queue expired10. Raw operation6217 all acked=true/correct=true/failure=null.
- Unknown/cleanup_unknown/timeouts/correctness failures all0 in executed phases; three lifecycle failure=null and36 cleanup failure=null. No automatic SQL retry or replacement label.
- Five preflight oracles PASS with20 write ACK (main/many_1/many_2/many_3/noisy each4); workload write ACK0. All recorded scenario ledger_rows=4;24 scenario oracles PASS, expected/actual account and ledger hashes/sums match. F owns independent ledger/model audit; no new audit SQL/API issued by author.
- session_cycles/idle reopen explicitly `native_empty_cache_proven=false`; not empty-cache/cold-open evidence. Graceful migration source snapshot recorded before actual migrate; no target-already-owns SKIP this run.
- Guard96 samples,94 during live CLI, max adjacent gap2.073867750s; start/end live coverage gaps0.058664375s/0.000320417s. All live samples have own-awake effective; no sample_error, STOP, refusal, or retained-child events.
- Clock guard max absolute delta1377583ns;13047 run/phase/RPC/workflow windows max absolute wall-minus-duration1376458ns. No >1s discrepancy; no sleep inferred from OS log metadata.
- External esbuild55991 recorded96 times under explicit policy, not killed; final `FUNCTIONAL_ONLY_CONFOUNDED`, performance_pass=false. No own builder found; not a performance PASS or paired improvement.
- Source/bin gate before/after/current:179 source entries+3 artifacts overlap to181 unique paths, all SHA matching; HEAD unchanged, pre-report git status only existing CLAUDE.md dirty. Accepted public C4 general3/owner1 and Worker128/control16/sql112 (general110/owner1/cleanup1) regime is not legacy general4.
- Resource185 CLI samples: max observed service RSS119029760 bytes; generated1752399 bytes; wrapper sampled output peak11305157 bytes. Samples are not hard OS quotas or proof of unobserved peaks.
- Own pre-provision caffeinate61354 and wrapper caffeinate61477 stopped after exact PID/UID/start/ARGV proof, both exit-15; no retained helper. Whole launcher72671 untouched/live; no whole-fixture cleanup claim.
- Historical R7 FAIL/R8 wrapper guard failure and hard-join RED unchanged. Mixed independent ACCEPT is prior-stage evidence, not an architecture independent verdict.

## Frozen raw provenance (relative paths under OUTPUT)
- `operator-spec.json` SHA256 `2ddd09bed57154543f78798bff79cda24bc08df32c854e455b20b4ca1b655916`.
- `joint-pass-round9.json` SHA256 `6d425261919725c2014b8e78250ee02335108a63f01c28324ec5a1eb1652c516`; exact six fields, not final mission scope.
- `gate-provenance.json` SHA256 `398560be5ace70fc408e3cb90b2b5a7f4312d5731ce2af87bf3a1e36482b0ab9`; historical/current runtime prerequisite, not pressure/Linux/SDK/UI performance acceptance.
- Native `/private/tmp/peri-stress-native-writer-admission-bin` SHA256 `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`.
- Go `/private/tmp/peri-stress-platform-owner-rate-bin` SHA256 `e6c9dc3060646fe64b4c016f43de56f3b87dda2b88f186b06f11ef106e217af8`; CLI `/private/tmp/peri-stress-offer-stop-bin` SHA256 `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`.
- `run_fd95f5cd5a18a970/run.json` SHA256 `5f2b1b7de7adcc1113c8eab5fd3e4a47ec5e1c3a2cd1b3eb5830c02a4fa03f16` (43753 bytes).
- `run_fd95f5cd5a18a970/operations.jsonl` SHA256 `7fe6a90c70e3ea3de8609d47ca0fdf5991062b47678e2a2d3275a2383b696208` (10977737 bytes).
- `guarded-events.jsonl` SHA256 `e500ea90c1cf85b7b68080e21a1633c5710805b03112cfb8c65a04698527bafc` (252913 bytes).
- Additional owner/CA/provision proof: `author-preflight-before.json`, `author-execution-result.json`, `author-wrapper-start.json`, `author-provision.stdout/stderr`, `author-wrapper.stdout/stderr`, `guarded-cli.stdout/stderr`.
- No common or next matrix until independent F review and explicit main GO.
