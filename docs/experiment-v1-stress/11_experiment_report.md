# R9 common13 — FAIL (raw UNRESOLVED), author failure record

- Scope: one authorized `r9common` provision and one common13 CLI; no retry/relabel/next stage/Linux/SDK/UI. Pressure STOPPED for F/main decision.
- Root before/after `f8642d7eb1b8eeecb8fb8e461a4f4179bd843ac4`; user CLAUDE.md dirty preserved. No source/spec/HEAD/cap changes, builds, Docker, bucket deletion, raw backend keys, or user-process signals by this runner.
- Fixture `/private/tmp/peri-stress-r9-owner-eoa8i69s`, verified-CA gateway19447; exact launcher72671/services identities checked. Accepted whole-smoke11 SHA matched; preflight TLS-only handshake, no added HTTP workload.
- OUTPUT `/private/tmp/peri-stress-r9-common-results-1r1aaikl`0700; private spec/gate/journal0600. Single Python3.12 provision `--execute --run-dir /private/tmp/peri-stress-r9-owner-eoa8i69s --output /private/tmp/peri-stress-r9-owner-eoa8i69s/stress-r9common.json --label r9common` exit0.
- Execution: Python3.12 `prototypes/v1/stress/run_guarded.py --spec /private/tmp/peri-stress-r9-common-results-1r1aaikl/operator-spec.json`; wrapper8978/CLI9001 naturally returned exit1; run `52cf42e22312b17d` UNRESOLVED, reason `operation_incomplete`.
- UTC run window 2026-10-07T18:02:52.184464Z–18:06:51.982258Z (2026-10-08 Asia/Shanghai); monotonic239.796381458s.
- Fixed common13 C1,2 W1s/S2s/M5s/rate25; rows1000/seed20261007/request10s; RSS536870912/generated268435456/artifact67108864 bytes. Functional-only record-confound; hard1200s/grace5s; guard interval/probe2s unchanged.

| Case | Actual status | Duration phases | Attempted=ACK |
|---|---|---:|---:|
| point_read | COMPLETE | 6 COMPLETE | 396 |
| index_range | COMPLETE | 6 COMPLETE | 394 |
| pagination | COMPLETE | 6 COMPLETE | 393 |
| mixed_oltp | COMPLETE | 6 COMPLETE | 142 |
| hot_keys | COMPLETE | 6 COMPLETE | 37 |
| bulk_tx | COMPLETE | 6 COMPLETE | 26 |
| sort_aggregate_join | COMPLETE | 6 COMPLETE | 391 |
| index_plan | COMPLETE | 6 COMPLETE | 388 |
| rollback | COMPLETE | 6 COMPLETE | 262 |
| constraint_error_reuse | COMPLETE; expected error/reuse check PASS | 0 | separate check |
| large_values | COMPLETE | 6 COMPLETE | 387 |
| sustained_wal | COMPLETE | 6 COMPLETE | 24 |
| checkpoint_reopen | UNRESOLVED after duration phases | 6 COMPLETE | 20 |

- Counts:12 case records COMPLETE/1 UNRESOLVED/no SKIP;72 duration phases COMPLETE; offered4800, attempted/completed2860=ACK2860, client admission drops1897/queue expired43. All2860 raw operations acked/correct with failure=null; phase unknown/cleanup_unknown/timeouts/correctness failures0. These phase zeros are NOT a global unknown0 conclusion.
- FIRST unexpected failure: POST `/v1/databases/checkpoint`, HTTP409, `class=unknown/code=operation_incomplete/unknown=true/execution_completed=false/may_have_committed=false/timed_out=false`; request_id `stress_52cf42e22312b17d_000000008453`.
- Checkpoint phase ACK/attempted: C1 warmup2/2, smoke2/2, measure6/6; C2 warmup2/2, smoke3/3, measure5/5. All six phase unknown/cleanup_unknown/timeouts/correctness0; subsequent lifecycle unknown1 makes this a FAIL, not rollback/cleanup success proof.
- Failure RPC window 2026-10-07T18:06:51.940977Z–18:06:51.982161Z; lifecycle checkpoint_reopen41.242958ms records same unknown event. Count one logical unknown, two raw records (RPC+lifecycle); no retry, blind close, later RPC or subsequent case after failure.
- Expected duplicate-primary-key HTTP400/sql_error in constraint_error_reuse is separately recorded; explicit rollback/balance/reuse checks PASS, not the unexpected checkpoint failure and not auto retry.
- Recorded oracles:preflight1+scenario36=37 PASS; all recorded expected/actual account hashes, ledger hashes and sums match. Last observed ledger331/account sum100000269 is before final measure/checkpoint failure; no final reopen oracle/final ledger audit, no inferred postfailure state.
- Last observed account hash `dc70472694bcba098870b51de16ba2d68ba3dd4e18f5a4a39066fa2f4dc5c31c`; ledger hash `d8fe29a8a978354a25497ec3a417af6c031acc598be83b95e97002d0cee1b3df`. F owns independent model/ledger verification.
- Guard120 samples/live118, max adjacent gap2.094256542s; first/end live coverage0.064143083s/0.000556083s; all live samples own-awake effective. No guard STOP/sample_error/signal refusal; child exited without pressure signal or forced termination.
- Wrapper final HOLD, child_exit1/child_alive=false/retained_pid=null/clean_join=false; journal CLI_exit natural_drain=true. Wrapper may_have_unknown=false describes its own termination path only: raw checkpoint unknown=true remains authoritative, not washed into success.
- Native drain NOT PROVEN: last pre-failure resource sample `operations.jsonl:8931` at18:06:51.864154Z still active_permits1; no afterfailure Native terminal sample. CLI exit/phase completion are not proof all Native calls drained. Unknown DB and fixture retained; no blind Close/reopen/delete.
- Cleanup108 records all failure=null; final `operations.jsonl:8937` at18:06:51.940949Z precedes failed checkpointRPC:8938. No postfailure cleanup proof. Read-ACK-shaped operation2323 records; last large_values read `operations.jsonl:7938` request...7522 has matching expected/actual rows hash; checkpoint workload20 ACK are writes, not reader ACK.
- Native logs under `/private/tmp/peri-stress-r9-owner-eoa8i69s/logs/`:worker-b.log:1 records only expected `native_sql_terminal_error/constraint/execute` request...6689 execution_completed=true; authority.log/platform.log/worker-a.log empty at inspection. No checkpoint...8453 terminal error in those logs; absent logging does not prove Native drain or rollback.
- External builders actually observed:esbuild55991 plus cargo9088/9645/9970/11384/16877/17887 and rustc9150/9698/10028/11471/16975/17950;159 confound events. No own builder detected; none killed. Author cannot attribute these external processes to another agent from comm-only samples.
- No performance PASS:performance_pass=false/confounded=true; accepted public C4 general3/owner1 and Worker128/control16/sql112 (general110/owner1/cleanup1) are a new regime, not paired legacy general4.
- Source/bin179 entries+3 artifacts overlap to181 unique paths:before/after/current SHA all match; HEAD unchanged and pre-report git status only CLAUDE.md dirty. Clock guard max absolute delta1414625ns;6792 run/phase/RPC/operation/lifecycle windows max discrepancy1412542ns, no >1s jump.
- CLI resource210 samples:service RSS max57606144 bytes; generated903147 bytes; wrapper sampled output peak8708195 bytes. Sampling is not hard OS quota enforcement/unobserved-peak proof.
- Own pre-provision caffeinate8871 and wrapper caffeinate8988 stopped only after exact PID/UID/start/ARGV proof, exit-15 each; no retained helper. Whole launcher72671 not signalled; no whole-fixture cleanup claimed. Historical R7/R8/prior RED failures unchanged.

## Frozen raw evidence (relative to OUTPUT)
- `operator-spec.json`: `fd5816de088beda255b69047e8f0d014d9696dc553171d81e7a1cdc2acfcf602`; `joint-pass-round9.json`: `cf2bbaad7e87cae6e0febeb3aca412949231465799956ccc87498bdb383b2836`.
- `gate-provenance.json`: `312bb109c1f027535a7c1d3446bb04132d610db739080cbeb15d3ce71c3641c0`; exact six-field gate and sidecar are prerequisites, not final mission/Linux/SDK/UI/pressure acceptance.
- Native `/private/tmp/peri-stress-native-writer-admission-bin`: `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`; Go `/private/tmp/peri-stress-platform-owner-rate-bin`: `e6c9dc3060646fe64b4c016f43de56f3b87dda2b88f186b06f11ef106e217af8`.
- CLI `/private/tmp/peri-stress-offer-stop-bin`: `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`.
- `run_52cf42e22312b17d/run.json`: `e7d56e114c3d8973a7fc3e1fcd88f990bfa833164b20d28d51e79d22bdd14eda` (111003 bytes).
- `run_52cf42e22312b17d/operations.jsonl`: `55f0983b524a478d1bbac3e7f2c7d12dc5ba7357af0a54f8cb39f03a91796d54` (8241514 bytes).
- `guarded-events.jsonl`: `969c8c690ca9283c6eab251a224dbae773bb2f7a748023b7ef204c2df6790d4b` (325507 bytes).
- Extra proof:author-preflight-before.json/author-execution-result.json/author-wrapper-start.json; author-provision/wrapper stdout/stderr; guarded-cli stdout/stderr. Evidence retained unchanged; no further action without main GO.
