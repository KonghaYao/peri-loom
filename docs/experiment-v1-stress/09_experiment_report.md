# R9 mixed — author execution record, independent F audit pending

- Scope: one authorized `r9mixed` provision and one mixed CLI; no architecture/common/SDK/UI/Linux/fault run. Pressure is STOPPED pending main GO and F.
- Actual UTC window: 2026-10-07T17:21:07.851091Z–17:22:43.807644Z (2026-10-08 local Asia/Shanghai); monotonic run duration 95.955864375s.
- Root HEAD before/after: `5ab1949aa5c348fa930e33b322bf83fef822e880`; existing user `CLAUDE.md` dirty preserved. No build, source/spec edits, commit, Docker, bucket deletion, raw backend keys, or user-process signals.
- Fixture: `/private/tmp/peri-stress-r9-owner-eoa8i69s`, verified-CA gateway19447; launcher72671 identity verified and retained live. Main/F whole-smoke11 PASS is prerequisite, not this author's new verification.
- OUTPUT: `/private/tmp/peri-stress-r9-mixed-results-p_9d2mzs` (0700); spec/journal/gate private0600.
- Provision actual exit0, label `r9mixed`, connection output `fixture/stress-r9mixed.json`; no retries/replacement labels.
- Invocation: Python3.12 `prototypes/v1/stress/run_guarded.py --spec /private/tmp/peri-stress-r9-mixed-results-p_9d2mzs/operator-spec.json`.
- Fixed mixed: C1,4; warmup2s/smoke10s/measure30s; rate50; rows1000; seed20261007; request timeout10s; RSS536870912/generated268435456/artifact67108864 bytes.
- Mode: `functional_only=true`, `external_builder_policy=record-confound`; guard interval2s/probe timeout2s; hard1200s/grace5s. No parameters changed.
- Wrapper exit0; owned CLI28152 natural exit0; run `dd9918641916a0f7` COMPLETE; all six phases COMPLETE. Clean join=true, retained PID=null, first STOP=null; no soft/hard pressure signal, may_have_unknown=false.

| C | Phase | Offered | Attempted/completed | ACK | Typed admission429 |
|---|---|---:|---:|---:|---:|
| 1 | warmup | 100 | 26 | 26 | 0 |
| 1 | smoke | 500 | 78 | 78 | 0 |
| 1 | measure | 1500 | 186 | 186 | 0 |
| 4 | warmup | 100 | 35 | 11 | 24 |
| 4 | smoke | 500 | 176 | 52 | 124 |
| 4 | measure | 1500 | 488 | 110 | 378 |

- Totals: offered4200, attempted/completed989, ACK463 (134 writes/329 reads), typed `admission/quota_exceeded` HTTP429=526; client admission drops3201 and queue expired10. These are not unknown commits or SQL retries.
- All phases: unknown0, cleanup_unknown0, timeouts0, correctness_failures0. Raw operations989 agree with phase totals; 15 cleanup records all failure=null.
- Recorded preflight oracle PASS (4 ledger writes), three scenario oracles PASS; final ledger138 =134 workload write ACK+4 preflight. Final account sum99999972 and account/ledger hashes match recorded model; independent ledger audit belongs to F.
- Final account hash `5ed7e1fa823029a1d159d24ce067fb9f95fea9ffade204f88345cf2d0b0dfa2a`; ledger hash `8e7905f988bd1e35b744db2e8c7731983cbe4adc90e78b9287eefb868e37f936`.
- Guard journal: 50 samples, 48 during CLI lifetime; maximum adjacent sample gap2.109828459s; first/last live sample coverage gaps0.062657250s/0.000426708s. All live samples own-awake effective; no sample_error/STOP/signal refusal/retained-child events.
- Clock: guard max absolute delta697417ns; 2135 run/phase/RPC/workflow windows max absolute wall-minus-duration688625ns; no >1s clock discrepancy. No sleep asserted from OS log metadata.
- External esbuild55991 UID501/start2026-10-01 was recorded50 times (CPU0.0%); never killed. Wrapper final `FUNCTIONAL_ONLY_CONFOUNDED`, performance_pass=false: not a performance PASS or paired improvement.
- Profile regime: public C4 general3/owner1; Worker128/control16/sql112 split general110/owner1/cleanup1, not legacy all-general4. Source gate=accepted fixed source/bin manifest; first/last raw native worker `writer_profile=true` and project `writer_admission=true`; observed profile booleans do not independently measure numeric capacities.
- Readonly provenance: spec179 source entries+3 artifacts overlap to181 unique paths; author-before, wrapper-after and final current hashes all match; no source/bin drift. Final pre-report git status still only user CLAUDE.md dirty.
- Sampled service RSS max55115776 bytes (72 CLI resource samples); generated request bytes346984; wrapper sampled output peak3281546 bytes. Sampling is not a hard OS quota or proof of unobserved peaks.
- Own pre-provision caffeinate27968 and wrapper caffeinate28141 stopped only after exact PID/UID/start/ARGV proof; each exit-15, no retained helper. Main launcher72671 remains live; no whole-fixture cleanup claimed.
- Historical R8 external-guard failure/monitor gap and prior hard-join RED remain unchanged; this run has no wrapper failure. R7 historical FAIL is not superseded or washed.

## Frozen evidence (all paths under OUTPUT unless stated)
- `operator-spec.json`: `095ad027a6ee6b17b4e2726bfde9df5eb0412ec89a172d0170525890732500f4`.
- `joint-pass-round9.json`: `c1512c8d649fd0d539dac7a8e57ccc453c57888e67e9a57440f1dfc5e4f34603`; exact six fields, historical P0–P6/runtime scope only.
- `gate-provenance.json`: `f0a27d36ede90119a34fb0828bdae5560d3f4e18e7d702612427a395246e0310`; not final mission/performance/SDK/UI/Linux acceptance.
- Native `/private/tmp/peri-stress-native-writer-admission-bin`: `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`.
- Go `/private/tmp/peri-stress-platform-owner-rate-bin`: `e6c9dc3060646fe64b4c016f43de56f3b87dda2b88f186b06f11ef106e217af8`.
- CLI `/private/tmp/peri-stress-offer-stop-bin`: `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`.
- `run_dd9918641916a0f7/run.json`: `d5cb75914175e5f1b9682203695210f26341f13af36b522c6e59f43eabfe491f` (12726 bytes).
- `run_dd9918641916a0f7/operations.jsonl`: `b034a91b6ed4fbc26d3a4dc454e3f5e9eeb0dfedd1dd25ce0cbe7d67a7ec4f84` (3081116 bytes).
- `guarded-events.jsonl`: `76aa9a16a4967e0c1a55ef3c2546240cceed3fb4d540a4509a242edb5e6c8307` (156716 bytes).
- Owner/awake/CA/provision provenance: `author-preflight-before.json`, `author-execution-result.json`, `author-wrapper-start.json`, `author-provision.stdout/stderr`, `author-wrapper.stdout/stderr`, `guarded-cli.stdout/stderr`.
- Author records evidence only; F independently verifies raw correctness/guard/provenance. No next matrix without explicit main GO.
