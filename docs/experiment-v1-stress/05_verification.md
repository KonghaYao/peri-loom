# Round 5 independent verification

Date: 2026-10-07. Verifier: F (Fermat). Scope: readonly review after all E runners stopped; no API/SQL, provisioning, signals, tests, builds or load were executed by this verifier.

## Verdict and provenance

**Evidence/method verification PASS within the recorded scope; whole Round 5 PARTIAL, with candidate mixed FAIL. No blanket system/performance PASS.** Four common sat/point runners actually completed with correctness oracles. Candidate mixed stopped on a real typed SQL error. Candidate architecture and SQL runners were not provisioned/run; their coverage is NOT RUN, not PASS. Control mixed was outside the declared matrix, not a skipped successful gate. Earlier failures and unknown classifications remain unchanged.

The accepted predeclaration is `05_PLAN.md`, SHA256 `e26efad821d724dcb04773de150d863a02849d4123db857e6dce4e66f47ea433`. Both arms used fresh fixtures and label-specific models, rows 1000, seed 20261007, warmup/smoke/measure 2/10/30 seconds, unchanged 10-second RPC deadlines and bounded drain. Both used the same Go platform/header harness and main-owned idle-sleep assertion template. The matching gate records HEAD `3dad8b668b5a60b5582a7639ecffebabcd0b314b`; it is an operator JSON attestation, not a cryptographic signature.

Wrapper provenance consistently records that HEAD with only user `CLAUDE.md` dirty. This does not make the executables clean-HEAD builds: Go/drain binaries retain their earlier precommit VCS/dirty metadata and accepted content identity. Control deliberately uses the preserved historical native, not native built from this HEAD. Current A diagnostic development is not part of any measured artifact or retrospectively frozen source proof.

Executable SHA256 values were independently read and matched recorded provenance:

- Control native `/tmp/peri-stress-native-baseline-bin`: `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`.
- Measured candidate native `/tmp/peri-stress-native-busy-bin`: `868962e6896fa166c1429aa149812955e9740e690118609ebff20d2f0ccc89b4`.
- Shared Go `/tmp/peri-stress-platform-correlation-bin`: `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d`.
- Shared drain harness `/tmp/peri-stress-phase-drain-bin`: `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`.

## Raw identity and independent counts

Evidence root: `/private/tmp/peri-stress-results-h3wtrdm2`. Below, operations are logical SQL workflows, not all HTTP requests. Admission negatives remain typed negatives, not ACKs or unknowns.

| Arm/case | Run ID | Actual exit/status | Attempted / ACK | Admission negatives | Cleanup records | Preflight + later oracles |
|---|---|---|---:|---:|---:|---:|
| Control sat | a66d3da894f7b624 | 0 / COMPLETE | 6526 / 5562 | 964 | 45 | 1 + 5 |
| Control point | d70a2475ab0b0c7e | 0 / COMPLETE | 3115 / 3115 | 0 | 15 | 1 + 3 |
| Candidate sat | 3ae63e17d9c8d1aa | 0 / COMPLETE | 7130 / 6327 | 803 | 45 | 1 + 5 |
| Candidate point | 9a56c88b23e870b2 | 0 / COMPLETE | 3303 / 3303 | 0 | 15 | 1 + 3 |
| Candidate mixed | 32f0ba1ea724d157 | 1 / FAIL, sql_error | 383 / 382 | 0 | 11 | 1 + 1 |

Every recorded cleanup above has no failure. Every phase reports unknown, cleanup-unknown, timeout and correctness-failure counts zero. The mixed SQL negative is still a real failure; zero unknown does not convert it to success. All logical business IDs and nonempty gateway RPC IDs are unique within each run, and every operation's request references exist in RPC evidence. No failed business operation is recorded as replayed. This is evidence plus the previously accepted no-replay harness contract, not an unrecorded network trace proving every internal action.

I independently partitioned JSONL operations by phase order, reconciled attempts/ACK/error classes and offered = attempted + client drop + queue expiry, recomputed nearest-rank all/ACK/queue latency quantiles and ACK rates, and matched summaries. All ACK read records with expected hashes match their actual hashes. Failed/non-ACK rows were not counted as corruption or success.

I also reconstructed the seeded 1000-account model and sorted acknowledged ledger rows, then independently matched canonical JSON SHA256, account sums and ledger lengths against all recorded oracle results. Each sat/point run has only four preflight ledger mutations; its later oracles confirm the same four-entry ledger and account sum 100000004. Those read workloads do not prove concurrent write durability. Mixed has 117 acknowledged workload ledger mutations plus four preflight entries: **121 known ACK ledger entries**, but its only later oracle covers **43 entries after C1 smoke** (account sum 99999998). The other 78 known ACK entries lack a later full ledger/account readback. There is no post-C1-measure, post-C4-smoke or final mixed oracle.

## Mixed first failure and natural drain

The first and only recorded SQL failure is **Execute**, request `stress_32f0ba1ea724d157_000000002821`, business operation `stress_32f0ba1ea724d157_000000002795`, slot 1, C4 smoke. Its RPC starts `2026-10-07T04:20:19.782468Z`, ends `04:20:20.819446Z`, elapsed **1036979125 ns**, HTTP **400**, response **173 bytes**. The retained failure is `sql_error`, `execution_completed=true`, `may_have_committed=true`, `unknown=false`, `timed_out=false`. HTTP 321 is not the observed status. The native error body/cause is not captured here; neither Busy nor lease expiry is established by this result or its approximately one-second duration. A completed error with may-have-committed does not prove the failed transaction had no effects.

Three already admitted peers finish naturally after that negative: requests ending **2847**, **2852**, **2850**, at `04:20:20.858533Z`, `.878668Z`, `.893550Z` respectively. All started before the failure completed and have ACK/correct=true; 2847 contributes one acknowledged mutation. No induced peer unknown appears. All eleven phase session cleanups finish normally. This demonstrates this known-error drain trajectory, not actual coverage of the separately approved trueunknown-drain branch, parent/resource hard cancellation or poisoned-session cleanup.

C1 warmup/smoke/measure have 31/92/216 ACKs and COMPLETE status. C4 warmup has 15 ACKs and COMPLETE status. C4 smoke stops after **4811033083 ns**, 237 offers, 29 attempts, 28 ACKs, one SQL error, 204 client drops and four queue expiries. C4 measure is absent. Wrapper records `runner_noncomplete_STOP`; no architecture/SQL follow-on provision or runner evidence exists. This failure is retained independently of future diagnostics or fresh authorized reruns. Round 3's two canceled unknowns are not reclassified from this newer drain result.

## Common complete measurements

All rows below use actual complete measure windows and the same configured offers: sat 200/s, 6000 offers/window; point 50/s, 1500 offers/window. ACK/s includes measured drain time. Latencies are successful HTTP workflow duration, **excluding client queue time**, in milliseconds; the separately recomputed queue quantiles are retained in raw summaries.

| Case/C | Control attempts/ACK | Candidate attempts/ACK | Control → candidate ACK/s | Control p50/p95/p99 ms | Candidate p50/p95/p99 ms |
|---|---:|---:|---:|---|---|
| Sat/1 | 739/739 | 1061/1061 | 24.613 → 35.359 | 38.925/54.270/67.059 | 26.950/41.835/53.696 |
| Sat/2 | 1281/1281 | 1450/1450 | 42.637 → 48.283 | 41.031/77.018/115.390 | 39.109/60.093/73.821 |
| Sat/4 | 1244/1244 | 1403/1403 | 41.324 → 46.606 | 79.002/170.641/227.010 | 80.284/118.293/159.908 |
| Sat/8 | 1397/691 | 1097/549 | 22.928 → 18.210 | 156.844/219.370/280.146 | 188.983/322.882/453.131 |
| Point/1 | 757/757 | 1002/1002 | 25.225 → 33.357 | 38.061/53.980/64.793 | 28.020/46.631/56.054 |
| Point/4 | 1447/1447 | 1333/1333 | 48.212 → 44.323 | 46.744/108.144/126.723 | 79.841/128.272/158.799 |

C8 sat measure includes 706 control and 548 candidate admission negatives; they are not successes, SQL failures or unknowns. Candidate ACK-rate differences are +43.66%, +13.24%, +12.78%, **-20.58%** for sat C1/2/4/8 and +32.24%, **-8.07%** for point C1/4. Do not hide the regressions. These are descriptive single fixed-order paired samples under nonisolated background load, not replicated effect estimates, maximum capacity, a universal win, or isolated OwnershipView/BUSY benefit. The native candidate changes the entire accepted BUSY+View bundle.

## Awake, timing, resources and confounds

Both arms have actual recorded `PreventUserIdleSystemSleep` owner lines, not just process liveness: control caffeinate **58764** bound to launcher **53783**, candidate caffeinate **17150** bound to launcher **9721**. Before/after and periodic records agree on PID/start identity, exact `-i -t 3600 -w <launcher>` argv, owner assertion and bounded age. Before/after records bracket each runner. Each recorded OS window has no selected sleep events; no OS setting, lease or deadline relaxation is evidenced. This sampled evidence does not guarantee absence of all forced sleep or shorter unsampled interference.

| Runner | Independently checked run/phase/RPC/workflow windows | Max absolute wall−monotonic gap | Awake periodic samples | Background compiler max summed ps CPU | Sampled owned process RSS max |
|---|---:|---:|---:|---:|---:|
| Control sat | 13391 | 85.416 µs | 18 | 238.9% | 42205184 bytes |
| Control point | 6403 | 42.709 µs | 9 | 27.4% | 43302912 bytes |
| Candidate sat | 14596 | 313.208 µs | 18 | **271.4%** | 39124992 bytes |
| Candidate point | 6779 | 53.583 µs | 9 | 96.4% | 42237952 bytes |
| Candidate mixed | 875 | 30.250 µs | 6 | 0% | 42762240 bytes |

Timing recomputation matches each saved post-run audit; no window exceeds the declared one-second threshold. Completion/audit records precede the next label's provenance, and mixed ends in STOP. R4's confirmed sleep and large gap are not present in this recorded R5 evidence; that does not prove the specific original native error. Go monotonic durations are throughput denominators, not a claim that those intervals use suspend-inclusive lease time.

Compiler values are sums from recorded background samples, not instantaneous CPU occupancy. Candidate sat's 271.4% sample at `04:15:57.203524Z` is inside its run; **228.8% is not the maximum of this summed recorded field**. Other interactive/VM/server activity also remains a confound; zero compiler activity during mixed is not host isolation. Background operational limits and low sampled owned RSS do not establish unsampled peak RSS, total host-memory isolation or cache-accounting equivalence.

Recorded charged request bytes are 1336181/695295/1447867/730061/204150 in table order, below the 256 MiB generated-request cap; this metric is not actual persisted database/S3 size. Observed process RSS is below the 512 MiB sampled abort threshold, not an enforced native allocator cap. The five primary JSON+JSONL pairs total 46797254 bytes; that aggregate does not prove all sidecar artifacts fit a total experiment budget or measure database storage. Raw request ledgers, admission/drop counters, resource samples and workflow latencies remain distinct measurement scopes.

## Evidence freeze and limits

Five `run.json`/`operations.jsonl` SHA256 pairs, respectively:

- a66d3da894f7b624: `2a41ea09f5211f9f0a49b123343e92ecdb69d599c942a6088edd37542964acb5` / `7bc65c85d55a1d4e52cf6bec7507640c394aa112681959f8933f55bb65a71f92`.
- d70a2475ab0b0c7e: `75b52b32a69fc9ce24ac871c2ed45e3266c52bbfc63c420e01308cef84aa033e` / `32348e8530cb8cb7aaacc60e8b95fd276df27d09bd1201bab5655bbc9cbd3695`.
- 3ae63e17d9c8d1aa: `613ab6f2b1d73322b42adc1f8aaeb972eed9e4189fbdfca8ce84343f64a76840` / `791a1161769a6f031c33d39060e3edc9c836e5d382eb2dcc8cf1c6c9dc031f44`.
- 9a56c88b23e870b2: `865d3ac0341334d1e942c6716ab58dc09163b9cfc15b98f44afcb1d3f7752410` / `9bb35e5e871695f1d194f716c7331dae10af87c696afd67290b22100872e1a4a`.
- 32f0ba1ea724d157: `6e7669476b8675a77a5ff37289c4ec5276217cc9613a5774df72c9ca7ab749cc` / `4e68ac8383e5e332d52339fb7c7a3f1bfb75f8351bd90baee28881f68d248e66`.

The 82-file raw/round5-prefix evidence manifest was identical before/after independent analysis: `/tmp/peri-round5-f-verification/{before,after}.json`, SHA256 `ea8c981a91bdbc6ca28252527b0118726b606a6059d8867969fd979a27ad335e`. Derived per-phase counts/quantiles and checks are in `analysis.json`/`analysis.log` beside it. These local derived files are not a signed independent capture of live events. No historical report/raw, production source, author test or user CLAUDE file was edited; this new report is the only repository write.

Architecture, SQL matrix, actual unknown-drain behavior, starvation, fault recovery and unsupported snapshot-under-load do not gain PASS coverage from these five runs. Mixed correctness after the sole readback remains incomplete. Future bounded diagnostic code needs its own source freeze/gates and a separately authorized fresh run to identify a native cause; it cannot repair this failure or inherit the measured 868 artifact's outcomes.
