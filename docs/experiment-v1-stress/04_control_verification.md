# Round 4 control failure verification — FAIL / environment-compromised

Independent verifier: Fermat, 2026-10-07. **The sole control runner actually failed; the paired experiment is incomplete and its affected measurement is not a valid performance sample.** OS idle sleep is now corroborated by power-event evidence. The underlying native SQL-error cause is still unconfirmed.

## Readonly evidence and provenance

After main confirmed the runner stopped, F parsed every operation/RPC record, independently reconstructed counts, latency summaries and the four-entry account/ledger oracle, compared clock labels with monotonic durations, and read local clock implementations and the supplied power log. No API, SQL, test, build, load, retry, process signal or system power-setting change was executed. Only this repository report and TMP derived evidence were written.

Raw directory: `/private/tmp/peri-stress-results-h3wtrdm2/run_95b311e297b4f1fb`.

- `run.json` SHA256: `1a44137bc81bc2762e146c9be2cd8c790dcaa93271421f554d1d06adeb7aa933`.
- `operations.jsonl` SHA256: `c15da398ce52091f11a75f51b7d0058ceaf07ab45e01c3fd76ba4ccfbb9c9ecd`.
- Prefix `round4-control-r4csat.*` in the same output base retains provenance, provision/stdout/exits, background samples, outcome and wrapper-stop records. Provision exit is **0**; runner exit is **1**, status **FAIL**, reason **`sql_error`**, monotonic run duration **24.983863042 s**. The wrapper stopped on that non-COMPLETE result; it did not retry the label.
- Independent 11-file evidence snapshots and analysis: `/tmp/peri-round4-control-f/evidence-{before,after}.json` and `analysis.json`. Before/after manifest SHA256: `7085cfcd0f6f5889d67bcf355979a9cbd2d3d466566028e742ccc54cd2f5fdf3`.

Recorded source/matching gate: `21c509d74bdc92c140915f18d6fb647c82b90ef7`; gate file SHA256 `ee0f1b1714967e8d7326756c52e2fa48e6ffe15d894986892a39ccecfdf421ee`. It is an ordinary private-JSON main operator attestation, not a cryptographic signature. Dirty status records only user `CLAUDE.md`, not a completely clean checkout. The deliberate control uses old native **`3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`**, Go **`8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d`**, and new drain harness **`9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`**. It is not the BUSY+View candidate.

Fixture provenance names `/private/tmp/peri-stress-r4-control-8dKNuhKU`, a separate initialized bucket, and an actual argv selecting `saturation_admission`, rows 1000, seed 20261007, C1/2/4/8, W2/S10/M30, offered rate 200 and sampled RSS guard 512 MiB. Actual coverage stops at C1; planned settings are not proof that the other cells ran.

## First failed response and accounting

First failure is JSONL line **4685**, **Query**, `POST /v1/sql/query`, request **`stress_95b311e297b4f1fb_000000004534`**. Corresponding operation line 4687 is business ID ending **`004526`**, `saturation_admission`, **C1 / measure**, slot 0, offered sequence 1593. HTTP **400**, **173** response bytes; decoded `sql_error`, `execution_completed=true`, `may_have_committed=true`, `unknown=false`, `timed_out=false`. RPC monotonic duration **529775584 ns**; workflow duration **529865333 ns**. This is not a Round 3 mixed Execute failure or evidence of a writer-lock/BUSY cause. Conservative completion/commit flags are not proof of a mutation in this read workflow.

| Actual C1 phase | Offered | Attempted | ACK | Client drop / queue-expired | Result |
| --- | --- | --- | --- | --- | --- |
| Warmup | 400 | 49 | 49 | 350 / 1 | COMPLETE |
| Smoke | 2000 | 247 | 247 | 1752 / 1 | COMPLETE; post-smoke oracle PASS |
| Measure | 1707 | 194 | 193 | 1512 / 1 | FAIL; one SQL error, not a complete 30 s measure |
| Total | 4107 | 490 | 489 | 3614 / 3 | FAIL |

All attempted business IDs and nonempty RPC IDs are unique and linked; no duplicated identifier indicates replay. All 489 ACKs are reads with `correct=true` and matching expected/actual row hashes; no workload mutation entered the ACK ledger. Count/nearest-rank quantile/rate arithmetic agrees with raw summaries, but arithmetic agreement does not make the interrupted sample scientifically valid. Queue-expired is the harness accounting residual, not server admission. Actual admission errors are **0**; no saturation was demonstrated by server rejections.

Every recorded phase has **unknown 0, cleanup unknown 0, timeout 0, confirmed correctness mismatch 0**. There are three successful phase cleanup records and a successful Close after the failed Query; no later workload Query/Execute or rollback replay follows it. The failed operation's null-row hash/`correct=false` is not a successful-row corruption finding. C1 had only one session, so this run does **not** demonstrate multi-peer drain behavior under a real failure. Historical Round 3's two canceled-request unknowns and two cleanup unknowns remain unchanged; the new harness's behavior cannot be backfilled into that run.

Only **one preflight and one post-smoke** durable account/ledger oracle ran. Independent seed/mutation reconstruction verifies 1000 accounts, sum **100000004**, ledger **4**, account hash `1f26a7b84410a5793a344bb0b5bcab10df2b1fce50aa7d9f642c62002868d91e` and ledger hash `82681d2055ca11a9b8dc9a362004edfda2e2721294e32c0ab2a33dfe95cf497b`; recorded actual hashes/sums match. There is **no post-measure/final oracle**. The failure remains failure despite zero confirmed mismatch and successful cleanup.

## Clock discontinuity: independently recomputed

Wall-clock labels are UTC observations, not elapsed/deadline measurements. Local display times below use `+0800` only where explicitly stated.

| Scope | Wall difference, s | Recorded monotonic elapsed, s | Wall minus monotonic, s |
| --- | --- | --- | --- |
| Whole run | 281.578082000 | 24.983863042 | **256.594218958** |
| Warmup | 2.013852000 | 2.013834167 | 0.000017833 |
| Smoke | 10.036414000 | 10.036327792 | 0.000086208 |
| Measure | 265.124996000 | 8.530917208 | **256.594078792** |
| First failed RPC | 257.123793000 | 0.529775584 | **256.594017416** |

Run labels are `2026-10-07T03:35:37.801848Z` → `03:40:19.37993Z`; measure `03:35:54.16824Z` → `03:40:19.293236Z`; failed RPC `03:36:02.169269Z` → `03:40:19.293062Z`. The failed RPC is the only recorded RPC with a gap of this magnitude; the next-largest absolute RPC discrepancy is approximately 7.1 microseconds.

Twenty-five resource samples include a final adjacent jump `03:36:01.468612Z` → `03:40:19.312215Z`, **257.843603 s** in wall labels. Five background samples stop at `03:35:57.835873+00:00`; none spans the return from sleep, so those background snapshots alone cannot locate or rule out the event. No interpolation of missing CPU/RSS or fabricated continuous-clock sample is made.

## OS sleep evidence and time-source scope

Supplied snapshot `/private/tmp/peri-stress-r4-control-8dKNuhKU/power-events.log`, SHA256 **`58c8f3343b5e043b0fd7d12153b4e825bbddb620f47b744d4adbb0068239913e`**, independently contains:

- `2026-10-07 11:36:00 +0800`: Sleep, entering due to **Idle Sleep**, recorded **259 secs**, TCPKeepAlive active, battery 50%.
- `2026-10-07 11:40:19 +0800`: **DarkWake from Deep Idle**, wake reason `smc.sysState.Wake(0x70070000) wifibt SMC.OutboxNotEmpty centauri-beta/`.
- A separate later **Maintenance Sleep** at `11:41:04 +0800`, recorded **10 secs**, followed by DarkWake at `11:41:14`; it is after this run and not counted into the earlier measured gap.

The first DarkWake is `03:40:19Z`, the same second as the failed RPC/run completion. The OS record corroborates actual suspend, not merely a guessed clock adjustment. Its 259-second sleep duration is compatible with the roughly 256.594-second wall/monotonic discrepancy, not claimed numerically identical: transition timing and whole-second OS labels limit exact alignment.

Static time-source evidence is deliberately separated from runtime causation. `ownership/clock.rs` on macOS calls **`mach_continuous_time`**, converts with Mach timebase information to milliseconds, and fails closed on unavailable/invalid clocks. Its SHA256 **`3c57d6ca9c5830fe40facb776d03e2b75bb653eeec5d7b04a10bac0f780e5195`** exactly matches the preserved native baseline source manifest `/tmp/peri-v1-joint-native/before.json`. This is source identity tied to the earlier accepted old binary proof, not new live clock telemetry. Go broker Darwin code also explicitly uses this continuous clock.

The installed Go **1.23.4** Darwin/arm64 runtime's `nanotime_trampoline` calls **`mach_absolute_time`**, while harness durations use `time.Since`/`Sub` and serialized UTC labels use wall time. Local primary source paths are `/opt/homebrew/Cellar/go/1.23.4/libexec/src/runtime/sys_darwin{.go,_arm64.s}`. The installed Apple header `/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/mach/mach_time.h` explicitly distinguishes continuous time as advancing during sleep. Rust SQL budgets use `Instant`; this review does not attest that standard-library clock's backend from these artifacts or conflate it with the explicitly continuous lease clock.

Lease context checks revoke at `now >= deadline`; authority live checks require `now < deadline`, with regression/unavailable-clock fail-closed handling. Thus a suspend-inclusive lease can expire across sleep even when harness elapsed budgets omit that interval; denying an expired lease is required, not a reason to weaken fencing. **The failed request's actual native error, relevant continuous-clock readings and deadline were not captured.** Main reports its service logs have no native body. Suspend is confirmed, but `LeaseExpired`, BUSY, a specific guard branch or exact TTL event is not confirmed as the SQL-error cause.

## Disposition and next experiment boundary

This is a faithfully recorded **control FAIL with confirmed environment interruption**, not a complete valid performance run, not an isolated native regression attribution and not a paired PASS. The 8.530917208-second monotonic measure is neither a completed 30-second window nor sleep-inclusive elapsed time. C2/C4/C8, `r4cpoint` provision/run and all candidate arms are **NOT EXECUTED**. No automatic continuation or new label to bypass the failure is authorized by this report.

Main separately reports normal SIGTERM of its owned launcher PID 99485, actual launcher exit 0, all five owned processes gone and precise authority-lock cleanup. F did not send the signal or independently observe that live lifecycle; this executor-reported normal termination is not expanded into proof of every project resource's cleanup or retroactive runner success.

A future Round 5 with bounded owned idle-sleep inhibition, fresh labels/fixtures and new matching gates needs a new predeclaration and explicit authorization. This verifier did not launch `caffeinate`, alter user `pmset` settings, retry SQL or execute either arm. Preserve this raw and all earlier unknown classifications separately from any future evidence.
