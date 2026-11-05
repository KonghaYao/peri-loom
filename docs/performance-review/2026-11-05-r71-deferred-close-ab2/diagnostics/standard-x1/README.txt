Post-hoc standard-X1 c8 diagnostic (NOT the frozen P2 metric)
==============================================================
This directory is supporting evidence only. The frozen P2 X1 metric is the
Gate 2 c8-only driver recorded under legs/p2/x1; the frozen decision is in
criteria.txt and is not changed by anything here.

Why it was run
--------------
The frozen c8-only driver produced B/A = 0.84384 (r1 0.73235, r2 0.79333),
which meets the frozen reproducible-degradation rule. To see whether that
result is specific to the c8-only runner (a cold 8-worker burst after an
8-request warm-up), the stock Issue 28 X1 scenario was run afterwards on fresh
legs: levels 1, 4, 8, 16, 100 requests each, 2 rounds x A/B/B/A.

Command shape
-------------
The runner was a temporary copy of the previous round's run_p2.sh
(2026-11-05-r71-deferred-close-ab/run_p2.sh) with:
  - container/volume prefix peri-r71-ab2-diag2- / peri-r71-ab2-diag3-
  - base ports 21174 (r1 legs) and 21178 (r2 legs)
  - OUT=/tmp/r71b-diag-x1b and /tmp/r71b-diag-x1c
  - REPO=/Users/kk/code/peri-loom,
    PLATFORM=/tmp/r71-ab2/bin/peri-platform-ab2
Legs: r1-a1 r1-b1 r1-b2 r1-a2, then r2-a1 r2-b1 r2-b2 r2-a2.
The copied driver.txt and harness/summary.json for every leg are stored here.

c8 results (100 requests at concurrency 8)
------------------------------------------
| leg   | elapsed ms | qps    | p50 ms | p95 ms | c8 429 |
|-------|-----------:|-------:|-------:|-------:|-------:|
| r1-a1 | 50         | 2000.0 | 2.92   | 7.33   | 0      |
| r1-b1 | 58         | 1724.1 | 2.91   | 10.62  | 0      |
| r1-b2 | 52         | 1923.1 | 2.65   | 10.13  | 0      |
| r1-a2 | 58         | 1724.1 | 3.25   | 7.86   | 0      |
| r2-a1 | 72         | 1388.9 | 4.52   | 13.66  | 0      |
| r2-b1 | 50         | 2000.0 | 2.74   | 6.91   | 0      |
| r2-b2 | 50         | 2000.0 | 3.49   | 7.41   | 0      |
| r2-a2 | 83         | 1204.8 | 5.94   | 13.42  | 0      |

Round medians: r1 A 1862.1 / B 1823.6 (ratio 0.97934); r2 A 1296.9 /
B 2000.0 (ratio 1.54219). Merged A 1556.5 / B 1961.5 (ratio 1.26021).

Every leg's c16 level produced 429s (13-53 per leg; the known default-quota
pacing artifact of the stock scenario), so every diagnostic leg has driver_exit=1.
The c8 level had 429=0 on every leg. These legs are not part of P3.

Interpretation
--------------
The stock X1 c8 level does not reproduce the c8-only driver's degradation; it
shows B faster overall. The frozen P2 verdict therefore remains FAIL, but the
P2 failure is runner-dependent and must be treated as a material protocol
discrepancy, not as proof that the candidate degrades every c8 workload.
