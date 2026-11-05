R71c smoke validation (NOT part of any criterion)
=================================================
These runs validated the campaign scripts before the official legs. They are
not used in P0/P1/P2/P3 and are not pooled with the official data.

1. F1 8-leg smoke
   Command shape: run_p2.sh f1 with OUT=/tmp/r71-ab3-smoke-evidence,
   PREFIX=peri-r71-ab3-smoke, ports 21250-21257.
   The script's LEGS variable was not overridable in that first invocation, so
   the nominal smoke ran the full frozen F1 8-leg design. It completed with
   driver_exit=0, settle_exit=0 and stop_exit=0 on all eight legs. Its F1
   analysis (0.44852 p50, 0.74652 p95) is consistent with the official run,
   but the official F1 evidence is legs/p2/f1 and only that is used.
   Summaries kept here as f1-<leg>.summary.json.

2. Sustained c8 A/B smoke
   Commands:
     OUT=/tmp/r71-ab3-smoke-evidence PREFIX=peri-r71-ab3-smoke \
       ./run_p2.sh c8 smoke-a1
     OUT=/tmp/r71-ab3-smoke-evidence PREFIX=peri-r71-ab3-smoke \
       ./run_p2.sh c8 smoke-b1
   Both completed driver_exit=0. A: 3200 timed requests, 0 failures,
   1297.824 qps, p50 4.1731 ms, p95 16.8368 ms. B: 3930 deferred enqueues,
   70 inline, pending 0, failed/retried/unresolved 0, 1349.809 qps,
   p50 3.8159 ms, p95 16.1088 ms, audit delta 12024 = 3 x 4008 requests
   (8 calibration + 800 warm-up + 3200 timed).
   The B smoke proves the deferred counter and the audit-count arithmetic; it
   is not used as an effect estimate.

Evidence root: /tmp/r71-ab3-smoke-evidence (kept). The campaign volumes
peri-r71-ab3-smoke-* are preserved; no smoke container remains.
