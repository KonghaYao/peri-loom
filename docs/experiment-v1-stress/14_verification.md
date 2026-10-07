# R12 focused checkpoint independent verification

- **Bounded focused functional subset ACCEPT; performance NOT PASS; full matrix PARTIAL.** F offline read-only reconstruction only; no API/SQL/retry/Close/reopen/build/signals/Unknown database operation. SOURCE/RAW RELEASE issued; next experiment requires main authorization.
- Sole run `e87f2f7d5769bb61`, COMPLETE, actual CLI83357 exit0/independently observed absent; monotonic26.564935542s. UTC `2026-10-07T20:33:27.040657Z`–`20:33:53.605478Z`; no repeated provision/run recorded.
- Exactly one checkpoint HTTP200/envelope ACK, request `stress_e87f2f7d5769bb61_000000000515`, RPC965.031250ms, lifecycle965.560458ms/failure=null. Harness checkpoint contract validates result.checkpointed=true before fresh final oracle; raw body is not separately retained, no fabricated physical root-lock proof.
- Fixed checkpoint_reopen C1/2,W1/S2/M5,rate25,rows1000,seed20261007,request10s/original caps; all six duration phases COMPLETE.

| C/phase | attempt/ACK |
| --- | ---: |
| 1/warmup | 4/4 |
| 1/smoke | 7/7 |
| 1/measure | 15/15 |
| 2/warmup | 5/5 |
| 2/smoke | 7/7 |
| 2/measure | 13/13 |

- Independently regrouped51/51 workflow ACK, all correct/failure=null; offered400=51 attempts+342 client admission drops+7 queue expiries. Workload/lifecycle unknown0, phase cleanup_unknown/timeouts/correctness-failures0;9 cleanup records successful. IDs unique and no recorded mutation replay.
- F reconstructed4 preflight+51 acknowledged workload mutations=55 known ledger rows. Preflight4 plus **three scenario oracles15/42/55 including postcheckpoint final** independently match both hashes and sums100000004/100000015/100000042/100000055; no unchecked ledger suffix.
- Final account hash `fe41d024cd9f9768c08568546a6a6553214ca3f90695d64c2ecb34f91575e4e0`; sorted-ledger hash `9c489dc4e600e1e8ab5e4d0199f4727685f236d05fc8d31634c486fa88ae71b1`. These are recorded fresh readback/oracle facts, not independent AEAD/backend durability/holder/fault/cold-cache proof.
- Important observation limitation: **27 GET /v1/metrics HTTP403/authorization-forbidden** records, unknown=false;27 resource records mark management metrics unavailable. These are optional monitoring negatives, not SQL/lifecycle failures or implicit mutation retries. No Native/management metric availability or comprehensive resource bound claimed; do not loosen Auth to fabricate visibility.
- Platform diagnostic log empty:0 accepted management_native_tool_error records. No failure cause assigned; empty log alone is not completion proof. Successful checkpoint correlation plus final oracle, not log absence, supports this scoped acceptance.
- Guard FUNCTIONAL_ONLY_CONFOUNDED/child_exit0/child_alive=false/clean_join=true/retained_pid=null/first_stop=null/soft=false/hard=false/may_have_unknown=false.16 samples/14 live, all live own-awake effective; maximum adjacent gap2.077127625s. Own caffeinate exit-15/no retained helper, not whole-fixture shutdown or actual pressure SIGUSR1 exercise.
- Independently recomputed204 run/phase/RPC/workflow/lifecycle clock windows, maximum wall-minus-monotonic114542ns; guard116334ns. Sampled timing/awake evidence is not full OS suspend exclusion. External esbuild confound/performance_pass=false retained; no capacity, fairness or isolated/paired benefit inference.
- At RELEASE exact **185 source paths+3 nonoverlapping artifacts=188** match spec/wrapper-after/current; provision.py13dffa14… and provision_tests.pyed527737… included, closing R11's declared provisioning-source inventory gap for this run. Root HEAD `1366d3bfe12c6cf277b40b83fe307ed21afd8a49` unchanged at verification; bounded content identity, not git-clean.
- Runtime Native64ec5bd6…/Go c2524538…/CLI f16dcd05… unchanged from prior candidate artifacts. Provision credential-output correction is not Native algorithm/server-Auth/lease/deadline change; no actual secret credential was read to prove identity. Main proper-CA whole11/binding audit is prerequisite, not new S3-five-test acceptance by F.
- Main-recorded before-SQL metadata-envelope/FileNotFound HOLD had no workload/API/provision; retained independently, not SQL retry or failed pressure. F initial overly broad all-RPC-no-error parser assertion encountered real metrics403; corrected offline category filter and preserved parser note, no source/data change or experiment rerun.
- Raw base `/private/tmp/peri-stress-r12-checkpoint-results-ga22ktjs`; run SHA `462acace98fbfc9f93fe3e21c770a75fda8e280d7b7de043d2331103d25e22a1`; operations SHA `83c3a918f1fb5616f690f8dfc88412cafa69fe8f8840355c06c4b818949cb56f`. Raw/safe platform-log hashes unchanged through independent analysis; secret configuration/connection/key files not read.
- Author report SHA `a317453992798a5ee6b9691dfad03a2d5ef7842fe5a2312afe29f40784dadc64` agrees on functional counts/checkpoint; its three checks are scenario-only, plus separately verified preflight. Independent metrics-unavailability qualification above is additional evidence. F proof `/tmp/peri-r12-checkpoint-independent/proof.json` SHA `0565e6c954edcece4d11ece06119f63e56b1e7996310a3271789fae050640133`; raw inventory SHA `310b2460b21b174966c2b925fb21df8c4cd2b2c531524f2c680269e96a6831bc`.
- Historical common/R10/R11 Unknown and P4 unrootcaused watchdog remain unchanged, not recovered/reclassified. Private Memory NOT_REPRODUCED separate; common13/Linux/SDK/UI/fault matrix not completed here. Only this report/private proof written; main alone authorizes next fresh common run.
