# R10 focused checkpoint independent failure verification

- **FAIL / UNRESOLVED; Native root cause unavailable; performance NOT PASS.** F read-only evidence reconstruction, no API/SQL/retry/Close/reopen/build/signals or old/new Unknown database access. RELEASE raw/source verification window, not follow-up workload authorization.
- Sole run `253aa1366133bb5d`, actual CLI64287 exit1 and independently observed absent; wrapper records natural exit/child_alive=false/retained_pid=null. Run operation_incomplete, monotonic23.402202125s, UTC `2026-10-07T19:23:17.112259Z`–`19:23:40.514399Z`.
- First actual failure: POST `/v1/databases/checkpoint`, request `stress_253aa1366133bb5d_000000000516`, HTTP409 in33.928708ms; observed `19:23:40.480400Z`–`19:23:40.514329Z`. Typed client unknown/operation_incomplete, execution_completed=false, may_have_committed=false, timed_out=false, unknown=true.
- RPC and failed lifecycle33.974917ms record the same logical unknown1, not two failures. Only one checkpoint RPC, no later RPC/replay/reopen/final oracle. Protocol-path failure recurs in a fresh S3 fixture, but does not establish the historical/common failure's identical Native cause.
- Fixed checkpoint_reopen profile C1/2,W1/S2/M5,rate25,rows1000,seed20261007,request10s and original resource caps. Six positive COMPLETE workload phases do not make checkpoint_reopen COMPLETE.

| C/phase | attempt/ACK |
| --- | ---: |
| 1/warmup | 4/4 |
| 1/smoke | 8/8 |
| 1/measure | 16/16 |
| 2/warmup | 4/4 |
| 2/smoke | 7/7 |
| 2/measure | 13/13 |

- Independently regrouped52/52 workflow ACK, all correct/no failure; offered400=52 attempts+339 client admission drops+9 queue expiries. Phase unknown/cleanup_unknown/timeouts/correctness-failures0 and9 successful cleanup records are pre-failure facts, not global unknown0 or postfailure cleanup proof.
- F rebuilt ACK ledger/account models:4 preflight+52 workload mutations=56 known mutations. Two recorded scenario oracles16/43 rows plus preflight4 independently match both hashes and sums100000016/100000043/100000004. Final13 ACK mutations lack a final readback; no inferred56-row durability/root state.
- Last verified account hash `d2fd4cd159b2c3c2a4aedc9f0347ea757f2282cf626533eaa634debee893e017`; ledger hash `98a5765d7c3cb59e536c784e8186f5d04da6a7a6cbacbc72820967de5dcfb297`. Workflow/nonempty RPC IDs unique, no recorded replay; no physical root-lock/holder/native drain proof.
- Guard final HOLD/clean_join=false/first_stop=null/soft=false/hard=false/**may_have_unknown=true**. Conservative nonzero-child risk now correctly retained; neither child death nor client may_have_committed=false proves Native termination/rollback. Actual SIGUSR1 delivery not exercised.
- Safe fixture worker-a.log and worker-b.log independently read as zero bytes, unchanged during analysis. No maintenance record/native_cause_class captured: absence means cause unavailable, not Native success, Busy, partial backfill, lease expiry, logger repair or absence of in-flight work. Private MemoryObjectStore NOT_REPRODUCED remains separate.
- Guard15 samples,13 during live CLI, all live awake effective; maximum adjacent gap2.087819666s.193 run/phase/RPC/workflow/lifecycle windows independently checked, maximum wall-minus-monotonic62125ns; guard62208ns. Sampled timing is not full OS suspend exclusion.
- External esbuild confound retained under functional_only/record-confound; final performance_pass=false. Resource21 samples, maximum sampled service RSS45989888 bytes; generated134143 bytes. No allocator-peak/S3-volume/performance guarantee. Own caffeinate exit-15/no retained helper, not whole-fixture shutdown.
- Exact inventory: **181 source paths+3 nonoverlapping artifacts=184**, not183. F current hashes equal spec/wrapper-after; bounded raw+safe-worker-log before/after invariant. Recorded/root HEAD `304a2ab432a03d77bb9595452dfb993f9d54f9c0`, dirty=true; no whole-repository clean claim. Initial count-only preflight HOLD is preserved, not a failed or repeated SQL run.
- Runtime identities: new Native `64ec5bd6e7b143b95151c9a52934c03f42a2a5396297f4cbe91c06de33c5f85e`; Go `e6c9dc3060646fe64b4c016f43de56f3b87dda2b88f186b06f11ef106e217af8`; CLI `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`. This run uses the diagnostic candidate, unlike historical common084c; no diagnostic cause was observed.
- Raw base `/private/tmp/peri-stress-r10-checkpoint-results-kbi0ddfa`; run SHA `1a90f2129027233051db3b25155e6344bc4205959ff7c1994a36bf3c41380c9d`; operations SHA `b0068b253c43a9315ae5acafdf6f66e7bec3fcb6f478ef5f69f2717ae0dbf17f`. Private raw0600, credential-pattern scans negative; secret connection/launch/key files not read.
- Author report SHA `252670fd309fb473b4c9ccacf6982b88409d79b08d9f99e43d36055dbc9470c8` agrees with failure/count/scope; its two checks refer to scenario oracles, separate preflight also verified. F `/tmp/peri-r10-checkpoint-independent/proof.json` SHA `7603a909ef497e95feb31aae64b24d0fb6cdaf4d7961e5f21806bec4a56ef525`; inventory SHA `9cc05b23c466d32de736d650f2e86d8612d3ab8a47ec69108ba1c32bac597f76`.
- Historical common Unknown, P4 unrootcaused watchdog and prior failures remain unchanged. Full matrix/algorithm fix/performance not accepted; main alone authorizes diagnosis/fresh follow-up. Only this report/private proof written.
