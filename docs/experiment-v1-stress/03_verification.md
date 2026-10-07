# Round 3 baseline independent verification — PARTIAL / UNRESOLVED

Verifier: Fermat, 2026-10-07. Scope: readonly analysis after the baseline runner stopped. **The saturation and point subsets completed with matching recorded correctness evidence; mixed and the overall paired experiment remain PARTIAL / UNRESOLVED. There is no new-native performance comparison or performance PASS.**

## Evidence and method

Independently parsed all three `run.json` and every `operations.jsonl` record, checked provenance/exit files, recomputed phase counts, nearest-rank latency quantiles and ACK/s denominators, reconciled operation IDs with actual RPC records, and reconstructed account/ledger expectations from the seed and acknowledged mutations. No tests, builds, network/API/SQL requests, retries, fixture changes or signals were executed.

Evidence base: `/private/tmp/peri-stress-results-h3wtrdm2`. Author report `03_experiment_report.md` remains unchanged: 172 lines, SHA256 `524fd3c10a885421a8ee9676e8a6d02b358a4d72c958de5a7ab453ea0a49616e`. Independent derived records are `/tmp/peri-round3-f-verification/analysis.json`; immutable evidence manifests are `evidence-{before,after}.json` in that directory.

| Raw run | run.json SHA256 | operations.jsonl SHA256 |
| --- | --- | --- |
| `run_5e446dcda7d2a559` | `7e0489e9973c42a426b81a480e03aecf35db5575908515e66f588acc1e659154` | `50f916af477a681251fb0443ee02412f564b8b2b7aa285d6993337a38615cc62` |
| `run_00480a7c7bfcb0b7` | `47c71436df8d0620e0627e800f206754ef4f10cc0dba1e75918eb28786b50210` | `781b83ad233686beb6adf2289d967fdd6d6d70704481b904678d9437ac756e53` |
| `run_8b7e29f6f8aeafb2` | `f765334cc656508cd3b0cfcd0a6227c6b4bedfd86433b69567de79d6f61e1ba9` | `1290f996079a0e06eb9d76e24b47d328afe05720952d9a99eba42e74ad63a770` |

All six hashes match the author report. The 28-file raw/provenance/exit/background evidence set is unchanged during verification; manifest SHA256 `dd5e90d6105ee29a1b44f3db3c97973b61910cfa77a0283f3ede3d6448890b2b`. Hashes establish byte identity, not independent observation of the live server.

## Identity and execution boundaries

All provenance/run records name source/main gate `792a7e1b7659bccde15f97b8e7ca9a887da7d0ee`. The intentional control is **old release native** `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`, with final correlation Go `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d` and harness `c186fd905c34f9d68d2f899e162fdae2bc0539e1a290fffa0cf64341b0e54886`. Independent hashing of all three current files matches. This is not the current OwnershipView native and not a clean-source reconstruction of that HEAD. The previously accepted candidate safety reports are not revised by this baseline result.

Provenance records the same service process identities across the three sequential labels, fresh per-label projects/databases, proper-CA service configuration, and the fixture `/private/tmp/peri-stress-pairbase-8m6qyLNX`. Actual argv select rows 1000, seed 20261007, W2/S10/M30; saturation C1/2/4/8 offered 200 workflows/s, point and mixed C1/4 offered 50. All three provision exit files are 0; runner exits are **0, 0, 1** respectively. Same-arm cache/history accumulates across labels: these are not three fresh service restarts or cold-cache samples.

Dirty provenance is explicit: the tracked diff SHA remains `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848` for user `CLAUDE.md`. The later runs additionally record the narrowly approved untracked planning document SHA `0c452c5ccc8b4a863b9a11f73ceff341bcf108eb23bd059cf577228f98bf996a`. The saved wrapper pause occurred before point provision, with saturation already exit 0 and point/mixed labels not consumed. It is not a SQL retry or a saturation failure. Runtime's prior 314-file frozen content still matches; the verifier does not claim Git was completely clean or live process memory is attested by file hashing.

## Reconciled coverage and accounting

| Run | Actual phases | Offered / attempted / ACK | Client drop / queue-expired | Outcome |
| --- | --- | --- | --- | --- |
| Saturation | 12 complete | 33600 / 7107 / 6190 | 26454 / 39 | COMPLETE; 917 typed admission errors |
| Point | 6 complete | 4200 / 3124 / 3124 | 1073 / 3 | COMPLETE |
| Mixed | 3 complete + partial C4 warmup | 2107 / 287 / 284 | 1816 / 4 | UNRESOLVED; 1 SQL error + 2 unknown |

Every attempted workflow has a unique business ID and corresponding recorded RPC identifier(s); nonempty RPC IDs are unique. Every ACK has `correct=true`, no failure, and matching expected/actual rows hashes where present. Phase counts reconcile with raw operations, and `offered = attempted + client drops + queue expired`. Queue-expired is that harness accounting residual, not an independently observed server rejection. No ACK/error/cleanup event is double-counted as another workflow.

Saturation's 917 failures are recorded HTTP admission outcomes at C8: W/S/M = **49/216/652**, not client drops and not ACKs. Saturation and point each have zero unknown, cleanup unknown, timeouts and confirmed correctness mismatches. Their 1 preflight plus 5/3 later oracle results respectively match independently reconstructed 1000-account data, sum **100000004**, ledger **4**, and both account/ledger hashes. These cases do not exercise concurrent mixed writes after preflight.

Completed 30-second measure ACKs are saturation C1/2/4/8 **778/1509/1490/643**, point C1/4 **763/1470**, mixed C1 **154**. Recomputed ACK rates and all/success/client-queue quantiles exactly match the raw summaries. Rate denominators are monotonic phase elapsed including drain; HTTP-workflow latency excludes the full offered-to-response queue window. C8 saturation has 643 success samples versus 1295 all-workflow samples; rejected requests are not mixed into success latency. These are descriptive baseline observations, not maximum capacity or paired improvement estimates.

## Mixed ACK ledger: what is and is not proved

Mixed C1 W/S/M acknowledge **30/97/154** workflows, including **6/23/44** write mutations. C4 partial warmup acknowledges three reads and no writes. Independent counting finds **73** unique business mutations plus **4** preflight entries: **77 known ACK ledger entries**, without inventing a final database readback.

The only post-preflight mixed oracle is after C1 smoke. Reconstructing the model from seed and the first **33** acknowledged entries gives exactly the reported account hash `06ba7a183f33a995ffe993b575bbed4dd6e68c1c701ff0a226a5be9547957014`, ledger hash `dcc7665186c1c18fccf1cc29517cfedb063560cf09f61cf001b05991a61512b6`, and sum **100000006**; recorded actual values match. The remaining **44 measure writes** have HTTP ACK evidence but no subsequent durable full-account/ledger oracle. No ledger-77 read or recovery PASS is claimed. Raw actual row payloads are not retained; reconstruction independently checks expectations and recorded hashes, not a new live query.

## First failed response and abort trajectory

Mixed JSONL line **2522** is the first failed RPC response: Execute `stress_8b7e29f6f8aeafb2_000000002439`, business ID ending `002438`, C4 warmup slot 0, HTTP **400**, **173** response bytes, RPC elapsed **50543125 ns**. The decoded failure is `sql_error`, `execution_completed=true`, `may_have_committed=true`, `unknown=false`, `timed_out=false`. This is a known terminal SQL-error response, not success, not a 429 correlation mismatch, and not proof that no statement prefix committed. The failed business operation contributes no ACK mutation.

The frozen harness cancels a phase on a non-admission failure. The subsequent raw sequence is consistent with that path:

1. Query `...002441`, slot 1, ends `request_failed`, HTTP status 0, zero response bytes, unknown true; RPC elapsed **30468541 ns**.
2. Execute `...002435`, slot 2, likewise ends unknown; RPC elapsed **90107625 ns**. It started earlier than the SQL-error request, but did not fail first.
3. Two other sessions have successful recorded Close RPCs. Slots 1 and 2 produce local `unresolved_session_not_closed` cleanup evidence without closing poisoned sessions. No later Execute/Query RPC follows the abort, and no duplicated request ID indicates a retry.

The C4 phase therefore has 7 offers, 6 attempts, 3 ACK, 1 SQL error, 2 unknown, 1 queue-expired and 2 cleanup-unknown events. Source review explains why cleanup overwrites the final reason with **`session_cleanup_not_proven`**; that final reason must not obscure the initiating SQL error. The canceled requests' false completion/commit flags are default absent-proof values, **not proof of no execution or no commit**. `correct=false` on failed operations is not a confirmed successful-row mismatch; the confirmed mismatch counter is zero, with correctness after abort still unresolved.

Neither original response body nor underlying native error message is retained in these artifacts. The first known protocol-level error explains why the runner stopped; it does **not** identify the engine root cause. BUSY/lock conflict, lease phase, ownership change, native exception and committed-prefix extent cannot be inferred. No rollback, replay or readback was attempted to classify either unknown request.

## Limits and disposition

Background samples number 18/11/6 and record zero detected compiler CPU in those samples, alongside other active host processes. This does not establish continuous zero interference. Sampled service RSS stays below the 512 MiB operational guard; it is not a hard process cap or proof of the unobserved peak. Later preflight guards explicitly relaxed compiler CPU to 400% with bounded RSS checks. Generated-request byte accounting is not actual cumulative S3 storage. Resource/latency fields that are unavailable are not fabricated as zero.

**Subset factual verification PASS; whole experiment PARTIAL / UNRESOLVED.** Mixed C4 smoke/measure and final oracle were not executed. The new-native arm, architecture/functional matrix, starvation/faultprobe/cold-cache tests and controlled before/after comparison are not established by these three runs and cannot be promoted to PASS. Preserve all first failures and unknown classifications. Any further diagnostic or measurement execution requires separate main authorization; this verifier performed only local reads and wrote this report/derived analysis.
