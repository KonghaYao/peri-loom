# Local 1.5× optimization progress

- Previous goal turn: progress — adversarial measurements established the limits of prior hypotheses; goal remains unachieved.
- Baseline frozen at 1198b18, identical real SQL release workload archived with hash in identity.json.
- Three agents are implementing workload/evidence, storage copy-on-write, and canonical WAL byte reuse.
- Existing staged IO and WAL work is preserved; no live services are stopped.
- Acceptance requires >=1.5× overall paired median and two confirmation rounds, complete work and cold-reopen oracle, with no material fixed-phase regression. Microbench speedups alone do not satisfy the goal.

## Experiment 01

- Corrected the harness bootstrap after an initial `Fenced unknown namespace` failure; preserve failed-v1 artifacts. Identical fixed harness hash: 99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef.
- Detected Cargo's timestamp reuse after tar extraction (mtime=0), discarded same-hash stale candidate before any formal sample, forced source recompile; final A/B hashes differ.
- Frozen candidate a3edf74410c9d2a4a32b09c05ab9b04a4365b4b4e0e7b6cf05aca2a9d68f44bb; complete source manifest/archive retained.
- 77 targeted release correctness tests pass, including real lost-ACK/adopted-WAL GC regression. Initial `wal::tests` filter matched zero and was explicitly supplemented by `wal::receipt_tests` (2 tests).
- Formal ABBA speedups: 1.026755429 / 0.885801225; median 0.956278327. NOT_YET_1_5X, historical commit phase regression gate triggered. All content oracles and non-timing work identical. No claim of end-to-end improvement.
- Goal turn classification: progress — falsified candidate-01 overall benefit and identified a concrete repeated boot deep-validation path for the next candidate; goal remains active.

## Experiment 02 and candidate 03

- Candidate02 boot admission frozen as 8e2e66a92cfc980e8ca367a852d84b7cad77c4ceabc0a061d0f495a0d9dc5e45. 52 targeted release cases pass (4 boot, 5 namespace-filtered, 8 ownership, 35 p4_authority).
- Formal ABBA speedups 1.507917399 / 1.404407688; median 1.456162544. NOT_YET_1_5X; no fixed-phase median regression over 10%. Three reopen phases improve about 1.96–2.00×, not a general all-request claim.
- Candidate03 frozen as e4620e51fa8e987a644aaab867cf4fe19099d662d4e0de2c7fe56b066ea3e3c2, same harness; 63 targeted release cases pass (4 Walker, 4 boot, 14 recovery, 35 p4_authority, 6 branch including real GC).
- Candidate03 differs from candidate02 only in semantic.rs, new semantic_wal_chunks_tests.rs, and external committed cfg(test) maintenance/integration_tests.rs. It caches validated WAL chunk identities within one immutable root walk, bounded at 4096 entries with safe eviction/revalidation. No candidate03 benchmark yet; waiting for experiment02 independent verification.

## Experiment 03 and final gates

- Formal ABBA: 23.961864125/10.106915791s = 2.370838406×; 24.353676666/10.058122584s = 2.421294477×; median 2.396066442×, all fixed phase median gates pass. Full oracles identical.
- Independent experiment03 verification PASS; final candidate63 targeted release tests pass.
- A provenance challenge resolved by complete fixed1198 source forced rebuild, lib/test fresh=false, new binary exactly matches the A used in all formal timing. No replacement performance samples.
- Final new independent adversarial review CONCLUSION_STANDS; baseline manifest toolchain omission fixed and independently checked. Performance claim is fixed request-stage total, dominated by reopen gains; excluding the three reopen phases yields only1.043769×/1.001178×.

## Completion

- All three experiment/report/independent verification cycles completed; fresh final adversary found no remaining fatal or serious defect in the scoped conclusion.
- Fixed local workload target achieved: paired median2.396066×, both pairs above1.5×, full oracles equal, no fixed-phase regression gate violation.
- Goal turn classification: achieved. No further required optimization or tests remain for this fixed local target. Hot SQL/production scalability remain outside the measured claim.
