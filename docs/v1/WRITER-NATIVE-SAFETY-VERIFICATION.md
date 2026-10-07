# FINAL23: independent conditional native API acceptance

Date: 2026-10-07, Asia/Shanghai. Verifier/executor: F.

## Decision

**ACCEPT — conditional native API child batch only.** No remaining blocker was found within the expressly bounded interface, selected oracles, and trusted-caller contract below. Main may commit the ten frozen child files and separately package the parent gitlink, isolated P0 fixture/oracles, and a scope-limited gate document. F did not stage, commit, push, or edit production/tests.

**Production provider, parent writer integration, whole candidate, S3, pressure readiness, and performance remain HOLD/unaccepted by this report.** Passing these oracles does not prove a production retirement capability, general SQL lifecycle completeness, FIFO/fairness, or resolution of historical stress failures.

## Immutable identities

- Exact private snapshot: `/private/tmp/peri-core-final23.EGpkcyCh/snapshot`.
- Parsed 336-input manifest SHA-256: `0b2f4cba305ebb11827ff74a79eaa3261d1539762cfda1779e277c7381e93573`.
- Independently hashed all 336 private inputs against their declared copies and corresponding production inputs, plus all ten declared child freeze hashes. No mismatches before or after the initial gate.
- Initial `before.json` and `after.json` are identical inventories. Supplemental unique-fixture `before.json` and `after.json` are also identical. This is content identity, not a clean Git working-tree claim.
- Across the two gates, the only changed captured input is main-owned `prototypes/p0/tests/statement_safety.rs`: old SHA `9416fa716edb0baf57de63a362f8e375b95ee93b2170d68240a29a08eb9674bc`; new SHA `1af823f89d87a50cff48a773a498b903eb86b81651cd41c36a45a43ed0771403`.
- Same native ten and same manifest remain fixed. Therefore the independently executed core23 result is retained by unchanged identity; it was not unnecessarily rerun after the P0 fixture change.

Ten child-relative SHA-256 identities:

| Path | SHA-256 |
| --- | --- |
| core/statement.rs | 5cca46ce1781687b9b6a7f05543283122e02c060b6b966f3101a3371ac4c4f30 |
| core/statement_safety.rs | c029f2df8fbb8ac655c8d3a20c846d8ec44b151e92b660c339bed6e04f79a6c4 |
| core/statement_safety_tests.rs | 822a354ffe729cce0e49f166fd7bcae5bfcc183d6035423dd25dc24ebabb745c |
| core/lib.rs | 1b07a158b70596bd9d86a0780dac69b93af8213fc23896d6ebce6e52954c8171 |
| core/vdbe/mod.rs | 4d7b19e77b69905e2b76089b01aee1d98b24328c1789a59770efee91b8236349 |
| core/vdbe/execute.rs | b4caa83d7e11823713d282613ed58431f0333d03a62839473caa8fae99c28150 |
| core/vdbe/admission.rs | ad1692c0d57323c4e388f3172d3fce38e12c65c9265acb2575b587b993aeca60 |
| core/statement_profile_tests.rs | 13e03652cd45c95c12c37de6b6c5d13f46f31e8477e0ef0e24424eb364b3b9af |
| core/connection.rs | c82c30c98eab4a2d2caa55445e151eedcc90b87a7fd0c713f91a00bc7badb6cf |
| core/connection_safety.rs | 120239db291cf1457c982173968e66aeaf55bf4b932de8bc4a4b15a407e794cf |

## Lock and toolchain audit

- Original lock SHA `55f187e3aea044cbd5571c61a03d3f15a99ece0688e0749f13af32a8687b28f4`; private derivative SHA `8923b12683d91532da43578cfad34a2e557e445171b123e1d229cac939856fa2`.
- Independently recomputed 849 original packages -> 438 retained, 411 removed, zero added. All retained non-dependency metadata, including version/source/checksum, is unchanged.
- 54 raw dependency-list changes reduce to 12 normalized graph changes: 42 representation-only normalizations, 23 removed dependency edges, zero added edges. This is not original-lock byte identity or universal feature equivalence.
- Parsed workspace manifests differ only in `members`/`default-members`. Core and other captured source manifests are exact copies.
- Tested feature selection is explicitly P0 `--no-default-features --features fs,uuid,json`; no claim about encryption/full-parent/other-target/all-feature combinations follows from this private derivative.
- `rustc 1.98.1 (48a229cea 2026-09-01)`, host `aarch64-apple-darwin`, LLVM 22.1.8; Cargo 1.98.1; rustfmt 1.9.0-stable. Exact outputs retained.
- Target for all independent Cargo executions: `/private/tmp/peri-core-isolated-prep.A504aba1/target`. Offline/locked, debug only, `CARGO_INCREMENTAL=0`; no fetch, release build, or lock mutation.

## Actual independent executions

Core command, executed separately for each of the three disjoint module filters:

`cargo +1.98.1 test --offline --locked -p turso_core --lib --no-default-features --features fs,uuid,json MODULE -- --nocapture`

| Independent gate | Actual result | Exit | Log |
| --- | --- | --- | --- |
| statement::safety::tests:: | 12 PASS / 0 fail / 0 ignored | 0 | core-module-1.log |
| statement::safety::profile_tests:: | 9 PASS / 0 fail / 0 ignored | 0 | core-module-2.log |
| connection::safety::tests:: | 2 PASS / 0 fail / 0 ignored | 0 | core-module-3.log |
| Original P0 six, default parallel | **1 PASS / 5 FAIL / 0 ignored** | **101** | production-six.log |
| New unique-fixture P0 six, default parallel | 6 PASS / 0 fail / 0 ignored | 0 | unique-fixture/production-six-default.log |
| New unique-fixture P0 six, explicit serial | 6 PASS / 0 fail / 0 ignored | 0 | unique-fixture/production-six-serial.log |
| Owned rustfmt check, native ten plus P0 test | PASS, no writes | 0 | unique-fixture/rustfmt-owned.log |
| Scoped child and parent git diff --check | Both PASS | 0 | unique-fixture/diff-*.log |

- Core23 unique test names exactly match the manifest's 23 expected names; selected three filters are not the full 2,489-test library gate. None of its unselected tests is reported PASS.
- New P0 commands: `cargo +1.98.1 test --offline --locked --manifest-path /Users/konghayao/code/ai/peri-loom/prototypes/p0/Cargo.toml --test statement_safety -- --nocapture`, then the same command with `--test-threads=1`.
- There are **29 unique selected tests** across core23 and P0 six; the second six-test execution is a repeated mode check, not six more unique tests. Ignored count is zero within these selected executions only.
- Compiler warnings are retained in raw logs. This was not a strict-clippy or warning-free gate. An inline count-parser syntax error was corrected in verifier tooling without rerunning/changing any source or test result.

## First failures remain evidence

- Independent original default-parallel P0 failure log SHA `dacd4e33010a7b30649c2c1c1d7b748188fbf564c47f44fba7b596cf049fe105`; five tests reported `Database is busy`. The actual initial gate and runner failed; supplemental PASS does not turn that run into PASS.
- Initial fixture gave each case a new MemoryIO but the same file-like path `statement-safety.db`. Static inspection confirms MemoryIO file identity is path-hashed and the process-wide DatabaseKey registry may return the same native Database for that identity.
- Main changed only fixture naming to atomic unique file-like paths, retaining file/WAL-like semantics rather than replacing them with `:memory:`. Main owns the minimal-change attribution; F independently checked the new source identity and independently observed default-parallel and serial six PASS with unchanged native inputs. This supports fixture-isolation attribution without alleging that every possible Busy has that cause.
- The original unsupported-profile behavioral RED and root-reuse behavioral RED remain separate immutable historical evidence. Their SHA values are respectively `a0d1504796bfe4b515a8a4ccee851783b3ee769cd49f3f937a0024400c00250a` and `a1ffa0d1528a03bd29ff1888b565afba24487adb1b57847671f978b121e4c2fd`.

## Accepted native boundary

- Entry/cleanup state is explicit. Initial unsupported rejection can stop an unentered statement without rolling back another existing writer or its committed prefix; detached IO still must be examined. Unsupported legacy adoption rejects before making abort sticky.
- Checked reprepare/reset oracles cover classification of replacement bytecode, cleanup after rejection, failed internal-reset error retention, and preservation of prior finalization. Error returns precede a successful-reset Unentered proof.
- Public subprogram reuse now returns Result, checks aborted/error state first, and requires actual Subprogram origin before cleanup. The root oracle preserves row values, execution/abort discriminants, cleanup state, finalization, and root/write counts; the cached-trigger oracle checks three actual trigger rows and totals.
- Retirement oracles cover late commit/checkpoint, failed IO cleanup, repeated retirement, active-sibling rejection, inactive prepared siblings, and real distinct `db.connect()` connections. Closed/count-once, local locks/state, final handle release, and prohibited primary IO entry assertions are exercised within these tests.
- Core Connection Drop does not call close/checkpoint. The parent explicit-close paths, including their sync/checkpoint behavior, are a separate integration obligation, not automatically repaired by the new native API.

## Mandatory caller proof / non-acceptance boundary

- NativeIoRetirementGuard's IO getter, ptr_eq, and active-count checks **are not the capability proof**. A production caller must actually disable new primary/remote IO, drain all issued bridged callback work under finite safety bounds, and hold database-operation/execution exclusion across retirement and every retained prepared/helper/connection/engine destruction, or retain an effective permanent dispatch tombstone.
- Counts do not enumerate all prepared handles or Arc holders. Provider exclusivity, callback lifetime safety, token acquisition failures, and production adapter routing are not established by these memory-backed core tests.
- The different-connection counter includes primary open/remove/file-id/step/cancel/drain/wait and major File operations, not just writes. Earlier late-IO wrappers have narrower File coverage. Neither is a universal proof over every IO method/backend/scheduling interleaving.
- Pager savepoint cleanup may truncate a separate MemoryIO subjournal. Allowed local-memory cleanup is distinct from prohibited new primary/remote durability IO; this report does not call all retirement operations IO-free.
- `RetiredUnsettled` permits only the conditionally safe local retirement boundary. Original remote unknown/error remains retained and poisoned; it does not become SQL complete, successful close ACK, a proved rollback, or a healthy namespace. No SQL replay is authorized.
- Production guard implementation, owner queue/hint protocol, worker outcome integration, broader fencing/budget regressions, real S3 and actual load require their own frozen independent gates. No historical Busy holder, BusySnapshot, fairness, or performance conclusion is retroactively inferred.

## Evidence handoff

All evidence is under `/tmp/peri-native-final23-independent`; core/current counts are in `counts.json`, exact commands/exits in `results.json` and `unique-fixture/results.json`, source inventories in both before/after pairs, and lock normalization in `lock-review.json`. These are local artifacts, not a claim of portable committed evidence.

New default-parallel log SHA: `2a8ad9359d63bdeb3d844cf8de91351c9a5be386b5c40a436f9f94700e3a8236`.
New serial log SHA: `c98731c3044b809def250d2eb40a7409eced717ca34ac1f640f19386f3cf6909`.

All independent test/build subprocesses have exited. F releases the exclusive Cargo window and will not launch another build/test without new authorization. No S3, API, load, signal, production edit, commit, or push was performed.
