# Writer-admission Parent batch independent verification

Date: 2026-10-07 (Asia/Shanghai). Verifier: F. **ACCEPT this frozen Parent code batch for main-controlled commit; not whole-project, deployment, fairness or pressure/performance PASS.** No remaining blocker observed in this gate scope. All F builders have stopped; no fixture/SQL load was launched by F.

## Source and provenance
- Rust closure: 1026 Rust/Cargo inputs; `/tmp/peri-parent-author-inputs-v14.json` SHA256 `dec38078fd78f2fb4e58745624e4db5eeafaaab1c95119dd92ae800374363ecb`; handoff v5 SHA256 `a96adf886ceb15051a7684eb2e47b752c83b5646c55b8768467907e744510cfc`. All 32 A-owned Rust paths match frozen SHA/line counts below 500.
- Rust before/after proof: `/tmp/peri-parent-full-independent-v4/{before,after}.json`, identical SHA256 `f9e3745919f38f9d45f0d8a2488d1eee69798de8a4fcbb88009860b3ea343a28`. Native child `ad892d4f113b2e62544bef48be6cebfd6e20da52` clean; prior conditional Core43 is not a new whole-parent gate.
- Go/platform closure: 133 inputs including local controller dependencies and embedded web assets; `/tmp/peri-parent-go-independent-v1/{before,after}.json` identical SHA256 `676e3523ae4bf4cce6c12d3b0a86c8ace4296848c90426597de8025f99f6003c`. New actual-profile test: 297 lines, SHA256 `becf11b82d2db1c6363404a89da6ae38dec83fcdc4430d6dbd21a33910cd2648`.
- F rehashed both closures and Python inputs after all tests/S3 evidence arrived: unchanged. Proof `/tmp/peri-python-profile-independent-v1/final-audit.json`. Markdown is outside compiled closures; parent WIP is not git-clean. Go binary records precommit VCS `ca9b918d68c95f21b9f4262ea4e9dc91dcc602a8`, `vcs.modified=true`; this does not mean that commit contains the candidate sources.

## Actual gates and evidence
- F Rust `+1.98.1`, host `aarch64-apple-darwin`, target `/tmp/peri-loom-p6-dev/prototypes/v1/target`: fmt check, locked/offline all-targets strict clippy `--no-deps -- -D warnings`, locked/offline release full tests `--no-fail-fast`, and release `--bin peri-loom-v1`: every actual exit 0. Commands/raw hashes/exits in `/tmp/peri-parent-full-independent-v4/results.json`.
- Rust: **396 unique harness/name cases = 391 PASS, 0 FAIL, 5 ignored**, 37 harnesses. Nested self-spawn names are deduplicated, not extra tests. Test log SHA256 `a695df913e2bf5ffca3abe97a0ff766340e68d48d621e83ff473bcb4c73c7811`; ignored real-S3 tests were not called PASS by this Rust run.
- F Go/platform: normal and race each **109 root PASS, 2 root SKIP, 0 FAIL; 139 subcase PASS**. Eight actual-native tests each PASS in both runs: previous seven P6 real-native cases plus `TestGatewayWriterNativeV2ReservedProgress`. These are two modes of the same tests, not double unique coverage. Race child CLI explicitly used `PERI_P6_VERIFY_RACE_CLI=1`; both used current Native below. Normal/race/vet/build all actual exit 0; raw/results/counts in `/tmp/peri-parent-go-independent-v1/`. Race log SHA256 `e50899b8a523a48ef3f95ee48c9eaa927228aa80b0c6faafcdaefa78732336a9`.
- F Python: independent `python3 tests/dev-{protocol,runtime,writer-profile}.test.py -v` actual **13+8+5=26 unique PASS**, each exit 0. `/tmp/peri-python-profile-independent-v1/results.json`; before/after SHA256 `d92b2eb863715f5eca87ceaa110a3b1be96dc7c1c26ed1f8d2e9896e42fa5a20`. B final freeze SHA256 `e3fde8a46a6fef58232df627bd790ae23b09145a3316b9e46c8db5e3b2386b88` verified. Scope is mocked HTTP/profile plus local OpenSSL/isolated Python child, no Rust/Go builder or real service.
- Main executed current direct-built real S3, session 66340: **5 PASS, 0 FAIL, 0 ignored, 3 filtered**, actual child exit 0; wrapper elapsed 9.595714959315956 s, harness 8.97 s. F independently read the five distinct names/raw summary/result and matched immutable copy to current compiled executable, both SHA256 `45da78394e8952dce7dbc6e8053caabcfb7e4997d7e00c60a63d76ed3a693a18`. F did not execute S3 or access credentials.
- S3 evidence: `/private/tmp/peri-parent-current-s3-main-j1wovza_/{tests.log,result.json}`, directory 0700/files 0600. Raw SHA256 `a065642da636a1ae313813ba2c555f8d210065ef69c6227a51a332c574f435d2`; result SHA256 `b7f19092c8b4c6bf8de7915427f4c9346cd3e0de0d720c7abe56a1789df6376f`. Env-only credentials, negative literal scans, unique fresh test buckets and no container/backend changes are main-executor attestation; F verifies recorded evidence, not an independent secret scan/backend inventory.

## Safety scope and preserved first failures
- Static review retains default-off/low-quota legacy profile, reserved capacity within existing quotas, exact private monotonic owner hints, strict public correlation/unknown poison, original fences/budgets and no SQL replay. Native owner retention and reserved COMMIT/ROLLBACK progress are demonstrated by the new real-native Go case, not inferred from SQL text.
- Retirement uses genuine new-IO exclusion/drain plus DB-operation exclusion and all-connection checks including both MAIN/TEMP pagers. Stopped statement cleanup does not replace connection retirement proof; local memory release never turns remote Unknown into ACK. Synchronous submitted-leaf IO errors are bridged without fabricated success/group completion. Wire-bound reclamation is not a global cap on trusted library sessions; FIFO/starvation or adversarial shutdown completeness is not proved by finite passing tests.
- Compaction preflight rejects pinned/busy registered peers before dispatch under DB write exclusion; actual native checkpoint error and incomplete backfill stay conservative Unsettled. Two partial-classification unit cases are not a real engine partial-backfill oracle. RemoteDB old test contract migration requires healthy TempFile success before the fault and same-IO rejection after retirement; original SQL/fault/path-denial assertions remain.
- Keep `/tmp/peri-parent-full-independent-v1/` seven unique strict lint failures; v2 full-release three unique failures (watchdog, retired TempFile expectation, session quarantine); v3 verifier preflight miscount (not production failure) and one shared-helper lint (not three failures). No log is overwritten. The watchdog historical failure remains **unrootcaused**: three repeat exact-case author passes, author parallel P4 20 passes and current F full-release pass do not prove a race fix.
- Keep author Python original 3ERROR/2ERROR logs under `/private/tmp/peri-python-writer-smoke.pea8EJpK/`; fixture-only `writer_admission=False` is not retroactive PASS. Author 77/53 results are not added to F unique totals. Controller/UI/SDK standalone matrices were not rerun; inherited unchanged identity is not a fresh execution claim. Historical pressure failures remain intact; **new whole-pressure/R8 NOT RUN here**.

## Immutable handoff artifacts
- Native `/tmp/peri-stress-native-writer-admission-bin`, mode 0500, SHA256 `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`.
- Go CLI `/tmp/peri-stress-platform-writer-admission-bin`, mode 0500, SHA256 `edf09e768c5845918ec007f8dbf699cd4e4f930eacf0af1d11e77f28c977b14c`, darwin/arm64, Go 1.23.4. Neither old baseline nor historical candidate artifacts were overwritten.

## Exact accepted changed source paths
75 paths below; per-path frozen SHA256 inventory `/tmp/peri-python-profile-independent-v1/accepted-source-paths.json`, SHA256 `4feb8395b9794a4dcd85377c373f87d77e9c2f8b9525f2a782e17252dab40781`. This report is the only F repository write. Author implementation/safety/testplan Markdown requires main documentation review separately; **CLAUDE.md excluded and untouched**. Commit/push decisions belong to main.

- `prototypes/v1/platform/admission.go`
- `prototypes/v1/platform/admission_writer_test.go`
- `prototypes/v1/platform/dev.py`
- `prototypes/v1/platform/gateway.go`
- `prototypes/v1/platform/gateway_http.go`
- `prototypes/v1/platform/gateway_sessions.go`
- `prototypes/v1/platform/gateway_writer.go`
- `prototypes/v1/platform/gateway_writer_boundary_test.go`
- `prototypes/v1/platform/gateway_writer_fixture_test.go`
- `prototypes/v1/platform/gateway_writer_native_integration_test.go`
- `prototypes/v1/platform/gateway_writer_test.go`
- `prototypes/v1/platform/model.go`
- `prototypes/v1/platform/service.go`
- `prototypes/v1/platform/service_config.go`
- `prototypes/v1/platform/sql_upstream.go`
- `prototypes/v1/platform/sql_upstream_profile.go`
- `prototypes/v1/platform/sql_upstream_writer.go`
- `prototypes/v1/platform/sql_upstream_writer_test.go`
- `prototypes/v1/platform/tests/dev-runtime.test.py`
- `prototypes/v1/platform/tests/dev-writer-profile.test.py`
- `prototypes/v1/src/database.rs`
- `prototypes/v1/src/database/connections.rs`
- `prototypes/v1/src/database/reclamation.rs`
- `prototypes/v1/src/database/reclamation/late_settled_tests.rs`
- `prototypes/v1/src/database/reclamation/peer_tests.rs`
- `prototypes/v1/src/database/reclamation/simultaneous_tests.rs`
- `prototypes/v1/src/database/reclamation/temp_tests.rs`
- `prototypes/v1/src/database/reclamation/tests.rs`
- `prototypes/v1/src/database/retirement.rs`
- `prototypes/v1/src/io.rs`
- `prototypes/v1/src/io/native.rs`
- `prototypes/v1/src/io/native/tests.rs`
- `prototypes/v1/src/io/native_file.rs`
- `prototypes/v1/src/io/native_file/native_tests.rs`
- `prototypes/v1/src/io/native_file/tests.rs`
- `prototypes/v1/src/ownership/context.rs`
- `prototypes/v1/src/ownership/handle.rs`
- `prototypes/v1/src/ownership/runtime.rs`
- `prototypes/v1/src/runtime.rs`
- `prototypes/v1/src/runtime/handle.rs`
- `prototypes/v1/src/runtime/quarantine.rs`
- `prototypes/v1/src/runtime/quarantine/tests.rs`
- `prototypes/v1/src/runtime/registry.rs`
- `prototypes/v1/src/service_cli.rs`
- `prototypes/v1/src/session.rs`
- `prototypes/v1/src/session/admission.rs`
- `prototypes/v1/src/session/admission/tests.rs`
- `prototypes/v1/src/session/budget.rs`
- `prototypes/v1/src/session/drive.rs`
- `prototypes/v1/src/session/drive/tests.rs`
- `prototypes/v1/src/session/execution.rs`
- `prototypes/v1/src/session/lane.rs`
- `prototypes/v1/src/worker_rpc.rs`
- `prototypes/v1/src/worker_rpc/admission.rs`
- `prototypes/v1/src/worker_rpc/diagnostics.rs`
- `prototypes/v1/src/worker_rpc/dispatch.rs`
- `prototypes/v1/src/worker_rpc/outcome.rs`
- `prototypes/v1/src/worker_rpc/protocol.rs`
- `prototypes/v1/src/worker_rpc/server.rs`
- `prototypes/v1/src/worker_rpc/server/shutdown.rs`
- `prototypes/v1/src/worker_rpc/session_entry.rs`
- `prototypes/v1/src/worker_rpc/sessions.rs`
- `prototypes/v1/src/worker_rpc/transport.rs`
- `prototypes/v1/tests/busy_tuning.rs`
- `prototypes/v1/tests/busy_wait.rs`
- `prototypes/v1/tests/concurrent_sessions.rs`
- `prototypes/v1/tests/p4_runtime_helpers/remote.rs`
- `prototypes/v1/tests/p4_runtime_helpers/scenarios.rs`
- `prototypes/v1/tests/remote_database.rs`
- `prototypes/v1/tests/writer_admission.rs`
- `prototypes/v1/tests/writer_admission/lifecycle.rs`
- `prototypes/v1/tests/writer_admission/queue.rs`
- `prototypes/v1/tests/writer_admission/support.rs`
- `prototypes/v1/tests/writer_admission/transactions.rs`
- `prototypes/v1/tests/writer_worker_rpc.rs`
