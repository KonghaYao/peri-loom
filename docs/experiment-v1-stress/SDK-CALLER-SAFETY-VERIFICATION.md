# SDK caller helper delta verification

- Decision: **ACCEPT helper code delta/build only; not actual SDK/API acceptance.** Historical full Rust/P4 FAIL and pending GoCLI/UI scopes remain separate.
- Accepted three paths: `prototypes/v1/examples/p6_sdk_smoke.rs`, `prototypes/v1/platform/tests/sdk-live.mjs`, and new `sdk-live.test.mjs`; 264/48/82 lines, each below 500.
- Rust example SHA256 `c2c2f9a9272842e584c646ad2cae7de3ea31dd6ac8c44c3cac4f6310c8cd0135`; TS helper SHA256 `eea43bd9ee6e31af3fad48e00f3670def0b87f2be18dbbb8b331b9300627dc59`; Node test SHA256 `bb9808ee70dfcb8d53ecfbd4bcf2b001cdbb7a2e7247a3701c3774d76a71c137`.
- James confirmed exact frozen identities and author Node evidence `/private/tmp/peri-sdk-ui-final-6dKNFtQS/helper-author-node.log`; author did not run Cargo. F independently executed all tests below.
- Actual Node v26.9.0 builtin tests: five PASS/0 fail, using the actual helper text with only its SDK import replaced by a fake child SDK; no network/API connection.
- Actual Rust +1.98.1 release `--example p6_sdk_smoke` tests: five pure callback PASS/0 fail/0 ignored. Example rustfmt check, strict release example-only clippy --no-deps -- -D warnings, and release example build each exited 0.
- Query Unknown/disconnect and bad oracle stop before Close; valid integer/shape oracle permits exactly one successful Close; Close error preserves failure and cannot print PASS or retry. Both helper tests cover these branches.
- Static failure path has no post-error status/transport probe, unconditional finally/defer Close, SQL replay, or original query error replaced by a cleanup error. Normal pre-query readonly status remains intentional.
- Rust failure retains the original SdkError/stage and emits fixed code only; TS emits PeriError code or helper_precondition, not private raw exception/SQL/token. Exact integer oracle must pass before Close.
- Root `src/sdk.rs` and `src/sdk/{client,session,types}.rs`, public SDK.Close API, production Native/library sources, Cargo manifests/lock and TLS/retry behavior are unchanged; this is caller policy, not a stronger Close API contract.
- Scoped inventory: 959 inherited library/dependency inputs plus three explicit helpers =962; 67 Root integration-test inputs excluded so C's test-only P4 diagnostics do not conflict. Prior 1026 inventory did not capture Root examples; this gate explicitly adds the example instead of claiming that historical coverage.
- All scoped hashes matched before/after, manifest SHA256 both `4349663d215e2eb0cb8e29c96bd96d449eff9ebf122abe392270f9031dbcbd41`; not a whole-working-tree clean/freeze claim.
- New immutable `/tmp/peri-sdk-smoke-caller-safe-bin`, mode0500, SHA256 `16ec821d1ae2c6baeb69c5f17c8f5922adacb6a1ec3815c66929c4f505dc51a9`, matches current `target/release/examples/p6_sdk_smoke`; no previous binary overwritten.
- Raw commands/logs/exits, scope, before/after and artifact proof: `/tmp/peri-sdk-caller-safety-independent/`; artifact JSON SHA256 `cf19387437f76954694f02af7a9290eb97dff742ec5e796276bfc65e7b070565`.
- Builder RELEASED and exact artifact handed to James/main; execution still requires main approval and independent actual SDK evidence. No full Rust rerun, live fixture/API, secret retrieval, real S3, old Unknown operation, performance claim, source edit, commit or push by F.
