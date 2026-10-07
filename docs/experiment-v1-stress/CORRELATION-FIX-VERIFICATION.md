# Correlation and console terminal verification

Independent verifier: Fermat. Date: 2026-10-07. Result: **protocol/client safety gate PASS within the tested scope; no performance conclusion**.

## Source identity

The tested working tree starts from report commit `9819a47`; implementation changes were frozen but not yet committed. No verifier production/test edits were made. `CLAUDE.md` was not touched.

- C's final 16-file manifest: `/tmp/peri-ui-rpc-freeze.json`, SHA256 `dc740977e129d48306ace900e79e1f726f63bc94ea2b6dec0fe58b0fbf13ec9f`.
- B's gateway and E's two client/header-test pairs retained their frozen source hashes.
- The unified 29-owned-file manifest is `/tmp/peri-correlation-f-rerun/owned-{before,after}.json`; both SHA256 `3ce2fe96be208bf06ef4f6223f25044d6efe3457395c3253286d7ae21531f440`.
- The 314-file verification snapshot is `/tmp/peri-correlation-f-rerun/source-{before,after}.json`; both SHA256 `dc3d61a5298665bb6100f12dfe5d38a8307dc577afa3f833236b846067201eb7`. It covers current Rust source/tests/examples/Cargo files and platform/stress source, excluding generated/cache directories; it is not a hash of the entire repository.
- P0's 10 historical native files and all 4,115 tracked Turso files match the preserved baseline proof. Historical native changes and the separate OwnershipView contract are documented in `OWNERSHIP-VIEW-VERIFICATION.md`.

## Independently checked protocol

Gateway validation accepts at most one valid `X-Request-ID` before authentication/admission, and echoes that identifier on early denial. Invalid/duplicate headers fail closed with a safe fallback identifier. Admission still precedes body parsing; a supplied body identifier must match the header. Correlation is neither authentication nor an idempotency key. Successful legacy body-only requests remain compatible; early body-only rejection cannot promise body-ID echo.

Go, TS, Rust, dev Python, UI, stress and faultprobe clients send the same generated identifier in header and body. Strict response checking, unknown-outcome/session fencing, TLS rules and no automatic SQL retry remain in effect. Focused gateway negative tests verify early quota/auth denial without body reads or upstream dispatch, malformed headers, mismatched IDs and compatibility.

The initial UI header patch alone was **not** accepted as a strict-response fix: JSON lacked ID validation and NDJSON EOF without a complete frame could report rows as success. Static review identified a possible false ACK because Query may execute writes. C's authorized follow-up introduced `web/rpc.js` and boundary tests; the old wrong-ID and missing-complete RED evidence remains in `/tmp/peri-ui-rpc-red-{json,stream}.log`.

The final UI checks exact JSON envelope/version/ID and SQL action result shapes. NDJSON requires valid row frames, exactly one matching terminal complete frame with the correct row count, and clean EOF; malformed, duplicate, trailing, missing-terminal, HTTP-error and invalid-UTF8 replies cannot acknowledge success. Rows are buffered until verification completes. Transport/unknown or moved outcomes make the old session unusable for Query, Execute and Close; an uncertain Open is not automatically repeated. A correctly flagged terminal SQL error still permits an explicit rollback. No automatic close, restore or SQL retry was added. Reader cancellation is transport cleanup, not proof that SQL was canceled.

Default UI bounds are 1 MiB request, 8 MiB reply/frame, 10,000 rows and 30 seconds. UI button tests exercise the actual app wiring, including old-session blocking and withholding unverified rows.

## Actual gates and counts

Counts distinguish top-level tests from named subcases; normal/race reruns and native subsets are not additional unique coverage.

| Independent gate | Actual result |
| --- | --- |
| Gateway focused normal and race | Each 15 roots + 23 subcases PASS |
| Final platform `go test -count=1 -json ./...` | 92 roots + 150 subcases PASS; 2 roots SKIP |
| Final platform `go test -race -count=1 -json ./...` | Same counts; PASS |
| Platform breakdown per mode | Platform 82 roots/136 subcases; CLI 3 roots; Go SDK 7 roots/14 subcases |
| Actual native P6 subset per mode | All 7 PASS, included in platform counts |
| Controller normal and race | Each 63 roots + 57 subcases PASS; 1 root SKIP |
| Platform/controller vet; platform build | All exit 0 |
| Readonly Go formatting check | 114 source files; no differences |
| Final UI `node --test tests/ui-*.test.mjs` | 84 PASS; 0 fail/cancel/skip |
| Python dev protocol/runtime | 13 + 8 PASS |
| TS cached compiler typecheck/TMP build/test | 25 PASS; frozen TS source unchanged afterward |
| Stress normal/race and vet | Each 34 roots + 24 subcases PASS; no skips |
| Faultprobe normal/race and vet | Each 20 roots + 18 subcases PASS; no skips |
| Unified Rust release package | 298 unique PASS + 5 ignored; fmt/strict all-target clippy/build exit 0 |

Platform skips are `TestBrokerUnsupportedContinuousClockRefusesStartup` and the process-child helper `TestStateLockChild`; controller's skipped helper is `TestStoreProcessLockChild`. They are not converted to PASS. The seven native tests cover encrypted ACK/recovery, terminal SQL error and same-session rollback, migration/failover, native ReadOnly/scoped tokens, bounded NDJSON/committed-prefix behavior, KEK rewrap/authenticated tamper, and reply-lost unknown outcome retaining admission. Tests used isolated test-owned processes, including their explicitly authorized failure-injection signals; no real measurement fixture was contacted.

Final logs/counts/statuses are `/tmp/peri-correlation-f-rerun/{go-platform-*,go-controller-*,go-counts.json,go-status.log,controller-status.log,ui-test.log,python-*.log,rust-*}`. Earlier independent TS/SDK logs are `/tmp/peri-correlation-f-gates`; focused gateway logs are `/tmp/peri-correlation-f-gateway-focused-*`; unchanged stress/probe logs are `/tmp/peri-correlation-f-{stress,faultprobe}-{normal,race}.*`.

## First failures remain evidence

The first unified Cargo run was **FAIL**: fmt 0, clippy 101, full test 101, release build 0. It remains under `/tmp/peri-correlation-f-gates/rust-*`, not overwritten by the rerun. Strict clippy found A's test-support dead code/type complexity and C's test `err().expect()`; the P4 semantic-read expiry test failed its old total-mutation assertion and then aborted on mutex poisoning. Authorized test-only repairs and their stronger publication assertions are described separately. A successful first release build was not treated as safety acceptance.

E's earlier negative world-readable-file test failure under umask 077 is also retained; independent tool verification used explicit umask 022, so the fixture could actually be world-readable. No old test expectation was silently weakened. Historical Round 2 C8 unknown operations remain unknown; this protocol fix does not retroactively classify their outcomes or invent an uncaptured first 429 body.

## Runnable frozen artifacts

Both are host **arm64**, not debug substitutes:

- `/tmp/peri-stress-platform-correlation-bin`: SHA256 `8692ec8f4b3e4f16bc6190494a61e437ceb92b5a3ffd060ef3503a3e5d80480d`; built with the final UI module embedded.
- `/tmp/peri-stress-native-view-bin`: SHA256 `b3e6d22dd0a16c967c56141f235122d79b7ef7bf25a8438916314d2f7fa6128a`; Rust release artifact, now accepted by these safety gates.
- Preserved `/tmp/peri-stress-native-baseline-bin`: SHA256 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`; never overwritten.

Artifact hashes, architecture and Go build information are recorded in `/tmp/peri-correlation-f-rerun/{final-binaries.sha256,native-architecture.log,go-build-info.log}`. The Go binary truthfully embeds pre-commit `vcs.revision=9819a47f88fd3f51a551f1c01f328563cd5d14c3` and `vcs.modified=true`; this is a content-frozen working-tree build, not a clean-HEAD build. Main's subsequent code commits require matching committed runtime content against this freeze; unchanged runtime content or docs-only commits do not require rebuilding merely to change VCS metadata. All verifier build/test processes completed before handing artifacts back. No real stress, fixture provisioning or performance comparison was run by this verifier. Source changes after this freeze require a new identity check and appropriate gates.
