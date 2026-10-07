# Offer-stop independent verification

Verifier: F. Local execution observed 2026-10-08, Asia/Shanghai. **ACCEPT the six frozen Go source/test paths below for this bounded soft-stop stage.** Not acceptance of the still-developing Python wrapper, real external guard delivery, pressure, isolated performance, or whole project. All F builders have stopped.

## Identity and scope
- Author freeze `/private/tmp/peri-offer-stop-author.9osV7RkV/freeze.json` SHA256 `1735cf8582e9b9367e5ac029463400d243f529d4752da6db141db1b57be63c05`; `source-final.json` SHA256 `60749cbeb4f85022f9fcab827214ccbaba1598df8f6bccb81ac837dda0a7405d` independently verified.
- Frozen recursive inventory contains 42 inputs: 25 stress-module inputs plus 17 Go files in the separate nested faultprobe module. F tests/vet/build apply only to the stress module; the 17 sibling files were identity/format checked, not compiled/tested here. Nested faultprobe go.mod is not in this inventory. Do not claim sibling coverage from `go test ./...`.
- F `/tmp/peri-offer-stop-independent/{before,after}.json` identical SHA256 `e66867092caf456c984b37c6b0d4a64b634a34e9d0fd6e993f2a7bc9dda9c5ed`, checked after each command. Current production Parent/Native/platform business sources were not edited or rebuilt; no Cargo/S3/fixture/API/load or external process signals were executed.
- F's first inventory preflight accidentally included nested faultprobe/go.mod: 43 versus frozen 42, with zero changed hashes among shared paths. Corrected the private inventory predicate to the declared set before any Go command. Original private log retained at `/tmp/peri-offer-stop-independent/inventory-preflight-v1.log`; not a production test failure or altered freeze.

## Actual independent gates
Commands ran sequentially in `prototypes/v1/stress`, Go1.23.4 darwin/arm64, `GOPROXY=off`, `GOSUMDB=off`, `GOTOOLCHAIN=local`.

| Gate | Actual exit | Actual result |
| --- | ---: | --- |
| gofmt -l frozen Go files | 0 | Empty output, no formatting edits |
| go test -count=1 -json ./... | 0 | 44 unique root PASS, 0 FAIL, 0 SKIP; 68 terminals including subtests |
| go test -race -count=1 -json ./... | 0 | Same 44 unique root PASS, 0 FAIL, 0 SKIP; 68 terminals including subtests |
| go vet ./... | 0 | Empty output |
| go build ./cmd/peri-stress | 0 | New immutable CLI below |

- Raw commands/exits/hashes: `/tmp/peri-offer-stop-independent/results.json`; normal log SHA256 `3b67f28f555a6d493bfabf8dc74ba8d27a1f5414de6eac12be917357aef12cbf`, race log SHA256 `3ac67769855192bed0a40747d6d8c5f87739758967ec539598f3d66692d462de`. Modes are repetitions of the same 44 cases, not 88 unique coverage; author results are not added.
- Four new cases independently PASS in both modes: admitted-two natural ACK/no third SQL; preclosed stop zero SQL; scenario stops after warmup without smoke/measure/next concurrency or fabricated correctness; Settings JSON excludes the runtime channel. Six inherited phase-drain/hard-cancel cases also execute in the full suite and PASS, including parent-force cancellation, lost commit Unknown/no replay, finite deadlines, known-error drain and artifact hard cancellation.

## Static safety and proof limits
- CLI registers SIGUSR1 separately from the existing SIGINT/SIGTERM parent context; the soft handler closes only OfferStop, once, and passes it through Settings to SerialRunner. This is the real command entrypoint, not merely a library test hook. Hard signals still cancel the parent and retain existing Unknown classification. F did not inject an actual OS SIGUSR1 or exercise a live external wrapper-to-CLI roundtrip.
- `phaseDispatch.admit()` checks both ordinary dispatch cancellation and OfferStop synchronously under its mutex. A job admitted before channel closure remains admitted; later dequeued jobs cannot pass admission. Scheduler timers/worker selection observe the same channel. RPC contexts remain derived from the original phase parent plus unchanged finite RequestTimeout, not the offer-stop dispatch context; no new deadline, replay or cancellation is fabricated by soft stop.
- New tests block two requests inside a fake HTTP RoundTripper, then close stop and release both; assert two actual transport entries, two ACKs, zero canceled/unknown, no third SQL, distinct IDs, preserved ledger mutations, original accounting/rate denominator and successful healthy Close. This is a simulated transport oracle, not actual Native/S3 or physical I/O proof.
- Stopped phase becomes FAIL `operator_stop_offers`, never COMPLETE. Scenario returns on that phase and omits later phases/concurrency and fresh-oracle PASS. Runner checks stop around coarse fixture/scenario boundaries and before final COMPLETE. Fixture initialization and correctness helpers are not per-RPC offer-gated; this acceptance is the business-offer/phase stopping contract, not proof that every setup/control RPC halts immediately upon signal.
- Nil OfferStop preserves legacy behavior; open and closed non-nil channels serialize identically to the default Settings through `json:"-"`. Parent/resource/artifact hard-cancel semantics and original poisoned-session behavior remain. No rollback, poisoned Close, new SQL retry or settings/budget relaxation was added.
- R8 `513e698c6a26984b` remains functional subset ACCEPT / operator STOP / overall PARTIAL / performance INVALID. This new unit/static stage does not rewrite its continued-C4 offers, missing guards or external cargo evidence. P4 historical watchdog failure remains unrootcaused; old Parent391/Go109/S3 results were not rerun here.

## Accepted paths and immutable artifact
Six changed paths, each below 500 lines; SHA256 values match the frozen inventory:
- `prototypes/v1/stress/config.go` (270 lines): `344c868358b74df8293eea38ec83e80b58a320b5344a3555fb1449c970d92214`.
- `prototypes/v1/stress/phase_dispatch.go` (46): `e9be5e3b1ac571cf751406f4214568cb2dc60cfb888223331224d2d6b415b845`.
- `prototypes/v1/stress/phase.go` (261): `df425d77e6b356f1e453a47a0cffa2cfc81995aeb8ee4a96cd97bb0bcd66456c`.
- `prototypes/v1/stress/runner.go` (412): `2871ab3e3731aad94f9e3746440713b29a9702229cbec97f5ddbafb4d8369edd`.
- `prototypes/v1/stress/cmd/peri-stress/main.go` (84): `574c3a7c99171325c46533013f91ad975b2aa10a1d5a21def5093412e28551c7`.
- `prototypes/v1/stress/offer_stop_test.go` (200): `10ddcf5d8e1281469aca875984b3ef53ee2b3339b84d57029c7ceb9ac75ef27e`.
- New `/tmp/peri-stress-offer-stop-bin`, mode0500, 8253826 bytes, SHA256 `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`. Built once after all gates passed, exclusive destination; old phase-drain binary was not overwritten. Build VCS records precommit `09c478be663fff8c90da5bcc0dc697b378d17183`, modified=true, not a clean committed candidate.
- Main alone decides commit and any fresh R9 authorization. Carver Python wrapper/guard continuation, identity-checked signal delivery and future functional-only shared-host run remain separate verification scope. This report is F's only repository edit; no stage/commit/push or CLAUDE change by F.
