# R7: successful native JSON handoff inside the Hrana gateway

Reviewed and implemented against `peri-loom-r7`, base `3703ca696d55e333aff746667fd39125579c4b26`, branch `perf/sqld-parity-round7`. The gateway and Hrana source matched restored R6 before this change. Product code is frozen for the root agent's comparative build; this agent has not committed it.

## Decision and exact slice

Keep the existing internal HTTP request and the complete `Gateway.ServeHTTP` lifecycle. Remove only the successful native result's second gateway envelope roundtrip.

| Location | Change |
|---|---|
| `prototypes/v1/platform/hrana_http.go`, `dispatch` | Replace the internal response recorder with a private `hranaGatewayResponse`; return its result after `ServeHTTP` finishes. Request marshaling, context markers, Authorization header and the HTTP request remain unchanged. |
| `prototypes/v1/platform/gateway_http.go`, `writeResponse` | After the existing stream branch, recognize that exact private writer only for code-free status 0/200 results. Attempt its RawMessage handoff before constructing the gateway envelope. |
| `prototypes/v1/platform/hrana_gateway_response.go` | Accept only a non-nil `json.RawMessage`; run `json.Compact` into an independent buffer. Return this result with status 200. On every noneligible result, use the old recorder and `hranaGatewayReply`. |
| `prototypes/v1/platform/hrana_gateway_response_test.go` | Legacy-versus-new equivalence, ownership, audit/terminal failures, first-page validation and a response-only benchmark. |

The success handoff is reached at the original `writeResponse` call in `ServeHTTP`'s deferred epilogue. The epilogue first settles operations, performs durable audit/unified completion, and can replace success with `outcome_unknown` or `audit_unavailable`. It then writes the response. The adapter receives its result only after `ServeHTTP` returns, so permit/session cleanup and the remaining defers still complete before the next adapter step.

The alternative typed internal request entry was rejected for this round: extracting the handler's request decoding, authentication, admission and epilogue would expand the review surface far beyond removing redundant output JSON work. This change does not remove `httptest.NewRequest`, request JSON encoding/decoding, authentication, native RPC, durable audit, operation storage or client-facing Hrana conversion.

## Semantics and boundaries

All failures, status 202, other success statuses, stream responses, map/struct results, nil RawMessage and malformed RawMessage follow the original envelope encoder and the shared `sqlcontract` decoder. No new failure decoder or error-code predicate was added. In particular, activation still becomes `activation_pending`, and an unencodable/malformed gateway result remains `invalid_gateway_reply` with unknown outcome. Normal external HTTP writers cannot activate the private concrete writer path and retain their original response bytes and headers.

RawMessage is deliberately compacted rather than returned by reference. The old `json.Encoder` with `SetEscapeHTML(false)` compacts the embedded raw payload before the adapter's envelope unmarshal copies it. `json.Compact` uses the corresponding non-HTML-escaping operation, retaining string and number spelling. The new result therefore has the same bytes and length, while owning its memory. This matters because `executeCursor` accumulates `len(reply.result)` against its 8 MiB result budget; preserving just JSON semantic equality would be insufficient.

Existing upstream UTF-8, duplicate-key and depth validation remains before this handoff. `sqlJSONValue` bounds native JSON to 128 levels; the one-level outer-envelope difference at encoding/json's 10,000-level limit is unreachable for a real native reply. The equivalence suite includes the native depth boundary, raw HTML-sensitive characters, U+2028/U+2029, escaped forms, integer/float lexical forms, invalid UTF-8 as a synthetic legacy writer edge, scalar/array/null, and invalid/empty raw values.

`startCursor` sets its private context marker and calls the same `dispatch`, so the fused metadata/first-page reply benefits automatically. It still passes `validCursorFirstResponse` and `MaxResultBytes` before writing. The 32 MiB `sqlWireLimit` remains in native RPC and request decoding. The separate bounded `hranaJSONCursorReply` used by the nonpaged v2-to-v3 conversion is unchanged. The old internal dispatch recorder itself was not bounded by `sqlWireLimit`, and this change neither adds nor removes that limit. Row limits, 8 MiB accumulation, autocommit snapshots, write cursor ACK/rollback/commit and cleanup behavior are untouched.

No SQL text classification, readonly guard or prepared-statement behavior changed.

## Mechanism and expected scope

Per eligible successful raw result:

| Work | Before | After |
|---|---:|---:|
| Raw payload compaction/validation | 1 | 1 |
| Gateway success envelope construction/encoding | 1 | 0 |
| Internal recorder body containing that envelope | 1 | 0 |
| Whole envelope JSON unmarshal | 1 | 0 |
| Owned compact result buffer delivered to adapter | 1 | 1 |
| Authentication, admission, operation/audit decisions, native RPCs | unchanged | unchanged |

The new path saves envelope construction, the recorder's payload-sized body copy, and the subsequent full envelope JSON scan. It is not zero-copy: one compact payload allocation remains. Benefits should grow with result bytes, but upstream validation, multiple adapter decodes and per-cell Hrana conversion remain. The total business p99 also includes connect, native work, durable barriers, response transfer, full result consumption and confirmed close. A microbenchmark improvement cannot establish sqld parity or explain the complete short-query gap.

## Verification completed by this agent

`/workspace/scratch/6757c94bd47c/toolchains/go/bin/go test . -run '^TestHranaGatewayResponse' -count=1` passed in `prototypes/v1/platform` (0.034 s package test time). `gofmt` and `git diff --check` passed.

New tests:

- `TestHranaGatewayResponseMatchesEnvelope`: exact `hranaReply` equality with legacy envelope encoding/decoding; identical status/headers; successful internal envelope body absent; fallback body byte-for-byte equal. Includes status 202, streams, malformed values, known/unknown failure shapes and encoding edge cases.
- `TestHranaGatewayResponseOwnsCompactResult`: exact compact bytes/length and independence from both the source buffer and a subsequent response.
- `TestHranaGatewayResponseWaitsForAudit`: a real internal statement call cannot return its native RawMessage while audit is blocked; one statement RPC and one audit row remain.
- `TestHranaGatewayResponseKeepsCompletionFailures`: real successful native statements followed by audit failure or terminal commit failure return the original failure contract, not their native result; settled audit-failure proof and unknown terminal failure are retained; no SQL replay.
- `TestHranaGatewayResponseKeepsCursorFirstValidation`: valid first page succeeds; malformed shape or a too-small result quota fails unknown; one fused first-page RPC, no split open/fetch or replay.

Root owns the complete Go suite, race run, existing lifecycle/contract regressions and business ABBA measurement to avoid duplicate workloads. The independent reviewer checked the frozen code and is reviewing the new tests.

## Mechanism benchmark entry

`BenchmarkHranaGatewayResultHandoff` has `HTTPEnvelope` and `InternalResult` subbenchmarks for empty first page, 20 rows with 64-byte text, 256 rows with 1 KiB text, and 256 rows with 16 KiB text. Both use the same gateway result and native payload. It reports ns/op, B/op, allocs/op, bytes throughput, and actual `envelope_B/op` observed in the internal recorder. The new path must report zero envelope bytes.

Recommended root command from the platform module:

```sh
/workspace/scratch/6757c94bd47c/toolchains/go/bin/go test . -run '^$' -bench '^BenchmarkHranaGatewayResultHandoff$' -benchmem -count=5
```

This benchmark excludes request construction, authentication, worker RPC, persistence and client result conversion by design. Record its outcome as a mechanism measurement, then separately report endpoint/lifecycle p50 and p99 from the authorized paired business runs. Do not substitute this response handoff ns/op for business p99.
