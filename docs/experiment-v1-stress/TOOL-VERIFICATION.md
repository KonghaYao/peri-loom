# P6 stress tools — independent readonly source verification

## Verdict

**Unit/tool gates PASS; CA-read blocker B1 corrected by the author and independently reverified. Real-run authorization and starvation conclusions remain pending.**

This is source review and fresh mock/unit validation only. No real probe, live/native worker signal, load, benchmark, real SQL stress, S3 request or performance measurement was executed. A passing build or test suite is not an architecture/performance result and is not a blanket SAFE certificate.

The initial optional CA loader finding was sent to main and the existing tool author. The author's separately assigned correction is independently verified below; no production or fork source was repaired by this verifier. The release-native build requested separately is a build, not a stress measurement.

## Code provenance and immutability

- Reviewed ROOT package: `/Users/konghayao/code/ai/peri-loom/prototypes/v1/stress` and its separate `faultprobe` module.
- Git HEAD actually read: `b96d700b246c171be29ad431ea12080f3735df77`.
- Git reported `?? stress/`: these are imported, uncommitted sources, not a claimed committed/final experiment revision.
- Go toolchain actually used: `go1.23.4 darwin/arm64`.
- Both modules declare Go 1.23 and standard-library-only dependencies.
- All **39** source/document/module files were hashed before and after the readonly gates; manifests compare byte-for-byte equal.
- Before manifest: `/tmp/peri-p6-stress-tools-before.sha256`.
- After manifest: `/tmp/peri-p6-stress-tools-after.sha256`.
- Both manifest SHA256 values: `9e51f5ab5782c61d9bf8b245855b7f6263704bbd0d4f37e47cbd6c52e8f4c313`.
- No ROOT production source or fork source was edited. The only verifier-authored document is this report; logs/manifests reside in `/tmp`.

## Independently executed gates and raw counts

For each module, fresh `go test -race -count=1 -json ./...`, `go vet ./...`, `go build ./...` and readonly `gofmt -l` were executed. No CLI was launched for a real run. The parent module excludes the nested module; faultprobe was checked separately.

| Module | Parent unit tests | Nested subtests | Test failures/skips | race / vet / build / fmt |
| --- | ---: | ---: | --- | --- |
| `peri-loom/stress` | 27 passed | 13 passed | 0 failed, 0 skipped tests | all exit 0 |
| `peri-loom/faultprobe` | 14 passed | 6 passed | 0 failed, 0 skipped tests | all exit 0 |

The independently verified parent counts match the author's 27/14 claims. There are **41 parent test identities plus 19 nested subtest identities**, not 60 independent live workflows. Each module's command package has no test files; Go JSON reports that package as `skip`, which is not a skipped test or an exercised executable. No race diagnostic appeared in these runs.

Raw logs, not summaries alone:

- `/tmp/peri-p6-stress-race.jsonl`, `/tmp/peri-p6-stress-race.stderr.log`.
- `/tmp/peri-p6-stress-vet.log`, `/tmp/peri-p6-stress-build.log`, `/tmp/peri-p6-stress-fmt.log`, `/tmp/peri-p6-stress-gate-status.log`.
- `/tmp/peri-p6-faultprobe-race.jsonl`, `/tmp/peri-p6-faultprobe-race.stderr.log`.
- `/tmp/peri-p6-faultprobe-vet.log`, `/tmp/peri-p6-faultprobe-build.log`, `/tmp/peri-p6-faultprobe-fmt.log`, `/tmp/peri-p6-faultprobe-gate-status.log`.

## Finding and independent correction recheck

### B1 — original parent CA read bounded only after allocation; now corrected

`stress/client.go:105` calls `os.ReadFile(caFile)` and only afterwards checks `len(pem) > 1<<20`. `ConnectionConfig.Validate` does not restrict the CA to a bounded regular file. Consequently a configured FIFO/device can block or continuously supply data, and an oversized regular file can consume substantially more than the advertised CA bound before rejection. HTTP request timeouts do not cover this synchronous preflight read. This is a source-inspection finding; no blocking device, large allocation or destructive reproducer was executed.

Author action before actual bounded load: reject non-regular/symlink CA inputs and check/read through a bounded opened-file path, with a strict maximum before allocation. Add negative unit coverage for special/oversized CA inputs and rerun readonly gates. The faultprobe CA loader already performs a regular-file/size preflight, although same-UID filesystem replacement remains within its explicitly disclosed trusted-host limitation.

The author changed only `stress/client.go` and added `stress/client_ca_test.go`. Reviewed `readBoundedCA` uses `O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC`, checks the opened descriptor is regular and at most 1MiB, then reads through `LimitReader(1MiB+1)` and rejects an over-limit result. Errors are constant and do not expose paths. Nonblocking open plus descriptor type checks reject a no-writer FIFO without waiting for a writer. Tests actually exercise FIFO, device, symlink, directory, missing file, oversized file, valid PEM exactly at the bound, and malformed PEM at the bound. TLS certificate verification, nil proxy, no redirects and no-retry policy remain intact. This closes B1 within the documented trusted-fixture scope; it is not a universal bounded-latency guarantee for arbitrary failing filesystems/devices or a same-UID adversarial filesystem defense.

Independent fresh parent `go test -race -count=1 -json ./...`, vet, build and readonly fmt all exited 0: **31 parent tests +18 subtests PASS**, zero failed/skipped tests. Together with the unchanged independently tested faultprobe module: **45 parents +24 subtests**. Parent new raw logs are `/tmp/peri-p6-stress-ca-recheck-race.jsonl`, `/tmp/peri-p6-stress-ca-recheck-race.stderr.log`, `/tmp/peri-p6-stress-ca-recheck-vet.log`, `/tmp/peri-p6-stress-ca-recheck-build.log`, `/tmp/peri-p6-stress-ca-recheck-fmt.log` and `/tmp/peri-p6-stress-ca-recheck-status.log`. These supersede the original parent count only; original logs remain historical evidence.

The complete 40-file stress tree (including the unchanged nested module) compares byte-for-byte equal across this recheck. `/tmp/peri-p6-stress-ca-recheck-before.sha256` and `/tmp/peri-p6-stress-ca-recheck-after.sha256` both have digest `ac3b31952706d39d7f127414298d66a135121369a429f2287c170e62398c8be7`. Author-frozen hashes match: client `b5fe9317c701595a5b606a1e6c55515fe72a75fd186943d44b337e099749cb99`; new test `32ea9fdcd1e27280915b3c35fea8a4e46eb727ef945b902d243dc34ca8c901eb`. Actual HEAD remains `b96d700b246c171be29ad431ea12080f3735df77`. No real CLI/probe/load/benchmark was launched. An initial verifier shell invocation failed because a zsh special variable was shadowed; it supplied no gate evidence. The subsequent bash execution supplied the successful logs and manifests above.

## Static safety and evidence boundaries

### Final author-approved faultprobe CA extension

After the parent recheck, the author separately corrected `faultprobe/client.go` and added `faultprobe/client_ca_test.go`. These are the only two changed entries versus the prior 40-file manifest; all parent sources remain unchanged. The same opened-descriptor nonblocking/nofollow regular-file/size guard and bounded read are now present in faultprobe. Independently reviewed negative fixtures cover actual no-writer FIFO, `/dev/null`, symlink, directory, missing file and over-limit input; valid exact-limit PEM is accepted and malformed exact-limit content rejected. TLS verification and no-retry configuration remain enforced. This supersedes the earlier faultprobe CA preflight description, not the disclosed same-UID filesystem/PID limitations.

Fresh nested-module race/vet/build/fmt each exit 0: **18 parent tests +11 subtests PASS**, zero failed/skipped tests. Final combined module counts are **49 parent tests +29 subtests**, using the unchanged parent's fresh 31+18 result. These are unit/mock tests, not 78 independent real workflows. Raw logs: `/tmp/peri-p6-faultprobe-ca-recheck-race.jsonl`, `/tmp/peri-p6-faultprobe-ca-recheck-race.stderr.log`, `/tmp/peri-p6-faultprobe-ca-recheck-vet.log`, `/tmp/peri-p6-faultprobe-ca-recheck-build.log`, `/tmp/peri-p6-faultprobe-ca-recheck-fmt.log`, `/tmp/peri-p6-faultprobe-ca-recheck-status.log`.

The final **41-file** tree is identical before/after this readonly recheck: `/tmp/peri-p6-faultprobe-ca-recheck-before.sha256` and `/tmp/peri-p6-faultprobe-ca-recheck-after.sha256`, both digest `e1afad1ad6b7a0731408d2804b51f731347c28b513a0622656b152624167820b`. Author-frozen hashes match: faultprobe client `00f7ad231518054ba5b7b10bb7da47b7686197187f2b33dba68523f89583a046`, new test `8f4d3bcaba790418b38317cce27c8ae4e2274beec924e0251ad6b76a046f6177`. No production/fork edits or real CLI/probe/load/benchmark were performed by this verifier. Earlier 39/40-file counts and hashes remain historical evidence, not the final freeze hash.

| Concern | What the reviewed code actually enforces | What is not proven |
| --- | --- | --- |
| Exact worker target | `faultprobe/host.go:89` checks topology PID, caller UID, nonempty start identity, exact Rust executable/`owned-worker`/authority/cache/worker/listen argv. The current Management placement selects the owner. Identity is checked again at the signal boundary. | Portable `ps` start data is coarse; checking then signalling an integer PID is not an atomic birth-identity operation. README explicitly discloses this residual TOCTOU. |
| No Authority/platform/launcher kill | `host.go:129` excludes declared Authority, platform, peer worker and launcher PIDs; exact `owned-worker` argv rejects an ordinary Authority process. Parent stress has no signalling or shell-fault operation. | No hard guarantee against an adversarial same-UID PID-reuse race after the final check. No live kill was exercised. |
| Cache/UID/path guard | Config fixes caches to `RunDir/cache-a` or `cache-b`; canonical private fixture directory and cache ownership match caller UID. Run-directory prefix and database prefix restrict supplied fixtures. Scan rejects symlinks/special files and bounds entries/bytes; deletion follows exit proof and `os.SameFile` identity recheck. | Path-based deletion is not a descriptor-relative atomic capability. Same-UID filesystem mutators are outside the documented trusted-fixture guarantee. Prefix alone is not ownership proof of a database. |
| Endpoints / SSRF scope | Faultprobe requires literal loopback service URLs and Gateway/topology agreement. Both transports disable proxies/redirect following, verify HTTPS certificates and use fixed API paths. Parent stress allows only literal-loopback HTTP, but intentionally supports configured remote HTTPS. | Parent HTTPS is not an IP/DNS allowlist or universal SSRF firewall. Configuration remains an explicitly trusted private operator input; server replies do not select a new endpoint. |
| Credential redaction | Private configuration/output modes; recursive artifact sanitizers redact configured secret values and credential-like fields; faultprobe also redacts opaque sessions/lease/grant/DEK fields. Wire failure classifications do not emit credentials as transport errors. Tests exercise embedded secrets, raw metrics and exact integer evidence. | No formal noninterference claim or protection against an adversarial same-UID filesystem reader. Only reviewed/logged fields are covered. |
| Zero SQL retry | No retry loop; `DisableKeepAlives`, nil proxies, no redirect replay, `request.GetBody=nil`; mock tests assert single native/API dispatch on lost/invalid/slow replies. Explicit rollback after a proven terminal SQL error is a new command, not replay. | Read-only placement polls are repeated observations, not SQL/migration retries. No statement deduplication or in-flight cancellation guarantee is invented. |
| True HEAD gate | Parent `gate.go:67` and faultprobe `gate.go:41` read actual `git rev-parse HEAD`; caller gate must match and attest main plus all P0–P6 stages. Missing/mismatched gates stop before execution. | Operator attestation is not a signature or independently verified joint PASS. Dirty/untracked content and executable-to-source identity are not fully bound by HEAD; the parent explicitly calls for a frozen content manifest before comparisons. |
| Unknown outcomes | Lost/invalid/truncated replies conservatively poison SQL sessions; no replay/reuse/blind close of poisoned sessions. Artifact failures prevent subsequent dispatch. Faultprobe owner uncertainty becomes `UNRESOLVED`; starvation uncertainty skips rollback/close and cancels subsequent work. | Already-dispatched work cannot be atomically recalled. Cleanly closing a known session is not proof that all timed-out work was cancelled. |

## Starvation and recovery verdict limitations

- `faultprobe/starvation.go:150` returns `starvation_terminal_and_lease_oracles_unavailable` if observations otherwise finish without a decisive error. `runner.go:138` maps that missing-oracle case to **SKIP**, not PASS.
- Lost/generic `operation_incomplete` management outcomes remain **UNRESOLVED**. Parent/probe mock tests verify that unknown migration outcomes do not trigger retry or an assumed rollback.
- Client migration-pending intervals are not proof of blocking inside a controller/global metadata mutex. Placement renewal sequence is not proof of applied Authority renewal or uninterrupted lease service. Both limitations are stated in the implementation evidence/README.
- A successful DB0 migration while its explicit transaction is held is a failed blocking expectation. Observing third-DB ACKs alone cannot certify a whole starvation PASS.
- Original-grant old-worker revival is explicitly **SKIP**: no public grant-replay route is exercised, no worker restart is invented, and no Authority failure/HA or user's RustFS fault is tested.
- Owner-crash scope is an **ACK barrier with zero in-flight SQL**. Mock tests validate ACK ledger, balance, integrity and newer-owner epoch interpretation. They do not prove actual native recovery after a real SIGKILL/cache deletion or an in-flight crash ambiguity contract.
- Missing/unstarted measurements remain unavailable/null or SKIP, not fabricated zero/passing measurements. Short gate phases are not performance conclusions.

## Release and future execution ordering

The separately requested ROOT host release-native build completed with exit 0 in 47.51 seconds using `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target cargo +1.98.1 build --locked --release --bin peri-loom-v1`. Log: `/tmp/peri-p6-root-release-build.log`. Artifact: `/tmp/peri-loom-p6-dev/prototypes/v1/target/release/peri-loom-v1`, Mach-O 64-bit arm64, SHA256 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`. This was an optimized native build, not a benchmark or performance measurement.

The 160-file Rust source/tests/examples/Cargo manifests are `/tmp/peri-p6-root-release-before.sha256` (digest `2307d9e72f9c3c307a6c27bc763d42b8f34e0da27ac1102aca1cc9d93f59a315`) and `/tmp/peri-p6-root-release-after.sha256` (digest `c8443d10626bc23f9eba3b4fe09ed77fe21320da70e8b0a7242ccb19b86ef42f`). They differ only in the concurrently edited `examples/p6_sdk_smoke.rs`; all other 159 entries, including src/tests/Cargo, are identical. The verifier made no production-source edits. This is not an assertion that the entire package stayed frozen during that build.

After Main's explicit FINAL SOURCE READY notification, ROOT `cargo +1.98.1 fmt --all --check` and `cargo +1.98.1 clippy --locked --all-targets --no-deps -- -D warnings` both exited 0 using the shared target directory. Logs: `/tmp/peri-p6-root-final-fmt.log`, `/tmp/peri-p6-root-final-clippy.log`, `/tmp/peri-p6-root-final-status.log`. Upstream Turso emitted eight dependency warnings; the requested package gate passed without changing Turso. No Go gates or full Rust tests were duplicated.

Final 160-file source manifests `/tmp/peri-p6-root-final-before.sha256` and `/tmp/peri-p6-root-final-after.sha256` compare identical, both digest `c8443d10626bc23f9eba3b4fe09ed77fe21320da70e8b0a7242ccb19b86ef42f`, and equal the release-after manifest. Against release-before, the only changed path remains the example; all 159 non-example entries are unchanged. Reviewed src/Cargo contain no inclusion of that example into the production binary. Release-binary before/after SHA manifests `/tmp/peri-p6-root-final-binary-before.sha256` and `/tmp/peri-p6-root-final-binary-after.sha256` are identical and retain the arm64 artifact SHA stated above. These establish that the host release build is usable for the separately authorized future measurement, not that performance or starvation correctness has passed. No actual pressure run has been executed by this verifier.

Neither a release build nor these unit gates authorizes real pressure. B1 is independently closed; Main must still freeze sources/artifacts, supply the genuine matching joint gate, stop concurrent heavy builds, and decide whether the documented trusted-host PID/filesystem limitations are acceptable. Only then may a separately approved real probe/load produce observations. Tools may be committed with P6 source; starvation correctness/performance conclusions remain pending actual evidence and adequate oracles.
