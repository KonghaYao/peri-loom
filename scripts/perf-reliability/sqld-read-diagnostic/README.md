# SDK read diagnostic against peri-loom and sqld

This opt-in tool uses exactly `@tursodatabase/serverless@1.4.1`. It does not change
the issue 11 runner or existing acceptance gates. Every result records
`formal_pass: false` and `recovery_proven: false`.

The supplied fixture is deterministic **synthetic proxy data**, not issue 15's
`threads-subset.db`, its indexes, six queries or business results. Matching a row
count does not establish workload equivalence.

## Install and seed independent empty test databases

```sh
npm ci --ignore-scripts
npm test
node fixture.mjs --out /absolute/new-fixture --rows 8625 --content-bytes 32

node runner.mjs --manifest /absolute/new-fixture/manifest.json \
  --setup-file /absolute/new-fixture/setup.jsonl --allow-synthetic-fixture-writes \
  --engine sqld --url http://127.0.0.1:8090 --no-auth --diagnostic-plaintext \
  --out /absolute/new-sqld-seed

node runner.mjs --manifest /absolute/new-fixture/manifest.json \
  --setup-file /absolute/new-fixture/setup.jsonl --allow-synthetic-fixture-writes \
  --engine peri --url http://127.0.0.1:8080/db/local_default/demo/main \
  --token-env PERI_SQL_TOKEN --diagnostic-plaintext --out /absolute/new-peri-seed
```

Generate once, then seed each arm from the same file into an independent empty
database reserved for this test. Output directories must not exist. The manifest
records typed full-dataset and expected full-result hashes. Setup contains one
CREATE and INSERT statements of at most 100 rows; no cleanup or DROP runs. Setup
is explicit, separate from measurement, and requires a file whose hash matches a
synthetic manifest. Each write response and close ACK is checked. Measured reads
then verify all result values. This does not prove durability or crash recovery.

`--connection FILE` can replace URL/environment credentials for a peri launcher
JSON containing `endpoint`, `database.namespace.tenant`,
`database.namespace.database`, `database.branch`, `project_api_key` and optional
`ca_file`. For HTTPS, `NODE_EXTRA_CA_CERTS` must equal that CA file. Never pass a
token on the command line. Credentials are not written to output.

## Measure one scenario

```sh
node runner.mjs --manifest /absolute/new-fixture/manifest.json \
  --scenario proxy_narrow --engine sqld --url http://127.0.0.1:8090 \
  --no-auth --diagnostic-plaintext --samples 200 --warmup 20 --arm S \
  --server-identity /absolute/sqld-identity.json --out /absolute/new-sqld-run
```

For peri use its authenticated target arguments above. Add `--source-sha` with
the actual commit. Server identity is supplied provenance, not automatic
attestation: include executable checksum, source commit, host/resource limits,
authentication, persistence and server configuration. The runner preserves this
JSON, manifest hash and SDK lock hash in `run.json`.

| Scenario | Result shape |
| --- | --- |
| `proxy_empty` | Zero rows |
| `proxy_one` | One integer and short string row |
| `proxy_twenty` | Twenty ordered narrow rows |
| `proxy_narrow` | All ordered narrow rows; default fixture has 8625 |
| `proxy_text` | All ordered integer and content rows |

For a larger text diagnostic generate a different fixture with an explicit
content width, e.g. `--content-bytes 2048`. The 32-byte fixture does not exercise
the issue's roughly 20 MB result. Do not silently change server quotas to pass.
Freeze `--timeout-ms` (default 30000) and `--pace-ms` (default 0) before comparison.
Zero pace is sequential concurrency 1 with observer/validation gaps; it is not
maximum sustainable throughput.

Run sides serially to avoid client contention. An exploratory comparison may
predeclare blocks `A,S,B` then `B,S,A`, 200 timed samples and 20 warmups per scenario.
This is not the formal three-round, six-business comparison. Keep all rounds;
failure stops the run and preserves its evidence without rerunning samples.

## Timing, correctness and failures

Each operation creates a logical SDK connection, awaits `all()` with its full
result materialized, then awaits `close()`. Logical connect does not guarantee a
new TCP/TLS handshake. A monotonic client timer starts before logical connect and
ends at the last necessary SDK action's resolution. It includes client setup,
server request processing, network transfer, SDK decode/materialization and
close. It does not assert that async server audit persistence has completed.

The observer clones response bodies. Clone/tee and body collection overlap and
can perturb SDK timing; this is common instrumented diagnostic overhead on both
arms. Observer JSON parsing, baton/close proof, full-result digest and evidence
writes happen after the business clock. `sdk_completed_ms`, `evidence_settled_ms`
and `validation_completed_ms` expose the boundaries. The typed full-result digest
preserves row order and compares every field; it is not a count or prefix check.

Actual HTTP close ACK is required even when the SDK swallows close failure. Each
action permits one network fetch. A second fetch, including the SDK's autocommit
fallback, is blocked before network and fails the action even when SDK cleanup
swallows the exception. HTTP error, Unknown, unconfirmed close, result mismatch or
retry attempt stops the run; failure triggers neither readback nor another try.

Quantiles use nearest rank. At N=200, p99 is the 198th sorted duration, with only
two samples above it; at N=2000 it is the 1980th. It is not the average, maximum,
slowest-1% average or sum of stage p99s. N=200 is weak tail-stability evidence and
cannot establish the formal ≤1.10× target or capacity.

## Evidence and input scope

- `run.json`: identity, supplied server provenance, SDK lock, target and clock contract.
- `operations.jsonl`: every warmup/setup/timed operation, full result hash, action
  clocks, failure/close state and request correlation IDs.
- `requests.jsonl`: actual requests, body/baton hashes, HTTP status, observed
  headers/body times, proof and response bytes; no raw results or token.
- `summary.json`: all timed and successful timed distributions separately,
  stopped-run state, failures, Unknown, retries and confirmed closes.
- `SHA256SUMS.json`: hashes of those four files.

Evidence storage keeps independent `operations.partial.jsonl` and
`requests.partial.jsonl` append files for interruption diagnostics and
retains every original operation and request observation in memory. After the
business clocks end, it first publishes the final `operations.jsonl` and
`requests.jsonl` from those observations using
a new temporary file, file `fsync`, atomic rename and directory `fsync`, then
verifies the resulting bytes. Final raw filenames never receive incremental
appends and cannot be republished or replaced. Partial files are not accepted
measurement evidence. Run/summary metadata use the same publication
method. The SHA manifest is published last. `summary.published_raw` records actual
record counts, byte counts and hashes of the original observation ledger. No
missing records are synthesized from summary statistics. A storage error is
fatal; absent or inconsistent manifests are invalid artifacts. This concerns
evidence-file publication and does not establish database crash recovery.

Custom manifests may declare `dataset_origin: "user-fixture"` with provenance and
complete expected hashes; formal acceptance remains disabled. SQL configuration
is trusted input: use read-only credentials for external manifests. This tool
does not classify arbitrary SQL as safe or recreate missing schema/data. It
rejects insecure TLS overrides, remote plaintext, unexpected endpoints,
redirects and existing output directories.

The mock tests run the real pinned SDK and check both URL forms, whole-result
corruption/order/row loss, close failures, swallowed autocommit fallback, timing
separation, a truncated empty cursor, output hashes, deterministic fixture generation and typed digests.
They prove collector behavior, not database performance.

## Validate a complete comparison

`analyze-comparison.py` checks all final records, file digests, published counts,
full-result hashes, close ACKs, clock order, source/binary identity and the declared
A/S/B/B/S/A sequence before producing statistics. It excludes explicitly named
setup runs and never uses partial logs to fill missing evidence. The default
contract is 200 timed samples plus 20 warmups for each of the four narrow scenarios
in each leg; custom frozen counts can be specified explicitly.

```sh
python analyze-comparison.py --manifest /absolute/fixture/manifest.json \
  --runs /absolute/01-A /absolute/02-S /absolute/03-B \
         /absolute/04-B /absolute/05-S /absolute/06-A \
  --out /absolute/new-analysis.json
python analyze-comparison.test.py
```

A failed integrity check produces no performance statistics. A valid diagnostic
still sets `formal_pass`, `recovery_proven` and `capacity_proven` to false.
