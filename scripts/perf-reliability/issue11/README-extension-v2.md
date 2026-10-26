# Issue 11 evidence extension v2

The frozen `acceptance.json`, `workloads.json`, SQL and `SHA256SUMS.json` are unchanged. `evidence-extension-v2.json` adds validation requirements; this does not claim new product performance or complete W0-B.

The gate no longer accepts equal self-reported recovery counts as proof. Successful write records contain `write_effect` with the acknowledged request identity, key, value and payload digest. The verifier reconstructs the complete warmup and timed ACK sequence, checks UPDATE deltas and INSERT identities, and compares exact final rows after restart. Missing v2 evidence is INCOMPLETE; wrong identities, values or restart windows FAIL. A valid integrity manifest is not proof of an independently supervised restart.

Supplemental evidence now enforces zero quota violations/Unknown/failures, the frozen concurrent execution limit of 4, closed overload accounting, correct rejection ratio, ordered quantiles and confidence bounds. `overload.completed` means successful completions only; `overload.offered_concurrency` must be 8. `quotas.observed` means peak simultaneous executions. Collectors for capacity and confidence intervals remain absent; validating supplied fields is not measuring those quantities.

## Python v2 HTTP evidence and recovery

The existing `http/runner.py` now emits write identities and `evidence_extension_version=2`. It still requires W0-B server evidence and remains INCOMPLETE for formal acceptance. The supervisor must preserve the database, stop the earlier instance, start a new instance and supply:

```json
{
  "before_instance": "supervisor-observed-old-instance",
  "after_instance": "supervisor-observed-new-instance",
  "method": "process_crash",
  "stopped_at_unix_ms": 1793000000000
}
```

Values above illustrate the schema, not evidence. Supported fault models are `graceful`, `process_crash`, and `storage_fault`; a process restart does not prove power-loss behavior. The verifier performs reads only, validates final values and actual close, and never restarts processes or retries SQL:

```bash
python3 scripts/perf-reliability/issue11/verify_recovery.py \
  --run-directory /absolute/campaign/runs/RUN_ID \
  --endpoint https://127.0.0.1:9443 \
  --ca-file /private/dev-ca.pem --credential-env PERI_SQL_TOKEN \
  --restart-evidence /private/restart.json --attach
```

Without `--attach` it creates a separate `recovery-proof.json`. Attaching updates `oracle.json` and reseals the manifest; the proof retains the source manifest digest. Missing rows, wrong values, stale instance identity, recovery before run completion and unconfirmed close fail. Preserve the supervisor's independent execution records.

## Real JavaScript SDK diagnostic runner

`sdk/runner.mjs` pins **@tursodatabase/serverless 1.4.1** and its npm lock. Source inspection shows that normal SQL uses `/v3/cursor`, with an SDK-added autocommit probe in the same HTTP request; close uses `/v3/pipeline`. The issue did not identify its SDK version, so this is an explicitly versioned SDK diagnostic, **not a claim to reproduce the issue's stated v2 transport**. The Python v2 runner is a separate comparison line. No URL rewriting or substitute SDK is used.

```bash
npm ci --ignore-scripts --prefix scripts/perf-reliability/issue11/sdk
node scripts/perf-reliability/issue11/sdk/runner.mjs --help
# Start Node with NODE_EXTRA_CA_CERTS matching the private connection.json ca_file.
node scripts/perf-reliability/issue11/sdk/runner.mjs \
  --connection /private/connection.json --out /absolute/new-evidence-directory \
  --scenario H4 --samples 2000 --warmup 200 --setup --arm A --source-sha COMMIT_SHA
```

The connection file is the launcher's private fixture file. Credentials are read directly and never printed. `--url https://HOST/db/TENANT/DATABASE/BRANCH --token-env PERI_SQL_TOKEN` is also supported. TLS verification is mandatory; plaintext requires `--diagnostic-plaintext` on loopback. Start the launcher and SDK process in the same network namespace.

`--setup` resets **only the isolated benchmark fixture** (`perf_kv`/`perf_events`); never point it at a business database. Each A/B run needs equivalent fresh fixture state or an explicitly controlled aged state and a new evidence directory. Use H1, H3, H4 or H6; fixture scale is 1000 or 100000.

| Scenario | Measured behavior |
| --- | --- |
| H1 | Reused SDK connection, one SELECT 1 HTTP request |
| H3 | Reused connection, one parameterized UPDATE HTTP request; final values checked after timing |
| H4 | INSERT followed by a separately awaited point SELECT; exactly two HTTP requests |
| H6 | New connection, INSERT, separate SELECT, acknowledged close; exactly three HTTP requests |

The runner calls the SDK's direct `run/get` methods, avoiding an additional describe request from `prepare`. It does not claim this was the issue author's exact API usage. It counts actual requests, checks baton reuse, blocks a second fetch within one API call before dispatch, and stops after Unknown. SDK close swallows some errors, so the observer independently requires a successful server close response. Successful H3 readback is not restart proof.

Outputs are `run.json`, `operations.jsonl`, `requests.jsonl`, `summary.json`, and hashes. All operations, including failures, remain in raw evidence. Both arms use the same response observer; its cloning/parsing overhead is included. These SDK records have an independent schema and are not accepted as v1 formal-gate evidence. The runner reports `formal_pass=false` and `recovery_proven=false`; no service-side performance or durability claim is inferred from mock tests.

Tests:

```bash
cd scripts/perf-reliability
python3 -m unittest discover -p 'test_issue11*.py'
node --test issue11/sdk/runner.test.mjs
```
