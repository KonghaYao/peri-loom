# Issue 28 performance harness

Pure peri-loom measurement for the plan in Issue 28. The harness talks to the
platform's Hrana v3 JSON/protobuf pipeline and cursor routes directly; it does
not use the Peri client, a TypeScript SDK, or local SQLite.

## Contract

- `F1`/`F2`: `SELECT 1`, with and without `want_rows`, both pipeline codecs and
  the cursor codecs.
- `L1`/`L2`: thread list SQL (`LIMIT 40`) with a populated and a missing `cwd`.
- `P1`: single thread by id.
- `H1`: all 8,625 message contents of the focus thread (about 20.2M characters)
  through explicit cursors. Pipeline requests would hit the 4 MiB result budget,
  so H1/H2/H3 deliberately use cursor mode only.
- `H2`/`H3`: projection-only and content+projection cursor reads. If the
  snapshot has no projection payload, the report marks these as non-representative.
- `R1`: `LIMIT 100/500/1000/2000/5000/8625`; `result_limit` is the expected
  outcome once the result budget is crossed.
- `B1`: 20 `SELECT 1` executes in one pipeline; `C1`: 10 executes on one baton.
- `W1`: create/insert/delete/drop table through both pipeline codecs.
- `X1`: 100 requests at concurrency 1/4/8/16. It requires
  `max_concurrent >= 16`; the report fails the level on 429s or non-zero
  `active_permits`/`unknown_held_permits`/`session_seats`.

Every request carries a unique `X-Request-ID`. The harness never retries SQL.
Cursor streams are closed with a baton pipeline request before the next sample;
a cursor close is recorded but not included in the measured duration.

## Run

The target must have `PERI_HRANA_EXPERIMENTAL=1`. Provide the private
`connection.json` produced by `dev.py`; the harness redacts credentials and
does not write them to the evidence.

```bash
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file /private/path/connection.json \
  --container <docker-container> \
  --out /tmp/issue28-results \
  --rounds 3 --warmup 1 --samples 5 \
  --post-idle-seconds 30
```

Useful options:

```bash
# Inspect the plan and dataset checks without sending benchmark requests.
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file /private/path/connection.json --dry-run

# Run one scenario or one protocol.
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file /private/path/connection.json \
  --scenario H1 --protocol json-cursor
```

The harness expects the Issue 28 fixed snapshot. It verifies 201 threads,
8,625 messages, 20,177,069 message characters, and 8,625 focus-thread messages.
A mismatch marks the report `N/A`; the run is still written for exploration but
must not be used as a regression verdict.

## Output

`<out>/summary.md` contains the environment, dataset checks, the per-scenario
table, per-round medians, the concurrency table, and warnings. `summary.json`
and `raw.jsonl` are the machine-readable evidence. `resource_before/after` and
`resource_after_idle` are populated when `--container` is passed.

## Tests

Offline wire, statistics, and report tests:

```bash
python3 -m unittest scripts/perf-reliability/test_issue28_support.py
```

The tests do not open sockets. Measured numbers require a running platform.
