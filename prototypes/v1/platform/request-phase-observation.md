# Request phase observation (server-side segment timing)

Date: 2026-10-16. Status: implementation + directed tests + Go build; **no end-to-end
measurement yet** (the parent schedules the frozen-stack run).

## 1. What a read request actually persists (source evidence)

Read path for `query` / `statement` / `execute` / `open` (the E01 C1 families use `query`):

| Step | Code | Durable write? |
| --- | --- | --- |
| Authenticate | `gateway_http.go` -> `Identity.Authenticate` | no |
| Narrow metadata read | `readGatewayRequestState` -> `stateStore.gatewayRequestState` | no (may *wait* for another request's append; `settleOperations`) |
| Admission | `gateway.admission.reserve` / `classify` | no (in-process quota) |
| Session + route | `gateway_sessions.go` `dispatch` -> `gateway.route` -> `controller.ReadRouting` | no (metadata store snapshot) |
| Intent | `beginDispatchOperation` -> `registerOperation` -> `updateOperationRecords` -> `stateStore.updateOperations` -> `journal.submit` + **`file.Sync`** | yes: one journal record + fsync |
| Worker | `callSession` -> `SQLUpstream.Call` -> worker `/sql` | not on this host's disk by the gateway; native side owns its own durability |
| Terminal | `settleOperation` / `settlePendingOperation` -> `updateOperations` -> `journal.submit` + **`file.Sync`** | yes: one journal record + fsync |
| Audit | `gateway.config.Audit.Record` -> `fileAudit.Record` -> `recordBatch` -> **`audit.file.Sync`** (`audit.go`), plus a head commit (`commitAuditBatch`) once per `auditBatchMaximum` records | yes: append + fsync, head fsync every 32 records |
| Response | `writeResponse` | no |

`nonCommittingOperation` (`operation_state.go`) returns true only for `describe`,
`session_state` and read cursors. `query`/`statement`/`execute`/`open`/`close` are committing
actions, so a read **query performs two journal fsyncs** - the intent before dispatch and the
terminal record before the reply - plus the audit fsync. That is a concrete, source-level
answer to "does a read write anything": yes, three barriers.

## 2. Implementation

Default off, no response or log change, no new durable file unless enabled.

| File | Change |
| --- | --- |
| `request_phase.go` (new) | Phase vocabulary, per-request stack accumulator, bounded observer, context binding |
| `request_phase_service.go` (new) | Private exclusive sink (`request-phase-observation.jsonl`) with the same directory/file validation as the existing observer, plus a closing line that states completeness |
| `sql_open_observation.go` | `SQLOpenObserver.phases *requestPhaseObserver` and a nil-safe `requestPhases()`; a fixture that only wants the open observer stays phase-free |
| `service.go` | Creates the phase observer and sink inside the existing profile start, and stops it with the profile |
| `gateway_http.go` | Binds the trace; segments: `auth`, `decode`, `state_read`, `admission`, `terminal`, `audit`, `respond`, plus `entry` computed at finish |
| `gateway_sessions.go` | Segments: `session` (dispatch lookup), `intent` (open and dispatch registration), `worker` (open's upstream call) |
| `gateway_writer.go` | Segment: `worker` around every dispatched worker call (`callSession`, deferred) |

Segments partition the request: a nested span charges its time to no parent, and what no span
covered is reported as `unaccounted_ns` instead of being folded into a neighbour. The stream
carries request id, action, outcome, status and durations only - never a credential, SQL text,
tenant, database or session id.

**Switch.** The private diagnostic profile that already exists: `PERI_SQL_OPEN_OBSERVER=1`,
which the launcher already forwards (`dev.py --sql-open-observer`). The phase stream is a
second sink of that profile, in its own file, so the open observer's artifact keeps exactly the
bytes its consumers already read. A separate switch would need a `dev.py` edit, and `dev.py` is
another owner's dirty file; reusing the forwarded profile avoids touching it. Off is the
default: `dev.py` sets `0` unless the flag is given, and the platform creates neither observer
nor file without `1`.

## 3. Directed tests and build

`GOMAXPROCS=2`, `-p 1`, offline, no Rust binary, no stack:

- `TestRequestPhaseDefaultOff` - no observer: no context value, no-op spans, requests still
  answer; `NewSQLOpenObserver()` alone never hands out a phase observer.
- `TestRequestPhaseRecordsReadSegments` - one real `query` through the gateway fixture reports
  `auth`, `intent`, `worker`, `terminal`, `audit`, `respond` with non-zero time; intent and
  terminal stay separate.
- `TestRequestPhaseRecordsFailureSegments` - a 401 denial still reports `entry` and `respond`
  and never claims a worker call.
- `TestRequestPhaseRecordIsConsistent` - for four concurrent requests: segments sum to
  `accounted_ns`, `accounted + unaccounted == total`, no segment exceeds the total, and each
  request id is recorded exactly once.
- `TestRequestPhaseRecordIsPrivate` - a statement whose SQL carries a secret leaks neither it
  nor the API key, session id or tenant into the record.
- `TestRequestPhaseSinkIsExclusiveAndComplete` - 0600 regular file, a second sink refuses,
  the closing line states completeness, and a disabled sink stops cleanly.

Default-off regression: the gateway/management/audit/admission/state/operation/sql/hrana/
service/telemetry families pass unchanged (only the `*Native*` tests fail, for the missing Rust
binary - NOT MEASURED, not a product failure).

Disabled-path micro-measurement (benchmark, not a system measurement):
`BenchmarkRequestPhaseDisabled` 5.4-5.7 ns/op, 0 B/op, 0 allocs/op for a bind + span + finish;
`BenchmarkRequestPhaseEnabled` ~14.4 ns/op for two spans.

## 4. How the parent runs it

1. Start the frozen stack with the phase profile on, using the frozen Rust binary and the Go
   binary built from this branch:

   ```sh
   python3 prototypes/v1/platform/dev.py --run-dir <fresh> --tls --sql-open-observer
   ```

   `--sql-open-observer` is the existing flag that forwards `PERI_SQL_OPEN_OBSERVER=1`; the
   phase stream lands in `<run-dir>/platform/request-phase-observation.jsonl` (0600).
2. Run the C1 query set unchanged. Each line is one request:
   `{"stream","request_id","action","outcome","status","total_ns","accounted_ns","unaccounted_ns","truncated","spans":[{"phase","ns"}]}`.
   The last line is the observer's own closing record (`action":"observer"`,
   `complete:true|false`) - `false` means the bounded stream stopped early.
3. Attribute: compare per-action `total_ns` against the client's own p50; read the `intent`,
   `worker`, `terminal`, `audit` and `respond` segments per family. `unaccounted_ns` is the part
   no segment covered (TLS handling, correlation parsing, route selection, quota bookkeeping,
   and the defer chain).

## 5. Hypotheses to test with this data (no conclusion yet)

1. **Two journal barriers per read query.** If `intent + terminal` is a large share of the
   p50 while `worker` is small, the fixed cost is the gateway's own durability discipline, not
   the engine. The ordered commit queue only merges *overlapping* appends, and the namespace
   fence serializes same-namespace commits, so a C1 (single-client, sequential) run should show
   no coalescing at all - the queue's benefit needs concurrency to appear.
2. **Audit barrier share.** The audit is already group-committed; in a C1 run it coalesces
   nothing, so `audit` should read as one barrier per request. That is a candidate for batching
   only if the data shows it dominating, and it touches the audit contract.
3. **Worker share.** With `worker` small, the remaining gap to native is gateway plumbing; with
   `worker` large, the gap is the worker RPC and native execution, which is a different
   workstream.
4. **Unaccounted share.** A large `unaccounted_ns` would mean the vocabulary is wrong, not that
   the cost is gone; the next step would be to add the missing segment rather than guess.

## 6. Update: the read barrier, after the first measurement

The measurement above answered hypothesis 1 (intent 4.30 ms, terminal 4.27 ms, audit 4.76 ms of
an 18.98 ms read, against 5.55 ms of worker). Two changes were built on that reading; only the
second is kept, and it is visible in this stream as a new segment:

- **A statement the worker prepared as read-only carries no intent.** The verdict is the engine's
  own `is_readonly` answer to a describe on the session about to execute the statement - never a
  Go-side reading of the SQL text. It is asked inside the new `classify` segment, which therefore
  reads as one describe round trip per statement the gateway has to prove read-only. A describe
  that is refused, unusable or absent keeps the statement committing, so `intent` is present for
  every statement the platform could not prove read-only, and absent for the reads it proved.

  The cache behind this holds the *committing* verdict and nothing else: a statement text a worker
  already proved committing is not asked about again, because skipping the describe only costs the
  intent that statement was going to write anyway. The read-only verdict - the one that removes a
  durable append - is never remembered, so every request that relies on it has asked the worker
  that is about to execute the statement, in this request. A stale entry can therefore never be
  the reason an intent is missing, which is what makes the cache safe to keep without a proof that
  a verdict outlives a schema change, a worker restart or a new session: the only stale entry that
  can exist is a committing verdict, and its consequence is a describe the gateway would otherwise
  have skipped.

- **The terminal record and the audit row were made to overlap, and that was rolled back.** The
  version that overlapped them reported the audit row's `Outcome` from the dispatch result rather
  than from the settled terminal record, which would have let a consumer read "the statement
  succeeded" where the client got `outcome_unknown`, and it moved `Audit.Record` onto its own
  goroutine, outside the recovery boundary the request handler already has. The overlap bought
  about 0.5 ms of an 18 ms read and did not reproduce reliably on this device (the fsyncs of the
  two files are effectively serialized here), which does not pay for either. `audit` and `terminal`
  are therefore again two sequential barriers, `terminal` before `audit`, and the row reports the
  outcome the reply reports.

Net effect on the phases of one read query that the worker proved read-only: `intent` is gone,
`classify` carries one describe round trip, and `terminal + audit` are unchanged. The two barriers
the platform pays for a proven read are the terminal record and the audit row.
