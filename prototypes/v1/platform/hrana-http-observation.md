# Hrana HTTP boundary observation

The existing Gateway phase trace measures the internal handler invoked through an
in-memory recorder. Its totals do not include all Hrana parsing, response conversion
or public writer work. A difference between client HTTP duration and those totals is
therefore not evidence of a storage, journal or network delay by itself.

With `PERI_SQL_OPEN_OBSERVER=1`, additionally set
`PERI_SQL_OPEN_OBSERVER_HTTP_BOUNDARY=1` to capture the public handler boundary.
The launcher forwards this flag only when `--sql-open-observer` is set. Both profiles
remain off by default, and the boundary flag accepts only `0` or `1`.

The same private bounded ring and sink receive one extra record at handler return:

- `scope` and `action` are `hrana_http`; `request_id` and `outer_request_id` are the
  validated outer request ID. No path, SQL, body, token, tenant or session is recorded.
- `started_unix_ns` and `finished_unix_ns` locate the boundary in wall time. Existing
  inner Gateway records also carry these timestamps. `total_ns` uses monotonic time;
  wall-clock adjustments must not be interpreted as execution duration.
- `outcome` is `handler_returned`. There is deliberately no HTTP `status`: the writer
  is not wrapped or inspected, preserving its optional interfaces and WebSocket
  hijack behaviour. A returned handler is not proof the client received its reply.
- The envelope has no phase spans, zero `accounted_ns`, and `unaccounted_ns=total_ns`.
  These are boundary timings, not an additional execution phase. **Never add an
  envelope total to its inner Gateway totals.** Select empty `scope` for existing
  Gateway analyses and join using `outer_request_id` for boundary comparisons.

The envelope uses the existing duration threshold and fast-success sampling rate as
a duration-only sample. It does not classify HTTP success or failure. Existing inner
Gateway failures retain their unconditional capture. For complete diagnostics use
threshold `0` or sampling `1000`, and inspect the shared footer for overwrites.

For sequential HTTP pipelines, compare `outer total - sum(inner totals)` to locate
time in the Hrana adapter outside Gateway calls; separately compare client duration
to the outer handler total. The latter still combines connection/TLS, transport,
server flush after return, client scheduling and client response consumption. This
does not individually measure those components. A WebSocket envelope is the full
handler lifetime and must not be treated as one SQL request.

Directed tests cover a delayed public writer invisible to inner phases, default-off
zero allocations, bounded overwrite accounting, private sink serialization,
unvalidated request content exclusion, and launcher flag scope. No durability or
SQL dispatch logic changes.
