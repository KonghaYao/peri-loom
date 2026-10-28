# Guarded read-only native boundary

Guarded read-only statement dispatch now records a `native` wait nested inside
its existing `worker` span. This uses the existing optional request-phase
observer. With observation disabled, both span helpers return the shared no-op
function before any clock read, trace allocation or trace lock.

The existing `sendSession` call, probe request ID, writer-state validation and
reply handling are unchanged. A successful read still has one guarded dispatch.
An ambiguous result remains Unknown and is not replayed. Only the existing
proven `requires_intent` refusal can enter the committing path, after durable
intent registration.

Older captures place this particular cross-process wait inside `worker`; the
absence of a `native` span in those records does not mean zero native time.
New captures subtract the nested native duration from worker's exclusive time.
Separate dispatches may produce multiple native spans in one request, such as
the guarded refusal followed by the committing dispatch. They must not be
confused with retrying an ambiguous SQL execution.

`TestRequestPhaseGuardedStatementNativeBoundary` exercises successful read,
transport Unknown and proven writable-refusal paths with the observer both on
and off. It checks the native/worker nesting during the actual dispatch, exact
request ownership, distinct probe IDs, dispatch and durable-commit counts, and
exclusive phase accounting. This is an attribution repair, not a performance
improvement claim or a new durability policy.
