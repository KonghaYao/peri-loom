import datetime
import hashlib
import json

import run_guarded as guarded
from capacity_fifo import database_key
from capacity_metrics import timestamp


EVENT_FIELDS = {"sequence", "monotonicMicros", "kind", "controlId", "holderControlId", "requestId", "operation", "reservedLane", "success", "bytes", "ioId", "terminalSequence", "bytesBefore", "bytesAfter"}
KINDS = {"enqueue", "admit", "owner_reuse", "cancel", "release", "invalidation", "cleanup_start", "cleanup_finish", "backend_start", "backend_finish"}
PAGE_STATUSES = {"available", "gap", "disabled", "lock_unavailable", "stream_unavailable", "sequence_gap", "export_truncated", "unmapped"}
PAGE_NUMERIC_FIELDS = {"requestedAfterSequence", "afterSequence", "nextSequence", "sequence", "oldestSequence", "lost", "evicted", "dropped", "pendingMessages"}
PAGE_FAILURE_REASONS = {
    "collector_page_schema_invalid", "collector_page_enabled_invalid", "collector_page_status_invalid",
    "collector_page_cursor_mismatch", "collector_page_cursor_no_progress", "collector_page_prefix_gap",
    "collector_page_source_or_DB_scope_changed", "collector_page_event_budget", "collector_page_poll_budget_exhausted",
    "collector_native_pending_invalid", "collector_native_events_dropped", "collector_ring_scope_status_unavailable",
    "collector_queue_identity_invalid", "collector_DB_epoch_mapping_invalid", "collector_ring_DB_epoch_or_sequence_changed",
    "collector_event_schema_invalid", "collector_nonopaque_control_identity", "collector_owner_reuse_binding_invalid",
    "collector_event_rewritten", "collector_trace_sequence_gap", "trace_monotonic_window_invalid",
    "collector_transport_or_trace_unavailable",
} | {"collector_page_status_" + status for status in PAGE_STATUSES - {"available", "gap"}}


class PageRejection(ValueError):
    def __init__(self, reason, diagnostic):
        guarded.require(type(reason) is str and reason in PAGE_FAILURE_REASONS
                        and isinstance(diagnostic, dict)
                        and set(diagnostic) <= PAGE_NUMERIC_FIELDS | {"role", "status", "prefixGap"}
                        and diagnostic.get("role") in ("worker", "authority")
                        and type(diagnostic.get("status")) is str and diagnostic["status"] in PAGE_STATUSES | {"invalid"}
                        and all(type(value) is int and 0 <= value < 1 << 64
                                for field, value in diagnostic.items() if field in PAGE_NUMERIC_FIELDS)
                        and ("prefixGap" not in diagnostic or type(diagnostic["prefixGap"]) is bool),
                        "collector_page_schema_invalid")
        super().__init__(reason)
        self.reason = reason
        self.page_rejection = dict(diagnostic)


def page_metadata(role, page, after):
    result = {"role": "authority" if role == "authority" else "worker", "status": "invalid"}
    if type(after) is int and 0 <= after < 1 << 64:
        result["requestedAfterSequence"] = after
    if not isinstance(page, dict):
        return result
    status = page.get("status")
    if type(status) is str and status in PAGE_STATUSES:
        result["status"] = status
    for field in ("afterSequence", "nextSequence"):
        value = page.get(field)
        if type(value) is int and 0 <= value < 1 << 64:
            result[field] = value
    stream = page.get("stream")
    if isinstance(stream, dict):
        for field in ("sequence", "oldestSequence", "lost", "evicted", "dropped", "pendingMessages"):
            value = stream.get(field)
            if type(value) is int and 0 <= value < 1 << 64:
                result[field] = value
        if type(stream.get("prefixGap")) is bool:
            result["prefixGap"] = stream["prefixGap"]
    return result


def wall_time(source, micros):
    guarded.require(type(micros) is int and 0 <= micros <= source["monotonicMicros"], "trace_monotonic_window_invalid")
    value = source["wallUnixMicros"] - source["monotonicMicros"] + micros
    return datetime.datetime.fromtimestamp(value / 1e6, datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def owned_databases(identity):
    result = {json.dumps(database, sort_keys=True): (identity["primary_project"], database) for database in identity["primary_databases"]}
    if identity["noise_project"]:
        result[json.dumps(identity["noise_database"], sort_keys=True)] = (identity["noise_project"], identity["noise_database"])
    return result


def native_traces(metrics):
    native = metrics.get("native", {})
    workers = native.get("workers", {})
    guarded.require(isinstance(workers, dict), "collector_native_workers_missing")
    results = [("worker:" + identifier, value.get("capacity_trace")) for identifier, value in workers.items()
               if isinstance(value, dict) and "capacity_trace" in value]
    authority = native.get("authority", {})
    if isinstance(authority, dict) and "capacity_trace" in authority:
        results.append(("authority", authority["capacity_trace"]))
    guarded.require(bool(results) and len(results) <= 16, "collector_opt_in_native_trace_missing")
    return results


class TraceCapture:
    def __init__(self, identity, workers=None):
        self.databases = owned_databases(identity)
        self.workers = set(workers) if workers is not None else None
        self.sources, self.rings = {}, {}
        self.initial = True

    def ingest(self, metrics, observed_at, fetch_page=None):
        diagnostic = {}
        try:
            return self._ingest(metrics, observed_at, fetch_page, diagnostic)
        except (ValueError, KeyError, TypeError) as error:
            if not diagnostic:
                raise
            reason = "collector_transport_or_trace_unavailable"
            if isinstance(error, ValueError):
                if len(error.args) != 1 or type(error.args[0]) is not str or error.args[0] not in PAGE_FAILURE_REASONS:
                    raise
                reason = error.args[0]
            raise PageRejection(reason, diagnostic) from None

    def _ingest(self, metrics, observed_at, fetch_page, diagnostic):
        updates = []
        for role, trace in native_traces(metrics):
            diagnostic.clear()
            if role.startswith("worker:") and self.workers is not None and role.removeprefix("worker:") not in self.workers:
                continue
            guarded.require(trace.get("schemaVersion") == 1 and trace.get("enabled") is True and trace.get("capacity") == 1024,
                            "collector_native_trace_schema_disabled_or_changed")
            guarded.require(trace.get("exportTruncated", False) is False and trace.get("evictedSources", 0) == 0,
                            "collector_native_export_truncated_or_sources_evicted")
            source = trace.get("source", {})
            guarded.require(set(source) == {"pid", "startUnixMicros", "generation", "wallUnixMicros", "monotonicMicros"}
                            and type(source["pid"]) is int and source["pid"] > 0 and source["generation"]
                            and all(type(source[field]) is int and source[field] >= 0 for field in ("startUnixMicros", "wallUnixMicros", "monotonicMicros")),
                            "collector_native_source_identity_missing")
            base = source["wallUnixMicros"] - source["monotonicMicros"]
            guarded.require(abs(timestamp(observed_at) * 1e6 - source["wallUnixMicros"]) <= 2_000_000, "collector_clock_bridge_outside_poll_window")
            previous = self.sources.get(role)
            guarded.require(previous is None or (source["pid"], source["startUnixMicros"], source["generation"], base) == previous,
                            "collector_native_PID_generation_clock_changed")
            self.sources[role] = (source["pid"], source["startUnixMicros"], source["generation"], base)
            mappings = {entry.get("queueInstance"): entry for entry in trace.get("runtimeMappings", [])}
            rings = [("database", entry) for entry in trace.get("databases", [])] + [("namespace", entry) for entry in trace.get("namespaces", [])]
            guarded.require(len(rings) <= 128, "collector_ring_budget")
            for category, ring in rings:
                diagnostic.clear()
                mapping = mappings.get(ring.get("queueInstance"), {})
                if category == "database" and "runtimeMappings" in trace and not mapping and (role, ring.get("queueInstance")) not in self.rings:
                    continue
                database = mapping.get("databaseId") or ring.get("databaseId")
                if category == "namespace":
                    matches = [value[1] for value in self.databases.values() if value[1]["namespace"] == ring.get("namespace")]
                    guarded.require(len(matches) <= 1, "collector_namespace_branch_ambiguity")
                    database = matches[0] if matches else None
                if database is None or json.dumps(database, sort_keys=True) not in self.databases:
                    continue
                ring = dict(ring)
                guarded.require(type(ring.get("pendingMessages")) is int and ring["pendingMessages"] >= 0,
                                "collector_native_pending_invalid")
                scope = (role, ring.get("queueInstance"))
                if ring.get("events") is None and not self.initial:
                    guarded.require(fetch_page is not None, "collector_read_only_page_bridge_required")
                    after = self.rings.get(scope, {}).get("sequence", 0)
                    events = []
                    for page_index in range(16):
                        diagnostic.clear()
                        page = fetch_page(role, ring["streamId"], after)
                        diagnostic.update(page_metadata(role, page, after))
                        guarded.require(isinstance(page, dict) and type(page.get("schemaVersion")) is int
                                        and page["schemaVersion"] == 1, "collector_page_schema_invalid")
                        guarded.require(page.get("enabled") is True, "collector_page_enabled_invalid")
                        status = page.get("status")
                        status_reasons = {"disabled": "collector_page_status_disabled", "lock_unavailable": "collector_page_status_lock_unavailable",
                                          "stream_unavailable": "collector_page_status_stream_unavailable", "sequence_gap": "collector_page_status_sequence_gap",
                                          "export_truncated": "collector_page_status_export_truncated", "unmapped": "collector_page_status_unmapped"}
                        reason = status_reasons.get(status, "collector_page_status_invalid") if isinstance(status, str) else "collector_page_status_invalid"
                        guarded.require(status in ("available", "gap"), reason)
                        guarded.require(type(page.get("afterSequence")) is int and page["afterSequence"] == after,
                                        "collector_page_cursor_mismatch")
                        guarded.require(isinstance(page.get("stream"), dict), "collector_page_schema_invalid")
                        guarded.require(type(page["stream"].get("pendingMessages")) is int and page["stream"]["pendingMessages"] >= 0,
                                        "collector_native_pending_invalid")
                        guarded.require(page["stream"].get("prefixGap", False) is False, "collector_page_prefix_gap")
                        page_source, stream = page["source"], page["stream"]
                        guarded.require((page_source["pid"], page_source["startUnixMicros"], page_source["generation"], page_source["wallUnixMicros"] - page_source["monotonicMicros"])
                                        == self.sources[role] and stream["queueInstance"] == ring["queueInstance"]
                                        and stream.get("namespace") == ring.get("namespace") and stream.get("databaseId") in (None, database)
                                        and stream.get("scope") == ring.get("scope") and stream.get("epoch") in (None, mapping.get("epoch", ring.get("epoch"))), "collector_page_source_or_DB_scope_changed")
                        guarded.require(isinstance(stream.get("events"), list) and len(stream["events"]) <= 128, "collector_page_event_budget")
                        events.extend(stream["events"])
                        next_sequence = page["nextSequence"]
                        guarded.require(type(next_sequence) is int and next_sequence >= after
                                        and (not page["hasMore"] or next_sequence > after), "collector_page_cursor_no_progress")
                        after, source = next_sequence, page_source
                        ring.update({key: stream[key] for key in ("sequence", "status", "lost", "dropped", "pendingMessages") if key in stream})
                        if not page["hasMore"]:
                            break
                    else:
                        guarded.require(False, "collector_page_poll_budget_exhausted")
                    ring["events"] = events
                elif ring.get("events") is None:
                    ring["events"] = []
                project, database = self.databases[json.dumps(database, sort_keys=True)]
                guarded.require(ring.get("scope") == ("physical_inner" if category == "namespace" else "actualWriterAdmission")
                                and ring.get("status") in ("available", "gap") and isinstance(ring.get("events"), list)
                                and len(ring["events"]) <= 2048 and type(ring.get("sequence")) is int and ring["sequence"] >= 0
                                and all(type(ring.get(field, 0)) is int and ring.get(field, 0) >= 0 for field in ("lost", "dropped", "evicted")),
                                "collector_ring_scope_status_unavailable")
                queue = ring["queueInstance"]
                guarded.require(isinstance(queue, str) and 1 <= len(queue) <= 256, "collector_queue_identity_invalid")
                epoch = mapping.get("epoch", ring.get("epoch"))
                if category == "database":
                    guarded.require(type(epoch) is int and epoch >= 0 and (ring.get("databaseId") is None or ring["databaseId"] == database), "collector_DB_epoch_mapping_invalid")
                scope = (role, queue)
                state = self.rings.get(scope)
                fresh = state is None
                if fresh:
                    state = {"sequence": ring["sequence"] if self.initial else 0, "hashes": {}, "database": database, "epoch": epoch,
                             "lost": ring.get("lost", 0), "dropped": ring.get("dropped", 0)}
                    self.rings[scope] = state
                guarded.require(ring.get("dropped", 0) == state["dropped"], "collector_native_events_dropped")
                guarded.require(state["database"] == database and state["epoch"] == epoch and ring["sequence"] >= state["sequence"], "collector_ring_DB_epoch_or_sequence_changed")
                events = []
                for event in ring["events"]:
                    guarded.require(set(event) <= EVENT_FIELDS and {"sequence", "kind", "monotonicMicros"} <= set(event)
                                    and event["kind"] in KINDS and type(event["sequence"]) is int and 1 <= event["sequence"] <= ring["sequence"], "collector_event_schema_invalid")
                    wall_time(source, event["monotonicMicros"])
                    for field in ("controlId", "holderControlId"):
                        guarded.require(event.get(field) is None or (isinstance(event[field], str) and len(event[field]) <= 256
                                        and event[field].startswith(queue + ":") and event[field].removeprefix(queue + ":").isdigit()), "collector_nonopaque_control_identity")
                    if event["kind"] == "owner_reuse":
                        guarded.require(isinstance(event.get("controlId"), str) and event["controlId"] == event.get("holderControlId"),
                                        "collector_owner_reuse_binding_invalid")
                    encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
                    digest = hashlib.sha256(encoded.encode()).hexdigest()
                    prior = state["hashes"].get(event["sequence"])
                    guarded.require(prior is None or prior == digest, "collector_event_rewritten")
                    state["hashes"][event["sequence"]] = digest
                    if not (fresh and self.initial) and event["sequence"] > state["sequence"]:
                        events.append(event)
                sequences = [event["sequence"] for event in events]
                guarded.require(sequences == list(range(state["sequence"] + 1, ring["sequence"] + 1)), "collector_trace_sequence_gap")
                state["sequence"] = ring["sequence"]
                state["hashes"] = {event["sequence"]: state["hashes"][event["sequence"]] for event in ring["events"]}
                updates.append({"type": "trace_snapshot", "observed_at": observed_at, "role": role, "source": source,
                                "category": category, "project": project, "database": database, "queue": queue, "epoch": epoch,
                                "producer_lost": ring.get("lost"), "sequence": ring["sequence"], "pending_messages": ring["pendingMessages"],
                                "baseline": fresh and self.initial, "events": events})
        self.initial = False
        guarded.require(bool(self.sources), "collector_frozen_native_worker_sources_missing")
        return updates
