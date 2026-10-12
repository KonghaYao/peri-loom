import copy
import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import ssl
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import run_guarded as guarded
from capacity_collect import capture, cleanup_events, io_events, join_rpc, normalize, queue_tickets
from capacity_plan import digest, write_private
from capacity_trace import PageRejection, TraceCapture, wall_time
from capacity_transport import HTTPRejection, MetricsReader, MAX_RESPONSE
from capacity_fifo import database_key, effective_fifo
from capacity_resources import ContainerResources, phase_resources, bind_native_source
from capacity_metrics import resource_window, writer_profiles


DATABASE = {"namespace": {"tenant": "owned", "database": "db"}, "branch": "main"}
IDENTITY = {"primary_project": "project", "primary_databases": [DATABASE], "noise_project": None, "noise_database": None}
BASE = 1791417600000000


def event(sequence, kind="enqueue", micros=None, **fields):
    return {"sequence": sequence, "monotonicMicros": micros or sequence * 1000, "kind": kind,
            "controlId": "queue:1", **fields}


def metrics(events, sequence=None, **fields):
    ring = {"queueInstance": "queue", "databaseId": DATABASE, "epoch": 1, "scope": "actualWriterAdmission",
            "status": "available", "lost": 0, "dropped": 0, "pendingMessages": 0,
            "sequence": sequence if sequence is not None else len(events), "events": events, **fields}
    trace = {"schemaVersion": 1, "enabled": True, "capacity": 1024,
             "source": {"pid": 42, "startUnixMicros": BASE, "generation": "42-start", "wallUnixMicros": BASE + 1000000, "monotonicMicros": 1000000},
             "status": "available", "databases": [ring], "namespaces": []}
    return {"native": {"workers": {"worker": {"capacity_trace": trace}}}}


def observed(value):
    source = value["native"]["workers"]["worker"]["capacity_trace"]["source"]
    return wall_time(source, source["monotonicMicros"])


def linux_metrics(value):
    value = copy.deepcopy(value)
    workers = value["native"]["workers"]
    primary = workers.pop("worker") if "worker" in workers else workers["worker-a"]
    workers["worker-a"] = primary
    for role, pid in [("worker-b", 43), ("authority", 40)]:
        trace = copy.deepcopy(primary["capacity_trace"])
        trace["source"].update(pid=pid, generation="UNIT-" + role)
        trace["databases"] = []
        if role == "authority":
            value["native"]["authority"] = {"capacity_trace": trace}
        else:
            workers[role] = {"capacity_trace": trace}
    return value


class TraceTests(unittest.TestCase):
    def test_native_owner_reuse_is_retained_without_new_FIFO_admission(self):
        native_events = [event(1, "enqueue"), event(2, "admit"),
                         event(3, "enqueue", controlId="queue:2", holderControlId="queue:1"),
                         event(4, "cancel", controlId="queue:2", holderControlId="queue:1"),
                         event(5, "owner_reuse", holderControlId="queue:1"), event(6, "release")]
        for paged in (False, True):
            with self.subTest(paged=paged):
                collector = TraceCapture(IDENTITY)
                first = metrics([], streamId="queue")
                if paged:
                    first["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]["events"] = None
                collector.ingest(first, observed(first))
                second = metrics(copy.deepcopy(native_events), streamId="queue")
                reader = None
                if paged:
                    trace = second["native"]["workers"]["worker"]["capacity_trace"]
                    ring = trace["databases"][0]
                    ring["events"] = None
                    pages = [{"schemaVersion": 1, "enabled": True, "status": "available", "afterSequence": after,
                              "nextSequence": next_sequence, "hasMore": has_more, "source": trace["source"],
                              "stream": {**ring, "events": copy.deepcopy(selected)}}
                             for after, next_sequence, has_more, selected in
                             [(0, 3, True, native_events[:3]), (3, 6, False, native_events[3:])]]
                    reader = Mock(side_effect=pages)
                stream = collector.ingest(second, observed(second), reader)[0]
                self.assertEqual(stream["events"], native_events)
                self.assertEqual(stream["sequence"], 6)
                selected = [{**entry, "observed_at": wall_time(stream["source"], entry["monotonicMicros"])} for entry in stream["events"]]
                tickets = queue_tickets(stream, selected)
                self.assertEqual([ticket["ticket"] for ticket in tickets], [1, 3])
                self.assertEqual([ticket["admitted_order"] for ticket in tickets], [2, None])
                self.assertEqual([ticket["valid"] for ticket in tickets], [True, False])
                self.assertEqual(effective_fifo(tickets, {database_key("project", DATABASE)})[0], "NOT MEASURED")
                if paged:
                    self.assertEqual([call.args for call in reader.call_args_list], [("worker:worker", "queue", 0), ("worker:worker", "queue", 3)])

    def test_owner_reuse_binding_and_unknown_event_schema_remain_fail_closed(self):
        cases = [
            (event(1, "owner_reuse"), "collector_owner_reuse_binding_invalid"),
            (event(1, "owner_reuse", controlId=None, holderControlId="queue:1"), "collector_owner_reuse_binding_invalid"),
            (event(1, "owner_reuse", holderControlId="queue:2"), "collector_owner_reuse_binding_invalid"),
            (event(1, "owner_reuse", holderControlId="foreign:1"), "collector_nonopaque_control_identity"),
            (event(1, "owner_reused", holderControlId="queue:1"), "collector_event_schema_invalid"),
            (event(1, "owner_reuse", holderControlId="queue:1", rawSession="NOT-LIVE-secret"), "collector_event_schema_invalid"),
            (event(True, "owner_reuse", holderControlId="queue:1"), "collector_event_schema_invalid"),
            (event(2, "owner_reuse", holderControlId="queue:1"), "collector_event_schema_invalid"),
        ]
        for selected, reason in cases:
            with self.subTest(reason=reason, selected=selected):
                collector = TraceCapture(IDENTITY)
                first = metrics([])
                collector.ingest(first, observed(first))
                second = metrics([selected], sequence=1)
                with self.assertRaises(ValueError) as raised:
                    collector.ingest(second, observed(second))
                self.assertEqual(str(raised.exception), reason)

    def test_capture_continuity_dedup_and_safe_already_captured_eviction(self):
        collector = TraceCapture(IDENTITY)
        first = metrics([event(1)])
        self.assertEqual(collector.ingest(first, observed(first))[0]["events"], [])
        second = metrics([event(1), event(2, "admit")])
        self.assertEqual([entry["sequence"] for entry in collector.ingest(second, observed(second))[0]["events"]], [2])
        third = metrics([event(2, "admit"), event(3, "release")], sequence=3, status="gap", lost=1)
        self.assertEqual([entry["sequence"] for entry in collector.ingest(third, observed(third))[0]["events"]], [3])
        broken = metrics([event(5)], sequence=5, status="gap", lost=3)
        with self.assertRaisesRegex(ValueError, "sequence_gap"):
            collector.ingest(broken, observed(broken))

    def test_new_stream_during_run_is_collected_from_sequence_one(self):
        collector = TraceCapture(IDENTITY)
        empty = metrics([])
        empty["native"]["workers"]["worker"]["capacity_trace"]["databases"] = []
        collector.ingest(empty, observed(empty))
        value = metrics([event(1), event(2, "admit")])
        self.assertEqual(len(collector.ingest(value, observed(value))[0]["events"]), 2)

    def test_source_epoch_rewrite_drop_and_export_errors_fail_closed(self):
        for mutate, reason in ((lambda ring: ring.update(epoch=2), "epoch"),
                               (lambda ring: ring.update(dropped=1), "dropped"),
                               (lambda ring: ring["events"][0].update(kind="admit"), "rewritten")):
            collector = TraceCapture(IDENTITY)
            first = metrics([event(1)])
            collector.ingest(first, observed(first))
            mutate(first["native"]["workers"]["worker"]["capacity_trace"]["databases"][0])
            with self.assertRaisesRegex(ValueError, reason):
                collector.ingest(first, observed(first))
        for field, value in (("exportTruncated", True), ("evictedSources", 1), ("enabled", False)):
            first = metrics([])
            first["native"]["workers"]["worker"]["capacity_trace"][field] = value
            with self.assertRaises(ValueError):
                TraceCapture(IDENTITY).ingest(first, observed(first))

    def test_paged_directory_is_bounded_and_cursor_checked(self):
        collector = TraceCapture(IDENTITY)
        first = metrics([], streamId="queue")
        first["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]["events"] = None
        collector.ingest(first, observed(first))
        second = copy.deepcopy(first)
        ring = second["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]
        ring["sequence"] = 2
        page = {"schemaVersion": 1, "enabled": True, "status": "available", "afterSequence": 0,
                "nextSequence": 2, "hasMore": False, "source": second["native"]["workers"]["worker"]["capacity_trace"]["source"],
                "stream": {**ring, "events": [event(1), event(2, "admit")]}}
        reader = Mock(return_value=page)
        self.assertEqual(len(collector.ingest(second, observed(second), reader)[0]["events"]), 2)
        reader.assert_called_once_with("worker:worker", "queue", 0)
        page["afterSequence"] = 99
        with self.assertRaisesRegex(ValueError, "cursor"):
            collector.ingest(second, observed(second), reader)

    def paged_fixture(self):
        collector = TraceCapture(IDENTITY)
        first = metrics([], streamId="queue")
        first["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]["events"] = None
        collector.ingest(first, observed(first))
        second = copy.deepcopy(first)
        trace = second["native"]["workers"]["worker"]["capacity_trace"]
        ring = trace["databases"][0]
        ring["sequence"] = 2
        page = {"schemaVersion": 1, "enabled": True, "status": "available", "afterSequence": 0,
                "nextSequence": 2, "hasMore": False, "source": copy.deepcopy(trace["source"]),
                "stream": {**ring, "events": [event(1), event(2, "admit")]}}
        return collector, second, page

    def test_page_failure_reasons_are_typed_fixed_and_fail_closed(self):
        mutations = [
            (lambda page: page.update(schemaVersion=None), "collector_page_schema_invalid"),
            (lambda page: page.update(schemaVersion=True), "collector_page_schema_invalid"),
            (lambda page: page.update(schemaVersion="1"), "collector_page_schema_invalid"),
            (lambda page: page.pop("schemaVersion"), "collector_page_schema_invalid"),
            (lambda page: page.update(enabled=False), "collector_page_enabled_invalid"),
            (lambda page: page.update(enabled=1), "collector_page_enabled_invalid"),
            (lambda page: page.pop("enabled"), "collector_page_enabled_invalid"),
            (lambda page: page.update(afterSequence=99), "collector_page_cursor_mismatch"),
            (lambda page: page.update(afterSequence=False), "collector_page_cursor_mismatch"),
            (lambda page: page.update(afterSequence="0"), "collector_page_cursor_mismatch"),
            (lambda page: page.pop("afterSequence"), "collector_page_cursor_mismatch"),
            (lambda page: page["stream"].update(prefixGap=True), "collector_page_prefix_gap"),
            (lambda page: page["stream"].update(prefixGap=None), "collector_page_prefix_gap"),
            (lambda page: page.update(stream=None), "collector_page_schema_invalid"),
        ]
        for status in ("disabled", "lock_unavailable", "stream_unavailable", "sequence_gap", "export_truncated", "unmapped"):
            mutations.append((lambda page, status=status: page.update(status=status), "collector_page_status_" + status))
        for status in (None, [], {"key": "NOT-LIVE-secret"}, "NOT-LIVE-secret-stream-key"):
            mutations.append((lambda page, status=status: page.update(status=status), "collector_page_status_invalid"))
        for mutate, reason in mutations:
            with self.subTest(reason=reason, mutate=mutate):
                collector, second, page = self.paged_fixture()
                mutate(page)
                reader = Mock(return_value=page)
                with self.assertRaises(ValueError) as raised:
                    collector.ingest(second, observed(second), reader)
                self.assertEqual(str(raised.exception), reason)
                self.assertNotIn("NOT-LIVE-secret", str(raised.exception))
                reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_failing_page_diagnostic_is_bounded_typed_and_private(self):
        cases = [
            ({"status": "sequence_gap"}, {"prefixGap": True, "oldestSequence": 3, "evicted": 2, "lost": 2}, "collector_page_status_sequence_gap"),
            ({"status": "sequence_gap"}, {"prefixGap": False, "oldestSequence": 1, "dropped": 1, "lost": 1}, "collector_page_status_sequence_gap"),
            ({}, {"prefixGap": True}, "collector_page_prefix_gap"),
            ({"afterSequence": 99}, {}, "collector_page_cursor_mismatch"),
            ({}, {"dropped": 1}, "collector_native_events_dropped"),
            ({}, {"events": [event(2, "admit")], "lost": 1}, "collector_trace_sequence_gap"),
            ({"nextSequence": -1}, {}, "collector_page_cursor_no_progress"),
        ]
        for page_fields, stream_fields, reason in cases:
            with self.subTest(reason=reason, stream_fields=stream_fields):
                collector, second, page = self.paged_fixture()
                page.update(page_fields)
                page["stream"].update(prefixGap=False, oldestSequence=1, evicted=0)
                page["stream"].update(stream_fields)
                page.update(body="NEVER-LOG-body", errors="NEVER-LOG-errors", metrics_key="NEVER-LOG-key", rawSQL="NEVER-LOG-sql")
                page["stream"].update(streamId="NEVER-LOG-stream", backendMetrics={"secret": "NEVER-LOG-backend"})
                reader = Mock(return_value=page)
                with self.assertRaises(ValueError) as raised:
                    collector.ingest(second, observed(second), reader)
                self.assertEqual(str(raised.exception), reason)
                expected = {"role": "worker", "status": page["status"], "requestedAfterSequence": 0,
                            "afterSequence": page["afterSequence"], "sequence": 2, "oldestSequence": page["stream"]["oldestSequence"],
                            "lost": page["stream"]["lost"], "evicted": page["stream"]["evicted"],
                            "dropped": page["stream"]["dropped"], "pendingMessages": 0, "prefixGap": page["stream"]["prefixGap"]}
                if page["nextSequence"] >= 0:
                    expected["nextSequence"] = page["nextSequence"]
                diagnostic = getattr(raised.exception, "page_rejection", None)
                self.assertEqual(diagnostic, expected)
                encoded = json.dumps(diagnostic, sort_keys=True)
                self.assertLess(len(encoded), 512)
                for forbidden in ("NEVER-LOG", "databaseId", "project", "worker:", "queue", "events", "source"):
                    self.assertNotIn(forbidden, encoded)
                reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_malformed_page_metadata_never_enters_diagnostic(self):
        for invalid in (True, False, -1, 1 << 64, 1.5, None, "NEVER-LOG-secret", [], {"secret": "NEVER-LOG-secret"}):
            with self.subTest(invalid=invalid):
                collector, second, page = self.paged_fixture()
                page.update(status=invalid, afterSequence=invalid, nextSequence=invalid)
                page["stream"].update({field: invalid for field in ("sequence", "oldestSequence", "lost", "evicted", "dropped", "pendingMessages")})
                page["stream"]["prefixGap"] = "NEVER-LOG-secret"
                reader = Mock(return_value=page)
                with self.assertRaisesRegex(ValueError, "^collector_page_status_invalid$") as raised:
                    collector.ingest(second, observed(second), reader)
                self.assertEqual(getattr(raised.exception, "page_rejection", None),
                                 {"role": "worker", "status": "invalid", "requestedAfterSequence": 0})
                reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_page_diagnostic_uses_first_failing_page_not_prior_page(self):
        collector, second, first = self.paged_fixture()
        first.update(hasMore=True, nextSequence=1)
        first["stream"]["events"] = [event(1)]
        failure = copy.deepcopy(first)
        failure.update(status="sequence_gap", afterSequence=1, nextSequence=1)
        failure["stream"].update(prefixGap=False, lost=1, dropped=1, oldestSequence=1)
        reader = Mock(side_effect=[first, failure, AssertionError("extra RPC")])
        with self.assertRaisesRegex(ValueError, "^collector_page_status_sequence_gap$") as raised:
            collector.ingest(second, observed(second), reader)
        diagnostic = getattr(raised.exception, "page_rejection", None)
        self.assertIsNotNone(diagnostic)
        self.assertEqual(diagnostic["requestedAfterSequence"], 1)
        self.assertEqual(diagnostic["afterSequence"], 1)
        self.assertEqual(diagnostic["dropped"], 1)
        self.assertIs(diagnostic["prefixGap"], False)
        self.assertEqual(reader.call_args_list, [unittest.mock.call("worker:worker", "queue", 0),
                                                unittest.mock.call("worker:worker", "queue", 1)])

    def test_non_object_page_and_stream_preserve_fixed_reasons(self):
        for invalid in (None, [], "NEVER-LOG-body", 7):
            for malformed_stream in (False, True):
                with self.subTest(invalid=invalid, malformed_stream=malformed_stream):
                    collector, second, page = self.paged_fixture()
                    if malformed_stream:
                        page["stream"] = invalid
                    else:
                        page = invalid
                    reader = Mock(return_value=page)
                    with self.assertRaisesRegex(PageRejection, "^collector_page_schema_invalid$") as raised:
                        collector.ingest(second, observed(second), reader)
                    expected = {"role": "worker", "status": "invalid", "requestedAfterSequence": 0}
                    if malformed_stream:
                        expected.update(status="available", afterSequence=0, nextSequence=2)
                    self.assertEqual(raised.exception.page_rejection, expected)
                    reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_page_rejection_constructor_rejects_untyped_or_unbounded_metadata(self):
        safe = {"role": "authority", "status": "sequence_gap", "sequence": (1 << 64) - 1, "prefixGap": False}
        self.assertEqual(PageRejection("collector_page_status_sequence_gap", safe).page_rejection, safe)
        for fields in ({"role": "NEVER-LOG-role"}, {"status": "NEVER-LOG-status"}, {"sequence": True},
                       {"sequence": 1 << 64}, {"prefixGap": 0}, {"rawSQL": "NEVER-LOG-sql"}):
            with self.subTest(fields=fields):
                with self.assertRaisesRegex(ValueError, "^collector_page_schema_invalid$"):
                    PageRejection("collector_page_status_sequence_gap", {**safe, **fields})
        with self.assertRaisesRegex(ValueError, "^collector_page_schema_invalid$"):
            PageRejection("NEVER-LOG-error", safe)

    def test_later_transport_error_never_gets_previous_page_diagnostic(self):
        collector, second, first = self.paged_fixture()
        first.update(hasMore=True, nextSequence=1)
        first["stream"]["events"] = [event(1)]
        rejection = HTTPRejection("capacity_trace", 503)
        reader = Mock(side_effect=[first, rejection, AssertionError("extra RPC")])
        with self.assertRaises(HTTPRejection) as raised:
            collector.ingest(second, observed(second), reader)
        self.assertIs(raised.exception, rejection)
        self.assertFalse(hasattr(raised.exception, "page_rejection"))
        self.assertEqual(reader.call_count, 2)

    def test_valid_page_statuses_preserve_cursor_source_and_loss_checks(self):
        for status in ("available", "gap"):
            with self.subTest(status=status):
                collector, second, page = self.paged_fixture()
                page.update(status=status)
                page["stream"].update(status=status, prefixGap=False)
                reader = Mock(return_value=page)
                updates = collector.ingest(second, observed(second), reader)
                self.assertEqual([entry["sequence"] for entry in updates[0]["events"]], [1, 2])
                reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_native_pending_is_strict_and_transmitted_from_inline_and_page(self):
        for pending in (0, 1, 1024):
            with self.subTest(pending=pending):
                value = metrics([], pendingMessages=pending)
                updates = TraceCapture(IDENTITY).ingest(value, observed(value))
                self.assertEqual(updates[0].get("pending_messages"), pending)
                collector, second, page = self.paged_fixture()
                page["stream"]["pendingMessages"] = pending
                updates = collector.ingest(second, observed(second), Mock(return_value=page))
                self.assertEqual(updates[0].get("pending_messages"), pending)
        for pending in (None, True, False, -1, 0.0, "0"):
            for origin in ("inline", "page"):
                with self.subTest(pending=pending, origin=origin):
                    with self.assertRaisesRegex(ValueError, "collector_native_pending_invalid"):
                        if origin == "inline":
                            value = metrics([], pendingMessages=pending)
                            if pending is None:
                                value["native"]["workers"]["worker"]["capacity_trace"]["databases"][0].pop("pendingMessages")
                            TraceCapture(IDENTITY).ingest(value, observed(value))
                        else:
                            collector, second, page = self.paged_fixture()
                            page["stream"]["pendingMessages"] = pending
                            if pending is None:
                                page["stream"].pop("pendingMessages")
                            collector.ingest(second, observed(second), Mock(return_value=page))
        mutations = [
            (lambda page: page["source"].update(generation="changed"), "collector_page_source_or_DB_scope_changed"),
            (lambda page: page["stream"].update(dropped=1), "collector_native_events_dropped"),
            (lambda page: page["stream"].update(events=[event(2, "admit")], lost=1), "collector_trace_sequence_gap"),
            (lambda page: page.update(nextSequence=-1), "collector_page_cursor_no_progress"),
        ]
        for mutate, reason in mutations:
            with self.subTest(reason=reason):
                collector, second, page = self.paged_fixture()
                mutate(page)
                reader = Mock(return_value=page)
                with self.assertRaises(ValueError) as raised:
                    collector.ingest(second, observed(second), reader)
                self.assertEqual(str(raised.exception), reason)
                reader.assert_called_once_with("worker:worker", "queue", 0)

    def test_clock_mismatch_and_namespace_branch_ambiguity_are_unmeasured(self):
        first = metrics([])
        with self.assertRaisesRegex(ValueError, "clock_bridge"):
            TraceCapture(IDENTITY).ingest(first, "2026-10-08T00:00:30Z")
        identity = {**IDENTITY, "primary_databases": [DATABASE, {**DATABASE, "branch": "other"}]}
        trace = first["native"]["workers"]["worker"]["capacity_trace"]
        trace["namespaces"] = [{"namespace": DATABASE["namespace"]}]
        with self.assertRaisesRegex(ValueError, "branch_ambiguity"):
            TraceCapture(identity).ingest(first, observed(first))


class NormalizerTests(unittest.TestCase):
    def fixture(self):
        operation = {"business_operation_id": "business", "acked": True, "project": "project", "database": DATABASE,
                     "transaction_terminal": "COMMIT"}
        rpc = {"request_id": "request", "envelope_ok": True, "failure": None, "path": "/v1/sql/execute",
               "started_at_observed": "2026-10-08T00:00:00Z", "finished_at_observed": "2026-10-08T00:00:05Z",
               "capacity_binding": {"project": "project", "database": DATABASE, "business_operation_id": "business"}}
        return operation, rpc

    def test_RPC_join_requires_ACK_DB_project_and_unique_window(self):
        operation, rpc = self.fixture()
        selected = {"requestId": "request", "started_at": "2026-10-08T00:00:01Z", "finished_at": "2026-10-08T00:00:02Z"}
        self.assertIsNotNone(join_rpc(selected, DATABASE, "project", [rpc], {"business": operation}))
        self.assertIsNone(join_rpc(selected, DATABASE, "other", [rpc], {"business": operation}))
        self.assertIsNone(join_rpc(selected, DATABASE, "project", [rpc, rpc], {"business": operation}))
        operation["acked"] = False
        self.assertIsNone(join_rpc(selected, DATABASE, "project", [rpc], {"business": operation}))

    def test_IO_uses_real_counter_snapshots_not_payload_invented_counter(self):
        operation, rpc = self.fixture()
        stream = {"project": "project", "database": DATABASE, "queue": "physical", "source": {"pid": 42}}
        events = [{"kind": "backend_start", "operation": "put", "ioId": 1, "sequence": 1,
                   "observed_at": "2026-10-08T00:00:01Z", "bytesBefore": 100},
                  {"kind": "backend_finish", "operation": "put", "ioId": 1, "sequence": 2, "requestId": None,
                   "success": True, "bytes": 10, "bytesAfter": 120, "observed_at": "2026-10-08T00:00:02Z"}]
        result = io_events(stream, events, [rpc], {"business": operation}, "native-identity")
        self.assertEqual((result["before_bytes"], result["after_bytes"]), (100, 120))
        self.assertEqual(result["activities"][0]["payload_bytes"], 10)
        self.assertEqual(result["activities"][0]["join_method"], "unique_DB_RPC_window")
        del events[0]["bytesBefore"]
        self.assertIsNone(io_events(stream, events, [rpc], {"business": operation}, "native-identity"))

    def test_native_cleanup_needs_client_ACK_reserved_lane_and_real_release(self):
        operation, rpc = self.fixture()
        stream = {"project": "project", "database": DATABASE}
        events = [{"kind": "cleanup_start", "sequence": 1, "requestId": "request", "controlId": "queue:1", "observed_at": "2026-10-08T00:00:01Z"},
                  {"kind": "release", "sequence": 2, "controlId": "queue:1", "observed_at": "2026-10-08T00:00:02Z"},
                  {"kind": "cleanup_finish", "sequence": 3, "requestId": "request", "controlId": "queue:1", "reservedLane": "owner_sql",
                   "success": True, "operation": "COMMIT", "observed_at": "2026-10-08T00:00:03Z"}]
        self.assertEqual(len(cleanup_events(stream, events, [rpc], {"business": operation})), 1)
        operation["acked"] = False
        self.assertFalse(cleanup_events(stream, events, [rpc], {"business": operation}))
        operation["acked"] = True
        events[-1]["reservedLane"] = None
        self.assertFalse(cleanup_events(stream, events, [rpc], {"business": operation}))


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.launcher = {"container_id": "owned-id", "container_name": "owned-container", "source_hash": "frozen-source",
                         "image": {"Id": "sha256:owned", "Os": "linux"}, "required_native_roles": ["authority", "worker-a", "worker-b", "platform", "launcher"]}
        self.sample = {"schema_version": 1, "source": "container.proc", "observed_at": "2026-10-08T00:00:00Z",
                       "container_id": "owned-id", "container_name": "owned-container", "image_id": "sha256:owned", "source_hash": "frozen-source",
                       "running": True, "oom_killed": False, "exit_code": None, "boot_id": "boot", "page_size_bytes": 4096,
                       "clock_ticks_per_second": 100, "processes": [{"role": role, "pid": 40 + index, "start_ticks": 1000,
                           "rss_pages": 100, "cpu_ticks": 100, "state": "S", "uids": [10001] * 4} for index, role in enumerate(self.launcher["required_native_roles"])]}

    def adapter(self):
        return ContainerResources(self.launcher, 512 << 20)

    def test_actual_interval_CPU_and_phase_source_binding(self):
        adapter = self.adapter()
        first = adapter.ingest(self.sample)
        second = copy.deepcopy(self.sample)
        second["observed_at"] = "2026-10-08T00:00:02Z"
        second["processes"][1]["cpu_ticks"] = 150
        last = adapter.ingest(second)
        self.assertIsNone(first["processes"][1]["cpu_percent_proc_interval"])
        self.assertEqual(last["processes"][1]["cpu_percent_proc_interval"], 25)
        self.assertNotIn("cpu_seconds_ps", last["processes"][1])
        phase = {"started_at_observed": self.sample["observed_at"], "finished_at_observed": second["observed_at"]}
        samples = phase_resources([first, last], phase, self.launcher)
        self.assertEqual(bind_native_source({"pid": 41}, "worker:worker-a", samples), "boot:1000")
        with self.assertRaisesRegex(ValueError, "not_actual"):
            bind_native_source({"pid": 99}, "worker:worker-a", samples)
        with self.assertRaisesRegex(ValueError, "two_actual"):
            phase_resources([first], phase, self.launcher)

    def test_same_numeric_host_and_container_PID_never_mix_CPU(self):
        adapter = self.adapter()
        first = adapter.ingest(self.sample)
        sample = copy.deepcopy(self.sample)
        sample["observed_at"] = "2026-10-08T00:00:02Z"
        last = adapter.ingest(sample)
        host = []
        for index, observed in enumerate([first["observed_at"], last["observed_at"]]):
            host.append({"source": "host.ps", "observed_at": observed, "processes_status": "available", "processes": [{
                "role": "launcher", "pid": 44, "status": "available", "rss_bytes": 100, "cpu_seconds_ps": index * 10,
                "cpu_percent_ps": 5, "process_start_identity": "actual-host-start"}]})
        result = resource_window([first, *host, last], {"started_at_observed": first["observed_at"], "finished_at_observed": last["observed_at"]})
        self.assertEqual(len(result["processes"]), 6)
        self.assertEqual(result["processes"]["host.ps:host:launcher:44"]["cpu_seconds_delta"], 10)
        self.assertEqual(result["processes"]["container.proc.status:owned-id:launcher:44"]["cpu_seconds_delta"], 0)
        profile = {"observed_at": first["observed_at"], "management_metrics_status": "available", "management_metrics": {
            "projects": {"owned": {"writer_admission": True}}, "native": {"workers": {"worker-a": {"writer_profile": True}}}}}
        phase = {"started_at_observed": first["observed_at"], "finished_at_observed": last["observed_at"]}
        self.assertEqual(writer_profiles([profile, first, last], phase, ["owned"])["status"], "PASS")
        self.assertEqual(writer_profiles([first, last], phase, ["owned"])["status"], "NOT MEASURED")

    def test_OOM_exit_RSS_identity_roles_fail_closed(self):
        mutations = [(lambda row: row.update(oom_killed=True), "OOM"), (lambda row: row.update(running=False), "exit"),
                     (lambda row: row.update(image_id="other"), "image_binding"), (lambda row: row.update(source_hash="other"), "binding"),
                     (lambda row: row["processes"].pop(), "missing"),
                     (lambda row: row["processes"][1].update(pid=40), "share_PID"),
                     (lambda row: row["processes"][4].update(uids=[10001, 0, 10001, 10001]), "four_UIDs"),
                     (lambda row: row["processes"][0].update(rss_pages=131073), "RSS")]
        for mutate, reason in mutations:
            with self.subTest(reason=reason):
                sample = copy.deepcopy(self.sample)
                mutate(sample)
                with self.assertRaisesRegex(ValueError, reason):
                    self.adapter().ingest(sample)

    def test_restart_counter_regression_and_units_change(self):
        for mutate, reason in [(lambda row: row["processes"][0].update(start_ticks=2000), "restarted"),
                               (lambda row: row["processes"][0].update(cpu_ticks=50), "regressed"),
                               (lambda row: row.update(clock_ticks_per_second=200), "units_changed"),
                               (lambda row: row.update(boot_id="other"), "boot")]:
            adapter = self.adapter()
            adapter.ingest(self.sample)
            sample = copy.deepcopy(self.sample)
            sample["observed_at"] = "2026-10-08T00:00:02Z"
            mutate(sample)
            with self.assertRaisesRegex(ValueError, reason):
                adapter.ingest(sample)


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        self.output = self.root / "job"
        self.output.mkdir(mode=0o700)
        self.connection = self.root / "connection.json"
        write_private(self.connection, {"worker_ids": ["worker"], "metrics_key": "NEVER-LOG-METRICS-KEY", "fixture_only": True})
        self.plan_path = self.root / "plan.json"
        write_private(self.plan_path, {"unit_fixture": "NOT LIVE EVIDENCE"})
        launch = self.root / "launch.json"
        write_private(launch, {"container_name": "unit-owned"})
        self.plan = {"plan_path": str(self.plan_path), "identities": {"single_hot": IDENTITY},
                     "config": {"connections": {"single_hot": str(self.connection)}, "launcher_report": str(launch),
                                "guard": {"hard_max_seconds": 1800, "max_output_bytes": 64 << 20}}}
        self.job = {"id": "unit-job", "output": str(self.output), "topology": "single_hot"}
        self.stop = self.output / "collector-stop.json"

    def test_capture_finalize_private_original_window_and_tamper_rejection(self):
        first = metrics([])
        last = metrics([event(1, micros=1020000), event(2, "admit", micros=1040000)], sequence=2)
        last["native"]["workers"]["worker"]["capacity_trace"]["source"].update(wallUnixMicros=BASE + 1100000, monotonicMicros=1100000)
        reader = Mock()
        reader.read.side_effect = [first, last]
        times = [observed(first), observed(first), observed(last), observed(last)]
        with patch("capacity_collect.now", side_effect=times):
            result = capture(self.plan, self.job, self.stop, reader=reader, sleep=lambda duration: write_private(self.stop, {}))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertNotIn("page_rejection", result)
        self.assertEqual(result["polls"], 2)
        self.assertNotIn("NEVER-LOG", (self.output / "capacity-trace.jsonl").read_text())
        self.assertEqual((self.output / "capacity-trace.jsonl").stat().st_mode & 0o777, 0o600)
        run = self.output / "run_unit"
        run.mkdir(mode=0o700)
        source = last["native"]["workers"]["worker"]["capacity_trace"]["source"]
        write_private(run / "run.json", {"run_id": "UNIT ONLY", "scenarios": [{"phases": [{"phase": "isolated_primary_measure", "concurrency": 1,
                      "started_at_observed": wall_time(source, 1010000), "finished_at_observed": wall_time(source, 1050000)}]}]})
        with os.fdopen(os.open(run / "operations.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
            output.write("")
        observation = normalize(self.plan, self.job)
        self.assertEqual(observation["run_id"], "UNIT ONLY")
        self.assertEqual(len(observation["phases"][0]["tickets"]), 1)
        with self.assertRaisesRegex(ValueError, "no_rerun"):
            capture(self.plan, self.job, self.stop, reader=reader)
        with (self.output / "capacity-trace.jsonl").open("a") as output:
            output.write("{}\n")
        with self.assertRaisesRegex(ValueError, "original_journal"):
            normalize(self.plan, self.job)

    def test_Linux_missing_proc_monitor_never_ready(self):
        self.connection.unlink()
        write_private(self.connection, {"worker_ids": ["worker-a", "worker-b"], "fixture_only": True})
        launch = Path(self.plan["config"]["launcher_report"])
        launch.unlink()
        write_private(launch, {"container_id": "owned-id", "container_name": "unit-owned", "source_hash": "source",
            "image": {"Id": "sha256:owned", "Os": "linux"}, "required_native_roles": ["authority", "worker-a", "worker-b", "platform", "launcher"]})
        self.plan["config"]["guard"]["max_process_rss_bytes"] = 512 << 20
        reader = Mock()
        value = metrics([])
        reader.read.return_value = linux_metrics(value)
        with patch("capacity_collect.now", return_value=observed(value)):
            result = capture(self.plan, self.job, self.stop, reader=reader)
        self.assertEqual(result["status"], "NOT MEASURED")
        self.assertIn("monitor_required", result["reason"])
        self.assertFalse((self.output / "collector-ready.json").exists())

    def test_Linux_callback_capture_normalize_binds_actual_proc_PID(self):
        fixture = ResourceTests()
        fixture.setUp()
        for row, pid in zip(fixture.sample["processes"], [40, 42, 43, 44, 45]):
            row["pid"] = pid
        launch = Path(self.plan["config"]["launcher_report"])
        launch.unlink()
        write_private(launch, fixture.launcher)
        self.connection.unlink()
        write_private(self.connection, {"worker_ids": ["worker-a", "worker-b"], "fixture_only": True})
        self.plan["config"]["guard"]["max_process_rss_bytes"] = 512 << 20
        values, samples, times = [], [], []
        for index, micros in enumerate([1000000, 1020000, 1040000, 1100000]):
            events = [event(1, micros=1020000), event(2, "admit", micros=1040000)][:min(index, 2)]
            value = metrics(events)
            worker = value["native"]["workers"].pop("worker")
            value["native"]["workers"]["worker-a"] = worker
            worker["capacity_trace"]["source"].update(wallUnixMicros=BASE + micros, monotonicMicros=micros)
            sample = copy.deepcopy(fixture.sample)
            sample["observed_at"] = wall_time(worker["capacity_trace"]["source"], micros)
            times.extend([sample["observed_at"]] * 3)
            values.append(linux_metrics(value))
            samples.append(sample)
        reader = Mock()
        reader.read.side_effect = values
        resources = Mock(side_effect=samples)
        def advance(duration):
            if reader.read.call_count == 3:
                write_private(self.stop, {})
        with patch("capacity_collect.now", side_effect=times):
            result = capture(self.plan, self.job, self.stop, reader=reader, resource_reader=resources, sleep=advance)
        self.assertEqual(result["status"], "COMPLETE")
        run = self.output / "run_unit"
        run.mkdir(mode=0o700)
        write_private(run / "run.json", {"run_id": "UNIT ONLY", "scenarios": [{"phases": [{"phase": "isolated_primary_measure", "concurrency": 1,
                      "started_at_observed": "2026-10-08T00:00:01.010000Z", "finished_at_observed": "2026-10-08T00:00:01.050000Z"}]}]})
        descriptor = os.open(run / "operations.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        observation = normalize(self.plan, self.job)
        self.assertEqual(len(observation["resource_samples"]), 4)
        identity = next(row for row in observation["phases"][0]["native_process_identities"] if row["role"] == "worker:worker-a")
        self.assertEqual(identity["proc_start_identity"], "boot:1000")
        self.assertEqual(identity["pid"], 42)

    def test_callback_resource_UID_failure_prevents_READY(self):
        self.connection.unlink()
        write_private(self.connection, {"worker_ids": ["worker-a", "worker-b"], "fixture_only": True})
        fixture = ResourceTests()
        fixture.setUp()
        launch = Path(self.plan["config"]["launcher_report"])
        launch.unlink()
        write_private(launch, fixture.launcher)
        self.plan["config"]["guard"]["max_process_rss_bytes"] = 512 << 20
        fixture.sample["processes"][4]["uids"][1] = 0
        reader = Mock()
        value = metrics([])
        reader.read.return_value = linux_metrics(value)
        with patch("capacity_collect.now", return_value=observed(value)):
            result = capture(self.plan, self.job, self.stop, reader=reader, resource_reader=lambda: fixture.sample)
        self.assertEqual(result["status"], "NOT MEASURED")
        self.assertIn("four_UIDs", result["reason"])
        self.assertFalse((self.output / "collector-ready.json").exists())

    def test_missing_trace_never_ready_and_does_not_start_load(self):
        reader = Mock()
        reader.read.return_value = {"native": {}}
        result = capture(self.plan, self.job, self.stop, reader=reader, sleep=lambda duration: None)
        self.assertEqual(result["status"], "NOT MEASURED")
        self.assertNotIn("http_rejection", result)
        self.assertFalse((self.output / "collector-ready.json").exists())
        reader.read.assert_called_once_with()

    def test_HTTP_rejection_status_is_private_safe_once_only_and_never_green(self):
        certificate = self.root / "ca.pem"
        certificate.write_text("mock cert; no network")
        config = {"fixture_only": True, "management_endpoint": "https://localhost:9443",
                  "metrics_key": "NEVER-LOG-METRICS-KEY", "ca_file": str(certificate)}
        for phase in ("metrics_initial", "metrics", "capacity_trace"):
            with self.subTest(phase=phase):
                output = self.root / phase
                output.mkdir(mode=0o700)
                job = {**self.job, "output": str(output)}
                first = metrics([])
                responses = []
                values = [] if phase == "metrics_initial" else [first]
                if phase == "capacity_trace":
                    second = metrics([], sequence=2, streamId="queue")
                    second["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]["events"] = None
                    values.append(second)
                for value in values:
                    response = Mock(status=200)
                    response.getheader.return_value = None
                    response.read1.side_effect = io.BytesIO(json.dumps(
                        {"version": 1, "ok": True, "error": None, "result": value}).encode()).read1
                    responses.append(response)
                rejection = Mock(status=503)
                responses.append(rejection)
                connection = Mock()
                connection.getresponse.side_effect = responses
                factory = Mock(return_value=connection)
                with patch("capacity_transport.ssl.create_default_context", return_value=ssl.create_default_context()):
                    reader = MetricsReader(config, factory)
                with patch("capacity_collect.now", return_value=observed(first)):
                    result = capture(self.plan, job, output / "stop.json", reader=reader, sleep=lambda duration: None)
                self.assertEqual(result["reason"], "collector_metrics_HTTP_failure")
                self.assertEqual(result["status"], "NOT MEASURED")
                self.assertIs(result["performance_pass"], False)
                self.assertEqual(result["polls"], 0 if phase == "metrics_initial" else 1)
                self.assertEqual((output / "collector-ready.json").exists(), phase != "metrics_initial")
                route = "capacity_trace" if phase == "capacity_trace" else "metrics"
                self.assertEqual(result.get("http_rejection"), {"route": route, "status": 503})
                self.assertNotIn("page_rejection", result)
                saved = output / "collector-status.json"
                self.assertEqual(json.loads(saved.read_text()), result)
                self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
                for forbidden in (config["metrics_key"], "worker_id", "stream_id", "Authorization", "/v1/"):
                    self.assertNotIn(forbidden, saved.read_text())
                self.assertEqual(connection.request.call_count, len(responses))
                self.assertEqual(connection.close.call_count, len(responses))
                for call in factory.call_args_list:
                    self.assertEqual(call.kwargs["timeout"], 2)
                rejection.getheader.assert_not_called()
                rejection.read1.assert_not_called()

    def test_page_rejection_is_persisted_private_without_retry_or_raw_response(self):
        for status, prefix_gap, reason in (("sequence_gap", True, "collector_page_status_sequence_gap"),
                                           ("sequence_gap", False, "collector_page_status_sequence_gap"),
                                           ("NEVER-LOG-status", "NEVER-LOG-prefix", "collector_page_status_invalid")):
            with self.subTest(status=status, prefix_gap=prefix_gap):
                output = self.root / ("prefix" if prefix_gap is True else "tail" if prefix_gap is False else "malformed")
                output.mkdir(mode=0o700)
                collector, second, page = TraceTests().paged_fixture()
                first = metrics([], streamId="queue")
                page.update(status=status, rawSQL="NEVER-LOG-sql", errors="NEVER-LOG-errors",
                            metrics_key="NEVER-LOG-METRICS-KEY", body="NEVER-LOG-body")
                page["stream"].update(prefixGap=prefix_gap, oldestSequence=1, lost=1, evicted=0, dropped=1,
                                      databaseId={"secret": "NEVER-LOG-database"}, streamId="NEVER-LOG-stream")
                reader = Mock()
                reader.read.side_effect = [first, second, AssertionError("extra metrics RPC")]
                reader.page.side_effect = [page, AssertionError("extra page RPC")]
                with patch("capacity_collect.now", return_value=observed(first)):
                    result = capture(self.plan, {**self.job, "output": str(output)}, output / "stop.json",
                                     reader=reader, sleep=lambda duration: None)
                self.assertEqual(result["status"], "NOT MEASURED")
                self.assertEqual(result["reason"], reason)
                self.assertEqual(result["polls"], 1)
                self.assertIs(result["performance_pass"], False)
                self.assertNotIn("http_rejection", result)
                expected = {"role": "worker", "status": "sequence_gap" if status == "sequence_gap" else "invalid",
                            "requestedAfterSequence": 0, "afterSequence": 0, "nextSequence": 2, "sequence": 2,
                            "oldestSequence": 1, "lost": 1, "evicted": 0, "dropped": 1, "pendingMessages": 0}
                if type(prefix_gap) is bool:
                    expected["prefixGap"] = prefix_gap
                self.assertEqual(result.get("page_rejection"), expected)
                saved = output / "collector-status.json"
                self.assertEqual(json.loads(saved.read_text()), result)
                self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
                self.assertTrue((output / "collector-ready.json").exists())
                for path in output.iterdir():
                    self.assertNotIn("NEVER-LOG", path.read_text())
                reader.read.assert_has_calls([unittest.mock.call(), unittest.mock.call()])
                self.assertEqual(reader.read.call_count, 2)
                reader.page.assert_called_once_with("worker:worker", "queue", 0)

    def test_capture_output_budget_is_not_a_new_load_budget(self):
        self.plan["config"]["guard"]["max_output_bytes"] = 1
        reader = Mock()
        reader.read.return_value = metrics([])
        with patch("capacity_collect.now", return_value=observed(reader.read.return_value)):
            result = capture(self.plan, self.job, self.stop, reader=reader)
        self.assertEqual(result["status"], "NOT MEASURED")
        self.assertIn("budget", result["reason"])

    def test_finishing_poll_requires_applied_prefix_without_pending_or_retry(self):
        for origin, final_pending in ((origin, pending) for origin in ("inline", "page") for pending in (0, 1)):
            with self.subTest(final_pending=final_pending, origin=origin):
                output = self.root / f"pending-{origin}-{final_pending}"
                output.mkdir(mode=0o700)
                stop = output / "stop.json"
                job = {**self.job, "output": str(output)}
                first = metrics([], pendingMessages=1)
                final = metrics([], pendingMessages=final_pending)
                reader = Mock()
                if origin == "page":
                    final = metrics([], sequence=2, streamId="queue")
                    ring = final["native"]["workers"]["worker"]["capacity_trace"]["databases"][0]
                    ring["events"] = None
                    reader.page.return_value = {"schemaVersion": 1, "enabled": True, "status": "available", "afterSequence": 0,
                        "nextSequence": 2, "hasMore": False, "source": final["native"]["workers"]["worker"]["capacity_trace"]["source"],
                        "stream": {**ring, "pendingMessages": final_pending, "events": [event(1), event(2, "admit")]}}
                reader.read.side_effect = [first, final]
                with patch("capacity_collect.now", return_value=observed(first)):
                    result = capture(self.plan, job, stop, reader=reader, sleep=lambda duration: write_private(stop, {}))
                reader.read.assert_has_calls([unittest.mock.call(), unittest.mock.call()])
                self.assertEqual(reader.read.call_count, 2)
                if origin == "page":
                    reader.page.assert_called_once_with("worker:worker", "queue", 0)
                else:
                    reader.page.assert_not_called()
                self.assertTrue((output / "collector-ready.json").exists())
                self.assertIs(result["performance_pass"], False)
                if final_pending:
                    self.assertEqual(result["status"], "NOT MEASURED")
                    self.assertEqual(result["reason"], "collector_final_native_pending")
                    self.assertEqual(result["polls"], 1)
                else:
                    self.assertEqual(result["status"], "COMPLETE")
                    self.assertEqual(result["polls"], 2)
                    rows = [json.loads(line) for line in (output / "capacity-trace.jsonl").read_text().splitlines()]
                    self.assertEqual([row.get("pending_messages") for row in rows if row["type"] == "trace_snapshot"], [1, 0])


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.certificate = Path(self.temporary.name).resolve() / "ca.pem"
        self.certificate.write_text("mock cert; no network")
        self.config = {"fixture_only": True, "management_endpoint": "https://localhost:9443", "metrics_key": "private-metrics-key", "ca_file": str(self.certificate)}

    def reader(self, response):
        connection = Mock()
        connection.getresponse.return_value = response
        factory = Mock(return_value=connection)
        context = ssl.create_default_context()
        with patch("capacity_transport.ssl.create_default_context", return_value=context) as create:
            reader = MetricsReader(self.config, factory)
        create.assert_called_once_with(cafile=str(self.certificate))
        return reader, connection

    def test_only_verified_TLS_read_only_GET_no_redirect_or_secret_output(self):
        body = io.BytesIO(json.dumps({"version": 1, "ok": True, "error": None, "result": {"native": {}}}).encode())
        response = Mock(status=200)
        response.getheader.return_value = None
        response.read1.side_effect = body.read1
        reader, connection = self.reader(response)
        self.assertEqual(reader.read(), {"native": {}})
        self.assertEqual(connection.request.call_args.args, ("GET", "/v1/metrics"))
        self.assertTrue(reader.context.check_hostname)
        self.assertEqual(reader.context.verify_mode, ssl.CERT_REQUIRED)
        connection.close.assert_called_once()
        with self.assertRaisesRegex(ValueError, "read_only"):
            reader.read("/v1/sql/open")

    def test_bad_endpoints_oversize_and_redirects_fail_without_network(self):
        for endpoint in ("http://localhost", "https://token@localhost", "https://localhost/?secret=x"):
            with self.assertRaisesRegex(ValueError, "proper_CA"):
                MetricsReader({**self.config, "management_endpoint": endpoint})
        response = Mock(status=200)
        response.getheader.return_value = str(MAX_RESPONSE + 1)
        reader, connection = self.reader(response)
        with self.assertRaisesRegex(ValueError, "budget"):
            reader.read()
        response.status = 302
        with self.assertRaisesRegex(ValueError, "HTTP_failure"):
            reader.read()

    def test_HTTP_rejection_metadata_only_fixed_route_and_integer_status(self):
        for route in ("metrics", "capacity_trace"):
            for status in (100, 302, 400, 403, 503, 599):
                with self.subTest(route=route, status=status):
                    response = Mock(status=status)
                    reader, connection = self.reader(response)
                    with self.assertRaises(ValueError) as raised:
                        if route == "metrics":
                            reader.read()
                        else:
                            reader.page("worker:NOT-LIVE-PRIVATE-WORKER", "NOT-LIVE-PRIVATE-STREAM", 17)
                    self.assertEqual(str(raised.exception), "collector_metrics_HTTP_failure")
                    self.assertEqual(getattr(raised.exception, "http_rejection", None), {"route": route, "status": status})
                    self.assertNotIn("PRIVATE", repr(raised.exception))
                    connection.request.assert_called_once()
                    connection.close.assert_called_once()
                    response.getheader.assert_not_called()
                    response.read1.assert_not_called()

    def test_invalid_HTTP_status_never_becomes_diagnostic_input(self):
        for status in (None, True, False, "503 PRIVATE", 503.0, 99, 600, -1):
            with self.subTest(status=status):
                response = Mock(status=status)
                reader, connection = self.reader(response)
                with self.assertRaises(ValueError) as raised:
                    reader.read()
                self.assertEqual(str(raised.exception), "collector_metrics_HTTP_failure")
                self.assertIsNone(getattr(raised.exception, "http_rejection", None))
                connection.request.assert_called_once()
                connection.close.assert_called_once()
                response.getheader.assert_not_called()
                response.read1.assert_not_called()

    def test_successful_page_preserves_request_and_has_no_rejection(self):
        response = Mock(status=200)
        response.getheader.return_value = None
        response.read1.side_effect = io.BytesIO(json.dumps(
            {"version": 1, "ok": True, "error": None, "result": {"unit": "NOT LIVE"}}).encode()).read1
        reader, connection = self.reader(response)
        self.assertEqual(reader.page("worker:unit-worker", "unit-stream", 17), {"unit": "NOT LIVE"})
        self.assertEqual(connection.request.call_args.args, ("GET", "/v1/capacity-trace?role=worker&stream_id=unit-stream&after_sequence=17&max_events=128&worker_id=unit-worker"))
        connection.request.assert_called_once()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
