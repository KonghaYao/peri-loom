import datetime
import json
import os
import time
from pathlib import Path

import run_guarded as guarded
from capacity_plan import digest, write_private
from capacity_trace import PageRejection, TraceCapture, wall_time
from capacity_transport import HTTPRejection, MetricsReader
from capacity_evidence import records
from capacity_metrics import timestamp
from capacity_fifo import database_key
from capacity_resources import ContainerResources, phase_resources, bind_native_source, TRACE_ROLES


MAX_JOURNAL = 32 << 20
COLLECTOR_PROFILE = {"poll_seconds": 0.25, "response_timeout_seconds": 2, "page_bytes": 65536,
                     "page_events": 128, "pages_per_stream_per_poll": 16, "poll_budget_seconds": 8,
                     "journal_bytes": MAX_JOURNAL, "shared_original_output_budget": True,
                     "HTTP_methods": ["GET"], "native_session_bearer": False}
COLLECTOR_PROFILE["resource_contract"] = "Main explicit Linux proc observer; no Docker or host PID substitution"
COLLECTOR_PROFILE["resource_schema"] = {"version": 1, "roles": ["authority", "worker-a", "worker-b", "platform", "launcher"],
                                        "uid_fields": ["ruid", "euid", "suid", "fsuid"], "uid": 10001, "phase_min_samples": 2,
                                        "trace_role_map": {"authority": "authority", "worker:worker-a": "worker-a", "worker:worker-b": "worker-b"}}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def select_job(plan, identifier):
    matches = [job for job in plan["jobs"] if job["id"] == identifier]
    guarded.require(len(matches) == 1 and not matches[0]["structural_unmeasured"], "collector_selected_frozen_job_required")
    return matches[0]


def capture(plan, job, stop_file, stopped=lambda: False, reader=None, sleep=time.sleep, resource_reader=None):
    root = Path(job["output"])
    guarded.private(root, 0o700, True)
    journal = root / "capacity-trace.jsonl"
    guarded.require(not journal.exists() and not (root / "collector-status.json").exists(), "collector_original_capture_no_rerun")
    connection = guarded.read_spec(Path(plan["config"]["connections"][job["topology"]]))
    reader = reader or MetricsReader(connection)
    workers = connection.get("worker_ids", [])
    guarded.require(isinstance(workers, list) and workers and len(workers) <= 16 and all(isinstance(worker, str) and worker for worker in workers), "collector_frozen_worker_ids_required")
    tracer = TraceCapture(plan["identities"][job["topology"]], workers)
    secrets = [connection.get(key) for key in ("api_key", "management_key", "metrics_key")]
    secrets.append(connection.get("noisy_tenant", {}).get("api_key"))
    size, polls, started = 0, 0, time.monotonic()
    status, reason = "COMPLETE", None
    http_rejection = None
    page_rejection = None
    launcher = guarded.read_spec(Path(plan["config"]["launcher_report"]))
    proc_required = launcher.get("image", {}).get("Os") == "linux"
    adapter = ContainerResources(launcher, plan["config"]["guard"]["max_process_rss_bytes"]) if resource_reader is not None or proc_required else None
    resource_offset = 0
    resource_prefix = []
    with os.fdopen(os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        try:
            while True:
                finishing = stopped() or stop_file.exists()
                if stop_file.exists():
                    guarded.private(stop_file, 0o600)
                guarded.require(time.monotonic() - started < plan["config"]["guard"]["hard_max_seconds"], "collector_duration_budget")
                poll_started = time.monotonic()
                def page(role, stream, after):
                    guarded.require(time.monotonic() - poll_started < 8, "collector_read_only_poll_budget")
                    return reader.page(role, stream, after)
                updates = tracer.ingest(reader.read(), now(), page)
                guarded.require(not finishing or all(update["pending_messages"] == 0 for update in updates),
                                "collector_final_native_pending")
                guarded.require(not proc_required or set(tracer.sources) == set(TRACE_ROLES), "Linux_all_three_actual_Native_producers_required")
                if resource_reader is not None:
                    sample = resource_reader()
                    adapter.ingest(sample)
                    guarded.require(abs(timestamp(now()) - timestamp(sample["observed_at"])) <= 2, "container_proc_monitor_stale")
                    updates.append({"type": "container_proc", "sample": sample})
                elif proc_required:
                    resource_path = root / "container-resources.jsonl"
                    guarded.require(resource_path.exists(), "actual_container_proc_monitor_required_before_READY")
                    resource_rows = list(records(resource_path))
                    guarded.require(len(resource_rows) >= resource_offset and resource_rows, "container_proc_monitor_empty_or_truncated")
                    guarded.require(resource_rows[:resource_offset] == resource_prefix, "container_proc_monitor_original_prefix_rewritten")
                    for sample in resource_rows[resource_offset:]:
                        adapter.ingest(sample)
                    resource_offset = len(resource_rows)
                    resource_prefix = resource_rows
                    guarded.require(abs(timestamp(now()) - timestamp(resource_rows[-1]["observed_at"])) <= 2, "container_proc_monitor_stale")
                header = {"type": "collector_poll", "job": job["id"], "plan_sha256": digest(plan["plan_path"]),
                          "observed_at": now(), "finishing": finishing, "sources": {role: list(source) for role, source in tracer.sources.items()}}
                for record in [header, *updates]:
                    encoded = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                    guarded.require(not any(secret and secret in encoded for secret in secrets), "collector_forbidden_credential_in_trace")
                    size += len(encoded.encode())
                    guarded.require(size <= MAX_JOURNAL and size + sum(path.stat().st_size for path in root.rglob("*") if path.is_file() and path != journal) <= plan["config"]["guard"]["max_output_bytes"],
                                    "collector_total_original_output_budget")
                    output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
                polls += 1
                if polls == 1:
                    write_private(root / "collector-ready.json", {"status": "READY READ ONLY NOT MEASURED", "job": job["id"], "plan_sha256": header["plan_sha256"]})
                if finishing:
                    break
                sleep(0.25)
        except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as error:
            status, reason = "NOT MEASURED", str(error) if isinstance(error, ValueError) else "collector_transport_or_trace_unavailable"
            if isinstance(error, HTTPRejection):
                http_rejection = dict(error.http_rejection)
            elif isinstance(error, PageRejection):
                reason = error.reason
                page_rejection = dict(error.page_rejection)
    result = {"status": status, "reason": reason, "job": job["id"], "polls": polls, "bytes": size,
              "plan_sha256": digest(plan["plan_path"]), "journal_sha256": digest(journal), "poll_seconds": 0.25,
              "RPC_scope": "read-only metrics/trace GET, not SQL/business load", "performance_pass": False}
    result["profile"] = dict(COLLECTOR_PROFILE)
    if http_rejection is not None:
        result["http_rejection"] = http_rejection
    if page_rejection is not None:
        result["page_rejection"] = page_rejection
    write_private(root / "collector-status.json", result)
    return result


def join_rpc(event, database, project, rpcs, operations):
    matches = []
    known = {rpc.get("request_id") for rpc in rpcs}
    for rpc in rpcs:
        binding = rpc.get("capacity_binding", {})
        if not rpc.get("envelope_ok") or rpc.get("failure") is not None:
            continue
        if event.get("requestId") in known and event["requestId"] != rpc.get("request_id"):
            continue
        if binding.get("database") != database or binding.get("project") != project:
            continue
        if not timestamp(rpc["started_at_observed"]) <= timestamp(event["started_at"]) < timestamp(event["finished_at"]) <= timestamp(rpc["finished_at_observed"]):
            continue
        operation = operations.get(binding.get("business_operation_id"))
        if operation and operation.get("acked") and operation.get("database") == database and operation.get("project") == project:
            matches.append((rpc, operation))
    return matches[0] if len(matches) == 1 else None


def normalize(plan, job):
    root = Path(job["output"])
    status = guarded.read_spec(root / "collector-status.json")
    guarded.require(status.get("status") == "COMPLETE" and status.get("polls", 0) >= 2 and status.get("job") == job["id"]
                    and status.get("plan_sha256") == digest(plan["plan_path"])
                    and status.get("journal_sha256") == digest(root / "capacity-trace.jsonl"), "collector_complete_original_journal_required")
    runs = list(root.glob("run_*/run.json"))
    guarded.require(len(runs) == 1, "collector_single_original_run_required")
    report = guarded.read_spec(runs[0])
    raw = list(records(runs[0].with_name("operations.jsonl")))
    operations = {record["business_operation_id"]: record for record in raw if record.get("type") == "operation"}
    rpcs = [record for record in raw if record.get("type") == "gateway_rpc"]
    journal = list(records(root / "capacity-trace.jsonl"))
    polls = [record for record in journal if record["type"] == "collector_poll"]
    phases = [phase for scenario in report.get("scenarios", []) for phase in scenario.get("phases", []) if phase.get("phase", "").endswith("measure") and phase.get("started_at_observed")]
    guarded.require(polls and polls[-1].get("finishing") is True and phases
                    and timestamp(polls[0]["observed_at"]) < min(timestamp(phase["started_at_observed"]) for phase in phases)
                    and timestamp(polls[-1]["observed_at"]) >= max(timestamp(phase["finished_at_observed"]) for phase in phases), "collector_run_window_not_fully_observed")
    streams = {}
    for record in journal:
        if record["type"] != "trace_snapshot":
            continue
        scope = (record["role"], record["queue"])
        stream = streams.setdefault(scope, {**record, "events": []})
        for event in record["events"]:
            stream["events"].append({**event, "observed_at": wall_time(record["source"], event["monotonicMicros"])})
    launcher = guarded.read_spec(Path(plan["config"]["launcher_report"]))
    result = {"run_id": report["run_id"], "container_name": launcher.get("container_name"),
              "collector": status, "resource_samples": [], "phases": []}
    resource_rows = [record["sample"] for record in journal if record["type"] == "container_proc"]
    resource_path = root / "container-resources.jsonl"
    if resource_path.exists():
        guarded.require(not resource_rows, "one_actual_container_proc_source_required")
        resource_rows = list(records(resource_path))
    if resource_rows:
        adapter = ContainerResources(launcher, plan["config"]["guard"]["max_process_rss_bytes"])
        result["resource_samples"] = [adapter.ingest(row) for row in resource_rows]
    proc_required = launcher.get("image", {}).get("Os") == "linux"
    guarded.require(not proc_required or resource_rows, "Linux_image_requires_actual_container_proc_not_host_ps")
    for phase in phases:
        start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
        selected = {"phase": phase["phase"], "concurrency": phase["concurrency"], "tickets": [], "owner_cleanup": [], "io_samples": [], "native_process_identities": []}
        proc_samples = phase_resources(result["resource_samples"], phase, launcher) if proc_required or resource_rows else []
        if proc_required:
            phase_polls = [poll for poll in polls if start <= timestamp(poll["observed_at"]) <= finish]
            guarded.require(phase_polls and all(set(poll.get("sources", {})) == set(TRACE_ROLES) for poll in phase_polls), "phase_all_actual_Native_sources_required")
            for role, source in phase_polls[0]["sources"].items():
                proc_identity = bind_native_source({"pid": source[0]}, role, proc_samples)
                selected["native_process_identities"].append({"pid": source[0], "start_identity": str(source[1]) + ":" + source[2],
                                                              "role": role, "proc_start_identity": proc_identity})
        for stream in streams.values():
            source, project, database = stream["source"], stream["project"], stream["database"]
            events = [event for event in stream["events"] if start <= timestamp(event["observed_at"]) <= finish]
            identity = str(source["startUnixMicros"]) + ":" + source["generation"]
            proc_identity = bind_native_source(source, stream["role"], proc_samples) if proc_samples else None
            if not proc_required:
                selected["native_process_identities"].append({"pid": source["pid"], "start_identity": identity, "role": stream["role"], "proc_start_identity": proc_identity})
            if not events:
                continue
            if stream["category"] == "database":
                selected["tickets"].extend(queue_tickets(stream, events))
                selected["owner_cleanup"].extend(cleanup_events(stream, events, rpcs, operations))
            else:
                io = io_events(stream, events, rpcs, operations, identity)
                if io:
                    selected["io_samples"].append(io)
        result["phases"].append(selected)
    return result


def queue_tickets(stream, events):
    tickets = []
    for enqueue in (event for event in events if event["kind"] == "enqueue"):
        session = enqueue.get("controlId")
        if not session:
            continue
        after = [event for event in events if event["sequence"] > enqueue["sequence"] and event.get("controlId") == session]
        admit = next((event for event in after if event["kind"] == "admit"), None)
        invalid = any(event["kind"] in ("cancel", "invalidation") and (admit is None or event["sequence"] < admit["sequence"]) for event in after)
        holder = enqueue.get("holderControlId")
        release = next((event for event in events if event["kind"] == "release" and event.get("controlId") == holder and event["sequence"] > enqueue["sequence"]), None) if holder else None
        tickets.append({"ticket": enqueue["sequence"], "project": stream["project"], "database": stream["database"],
                        "queue_id": stream["queue"], "queue_epoch": str(stream["epoch"]), "session": session,
                        "enqueue_order": enqueue["sequence"], "valid": not invalid, "invalidated": invalid,
                        "admitted_order": admit["sequence"] if admit else None, "holder_session": holder,
                        "holder_release_at": release["observed_at"] if release else None,
                        "enqueued_at": enqueue["observed_at"], "admitted_at": admit["observed_at"] if admit else None})
    return tickets


def cleanup_events(stream, events, rpcs, operations):
    result = []
    for first in (event for event in events if event["kind"] == "cleanup_start"):
        last = next((event for event in events if event["kind"] == "cleanup_finish" and event.get("requestId") == first.get("requestId")
                     and event.get("controlId") == first.get("controlId") and event["sequence"] > first["sequence"]), None)
        if not last or last.get("success") is not True or last.get("reservedLane") not in ("owner_sql", "cleanup_sql"):
            continue
        event = {**last, "started_at": first["observed_at"], "finished_at": last["observed_at"]}
        joined = join_rpc(event, stream["database"], stream["project"], rpcs, operations)
        release = next((entry for entry in events if entry["kind"] == "release" and entry.get("controlId") == last.get("controlId")
                        and timestamp(first["observed_at"]) <= timestamp(entry["observed_at"]) <= timestamp(last["observed_at"])), None)
        if not joined or not release or joined[0].get("request_id") != last.get("requestId") or joined[1].get("transaction_terminal") != last.get("operation"):
            continue
        result.append({"project": stream["project"], "database": stream["database"], "session": last["controlId"],
                       "terminal": last["operation"], "terminal_acked": True, "reserved_slots": 1,
                       "reserved_lane": last["reservedLane"], "started_at": first["observed_at"], "finished_at": last["observed_at"],
                       "released_at": release["observed_at"], "elapsed_seconds": timestamp(last["observed_at"]) - timestamp(first["observed_at"])})
    return result


def io_events(stream, events, rpcs, operations, identity):
    activities = []
    for first in (event for event in events if event["kind"] == "backend_start" and event.get("operation") == "put"):
        last = next((event for event in events if event["kind"] == "backend_finish" and event.get("ioId") == first.get("ioId") and event["sequence"] > first["sequence"]), None)
        if not last or last.get("success") is not True or type(last.get("bytes")) is not int or last["bytes"] <= 0:
            continue
        before, after = first.get("bytesBefore"), last.get("bytesAfter")
        if type(before) is not int or type(after) is not int or not 0 <= before < after or after - before < last["bytes"]:
            continue
        event = {**last, "started_at": first["observed_at"], "finished_at": last["observed_at"]}
        joined = join_rpc(event, stream["database"], stream["project"], rpcs, operations)
        if not joined:
            continue
        rpc, operation = joined
        kind = "checkpoint" if rpc["path"] == "/v1/databases/checkpoint" else "write" if rpc["path"] == "/v1/sql/execute" else None
        if kind:
            activities.append({"business_operation_id": operation["business_operation_id"], "kind": kind, "before_bytes": before,
                               "join_method": "request_id" if event.get("requestId") == rpc.get("request_id") else "unique_DB_RPC_window",
                               "RPC_request_id": rpc["request_id"],
                               "after_bytes": after, "payload_bytes": last["bytes"], "started_at": first["observed_at"], "finished_at": last["observed_at"]})
    if not activities:
        return None
    return {"project": stream["project"], "database": stream["database"], "source": "native.io_counter",
            "counter_scope": stream["queue"] + ":physical_inner", "pid": stream["source"]["pid"], "process_start_identity": identity,
            "sample_count": 2 * len(activities), "before_bytes": min(entry["before_bytes"] for entry in activities),
            "after_bytes": max(entry["after_bytes"] for entry in activities), "activities": activities,
            "started_at": min(entry["started_at"] for entry in activities), "finished_at": max(entry["finished_at"] for entry in activities)}
