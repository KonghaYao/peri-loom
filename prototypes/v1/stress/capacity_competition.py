from capacity_fifo import database_key
from capacity_metrics import timestamp


def io_overlap(entries, phase):
    scopes = [(entry.get("counter_scope"), entry.get("pid"), entry.get("process_start_identity")) for entry in entries]
    physical = [(entry.get("source"), entry.get("pid"), entry.get("process_start_identity")) if entry.get("source") == "proc.io"
                else (entry.get("source"), entry.get("counter_scope")) if entry.get("source") == "cgroup.io.stat"
                else (entry.get("source"), *scope) for entry, scope in zip(entries, scopes)]
    if (any(not scope[0] or type(scope[1]) is not int or scope[1] <= 0 or not scope[2] for scope in scopes)
            or len(set(scopes)) != len(scopes) or len(set(physical)) != len(physical)):
        return "NOT MEASURED", "physical IO scope/PID identity missing or shared counter relabeled across DBs"
    processes = phase.get("observed_processes", {})
    reported = phase.get("native_process_identities", [])
    if not all(any(key.endswith(":" + str(entry["pid"])) and process.get("start_identity") == entry["process_start_identity"]
                   for key, process in processes.items()) or any(identity.get("pid") == entry["pid"] and identity.get("start_identity") == entry["process_start_identity"]
                                                              for identity in reported) for entry in entries):
        return "NOT MEASURED", "IO PID/start identity not present in actual phase resource observations"
    raw = {operation.get("business_operation_id"): operation for operation in phase.get("observed_operations", []) if operation.get("acked")}
    spans = []
    for entry in entries:
        activities = entry.get("activities", [])
        if not activities or {activity.get("kind") for activity in activities} != {"write", "checkpoint"}:
            return "NOT MEASURED", "actual write/checkpoint IO activity trace missing"
        for activity in activities:
            operation = raw.get(activity.get("business_operation_id"))
            if (not operation or database_key(operation["project"], operation["database"]) != database_key(entry.get("project"), entry.get("database"))
                    or type(activity.get("before_bytes")) is not int or type(activity.get("after_bytes")) is not int
                    or not 0 <= activity["before_bytes"] < activity["after_bytes"]):
                return "NOT MEASURED", "physical IO activity not attributable to original ACK workflow and actual counter delta"
            start, finish = timestamp(activity["started_at"]), timestamp(activity["finished_at"])
            if not timestamp(operation["started_at_observed"]) <= start < finish <= timestamp(operation["finished_at_observed"]):
                return "FAIL", "physical IO activity outside original workflow window"
            if (not timestamp(entry["started_at"]) <= start < finish <= timestamp(entry["finished_at"])
                    or not entry["before_bytes"] <= activity["before_bytes"] < activity["after_bytes"] <= entry["after_bytes"]):
                return "FAIL", "physical IO activity outside attributable counter sample window"
            spans.append((entry["project"], activity["kind"], start, finish))
    projects = {entry["project"] for entry in entries}
    if phase["phase"].startswith("combined"):
        paired = {project for project, kind, start, finish in spans
                  if any(other != project and kind != other_kind and max(start, other_start) < min(finish, other_finish)
                         for other, other_kind, other_start, other_finish in spans)}
        if len(projects) < 2 or paired != projects:
            return "NOT MEASURED", "actual cross-project write/checkpoint physical IO overlap not observed"
    return "PASS", None


def cleanup_contention(entries, tickets, databases):
    witnessed = set()
    for entry in entries:
        for ticket in tickets:
            if (ticket.get("valid") is not True or ticket.get("invalidated") is not False
                    or not entry.get("session") or ticket.get("holder_session") != entry["session"]
                    or database_key(entry.get("project"), entry.get("database")) != database_key(ticket.get("project"), ticket.get("database"))):
                continue
            if not entry.get("started_at") or not entry.get("finished_at"):
                continue
            release = timestamp(ticket["holder_release_at"])
            if (timestamp(ticket["enqueued_at"]) < release <= timestamp(ticket["admitted_at"])
                    and timestamp(entry["started_at"]) < release == timestamp(entry.get("released_at", entry["finished_at"])) <= timestamp(entry["finished_at"])
                    and abs(timestamp(entry["finished_at"]) - timestamp(entry["started_at"]) - entry["elapsed_seconds"]) <= 0.001):
                witnessed.add(database_key(entry["project"], entry["database"]))
    return witnessed == set(databases) and bool(databases)
