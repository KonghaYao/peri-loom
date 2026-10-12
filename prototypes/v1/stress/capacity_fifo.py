import json
from capacity_metrics import timestamp


def database_key(project, database):
    return json.dumps([project, database], sort_keys=True)


def effective_fifo(tickets, expected_databases):
    required = {"ticket", "project", "database", "queue_id", "queue_epoch", "session", "enqueue_order", "valid", "invalidated", "admitted_order",
                "holder_session", "holder_release_at", "enqueued_at", "admitted_at"}
    if not tickets or any(set(ticket) != required for ticket in tickets):
        return "NOT MEASURED", "DB-scoped native queue/enqueue/session trace unavailable"
    groups = {}
    for ticket in tickets:
        if (type(ticket["valid"]) is not bool or type(ticket["invalidated"]) is not bool
                or not ticket["queue_id"] or not ticket["queue_epoch"] or not ticket["session"]
                or type(ticket["enqueue_order"]) is not int or ticket["enqueue_order"] < 0):
            return "FAIL", "invalid native FIFO trace"
        scope = (database_key(ticket["project"], ticket["database"]), str(ticket["queue_id"]), str(ticket["queue_epoch"]))
        groups.setdefault(scope, []).append(ticket)
    witnessed = set()
    for scope, entries in groups.items():
        if (len({str(entry["ticket"]) for entry in entries}) != len(entries)
                or len({entry["enqueue_order"] for entry in entries}) != len(entries)):
            return "FAIL", "duplicate ticket/enqueue order within native DB queue epoch"
        active = [entry for entry in entries if entry["valid"] and not entry["invalidated"]]
        if any(type(entry["admitted_order"]) is not int or entry["admitted_order"] < 0 for entry in active):
            return "NOT MEASURED", "valid FIFO ticket admission unresolved"
        if len({entry["admitted_order"] for entry in active}) != len(active):
            return "FAIL", "duplicate admission order within native DB queue epoch"
        ordered = sorted(active, key=lambda entry: entry["admitted_order"])
        if [entry["enqueue_order"] for entry in ordered] != sorted(entry["enqueue_order"] for entry in active):
            return "FAIL", "effective FIFO violated within native DB queue epoch"
        contended = [entry for entry in active if entry["holder_session"]
                     and entry["holder_session"] != entry["session"]
                     and any(other["session"] == entry["holder_session"] and timestamp(other["admitted_at"]) <= timestamp(entry["enqueued_at"])
                             for other in active)
                     and timestamp(entry["enqueued_at"]) < timestamp(entry["holder_release_at"]) <= timestamp(entry["admitted_at"])]
        if len({entry["session"] for entry in active}) >= 2 and contended:
            witnessed.add(scope[0])
    if not expected_databases or witnessed != set(expected_databases):
        return "NOT MEASURED", "actual holder/waiter enqueue < release <= admit overlap not observed for every active DB"
    return "PASS", None
