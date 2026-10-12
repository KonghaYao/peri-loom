import math

import run_guarded as guarded
from capacity_metrics import timestamp


RESOURCE_FIELDS = {"schema_version", "source", "observed_at", "container_id", "container_name", "image_id", "source_hash",
                   "running", "oom_killed", "exit_code", "boot_id", "page_size_bytes", "clock_ticks_per_second", "processes"}
PROCESS_FIELDS = {"role", "pid", "start_ticks", "rss_pages", "cpu_ticks", "state", "uids"}
REQUIRED_ROLES = ["authority", "worker-a", "worker-b", "platform", "launcher"]
TRACE_ROLES = {"authority": "authority", "worker:worker-a": "worker-a", "worker:worker-b": "worker-b"}


class ContainerResources:
    def __init__(self, launcher, rss_limit):
        self.launcher, self.limit, self.previous = launcher, rss_limit, {}
        self.units = None
        guarded.require(type(rss_limit) is int and 0 < rss_limit <= 536870912, "original_Native_RSS_limit_required")
        guarded.require(launcher.get("container_id") and launcher.get("source_hash") and launcher.get("required_native_roles"),
                        "frozen_container_source_and_Native_roles_required")
        guarded.require(launcher["required_native_roles"] == REQUIRED_ROLES, "five_actual_frozen_container_roles_required")

    def ingest(self, record):
        guarded.require(set(record) == RESOURCE_FIELDS and record.get("schema_version") == 1 and record.get("source") == "container.proc", "actual_container_proc_schema_required")
        for field in ("container_id", "container_name", "source_hash"):
            guarded.require(record.get(field) == self.launcher[field], "container_resource_binding_changed")
        guarded.require(record.get("image_id") == self.launcher["image"]["Id"], "container_image_binding_changed")
        guarded.require(record.get("running") is True and record.get("oom_killed") is False
                        and (record.get("exit_code") is None or type(record["exit_code"]) is int and record["exit_code"] == 0), "container_exit_or_OOM_observed")
        guarded.require(record.get("boot_id") and type(record.get("page_size_bytes")) is int and record["page_size_bytes"] > 0
                        and type(record.get("clock_ticks_per_second")) is int and record["clock_ticks_per_second"] > 0, "container_proc_units_missing")
        rows = record.get("processes")
        roles = self.launcher["required_native_roles"]
        guarded.require(isinstance(rows, list) and {row.get("role") for row in rows} == set(roles) and len(rows) == len(roles), "required_Native_process_missing_or_duplicate")
        guarded.require(len({row.get("pid") for row in rows}) == len(roles), "Native_roles_cannot_share_PID")
        units = (record["boot_id"], record["page_size_bytes"], record["clock_ticks_per_second"])
        guarded.require(self.units is None or self.units == units, "container_boot_or_proc_units_changed")
        self.units = units
        sampled = timestamp(record["observed_at"])
        processes = []
        for row in rows:
            guarded.require(set(row) == PROCESS_FIELDS and all(type(row[field]) is int and row[field] >= 0 for field in ("pid", "start_ticks", "rss_pages", "cpu_ticks"))
                            and row["pid"] > 0 and row["state"] in ("R", "S", "D", "I"), "Native_proc_state_or_fields_invalid")
            guarded.require(isinstance(row["uids"], list) and len(row["uids"]) == 4 and all(type(uid) is int and uid == 10001 for uid in row["uids"]),
                            "container_process_four_UIDs_must_equal_10001")
            identity = (record["container_id"], record["boot_id"], row["pid"], row["start_ticks"])
            previous = self.previous.get(row["role"])
            guarded.require(previous is None or previous[0] == identity, "Native_PID_start_reused_or_restarted")
            rss = row["rss_pages"] * record["page_size_bytes"]
            guarded.require(rss <= self.limit, "Native_RSS_exceeded_original_512MiB_budget")
            cpu = row["cpu_ticks"] / record["clock_ticks_per_second"]
            percent = None
            if previous:
                guarded.require(sampled > previous[1] and cpu >= previous[2], "Native_proc_time_or_CPU_counter_regressed")
                percent = (cpu - previous[2]) / (sampled - previous[1]) * 100
                guarded.require(math.isfinite(percent), "Native_proc_CPU_nonfinite")
            self.previous[row["role"]] = (identity, sampled, cpu)
            processes.append({"role": row["role"], "pid": row["pid"], "status": "available", "rss_bytes": rss,
                              "cpu_seconds_proc": cpu, "cpu_seconds_semantics": "Linux proc ticks / CLK_TCK, not ps",
                              "cpu_percent_ps": None, "cpu_percent_proc_interval": percent, "uids": row["uids"],
                              "process_start_identity": record["boot_id"] + ":" + str(row["start_ticks"])})
        return {"type": "resource_sample", "source": "container.proc.status", "observed_at": record["observed_at"],
                "container_name": record["container_name"], "container_id": record["container_id"], "image_id": record["image_id"],
                "source_hash": record["source_hash"], "processes_status": "available", "processes": processes,
                "container_state": {field: record[field] for field in ("running", "oom_killed", "exit_code")}}


def phase_resources(samples, phase, launcher):
    start, finish = timestamp(phase["started_at_observed"]), timestamp(phase["finished_at_observed"])
    selected = [sample for sample in samples if start <= timestamp(sample["observed_at"]) <= finish]
    guarded.require(len(selected) >= 2 and all({row["role"] for row in sample["processes"]} == set(launcher["required_native_roles"]) for sample in selected),
                    "phase_requires_two_actual_container_proc_snapshots_all_roles")
    return selected


def bind_native_source(source, role, samples):
    guarded.require(role in TRACE_ROLES, "Native_trace_role_not_frozen_explicit_mapping")
    proc_role = TRACE_ROLES[role]
    matches = [row for sample in samples for row in sample["processes"] if row["role"] == proc_role]
    guarded.require(matches and all(row["pid"] == source["pid"] for row in matches), "Native_trace_PID_not_actual_container_proc_role")
    return matches[0]["process_start_identity"]
