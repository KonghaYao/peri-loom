"""E01 离线输入合同；计划不是运行授权。"""

import hashlib
import json
import math

FAMILIES = ("point", "index_list", "offset", "keyset")
VERSION = "e01-draft-v1"


def require(condition, code):
    if not condition:
        raise ValueError(code)


def integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def exact(value, keys):
    require(type(value) is dict and set(value) == set(keys), "invalid_fields")


def positive(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def template():
    return {
        "version": VERSION, "seed": 1, "mode": "smoke", "reference": None,
        "sync_mode": "full",
        "distribution": {"rows": 96, "group_weights": [3, 1],
                         "payload_bytes": [32, 64, 128], "time_span": 16,
                         "hot_fraction_ppm": 100000, "hot_access_ppm": 700000},
        "queries": {"per_family": 16, "page_size": 4, "deep_ppm": 900000},
        "versions": {"engine_commit": None, "build": None, "adapter": None},
        "budget": {"cpu_cores": None, "memory_bytes": None, "cache_bytes": None,
                   "storage_bytes": None, "max_seconds": None, "cost_limit": None},
        "slo": {"p99_ms": None, "success_fraction": None},
    }


def validate(config):
    exact(config, template())
    require(config["version"] == VERSION and config["mode"] in ("smoke", "formal"), "invalid_version_or_mode")
    require(config["sync_mode"] in ("full", "normal", "off"), "invalid_sync_mode")
    require(integer(config["seed"], 0, 2**63 - 1), "invalid_seed")
    data, queries = config["distribution"], config["queries"]
    exact(data, template()["distribution"])
    require(integer(data["rows"], 1, 2**63 - 1), "invalid_rows")
    weights, sizes = data["group_weights"], data["payload_bytes"]
    require(type(weights) is list and 1 <= len(weights) <= 64
            and all(integer(w, 1, 1000000) for w in weights), "invalid_weights")
    require(type(sizes) is list and 1 <= len(sizes) <= 64
            and all(integer(s, 1, 1048576) for s in sizes), "invalid_payload_sizes")
    require(integer(data["time_span"], 1, 2**63 - 1), "invalid_time_span")
    require(integer(data["hot_fraction_ppm"], 1, 1000000)
            and integer(data["hot_access_ppm"], 0, 1000000), "invalid_hot_distribution")
    exact(queries, template()["queries"])
    # deep_ppm 是组内相对深度：smoke 默认很深；正式阶梯由场景文档规定下限。
    require(integer(queries["per_family"], 1, 2**63 - 1)
            and integer(queries["page_size"], 1, 10000)
            and integer(queries["deep_ppm"], 0, 999999), "invalid_queries")
    for section in ("versions", "budget", "slo"):
        exact(config[section], template()[section])
    for value in config["versions"].values():
        require(value is None or (type(value) is str and 1 <= len(value) <= 128), "invalid_version_field")
    for value in config["budget"].values():
        require(value is None or positive(value), "invalid_budget")
    slo = config["slo"]
    require(slo["p99_ms"] is None or positive(slo["p99_ms"]), "invalid_slo")
    require(slo["success_fraction"] is None or
            (positive(slo["success_fraction"]) and slo["success_fraction"] <= 1), "invalid_slo")
    reference = config["reference"]
    if reference is not None:
        validate_reference(reference)
    return config


def validate_reference(reference):
    exact(reference, ("kind", "captured_at", "window", "source", "method", "snapshot_sha256",
                      "rows", "logical_bytes", "working_set_bytes", "concurrency", "rate"))
    require(reference["kind"] in ("real", "synthetic"), "invalid_reference_kind")
    for key in ("captured_at", "window", "source", "method"):
        require(type(reference[key]) is str and 0 < len(reference[key]) <= 512, "invalid_reference_metadata")
    checksum = reference["snapshot_sha256"]
    require(type(checksum) is str and len(checksum) == 64
            and all(c in "0123456789abcdef" for c in checksum), "invalid_snapshot_digest")
    for key in ("rows", "logical_bytes", "concurrency"):
        require(integer(reference[key], 1, 2**63 - 1), "invalid_reference_scale")
    require(positive(reference["rate"]), "invalid_reference_rate")
    exact(reference["working_set_bytes"], FAMILIES)
    require(all(integer(v, 1, 2**63 - 1) for v in reference["working_set_bytes"].values()), "invalid_working_set")


def plan(config):
    validate(config)
    reference = config["reference"]
    status = "REFERENCE_MISSING" if reference is None else (
        "SYNTHETIC_REFERENCE" if reference["kind"] == "synthetic" else "REAL_REFERENCE_DECLARED")
    stages = []
    if reference is not None:
        for multiplier in (2, 4, 8):
            stages.append({"multiplier": multiplier, "minimum_rows": multiplier * reference["rows"],
                           "minimum_logical_bytes": multiplier * reference["logical_bytes"],
                           "minimum_working_set_bytes": {q: multiplier * reference["working_set_bytes"][q] for q in FAMILIES},
                           "data_axis_concurrency": reference["concurrency"],
                           "separate_concurrency_axis": multiplier * reference["concurrency"]})
    return {"version": VERSION, "config_sha256": digest(config), "reference_status": status,
            "mode": config["mode"], "stages": stages,
            "execution": "BLOCKED_SCHEMA_AND_RUN_APPROVAL",
            "performance": "NOT MEASURED", "adapters": "NOT IMPLEMENTED",
            "missing_inputs": [f"{s}.{k}" for s in ("versions", "budget", "slo")
                               for k, v in config[s].items() if v is None]}
