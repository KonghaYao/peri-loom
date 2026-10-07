_COMMON = {
    "rows": "1000",
    "seed": "20261007",
    "request-timeout": "10s",
    "max-process-rss-bytes": "536870912",
    "max-generated-bytes": "268435456",
    "max-artifact-bytes": "67108864",
}

_PROFILES = {
    "mixed": {
        "cases": "mixed_oltp",
        "concurrency": "1,4",
        "warmup": "2s",
        "smoke": "10s",
        "measure": "30s",
        "offered-rate": "50",
    },
    "architecture": {
        "cases": "session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate",
        "concurrency": "1,2",
        "warmup": "2s",
        "smoke": "10s",
        "measure": "10s",
        "offered-rate": "50",
    },
    "common": {
        "cases": "point_read,index_range,pagination,mixed_oltp,hot_keys,bulk_tx,sort_aggregate_join,index_plan,rollback,constraint_error_reuse,large_values,sustained_wal,checkpoint_reopen",
        "concurrency": "1,2",
        "warmup": "1s",
        "smoke": "2s",
        "measure": "5s",
        "offered-rate": "25",
    },
}


def parameters(name="mixed"):
    if not isinstance(name, str) or name not in _PROFILES:
        raise ValueError("unknown guard profile")
    return {**_COMMON, **_PROFILES[name]}
