from guard_profiles import CAPACITY_WORKLOADS


COVERAGE_ID = "Main-approved-D05-63jobs-127phases-v1"
FAIRNESS_RATES = {"safe": 5, "interference": 25}
SAFE_RATES = [5, 10, 25]
KNEE_EXTENSIONS = [500, 1000]


def schedule(config):
    selected = [("single_hot", "mixed", rate, 1, "safe_envelope", None) for rate in config["safe_rates"]]
    selected += [("single_hot", workload, rate, concurrency, "hot_knee", None)
                 for workload in CAPACITY_WORKLOADS for concurrency in (1, 8) for rate in config["rates"]]
    selected += [(topology, workload, rate, (4 if topology.endswith("many") else 2) if tier == "safe" else 8,
                  "fairness_" + tier, None)
                 for topology in ("equal_hot", "equal_many", "victim_hot", "victim_many")
                 for workload in CAPACITY_WORKLOADS for tier, rate in config["fairness_rates"].items()]
    selected += [("single_many", workload, config["fairness_rates"]["safe"], 2, "multi_db", None)
                 for workload in CAPACITY_WORKLOADS]
    selected += [("single_hot", workload, rate, concurrency, "hot_knee", round_index + 1)
                 for round_index, rate in enumerate(config["knee_extensions"])
                 for workload in CAPACITY_WORKLOADS for concurrency in (1, 8)]
    return selected


def coverage_certificate(jobs):
    base = [job for job in jobs if job.get("extension_round") is None]
    return {"id": COVERAGE_ID, "selected_base_jobs": len(base), "selected_base_measurement_cells": 127,
            "maximum_jobs": len(jobs), "maximum_measurement_cells": 143,
            "measurement_seconds": 3810, "stage_seconds": 5334, "maximum_stage_seconds": 6006,
            "setup_oracle_drain_seconds": "NOT MEASURED",
            "full_cartesian_status": "NOT MEASURED", "baseline_reuse": False,
            "extension_rule": "at most two pre-frozen higher rates per hot curve, only after NOT FOUND with all prior gates MEASURED; never FAIL/Unknown retry",
            "expected_progress": {"window_seconds": 5, "minimum_ACK_each_window": 1,
                                  "maximum_observed_gap_seconds": 5, "victim_ACK_offered_minimum": 0.8,
                                  "equal_normalized_Jain_minimum": 0.9},
            "admission_drop_classification": "client_drop/client_expiry separate from typed429/other_determinate/Unknown/pending; overload never excuses finite fairness bounds",
            "no_starvation_guarantee": False}


def omitted_cells(config, jobs, topologies):
    selected = {(job["topology"], job["workload"], job["rate"], job["concurrency"]) for job in jobs}
    rates = sorted(set(config["rates"] + config["safe_rates"] + list(config["fairness_rates"].values()) + config["knee_extensions"]))
    return [{"topology": topology, "workload": workload, "rate": rate, "concurrency": concurrency,
             "status": "NOT MEASURED", "reason": "outside Main-approved frozen selected coverage; no Cartesian PASS"}
            for topology in topologies for workload in CAPACITY_WORKLOADS for rate in rates for concurrency in (1, 2, 4, 8)
            if (topology, workload, rate, concurrency) not in selected]


def phase_slots(topology, phase, concurrency):
    return concurrency if topology.startswith("single") or phase.startswith("combined") else concurrency // 2
