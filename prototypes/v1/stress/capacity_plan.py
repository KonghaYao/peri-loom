import hashlib
import json
import os
from pathlib import Path

import run_guarded as guarded
from capacity_metrics import DEFAULT_ACCEPTANCE
from guard_profiles import CAPACITY_RATES, CAPACITY_WORKLOADS, parameters
from capacity_coverage import FAIRNESS_RATES, SAFE_RATES, KNEE_EXTENSIONS, schedule, coverage_certificate


TOPOLOGIES = ("single_hot", "single_many", "equal_hot", "equal_many", "victim_hot", "victim_many")
CONCURRENCY = (1, 2, 4, 8)


def expected_phases(topology):
    return ["isolated_primary_measure"] if topology.startswith("single") else ["isolated_primary_measure", "isolated_noise_measure", "combined_measure"]


def minimum_sessions(topology, phase):
    primary = 2 if topology.endswith("many") else 1
    return primary + 1 if phase.startswith("combined") else primary if phase.startswith("isolated_primary") else 1


def demand_fraction(topology, phase):
    if topology.startswith("single") or phase.startswith("combined"):
        return 1, 1
    if topology.startswith("victim"):
        return (9, 10) if phase.startswith("isolated_noise") else (1, 10)
    return 1, 2


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_private(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def template():
    return {
        "version": 1, "execution_owner": "Main", "fixture_only": True,
        "functional_only": True, "host_scope": "shared/container", "external_busy": False,
        "writer_admission": True, "launcher_report": "/private/capacity/launcher-report.json",
        "binary": "/private/capacity/peri-stress", "joint_pass": "/private/capacity/joint-pass.json",
        "output": "/private/capacity/matrix-new",
        "connections": {name: f"/private/capacity/{name}.json" for name in TOPOLOGIES},
        "rates": [25, 100, 250], "safe_rates": list(SAFE_RATES),
        "fairness_rates": dict(FAIRNESS_RATES), "knee_extensions": list(KNEE_EXTENSIONS),
        "workloads": list(CAPACITY_WORKLOADS),
        "resources": {"cpu_quota_cores": 2, "memory_limit_bytes": 2147483648,
                      "storage_limit_bytes": 1073741824, "io_budget_bytes": 268435456,
                      "scope": "operator-declared limits, actual deployment config must be in guard.sources"},
        "acceptance": dict(DEFAULT_ACCEPTANCE),
        "guard": {
            "artifacts": {"/private/capacity/peri-stress": "REPLACE_SHA256"},
            "sources": {"/private/capacity/frozen-container-config.json": "REPLACE_SHA256"},
            "launcher": {"pid": 2, "ppid": 1, "uid": os.getuid(), "start": "REPLACE_PS_START", "argv": "REPLACE_EXACT_ARGV"},
            "services": [], "owner_roots": [], "soft_stop_capable": True,
            "hard_max_seconds": 1800, "hard_grace_seconds": 5,
            "interval_seconds": 2, "probe_timeout_seconds": 2,
            "max_process_rss_bytes": 536870912, "max_output_bytes": 67108864,
            "external_builder_policy": "strict-stop", "functional_only": True,
        },
    }


def validate_config(config):
    guarded.require(set(config) == set(template()), "unexpected_matrix_fields")
    guarded.require(config["version"] == 1 and config["execution_owner"] == "Main"
                    and config["fixture_only"] is True and config["functional_only"] is True,
                    "Main_owned_fixture_functional_attestation_required")
    guarded.require(config["host_scope"] == "shared/container" and type(config["external_busy"]) is bool,
                    "explicit_shared_host_and_external_busy_required")
    guarded.require(config["writer_admission"] is True, "explicit_writer_admission_true_required")
    launcher_report = guarded.read_spec(Path(config["launcher_report"]))
    guarded.require(launcher_report.get("writer_admission") is True, "launcher_writer_profile_not_enabled")
    image_id = launcher_report.get("image", {}).get("Id")
    guarded.require(isinstance(image_id, str) and image_id.startswith("sha256:") and len(image_id) == 71, "frozen_image_id_required")
    guarded.require(launcher_report["image"].get("Os") == "linux" and launcher_report.get("container_id") and launcher_report.get("container_name")
                    and launcher_report.get("source_hash"), "frozen_Linux_container_and_source_required")
    roles = launcher_report.get("required_native_roles")
    guarded.require(roles == ["authority", "worker-a", "worker-b", "platform", "launcher"], "five_frozen_actual_container_roles_required")
    guarded.require(config["acceptance"] == DEFAULT_ACCEPTANCE, "default_acceptance_must_be_frozen_before_results")
    rates = config["rates"]
    guarded.require(isinstance(rates, list) and len(rates) == 3 and all(type(rate) is int and rate in CAPACITY_RATES for rate in rates)
                    and rates == sorted(set(rates)), "at_least_three_unique_increasing_bounded_rates_required")
    guarded.require(config["safe_rates"] == SAFE_RATES and config["fairness_rates"] == FAIRNESS_RATES
                    and config["knee_extensions"] == KNEE_EXTENSIONS and rates[-1] < KNEE_EXTENSIONS[0],
                    "Main_approved_safe_interference_and_extension_parameters_required")
    guarded.require(config["workloads"] == list(CAPACITY_WORKLOADS), "complete_workload_matrix_required")
    guarded.require(set(config["connections"]) == set(TOPOLOGIES), "complete_topology_matrix_required")
    resources = config["resources"]
    guarded.require(set(resources) == set(template()["resources"]), "explicit_resource_limits_required")
    for key in ("cpu_quota_cores", "memory_limit_bytes", "storage_limit_bytes", "io_budget_bytes"):
        value = resources[key]
        guarded.require(type(value) in (int, float) and value > 0 and value < 1 << 50, "invalid_resource_limit")
    guarded.require(bool(config["guard"].get("sources")), "frozen_deployment_source_required")
    guarded.require(config["guard"].get("functional_only") is True, "guard_functional_only_required")
    guarded.require(launcher_report.get("resources", {}).get("Memory") == resources["memory_limit_bytes"]
                    and launcher_report.get("resources", {}).get("NanoCpus") == resources["cpu_quota_cores"] * 1_000_000_000,
                    "launcher_container_limits_do_not_match_frozen_budget")
    identities = {}
    inputs = {}
    for name, filename in config["connections"].items():
        path = Path(filename)
        connection = guarded.read_spec(path)
        guarded.require(connection.get("fixture_only") is True, "new_owned_fixture_connection_required")
        primary = connection["project"]
        noise = connection.get("noisy_tenant")
        many = name.endswith("many")
        dual = not name.startswith("single")
        databases = [connection["database"], *connection.get("databases", [])]
        guarded.require(len({json.dumps(database, sort_keys=True) for database in databases}) == len(databases), "duplicate_database")
        guarded.require((len(databases) >= 2) == many and len(databases) <= 2, "topology_requires_one_or_two_primary_databases")
        guarded.require(bool(noise) == dual, "topology_project_count_mismatch")
        if dual:
            guarded.require(noise["project"] != primary and noise["api_key"] != connection["api_key"]
                            and noise["database"] not in databases, "independent_project_key_database_required")
        identities[name] = {"primary_project": primary, "noise_project": noise["project"] if noise else None,
                            "primary_databases": databases, "noise_database": noise["database"] if noise else None}
        inputs[filename] = digest(path)
    inputs[config["joint_pass"]] = digest(config["joint_pass"])
    inputs[config["launcher_report"]] = digest(config["launcher_report"])
    for filename in config["connections"].values():
        certificate = guarded.read_spec(Path(filename)).get("ca_file")
        if certificate:
            inputs[certificate] = digest(certificate)
    return identities, inputs


def freeze(config_path, destination):
    config = guarded.read_spec(Path(config_path))
    identities, inputs = validate_config(config)
    root = Path(config["output"])
    guarded.private(root, 0o700, True)
    guarded.require(not any(root.iterdir()), "fresh_empty_matrix_output_required")
    destination = Path(destination)
    guarded.private(destination.parent, 0o700, True)
    guarded.require(not destination.exists(), "frozen_plan_already_exists")
    tool_sources = {str(path.resolve()): digest(path) for path in Path(__file__).parent.glob("*.go")}
    tool_sources.update({str(path.resolve()): digest(path) for path in Path(__file__).parent.joinpath("cmd").rglob("*.go")})
    module = Path(__file__).parent / "go.mod"
    tool_sources[str(module.resolve())] = digest(module)
    tool_sources.update({str(path.resolve()): digest(path) for path in Path(__file__).parent.glob("*.py")
                        if path.name in {"run_guarded.py", "guard_profiles.py"} or path.name.startswith("capacity_")})
    wrapper = Path(__file__).parent.parent / "deploy" / "capacity-matrix.py"
    tool_sources[str(wrapper.resolve())] = digest(wrapper)
    collector = wrapper.with_name("collect-capacity.py")
    tool_sources[str(collector.resolve())] = digest(collector)
    for path in (wrapper.with_name("verify-image.py"), Path(__file__).parent.parent / "platform" / "dev.py"):
        tool_sources[str(path.resolve())] = digest(path)
    schema = Path(__file__).parent / "capacity-result.schema.json"
    tool_sources[str(schema.resolve())] = digest(schema)
    jobs = []
    for topology, workload, rate, concurrency, purpose, extension_round in schedule(config):
        demand = "victim" if topology.startswith("victim") else "equal"
        identifier = f"{topology}-{workload}-{rate}-c{concurrency}"
        output = root / identifier
        output.mkdir(mode=0o700)
        profile = f"capacity-{workload}-{demand}-{rate}-c{concurrency}"
        command = [config["binary"]]
        for key, value in {**parameters(profile), "connection": config["connections"][topology],
                           "output": str(output), "joint-pass": config["joint_pass"]}.items():
            command.extend(["-" + key, value])
        spec = {**config["guard"], "phase_profile": profile, "command": command, "output": str(output),
                "sources": {**config["guard"]["sources"], **tool_sources}}
        guarded.validate(spec)
        spec_path = output / "guard-spec.json"
        write_private(spec_path, spec)
        jobs.append({"id": identifier, "topology": topology, "workload": workload, "rate": rate,
                     "concurrency": concurrency, "demand": demand, "spec": str(spec_path),
                     "purpose": purpose, "extension_round": extension_round,
                     "spec_sha256": digest(spec_path), "output": str(output),
                     "structural_unmeasured": topology == "single_many" and concurrency == 1})
    plan = {"version": 1, "config_sha256": digest(config_path), "config": config, "identities": identities,
            "plan_path": str(destination),
            "private_input_sha256": inputs, "concurrency": list(CONCURRENCY), "jobs": jobs,
            "coverage": coverage_certificate(jobs),
            "result_schema_sha256": digest(schema),
            "scope": "D05/Q02 bounded functional capacity/fairness; G02 performance NOT PASS"}
    from capacity_collect import COLLECTOR_PROFILE
    plan["collector_profile"] = dict(COLLECTOR_PROFILE)
    write_private(destination, plan)
    write_private(str(destination) + ".sha256.json", {"sha256": digest(destination)})
    return plan


def read_plan(path):
    path = Path(path)
    plan = guarded.read_spec(path)
    seal = guarded.read_spec(Path(str(path) + ".sha256.json"))
    guarded.require(digest(path) == seal["sha256"], "frozen_plan_changed")
    validate_config(plan["config"])
    for filename, expected in plan["private_input_sha256"].items():
        guarded.require(digest(filename) == expected, "private_input_changed_since_freeze")
    checked = {}
    for job in plan["jobs"]:
        guarded.require(digest(job["spec"]) == job["spec_sha256"], "frozen_job_changed")
        spec = guarded.read_spec(Path(job["spec"]))
        for filename, expected in {**spec["artifacts"], **spec["sources"]}.items():
            if filename not in checked:
                checked[filename] = digest(filename)
            guarded.require(checked[filename] == expected, "frozen_artifact_or_source_changed")
    return plan
