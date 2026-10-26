#!/usr/bin/env python3
"""Read the recovered fixture after an independently supervised restart.

Never starts/stops a product process, replays SQL, or infers a restart from a
successful read. --restart-evidence must come from the run supervisor.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid

SCRIPT_ROOT = Path(__file__).resolve().parent
PERF_ROOT = SCRIPT_ROOT.parent
# Direct execution adds issue11/ to sys.path. Its local http/ package must not
# shadow the standard-library http.client used by the TLS transport. Import
# issue11 only through PERF_ROOT, including when launched from SCRIPT_ROOT.
sys.path[:] = [entry for entry in sys.path if Path(entry or ".").resolve() != SCRIPT_ROOT]
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11 import evidence_io
from issue11.gate_checks import _verify_run_hashes
from issue11.http import protocol
from issue11.http.client import HranaClient
from issue11.http.runtime import _read_credential
from issue11.recovery_v2 import digest, expected_writes, validate_recovery


def collect(view, client, restart):
    problems = []
    writes, expected = expected_writes(view, problems)
    if problems:
        raise ValueError("raw_acknowledgements_invalid: " + repr(problems))
    database = view["metadata"]["database"]
    path = protocol.pipeline_path(database)
    baton = ""
    rows, requests = [], []

    def request(step):
        nonlocal baton
        request_id = uuid.uuid4().hex
        reply, exchange, failure = client.post(protocol.encode_pipeline(baton, [step]),
            path, request_id, uuid.uuid4().hex, [step], baton_in=baton)
        requests.append({"request_id": request_id, "status": exchange.status,
                         "outcome": exchange.outcome, "step_id": step.step_id})
        if failure or reply is None or not reply.ok:
            raise ValueError("recovery_read_unconfirmed: " + (exchange.error_code or "reply_error"))
        baton = reply.baton or ""
        return reply

    for expected_row in expected:
        table = expected_row["table"]  # Derived only from the fixed fixture contract.
        step = protocol.Step(uuid.uuid4().hex,
            "SELECT value, payload FROM " + table + " WHERE id = ?",
            [protocol.integer_arg(expected_row["id"])])
        reply = request(step)
        values = reply.results[0].rows()
        if len(values) != 1 or len(values[0]) != 2 or not isinstance(values[0][1], str):
            raise ValueError("recovery_row_missing_or_invalid")
        rows.append({"table": table, "id": expected_row["id"], "value": int(values[0][0]),
                     "payload_sha256": hashlib.sha256(values[0][1].encode()).hexdigest()})
    if baton:
        reply = request(protocol.Step(uuid.uuid4().hex, kind="close"))
        if not reply.close_acknowledged or baton:
            raise ValueError("recovery_close_unconfirmed")
    proof = {
        "schema": 2, "run_id": view["run_id"], "database": database,
        "acknowledged_operations_sha256": digest(writes), "acked_writes": len(writes),
        "recovered_writes": len(writes), "mismatches": 0 if rows == expected else 1,
        "rows": rows, "restart": {**restart, "verified_at_unix_ms": int(time.time() * 1000)},
        "requests": requests,
    }
    updated = dict(view, oracle={**view["oracle"], "recovery": proof})
    validate_recovery(updated, problems)
    if problems:
        raise ValueError("recovery_proof_invalid: " + repr(problems))
    return proof


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--restart-evidence", type=Path, required=True)
    parser.add_argument("--credential-env")
    parser.add_argument("--credential-file")
    parser.add_argument("--ca-file")
    parser.add_argument("--diagnostic-plaintext", action="store_true")
    parser.add_argument("--attach", action="store_true", help="attach proof to oracle.json and reseal manifests")
    arguments = parser.parse_args(argv)
    root = arguments.run_directory
    problems = []
    _verify_run_hashes(root, problems)
    if problems:
        parser.error("source evidence is not intact: " + repr(problems))
    metadata = evidence_io.read_json(root / "run.json")
    view = {"run_id": root.name, "record": metadata, "metadata": metadata,
            "oracle": evidence_io.read_json(root / "oracle.json")}
    for name in ("requests", "operations"):
        view[name] = [json.loads(line) for line in (root / (name + ".jsonl")).read_text().splitlines()]
    client = HranaClient(arguments.endpoint, _read_credential(arguments), ca_file=arguments.ca_file,
                         allow_plaintext_loopback=arguments.diagnostic_plaintext)
    try:
        proof = collect(view, client, evidence_io.read_json(arguments.restart_evidence))
    finally:
        client.close()
    proof["source_manifest_sha256"] = hashlib.sha256((root / "SHA256SUMS.json").read_bytes()).hexdigest()
    if arguments.attach:
        oracle = {**view["oracle"], "recovery": proof}
        evidence_io.write_json(root / "oracle.json", oracle, reseal=True)
        evidence_io.write_manifests(root, root.name, reseal=True)
    else:
        evidence_io.write_json(root / "recovery-proof.json", proof)
    print(json.dumps({"run_id": root.name, "verified_writes": proof["acked_writes"],
                      "verified_final_rows": len(proof["rows"]), "attached": arguments.attach,
                      "fault_model": proof["restart"].get("method"), "formal_pass": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
