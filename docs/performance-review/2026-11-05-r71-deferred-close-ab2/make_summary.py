#!/usr/bin/env python3
"""Assemble the R71b machine-readable summary from this round's analyses.

Reads analysis-p0.json, analysis-p1.json, analysis-p2.json, invariants.json and
diagnostics/standard-x1/c8-summary.json. Writes summary.json. It never reads the
previous round's files.
"""
from __future__ import annotations

import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent


def load(name: str) -> dict:
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def main() -> int:
    p0 = load("analysis-p0.json")
    p1 = load("analysis-p1.json")
    p2 = load("analysis-p2.json")
    invariants = load("invariants.json")
    diagnostics = load("diagnostics/standard-x1/c8-summary.json")

    p0_zero = p0["zero_control"]
    p0_positive = p0["effect"]
    p1_zero = p1["zero_control"]
    p1_effect = p1["effect"]
    p2_f1 = p2["scenarios"]["f1"]["checks"]
    p2_l1 = p2["scenarios"]["l1"]["checks"]
    p2_x1 = p2["scenarios"]["x1"]["checks"]["throughput_qps"]

    verdicts = {
        "p0_zero_control": "PASS" if p0_zero["pass"] else "FAIL",
        "p0_positive_control": "PASS" if p0_positive["pass"] else "FAIL",
        "p1": "PASS" if p1_effect["pass"] else "FAIL",
        "p2": "PASS" if p2["verdict"] == "PASS" else "FAIL",
        "p3": "PASS" if invariants["passed"] else "FAIL",
    }
    verdicts["overall"] = ("PASS" if all(value == "PASS" for value in verdicts.values())
                           else "FAIL")

    document = {
        "round": "R71b",
        "candidate": {
            "branch": "perf/r71-deferred-session-close",
            "commit": "ac5e0c7",
            "platform_sha256": "3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014",
            "worker_sha256": "b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff",
            "image": "ghcr.io/konghayao/peri-loom:v1.2.7",
            "image_id": "sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff",
        },
        "criteria_sha256": "57a38ba4df5b8e08abe69c81195eac2a5fc5b82c8468f041c27bf7160bd1234a",
        "verdicts": verdicts,
        "p0_zero_control": {
            "triples": p1["valid_triples"],
            "median_ratio": p1_zero["per_triple"]["median"],
            "ci95": p1_zero["per_triple"]["ci95"],
            "rounds": {name: item["zero_ratio"] for name, item in
                       p1_effect["rounds"].items()},
        },
        "p0_positive_control": {
            "triples": p0["valid_triples"],
            "median_effect": p0_positive["per_triple"]["median"],
            "ci95": p0_positive["per_triple"]["ci95"],
            "paired_difference_ms": p0_positive["paired_difference_ms"],
            "zero_control": {
                "median_ratio": p0_zero["per_triple"]["median"],
                "ci95": p0_zero["per_triple"]["ci95"],
            },
            "runner": "probe binary 6517583e..., dual-stack interleaved design",
        },
        "p1": {
            "triples": p1["valid_triples"],
            "void_triples": p1["void_triples"],
            "a_requests": p1["a_requests"],
            "b_requests": p1["b_requests"],
            "blocks": p1["blocks"],
            "effect": p1_effect["per_triple"],
            "paired_difference_ms": p1_effect["paired_difference_ms"],
            "rounds": {name: item["effect"] for name, item in
                       p1_effect["rounds"].items()},
        },
        "p2": {
            "f1": p2_f1,
            "l1": p2_l1,
            "x1_c8": p2_x1,
            "verdict": p2["verdict"],
            "degradations": p2["degradations"],
        },
        "p3": {
            "legs_total": invariants["legs_total"],
            "legs_passed": invariants["legs_passed"],
            "audit_count_checks": invariants["audit_count_checks"],
        },
        "diagnostics_standard_x1_c8": diagnostics,
        "note": ("The frozen P2 metric uses the Gate 2 c8-only driver. The "
                 "post-hoc stock X1 diagnostic is reported separately and does "
                 "not change the frozen verdict."),
    }
    (ROOT / "summary.json").write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(verdicts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
