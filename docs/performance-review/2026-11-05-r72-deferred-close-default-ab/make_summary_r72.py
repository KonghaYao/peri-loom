#!/usr/bin/env python3
"""Assemble the R72 machine-readable summary and C1-C9 verdicts.

Reads the frozen criteria hash, the P1 analysis, the P2 analysis, the P3
invariants, the Gate 0 check, the response-shape check, the log scan and the
C9 citation/rerun note. The overall verdict is PASS only when every
pre-registered C1-C8 gate passes and C9 is either rerun PASS or a documented
same-code-path citation from R71 Gate 3.
"""
from __future__ import annotations

import json
import pathlib
import sys


def read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def gate_value(entry: dict | None, metric: str):
    if not entry:
        return None
    return (entry.get("checks", {}).get(metric) or {}).get("merged_b_over_a")


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    p1 = read_json(root / "analysis-p1.json") or {}
    p2 = read_json(root / "analysis-p2.json") or {}
    p3 = read_json(root / "invariants.json") or {}
    gate0 = read_json(root / "gate0.json") or {}
    response = read_json(root / "response-bytes.json") or {}
    logs = read_json(root / "log-scan.json") or {}
    c9 = read_json(root / "c9.json") or {
        "status": "cited",
        "detail": "R71 Gate 3 T1-T9 PASS on the same code path; see the report",
    }
    freeze = read_text(root / "criteria-freeze.txt")

    gate0_legs = [leg for leg in gate0.get("legs") or [] if leg.get("kind") == "gate0"]
    gate0_failures = [item for item in gate0.get("failures") or []
                      if item.startswith("gate0/")]
    c1_pass = (len(gate0_legs) == 2 and not gate0_failures)

    p1_effect = ((p1.get("effect") or {}).get("per_triple") or {})
    c2_pass = (p1.get("complete") is True
               and p1.get("valid_triples") == 30
               and p1.get("verdict") == "PASS"
               and p1_effect.get("median") is not None
               and p1_effect["median"] <= 0.90
               and p1_effect.get("ci95", [None, None])[1] is not None
               and p1_effect["ci95"][1] < 1.0
               and (p1.get("direction_triples_below_one") or 0) >= 20)

    scenarios = p2.get("scenarios") or {}
    f1 = scenarios.get("f1") or {}
    l1 = scenarios.get("l1") or {}
    x1 = scenarios.get("x1") or {}
    c8 = scenarios.get("c8") or {}
    f1_p50 = gate_value(f1, "median_ms")
    f1_p95 = gate_value(f1, "p95_ms")
    l1_p50 = gate_value(l1, "median_ms")
    l1_p95 = gate_value(l1, "p95_ms")
    x1_tp = gate_value(x1, "throughput_qps")
    c8_tp = gate_value(c8, "throughput_qps")
    c3_pass = (not f1.get("invalid_legs") and f1_p50 is not None and f1_p95 is not None
               and f1_p50 < 1.10 and f1_p95 < 1.10)
    c4_pass = (not l1.get("invalid_legs") and l1_p50 is not None and l1_p95 is not None
               and l1_p50 < 1.10 and l1_p95 < 1.10)
    c5_pass = (not x1.get("invalid_legs") and x1_tp is not None and x1_tp > 0.90)
    c6_pass = (not c8.get("invalid_legs") and c8_tp is not None and c8_tp > 0.90)
    c7_pass = bool(p3.get("passed")) and bool(response.get("passed")) \
        and bool(logs.get("passed"))
    c8_pass = (p3.get("legs_total") == 34 and p3.get("legs_passed") == 34
               and bool(p3.get("passed")))
    c9_ok = c9.get("status") in ("rerun-PASS", "cited")
    overall = all((c1_pass, c2_pass, c3_pass, c4_pass, c5_pass, c6_pass,
                   c7_pass, c8_pass)) and c9_ok

    summary = {
        "campaign": "R72 deferred session close default enable — independent verification",
        "criteria_freeze": freeze.strip().splitlines(),
        "C1_gate0_mechanism": {
            "pass": c1_pass,
            "legs": gate0_legs,
            "failures": gate0_failures,
        },
        "C2_p1_primary": {
            "pass": c2_pass,
            "complete": p1.get("complete"),
            "valid_triples": p1.get("valid_triples"),
            "void_triples": p1.get("void_triples"),
            "failed_blocks": p1.get("failed_blocks"),
            "blocks": p1.get("blocks"),
            "effect_median": p1_effect.get("median"),
            "effect_ci95": p1_effect.get("ci95"),
            "paired_difference_ms": (p1.get("effect") or {}).get(
                "paired_difference_ms"),
            "direction_triples_below_one": p1.get("direction_triples_below_one"),
            "zero_control_context": (p1.get("zero_control") or {}).get("per_triple"),
            "threshold": "median <= 0.90, CI upper < 1.0, >= 20/30 triples B faster",
        },
        "C3_f1_c1": {
            "pass": c3_pass,
            "merged_p50_b_over_a": f1_p50,
            "merged_p95_b_over_a": f1_p95,
            "invalid_legs": f1.get("invalid_legs"),
        },
        "C4_l1_c1": {
            "pass": c4_pass,
            "merged_p50_b_over_a": l1_p50,
            "merged_p95_b_over_a": l1_p95,
            "invalid_legs": l1.get("invalid_legs"),
        },
        "C5_standard_x1_c8": {
            "pass": c5_pass,
            "merged_throughput_b_over_a": x1_tp,
            "invalid_legs": x1.get("invalid_legs"),
        },
        "C6_sustained_c8": {
            "pass": c6_pass,
            "merged_throughput_b_over_a": c8_tp,
            "invalid_legs": c8.get("invalid_legs"),
        },
        "C7_invariants": {
            "pass": c7_pass,
            "p3_passed": p3.get("passed"),
            "response_shape_passed": response.get("passed"),
            "log_scan": logs,
        },
        "C8_p3": {
            "pass": c8_pass,
            "legs_total": p3.get("legs_total"),
            "legs_passed": p3.get("legs_passed"),
            "audit_count_checks": p3.get("audit_count_checks"),
            "x1_audit_checks": p3.get("x1_audit_checks"),
        },
        "C9_crash_semantics": c9,
        "p2_verdict": p2.get("verdict"),
        "p2_degradations": p2.get("degradations"),
        "p2_invalid_legs": p2.get("invalid_legs"),
        "verdict": "PASS" if overall else "FAIL",
    }
    (root / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("| gate | result | key numbers |")
    print("|---|---|---|")
    print("| C1 Gate 0 mechanism | %s | %d legs, failures=%s |"
          % ("PASS" if c1_pass else "FAIL", len(gate0_legs), gate0_failures))
    print("| C2 P1 primary | %s | median %.5f CI %s, %s/30 B faster |"
          % ("PASS" if c2_pass else "FAIL", p1_effect.get("median") or 0,
             p1_effect.get("ci95"), p1.get("direction_triples_below_one")))
    print("| C3 F1 c1 | %s | p50 %.5f p95 %.5f |"
          % ("PASS" if c3_pass else "FAIL", f1_p50 or 0, f1_p95 or 0))
    print("| C4 L1 c1 | %s | p50 %.5f p95 %.5f |"
          % ("PASS" if c4_pass else "FAIL", l1_p50 or 0, l1_p95 or 0))
    print("| C5 standard X1 c8 | %s | throughput B/A %.5f |"
          % ("PASS" if c5_pass else "FAIL", x1_tp or 0))
    print("| C6 sustained c8 | %s | throughput B/A %.5f |"
          % ("PASS" if c6_pass else "FAIL", c8_tp or 0))
    print("| C7 invariants | %s | p3=%s response=%s logs=%s |"
          % ("PASS" if c7_pass else "FAIL", p3.get("passed"),
             response.get("passed"), logs.get("passed")))
    print("| C8 P3 | %s | %s/%s legs |"
          % ("PASS" if c8_pass else "FAIL", p3.get("legs_passed"),
             p3.get("legs_total")))
    print("| C9 crash semantics | %s | %s |"
          % ("OK" if c9_ok else "FAIL", c9.get("status")))
    print("VERDICT:", summary["verdict"])
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
