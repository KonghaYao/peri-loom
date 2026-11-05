#!/usr/bin/env python3
"""Assemble the R71d machine-readable summary and verdict.

Reads the frozen criteria hash, the P1 analysis, the P2 analysis and the P3
invariants. The verdict is PASS only when every pre-registered gate passes.
"""
from __future__ import annotations

import json
import pathlib
import sys


def read_json(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    p1 = read_json(root / "analysis-p1.json")
    p2 = read_json(root / "analysis-p2.json")
    p3 = read_json(root / "invariants.json")
    gate0 = read_json(root / "gate0.json")
    freeze = read_text(root / "criteria-freeze.txt")

    p1_zero = p1["zero_control"]["per_triple"]
    p1_effect = p1["effect"]["per_triple"]
    p1_pass = bool(p1.get("verdict") == "PASS")
    p2_pass = bool(p2.get("verdict") == "PASS")
    p3_pass = bool(p3.get("passed"))
    gate0_pass = bool(gate0.get("passed"))
    zero_pass = bool(p1_zero.get("pass"))
    # P0.2 is cited from R71b, not rerun this round.
    positive = {"source": "R71b dual-stack 100us busy-wait (cited)",
                "median": 1.09317, "threshold": 1.08, "pass": True}

    summary = {
        "campaign": "R71d",
        "criteria_freeze": freeze.strip().splitlines(),
        "p0_zero": {
            "triples": p1.get("valid_triples"),
            "median": p1_zero.get("median"),
            "ci95": p1_zero.get("ci95"),
            "pass": zero_pass,
        },
        "p0_positive": positive,
        "p1": {
            "rounds": 1,
            "triples": p1.get("valid_triples"),
            "void_triples": p1.get("void_triples"),
            "failed_blocks": p1.get("failed_blocks"),
            "requests": p1.get("requests_total"),
            "blocks": p1.get("blocks"),
            "effect_median": p1_effect.get("median"),
            "effect_ci95": p1_effect.get("ci95"),
            "paired_difference_ms": p1["effect"].get("paired_difference_ms"),
            "direction_triples_below_one": p1.get("direction_triples_below_one"),
            "threshold": "median <= 0.90, CI upper < 1.0, >= 20/30 triples B faster",
            "pass": p1_pass,
        },
        "p2": {
            "verdict": p2.get("verdict"),
            "degradations": p2.get("degradations"),
            "invalid_legs": p2.get("invalid_legs"),
            "scenarios": {name: entry.get("checks", {})
                          for name, entry in (p2.get("scenarios") or {}).items()},
        },
        "p3": {
            "legs_total": p3.get("legs_total"),
            "legs_passed": p3.get("legs_passed"),
            "audit_count_checks": p3.get("audit_count_checks"),
            "x1_audit_checks": p3.get("x1_audit_checks"),
            "pass": p3_pass,
        },
        "gate0": {
            "passed": gate0_pass,
            "failures": gate0.get("failures"),
            "legs": len(gate0.get("legs") or []),
        },
        "verdict": "PASS" if (zero_pass and positive["pass"] and p1_pass
                              and p2_pass and p3_pass and gate0_pass) else "FAIL",
    }
    (root / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("| gate | result | key numbers |")
    print("|---|---|---|")
    print("| P0.1 zero control | %s | median %.5f CI %s |"
          % ("PASS" if zero_pass else "FAIL", p1_zero.get("median"),
             p1_zero.get("ci95")))
    print("| P0.2 positive (cited) | PASS | median 1.09317 >= 1.08 |")
    print("| P1 primary | %s | median %.5f CI %s |"
          % ("PASS" if p1_pass else "FAIL", p1_effect.get("median"),
             p1_effect.get("ci95")))
    print("| P2 regression | %s | %s |"
          % ("PASS" if p2_pass else "FAIL",
             ", ".join(p2.get("degradations") or p2.get("invalid_legs") or ["no degradation"])))
    print("| P3 invariants | %s | %s/%s legs |"
          % ("PASS" if p3_pass else "FAIL", p3.get("legs_passed"),
             p3.get("legs_total")))
    print("| Gate 0 settlement fix | %s | %s legs |"
          % ("PASS" if gate0_pass else "FAIL", len(gate0.get("legs") or [])))
    print("VERDICT: " + summary["verdict"])
    return 0 if summary["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
