#!/usr/bin/env python3
"""C7 log scan: no 502 / outcome_unknown / audit-unavailable in any leg.

Reads every container.txt written by the R72 runners (Gate 0, P1, P2) and
records the number of matching lines. The same text search R71d used on the
34 legs is applied here; the raw leg directories remain in the evidence tree
for manual inspection.

Writes log-scan.json and prints the per-leg counts only for legs with hits.
"""
from __future__ import annotations

import json
import pathlib
import sys

PATTERNS = (
    "outcome unknown",
    "outcome_unknown",
    " 502",
    "audit unavailable",
    "audit_unavailable",
    "session_moved",
)


def scan(path: pathlib.Path) -> dict:
    counts = {pattern: 0 for pattern in PATTERNS}
    if not path.exists():
        return {"present": False, "counts": counts, "total": 0}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        for pattern in PATTERNS:
            if pattern in line:
                counts[pattern] += 1
    return {"present": True, "counts": counts, "total": sum(counts.values())}


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    legs = []
    for path in sorted((root / "legs").rglob("container.txt")):
        directory = path.parent
        relative = directory.relative_to(root / "legs")
        legs.append({"leg": str(relative), **scan(path)})
    failures = [leg["leg"] for leg in legs if leg["total"]]
    report = {
        "pattern": list(PATTERNS),
        "legs": legs,
        "legs_total": len(legs),
        "legs_with_hits": failures,
        "passed": len(legs) > 0 and not failures,
    }
    (root / "log-scan.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("log scan: legs=%d legs_with_hits=%d" % (len(legs), len(failures)))
    for leg in legs:
        if leg["total"]:
            print("  HIT", leg["leg"], leg["counts"])
    print("LOG-SCAN:", "PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
