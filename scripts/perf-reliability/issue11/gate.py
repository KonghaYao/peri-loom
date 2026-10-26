#!/usr/bin/env python3
"""Issue 11 gate: recompute status from raw JSONL, manifests, and snapshots.

The gate never reads summary.json. A modified summary cannot change a result;
only raw records, the integrity manifest, the frozen contract, and the operator
environment document can. Exit code 0 is reserved for a complete formal PASS.
"""
from pathlib import Path
import sys

PERF_ROOT = Path(__file__).resolve().parents[1]
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))

from issue11.gate_status import _stability_problems, _combine  # noqa: E402
from issue11.gate_checks import _verify_run_hashes, _latency_stats, _target_problems  # noqa: E402
from issue11.gate_evaluate import evaluate, main  # noqa: E402
from issue11.gate_common import PASS, PHASE_PASS, INCOMPLETE, NOT_RUN, FAIL  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
