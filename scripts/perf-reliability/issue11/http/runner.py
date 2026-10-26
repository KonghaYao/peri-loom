#!/usr/bin/env python3
"""W0-A Hrana runner；仅 H1–H7，非正式性能数据。"""
from pathlib import Path
import sys
PERF_ROOT = Path(__file__).resolve().parents[2]
if str(PERF_ROOT) not in sys.path:
    sys.path.insert(0, str(PERF_ROOT))
from issue11.http.runner_cli import main, parse_arguments
if __name__ == "__main__":
    raise SystemExit(main())
