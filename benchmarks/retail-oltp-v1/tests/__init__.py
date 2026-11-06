"""Test bootstrap: make the package importable without installing it."""
from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
TESTS_ROOT = Path(__file__).resolve().parent
for entry in (str(PACKAGE_ROOT), str(TESTS_ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)
