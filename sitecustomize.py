"""Local development bootstrap for src-layout imports."""

from __future__ import annotations

import sys
from pathlib import Path

_SRC_PATH = Path(__file__).resolve().parent / "src"
if _SRC_PATH.exists():
    src_as_str = str(_SRC_PATH)
    if src_as_str not in sys.path:
        sys.path.insert(0, src_as_str)
