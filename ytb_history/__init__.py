"""Compatibility package to allow running without installation."""

from __future__ import annotations

from pathlib import Path

_pkg_dir = Path(__file__).resolve().parent
_src_pkg_dir = _pkg_dir.parent / "src" / "ytb_history"

if _src_pkg_dir.exists():
    __path__.append(str(_src_pkg_dir))
