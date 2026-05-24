"""Helpers for environment resolution across local execution contexts."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _read_windows_persistent_env(name: str) -> str:
    if sys.platform != "win32":
        return ""

    try:
        import winreg
    except ImportError:
        return ""

    key_specs = (
        (winreg.HKEY_CURRENT_USER, r"Environment"),
        (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
    )
    for hive, sub_key in key_specs:
        try:
            with winreg.OpenKey(hive, sub_key) as handle:
                value, _value_type = winreg.QueryValueEx(handle, name)
        except OSError:
            continue
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    return ""


def _read_dotenv(name: str) -> str:
    """Read a simple KEY=value entry from a local .env file when present."""

    for root in [Path.cwd(), *Path.cwd().parents]:
        dotenv_path = root / ".env"
        if not dotenv_path.exists():
            continue
        try:
            lines = dotenv_path.read_text(encoding="utf-8-sig").splitlines()
        except OSError:
            continue
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() != name:
                continue
            return value.strip().strip('"').strip("'")
    return ""


def resolve_environment_variable(name: str) -> str:
    """Return the effective environment value for the current local machine."""

    value = os.getenv(name, "").strip()
    if value:
        return value
    dotenv_value = _read_dotenv(name)
    if dotenv_value:
        return dotenv_value
    return _read_windows_persistent_env(name)
