"""Helpers for environment resolution across local execution contexts."""

from __future__ import annotations

import os
import sys


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


def resolve_environment_variable(name: str) -> str:
    """Return the effective environment value for the current local machine."""

    value = os.getenv(name, "").strip()
    if value:
        return value
    return _read_windows_persistent_env(name)
