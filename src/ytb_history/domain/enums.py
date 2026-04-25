"""Domain enums for execution state."""

from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    SUCCESS = "success"
    SUCCESS_WITH_WARNINGS = "success_with_warnings"
    FAILED = "failed"
