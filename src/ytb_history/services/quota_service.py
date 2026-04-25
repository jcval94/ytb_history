"""Quota status service."""

from __future__ import annotations


def evaluate_quota_status(
    *,
    total_estimated_units: int,
    operational_limit: int,
    warning_limit: int,
    soft_warning_limit: int,
) -> str:
    if total_estimated_units >= operational_limit:
        return "over_operational_limit"
    if total_estimated_units >= warning_limit:
        return "warning"
    if total_estimated_units >= soft_warning_limit:
        return "soft_warning"
    return "ok"
