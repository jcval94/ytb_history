"""Validation helpers for domain values."""

from __future__ import annotations


def non_negative_int(value: int, field_name: str) -> int:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value
