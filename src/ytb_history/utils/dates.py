"""Date helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def days_ago(days: int, *, base: datetime | None = None) -> datetime:
    pivot = base or utc_now()
    return pivot - timedelta(days=days)
