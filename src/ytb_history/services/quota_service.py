"""Quota estimation and guardrail service."""

from __future__ import annotations

import math
from datetime import datetime

from ytb_history.domain.models import QuotaReport


def estimate_resolution_cost(uncached_channels: int) -> int:
    """Estimate units for resolving uncached channels via channels.list."""
    if uncached_channels < 0:
        raise ValueError("uncached_channels must be >= 0")
    return uncached_channels


def estimate_discovery_cost(channels_to_check: int, *, pages_per_channel: int = 1) -> int:
    """Estimate units for discovery via playlistItems.list pages."""
    if channels_to_check < 0:
        raise ValueError("channels_to_check must be >= 0")
    if pages_per_channel < 0:
        raise ValueError("pages_per_channel must be >= 0")
    return channels_to_check * pages_per_channel


def estimate_tracking_cost(videos_to_track: int, *, batch_size: int = 50) -> int:
    """Estimate units for tracking enrichment via videos.list batches."""
    if videos_to_track < 0:
        raise ValueError("videos_to_track must be >= 0")
    if batch_size <= 0 or batch_size > 50:
        raise ValueError("batch_size must be in the range 1..50")
    if videos_to_track == 0:
        return 0
    return math.ceil(videos_to_track / batch_size)


def estimate_total_quota_cost(
    *,
    uncached_channels: int,
    channels_to_check: int,
    pages_per_channel: int,
    videos_to_track: int,
    batch_size: int = 50,
) -> dict[str, int]:
    """Estimate quota units by endpoint for one run (without search.list)."""
    return {
        "channels.list": estimate_resolution_cost(uncached_channels),
        "playlistItems.list": estimate_discovery_cost(
            channels_to_check,
            pages_per_channel=pages_per_channel,
        ),
        "videos.list": estimate_tracking_cost(videos_to_track, batch_size=batch_size),
    }


def classify_quota_status(
    total_estimated_units: int,
    *,
    operational_limit: int = 7000,
    warning_limit: int = 5000,
    soft_warning_limit: int = 1000,
) -> str:
    """Classify estimated quota units against configured limits."""
    if total_estimated_units < 0:
        raise ValueError("total_estimated_units must be >= 0")
    if not (soft_warning_limit <= warning_limit <= operational_limit):
        raise ValueError("Limits must satisfy soft_warning_limit <= warning_limit <= operational_limit")

    if total_estimated_units >= operational_limit:
        return "over_operational_limit"
    if total_estimated_units >= warning_limit:
        return "warning"
    if total_estimated_units >= soft_warning_limit:
        return "soft_warning"
    return "ok"


def should_abort_run(total_estimated_units: int, *, operational_limit: int = 7000) -> bool:
    """Return whether an execution should be aborted before running."""
    if total_estimated_units < 0:
        raise ValueError("total_estimated_units must be >= 0")
    return total_estimated_units >= operational_limit


def build_quota_report(
    *,
    execution_date: datetime,
    estimated_units: dict[str, int],
    observed_units: dict[str, int] | None = None,
    operational_limit: int = 7000,
    warning_limit: int = 5000,
    soft_warning_limit: int = 1000,
) -> QuotaReport:
    """Build a complete quota report including guardrail status."""
    observed = dict(observed_units or {})
    estimated = dict(estimated_units)
    total_estimated = sum(estimated.values())
    total_observed = sum(observed.values())

    status = classify_quota_status(
        total_estimated,
        operational_limit=operational_limit,
        warning_limit=warning_limit,
        soft_warning_limit=soft_warning_limit,
    )
    abort = should_abort_run(total_estimated, operational_limit=operational_limit)

    return QuotaReport(
        execution_date=execution_date,
        estimated_units=estimated,
        observed_units=observed,
        total_estimated_units=total_estimated,
        total_observed_units=total_observed,
        operational_limit=operational_limit,
        warning_limit=warning_limit,
        soft_warning_limit=soft_warning_limit,
        limit_status=status,
        should_abort=abort,
    )


def evaluate_quota_status(
    *,
    total_estimated_units: int,
    operational_limit: int,
    warning_limit: int,
    soft_warning_limit: int,
) -> str:
    """Backward-compatible wrapper kept for existing callers/tests."""
    return classify_quota_status(
        total_estimated_units,
        operational_limit=operational_limit,
        warning_limit=warning_limit,
        soft_warning_limit=soft_warning_limit,
    )
