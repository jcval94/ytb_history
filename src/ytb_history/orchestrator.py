"""Pipeline orchestrator scaffold."""

from __future__ import annotations

from datetime import datetime, timezone

from ytb_history.domain.models import RunSummary
from ytb_history.services.delta_service import build_deltas
from ytb_history.services.quota_service import evaluate_quota_status


def run_pipeline() -> dict:
    """Run a minimal scaffold pipeline without external API calls."""
    now = datetime.now(timezone.utc)
    _ = build_deltas(current=[], previous=[])
    quota_status = evaluate_quota_status(
        total_estimated_units=0,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    )
    summary = RunSummary(
        execution_date=now,
        status="success",
        channels_total=0,
        channels_ok=0,
        channels_failed=0,
        videos_discovered=0,
        videos_tracked=0,
        errors=[],
    )
    data = summary.to_dict()
    data["quota_status"] = quota_status
    return data
