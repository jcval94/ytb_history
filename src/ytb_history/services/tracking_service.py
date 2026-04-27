"""Tracking window logic for active videos."""

from __future__ import annotations

from datetime import datetime, timedelta

from ytb_history.domain.models import TrackedVideoRecord, VideoSnapshot


def select_active_video_ids(
    catalog: list[TrackedVideoRecord],
    *,
    execution_date: datetime,
) -> list[str]:
    seen: set[str] = set()
    active_ids: list[str] = []
    for record in catalog:
        if record.video_id in seen:
            continue
        if record.active and record.tracking_until_date >= execution_date:
            seen.add(record.video_id)
            active_ids.append(record.video_id)
    return active_ids


def build_tracking_video_ids(
    catalog: list[TrackedVideoRecord],
    discovered_video_ids: list[str],
    *,
    execution_date: datetime,
) -> list[str]:
    tracking_ids = select_active_video_ids(catalog, execution_date=execution_date)

    seen = set(tracking_ids)
    for video_id in discovered_video_ids:
        if video_id in seen:
            continue
        seen.add(video_id)
        tracking_ids.append(video_id)
    return tracking_ids


def update_tracking_catalog(
    catalog: list[TrackedVideoRecord],
    snapshots: list[VideoSnapshot],
    *,
    execution_date: datetime,
    tracking_window_days: int = 183,
) -> list[TrackedVideoRecord]:
    records_by_video_id: dict[str, TrackedVideoRecord] = {}

    for record in catalog:
        current = records_by_video_id.get(record.video_id)
        if current is None or record.last_seen_execution_date >= current.last_seen_execution_date:
            records_by_video_id[record.video_id] = record

    for snapshot in snapshots:
        existing = records_by_video_id.get(snapshot.video_id)
        if existing is None:
            tracking_until = execution_date + timedelta(days=tracking_window_days)
            records_by_video_id[snapshot.video_id] = TrackedVideoRecord(
                video_id=snapshot.video_id,
                channel_id=snapshot.channel_id,
                channel_name=snapshot.channel_name,
                first_seen_date=execution_date,
                last_seen_execution_date=execution_date,
                tracking_until_date=tracking_until,
                active=True,
            )
            continue

        if snapshot.channel_id:
            existing.channel_id = snapshot.channel_id
        if snapshot.channel_name:
            existing.channel_name = snapshot.channel_name
        existing.last_seen_execution_date = execution_date

    for record in records_by_video_id.values():
        record.active = record.tracking_until_date >= execution_date

    return sorted(records_by_video_id.values(), key=lambda item: item.first_seen_date)
