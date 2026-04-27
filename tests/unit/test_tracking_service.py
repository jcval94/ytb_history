from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ytb_history.domain.models import TrackedVideoRecord, VideoSnapshot
from ytb_history.services.tracking_service import (
    build_tracking_video_ids,
    select_active_video_ids,
    update_tracking_catalog,
)


def _record(
    *,
    video_id: str,
    active: bool = True,
    tracking_until_days: int = 1,
    first_seen_days_ago: int = 2,
    last_seen_days_ago: int = 1,
    channel_id: str = "UC1",
) -> TrackedVideoRecord:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    return TrackedVideoRecord(
        video_id=video_id,
        channel_id=channel_id,
        channel_name="Channel",
        first_seen_date=execution_date - timedelta(days=first_seen_days_ago),
        last_seen_execution_date=execution_date - timedelta(days=last_seen_days_ago),
        tracking_until_date=execution_date + timedelta(days=tracking_until_days),
        active=active,
    )


def _snapshot(video_id: str, *, channel_id: str = "UC9", channel_name: str = "New Name") -> VideoSnapshot:
    return VideoSnapshot(
        execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
        channel_id=channel_id,
        channel_name=channel_name,
        video_id=video_id,
        title="t",
        description="d",
        upload_date=datetime(2026, 4, 26, tzinfo=timezone.utc),
        tags=[],
        thumbnail_url="",
        duration_seconds=0,
        views=None,
        likes=None,
        comments=None,
    )


def test_select_active_video_ids_returns_only_active_in_window() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    catalog = [_record(video_id="v1", active=True, tracking_until_days=3), _record(video_id="v2", active=False)]

    result = select_active_video_ids(catalog, execution_date=execution_date)

    assert result == ["v1"]


def test_select_active_video_ids_excludes_expired() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    catalog = [_record(video_id="v1", active=True, tracking_until_days=-1)]

    result = select_active_video_ids(catalog, execution_date=execution_date)

    assert result == []


def test_build_tracking_video_ids_unites_active_and_discovered_preserving_order() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    catalog = [_record(video_id="v1"), _record(video_id="v2")]

    result = build_tracking_video_ids(catalog, ["v3", "v4"], execution_date=execution_date)

    assert result == ["v1", "v2", "v3", "v4"]


def test_build_tracking_video_ids_deduplicates() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    catalog = [_record(video_id="v1")]

    result = build_tracking_video_ids(catalog, ["v1", "v2", "v2"], execution_date=execution_date)

    assert result == ["v1", "v2"]


def test_update_tracking_catalog_creates_new_record() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)

    updated = update_tracking_catalog([], [_snapshot("v1")], execution_date=execution_date)

    assert len(updated) == 1
    assert updated[0].video_id == "v1"
    assert updated[0].active is True
    assert updated[0].first_seen_date == execution_date


def test_update_tracking_catalog_keeps_first_seen_for_existing_record() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    existing = _record(video_id="v1", first_seen_days_ago=10, last_seen_days_ago=10)

    updated = update_tracking_catalog([existing], [_snapshot("v1")], execution_date=execution_date)

    assert updated[0].first_seen_date == existing.first_seen_date


def test_update_tracking_catalog_updates_last_seen_and_channel_data() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    existing = _record(video_id="v1", channel_id="OLD")

    updated = update_tracking_catalog([existing], [_snapshot("v1", channel_id="UC-NEW")], execution_date=execution_date)

    assert updated[0].last_seen_execution_date == execution_date
    assert updated[0].channel_id == "UC-NEW"


def test_update_tracking_catalog_marks_expired_as_inactive() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    expired = _record(video_id="v1", tracking_until_days=-1, active=True)

    updated = update_tracking_catalog([expired], [], execution_date=execution_date)

    assert updated[0].active is False


def test_update_tracking_catalog_does_not_delete_expired() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    expired = _record(video_id="v1", tracking_until_days=-1, active=True)

    updated = update_tracking_catalog([expired], [_snapshot("v2")], execution_date=execution_date)

    assert {record.video_id for record in updated} == {"v1", "v2"}


def test_update_tracking_catalog_deduplicates_by_video_id() -> None:
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)
    older = _record(video_id="v1", last_seen_days_ago=3)
    newer = _record(video_id="v1", last_seen_days_ago=1)

    updated = update_tracking_catalog([older, newer], [], execution_date=execution_date)

    assert len(updated) == 1
    assert updated[0].last_seen_execution_date == newer.last_seen_execution_date
