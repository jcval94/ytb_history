from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ytb_history.domain.models import TrackedVideoRecord
from ytb_history.repositories.video_catalog_repo import VideoCatalogRepo


def _record(video_id: str, *, last_seen_offset_days: int = 0) -> TrackedVideoRecord:
    base = datetime(2026, 4, 27, tzinfo=timezone.utc)
    return TrackedVideoRecord(
        video_id=video_id,
        channel_id="UC1",
        channel_name="Channel",
        first_seen_date=base - timedelta(days=10),
        last_seen_execution_date=base + timedelta(days=last_seen_offset_days),
        tracking_until_date=base + timedelta(days=100),
        active=True,
    )


def test_video_catalog_repo_save_and_load(tmp_path) -> None:
    path = tmp_path / "state" / "tracked_videos_catalog.jsonl"
    repo = VideoCatalogRepo(path=path)

    repo.save([_record("v1")])
    loaded = repo.load()

    assert len(loaded) == 1
    assert loaded[0].video_id == "v1"


def test_video_catalog_repo_upsert_adds_new_video(tmp_path) -> None:
    path = tmp_path / "state" / "tracked_videos_catalog.jsonl"
    repo = VideoCatalogRepo(path=path)

    repo.save([_record("v1")])
    merged = repo.upsert([_record("v2")])

    assert {row.video_id for row in merged} == {"v1", "v2"}


def test_video_catalog_repo_upsert_updates_existing_video(tmp_path) -> None:
    path = tmp_path / "state" / "tracked_videos_catalog.jsonl"
    repo = VideoCatalogRepo(path=path)

    repo.save([_record("v1", last_seen_offset_days=0)])
    merged = repo.upsert([_record("v1", last_seen_offset_days=2)])

    assert len(merged) == 1
    assert merged[0].last_seen_execution_date == datetime(2026, 4, 29, tzinfo=timezone.utc)


def test_video_catalog_repo_deduplicates_by_video_id(tmp_path) -> None:
    path = tmp_path / "state" / "tracked_videos_catalog.jsonl"
    repo = VideoCatalogRepo(path=path)

    repo.save([_record("v1", last_seen_offset_days=0), _record("v1", last_seen_offset_days=1)])
    loaded = repo.load()

    assert len(loaded) == 1
    assert loaded[0].last_seen_execution_date == datetime(2026, 4, 28, tzinfo=timezone.utc)


def test_video_catalog_repo_creates_state_folder(tmp_path) -> None:
    path = tmp_path / "nested" / "state" / "tracked_videos_catalog.jsonl"
    repo = VideoCatalogRepo(path=path)

    repo.save([_record("v1")])

    assert path.exists()
