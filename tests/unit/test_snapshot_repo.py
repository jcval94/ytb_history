from __future__ import annotations

from datetime import datetime, timezone

from ytb_history.domain.models import VideoSnapshot
from ytb_history.repositories.snapshot_repo import SnapshotRepo


def _snapshot(execution_date: datetime, *, video_id: str = "v1", channel_id: str = "UCX") -> VideoSnapshot:
    return VideoSnapshot(
        execution_date=execution_date,
        channel_id=channel_id,
        channel_name="Channel",
        video_id=video_id,
        title="Title",
        description="Desc",
        upload_date=datetime(2026, 4, 20, tzinfo=timezone.utc),
        tags=["a"],
        thumbnail_url="http://example.com/1.jpg",
        duration_seconds=123,
        views=100,
        likes=10,
        comments=1,
    )


def test_save_for_run_creates_snapshots_gz(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    path = repo.save_for_run(dt, [_snapshot(dt)])

    assert path.exists()
    assert path.name == "snapshots.jsonl.gz"


def test_load_from_path_reconstructs_video_snapshot(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    path = repo.save_for_run(dt, [_snapshot(dt, video_id="v2", channel_id="UCZ")])

    loaded = repo.load_from_path(path)

    assert len(loaded) == 1
    assert loaded[0].video_id == "v2"
    assert loaded[0].channel_id == "UCZ"


def test_save_for_run_fails_when_path_exists(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    repo.save_for_run(dt, [_snapshot(dt)])

    try:
        repo.save_for_run(dt, [_snapshot(dt)])
        assert False, "Expected FileExistsError"
    except FileExistsError:
        assert True


def test_load_latest_before_returns_empty_without_previous(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    assert repo.load_latest_before(dt) == []


def test_load_latest_before_returns_most_recent_previous_snapshot(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    older = datetime(2026, 4, 27, 8, 0, 0, tzinfo=timezone.utc)
    newer = datetime(2026, 4, 27, 8, 30, 0, tzinfo=timezone.utc)
    current = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone.utc)
    repo.save_for_run(older, [_snapshot(older, video_id="old")])
    repo.save_for_run(newer, [_snapshot(newer, video_id="new")])

    loaded = repo.load_latest_before(current)

    assert len(loaded) == 1
    assert loaded[0].video_id == "new"


def test_load_latest_before_excludes_same_execution_date(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone.utc)
    repo.save_for_run(dt, [_snapshot(dt, video_id="same")])

    loaded = repo.load_latest_before(dt)

    assert loaded == []


def test_channel_id_is_preserved(tmp_path) -> None:
    repo = SnapshotRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone.utc)
    path = repo.save_for_run(dt, [_snapshot(dt, channel_id="UC123")])

    loaded = repo.load_from_path(path)

    assert loaded[0].channel_id == "UC123"
