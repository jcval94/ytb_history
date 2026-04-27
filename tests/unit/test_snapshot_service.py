from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ytb_history.domain.models import VideoSnapshot
from ytb_history.repositories.delta_repo import DeltaRepo
from ytb_history.repositories.snapshot_repo import SnapshotRepo
from ytb_history.services.snapshot_service import persist_snapshot_and_deltas


def _snapshot(execution_date: datetime, *, video_id: str = "v1", views: int = 10, likes: int = 1, comments: int = 1) -> VideoSnapshot:
    return VideoSnapshot(
        execution_date=execution_date,
        channel_id="UC1",
        channel_name="Channel",
        video_id=video_id,
        title="Title",
        description="Desc",
        upload_date=datetime(2026, 4, 20, tzinfo=timezone.utc),
        tags=["x"],
        thumbnail_url="http://example.com/thumb.jpg",
        duration_seconds=100,
        views=views,
        likes=likes,
        comments=comments,
    )


def test_persist_snapshot_and_delta(tmp_path) -> None:
    snapshot_repo = SnapshotRepo(base_dir=tmp_path / "snapshots")
    delta_repo = DeltaRepo(base_dir=tmp_path / "deltas")
    execution_date = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    result = persist_snapshot_and_deltas(
        execution_date=execution_date,
        snapshots=[_snapshot(execution_date)],
        snapshot_repo=snapshot_repo,
        delta_repo=delta_repo,
    )

    assert Path(result.snapshot_path).exists()
    assert Path(result.delta_path).exists()
    assert result.snapshots_written == 1
    assert result.deltas_written == 1


def test_when_no_previous_snapshot_all_deltas_are_new_video(tmp_path) -> None:
    snapshot_repo = SnapshotRepo(base_dir=tmp_path / "snapshots")
    delta_repo = DeltaRepo(base_dir=tmp_path / "deltas")
    execution_date = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    persist_snapshot_and_deltas(
        execution_date=execution_date,
        snapshots=[_snapshot(execution_date, video_id="new")],
        snapshot_repo=snapshot_repo,
        delta_repo=delta_repo,
    )

    delta_path = delta_repo.list_delta_files()[0]
    deltas = delta_repo.load_from_path(delta_path)
    assert deltas[0].is_new_video is True


def test_with_previous_snapshot_deltas_include_metric_changes(tmp_path) -> None:
    snapshot_repo = SnapshotRepo(base_dir=tmp_path / "snapshots")
    delta_repo = DeltaRepo(base_dir=tmp_path / "deltas")
    previous_date = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone.utc)
    execution_date = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    snapshot_repo.save_for_run(previous_date, [_snapshot(previous_date, views=10, likes=5, comments=2)])
    persist_snapshot_and_deltas(
        execution_date=execution_date,
        snapshots=[_snapshot(execution_date, views=15, likes=8, comments=3)],
        snapshot_repo=snapshot_repo,
        delta_repo=delta_repo,
    )

    delta_path = delta_repo.list_delta_files()[-1]
    delta = delta_repo.load_from_path(delta_path)[0]
    assert delta.views_delta == 5
    assert delta.likes_delta == 3
    assert delta.comments_delta == 1


class FailingSnapshotRepo:
    def __init__(self) -> None:
        self.called = 0

    def load_latest_before(self, execution_date: datetime) -> list[VideoSnapshot]:
        return []

    def save_for_run(self, execution_date: datetime, snapshots: list[VideoSnapshot]) -> Path:
        self.called += 1
        raise RuntimeError("snapshot write failed")


class SpyDeltaRepo:
    def __init__(self) -> None:
        self.called = 0

    def save_for_run(self, execution_date: datetime, deltas: list) -> Path:
        self.called += 1
        return Path("should-not-happen")


def test_if_snapshot_save_fails_deltas_are_not_saved() -> None:
    snapshot_repo = FailingSnapshotRepo()
    delta_repo = SpyDeltaRepo()
    execution_date = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    try:
        persist_snapshot_and_deltas(
            execution_date=execution_date,
            snapshots=[_snapshot(execution_date)],
            snapshot_repo=snapshot_repo,  # type: ignore[arg-type]
            delta_repo=delta_repo,  # type: ignore[arg-type]
        )
        assert False, "Expected RuntimeError"
    except RuntimeError:
        assert snapshot_repo.called == 1
        assert delta_repo.called == 0


def test_returns_snapshot_persistence_result_fields(tmp_path) -> None:
    snapshot_repo = SnapshotRepo(base_dir=tmp_path / "snapshots")
    delta_repo = DeltaRepo(base_dir=tmp_path / "deltas")
    execution_date = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    result = persist_snapshot_and_deltas(
        execution_date=execution_date,
        snapshots=[_snapshot(execution_date)],
        snapshot_repo=snapshot_repo,
        delta_repo=delta_repo,
    )

    assert result.snapshot_path.endswith("snapshots.jsonl.gz")
    assert result.delta_path.endswith("deltas.jsonl.gz")
    assert result.previous_snapshot_found is False
