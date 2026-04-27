from __future__ import annotations

from datetime import datetime, timezone

from ytb_history.domain.models import VideoDelta
from ytb_history.repositories.delta_repo import DeltaRepo


def _delta(execution_date: datetime, *, video_id: str = "v1") -> VideoDelta:
    return VideoDelta(
        execution_date=execution_date,
        video_id=video_id,
        views_delta=5,
        likes_delta=2,
        comments_delta=1,
        previous_views=10,
        current_views=15,
        previous_likes=5,
        current_likes=7,
        previous_comments=2,
        current_comments=3,
        is_new_video=False,
        title_changed=False,
        description_changed=False,
        tags_changed=False,
    )


def test_save_for_run_creates_deltas_jsonl_gz(tmp_path) -> None:
    repo = DeltaRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)

    path = repo.save_for_run(dt, [_delta(dt)])

    assert path.exists()
    assert path.name == "deltas.jsonl.gz"


def test_load_from_path_reconstructs_video_delta(tmp_path) -> None:
    repo = DeltaRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    path = repo.save_for_run(dt, [_delta(dt, video_id="v2")])

    loaded = repo.load_from_path(path)

    assert len(loaded) == 1
    assert loaded[0].video_id == "v2"
    assert loaded[0].views_delta == 5


def test_save_for_run_fails_when_path_exists(tmp_path) -> None:
    repo = DeltaRepo(base_dir=tmp_path)
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    repo.save_for_run(dt, [_delta(dt)])

    try:
        repo.save_for_run(dt, [_delta(dt)])
        assert False, "Expected FileExistsError"
    except FileExistsError:
        assert True


def test_list_delta_files_finds_recursive_files(tmp_path) -> None:
    repo = DeltaRepo(base_dir=tmp_path)
    dt1 = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    dt2 = datetime(2026, 4, 27, 10, 5, 1, tzinfo=timezone.utc)
    repo.save_for_run(dt1, [_delta(dt1, video_id="v1")])
    repo.save_for_run(dt2, [_delta(dt2, video_id="v2")])

    files = repo.list_delta_files()

    assert len(files) == 2
    assert files[0].name == "deltas.jsonl.gz"
    assert files[1].name == "deltas.jsonl.gz"
