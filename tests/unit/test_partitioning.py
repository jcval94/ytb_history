from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from ytb_history.storage.partitioning import (
    delta_path_for_run,
    export_dir_for_run,
    export_summary_path_for_run,
    growth_summary_csv_path_for_run,
    extract_execution_date_from_snapshot_path,
    list_snapshot_files,
    latest_deltas_csv_path_for_run,
    latest_snapshots_csv_path_for_run,
    make_dt_partition,
    make_run_id,
    snapshot_path_for_run,
)


def test_make_dt_partition_uses_utc() -> None:
    local = datetime(2026, 4, 27, 0, 30, 0, tzinfo=timezone(timedelta(hours=-4)))
    assert make_dt_partition(local) == "dt=2026-04-27"


def test_make_run_id_uses_hhmmssz() -> None:
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    assert make_run_id(dt) == "090501Z"


def test_snapshot_path_for_run_generates_expected_path() -> None:
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    path = snapshot_path_for_run(dt)
    assert path == Path("data/snapshots/dt=2026-04-27/run=090501Z/snapshots.jsonl.gz")


def test_delta_path_for_run_generates_expected_path() -> None:
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    path = delta_path_for_run(dt)
    assert path == Path("data/deltas/dt=2026-04-27/run=090501Z/deltas.jsonl.gz")


def test_list_snapshot_files_finds_recursive(tmp_path) -> None:
    first = tmp_path / "dt=2026-04-26" / "run=010203Z" / "snapshots.jsonl.gz"
    second = tmp_path / "dt=2026-04-27" / "run=010203Z" / "snapshots.jsonl.gz"
    first.parent.mkdir(parents=True, exist_ok=True)
    second.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"")
    second.write_bytes(b"")

    assert list_snapshot_files(tmp_path) == [first, second]


def test_extract_execution_date_from_snapshot_path_parses_dt_and_run() -> None:
    path = Path("data/snapshots/dt=2026-04-27/run=090501Z/snapshots.jsonl.gz")
    parsed = extract_execution_date_from_snapshot_path(path)
    assert parsed == datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)


def test_export_paths_for_run_generate_expected_paths() -> None:
    dt = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)
    assert export_dir_for_run(dt) == Path("data/exports/dt=2026-04-27/run=090501Z")
    assert latest_snapshots_csv_path_for_run(dt) == Path(
        "data/exports/dt=2026-04-27/run=090501Z/latest_snapshots.csv"
    )
    assert latest_deltas_csv_path_for_run(dt) == Path(
        "data/exports/dt=2026-04-27/run=090501Z/latest_deltas.csv"
    )
    assert growth_summary_csv_path_for_run(dt) == Path(
        "data/exports/dt=2026-04-27/run=090501Z/video_growth_summary.csv"
    )
    assert export_summary_path_for_run(dt) == Path(
        "data/exports/dt=2026-04-27/run=090501Z/export_summary.json"
    )
