from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from ytb_history.domain.models import ChannelDiscoveryReport, QuotaReport, RunSummary
from ytb_history.repositories.run_report_repo import RunReportRepo


def _dt() -> datetime:
    return datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)


def _quota_report() -> QuotaReport:
    return QuotaReport(
        execution_date=_dt(),
        estimated_units={"channels.list": 0, "playlistItems.list": 100, "videos.list": 24},
        observed_units={"playlistItems.list": 90},
        total_estimated_units=124,
        total_observed_units=90,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
        limit_status="ok",
        should_abort=False,
    )


def _summary() -> RunSummary:
    return RunSummary(
        execution_date=_dt(),
        status="success",
        channels_total=2,
        channels_ok=1,
        channels_failed=1,
        videos_discovered=5,
        videos_tracked=5,
        videos_snapshotted=4,
        videos_unavailable=1,
        snapshot_path="data/snapshots/dt=2026-04-27/run=090501Z/snapshots.jsonl.gz",
        delta_path="data/deltas/dt=2026-04-27/run=090501Z/deltas.jsonl.gz",
        quota_status="ok",
        estimated_quota_units=124,
        observed_quota_units=120,
        errors=["error de canal ñ"],
    )


def test_save_quota_report_creates_json_file(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)

    path = repo.save_quota_report(_dt(), _quota_report())

    assert path == tmp_path / "dt=2026-04-27" / "run=090501Z" / "quota_report.json"
    assert path.exists()


def test_load_quota_report_reconstructs_dict_with_total(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    path = repo.save_quota_report(_dt(), _quota_report())

    loaded = repo.load_quota_report(path)

    assert loaded["total_estimated_units"] == 124


def test_save_run_summary_creates_json_file(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)

    path = repo.save_run_summary(_dt(), _summary())

    assert path == tmp_path / "dt=2026-04-27" / "run=090501Z" / "run_summary.json"
    assert path.exists()


def test_save_discovery_report_creates_jsonl_file(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    reports = [
        ChannelDiscoveryReport(
            channel_id="ch1",
            channel_name="Canal Uno",
            uploads_playlist_id="upl1",
            pages_read=1,
            videos_seen=10,
            videos_recent=2,
            stopped_reason="reached_limit",
            error_message=None,
        )
    ]

    path = repo.save_discovery_report(_dt(), reports)

    assert path == tmp_path / "dt=2026-04-27" / "run=090501Z" / "discovery_report.jsonl"
    assert path.exists()


def test_load_discovery_report_returns_list_of_dicts(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    reports = [
        ChannelDiscoveryReport(
            channel_id="ch1",
            channel_name="Canal Uno",
            uploads_playlist_id="upl1",
            pages_read=1,
            videos_seen=10,
            videos_recent=2,
            stopped_reason="ok",
            error_message=None,
        )
    ]
    path = repo.save_discovery_report(_dt(), reports)

    loaded = repo.load_discovery_report(path)

    assert isinstance(loaded, list)
    assert loaded[0]["channel_name"] == "Canal Uno"


def test_save_channel_errors_creates_jsonl_file(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)

    path = repo.save_channel_errors(_dt(), [{"channel_id": "ch1", "error": "falló"}])

    assert path == tmp_path / "dt=2026-04-27" / "run=090501Z" / "channel_errors.jsonl"
    assert path.exists()


def test_load_channel_errors_returns_list_of_dicts(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    path = repo.save_channel_errors(_dt(), [{"channel_id": "ch1", "error": "falló"}])

    loaded = repo.load_channel_errors(path)

    assert loaded == [{"channel_id": "ch1", "error": "falló"}]


def test_report_paths_use_dt_and_run_partitions(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)

    path = repo.save_quota_report(_dt(), _quota_report())

    assert "dt=2026-04-27" in str(path)
    assert "run=090501Z" in str(path)


def test_reports_allow_overwrite_within_same_run(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    execution_date = _dt()

    first = _quota_report()
    second = QuotaReport(
        execution_date=execution_date,
        estimated_units={"channels.list": 4, "playlistItems.list": 100, "videos.list": 24},
        observed_units={},
        total_estimated_units=128,
        total_observed_units=0,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
        limit_status="ok",
        should_abort=False,
    )

    path = repo.save_quota_report(execution_date, first)
    repo.save_quota_report(execution_date, second)

    loaded = repo.load_quota_report(path)
    assert loaded["total_estimated_units"] == 128


def test_ensure_ascii_false_preserves_spanish_characters(tmp_path: Path) -> None:
    repo = RunReportRepo(base_dir=tmp_path)
    summary = _summary()

    path = repo.save_run_summary(_dt(), summary)
    raw = path.read_text(encoding="utf-8")
    parsed = json.loads(raw)

    assert "ñ" in raw
    assert parsed["errors"] == ["error de canal ñ"]
