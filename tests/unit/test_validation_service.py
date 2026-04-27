from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.validation_service import validate_latest_run


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _mk_report_dir(base_data: Path, dt: str = "2026-04-27", run: str = "101500Z") -> Path:
    report_dir = base_data / "reports" / f"dt={dt}" / f"run={run}"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def test_validate_latest_warns_when_reports_dir_missing(tmp_path: Path) -> None:
    result = validate_latest_run(data_dir=tmp_path)

    assert result["validations_passed"] is False
    assert result["latest_report_dir"] is None
    assert any("No existe el directorio de reportes" in warning for warning in result["warnings"])


def test_validate_latest_detects_most_recent_run(tmp_path: Path) -> None:
    old_dir = _mk_report_dir(tmp_path, dt="2026-04-26", run="235959Z")
    new_dir = _mk_report_dir(tmp_path, dt="2026-04-27", run="000001Z")

    _write_json(old_dir / "run_summary.json", {"status": "aborted_quota_guardrail"})
    _write_json(old_dir / "quota_report.json", {"limit_status": "warning"})
    (old_dir / "discovery_report.jsonl").write_text("", encoding="utf-8")
    (old_dir / "channel_errors.jsonl").write_text("", encoding="utf-8")

    _write_json(new_dir / "run_summary.json", {"status": "aborted_quota_guardrail"})
    _write_json(new_dir / "quota_report.json", {"limit_status": "ok"})
    (new_dir / "discovery_report.jsonl").write_text("", encoding="utf-8")
    (new_dir / "channel_errors.jsonl").write_text("", encoding="utf-8")

    result = validate_latest_run(data_dir=tmp_path)

    assert result["latest_report_dir"] == str(new_dir)


def test_validate_latest_passes_with_all_expected_artifacts(tmp_path: Path) -> None:
    report_dir = _mk_report_dir(tmp_path)
    snapshot = tmp_path / "snapshots" / "dt=2026-04-27" / "run=101500Z" / "snapshots.jsonl.gz"
    delta = tmp_path / "deltas" / "dt=2026-04-27" / "run=101500Z" / "deltas.jsonl.gz"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    delta.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_text("snapshot", encoding="utf-8")
    delta.write_text("delta", encoding="utf-8")

    _write_json(
        report_dir / "run_summary.json",
        {
            "status": "success",
            "snapshot_path": str(snapshot),
            "delta_path": str(delta),
            "quota_status": "ok",
            "estimated_quota_units": 12,
            "observed_quota_units": 9,
        },
    )
    _write_json(
        report_dir / "quota_report.json",
        {
            "limit_status": "ok",
            "total_estimated_units": 12,
            "total_observed_units": 9,
            "operational_limit": 100,
        },
    )
    (report_dir / "discovery_report.jsonl").write_text("{}\n", encoding="utf-8")
    (report_dir / "channel_errors.jsonl").write_text("", encoding="utf-8")

    result = validate_latest_run(data_dir=tmp_path)

    assert result["validations_passed"] is True
    assert result["snapshot_found"] is True
    assert result["delta_found"] is True
    assert result["warnings"] == []


def test_validate_latest_does_not_require_snapshot_delta_for_aborted_quota_guardrail(tmp_path: Path) -> None:
    report_dir = _mk_report_dir(tmp_path)

    _write_json(report_dir / "run_summary.json", {"status": "aborted_quota_guardrail"})
    _write_json(
        report_dir / "quota_report.json",
        {
            "limit_status": "warning",
            "total_estimated_units": 120,
            "total_observed_units": 0,
            "operational_limit": 100,
        },
    )
    (report_dir / "discovery_report.jsonl").write_text("", encoding="utf-8")
    (report_dir / "channel_errors.jsonl").write_text("", encoding="utf-8")

    result = validate_latest_run(data_dir=tmp_path)

    assert result["validations_passed"] is True
    assert result["snapshot_found"] is False
    assert result["delta_found"] is False
    assert any("total_estimated_units está en o sobre operational_limit" in w for w in result["warnings"])


def test_validate_latest_warns_if_snapshot_missing_for_success_status(tmp_path: Path) -> None:
    report_dir = _mk_report_dir(tmp_path)
    delta = tmp_path / "deltas" / "dt=2026-04-27" / "run=101500Z" / "deltas.jsonl.gz"
    delta.parent.mkdir(parents=True, exist_ok=True)
    delta.write_text("delta", encoding="utf-8")

    _write_json(
        report_dir / "run_summary.json",
        {
            "status": "success_with_warnings",
            "snapshot_path": str(tmp_path / "snapshots" / "missing.jsonl.gz"),
            "delta_path": str(delta),
        },
    )
    _write_json(
        report_dir / "quota_report.json",
        {
            "limit_status": "ok",
            "total_estimated_units": 12,
            "total_observed_units": 9,
            "operational_limit": 100,
        },
    )
    (report_dir / "discovery_report.jsonl").write_text("{}\n", encoding="utf-8")
    (report_dir / "channel_errors.jsonl").write_text("", encoding="utf-8")

    result = validate_latest_run(data_dir=tmp_path)

    assert result["validations_passed"] is False
    assert result["delta_found"] is True
    assert result["snapshot_found"] is False
    assert any("no se encontró snapshot" in warning for warning in result["warnings"])


def test_validation_service_contains_no_search_list_usage() -> None:
    content = Path("src/ytb_history/services/validation_service.py").read_text(encoding="utf-8")
    assert "search.list" not in content
