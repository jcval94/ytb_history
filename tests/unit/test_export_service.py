from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.export_service import export_latest_run
from ytb_history.storage.jsonl import write_jsonl_gz


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _mk_report_dir(base_data: Path, dt: str, run: str) -> Path:
    report_dir = base_data / "reports" / f"dt={dt}" / f"run={run}"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def _prepare_success_run(tmp_path: Path, dt: str = "2026-04-27", run: str = "101500Z") -> tuple[Path, Path, Path, Path]:
    report_dir = _mk_report_dir(tmp_path, dt, run)
    snapshot_path = tmp_path / "snapshots" / f"dt={dt}" / f"run={run}" / "snapshots.jsonl.gz"
    delta_path = tmp_path / "deltas" / f"dt={dt}" / f"run={run}" / "deltas.jsonl.gz"

    write_jsonl_gz(
        snapshot_path,
        [
            {
                "execution_date": "2026-04-27T10:15:00+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Ñ",
                "video_id": "vid-1",
                "title": "Título",
                "description": "Descripción",
                "upload_date": "2026-04-20T00:00:00+00:00",
                "tags": ["python", "datos"],
                "thumbnail_url": "https://example.com/t.jpg",
                "duration_seconds": 120,
                "views": 100,
                "likes": 10,
                "comments": 5,
            },
            {
                "execution_date": "2026-04-27T10:15:00+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal 2",
                "video_id": "vid-2",
                "title": "Otro",
                "description": "",
                "upload_date": "2026-04-19T00:00:00+00:00",
                "tags": ["a", "b"],
                "thumbnail_url": "",
                "duration_seconds": 60,
                "views": 0,
                "likes": None,
                "comments": None,
            },
        ],
    )

    write_jsonl_gz(
        delta_path,
        [
            {
                "execution_date": "2026-04-27T10:15:00+00:00",
                "video_id": "vid-1",
                "views_delta": 25,
                "likes_delta": 2,
                "comments_delta": 1,
                "previous_views": 75,
                "current_views": 100,
                "previous_likes": 8,
                "current_likes": 10,
                "previous_comments": 4,
                "current_comments": 5,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
            },
            {
                "execution_date": "2026-04-27T10:15:00+00:00",
                "video_id": "vid-3",
                "views_delta": 1,
                "likes_delta": 0,
                "comments_delta": 0,
                "previous_views": 9,
                "current_views": 10,
                "previous_likes": 1,
                "current_likes": 1,
                "previous_comments": 0,
                "current_comments": 0,
                "is_new_video": True,
                "title_changed": True,
                "description_changed": False,
                "tags_changed": True,
            },
        ],
    )

    _write_json(
        report_dir / "run_summary.json",
        {
            "execution_date": "2026-04-27T10:15:00+00:00",
            "status": "success",
            "snapshot_path": str(snapshot_path),
            "delta_path": str(delta_path),
        },
    )
    _write_json(report_dir / "quota_report.json", {"limit_status": "ok"})
    return report_dir, snapshot_path, delta_path, tmp_path / "exports"


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_export_latest_run_warns_when_reports_dir_missing(tmp_path: Path) -> None:
    result = export_latest_run(data_dir=tmp_path)

    assert result["status"] == "failed"
    assert any("No existe data/reports" in warning for warning in result["warnings"])


def test_export_latest_run_detects_latest_run(tmp_path: Path) -> None:
    _prepare_success_run(tmp_path, dt="2026-04-26", run="235959Z")
    new_report_dir, _, _, _ = _prepare_success_run(tmp_path, dt="2026-04-27", run="000001Z")

    result = export_latest_run(data_dir=tmp_path)

    assert result["status"] == "success"
    export_summary = json.loads(Path(result["export_summary_path"]).read_text(encoding="utf-8"))
    assert export_summary["source_run_summary_path"] == str(new_report_dir / "run_summary.json")


def test_export_latest_run_generates_all_files_and_columns(tmp_path: Path) -> None:
    _prepare_success_run(tmp_path)

    result = export_latest_run(data_dir=tmp_path)

    assert result["status"] == "success"
    snapshots_csv = Path(result["snapshots_csv_path"])
    deltas_csv = Path(result["deltas_csv_path"])
    growth_csv = Path(result["growth_summary_csv_path"])
    export_summary = Path(result["export_summary_path"])
    assert snapshots_csv.exists()
    assert deltas_csv.exists()
    assert growth_csv.exists()
    assert export_summary.exists()

    snapshot_rows = _read_csv_rows(snapshots_csv)
    delta_rows = _read_csv_rows(deltas_csv)
    growth_rows = _read_csv_rows(growth_csv)

    assert set(snapshot_rows[0].keys()) == {
        "execution_date",
        "channel_id",
        "channel_name",
        "video_id",
        "title",
        "description",
        "upload_date",
        "tags",
        "thumbnail_url",
        "duration_seconds",
        "views",
        "likes",
        "comments",
    }
    assert set(delta_rows[0].keys()) == {
        "execution_date",
        "video_id",
        "views_delta",
        "likes_delta",
        "comments_delta",
        "previous_views",
        "current_views",
        "previous_likes",
        "current_likes",
        "previous_comments",
        "current_comments",
        "is_new_video",
        "title_changed",
        "description_changed",
        "tags_changed",
    }

    assert growth_rows[0]["video_id"] == "vid-1"
    assert growth_rows[0]["views_delta"] == "25"
    assert float(growth_rows[0]["engagement_rate"]) == 0.15
    assert growth_rows[1]["engagement_rate"] == ""
    assert growth_rows[1]["views_delta"] == ""

    assert snapshot_rows[1]["likes"] == ""
    assert snapshot_rows[1]["comments"] == ""
    assert snapshot_rows[0]["tags"] == json.dumps(["python", "datos"], ensure_ascii=False)

    export_summary_payload = json.loads(export_summary.read_text(encoding="utf-8"))
    assert export_summary_payload["snapshots_exported"] == 2
    assert export_summary_payload["deltas_exported"] == 2
    assert export_summary_payload["growth_rows_exported"] == 2


def test_export_latest_run_skips_on_aborted_quota_guardrail(tmp_path: Path) -> None:
    report_dir = _mk_report_dir(tmp_path, "2026-04-27", "101500Z")
    _write_json(
        report_dir / "run_summary.json",
        {
            "execution_date": "2026-04-27T10:15:00+00:00",
            "status": "aborted_quota_guardrail",
            "snapshot_path": None,
            "delta_path": None,
        },
    )
    _write_json(report_dir / "quota_report.json", {"limit_status": "warning"})

    result = export_latest_run(data_dir=tmp_path)

    assert result["status"] == "skipped"
    assert any("aborted_quota_guardrail" in warning for warning in result["warnings"])


def test_export_latest_writes_only_inside_data_exports(tmp_path: Path) -> None:
    _prepare_success_run(tmp_path)

    result = export_latest_run(data_dir=tmp_path)

    exports_root = (tmp_path / "exports").resolve()
    for key in [
        "export_dir",
        "snapshots_csv_path",
        "deltas_csv_path",
        "growth_summary_csv_path",
        "export_summary_path",
    ]:
        assert Path(result[key]).resolve().is_relative_to(exports_root)


def test_export_service_contains_no_search_list_usage() -> None:
    content = Path("src/ytb_history/services/export_service.py").read_text(encoding="utf-8")
    assert "search.list" not in content
