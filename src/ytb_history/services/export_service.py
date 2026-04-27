"""Export helpers for latest pipeline run artifacts."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.storage.jsonl import read_jsonl_gz
from ytb_history.storage.partitioning import (
    export_dir_for_run,
    export_summary_path_for_run,
    growth_summary_csv_path_for_run,
    latest_deltas_csv_path_for_run,
    latest_snapshots_csv_path_for_run,
)
from ytb_history.utils.dates import parse_iso8601_utc

SNAPSHOT_COLUMNS = [
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
]

DELTA_COLUMNS = [
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
]

GROWTH_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "video_id",
    "title",
    "upload_date",
    "duration_seconds",
    "views",
    "likes",
    "comments",
    "views_delta",
    "likes_delta",
    "comments_delta",
    "is_new_video",
    "title_changed",
    "description_changed",
    "tags_changed",
    "engagement_rate",
]


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _latest_report_dir(reports_root: Path) -> Path | None:
    candidates: list[Path] = []
    for dt_dir in reports_root.glob("dt=*"):
        if not dt_dir.is_dir():
            continue
        for run_dir in dt_dir.glob("run=*"):
            if run_dir.is_dir():
                candidates.append(run_dir)

    if not candidates:
        return None

    return max(candidates, key=lambda p: (p.parent.name, p.name))


def _empty_if_none(value: Any) -> Any:
    if value is None:
        return ""
    return value


def _tags_as_json_string(raw_tags: Any) -> str:
    if not isinstance(raw_tags, list):
        return "[]"
    return json.dumps([str(tag) for tag in raw_tags], ensure_ascii=False)


def _as_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_snapshot_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for row in rows:
        mapped.append(
            {
                "execution_date": _empty_if_none(row.get("execution_date")),
                "channel_id": _empty_if_none(row.get("channel_id")),
                "channel_name": _empty_if_none(row.get("channel_name")),
                "video_id": _empty_if_none(row.get("video_id")),
                "title": _empty_if_none(row.get("title")),
                "description": _empty_if_none(row.get("description")),
                "upload_date": _empty_if_none(row.get("upload_date")),
                "tags": _tags_as_json_string(row.get("tags")),
                "thumbnail_url": _empty_if_none(row.get("thumbnail_url")),
                "duration_seconds": _empty_if_none(row.get("duration_seconds")),
                "views": _empty_if_none(row.get("views")),
                "likes": _empty_if_none(row.get("likes")),
                "comments": _empty_if_none(row.get("comments")),
            }
        )
    return mapped


def _build_delta_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for row in rows:
        mapped.append(
            {
                "execution_date": _empty_if_none(row.get("execution_date")),
                "video_id": _empty_if_none(row.get("video_id")),
                "views_delta": _empty_if_none(row.get("views_delta")),
                "likes_delta": _empty_if_none(row.get("likes_delta")),
                "comments_delta": _empty_if_none(row.get("comments_delta")),
                "previous_views": _empty_if_none(row.get("previous_views")),
                "current_views": _empty_if_none(row.get("current_views")),
                "previous_likes": _empty_if_none(row.get("previous_likes")),
                "current_likes": _empty_if_none(row.get("current_likes")),
                "previous_comments": _empty_if_none(row.get("previous_comments")),
                "current_comments": _empty_if_none(row.get("current_comments")),
                "is_new_video": _empty_if_none(row.get("is_new_video")),
                "title_changed": _empty_if_none(row.get("title_changed")),
                "description_changed": _empty_if_none(row.get("description_changed")),
                "tags_changed": _empty_if_none(row.get("tags_changed")),
            }
        )
    return mapped


def _compute_engagement_rate(snapshot: dict[str, Any]) -> str:
    views = _safe_int(snapshot.get("views"))
    likes = _safe_int(snapshot.get("likes")) or 0
    comments = _safe_int(snapshot.get("comments")) or 0
    if views is None or views == 0:
        return ""
    return str((likes + comments) / views)


def _build_growth_rows(
    snapshots: list[dict[str, Any]],
    deltas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    delta_by_video = {str(delta.get("video_id", "")): delta for delta in deltas}

    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        video_id = str(snapshot.get("video_id", ""))
        delta = delta_by_video.get(video_id, {})
        rows.append(
            {
                "execution_date": _empty_if_none(snapshot.get("execution_date")),
                "channel_id": _empty_if_none(snapshot.get("channel_id")),
                "channel_name": _empty_if_none(snapshot.get("channel_name")),
                "video_id": _empty_if_none(snapshot.get("video_id")),
                "title": _empty_if_none(snapshot.get("title")),
                "upload_date": _empty_if_none(snapshot.get("upload_date")),
                "duration_seconds": _empty_if_none(snapshot.get("duration_seconds")),
                "views": _empty_if_none(snapshot.get("views")),
                "likes": _empty_if_none(snapshot.get("likes")),
                "comments": _empty_if_none(snapshot.get("comments")),
                "views_delta": _empty_if_none(delta.get("views_delta")),
                "likes_delta": _empty_if_none(delta.get("likes_delta")),
                "comments_delta": _empty_if_none(delta.get("comments_delta")),
                "is_new_video": _empty_if_none(delta.get("is_new_video")),
                "title_changed": _empty_if_none(delta.get("title_changed")),
                "description_changed": _empty_if_none(delta.get("description_changed")),
                "tags_changed": _empty_if_none(delta.get("tags_changed")),
                "engagement_rate": _compute_engagement_rate(snapshot),
            }
        )

    rows.sort(
        key=lambda row: _safe_int(row.get("views_delta")) if _safe_int(row.get("views_delta")) is not None else -1,
        reverse=True,
    )
    return rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def export_latest_run(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    reports_root = data_root / "reports"
    exports_root = data_root / "exports"

    result: dict[str, Any] = {
        "status": "failed",
        "export_dir": None,
        "snapshots_csv_path": None,
        "deltas_csv_path": None,
        "growth_summary_csv_path": None,
        "export_summary_path": None,
        "snapshots_exported": 0,
        "deltas_exported": 0,
        "warnings": [],
    }
    warnings: list[str] = result["warnings"]

    if not reports_root.exists():
        warnings.append(f"No existe data/reports: {reports_root}")
        return result

    latest_dir = _latest_report_dir(reports_root)
    if latest_dir is None:
        warnings.append(f"No hay corridas previas dentro de: {reports_root}")
        return result

    run_summary_path = latest_dir / "run_summary.json"
    quota_report_path = latest_dir / "quota_report.json"
    run_summary = _load_json(run_summary_path)
    quota_report = _load_json(quota_report_path)

    if run_summary is None:
        warnings.append(f"Falta o es inválido run_summary.json: {run_summary_path}")
        return result

    if quota_report is None:
        warnings.append(f"Falta o es inválido quota_report.json: {quota_report_path}")
        return result

    run_status = str(run_summary.get("status", ""))
    if run_status == "aborted_quota_guardrail":
        warnings.append("La última corrida terminó con status=aborted_quota_guardrail; no hay snapshots/deltas para exportar.")
        result["status"] = "skipped"
        return result

    execution_date = parse_iso8601_utc(str(run_summary.get("execution_date", "")))
    if execution_date is None:
        warnings.append("No se pudo parsear execution_date desde run_summary.json")
        return result

    snapshot_path_raw = run_summary.get("snapshot_path")
    delta_path_raw = run_summary.get("delta_path")
    if not snapshot_path_raw or not delta_path_raw:
        warnings.append("run_summary.json no contiene snapshot_path y/o delta_path")
        return result

    snapshot_path = Path(str(snapshot_path_raw))
    delta_path = Path(str(delta_path_raw))
    if not snapshot_path.exists() or not delta_path.exists():
        warnings.append("No se encontraron snapshot_path y/o delta_path para la última corrida")
        return result

    snapshots_raw = read_jsonl_gz(snapshot_path)
    deltas_raw = read_jsonl_gz(delta_path)

    export_dir = export_dir_for_run(execution_date, base_dir=exports_root)
    export_dir_abs = export_dir.resolve()
    exports_root_abs = exports_root.resolve()
    if exports_root_abs not in export_dir_abs.parents and export_dir_abs != exports_root_abs:
        warnings.append("Ruta de export inválida: fuera de data/exports")
        return result

    snapshots_csv_path = latest_snapshots_csv_path_for_run(execution_date, base_dir=exports_root)
    deltas_csv_path = latest_deltas_csv_path_for_run(execution_date, base_dir=exports_root)
    growth_csv_path = growth_summary_csv_path_for_run(execution_date, base_dir=exports_root)
    export_summary_path = export_summary_path_for_run(execution_date, base_dir=exports_root)

    snapshot_rows = _build_snapshot_rows(snapshots_raw)
    delta_rows = _build_delta_rows(deltas_raw)
    growth_rows = _build_growth_rows(snapshots_raw, deltas_raw)

    _write_csv(snapshots_csv_path, SNAPSHOT_COLUMNS, snapshot_rows)
    _write_csv(deltas_csv_path, DELTA_COLUMNS, delta_rows)
    _write_csv(growth_csv_path, GROWTH_COLUMNS, growth_rows)

    export_summary = {
        "execution_date": execution_date.isoformat(),
        "source_run_summary_path": str(run_summary_path),
        "source_quota_report_path": str(quota_report_path),
        "source_snapshot_path": str(snapshot_path),
        "source_delta_path": str(delta_path),
        "export_dir": str(export_dir),
        "snapshots_exported": len(snapshot_rows),
        "deltas_exported": len(delta_rows),
        "growth_rows_exported": len(growth_rows),
        "generated_at": _as_iso_now(),
        "warnings": warnings,
    }
    export_summary_path.parent.mkdir(parents=True, exist_ok=True)
    export_summary_path.write_text(json.dumps(export_summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    result.update(
        {
            "status": "success",
            "export_dir": str(export_dir),
            "snapshots_csv_path": str(snapshots_csv_path),
            "deltas_csv_path": str(deltas_csv_path),
            "growth_summary_csv_path": str(growth_csv_path),
            "export_summary_path": str(export_summary_path),
            "snapshots_exported": len(snapshot_rows),
            "deltas_exported": len(delta_rows),
        }
    )
    return result
