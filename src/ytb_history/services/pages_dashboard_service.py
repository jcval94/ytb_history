"""Static dashboard builder for GitHub Pages artifacts."""

from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CSV_TABLE_SPECS: tuple[tuple[str, str], ...] = (
    ("latest_video_metrics", "analytics/latest/latest_video_metrics.csv"),
    ("latest_channel_metrics", "analytics/latest/latest_channel_metrics.csv"),
    ("latest_video_scores", "analytics/latest/latest_video_scores.csv"),
    ("latest_video_advanced_metrics", "analytics/latest/latest_video_advanced_metrics.csv"),
    ("latest_channel_advanced_metrics", "analytics/latest/latest_channel_advanced_metrics.csv"),
    ("latest_title_metrics", "analytics/latest/latest_title_metrics.csv"),
    ("latest_metric_eligibility", "analytics/latest/latest_metric_eligibility.csv"),
    ("latest_video_signals", "signals/latest_video_signals.csv"),
    ("latest_channel_signals", "signals/latest_channel_signals.csv"),
    ("latest_signal_candidates", "signals/latest_signal_candidates.csv"),
    ("channel_baselines", "analytics/baselines/channel_baselines.csv"),
    ("video_lifecycle_metrics", "analytics/baselines/video_lifecycle_metrics.csv"),
    ("period_daily_video_metrics", "analytics/periods/grain=daily/video_metrics.csv"),
    ("period_weekly_video_metrics", "analytics/periods/grain=weekly/video_metrics.csv"),
    ("period_monthly_video_metrics", "analytics/periods/grain=monthly/video_metrics.csv"),
    ("period_daily_channel_metrics", "analytics/periods/grain=daily/channel_metrics.csv"),
    ("period_weekly_channel_metrics", "analytics/periods/grain=weekly/channel_metrics.csv"),
    ("period_monthly_channel_metrics", "analytics/periods/grain=monthly/channel_metrics.csv"),
    ("latest_model_leaderboard", "model_reports/latest_model_leaderboard.csv"),
    ("latest_feature_importance", "model_reports/latest_feature_importance.csv"),
    ("latest_feature_direction", "model_reports/latest_feature_direction.csv"),
    ("latest_video_nlp_features", "nlp_features/latest_video_nlp_features.csv"),
    ("latest_title_nlp_features", "nlp_features/latest_title_nlp_features.csv"),
    ("latest_semantic_clusters", "nlp_features/latest_semantic_clusters.csv"),
    ("latest_video_topics", "topic_intelligence/latest_video_topics.csv"),
    ("latest_topic_metrics", "topic_intelligence/latest_topic_metrics.csv"),
    ("latest_title_pattern_metrics", "topic_intelligence/latest_title_pattern_metrics.csv"),
    ("latest_keyword_metrics", "topic_intelligence/latest_keyword_metrics.csv"),
    ("latest_topic_opportunities", "topic_intelligence/latest_topic_opportunities.csv"),
    ("latest_content_driver_leaderboard", "model_reports/latest_content_driver_leaderboard.csv"),
    ("latest_content_driver_feature_importance", "model_reports/latest_content_driver_feature_importance.csv"),
    ("latest_content_driver_feature_direction", "model_reports/latest_content_driver_feature_direction.csv"),
    ("latest_content_driver_group_importance", "model_reports/latest_content_driver_group_importance.csv"),
    ("latest_creative_packages", "creative_packages/latest_creative_packages.csv"),
    ("latest_title_candidates", "creative_packages/latest_title_candidates.csv"),
    ("latest_hook_candidates", "creative_packages/latest_hook_candidates.csv"),
    ("latest_thumbnail_briefs", "creative_packages/latest_thumbnail_briefs.csv"),
    ("latest_script_outlines", "creative_packages/latest_script_outlines.csv"),
    ("latest_originality_checks", "creative_packages/latest_originality_checks.csv"),
    ("latest_production_checklist", "creative_packages/latest_production_checklist.csv"),
)

JSON_FILE_SPECS: tuple[tuple[str, str], ...] = (
    ("signal_summary", "signals/signal_summary.json"),
    ("latest_alerts", "alerts/latest_alerts.json"),
    ("alert_summary", "alerts/alert_summary.json"),
    ("latest_model_manifest", "model_registry/latest_model_manifest.json"),
    ("nlp_feature_summary", "nlp_features/nlp_feature_summary.json"),
    ("topic_intelligence_summary", "topic_intelligence/topic_intelligence_summary.json"),
    ("latest_model_readiness_diagnostics", "modeling/latest_model_readiness_diagnostics.json"),
    ("latest_training_gap_report", "modeling/latest_training_gap_report.json"),
    ("creative_packages_summary", "creative_packages/creative_packages_summary.json"),
)

BRIEF_FILE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("latest_weekly_brief", "briefs/latest_weekly_brief.json", "json"),
    ("latest_weekly_brief_markdown", "briefs/latest_weekly_brief.md", "text"),
    ("latest_weekly_brief_html", "briefs/latest_weekly_brief.html", "text"),
)

MODEL_REPORT_FILE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("latest_model_suite_report_html", "model_reports/latest_model_suite_report.html", "text"),
    ("latest_content_driver_report_html", "model_reports/latest_content_driver_report.html", "text"),
    ("latest_model_readiness_report_html", "modeling/latest_model_readiness_report.html", "text"),
)

READINESS_TABLE_SPECS: tuple[tuple[str, str], ...] = (
    ("latest_target_coverage_report", "modeling/latest_target_coverage_report.csv"),
)

FRONTEND_TEMPLATE_ROOT = Path("apps/pages_dashboard/src")
ASSET_FILES: tuple[str, ...] = (
    "assets/styles.css",
    "assets/app.js",
    "assets/formatters.js",
    "assets/tables.js",
    "assets/charts.js",
)
PLACEHOLDER_HTML = "Dashboard build placeholder. Frontend pending."
SLA_POLICY = {
    "operational_data_status": {"cadence": "daily", "max_age_hours": 36},
    "ml_data_status": {"cadence": "weekly_or_biweekly", "max_age_hours": 24 * 16},
}


def build_pages_dashboard(*, data_dir: str | Path = "data", site_dir: str | Path = "site") -> dict[str, Any]:
    """Build static dashboard JSON artifacts for GitHub Pages."""

    generated_at = datetime.now(timezone.utc).isoformat()
    data_root = Path(data_dir)
    site_root = Path(site_dir)
    _resolve_output(site_root, "data").mkdir(parents=True, exist_ok=True)
    _resolve_output(site_root, "assets").mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    files_written: list[str] = []
    row_counts: dict[str, int] = {}

    dashboard_index_path = data_root / "analytics" / "latest" / "dashboard_index.json"
    analytics_manifest_path = data_root / "analytics" / "latest" / "analytics_manifest.json"

    dashboard_index = _read_json_or_empty(dashboard_index_path, "dashboard_index", warnings)
    analytics_manifest = _read_json_or_empty(analytics_manifest_path, "analytics_manifest", warnings)

    files_written.append(_write_json(site_root, "data/dashboard_index.json", dashboard_index))
    files_written.append(_write_json(site_root, "data/analytics_manifest.json", analytics_manifest))

    tables: list[str] = []
    for table_name, csv_relative in CSV_TABLE_SPECS:
        tables.append(table_name)
        csv_path = data_root / csv_relative
        table_payload = _csv_to_table_json(table_name=table_name, csv_path=csv_path, generated_at=generated_at, warnings=warnings)
        row_counts[table_name] = int(table_payload["row_count"])
        files_written.append(_write_json(site_root, f"data/{table_name}.json", table_payload))

    for table_name, json_relative in JSON_FILE_SPECS:
        tables.append(table_name)
        json_path = data_root / json_relative
        payload = _read_json_or_empty(json_path, table_name, warnings)
        row_counts[table_name] = _payload_row_count(payload)
        files_written.append(_write_json(site_root, f"data/{table_name}.json", payload))

    for table_name, csv_relative in READINESS_TABLE_SPECS:
        tables.append(table_name)
        csv_path = data_root / csv_relative
        table_payload = _csv_to_table_json(table_name=table_name, csv_path=csv_path, generated_at=generated_at, warnings=warnings)
        row_counts[table_name] = int(table_payload["row_count"])
        files_written.append(_write_json(site_root, f"data/{table_name}.json", table_payload))

    brief_outputs: dict[str, str] = {}
    for brief_name, brief_relative, payload_type in BRIEF_FILE_SPECS:
        source = data_root / brief_relative
        destination = f"data/{Path(brief_relative).name}"
        if payload_type == "json":
            if source.exists():
                payload = _read_json_or_empty(source, brief_name, warnings)
                row_counts[brief_name] = _payload_row_count(payload)
            else:
                warnings.append(f"Missing brief JSON input: {source}")
                payload = {}
                row_counts[brief_name] = 0
            files_written.append(_write_json(site_root, destination, payload))
        else:
            if source.exists():
                content = source.read_text(encoding="utf-8")
            else:
                warnings.append(f"Missing brief text input: {source}")
                content = ""
            files_written.append(_write_text(site_root, destination, content))
        brief_outputs[brief_name] = destination

    model_report_outputs: dict[str, str] = {}
    for report_name, report_relative, payload_type in MODEL_REPORT_FILE_SPECS:
        source = data_root / report_relative
        destination = f"data/{Path(report_relative).name}"
        if payload_type == "json":
            if source.exists():
                payload = _read_json_or_empty(source, report_name, warnings)
                row_counts[report_name] = _payload_row_count(payload)
            else:
                warnings.append(f"Missing model report JSON input: {source}")
                payload = {}
                row_counts[report_name] = 0
            files_written.append(_write_json(site_root, destination, payload))
        else:
            if source.exists():
                content = source.read_text(encoding="utf-8")
            else:
                warnings.append(f"Missing model report text input: {source}")
                content = ""
            files_written.append(_write_text(site_root, destination, content))
        model_report_outputs[report_name] = destination

    freshness = _build_data_freshness(
        generated_at=generated_at,
        data_root=data_root,
    )
    warnings.extend(freshness["warnings"])
    notices = list(freshness["notices"])
    site_manifest = {
        "generated_at": generated_at,
        "source_dashboard_index": str(dashboard_index_path),
        "source_analytics_manifest": str(analytics_manifest_path),
        "tables": tables,
        "row_counts": row_counts,
        "brief_outputs": brief_outputs,
        "model_report_outputs": model_report_outputs,
        "data_freshness": freshness["blocks"],
        "warnings": warnings,
        "notices": notices,
        "schema_version": "pages_dashboard_v1",
    }
    files_written.append(_write_json(site_root, "data/site_manifest.json", site_manifest))

    files_written.extend(_copy_frontend_assets(site_root=site_root, warnings=warnings))

    status = "success" if not warnings else "success_with_warnings"
    return {
        "status": status,
        "site_dir": str(site_root),
        "files_written": files_written,
        "warnings": warnings,
        "row_counts": row_counts,
    }


def _build_data_freshness(*, generated_at: str, data_root: Path) -> dict[str, Any]:
    generated_dt = datetime.fromisoformat(generated_at)
    blocks = {
        "operational_data_status": _freshness_block(
            name="operational_data_status",
            key_paths=[
                data_root / "analytics/latest/latest_video_metrics.csv",
                data_root / "signals/latest_video_signals.csv",
                data_root / "alerts/latest_alerts.json",
            ],
            generated_dt=generated_dt,
        ),
        "ml_data_status": _freshness_block(
            name="ml_data_status",
            key_paths=[
                data_root / "model_reports/latest_model_leaderboard.csv",
                data_root / "model_reports/latest_content_driver_leaderboard.csv",
                data_root / "model_reports/latest_model_suite_report.html",
            ],
            generated_dt=generated_dt,
        ),
    }

    warnings: list[str] = []
    notices: list[str] = []
    for block_name, block in blocks.items():
        state = block["state"]
        if block_name == "operational_data_status" and state in {"not_initialized", "generation_error"}:
            warnings.append(f"{block_name}: Error real de generación o faltante inesperado.")
        elif block_name == "ml_data_status" and state in {"not_initialized", "stale_expected"}:
            notices.append(f"{block_name}: {block['message']}.")
        elif block_name == "ml_data_status" and state == "generation_error":
            warnings.append(f"{block_name}: Error real de generación o faltante inesperado.")
    return {"blocks": blocks, "warnings": warnings, "notices": notices}


def _freshness_block(*, name: str, key_paths: list[Path], generated_dt: datetime) -> dict[str, Any]:
    policy = SLA_POLICY[name]
    existing = [path for path in key_paths if path.exists()]
    missing = [str(path) for path in key_paths if not path.exists()]
    if not existing:
        return {
            "cadence": policy["cadence"],
            "state": "not_initialized",
            "message": "No inicializado todavía",
            "latest_source_mtime": None,
            "missing_inputs": missing,
        }

    latest_mtime = max(path.stat().st_mtime for path in existing)
    latest_dt = datetime.fromtimestamp(latest_mtime, tz=timezone.utc)
    age_hours = (generated_dt - latest_dt).total_seconds() / 3600
    if age_hours > float(policy["max_age_hours"]):
        return {
            "cadence": policy["cadence"],
            "state": "stale_expected",
            "message": "Desactualizado",
            "latest_source_mtime": latest_dt.isoformat(),
            "age_hours": round(age_hours, 2),
            "missing_inputs": missing,
        }

    if missing:
        return {
            "cadence": policy["cadence"],
            "state": "generation_error",
            "message": "Error real de generación",
            "latest_source_mtime": latest_dt.isoformat(),
            "age_hours": round(age_hours, 2),
            "missing_inputs": missing,
        }

    return {
        "cadence": policy["cadence"],
        "state": "ready",
        "message": "Datos al día",
        "latest_source_mtime": latest_dt.isoformat(),
        "age_hours": round(age_hours, 2),
        "missing_inputs": [],
    }


def _copy_frontend_assets(*, site_root: Path, warnings: list[str]) -> list[str]:
    template_root = FRONTEND_TEMPLATE_ROOT
    written: list[str] = []

    index_source = template_root / "index.html"
    if index_source.exists():
        written.append(_copy_file(site_root, index_source, "index.html"))
    else:
        warnings.append(f"Missing frontend template: {index_source}")
        index_path = _resolve_output(site_root, "index.html")
        if not index_path.exists():
            index_path.write_text(PLACEHOLDER_HTML, encoding="utf-8")
            written.append(str(index_path))

    for relative_asset in ASSET_FILES:
        source = template_root / relative_asset
        if not source.exists():
            warnings.append(f"Missing frontend asset: {source}")
            continue
        written.append(_copy_file(site_root, source, relative_asset))

    return written


def _copy_file(site_root: Path, source: Path, destination_relative: str) -> str:
    destination = _resolve_output(site_root, destination_relative)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return str(destination)


def _read_json_or_empty(path: Path, name: str, warnings: list[str]) -> dict[str, Any]:
    if not path.exists():
        warnings.append(f"Missing required JSON input: {path}")
        return {}

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError:
        warnings.append(f"Invalid JSON in {name}: {path}")
        return {}

    if isinstance(payload, dict):
        return payload

    warnings.append(f"Unexpected JSON payload type in {name}: {path}")
    return {}


def _csv_to_table_json(*, table_name: str, csv_path: Path, generated_at: str, warnings: list[str]) -> dict[str, Any]:
    if not csv_path.exists():
        warnings.append(f"Missing CSV input for {table_name}: {csv_path}")
        return {
            "name": table_name,
            "generated_at": generated_at,
            "row_count": 0,
            "columns": [],
            "rows": [],
        }

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = [_convert_csv_row(row, columns) for row in reader]

    return {
        "name": table_name,
        "generated_at": generated_at,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
    }


def _convert_csv_row(row: dict[str, str | None], columns: list[str]) -> dict[str, Any]:
    return {column: _convert_csv_value(row.get(column)) for column in columns}


def _convert_csv_value(raw_value: str | None) -> Any:
    if raw_value is None:
        return None

    value = raw_value.strip()
    if value == "":
        return None

    if value == "True":
        return True
    if value == "False":
        return False

    if _looks_like_int(value):
        try:
            return int(value)
        except ValueError:
            return raw_value

    if _looks_like_float(value):
        try:
            return float(value)
        except ValueError:
            return raw_value

    return raw_value


def _looks_like_int(value: str) -> bool:
    if value.startswith(("+", "-")):
        return value[1:].isdigit()
    return value.isdigit()


def _looks_like_float(value: str) -> bool:
    if any(marker in value for marker in ("e", "E", "inf", "nan", "Infinity", "NaN")):
        return False

    signless = value[1:] if value.startswith(("+", "-")) else value
    if signless.count(".") != 1:
        return False

    left, right = signless.split(".", maxsplit=1)
    if left == "" and right == "":
        return False

    left_ok = left == "" or left.isdigit()
    right_ok = right == "" or right.isdigit()
    return left_ok and right_ok


def _resolve_output(site_root: Path, relative_path: str) -> Path:
    destination = (site_root / relative_path).resolve()
    root_resolved = site_root.resolve()
    try:
        destination.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"Refusing to write outside site_dir: {destination}") from exc
    return destination


def _write_json(site_root: Path, relative_path: str, payload: dict[str, Any]) -> str:
    destination = _resolve_output(site_root, relative_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return str(destination)


def _write_text(site_root: Path, relative_path: str, content: str) -> str:
    destination = _resolve_output(site_root, relative_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    return str(destination)


def _payload_row_count(payload: dict[str, Any]) -> int:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return len(rows)
    alerts = payload.get("alerts")
    if isinstance(alerts, list):
        return len(alerts)
    for key in ("row_count", "alert_count", "total_alerts"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
    return 0
