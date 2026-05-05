"""Build supervised modeling dataset and readiness audit from local artifacts."""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

TARGET_COLUMNS = [
    "future_views_delta_7d",
    "future_log_views_delta_7d",
    "is_top_growth_7d",
    "outperforms_channel_7d",
]

FEATURE_COLUMNS = [
    "views_delta",
    "engagement_rate",
    "comment_rate",
    "video_age_days",
    "duration_bucket",
    "is_short",
    "alpha_score",
    "opportunity_score",
    "trend_burst_score",
    "evergreen_score",
    "packaging_problem_score",
    "metric_confidence_score",
    "channel_momentum_score",
    "channel_relative_success_score",
    "title_length_chars",
    "has_number",
    "has_question",
    "has_ai_word",
    "has_finance_word",
    "metadata_changed",
]

LEAKAGE_BANNED_EXACT = set(TARGET_COLUMNS)


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _to_bool_str(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if value is None:
        return "False"
    return "True" if str(value).strip().lower() in {"1", "true", "yes", "y"} else "False"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _first_capture_growth_files_by_day(exports_root: Path) -> list[Path]:
    first_by_day: dict[str, Path] = {}
    for csv_path in sorted(exports_root.glob("dt=*/run=*/video_growth_summary.csv")):
        day_token = next((part for part in csv_path.parts if part.startswith("dt=")), "")
        if not day_token or day_token in first_by_day:
            continue
        first_by_day[day_token] = csv_path
    return [first_by_day[day] for day in sorted(first_by_day)]


def _build_latest_inference_examples(
    *,
    data_root: Path,
    allowed_features: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    analytics_latest = data_root / "analytics" / "latest"
    required_paths = {
        "video_metrics": analytics_latest / "latest_video_metrics.csv",
        "video_scores": analytics_latest / "latest_video_scores.csv",
        "video_advanced": analytics_latest / "latest_video_advanced_metrics.csv",
        "channel_advanced": analytics_latest / "latest_channel_advanced_metrics.csv",
        "title_metrics": analytics_latest / "latest_title_metrics.csv",
    }

    loaded_rows: dict[str, list[dict[str, str]]] = {}
    for key, path in required_paths.items():
        if not path.exists():
            warnings.append(f"Missing inference source file: {path}")
            loaded_rows[key] = []
            continue
        loaded_rows[key] = _read_csv(path)

    base_rows = loaded_rows["video_metrics"]
    if not base_rows:
        return [], warnings

    video_scores = {row.get("video_id", ""): row for row in loaded_rows["video_scores"] if row.get("video_id")}
    video_advanced = {row.get("video_id", ""): row for row in loaded_rows["video_advanced"] if row.get("video_id")}
    title_metrics = {row.get("video_id", ""): row for row in loaded_rows["title_metrics"] if row.get("video_id")}
    channel_advanced = {row.get("channel_id", ""): row for row in loaded_rows["channel_advanced"] if row.get("channel_id")}

    out_rows: list[dict[str, Any]] = []
    for row in base_rows:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue
        channel_id = str(row.get("channel_id", "")).strip()
        score_row = video_scores.get(video_id, {})
        advanced_row = video_advanced.get(video_id, {})
        title_row = title_metrics.get(video_id, {})
        channel_row = channel_advanced.get(channel_id, {})

        merged = {
            **row,
            **score_row,
            **advanced_row,
            **title_row,
            **channel_row,
        }

        inference_row: dict[str, Any] = {
            "video_id": video_id,
            "execution_date": merged.get("execution_date", ""),
            "channel_id": channel_id,
            "channel_name": merged.get("channel_name", ""),
            "title": merged.get("title", ""),
        }
        for feature in allowed_features:
            value = merged.get(feature)
            if feature in {"duration_bucket"}:
                inference_row[feature] = value or ""
            elif feature in {"is_short", "has_number", "has_question", "has_ai_word", "has_finance_word", "metadata_changed"}:
                inference_row[feature] = _to_bool_str(value)
            else:
                numeric = _safe_float(value)
                inference_row[feature] = numeric if numeric is not None else ""
        out_rows.append(inference_row)

    return out_rows, warnings


def _percentile_threshold(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, max(0, int(math.ceil(q * len(sorted_values)) - 1)))
    return float(sorted_values[index])


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def build_model_dataset(*, data_dir: str | Path = "data", target_horizon_days: int = 7, tolerance_days: int = 2) -> dict[str, Any]:
    data_root = Path(data_dir)
    modeling_dir = data_root / "modeling"
    warnings: list[str] = []

    export_paths = _first_capture_growth_files_by_day(data_root / "exports")
    if not export_paths:
        warnings.append("No export video_growth_summary.csv files were found.")

    observations: list[dict[str, Any]] = []
    for path in export_paths:
        rows = _read_csv(path)
        for row in rows:
            ts = _parse_datetime(str(row.get("execution_date", "")))
            if ts is None:
                continue
            observations.append(
                {
                    "execution_date": ts,
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "video_id": row.get("video_id", ""),
                    "title": row.get("title", ""),
                    "views_delta": _safe_float(row.get("views_delta")),
                    "engagement_rate": _safe_float(row.get("engagement_rate")),
                    "comment_rate": _safe_float(row.get("comment_rate")),
                    "video_age_days": _safe_float(row.get("video_age_days")),
                    "duration_bucket": row.get("duration_bucket", ""),
                    "is_short": _to_bool_str(row.get("is_short")),
                    "metadata_changed": _to_bool_str(
                        row.get("metadata_changed") or row.get("title_changed") or row.get("description_changed") or row.get("tags_changed")
                    ),
                    "source_export_path": _rel(path, data_root),
                }
            )

    observations.sort(key=lambda item: (str(item.get("video_id", "")), item["execution_date"]))

    latest_video_scores = {}
    latest_video_advanced = {}
    latest_channel_advanced = {}
    latest_title_metrics = {}

    analytics_latest = data_root / "analytics" / "latest"
    file_map = {
        "video_scores": analytics_latest / "latest_video_scores.csv",
        "video_advanced": analytics_latest / "latest_video_advanced_metrics.csv",
        "channel_advanced": analytics_latest / "latest_channel_advanced_metrics.csv",
        "title_metrics": analytics_latest / "latest_title_metrics.csv",
    }
    for key, path in file_map.items():
        if not path.exists():
            warnings.append(f"Missing analytics feature file: {path}")
            continue
        rows = _read_csv(path)
        if key == "video_scores":
            latest_video_scores = {row.get("video_id", ""): row for row in rows if row.get("video_id")}
        elif key == "video_advanced":
            latest_video_advanced = {row.get("video_id", ""): row for row in rows if row.get("video_id")}
        elif key == "channel_advanced":
            latest_channel_advanced = {row.get("channel_id", ""): row for row in rows if row.get("channel_id")}
        elif key == "title_metrics":
            latest_title_metrics = {row.get("video_id", ""): row for row in rows if row.get("video_id")}

    by_video: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        video_id = str(obs.get("video_id", ""))
        if not video_id:
            continue
        by_video.setdefault(video_id, []).append(obs)

    output_rows: list[dict[str, Any]] = []
    excluded_no_future = 0

    for video_id, rows in by_video.items():
        rows = sorted(rows, key=lambda item: item["execution_date"])
        for idx, current in enumerate(rows):
            t = current["execution_date"]
            target_date = t + timedelta(days=target_horizon_days)
            future_candidates = []
            for candidate in rows[idx + 1 :]:
                gap_days = abs((candidate["execution_date"] - target_date).total_seconds()) / 86400.0
                if gap_days <= tolerance_days:
                    future_candidates.append((gap_days, candidate))
            if not future_candidates:
                excluded_no_future += 1
                continue
            future = sorted(future_candidates, key=lambda item: item[0])[0][1]

            score_row = latest_video_scores.get(video_id, {})
            adv_row = latest_video_advanced.get(video_id, {})
            channel_row = latest_channel_advanced.get(str(current.get("channel_id", "")), {})
            title_row = latest_title_metrics.get(video_id, {})

            future_views_delta = _safe_float(future.get("views_delta"))
            if future_views_delta is None:
                excluded_no_future += 1
                continue

            row: dict[str, Any] = {
                "execution_date": t.isoformat(),
                "target_date": future["execution_date"].isoformat(),
                "video_id": video_id,
                "channel_id": current.get("channel_id", ""),
                "channel_name": current.get("channel_name", ""),
                "title": current.get("title", ""),
                "source_export_path": current.get("source_export_path", ""),
                "views_delta": current.get("views_delta"),
                "engagement_rate": current.get("engagement_rate"),
                "comment_rate": current.get("comment_rate"),
                "video_age_days": current.get("video_age_days"),
                "duration_bucket": current.get("duration_bucket", ""),
                "is_short": current.get("is_short", "False"),
                "alpha_score": _safe_float(score_row.get("alpha_score")),
                "opportunity_score": _safe_float(score_row.get("opportunity_score")),
                "trend_burst_score": _safe_float(adv_row.get("trend_burst_score")),
                "evergreen_score": _safe_float(adv_row.get("evergreen_score")),
                "packaging_problem_score": _safe_float(adv_row.get("packaging_problem_score")),
                "metric_confidence_score": _safe_float(adv_row.get("metric_confidence_score")),
                "channel_momentum_score": _safe_float(channel_row.get("channel_momentum_score")),
                "channel_relative_success_score": _safe_float(adv_row.get("channel_relative_success_score")),
                "title_length_chars": _safe_float(title_row.get("title_length_chars")),
                "has_number": _to_bool_str(title_row.get("has_number")),
                "has_question": _to_bool_str(title_row.get("has_question")),
                "has_ai_word": _to_bool_str(title_row.get("has_ai_word")),
                "has_finance_word": _to_bool_str(title_row.get("has_finance_word")),
                "metadata_changed": current.get("metadata_changed", "False"),
                "future_views_delta_7d": future_views_delta,
                "future_log_views_delta_7d": round(math.log1p(max(future_views_delta, 0.0)), 6),
            }
            output_rows.append(row)

    if not output_rows:
        warnings.append("No trainable examples could be created with the current horizon and tolerance.")

    future_values = [float(row["future_views_delta_7d"]) for row in output_rows]
    threshold_top = _percentile_threshold(future_values, 0.8)

    by_channel_target: dict[tuple[str, str], list[float]] = {}
    for row in output_rows:
        key = (str(row.get("channel_id", "")), str(row.get("target_date", ""))[:10])
        by_channel_target.setdefault(key, []).append(float(row["future_views_delta_7d"]))

    for row in output_rows:
        fv = float(row["future_views_delta_7d"])
        row["is_top_growth_7d"] = "True" if fv >= threshold_top else "False"
        key = (str(row.get("channel_id", "")), str(row.get("target_date", ""))[:10])
        channel_values = by_channel_target.get(key, [])
        channel_median = sorted(channel_values)[len(channel_values) // 2] if channel_values else fv
        row["outperforms_channel_7d"] = "True" if fv > channel_median else "False"

    all_columns = [
        "execution_date",
        "target_date",
        "video_id",
        "channel_id",
        "channel_name",
        "title",
        "source_export_path",
        *FEATURE_COLUMNS,
        *TARGET_COLUMNS,
    ]

    leakage_found = []
    allowed_features = []
    for feature in FEATURE_COLUMNS:
        if feature.startswith("future_") or feature in LEAKAGE_BANNED_EXACT:
            leakage_found.append(feature)
        else:
            allowed_features.append(feature)

    leakage_audit = {
        "status": "pass" if not leakage_found else "fail",
        "banned_rules": {
            "prefixes": ["future_"],
            "targets": TARGET_COLUMNS,
        },
        "leakage_found": leakage_found,
        "allowed_features": allowed_features,
        "excluded_feature_candidates": sorted(set(FEATURE_COLUMNS) - set(allowed_features)),
    }

    trainable_examples = len(output_rows)
    unique_videos = len({row.get("video_id", "") for row in output_rows})
    unique_channels = len({row.get("channel_id", "") for row in output_rows})
    dates = [row.get("execution_date", "") for row in output_rows]
    date_range = {"min": min(dates) if dates else "", "max": max(dates) if dates else ""}

    if trainable_examples < 300:
        recommended_status = "not_ready"
    elif trainable_examples < 1000:
        recommended_status = "exploratory_only"
    else:
        recommended_status = "ready_for_baseline"

    target_coverage = {
        target: {
            "non_null_rows": len([row for row in output_rows if row.get(target) not in (None, "")]),
            "coverage_ratio": round(
                len([row for row in output_rows if row.get(target) not in (None, "")]) / trainable_examples,
                6,
            )
            if trainable_examples
            else 0.0,
        }
        for target in TARGET_COLUMNS
    }

    feature_dictionary = {
        "features": [
            {"name": feature, "type": "numeric" if feature not in {"duration_bucket", "is_short", "has_number", "has_question", "has_ai_word", "has_finance_word", "metadata_changed"} else "categorical_or_bool", "description": f"Feature {feature} available at time t."}
            for feature in allowed_features
        ],
        "excluded_by_leakage_policy": leakage_audit["excluded_feature_candidates"],
    }

    target_dictionary = {
        "targets": [
            {"name": "future_views_delta_7d", "type": "regression", "description": "Observed views_delta near t+7d."},
            {"name": "future_log_views_delta_7d", "type": "regression", "description": "log1p of future_views_delta_7d."},
            {"name": "is_top_growth_7d", "type": "classification", "description": "True if future growth is in top quantile."},
            {"name": "outperforms_channel_7d", "type": "classification", "description": "True if video outperforms channel median at target date."},
        ],
        "horizon_days": target_horizon_days,
        "tolerance_days": tolerance_days,
    }

    readiness_report = {
        "total_examples": len(observations),
        "trainable_examples": trainable_examples,
        "excluded_no_future": excluded_no_future,
        "unique_videos": unique_videos,
        "unique_channels": unique_channels,
        "date_range": date_range,
        "target_coverage": target_coverage,
        "minimum_recommended_examples": 300,
        "recommended_status": recommended_status,
        "recommended_models": [
            "logistic regression",
            "ridge/lasso",
            "decision tree",
            "random forest later",
        ],
        "warnings": warnings,
    }

    outputs = {
        "supervised_examples": modeling_dir / "supervised_examples.csv",
        "feature_dictionary": modeling_dir / "feature_dictionary.json",
        "target_dictionary": modeling_dir / "target_dictionary.json",
        "leakage_audit": modeling_dir / "leakage_audit.json",
        "model_readiness_report": modeling_dir / "model_readiness_report.json",
        "latest_inference_examples": modeling_dir / "latest_inference_examples.csv",
    }

    latest_inference_rows, inference_warnings = _build_latest_inference_examples(data_root=data_root, allowed_features=allowed_features)
    warnings.extend(inference_warnings)
    inference_columns = ["video_id", "execution_date", "channel_id", "channel_name", "title", *allowed_features]

    _write_csv(outputs["supervised_examples"], all_columns, output_rows)
    _write_csv(outputs["latest_inference_examples"], inference_columns, latest_inference_rows)
    _write_json(outputs["feature_dictionary"], feature_dictionary)
    _write_json(outputs["target_dictionary"], target_dictionary)
    _write_json(outputs["leakage_audit"], leakage_audit)
    _write_json(outputs["model_readiness_report"], readiness_report)

    return {
        "status": "success_with_warnings" if warnings else "success",
        "modeling_dir": str(modeling_dir),
        "outputs": {key: _rel(path, data_root) for key, path in outputs.items()},
        "trainable_examples": trainable_examples,
        "recommended_status": recommended_status,
        "warnings": warnings,
    }
