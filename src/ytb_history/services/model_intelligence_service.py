"""Build lightweight model intelligence artifacts from local prediction/decision outputs."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.domain.content_format import CONTENT_FORMATS

OUTPUT_COLUMNS = [
    "content_format",
    "video_id",
    "hybrid_decision_score",
    "model_score_percentile",
    "model_score",
    "prediction_rank",
    "decision_score",
    "confidence_level",
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _percentiles(rows: list[dict[str, str]]) -> dict[str, float]:
    if not rows:
        return {}
    sorted_rows = sorted(rows, key=lambda row: _safe_float(row.get("model_score")), reverse=True)
    n = max(1, len(sorted_rows))
    result: dict[str, float] = {}
    for idx, row in enumerate(sorted_rows):
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue
        pct = 100.0 if n == 1 else 100.0 * (1.0 - (idx / (n - 1)))
        result[video_id] = round(_clamp(pct), 4)
    return result


def build_model_intelligence(*, data_dir: str | Path = "data", content_format: str = "all") -> dict[str, Any]:
    """Build model-intelligence files without external API calls."""
    normalized_format = "all" if content_format in {"", "all", None} else str(content_format)
    if normalized_format not in {"all", *CONTENT_FORMATS}:
        raise ValueError("content_format must be one of: all, shorts, videos")
    if normalized_format == "all":
        data_root = Path(data_dir)
        has_format_predictions = any(
            (data_root / "predictions" / "formats" / fmt / "latest_predictions.csv").exists()
            for fmt in CONTENT_FORMATS
        )
        if not has_format_predictions:
            # Legacy root-only predictions/decisions: compute one root view. Once
            # per-format predictions exist, the branch below keeps rankings split.
            prediction_path = data_root / "predictions" / "latest_predictions.csv"
            decision_path = data_root / "decision" / "latest_action_candidates.csv"
            output_dir = data_root / "model_intelligence"
            return _build_single_model_intelligence(
                content_format="all",
                prediction_path=prediction_path,
                decision_path=decision_path,
                output_dir=output_dir,
            )
        format_results = {fmt: build_model_intelligence(data_dir=data_root, content_format=fmt) for fmt in CONTENT_FORMATS}
        output_dir = data_root / "model_intelligence"
        output_dir.mkdir(parents=True, exist_ok=True)
        combined_rows: list[dict[str, str]] = []
        for fmt in CONTENT_FORMATS:
            path = data_root / "model_intelligence" / "formats" / fmt / "latest_hybrid_recommendations.csv"
            if path.exists():
                combined_rows.extend(_read_csv(path))
        output_csv = output_dir / "latest_hybrid_recommendations.csv"
        output_json = output_dir / "model_intelligence_summary.json"
        _write_csv(output_csv, OUTPUT_COLUMNS, combined_rows)
        warnings = [warning for result in format_results.values() for warning in result.get("warnings", [])]
        summary = {
            "status": "success",
            "content_format": "all",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "format_results": format_results,
            "hybrid_rows": len(combined_rows),
            "warnings": warnings,
            "files_written": [str(output_csv), str(output_json)],
        }
        _write_json(output_json, summary)
        return summary

    data_root = Path(data_dir)
    prediction_path = data_root / "predictions" / "formats" / normalized_format / "latest_predictions.csv"
    decision_path = data_root / "decision" / "latest_action_candidates.csv"
    output_dir = data_root / "model_intelligence" / "formats" / normalized_format
    return _build_single_model_intelligence(
        content_format=normalized_format,
        prediction_path=prediction_path,
        decision_path=decision_path,
        output_dir=output_dir,
    )


def _build_single_model_intelligence(
    *,
    content_format: str,
    prediction_path: Path,
    decision_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    warnings: list[str] = []
    predictions: list[dict[str, str]] = []
    decisions: list[dict[str, str]] = []

    if prediction_path.exists():
        predictions = [
            row for row in _read_csv(prediction_path)
            if content_format == "all" or str(row.get("content_format", "")).strip().lower() == content_format
        ]
    else:
        warnings.append(f"Predictions file not found: {prediction_path}")

    if decision_path.exists():
        decisions = [
            row for row in _read_csv(decision_path)
            if content_format == "all" or str(row.get("content_format", "")).strip().lower() == content_format
        ]
    else:
        warnings.append(f"Decision file not found: {decision_path}")

    pred_by_video = {row.get("video_id", ""): row for row in predictions if row.get("video_id")}
    best_decision_by_video: dict[str, dict[str, str]] = {}
    for row in decisions:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue
        current = best_decision_by_video.get(video_id)
        if current is None or _safe_float(row.get("decision_score")) > _safe_float(current.get("decision_score")):
            best_decision_by_video[video_id] = row

    percentile_by_video = _percentiles(predictions)
    all_video_ids = sorted(set(pred_by_video) | set(best_decision_by_video))

    hybrid_rows: list[dict[str, Any]] = []
    for video_id in all_video_ids:
        pred = pred_by_video.get(video_id, {})
        dec = best_decision_by_video.get(video_id, {})
        model_score = _safe_float(pred.get("model_score"))
        decision_score = _safe_float(dec.get("decision_score"))
        model_pct = _safe_float(pred.get("model_score_percentile")) or percentile_by_video.get(video_id, 0.0)
        hybrid = round(_clamp(0.6 * model_pct + 0.4 * decision_score), 4)

        hybrid_rows.append(
            {
                "video_id": video_id,
                "content_format": content_format,
                "hybrid_decision_score": hybrid,
                "model_score_percentile": round(_clamp(model_pct), 4),
                "model_score": round(model_score, 8),
                "prediction_rank": pred.get("prediction_rank", ""),
                "decision_score": round(decision_score, 4),
                "confidence_level": dec.get("confidence_level", ""),
            }
        )

    hybrid_rows.sort(key=lambda row: (_safe_float(row.get("hybrid_decision_score")), str(row.get("video_id", ""))), reverse=True)

    output_csv = output_dir / "latest_hybrid_recommendations.csv"
    output_json = output_dir / "model_intelligence_summary.json"

    _write_csv(output_csv, OUTPUT_COLUMNS, hybrid_rows)

    summary: dict[str, Any] = {
        "status": "success",
        "content_format": content_format,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prediction_rows": len(predictions),
        "decision_rows": len(decisions),
        "hybrid_rows": len(hybrid_rows),
        "warnings": warnings,
        "files_written": [str(output_csv), str(output_json)],
    }
    _write_json(output_json, summary)
    return summary
