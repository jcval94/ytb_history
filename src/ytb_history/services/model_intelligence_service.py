"""Build lightweight model intelligence artifacts from local prediction/decision outputs."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUTPUT_COLUMNS = [
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


def build_model_intelligence(*, data_dir: str | Path = "data") -> dict[str, Any]:
    """Build model-intelligence files without external API calls."""
    data_root = Path(data_dir)
    prediction_path = data_root / "predictions" / "latest_predictions.csv"
    decision_path = data_root / "decision" / "latest_action_candidates.csv"
    output_dir = data_root / "model_intelligence"

    warnings: list[str] = []
    predictions: list[dict[str, str]] = []
    decisions: list[dict[str, str]] = []

    if prediction_path.exists():
        predictions = _read_csv(prediction_path)
    else:
        warnings.append(f"Predictions file not found: {prediction_path}")

    if decision_path.exists():
        decisions = _read_csv(decision_path)
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prediction_rows": len(predictions),
        "decision_rows": len(decisions),
        "hybrid_rows": len(hybrid_rows),
        "warnings": warnings,
        "files_written": [str(output_csv), str(output_json)],
    }
    _write_json(output_json, summary)
    return summary
