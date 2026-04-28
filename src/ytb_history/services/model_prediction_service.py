"""Prediction service using model suite artifact."""

from __future__ import annotations

import csv
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import joblib
except Exception:  # pragma: no cover
    joblib = None


def _read_json(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        return 1.0 if text in {"true", "yes", "y"} else 0.0


def _resolve_model_choice(*, model_root: Path, target: str | None, model_id: str | None, registry_manifest: dict[str, Any]) -> tuple[Path | None, dict[str, Any], list[str]]:
    warnings: list[str] = []
    suite_manifest_path = model_root / "suite_manifest.json"
    suite_manifest = _read_json(suite_manifest_path) if suite_manifest_path.exists() else {}
    if not suite_manifest:
        return None, {}, ["suite_manifest_missing"]

    selected = None
    if model_id:
        for item in suite_manifest.get("models", []):
            if item.get("model_id") == model_id:
                selected = item
                break
    else:
        target_name = target or registry_manifest.get("prediction_target") or "is_top_growth_7d"
        champ = (suite_manifest.get("champions") or {}).get(target_name)
        if champ:
            for item in suite_manifest.get("models", []):
                if item.get("model_id") == champ.get("model_id"):
                    selected = item
                    break

    if selected is None:
        return None, suite_manifest, ["model_not_found_for_target_or_model_id"]

    return model_root / str(selected.get("path", "")), suite_manifest, warnings


def predict_with_model_artifact(
    *,
    model_dir: str | Path,
    data_dir: str | Path = "data",
    output_dir: str | Path = "data/predictions",
    target: str = "is_top_growth_7d",
    model_id: str | None = None,
    allow_historical_supervised_fallback: bool = False,
) -> dict[str, Any]:
    del allow_historical_supervised_fallback
    model_root = Path(model_dir)
    data_root = Path(data_dir)
    rows_path = data_root / "modeling" / "latest_inference_examples.csv"
    if not rows_path.exists():
        return {"status": "failed_no_inference_rows", "warnings": ["latest_inference_examples_missing_or_empty"], "prediction_rows": 0}
    rows = _read_csv(rows_path)
    if not rows:
        return {"status": "failed_no_inference_rows", "warnings": ["latest_inference_examples_missing_or_empty"], "prediction_rows": 0}

    registry_manifest = _read_json(data_root / "model_registry" / "latest_model_manifest.json") if (data_root / "model_registry" / "latest_model_manifest.json").exists() else {}
    resolved_model_dir, suite_manifest, warnings = _resolve_model_choice(model_root=model_root, target=target, model_id=model_id, registry_manifest=registry_manifest)
    if resolved_model_dir is None:
        return {"status": "failed_model_resolution", "warnings": warnings, "prediction_rows": 0}

    if joblib is not None:
        payload = joblib.load(resolved_model_dir / "model.joblib")
    else:
        with (resolved_model_dir / "model.joblib").open("rb") as handle:
            payload = pickle.load(handle)
    feature_list = payload.get("feature_list", [])
    features = [{feature: _safe_float(row.get(feature)) for feature in feature_list} for row in rows]
    matrix = [[feat.get(name, 0.0) for name in feature_list] for feat in features]

    model = payload["model"]
    task_type = payload.get("task_type", "classification")
    if task_type == "classification" and hasattr(model, "predict_proba"):
        proba = model.predict_proba(matrix)
        if hasattr(proba, "tolist"):
            proba = proba.tolist()
        scores = [float(row[1] if isinstance(row, list) and len(row) > 1 else row[0] if isinstance(row, list) and row else 0.0) for row in proba]
    else:
        raw_scores = model.predict(matrix)
        if hasattr(raw_scores, "tolist"):
            raw_scores = raw_scores.tolist()
        scores = [float(v) for v in raw_scores]

    ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
    rank_map = {idx: rank + 1 for rank, idx in enumerate(ranked)}

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    pred_path = output_root / "latest_predictions.csv"
    with pred_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["video_id", "execution_date", "target", "model_id", "model_family", "model_score", "prediction_rank"])
        writer.writeheader()
        for idx, row in enumerate(rows):
            writer.writerow(
                {
                    "video_id": row.get("video_id", f"row_{idx}"),
                    "execution_date": row.get("execution_date", ""),
                    "target": target,
                    "model_id": payload.get("model_id", ""),
                    "model_family": payload.get("model_family", ""),
                    "model_score": round(float(scores[idx]), 8),
                    "prediction_rank": rank_map[idx],
                }
            )

    summary = {
        "status": "success",
        "suite_id": suite_manifest.get("suite_id"),
        "artifact_name": registry_manifest.get("artifact_name"),
        "workflow_run_id": registry_manifest.get("workflow_run_id"),
        "model_id": payload.get("model_id"),
        "model_family": payload.get("model_family"),
        "target": target,
        "prediction_rows": len(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warnings": warnings,
    }
    _write_json(output_root / "prediction_summary.json", summary)
    return {"status": "success", "latest_predictions": str(pred_path), "prediction_rows": len(rows), "warnings": warnings}
