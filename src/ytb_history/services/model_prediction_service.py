"""Prediction service using a previously downloaded model artifact."""

from __future__ import annotations

import csv
import importlib
import hashlib
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    text = str(value).strip().lower()
    if text in {"true", "yes", "y"}:
        return 1.0
    if text in {"false", "no", "n"}:
        return 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _validate_model_dir(model_dir: Path) -> tuple[bool, list[str]]:
    required = ["model.joblib", "feature_list.json", "preprocessing.json", "training_manifest.json"]
    missing = [name for name in required if not (model_dir / name).exists()]
    return len(missing) == 0, missing


def _load_feature_rows(data_dir: Path) -> tuple[list[dict[str, str]], str]:
    inference_path = data_dir / "modeling" / "latest_inference_examples.csv"
    if inference_path.exists():
        return _read_csv(inference_path), "modeling/latest_inference_examples.csv"
    return [], ""


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_model_payload(model_joblib: Path) -> Any:
    has_joblib = importlib.util.find_spec("joblib") is not None
    if has_joblib:
        joblib = importlib.import_module("joblib")
        return joblib.load(model_joblib)
    with model_joblib.open("rb") as handle:
        return pickle.load(handle)


def _extract_scores(prediction: Any) -> list[float]:
    if hasattr(prediction, "tolist"):
        prediction = prediction.tolist()
    if not isinstance(prediction, list):
        return []
    scores: list[float] = []
    for row in prediction:
        if isinstance(row, (int, float)):
            scores.append(float(row))
        elif isinstance(row, list) and row:
            if len(row) == 1:
                scores.append(float(row[0]))
            else:
                scores.append(float(row[1]))
    return scores


def predict_with_model_artifact(
    *,
    model_dir: str | Path,
    data_dir: str | Path = "data",
    output_dir: str | Path = "data/predictions",
    allow_historical_supervised_fallback: bool = False,
) -> dict[str, Any]:
    """Generate predictions using a local model artifact directory."""
    local_model_dir = Path(model_dir)
    is_valid, missing = _validate_model_dir(local_model_dir)
    if not is_valid:
        return {
            "status": "failed_missing_model_files",
            "warnings": [f"missing:{name}" for name in missing],
            "prediction_rows": 0,
        }

    feature_doc = _read_json(local_model_dir / "feature_list.json")
    features = feature_doc.get("features", [])
    if not isinstance(features, list):
        features = []
    feature_list = [str(name) for name in features]

    training_manifest = _read_json(local_model_dir / "training_manifest.json")

    data_root = Path(data_dir)
    rows, source_name = _load_feature_rows(data_root)
    warnings: list[str] = []
    if not rows and allow_historical_supervised_fallback:
        supervised_fallback = data_root / "modeling" / "supervised_examples.csv"
        if supervised_fallback.exists():
            rows = _read_csv(supervised_fallback)
            source_name = "modeling/supervised_examples.csv"
            warnings.append("using_historical_supervised_fallback")

    if not rows:
        return {
            "status": "failed_no_inference_rows",
            "warnings": ["latest_inference_examples_missing_or_empty"],
            "prediction_rows": 0,
        }

    registry_manifest = _read_json(data_root / "model_registry" / "latest_model_manifest.json") if (data_root / "model_registry" / "latest_model_manifest.json").exists() else {}
    artifact_model_id = str(training_manifest.get("model_id") or "")
    registry_model_id = str(registry_manifest.get("model_id") or "")
    if registry_model_id and artifact_model_id != registry_model_id:
        return {
            "status": "failed_artifact_contract_mismatch",
            "warnings": [f"model_id_mismatch:artifact={artifact_model_id}:registry={registry_model_id}"],
            "prediction_rows": 0,
        }
    if registry_manifest and (not registry_manifest.get("artifact_name") or not registry_manifest.get("workflow_run_id")):
        return {
            "status": "failed_artifact_contract_mismatch",
            "warnings": ["registry_missing_artifact_name_or_workflow_run_id"],
            "prediction_rows": 0,
        }

    feature_hash = _sha256_file(local_model_dir / "feature_list.json")
    manifest_hash = _sha256_file(local_model_dir / "training_manifest.json")
    registry_feature_hash = registry_manifest.get("feature_list_sha256")
    registry_manifest_hash = registry_manifest.get("training_manifest_sha256")
    if registry_feature_hash and registry_feature_hash != feature_hash:
        return {
            "status": "failed_artifact_contract_mismatch",
            "warnings": ["feature_list_sha256_mismatch"],
            "prediction_rows": 0,
        }
    if registry_manifest_hash and registry_manifest_hash != manifest_hash:
        return {
            "status": "failed_artifact_contract_mismatch",
            "warnings": ["training_manifest_sha256_mismatch"],
            "prediction_rows": 0,
        }
    if registry_manifest and not registry_feature_hash:
        warnings.append("registry_missing_feature_list_sha256")
    if registry_manifest and not registry_manifest_hash:
        warnings.append("registry_missing_training_manifest_sha256")

    missing_features = sorted({feature for feature in feature_list if feature not in rows[0]})

    model_payload = _load_model_payload(local_model_dir / "model.joblib")
    model = model_payload.get("model") if isinstance(model_payload, dict) else model_payload
    vectorizer = model_payload.get("vectorizer") if isinstance(model_payload, dict) else None

    feature_rows = [{feature: _safe_float(row.get(feature)) for feature in feature_list} for row in rows]
    if vectorizer is not None and hasattr(vectorizer, "transform"):
        model_input = vectorizer.transform(feature_rows)
    else:
        model_input = [[item.get(feature, 0.0) for feature in feature_list] for item in feature_rows]

    if not hasattr(model, "predict_proba"):
        return {
            "status": "failed_invalid_model",
            "warnings": ["model_missing_predict_proba"],
            "prediction_rows": 0,
        }

    raw_scores = model.predict_proba(model_input)
    scores = _extract_scores(raw_scores)
    if len(scores) != len(rows):
        return {
            "status": "failed_prediction_shape",
            "warnings": ["predictions_length_mismatch"],
            "prediction_rows": 0,
        }

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    predictions_path = output_root / "latest_predictions.csv"
    summary_path = output_root / "prediction_summary.json"

    with predictions_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["video_id", "execution_date", "model_score", "model_id"])
        writer.writeheader()
        for idx, row in enumerate(rows):
            writer.writerow(
                {
                    "video_id": row.get("video_id", f"row_{idx}"),
                    "execution_date": row.get("execution_date", ""),
                    "model_score": round(float(scores[idx]), 8),
                    "model_id": training_manifest.get("model_id", ""),
                }
            )

    if missing_features:
        warnings.append(f"missing_features_filled_with_zero:{','.join(missing_features)}")

    summary = {
        "status": "success",
        "model_id": training_manifest.get("model_id"),
        "artifact_name": registry_manifest.get("artifact_name"),
        "workflow_run_id": registry_manifest.get("workflow_run_id"),
        "prediction_rows": len(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warnings": warnings,
        "source_rows": source_name,
    }

    _write_json(summary_path, summary)

    return {
        "status": "success",
        "output_dir": str(output_root),
        "latest_predictions": str(predictions_path),
        "prediction_summary": str(summary_path),
        "prediction_rows": len(rows),
        "warnings": warnings,
    }
