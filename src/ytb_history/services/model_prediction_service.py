"""Prediction service using model suite artifact."""

from __future__ import annotations

import csv
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.domain.content_format import CONTENT_FORMATS

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


def _resolve_model_choice(*, model_root: Path, target: str | None, model_id: str | None, registry_manifest: dict[str, Any], content_format: str = "all") -> tuple[Path | None, dict[str, Any], list[str]]:
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
        champions = suite_manifest.get("champions") or {}
        if content_format != "all" and isinstance(champions.get(content_format), dict):
            champ = champions.get(content_format, {}).get(target_name)
        else:
            champ = champions.get(target_name)
        if champ:
            for item in suite_manifest.get("models", []):
                if item.get("model_id") == champ.get("model_id") and (content_format == "all" or item.get("content_format", content_format) == content_format):
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
    content_format: str = "all",
) -> dict[str, Any]:
    del allow_historical_supervised_fallback
    normalized_format = "all" if content_format in {"", "all", None} else str(content_format)
    if normalized_format not in {"all", *CONTENT_FORMATS}:
        raise ValueError("content_format must be one of: all, shorts, videos")
    if normalized_format == "all":
        data_root = Path(data_dir)
        model_root = Path(model_dir)
        suite_manifest = _read_json(model_root / "suite_manifest.json") if (model_root / "suite_manifest.json").exists() else {}
        champions = suite_manifest.get("champions") if isinstance(suite_manifest.get("champions"), dict) else {}
        has_format_contract = any(
            (data_root / "modeling" / "formats" / fmt / "latest_inference_examples.csv").exists()
            or (model_root / "formats" / fmt).exists()
            or isinstance(champions.get(fmt), dict)
            for fmt in CONTENT_FORMATS
        )
        if not has_format_contract:
            # Legacy root-only artifact: keep the old contract working, but do not
            # use this path once per-format inputs/artifacts exist.
            pass
        else:
            format_results = {
                fmt: predict_with_model_artifact(
                    model_dir=model_dir,
                    data_dir=data_dir,
                    output_dir=Path(output_dir) / "formats" / fmt,
                    target=target,
                    model_id=model_id,
                    content_format=fmt,
                )
                for fmt in CONTENT_FORMATS
            }
            output_root = Path(output_dir)
            output_root.mkdir(parents=True, exist_ok=True)
            combined_rows: list[dict[str, str]] = []
            for fmt, result in format_results.items():
                latest_path = str(result.get("latest_predictions", "") or "")
                if not latest_path:
                    continue
                pred_path = Path(latest_path)
                if pred_path.exists() and pred_path.is_file():
                    for row in _read_csv(pred_path):
                        row["content_format"] = fmt
                        combined_rows.append(row)
            combined_path = output_root / "latest_predictions.csv"
            fields = ["content_format", "video_id", "execution_date", "target", "model_id", "model_family", "model_score", "prediction_rank"]
            with combined_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                for row in combined_rows:
                    writer.writerow({field: row.get(field, "") for field in fields})
            summary = {
                "status": "success" if combined_rows else "failed_no_predictions",
                "content_format": "all",
                "format_results": format_results,
                "prediction_rows": len(combined_rows),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "warnings": [warning for result in format_results.values() for warning in result.get("warnings", [])],
            }
            _write_json(output_root / "prediction_summary.json", summary)
            return {"status": summary["status"], "latest_predictions": str(combined_path), "prediction_rows": len(combined_rows), "warnings": summary["warnings"], "format_results": format_results}

    model_root = Path(model_dir)
    data_root = Path(data_dir)
    rows_path = data_root / "modeling" / "latest_inference_examples.csv" if normalized_format == "all" else data_root / "modeling" / "formats" / normalized_format / "latest_inference_examples.csv"
    if not rows_path.exists():
        return {"status": "failed_no_inference_rows", "warnings": ["latest_inference_examples_missing_or_empty"], "prediction_rows": 0}
    rows = _read_csv(rows_path)
    if not rows:
        return {"status": "failed_no_inference_rows", "warnings": ["latest_inference_examples_missing_or_empty"], "prediction_rows": 0}

    registry_manifest = _read_json(data_root / "model_registry" / "latest_model_manifest.json") if (data_root / "model_registry" / "latest_model_manifest.json").exists() else {}
    resolved_model_dir, suite_manifest, warnings = _resolve_model_choice(model_root=model_root, target=target, model_id=model_id, registry_manifest=registry_manifest, content_format=normalized_format)
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
        writer = csv.DictWriter(handle, fieldnames=["content_format", "video_id", "execution_date", "target", "model_id", "model_family", "model_score", "prediction_rank"])
        writer.writeheader()
        for idx, row in enumerate(rows):
            writer.writerow(
                {
                    "video_id": row.get("video_id", f"row_{idx}"),
                    "content_format": normalized_format,
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
        "content_format": normalized_format,
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
