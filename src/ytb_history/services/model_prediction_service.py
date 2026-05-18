"""Prediction service using model suite artifact."""

from __future__ import annotations

import csv
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.domain.content_format import CONTENT_FORMATS

PREDICTION_COLUMNS = [
    "content_format",
    "video_id",
    "execution_date",
    "target",
    "model_id",
    "model_family",
    "model_score",
    "prediction_rank",
]

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


def _write_predictions_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PREDICTION_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in PREDICTION_COLUMNS})


def _write_empty_prediction_result(
    *,
    output_dir: Path,
    status: str,
    content_format: str,
    warnings: list[str],
    target: str,
    extra_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pred_path = output_dir / "latest_predictions.csv"
    _write_predictions_csv(pred_path, [])
    summary: dict[str, Any] = {
        "status": status,
        "content_format": content_format,
        "target": target,
        "prediction_rows": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warnings": warnings,
    }
    if extra_summary:
        summary.update(extra_summary)
    _write_json(output_dir / "prediction_summary.json", summary)
    return {"status": status, "latest_predictions": str(pred_path), "prediction_rows": 0, "warnings": warnings}


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        return 1.0 if text in {"true", "yes", "y"} else 0.0


def _is_present_feature_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _filter_eligible_inference_rows(
    rows: list[dict[str, str]],
    feature_list: list[str],
) -> tuple[list[dict[str, str]], int, dict[str, int]]:
    eligible: list[dict[str, str]] = []
    missing_by_feature: dict[str, int] = {}
    for row in rows:
        missing = [feature for feature in feature_list if not _is_present_feature_value(row.get(feature))]
        if missing:
            for feature in missing:
                missing_by_feature[feature] = missing_by_feature.get(feature, 0) + 1
            continue
        eligible.append(row)
    return eligible, len(rows) - len(eligible), missing_by_feature


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
            warnings: list[str] = []
            skipped_incomplete_rows = 0
            missing_feature_counts: dict[str, int] = {}
            for fmt, result in format_results.items():
                fmt_status = str(result.get("status", "unknown"))
                if fmt_status != "success":
                    warnings.append(f"{fmt}: {fmt_status}")
                skipped_incomplete_rows += int(result.get("skipped_incomplete_rows", 0) or 0)
                for feature, count in (result.get("missing_feature_counts", {}) or {}).items():
                    missing_feature_counts[str(feature)] = missing_feature_counts.get(str(feature), 0) + int(count)
                latest_path = str(result.get("latest_predictions", "") or "")
                if not latest_path:
                    continue
                pred_path = Path(latest_path)
                if pred_path.exists() and pred_path.is_file():
                    for row in _read_csv(pred_path):
                        row["content_format"] = fmt
                        combined_rows.append(row)
            combined_path = output_root / "latest_predictions.csv"
            _write_predictions_csv(combined_path, combined_rows)
            warnings.extend(warning for result in format_results.values() for warning in result.get("warnings", []))
            summary = {
                "status": "success" if combined_rows else "failed_no_predictions",
                "content_format": "all",
                "format_results": format_results,
                "prediction_rows": len(combined_rows),
                "skipped_incomplete_rows": skipped_incomplete_rows,
                "missing_feature_counts": missing_feature_counts,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "warnings": warnings,
            }
            _write_json(output_root / "prediction_summary.json", summary)
            return {
                "status": summary["status"],
                "latest_predictions": str(combined_path),
                "prediction_rows": len(combined_rows),
                "skipped_incomplete_rows": skipped_incomplete_rows,
                "missing_feature_counts": missing_feature_counts,
                "warnings": summary["warnings"],
                "format_results": format_results,
            }

    model_root = Path(model_dir)
    data_root = Path(data_dir)
    rows_path = data_root / "modeling" / "latest_inference_examples.csv" if normalized_format == "all" else data_root / "modeling" / "formats" / normalized_format / "latest_inference_examples.csv"
    if not rows_path.exists():
        return _write_empty_prediction_result(
            output_dir=Path(output_dir),
            status="failed_no_inference_rows",
            content_format=normalized_format,
            warnings=["latest_inference_examples_missing_or_empty"],
            target=target,
        )
    rows = _read_csv(rows_path)
    if not rows:
        return _write_empty_prediction_result(
            output_dir=Path(output_dir),
            status="failed_no_inference_rows",
            content_format=normalized_format,
            warnings=["latest_inference_examples_missing_or_empty"],
            target=target,
        )

    registry_manifest = _read_json(data_root / "model_registry" / "latest_model_manifest.json") if (data_root / "model_registry" / "latest_model_manifest.json").exists() else {}
    resolved_model_dir, suite_manifest, warnings = _resolve_model_choice(model_root=model_root, target=target, model_id=model_id, registry_manifest=registry_manifest, content_format=normalized_format)
    if resolved_model_dir is None:
        return _write_empty_prediction_result(
            output_dir=Path(output_dir),
            status="failed_model_resolution",
            content_format=normalized_format,
            warnings=warnings,
            target=target,
        )

    if joblib is not None:
        payload = joblib.load(resolved_model_dir / "model.joblib")
    else:
        with (resolved_model_dir / "model.joblib").open("rb") as handle:
            payload = pickle.load(handle)
    feature_list = payload.get("feature_list", [])
    rows, skipped_incomplete_rows, missing_by_feature = _filter_eligible_inference_rows(rows, feature_list)
    if skipped_incomplete_rows:
        warnings.append(f"skipped_incomplete_inference_rows:{skipped_incomplete_rows}")
    if not rows:
        return _write_empty_prediction_result(
            output_dir=Path(output_dir),
            status="failed_no_eligible_inference_rows",
            content_format=normalized_format,
            warnings=warnings,
            target=target,
            extra_summary={
                "input_rows": skipped_incomplete_rows,
                "skipped_incomplete_rows": skipped_incomplete_rows,
                "missing_feature_counts": missing_by_feature,
            },
        )
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
    pred_path = output_root / "latest_predictions.csv"
    prediction_rows = [
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
        for idx, row in enumerate(rows)
    ]
    _write_predictions_csv(pred_path, prediction_rows)

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
        "skipped_incomplete_rows": skipped_incomplete_rows,
        "missing_feature_counts": missing_by_feature,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warnings": warnings,
    }
    _write_json(output_root / "prediction_summary.json", summary)
    return {
        "status": "success",
        "latest_predictions": str(pred_path),
        "prediction_rows": len(rows),
        "skipped_incomplete_rows": skipped_incomplete_rows,
        "missing_feature_counts": missing_by_feature,
        "warnings": warnings,
    }
