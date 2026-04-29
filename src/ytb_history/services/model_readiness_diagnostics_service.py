"""Model readiness diagnostics from local modeling artifacts."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.services.model_artifact_registry_service import _DEFAULT_MODELING_CONFIG


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else None


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _build_target_diagnostics(rows: list[dict[str, str]], targets: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for target_name, meta in targets.items():
        available = len(rows)
        values = [r.get(target_name, "") for r in rows]
        non_null = [v for v in values if str(v).strip() != ""]
        uniq = sorted({str(v).strip() for v in non_null})
        target_type = str(meta.get("target_type") or "unknown")
        blocker = ""
        status = "ready"
        if available == 0:
            blocker, status = "no_supervised_examples", "not_ready"
        elif not non_null:
            blocker, status = "insufficient_target_coverage", "insufficient"
        elif target_type == "classification" and len(uniq) <= 1:
            blocker, status = "single_class_target", "not_ready"
        elif len(non_null) < max(2, int(available * 0.2)):
            blocker, status = "insufficient_target_coverage", "insufficient"

        positive_rate: float | None = None
        if target_type == "classification" and non_null:
            positives = sum(1 for v in non_null if str(v).strip().lower() in {"1", "true", "yes"})
            positive_rate = positives / len(non_null)

        diagnostics.append(
            {
                "target_name": target_name,
                "target_type": target_type,
                "available_rows": available,
                "non_null_rows": len(non_null),
                "coverage_pct": round((len(non_null) / available) * 100.0, 2) if available else 0.0,
                "positive_class_rate": round(positive_rate, 6) if positive_rate is not None else None,
                "unique_values": len(uniq),
                "trainable_rows": len(non_null),
                "blocker": blocker,
                "status": status,
            }
        )
    return diagnostics


def analyze_model_readiness(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    modeling_dir = data_root / "modeling"
    registry_dir = data_root / "model_registry"
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    model_readiness = _read_json(modeling_dir / "model_readiness_report.json")
    feature_dict = _read_json(modeling_dir / "feature_dictionary.json")
    target_dict = _read_json(modeling_dir / "target_dictionary.json") or {}
    latest_manifest = _read_json(registry_dir / "latest_model_manifest.json")
    supervised_rows = _read_csv(modeling_dir / "supervised_examples.csv")
    inference_rows = _read_csv(modeling_dir / "latest_inference_examples.csv")

    min_exp = int(_DEFAULT_MODELING_CONFIG["min_trainable_examples_exploratory"])
    min_base = int(_DEFAULT_MODELING_CONFIG["min_trainable_examples_baseline"])
    trainable_examples = int(model_readiness.get("trainable_examples", 0)) if model_readiness else 0
    recommended_status = str((model_readiness or {}).get("recommended_status") or "unknown")

    blockers: list[str] = []
    warnings: list[str] = []
    if model_readiness is None:
        blockers.append("missing_model_readiness_report")
    if feature_dict is None:
        blockers.append("missing_feature_dictionary")
    if target_dict == {}:
        blockers.append("missing_target_dictionary")
    if not supervised_rows:
        blockers.append("no_supervised_examples")
    if trainable_examples <= 0:
        blockers.append("no_trainable_examples")
    if trainable_examples < min_exp:
        blockers.append("insufficient_trainable_examples")
    if not inference_rows:
        blockers.append("missing_latest_inference_examples")
    if latest_manifest is None or latest_manifest.get("status") != "valid":
        blockers.append("model_artifact_not_available")

    unique_dates = {str(r.get("execution_date", "")).strip() for r in supervised_rows if str(r.get("execution_date", "")).strip()}
    if len(unique_dates) < 2:
        blockers.append("no_temporal_validation_split")

    if model_readiness and not bool(model_readiness.get("future_observations_7d_available", trainable_examples > 0)):
        blockers.append("no_future_observations_7d")

    targets = target_dict.get("targets", {}) if isinstance(target_dict.get("targets"), dict) else {}
    target_diagnostics = _build_target_diagnostics(supervised_rows, targets)
    for tdiag in target_diagnostics:
        if tdiag["blocker"] and tdiag["blocker"] not in blockers:
            blockers.append(str(tdiag["blocker"]))

    missing_exp = max(0, min_exp - trainable_examples)
    missing_base = max(0, min_base - trainable_examples)

    timeline_path = modeling_dir / "latest_model_readiness_timeline.csv"
    timeline_columns = [
        "generated_at", "total_examples", "trainable_examples", "inference_examples", "unique_videos", "unique_channels",
        "recommended_status", "can_train_now", "examples_missing_for_exploratory", "examples_missing_for_baseline",
    ]
    timeline_rows = _read_csv(timeline_path)
    new_tl = {
        "generated_at": now,
        "total_examples": len(supervised_rows),
        "trainable_examples": trainable_examples,
        "inference_examples": len(inference_rows),
        "unique_videos": len({r.get("video_id", "") for r in supervised_rows if r.get("video_id")}),
        "unique_channels": len({r.get("channel_id", "") for r in supervised_rows if r.get("channel_id")}),
        "recommended_status": recommended_status,
        "can_train_now": str(recommended_status in {"exploratory_only", "ready_for_baseline"}),
        "examples_missing_for_exploratory": missing_exp,
        "examples_missing_for_baseline": missing_base,
    }
    if all(r.get("generated_at") != now for r in timeline_rows):
        timeline_rows.append(new_tl)
    _write_csv(timeline_path, timeline_columns, timeline_rows)

    forecast: dict[str, Any]
    if len(timeline_rows) < 2:
        forecast = {"status": "insufficient_history"}
    else:
        first = timeline_rows[0]
        last = timeline_rows[-1]
        t0 = datetime.fromisoformat(str(first["generated_at"]))
        t1 = datetime.fromisoformat(str(last["generated_at"]))
        days = max((t1 - t0).total_seconds() / 86400.0, 1e-9)
        first_trainable = _safe_float(first.get("trainable_examples")) or 0.0
        last_trainable = _safe_float(last.get("trainable_examples")) or 0.0
        growth = (last_trainable - first_trainable) / days
        if growth <= 0:
            forecast = {"status": "no_positive_growth", "daily_trainable_growth": growth}
        else:
            forecast = {
                "status": "ok",
                "daily_trainable_growth": growth,
                "estimated_days_to_exploratory": round(missing_exp / growth, 2) if missing_exp > 0 else 0.0,
                "estimated_days_to_baseline": round(missing_base / growth, 2) if missing_base > 0 else 0.0,
            }

    actions_map = {
        "no_future_observations_7d": "Seguir ejecutando YouTube Monitor diariamente hasta acumular observaciones futuras de 7 días.",
        "insufficient_trainable_examples": "Esperar más snapshots o ampliar canales monitoreados.",
        "single_class_target": "Esperar mayor variedad de resultados antes de entrenar clasificación.",
        "no_temporal_validation_split": "Acumular más fechas de ejecución para validación temporal.",
        "missing_latest_inference_examples": "Ejecutar build-model-dataset.",
    }
    next_steps = [actions_map[b] for b in blockers if b in actions_map]

    diagnostics = {
        "generated_at": now,
        "status": "not_ready" if blockers else "ready",
        "recommended_status": recommended_status,
        "can_train_now": recommended_status in {"exploratory_only", "ready_for_baseline"} and not blockers,
        "can_predict_now": latest_manifest is not None and latest_manifest.get("status") == "valid",
        "total_examples": len(supervised_rows),
        "trainable_examples": trainable_examples,
        "inference_examples": len(inference_rows),
        "unique_videos": new_tl["unique_videos"],
        "unique_channels": new_tl["unique_channels"],
        "date_range": {"start": min(unique_dates) if unique_dates else None, "end": max(unique_dates) if unique_dates else None},
        "min_trainable_examples_exploratory": min_exp,
        "min_trainable_examples_baseline": min_base,
        "examples_missing_for_exploratory": missing_exp,
        "examples_missing_for_baseline": missing_base,
        "blockers": blockers,
        "target_diagnostics": target_diagnostics,
        "forecast": forecast,
        "recommended_next_steps": next_steps,
        "warnings": warnings,
    }

    coverage_columns = ["target_name", "target_type", "available_rows", "non_null_rows", "coverage_pct", "positive_class_rate", "unique_values", "trainable_rows", "blocker", "status"]
    _write_csv(modeling_dir / "latest_target_coverage_report.csv", coverage_columns, target_diagnostics)

    gap_report = {
        "current_trainable_examples": trainable_examples,
        "needed_for_exploratory": min_exp,
        "needed_for_baseline": min_base,
        "examples_missing_for_exploratory": missing_exp,
        "examples_missing_for_baseline": missing_base,
        "primary_blocker": blockers[0] if blockers else None,
        "secondary_blockers": blockers[1:] if len(blockers) > 1 else [],
        "recommended_actions": next_steps,
    }

    md = "\n".join([
        "# Model Readiness Report",
        "## Current Status",
        f"- status: {diagnostics['status']}",
        "## Why training is blocked",
        f"- blockers: {', '.join(blockers) if blockers else 'none'}",
        "## Target Coverage",
        f"- targets: {len(target_diagnostics)}",
        "## Gap to Exploratory Training",
        f"- missing: {missing_exp}",
        "## Gap to Baseline Training",
        f"- missing: {missing_base}",
        "## Forecast",
        f"- {forecast.get('status')}",
        "## Recommended Next Steps",
        *(f"- {s}" for s in next_steps),
    ]) + "\n"

    html = f"<html><body><pre>{md}</pre></body></html>\n"

    _write_json(modeling_dir / "latest_model_readiness_diagnostics.json", diagnostics)
    _write_json(modeling_dir / "latest_training_gap_report.json", gap_report)
    (modeling_dir / "latest_model_readiness_report.md").write_text(md, encoding="utf-8")
    (modeling_dir / "latest_model_readiness_report.html").write_text(html, encoding="utf-8")

    return {
        "status": diagnostics["status"],
        "can_train_now": diagnostics["can_train_now"],
        "recommended_status": recommended_status,
        "diagnostics_path": str(modeling_dir / "latest_model_readiness_diagnostics.json"),
        "gap_report_path": str(modeling_dir / "latest_training_gap_report.json"),
        "warnings": warnings,
    }
