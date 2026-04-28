"""Model artifact registry contracts and local readiness report."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

_INITIAL_LATEST_MODEL_MANIFEST: dict[str, Any] = {
    "schema_version": "latest_model_manifest_v1",
    "status": "none",
    "model_id": None,
    "artifact_name": None,
    "workflow_run_id": None,
    "target": None,
    "created_at": None,
    "expires_at_estimate": None,
    "metrics": {},
    "feature_list_sha256": None,
    "training_manifest_sha256": None,
    "warnings": [],
}

_INITIAL_TRAINING_RUNS_INDEX: dict[str, Any] = {
    "schema_version": "training_runs_index_v1",
    "runs": [],
}

_INITIAL_PREDICTION_CONTRACT: dict[str, Any] = {
    "schema_version": "prediction_contract_v1",
    "target": "is_top_growth_7d",
    "required_features": [],
    "optional_features": [],
    "model_artifact_expected_files": [
        "model.joblib",
        "preprocessing.json",
        "feature_list.json",
        "metrics.json",
        "training_manifest.json",
        "model_card.md",
    ],
}


_DEFAULT_MODELING_CONFIG: dict[str, Any] = {
    "retraining_schedule": "weekly",
    "artifact_retention_days": 30,
    "min_trainable_examples_exploratory": 300,
    "min_trainable_examples_baseline": 1000,
    "artifact_storage": "github_actions_artifacts",
    "commit_model_binaries_to_git": False,
    "use_git_lfs": False,
    "champion_selection_metric": "precision_at_10",
    "prediction_target": "is_top_growth_7d",
    "allowed_targets": ["is_top_growth_7d", "future_log_views_delta_7d"],
}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _ensure_json(path: Path, payload: dict[str, Any], written_files: list[str]) -> dict[str, Any]:
    current = _read_json(path)
    if current is not None:
        return current
    _write_json(path, payload)
    written_files.append(str(path))
    return dict(payload)


def _load_modeling_config(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = {}
    if path.exists():
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            loaded = raw
    resolved = dict(_DEFAULT_MODELING_CONFIG)
    resolved.update(loaded)
    return resolved


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def build_model_artifact_registry_report(
    *,
    data_dir: str | Path = "data",
    modeling_config_path: str | Path = "config/modeling.yaml",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a local report for artifact-based model registry readiness."""
    reference_now = now or datetime.now(timezone.utc)
    data_root = Path(data_dir)
    registry_dir = data_root / "model_registry"
    written_files: list[str] = []

    latest_manifest = _ensure_json(registry_dir / "latest_model_manifest.json", _INITIAL_LATEST_MODEL_MANIFEST, written_files)
    _ensure_json(registry_dir / "training_runs_index.json", _INITIAL_TRAINING_RUNS_INDEX, written_files)
    _ensure_json(registry_dir / "prediction_contract.json", _INITIAL_PREDICTION_CONTRACT, written_files)

    modeling_config = _load_modeling_config(Path(modeling_config_path))
    readiness = _read_json(data_root / "modeling" / "model_readiness_report.json")

    recommended_status = str((readiness or {}).get("recommended_status") or "unknown")
    can_train = recommended_status in {"exploratory_only", "ready_for_baseline"}

    latest_model_available = latest_manifest.get("status") == "valid"
    can_predict = latest_model_available
    latest_model_expired_estimate = False
    warnings: list[str] = []
    reason = "ok"

    if readiness is None:
        warnings.append("model_readiness_report_missing")

    if not can_train:
        reason = "model_readiness_not_trainable"

    if not latest_model_available:
        can_predict = False
        if reason == "ok":
            reason = "latest_model_not_valid"

    if not latest_manifest.get("artifact_name") or not latest_manifest.get("workflow_run_id"):
        can_predict = False
        if reason == "ok":
            reason = "latest_model_artifact_missing_metadata"

    expires_at = _parse_iso8601(latest_manifest.get("expires_at_estimate"))
    if expires_at is not None and expires_at < reference_now:
        latest_model_expired_estimate = True
        can_predict = False
        warnings.append("latest_model_expired_estimate")
        if reason == "ok":
            reason = "latest_model_expired_estimate"

    return {
        "status": "success",
        "registry_contract": "artifact_based",
        "config": modeling_config,
        "model_readiness_report_found": readiness is not None,
        "latest_model_manifest_found": (registry_dir / "latest_model_manifest.json").exists(),
        "can_train": can_train,
        "can_predict": can_predict,
        "latest_model_available": latest_model_available,
        "latest_model_expired_estimate": latest_model_expired_estimate,
        "reason": reason,
        "warnings": warnings,
        "written_files": written_files,
    }
