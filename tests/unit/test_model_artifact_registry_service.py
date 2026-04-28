from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.model_artifact_registry_service import build_model_artifact_registry_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_modeling_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "retraining_schedule: weekly",
                "artifact_retention_days: 30",
                "min_trainable_examples_exploratory: 300",
                "min_trainable_examples_baseline: 1000",
                "artifact_storage: github_actions_artifacts",
                "commit_model_binaries_to_git: false",
                "use_git_lfs: false",
                "champion_selection_metric: precision_at_10",
                "prediction_target: is_top_growth_7d",
                "allowed_targets:",
                "  - is_top_growth_7d",
                "  - future_log_views_delta_7d",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_model_artifact_registry_generates_initial_manifests(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["status"] == "success"
    latest = json.loads((data_dir / "model_registry" / "latest_model_manifest.json").read_text(encoding="utf-8"))
    assert latest["schema_version"] == "latest_model_manifest_v1"

    index_doc = json.loads((data_dir / "model_registry" / "training_runs_index.json").read_text(encoding="utf-8"))
    assert index_doc["schema_version"] == "training_runs_index_v1"

    contract = json.loads((data_dir / "model_registry" / "prediction_contract.json").read_text(encoding="utf-8"))
    assert contract["schema_version"] == "prediction_contract_v1"


def test_model_artifact_registry_config_contract_flags() -> None:
    content = Path("config/modeling.yaml").read_text(encoding="utf-8")
    assert "artifact_storage: github_actions_artifacts" in content
    assert "commit_model_binaries_to_git: false" in content
    assert "use_git_lfs: false" in content


def test_model_artifact_registry_can_train_false_when_not_ready(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "not_ready"})

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["can_train"] is False
    assert report["reason"] == "model_readiness_not_trainable"


def test_model_artifact_registry_can_train_false_when_readiness_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["can_train"] is False
    assert "model_readiness_report_missing" in report["warnings"]


def test_model_artifact_registry_can_train_false_when_readiness_unknown(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "unknown"})

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["can_train"] is False


def test_model_artifact_registry_can_train_true_for_exploratory_only(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "exploratory_only"})

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["can_train"] is True


def test_model_artifact_registry_can_train_true_for_ready_for_baseline(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "ready_for_baseline"})

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["can_train"] is True


def test_model_artifact_registry_can_predict_false_without_valid_artifact(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)

    report = build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)

    assert report["latest_model_available"] is False
    assert report["can_predict"] is False


def test_model_artifact_registry_no_api_no_search_list_and_writes_only_registry(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    _write_modeling_config(config_path)

    existing = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}
    build_model_artifact_registry_report(data_dir=data_dir, modeling_config_path=config_path)
    updated = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}
    created = updated - existing

    assert created
    assert all(str(path).startswith("model_registry/") for path in created)

    source = Path("src/ytb_history/services/model_artifact_registry_service.py").read_text(encoding="utf-8")
    assert "search.list" not in source
    assert "requests." not in source
    assert "youtube" not in source.lower()
