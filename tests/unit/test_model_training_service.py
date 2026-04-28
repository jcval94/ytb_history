from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services import model_training_service
from ytb_history.services.model_training_service import register_trained_artifact, train_model_suite


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_modeling_inputs(tmp_path: Path, *, not_ready: bool = False) -> tuple[Path, Path, Path]:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config" / "modeling.yaml"
    artifact_dir = tmp_path / "build" / "model_artifact"

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "artifact_retention_days: 30",
                "model_suite:",
                "  enabled: true",
                "  models:",
                "    - linear_regularized",
                "    - random_forest",
                "    - shallow_tree",
                "  targets:",
                "    - name: is_top_growth_7d",
                "      task_type: classification",
                "      horizon: 7d",
                "      champion_metric: precision_at_10",
                "    - name: future_log_views_delta_7d",
                "      task_type: regression",
                "      horizon: 7d",
                "      champion_metric: spearman_corr",
                "  validation:",
                "    split: temporal",
                "    validation_fraction: 0.25",
                "  random_state: 42",
                "  shallow_tree_max_depth: 4",
                "  random_forest_n_estimators: 20",
                "  random_forest_max_depth: 4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "not_ready" if not_ready else "ready_for_baseline"})
    _write_json(data_dir / "modeling" / "feature_dictionary.json", {"features": [{"name": "views_delta"}, {"name": "alpha_score"}, {"name": "decision_score"}, {"name": "duration_bucket"}, {"name": "future_log_views_delta_7d"}, {"name": "is_top_growth_7d"}]})

    fieldnames = ["execution_date", "video_id", "views_delta", "alpha_score", "decision_score", "duration_bucket", "is_top_growth_7d", "future_log_views_delta_7d"]
    rows: list[dict[str, object]] = []
    for idx in range(40):
        rows.append(
            {
                "execution_date": f"2026-03-{(idx % 28) + 1:02d}T00:00:00+00:00",
                "video_id": f"v{idx}",
                "views_delta": idx * 3,
                "alpha_score": 10 + idx,
                "decision_score": 20 + idx,
                "duration_bucket": 1 if idx % 2 == 0 else 0,
                "is_top_growth_7d": "True" if idx >= 20 else "False",
                "future_log_views_delta_7d": 0.2 * idx,
            }
        )
    _write_csv(data_dir / "modeling" / "supervised_examples.csv", fieldnames, rows)
    return data_dir, config_path, artifact_dir


def test_train_model_suite_skips_when_not_ready(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path, not_ready=True)
    result = train_model_suite(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)
    if not (model_training_service._HAS_SKLEARN and model_training_service._HAS_JOBLIB):
        assert result["status"] == "failed_missing_ml_dependencies"
    else:
        assert result["status"] == "skipped_not_ready"


def test_train_model_suite_fails_when_ml_dependencies_missing(tmp_path: Path, monkeypatch) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)
    monkeypatch.setattr(model_training_service, "_HAS_SKLEARN", False)
    monkeypatch.setattr(model_training_service, "_HAS_JOBLIB", False)
    result = train_model_suite(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)
    assert result["status"] == "failed_missing_ml_dependencies"
    assert not (artifact_dir / "model.joblib").exists()
    assert not (artifact_dir / "models").exists()


def test_train_model_suite_generates_expected_files(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)
    result = train_model_suite(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)
    if not (model_training_service._HAS_SKLEARN and model_training_service._HAS_JOBLIB):
        assert result["status"] == "failed_missing_ml_dependencies"
        assert not (artifact_dir / "models").exists()
        return
    assert result["status"] == "success"
    assert (artifact_dir / "suite_manifest.json").exists()
    assert (artifact_dir / "model_leaderboard.csv").exists()
    assert (artifact_dir / "feature_importance_global.csv").exists()
    assert (artifact_dir / "feature_direction_global.csv").exists()
    assert (artifact_dir / "models" / "linear_regularized" / "coefficients.csv").exists()
    assert (artifact_dir / "models" / "random_forest" / "permutation_importance.csv").exists()
    assert (artifact_dir / "models" / "random_forest" / "feature_direction.csv").exists()
    assert (artifact_dir / "models" / "shallow_tree" / "tree_rules.txt").exists()


def test_coefficients_contains_direction(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)
    result = train_model_suite(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)
    if not (model_training_service._HAS_SKLEARN and model_training_service._HAS_JOBLIB):
        assert result["status"] == "failed_missing_ml_dependencies"
        return
    rows = list(csv.DictReader((artifact_dir / "models" / "linear_regularized" / "coefficients.csv").open(encoding="utf-8")))
    assert rows
    assert {row["direction"] for row in rows}.issubset({"positive", "negative"})


def test_leaderboard_and_registry_champions(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)
    result = train_model_suite(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)
    if not (model_training_service._HAS_SKLEARN and model_training_service._HAS_JOBLIB):
        assert result["status"] == "failed_missing_ml_dependencies"
        register = register_trained_artifact(artifact_name="ytb-model-suite-123", workflow_run_id="123", artifact_dir=artifact_dir, data_dir=data_dir)
        assert register["status"] == "skipped_no_artifact"
        return
    register_trained_artifact(artifact_name="ytb-model-suite-123", workflow_run_id="123", artifact_dir=artifact_dir, data_dir=data_dir)

    leaderboard = list(csv.DictReader((data_dir / "model_reports" / "latest_model_leaderboard.csv").open(encoding="utf-8")))
    assert leaderboard
    assert any(row["selected_as_champion"] == "True" for row in leaderboard)

    manifest = json.loads((data_dir / "model_registry" / "latest_model_manifest.json").read_text(encoding="utf-8"))
    assert "champions" in manifest
    assert "is_top_growth_7d" in manifest["champions"]
    assert "future_log_views_delta_7d" in manifest["champions"]
