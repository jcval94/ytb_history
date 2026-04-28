from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from ytb_history.services.model_training_service import register_trained_artifact, train_baseline_model


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
                "retraining_schedule: weekly",
                "artifact_retention_days: 30",
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

    _write_json(
        data_dir / "modeling" / "model_readiness_report.json",
        {"recommended_status": "not_ready" if not_ready else "ready_for_baseline"},
    )
    _write_json(
        data_dir / "modeling" / "feature_dictionary.json",
        {
            "features": [
                {"name": "views_delta"},
                {"name": "alpha_score"},
                {"name": "duration_bucket"},
                {"name": "metadata_changed"},
                {"name": "future_log_views_delta_7d"},
                {"name": "is_top_growth_7d"},
            ]
        },
    )

    fieldnames = ["execution_date", "video_id", "views_delta", "alpha_score", "duration_bucket", "metadata_changed", "is_top_growth_7d", "future_log_views_delta_7d"]
    rows: list[dict[str, object]] = []
    for idx in range(30):
        rows.append(
            {
                "execution_date": f"2026-03-{idx + 1:02d}T00:00:00+00:00",
                "video_id": f"v{idx}",
                "views_delta": idx * 5,
                "alpha_score": 10 + idx,
                "duration_bucket": "short" if idx % 2 == 0 else "long",
                "metadata_changed": "True" if idx % 3 == 0 else "False",
                "is_top_growth_7d": "True" if idx >= 15 else "False",
                "future_log_views_delta_7d": 0.1 * idx,
            }
        )

    _write_csv(data_dir / "modeling" / "supervised_examples.csv", fieldnames, rows)
    return data_dir, config_path, artifact_dir


def test_train_baseline_model_skips_when_not_ready(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path, not_ready=True)

    result = train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    assert result["status"] == "skipped_not_ready"
    assert not artifact_dir.exists()


def test_train_baseline_model_generates_artifact_files(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)

    result = train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    assert result["status"] == "success"
    assert (artifact_dir / "model.joblib").exists()
    assert (artifact_dir / "preprocessing.json").exists()
    assert (artifact_dir / "feature_list.json").exists()
    assert (artifact_dir / "metrics.json").exists()
    assert (artifact_dir / "training_manifest.json").exists()
    assert (artifact_dir / "validation_predictions.csv.gz").exists()
    assert (artifact_dir / "model_card.md").exists()


def test_train_baseline_model_feature_list_excludes_future_and_targets(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)

    train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    feature_list = json.loads((artifact_dir / "feature_list.json").read_text(encoding="utf-8"))["features"]
    assert "future_log_views_delta_7d" not in feature_list
    assert "is_top_growth_7d" not in feature_list


def test_train_baseline_model_uses_temporal_split(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)

    train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    manifest = json.loads((artifact_dir / "training_manifest.json").read_text(encoding="utf-8"))
    assert manifest["train_end_date"] < manifest["validation_start_date"]


def test_register_trained_artifact_updates_latest_manifest_and_ids(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)
    train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    _write_json(data_dir / "model_registry" / "latest_model_manifest.json", {"schema_version": "latest_model_manifest_v1", "status": "none"})
    _write_json(data_dir / "model_registry" / "training_runs_index.json", {"schema_version": "training_runs_index_v1", "runs": []})

    result = register_trained_artifact(
        artifact_name="ytb-model-is-top-growth-7d-123",
        workflow_run_id="123",
        artifact_dir=artifact_dir,
        data_dir=data_dir,
    )

    assert result["status"] == "success"
    latest = json.loads((data_dir / "model_registry" / "latest_model_manifest.json").read_text(encoding="utf-8"))
    assert latest["status"] == "valid"
    assert latest["artifact_name"] == "ytb-model-is-top-growth-7d-123"
    assert latest["workflow_run_id"] == "123"

    runs_index = json.loads((data_dir / "model_registry" / "training_runs_index.json").read_text(encoding="utf-8"))
    assert runs_index["runs"]
    assert runs_index["runs"][-1]["artifact_name"] == "ytb-model-is-top-growth-7d-123"


def test_train_baseline_model_writes_compressed_validation_predictions(tmp_path: Path) -> None:
    data_dir, config_path, artifact_dir = _prepare_modeling_inputs(tmp_path)

    train_baseline_model(data_dir=data_dir, modeling_config_path=config_path, artifact_dir=artifact_dir)

    with gzip.open(artifact_dir / "validation_predictions.csv.gz", "rt", encoding="utf-8") as handle:
        header = handle.readline().strip()

    assert "model_score" in header
