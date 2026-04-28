from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path

from ytb_history.services.model_prediction_service import predict_with_model_artifact
from ytb_history.services import model_training_service
from ytb_history.services.model_training_service import train_model_suite


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_suite(tmp_path: Path) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    artifact_dir = tmp_path / "downloaded_model"
    config = tmp_path / "config/modeling.yaml"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        """
model_suite:
  models: [linear_regularized, random_forest, shallow_tree]
  targets:
    - name: is_top_growth_7d
      task_type: classification
      horizon: 7d
      champion_metric: precision_at_10
    - name: future_log_views_delta_7d
      task_type: regression
      horizon: 7d
      champion_metric: spearman_corr
  validation:
    validation_fraction: 0.25
""",
        encoding="utf-8",
    )
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"recommended_status": "ready_for_baseline"})
    _write_json(data_dir / "modeling" / "feature_dictionary.json", {"features": [{"name": "views_delta"}, {"name": "alpha_score"}, {"name": "is_top_growth_7d"}]})
    train_rows = []
    for i in range(20):
        train_rows.append({"execution_date": f"2026-03-{i+1:02d}T00:00:00+00:00", "video_id": f"v{i}", "views_delta": i, "alpha_score": i + 2, "is_top_growth_7d": "True" if i > 10 else "False", "future_log_views_delta_7d": i * 0.1})
    _write_csv(data_dir / "modeling" / "supervised_examples.csv", ["execution_date", "video_id", "views_delta", "alpha_score", "is_top_growth_7d", "future_log_views_delta_7d"], train_rows)
    infer_rows = [{"video_id": "p1", "execution_date": "2026-04-01T00:00:00+00:00", "views_delta": 15, "alpha_score": 20}, {"video_id": "p2", "execution_date": "2026-04-02T00:00:00+00:00", "views_delta": 1, "alpha_score": 2}]
    _write_csv(data_dir / "modeling" / "latest_inference_examples.csv", ["video_id", "execution_date", "views_delta", "alpha_score"], infer_rows)

    result = train_model_suite(data_dir=data_dir, modeling_config_path=config, artifact_dir=artifact_dir)
    if result.get("status") == "failed_missing_ml_dependencies":
        model_path = artifact_dir / "models" / "linear_regularized"
        model_path.mkdir(parents=True, exist_ok=True)
        payload = {"model": _DummyBinaryModel(), "feature_list": ["views_delta", "alpha_score"], "task_type": "classification", "model_family": "linear_regularized", "model_id": "dummy-model-1"}
        with (model_path / "model.joblib").open("wb") as handle:
            pickle.dump(payload, handle)
        suite_manifest = {
            "suite_id": "suite-dummy-1",
            "champions": {"is_top_growth_7d": {"model_id": "dummy-model-1"}},
            "models": [{"model_id": "dummy-model-1", "path": "models/linear_regularized"}],
        }
        _write_json(artifact_dir / "suite_manifest.json", suite_manifest)
    else:
        suite_manifest = json.loads((artifact_dir / "suite_manifest.json").read_text(encoding="utf-8"))
    _write_json(data_dir / "model_registry" / "latest_model_manifest.json", {"artifact_name": "ytb-model-suite-1", "workflow_run_id": "1", "status": "valid", "suite_id": suite_manifest["suite_id"], "champions": suite_manifest["champions"]})
    return data_dir, artifact_dir


class _DummyBinaryModel:
    def predict_proba(self, rows):
        return [[0.3, 0.7] for _ in rows]


def test_predict_uses_target_champion_by_default(tmp_path: Path) -> None:
    data_dir, model_dir = _prepare_suite(tmp_path)
    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=tmp_path / "out", target="is_top_growth_7d")
    assert result["status"] == "success"
    rows = list(csv.DictReader((tmp_path / "out/latest_predictions.csv").open(encoding="utf-8")))
    assert rows
    assert "model_family" in rows[0]
    assert rows[0]["target"] == "is_top_growth_7d"


def test_predict_accepts_model_id(tmp_path: Path) -> None:
    data_dir, model_dir = _prepare_suite(tmp_path)
    suite_manifest = json.loads((model_dir / "suite_manifest.json").read_text(encoding="utf-8"))
    explicit_model_id = suite_manifest["models"][0]["model_id"]
    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=tmp_path / "out", target="is_top_growth_7d", model_id=explicit_model_id)
    assert result["status"] == "success"


def test_predict_fails_without_inference_data(tmp_path: Path) -> None:
    result = predict_with_model_artifact(model_dir=tmp_path / "missing", data_dir=tmp_path / "data", output_dir=tmp_path / "out")
    assert result["status"] == "failed_no_inference_rows"
