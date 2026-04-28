from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path

from ytb_history.services.model_prediction_service import predict_with_model_artifact


class DummyModel:
    def predict_proba(self, rows):
        output = []
        for idx, _row in enumerate(rows):
            score = min(0.95, 0.1 + 0.1 * idx)
            output.append([1.0 - score, score])
        return output


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_predict_with_model_artifact_fails_when_model_missing(tmp_path: Path) -> None:
    model_dir = tmp_path / "downloaded_model"
    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_list.json", {"features": ["views_delta"]})
    _write_json(model_dir / "preprocessing.json", {"schema_version": "v1"})
    _write_json(model_dir / "training_manifest.json", {"model_id": "m1"})

    result = predict_with_model_artifact(model_dir=model_dir, data_dir=tmp_path / "data", output_dir=tmp_path / "out")

    assert result["status"] == "failed_missing_model_files"
    assert any("model.joblib" in warning for warning in result["warnings"])


def test_predict_with_model_artifact_generates_predictions_with_dummy_model(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    model_dir = tmp_path / "downloaded_model"
    output_dir = tmp_path / "predictions"

    _write_csv(
        data_dir / "modeling" / "latest_inference_examples.csv",
        ["video_id", "execution_date", "views_delta", "alpha_score"],
        [
            {"video_id": "v1", "execution_date": "2026-04-01T00:00:00+00:00", "views_delta": 10, "alpha_score": 55},
            {"video_id": "v2", "execution_date": "2026-04-02T00:00:00+00:00", "views_delta": 20, "alpha_score": 60},
        ],
    )
    _write_json(
        data_dir / "model_registry" / "latest_model_manifest.json",
        {"artifact_name": "artifact-123", "workflow_run_id": "999", "model_id": "model-xyz"},
    )

    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_list.json", {"features": ["views_delta", "alpha_score"]})
    _write_json(model_dir / "preprocessing.json", {"schema_version": "baseline_preprocessing_v1"})
    _write_json(model_dir / "training_manifest.json", {"model_id": "model-xyz"})
    with (model_dir / "model.joblib").open("wb") as handle:
        pickle.dump({"model": DummyModel(), "vectorizer": None}, handle)

    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=output_dir)

    assert result["status"] == "success"
    predictions_path = output_dir / "latest_predictions.csv"
    summary_path = output_dir / "prediction_summary.json"
    assert predictions_path.exists()
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["model_id"] == "model-xyz"
    assert summary["artifact_name"] == "artifact-123"
    assert summary["workflow_run_id"] == "999"
    assert summary["prediction_rows"] == 2


def test_predict_with_model_artifact_validates_feature_list(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    model_dir = tmp_path / "downloaded_model"
    output_dir = tmp_path / "predictions"

    _write_csv(
        data_dir / "modeling" / "latest_inference_examples.csv",
        ["video_id", "execution_date", "views_delta"],
        [{"video_id": "v1", "execution_date": "2026-04-01T00:00:00+00:00", "views_delta": 10}],
    )
    _write_json(data_dir / "model_registry" / "latest_model_manifest.json", {"artifact_name": "artifact-123", "workflow_run_id": "999", "model_id": "model-xyz"})

    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_list.json", {"features": ["views_delta", "missing_feature"]})
    _write_json(model_dir / "preprocessing.json", {"schema_version": "baseline_preprocessing_v1"})
    _write_json(model_dir / "training_manifest.json", {"model_id": "model-xyz"})
    with (model_dir / "model.joblib").open("wb") as handle:
        pickle.dump({"model": DummyModel(), "vectorizer": None}, handle)

    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=output_dir)

    assert result["status"] == "success"
    summary = json.loads((output_dir / "prediction_summary.json").read_text(encoding="utf-8"))
    assert any("missing_features_filled_with_zero" in warning for warning in summary["warnings"])


def test_predict_with_model_artifact_does_not_use_supervised_by_default(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    model_dir = tmp_path / "downloaded_model"

    _write_csv(
        data_dir / "modeling" / "supervised_examples.csv",
        ["video_id", "execution_date", "views_delta"],
        [{"video_id": "v1", "execution_date": "2026-04-01T00:00:00+00:00", "views_delta": 10}],
    )
    _write_json(data_dir / "model_registry" / "latest_model_manifest.json", {"artifact_name": "artifact-123", "workflow_run_id": "999", "model_id": "model-xyz"})

    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_list.json", {"features": ["views_delta"]})
    _write_json(model_dir / "preprocessing.json", {"schema_version": "baseline_preprocessing_v1"})
    _write_json(model_dir / "training_manifest.json", {"model_id": "model-xyz"})
    with (model_dir / "model.joblib").open("wb") as handle:
        pickle.dump({"model": DummyModel(), "vectorizer": None}, handle)

    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=tmp_path / "out")

    assert result["status"] == "failed_no_inference_rows"


def test_predict_with_model_artifact_fails_on_hash_mismatch(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    model_dir = tmp_path / "downloaded_model"
    output_dir = tmp_path / "predictions"

    _write_csv(
        data_dir / "modeling" / "latest_inference_examples.csv",
        ["video_id", "execution_date", "views_delta"],
        [{"video_id": "v1", "execution_date": "2026-04-01T00:00:00+00:00", "views_delta": 10}],
    )
    _write_json(
        data_dir / "model_registry" / "latest_model_manifest.json",
        {
            "artifact_name": "artifact-123",
            "workflow_run_id": "999",
            "model_id": "model-xyz",
            "feature_list_sha256": "bad",
            "training_manifest_sha256": "bad",
        },
    )

    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_list.json", {"features": ["views_delta"]})
    _write_json(model_dir / "preprocessing.json", {"schema_version": "baseline_preprocessing_v1"})
    _write_json(model_dir / "training_manifest.json", {"model_id": "model-xyz"})
    with (model_dir / "model.joblib").open("wb") as handle:
        pickle.dump({"model": DummyModel(), "vectorizer": None}, handle)

    result = predict_with_model_artifact(model_dir=model_dir, data_dir=data_dir, output_dir=output_dir)

    assert result["status"] == "failed_artifact_contract_mismatch"
