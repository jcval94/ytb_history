from __future__ import annotations

import csv
import json
from pathlib import Path

import ytb_history.services.model_smoke_test_service as smoke_service


def test_smoke_test_generates_artifacts_and_predictions(tmp_path: Path) -> None:
    out = tmp_path / "build" / "model_smoke_test"
    result = smoke_service.smoke_test_model_training(output_dir=out, n_rows=220)
    if result["status"] == "skipped_missing_ml_dependencies":
        assert "missing_dependency" in " ".join(result["warnings"])
        return

    assert result["status"] == "success"
    assert result["models_trained"] >= 3
    assert (out / "model_artifact" / "suite_manifest.json").exists()
    assert (out / "model_artifact" / "model_leaderboard.csv").exists()
    assert (out / "model_artifact" / "feature_importance_global.csv").exists()
    assert (out / "predictions" / "latest_predictions.csv").exists()

    preds = list(csv.DictReader((out / "predictions" / "latest_predictions.csv").open("r", encoding="utf-8", newline="")))
    assert preds


def test_smoke_test_writes_only_output_dir_and_no_api_reference(tmp_path: Path) -> None:
    out = tmp_path / "build" / "model_smoke_test"
    smoke_service.smoke_test_model_training(output_dir=out, n_rows=120)
    assert not (tmp_path / "data").exists()
    source = Path("src/ytb_history/services/model_smoke_test_service.py").read_text(encoding="utf-8")
    assert "search.list" not in source
    assert "youtube_client" not in source


def test_smoke_test_missing_deps_is_controlled_skip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(smoke_service, "_HAS_SKLEARN", False)
    monkeypatch.setattr(smoke_service, "_HAS_JOBLIB", False)
    out = tmp_path / "build" / "model_smoke_test"
    result = smoke_service.smoke_test_model_training(output_dir=out, n_rows=80)
    assert result["status"] == "skipped_missing_ml_dependencies"
    report = json.loads((out / "smoke_test_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "skipped_missing_ml_dependencies"
