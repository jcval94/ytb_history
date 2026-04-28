from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.model_intelligence_service import build_model_intelligence


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_model_intelligence_merges_predictions_and_decisions(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_csv(
        data_dir / "predictions" / "latest_predictions.csv",
        ["video_id", "model_score", "prediction_rank"],
        [
            {"video_id": "v1", "model_score": 0.95, "prediction_rank": 1},
            {"video_id": "v2", "model_score": 0.75, "prediction_rank": 2},
        ],
    )
    _write_csv(
        data_dir / "decision" / "latest_action_candidates.csv",
        ["video_id", "decision_score", "confidence_level"],
        [
            {"video_id": "v1", "decision_score": 80, "confidence_level": "high"},
            {"video_id": "v3", "decision_score": 70, "confidence_level": "medium"},
        ],
    )

    result = build_model_intelligence(data_dir=data_dir)

    assert result["status"] == "success"
    assert result["hybrid_rows"] == 3

    rows = list(csv.DictReader((data_dir / "model_intelligence" / "latest_hybrid_recommendations.csv").open("r", encoding="utf-8", newline="")))
    by_video = {row["video_id"]: row for row in rows}
    assert set(by_video) == {"v1", "v2", "v3"}
    assert float(by_video["v1"]["model_score_percentile"]) > float(by_video["v2"]["model_score_percentile"])



def test_build_model_intelligence_writes_outputs_even_without_inputs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"

    result = build_model_intelligence(data_dir=data_dir)

    assert result["status"] == "success"
    assert result["hybrid_rows"] == 0
    assert len(result["warnings"]) == 2
    assert (data_dir / "model_intelligence" / "latest_hybrid_recommendations.csv").exists()
    summary = json.loads((data_dir / "model_intelligence" / "model_intelligence_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "success"
