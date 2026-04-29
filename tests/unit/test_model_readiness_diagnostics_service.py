from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.model_readiness_diagnostics_service import analyze_model_readiness


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_common(data_dir: Path, *, trainable: int = 0, recommended_status: str = "not_ready", cls_value: str = "True") -> None:
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"trainable_examples": trainable, "recommended_status": recommended_status})
    _write_json(data_dir / "modeling" / "feature_dictionary.json", {"features": ["views_delta"]})
    _write_json(data_dir / "modeling" / "target_dictionary.json", {"targets": {"is_top_growth_7d": {"target_type": "classification"}}})
    _write_csv(data_dir / "modeling" / "latest_inference_examples.csv", ["video_id", "channel_id"], [{"video_id": "v1", "channel_id": "c1"}])
    _write_csv(
        data_dir / "modeling" / "supervised_examples.csv",
        ["video_id", "channel_id", "execution_date", "is_top_growth_7d"],
        [
            {"video_id": "v1", "channel_id": "c1", "execution_date": "2026-04-01", "is_top_growth_7d": cls_value},
            {"video_id": "v2", "channel_id": "c1", "execution_date": "2026-04-08", "is_top_growth_7d": cls_value},
        ],
    )


def test_missing_supervised_examples_reports_blocker(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "modeling" / "model_readiness_report.json", {"trainable_examples": 0, "recommended_status": "not_ready"})
    _write_json(data_dir / "modeling" / "target_dictionary.json", {"targets": {}})
    _write_json(data_dir / "modeling" / "feature_dictionary.json", {"features": []})
    result = analyze_model_readiness(data_dir=data_dir)
    diag = json.loads((data_dir / "modeling" / "latest_model_readiness_diagnostics.json").read_text(encoding="utf-8"))
    assert result["can_train_now"] is False
    assert "no_supervised_examples" in diag["blockers"]


def test_trainable_zero_recommended_not_ready_and_gaps(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _seed_common(data_dir, trainable=0, recommended_status="not_ready")
    result = analyze_model_readiness(data_dir=data_dir)
    diag = json.loads((data_dir / "modeling" / "latest_model_readiness_diagnostics.json").read_text(encoding="utf-8"))
    coverage_rows = list(csv.DictReader((data_dir / "modeling" / "latest_target_coverage_report.csv").open("r", encoding="utf-8", newline="")))
    assert result["recommended_status"] == "not_ready"
    assert result["can_train_now"] is False
    assert "no_trainable_examples" in diag["blockers"]
    assert diag["examples_missing_for_exploratory"] == 300
    assert diag["examples_missing_for_baseline"] == 1000
    assert coverage_rows
    assert coverage_rows[0]["target_name"] == "is_top_growth_7d"
    assert "single_class_target" in diag["blockers"]


def test_temporal_split_timeline_forecast_and_reports(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _seed_common(data_dir, trainable=200, recommended_status="not_ready", cls_value="")
    # force no temporal split
    _write_csv(data_dir / "modeling" / "supervised_examples.csv", ["video_id", "channel_id", "execution_date", "is_top_growth_7d"], [{"video_id": "v1", "channel_id": "c1", "execution_date": "2026-04-01", "is_top_growth_7d": ""}])
    analyze_model_readiness(data_dir=data_dir)
    diag1 = json.loads((data_dir / "modeling" / "latest_model_readiness_diagnostics.json").read_text(encoding="utf-8"))
    assert "no_temporal_validation_split" in diag1["blockers"]
    assert diag1["forecast"]["status"] == "insufficient_history"
    assert (data_dir / "modeling" / "latest_model_readiness_report.md").exists()
    assert (data_dir / "modeling" / "latest_model_readiness_report.html").exists()

    _seed_common(data_dir, trainable=450, recommended_status="not_ready")
    timeline_path = data_dir / "modeling" / "latest_model_readiness_timeline.csv"
    rows = list(csv.DictReader(timeline_path.open("r", encoding="utf-8", newline="")))
    rows.append({**rows[-1], "generated_at": "2026-05-01T00:00:00+00:00", "trainable_examples": "600"})
    _write_csv(timeline_path, list(rows[0].keys()), rows)
    analyze_model_readiness(data_dir=data_dir)
    diag2 = json.loads((data_dir / "modeling" / "latest_model_readiness_diagnostics.json").read_text(encoding="utf-8"))
    assert diag2["forecast"]["status"] in {"ok", "no_positive_growth"}


def test_writes_only_modeling_and_no_api_reference(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _seed_common(data_dir, trainable=10, recommended_status="not_ready")
    before = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}
    analyze_model_readiness(data_dir=data_dir)
    after = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}
    created = after - before
    assert created
    assert all(str(path).startswith("modeling/") for path in created)
    source = Path("src/ytb_history/services/model_readiness_diagnostics_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in source
    assert "search.list" not in source
