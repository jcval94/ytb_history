from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.creative_packages_service import build_creative_packages


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_creative_packages_outputs_and_rules(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_csv(
        data_dir / "decision" / "latest_action_candidates.csv",
        ["action_id", "action_type", "video_id", "entity_id", "channel_name", "title", "decision_score", "metric_confidence_score", "timeframe", "signal_type"],
        [
            {"action_id": "a1", "action_type": "create_fast_reaction", "video_id": "v1", "entity_id": "v1", "channel_name": "Canal A", "title": "Título original A", "decision_score": 90, "metric_confidence_score": 80, "timeframe": "next_3_days", "signal_type": "trend_burst"},
            {"action_id": "a2", "action_type": "create_evergreen", "video_id": "v2", "entity_id": "v2", "channel_name": "Canal B", "title": "Título original B", "decision_score": 70, "metric_confidence_score": 70, "timeframe": "this_month", "signal_type": "evergreen_candidate"},
            {"action_id": "a3", "action_type": "repackage_idea", "video_id": "v3", "entity_id": "v3", "channel_name": "Canal C", "title": "Título original C", "decision_score": 65, "metric_confidence_score": 60, "timeframe": "this_week", "signal_type": "packaging_problem"},
        ],
    )
    _write_csv(data_dir / "decision" / "latest_content_opportunities.csv", ["opportunity_id", "source_video_id", "opportunity_type", "recommended_timeframe", "source_title"], [{"opportunity_id": "o1", "source_video_id": "v1", "opportunity_type": "emerging_topic", "recommended_timeframe": "next_3_days", "source_title": "Título original A"}])
    _write_csv(data_dir / "topic_intelligence" / "latest_topic_opportunities.csv", ["video_id", "topic", "topic_opportunity_score", "topic_saturation_score", "title_pattern", "tutorial_semantic_score", "title_pattern_success_score"], [{"video_id": "v1", "topic": "IA para negocios", "topic_opportunity_score": 80, "topic_saturation_score": 20, "title_pattern": "trend", "tutorial_semantic_score": 0, "title_pattern_success_score": 75}])

    result = build_creative_packages(data_dir=data_dir)
    assert result["status"] in {"success", "warning"}

    out = data_dir / "creative_packages"
    expected = [
        "latest_creative_packages.csv", "latest_title_candidates.csv", "latest_hook_candidates.csv", "latest_thumbnail_briefs.csv",
        "latest_script_outlines.csv", "latest_originality_checks.csv", "latest_production_checklist.csv", "creative_packages_summary.json",
    ]
    for filename in expected:
        assert (out / filename).exists()

    packages = list(csv.DictReader((out / "latest_creative_packages.csv").open("r", encoding="utf-8", newline="")))
    assert len(packages) == 3
    by_action = {r["source_action_id"]: r for r in packages}
    assert by_action["a1"]["package_type"] == "fast_reaction_package"
    assert by_action["a2"]["package_type"] == "evergreen_explainer_package"
    assert by_action["a3"]["package_type"] == "repackage_package"
    assert float(by_action["a1"]["source_decision_score"]) == 90.0
    assert float(by_action["a1"]["creative_execution_score"]) > 0

    titles = list(csv.DictReader((out / "latest_title_candidates.csv").open("r", encoding="utf-8", newline="")))
    assert len(titles) >= 9
    assert all(t["title_candidate"].strip().lower() != "título original a" for t in titles)

    originality = list(csv.DictReader((out / "latest_originality_checks.csv").open("r", encoding="utf-8", newline="")))
    assert originality
    assert all(r["originality_status"] in {"ok", "risky"} for r in originality)

    hooks = list(csv.DictReader((out / "latest_hook_candidates.csv").open("r", encoding="utf-8", newline="")))
    assert hooks
    thumbs = list(csv.DictReader((out / "latest_thumbnail_briefs.csv").open("r", encoding="utf-8", newline="")))
    assert thumbs
    outlines = list(csv.DictReader((out / "latest_script_outlines.csv").open("r", encoding="utf-8", newline="")))
    assert outlines
    checklist = list(csv.DictReader((out / "latest_production_checklist.csv").open("r", encoding="utf-8", newline="")))
    assert checklist

    summary = json.loads((out / "creative_packages_summary.json").read_text(encoding="utf-8"))
    assert summary["total_packages"] == 3


def test_creative_service_no_api_and_no_search_list() -> None:
    text = Path("src/ytb_history/services/creative_packages_service.py").read_text(encoding="utf-8")
    assert "youtube" not in text.lower()
    assert "search.list" not in text
