from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.category_report_service import generate_category_report


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_data(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "title", "channel_name", "opportunity_score", "alpha_score", "topic_primary"],
        [
            {"video_id": "v1", "title": "AI tools that changed work", "channel_name": "Canal AI", "opportunity_score": 91, "alpha_score": 85, "topic_primary": "ai_tools"},
            {"video_id": "v2", "title": "Productivity basics", "channel_name": "Other", "opportunity_score": 50, "alpha_score": 40, "topic_primary": "productivity"},
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv",
        ["video_id", "title", "growth_trend_label", "growth_acceleration", "growth_acceleration_score", "topic_primary"],
        [{"video_id": "v1", "title": "AI tools that changed work", "growth_trend_label": "accelerating", "growth_acceleration": 30, "growth_acceleration_score": 88, "topic_primary": "ai_tools"}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_name", "channel_momentum_score", "total_views_delta", "top_video_title"],
        [{"channel_id": "c1", "channel_name": "Canal AI", "channel_momentum_score": 77, "total_views_delta": 1000, "top_video_title": "AI tools that changed work"}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_opportunities.csv",
        ["topic", "opportunity_type", "topic_opportunity_score", "topic_velocity_score", "recommended_action"],
        [{"topic": "ai_tools", "opportunity_type": "emerging_topic", "topic_opportunity_score": 93, "topic_velocity_score": 90, "recommended_action": "scale test"}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_title_pattern_metrics.csv",
        ["title_pattern", "video_count", "avg_views_delta", "avg_engagement_rate", "title_pattern_success_score"],
        [{"title_pattern": "numbered_list", "video_count": 3, "avg_views_delta": 250, "avg_engagement_rate": 0.1, "title_pattern_success_score": 81}],
    )
    _write_csv(
        data_dir / "model_reports" / "latest_content_driver_feature_direction.csv",
        ["feature", "direction", "direction_score"],
        [{"feature": "ai_semantic_score", "direction": "positive", "direction_score": 0.4}],
    )
    _write_csv(
        data_dir / "model_intelligence" / "latest_hybrid_recommendations.csv",
        ["video_id", "hybrid_decision_score", "model_score_percentile", "decision_score", "confidence_level"],
        [{"video_id": "v1", "hybrid_decision_score": 99, "model_score_percentile": 95, "decision_score": 88, "confidence_level": "high"}],
    )
    (data_dir / "alerts").mkdir(parents=True, exist_ok=True)
    (data_dir / "alerts" / "latest_alerts.json").write_text(
        json.dumps({"alerts": [{"signal_type": "trend_burst", "entity_id": "v1", "severity": "high", "adjusted_signal_score": 89}]}),
        encoding="utf-8",
    )
    return data_dir


def test_generate_category_report_writes_md_html_and_summary(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    output_dir = tmp_path / "reports"

    summary = generate_category_report(
        category_name="ai_tools",
        data_dir=data_dir,
        output_dir=output_dir,
        period_days=14,
        format="html",
    )

    md_path = output_dir / "latest_category_report.md"
    html_path = output_dir / "latest_category_report.html"
    summary_path = output_dir / "category_report_summary.json"

    assert summary["status"] == "success"
    assert summary["primary_report_path"] == str(html_path)
    assert md_path.exists()
    assert html_path.exists()
    assert summary_path.exists()
    assert "## Top oportunidades" in md_path.read_text(encoding="utf-8")
    assert "## Videos acelerando" in md_path.read_text(encoding="utf-8")
    assert "## Limitaciones de datos" in md_path.read_text(encoding="utf-8")
    assert json.loads(summary_path.read_text(encoding="utf-8"))["row_counts"]["top_opportunities"] == 1


def test_generate_category_report_rejects_invalid_format(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)

    try:
        generate_category_report(category_name="ai_tools", data_dir=data_dir, format="pdf")
    except ValueError as exc:
        assert "format" in str(exc)
    else:
        raise AssertionError("Expected invalid format to raise ValueError")
