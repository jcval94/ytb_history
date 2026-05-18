from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ytb_history.services.category_report_service import generate_category_report


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _prepare_data(data_dir: Path) -> None:
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["execution_date", "channel_id", "channel_name", "video_id", "title", "upload_date", "content_format", "views_delta", "engagement_rate"],
        [
            {
                "execution_date": "2026-05-12T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal Uno",
                "video_id": "v1",
                "title": "Nuevo agente de IA",
                "upload_date": "2026-05-10T00:00:00+00:00",
                "content_format": "shorts",
                "views_delta": 900,
                "engagement_rate": 0.08,
            },
            {
                "execution_date": "2026-05-12T00:00:00+00:00",
                "channel_id": "c2",
                "channel_name": "Canal Dos",
                "video_id": "v2",
                "title": "Video anterior",
                "upload_date": "2026-04-01T00:00:00+00:00",
                "content_format": "videos",
                "views_delta": 2000,
                "engagement_rate": 0.02,
            },
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_name", "channel_momentum_score", "total_views_delta", "top_video_title"],
        [{"channel_id": "c1", "channel_name": "Canal Uno", "channel_momentum_score": 88, "total_views_delta": 1200, "top_video_title": "Nuevo agente de IA"}],
    )
    _write_csv(data_dir / "analytics" / "latest" / "latest_title_metrics.csv", ["title_pattern", "video_count"], [])
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_opportunities.csv",
        ["content_format", "topic", "opportunity_type", "topic_opportunity_score", "recommended_action", "why_it_matters"],
        [{"topic": "ai_tools", "opportunity_type": "watch_topic", "topic_opportunity_score": 77.5, "recommended_action": "Crear comparativa", "why_it_matters": "Alta tracción"}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_metrics.csv",
        ["content_format", "topic", "topic_velocity_score", "topic_opportunity_score", "video_count"],
        [{"topic": "ai_tools", "topic_velocity_score": 95, "topic_opportunity_score": 77.5, "video_count": 3}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_title_pattern_metrics.csv",
        ["content_format", "title_pattern", "title_pattern_success_score", "avg_views_delta", "example_titles"],
        [{"title_pattern": "warning", "title_pattern_success_score": 15.5, "avg_views_delta": 321, "example_titles": "No uses esta IA"}],
    )
    (data_dir / "topic_intelligence").mkdir(parents=True, exist_ok=True)
    (data_dir / "topic_intelligence" / "topic_intelligence_summary.json").write_text(json.dumps({"status": "success"}), encoding="utf-8")
    (data_dir / "alerts").mkdir(parents=True, exist_ok=True)
    (data_dir / "alerts" / "latest_alerts.json").write_text(json.dumps({"alerts": [{"entity_id": "v1", "adjusted_signal_score": 90}]}), encoding="utf-8")
    (data_dir / "alerts" / "alert_summary.json").write_text(json.dumps({"alert_count": 1}), encoding="utf-8")
    _write_csv(data_dir / "model_reports" / "latest_content_driver_leaderboard.csv", ["model", "spearman_corr"], [{"model": "m1", "spearman_corr": 0.3}])
    _write_csv(data_dir / "model_reports" / "latest_content_driver_feature_importance.csv", ["feature", "importance_rank"], [{"feature": "title", "importance_rank": 1}])
    _write_csv(data_dir / "model_reports" / "latest_content_driver_feature_direction.csv", ["feature", "direction_score"], [{"feature": "title", "direction_score": 1}])
    _write_csv(
        data_dir / "model_intelligence" / "latest_hybrid_recommendations.csv",
        ["content_format", "video_id", "hybrid_decision_score", "confidence_level"],
        [{"video_id": "v1", "hybrid_decision_score": 33, "confidence_level": "high"}],
    )
    (data_dir / "model_intelligence").mkdir(parents=True, exist_ok=True)
    (data_dir / "model_intelligence" / "model_intelligence_summary.json").write_text(json.dumps({"status": "success"}), encoding="utf-8")
    (data_dir / "briefs").mkdir(parents=True, exist_ok=True)
    (data_dir / "briefs" / "latest_weekly_brief.json").write_text(json.dumps({"week": "2026-20"}), encoding="utf-8")
    (data_dir / "briefs" / "latest_weekly_brief.md").write_text("# Brief\n", encoding="utf-8")


def test_generate_category_report_writes_md_html_and_summary(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "reports"
    _prepare_data(data_dir)

    result = generate_category_report(category_name="IA", data_dir=data_dir, output_dir=output_dir, period_days=14, format="html")

    assert result["status"] == "success"
    assert result["preferred_report_path"] == str(output_dir / "latest_category_report.html")
    assert (output_dir / "latest_category_report.md").exists()
    assert (output_dir / "latest_category_report.html").exists()
    assert (output_dir / "category_report_summary.json").exists()

    markdown = (output_dir / "latest_category_report.md").read_text(encoding="utf-8")
    assert "## 1. Portada" in markdown
    assert "## Shorts vs Videos" in markdown
    assert "## 9. Limitaciones de datos" in markdown
    assert "Nuevo agente de IA" in markdown
    assert "Video anterior" not in markdown

    summary = json.loads((output_dir / "category_report_summary.json").read_text(encoding="utf-8"))
    assert summary["category_name"] == "IA"
    assert summary["key_metrics"]["videos_in_period"] == 1
    assert summary["format_breakdown"]["shorts"]["videos_in_period"] == 1
    assert summary["format_breakdown"]["videos"]["videos_in_period"] == 0


def test_generate_category_report_defaults_output_dir_to_data_dir(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _prepare_data(data_dir)

    result = generate_category_report(category_name="IA", data_dir=data_dir)

    assert result["markdown_path"] == str(data_dir / "category_reports" / "latest_category_report.md")
    assert (data_dir / "category_reports" / "latest_category_report.md").exists()


def test_generate_category_report_validates_arguments(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="category_name"):
        generate_category_report(category_name=" ", data_dir=tmp_path)
    with pytest.raises(ValueError, match="period_days"):
        generate_category_report(category_name="IA", data_dir=tmp_path, period_days=0)
    with pytest.raises(ValueError, match="format"):
        generate_category_report(category_name="IA", data_dir=tmp_path, format="pdf")
