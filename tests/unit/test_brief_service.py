from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.brief_service import generate_weekly_brief


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_data(tmp_path: Path, *, with_alerts: bool = True, with_decision: bool = True) -> Path:
    data_dir = tmp_path / "data"

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["video_id", "title", "views_delta", "likes_delta", "comments_delta", "engagement_rate"],
        [
            {"video_id": "v1", "title": "Video 1", "views_delta": 200, "likes_delta": 20, "comments_delta": 4, "engagement_rate": 0.11},
            {"video_id": "v2", "title": "Video 2", "views_delta": 400, "likes_delta": 40, "comments_delta": 10, "engagement_rate": 0.14},
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_metrics.csv",
        ["channel_id", "channel_name"],
        [{"channel_id": "c1", "channel_name": "Canal 1"}, {"channel_id": "c2", "channel_name": "Canal 2"}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "title", "alpha_score"],
        [{"video_id": "v1", "title": "Video 1", "alpha_score": 87}, {"video_id": "v2", "title": "Video 2", "alpha_score": 95}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv",
        ["video_id", "dummy"],
        [{"video_id": "v1", "dummy": 1}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_name", "channel_momentum_score"],
        [
            {"channel_id": "c1", "channel_name": "Canal 1", "channel_momentum_score": 88},
            {"channel_id": "c2", "channel_name": "Canal 2", "channel_momentum_score": 73},
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_title_metrics.csv",
        ["title_pattern", "sample_size", "avg_views_delta"],
        [
            {"title_pattern": "has_number", "sample_size": 4, "avg_views_delta": 100},
            {"title_pattern": "has_question", "sample_size": 2, "avg_views_delta": 60},
            {"title_pattern": "mentions_ai", "sample_size": 3, "avg_views_delta": 120},
            {"title_pattern": "mentions_finance", "sample_size": 1, "avg_views_delta": 80},
        ],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_opportunities.csv",
        ["topic", "opportunity_type", "topic_opportunity_score", "recommended_action"],
        [{"topic": "ai_tools", "opportunity_type": "emerging_topic", "topic_opportunity_score": 80, "recommended_action": "scale"}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_title_pattern_metrics.csv",
        ["title_pattern", "video_count", "avg_views_delta", "avg_engagement_rate", "title_pattern_success_score"],
        [{"title_pattern": "tutorial_how_to", "video_count": 4, "avg_views_delta": 120, "avg_engagement_rate": 0.1, "title_pattern_success_score": 75}],
    )
    _write_csv(
        data_dir / "nlp_features" / "latest_semantic_clusters.csv",
        ["video_id", "semantic_cluster_id", "semantic_cluster_label", "cluster_top_terms", "semantic_cluster_size"],
        [{"video_id": "v1", "semantic_cluster_id": 1, "semantic_cluster_label": "ai_cluster", "cluster_top_terms": "chatgpt ai", "semantic_cluster_size": 3}],
    )
    _write_csv(
        data_dir / "model_reports" / "latest_content_driver_feature_importance.csv",
        ["target", "model_family", "feature", "importance_rank"],
        [
            {"target": "future_log_views_delta_7d", "model_family": "random_forest_regressor", "feature": "ai_semantic_score", "importance_rank": 1},
            {"target": "future_engagement_delta_7d", "model_family": "linear_regularized_regressor", "feature": "topic_opportunity_score", "importance_rank": 1},
        ],
    )
    _write_csv(
        data_dir / "model_reports" / "latest_content_driver_feature_direction.csv",
        ["feature", "direction", "direction_score", "direction_method"],
        [
            {"feature": "ai_semantic_score", "direction": "positive", "direction_score": 0.2, "direction_method": "quantile_directional_analysis"},
            {"feature": "clickbait_flag", "direction": "negative", "direction_score": -0.1, "direction_method": "quantile_directional_analysis"},
        ],
    )
    _write_csv(
        data_dir / "model_reports" / "latest_content_driver_leaderboard.csv",
        ["target", "model_family", "spearman_corr"],
        [{"target": "future_log_views_delta_7d", "model_family": "random_forest_regressor", "spearman_corr": 0.6}],
    )

    (data_dir / "signals").mkdir(parents=True, exist_ok=True)
    (data_dir / "signals" / "signal_summary.json").write_text(
        json.dumps({"confidence_distribution": {"low": 4}}, ensure_ascii=False),
        encoding="utf-8",
    )

    if with_alerts:
        (data_dir / "alerts").mkdir(parents=True, exist_ok=True)
        (data_dir / "alerts" / "latest_alerts.json").write_text(
            json.dumps(
                {
                    "alerts": [
                        {"severity": "high", "signal_type": "trend_burst", "entity_id": "v2", "adjusted_signal_score": 90},
                        {"severity": "critical", "signal_type": "alpha_breakout", "entity_id": "v1", "adjusted_signal_score": 92},
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (data_dir / "alerts" / "alert_summary.json").write_text(json.dumps({"total_alerts": 2}, ensure_ascii=False), encoding="utf-8")

    if with_decision:
        _write_csv(
            data_dir / "decision" / "latest_action_candidates.csv",
            [
                "execution_date",
                "priority",
                "action_type",
                "recommended_action",
                "reason",
                "confidence_level",
                "decision_score",
                "evidence_json",
                "dashboard_tab",
            ],
            [
                {
                    "execution_date": "2026-04-28",
                    "priority": "high",
                    "action_type": "create_fast_reaction",
                    "recommended_action": "A1",
                    "reason": "R1",
                    "confidence_level": "high",
                    "decision_score": 85,
                    "evidence_json": "{\"a\":1}",
                    "dashboard_tab": "alerts",
                },
                {
                    "execution_date": "2026-04-28",
                    "priority": "critical",
                    "action_type": "create_evergreen",
                    "recommended_action": "A2",
                    "reason": "R2",
                    "confidence_level": "medium",
                    "decision_score": 95,
                    "evidence_json": "{\"b\":2}",
                    "dashboard_tab": "scores",
                },
            ],
        )
        _write_csv(
            data_dir / "decision" / "latest_opportunity_matrix.csv",
            ["action_type", "candidates_count", "avg_decision_score", "recommended_focus"],
            [{"action_type": "create_evergreen", "candidates_count": 1, "avg_decision_score": 95, "recommended_focus": "focus"}],
        )
        _write_csv(
            data_dir / "decision" / "latest_content_opportunities.csv",
            ["content_strategy", "source_title", "why_it_matters", "evidence_score", "recommended_timeframe"],
            [
                {"content_strategy": "evergreen", "source_title": "S1", "why_it_matters": "W1", "evidence_score": 70, "recommended_timeframe": "this_month"},
                {"content_strategy": "fast", "source_title": "S2", "why_it_matters": "W2", "evidence_score": 90, "recommended_timeframe": "next_3_days"},
            ],
        )
        _write_csv(
            data_dir / "decision" / "latest_watchlist_recommendations.csv",
            ["entity_type", "entity_id", "title", "reason", "watch_priority"],
            [
                {"entity_type": "video", "entity_id": "v1", "title": "T1", "reason": "watch", "watch_priority": 3},
                {"entity_type": "video", "entity_id": "v2", "title": "T2", "reason": "watch2", "watch_priority": 7},
            ],
        )
        (data_dir / "decision" / "decision_summary.json").write_text(json.dumps({"total_action_candidates": 2}, ensure_ascii=False), encoding="utf-8")

    return data_dir


def test_generate_weekly_brief_generates_outputs_and_sections(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)

    result = generate_weekly_brief(data_dir=data_dir)

    assert result["status"] in {"success", "success_with_warnings"}
    assert (data_dir / "briefs" / "latest_weekly_brief.md").exists()
    assert (data_dir / "briefs" / "latest_weekly_brief.html").exists()
    assert (data_dir / "briefs" / "latest_weekly_brief.json").exists()

    weekly_dir = Path(result["weekly_markdown_path"]).parent
    assert weekly_dir.name.startswith("week=")
    assert weekly_dir.exists()

    markdown_text = (data_dir / "briefs" / "latest_weekly_brief.md").read_text(encoding="utf-8")
    assert "## What Actions Should I Take This Week?" in markdown_text
    assert "## Top Content Opportunities" in markdown_text
    assert "## Watchlist" in markdown_text
    assert "## Topic Opportunities" in markdown_text
    assert "## Content Drivers" in markdown_text
    assert "predictivas, no causales" in markdown_text

    payload = json.loads((data_dir / "briefs" / "latest_weekly_brief.json").read_text(encoding="utf-8"))
    assert "top_actions_this_week" in payload
    assert payload["top_actions_this_week"][0]["decision_score"] == "95"
    assert any("acciones prioritarias" in bullet for bullet in payload["executive_summary"])


def test_generate_weekly_brief_includes_topic_and_content_driver_sections_when_inputs_exist(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    assert (data_dir / "topic_intelligence" / "latest_topic_opportunities.csv").exists()
    assert (data_dir / "model_reports" / "latest_content_driver_feature_importance.csv").exists()

    generate_weekly_brief(data_dir=data_dir)

    markdown_text = (data_dir / "briefs" / "latest_weekly_brief.md").read_text(encoding="utf-8")
    assert "## Topic Opportunities" in markdown_text
    assert "## Content Drivers" in markdown_text
    assert "predictivas, no causales" in markdown_text


def test_generate_weekly_brief_handles_missing_alerts_and_decision_with_warnings(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path, with_alerts=False, with_decision=False)

    result = generate_weekly_brief(data_dir=data_dir)

    assert result["status"] == "success_with_warnings"
    assert result["warnings"]
    assert (data_dir / "briefs" / "latest_weekly_brief.json").exists()


def test_generate_weekly_brief_writes_only_inside_briefs(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    existing_files = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}

    generate_weekly_brief(data_dir=data_dir)

    updated_files = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}
    created = updated_files - existing_files

    assert created
    assert all(str(path).startswith("briefs/") for path in created)


def test_generate_weekly_brief_has_no_api_or_search_list_dependency() -> None:
    source = Path("src/ytb_history/services/brief_service.py").read_text(encoding="utf-8")
    assert "search.list" not in source
    assert "youtube_client" not in source
