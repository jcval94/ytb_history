from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.pages_dashboard_service import build_pages_dashboard


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _prepare_minimal_analytics(data_dir: Path) -> None:
    latest_dir = data_dir / "analytics" / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    _write_text(latest_dir / "dashboard_index.json", json.dumps({"dashboard": "ok"}, ensure_ascii=False))
    _write_text(latest_dir / "analytics_manifest.json", json.dumps({"version": 1}, ensure_ascii=False))
    _write_text(
        latest_dir / "latest_video_metrics.csv",
        "video_id,views,is_new_video,title,ratio,empty_col,channel_name,duration_bucket\n"
        "v1,123,True,Canción,1.5,,Canal Uno,short\n"
        "v2,001,False,Título UTF-8,,,Canal Dos,long\n",
    )


def _prepare_signals_and_alerts(data_dir: Path) -> None:
    _write_text(
        data_dir / "signals" / "latest_video_signals.csv",
        "execution_date,video_id,channel_id,channel_name,title,alpha_breakout,max_signal_score,top_signal_type,alert_count\n"
        "2026-04-28,v1,c1,Canal Uno,Video Uno,90,90,alpha_breakout,1\n",
    )
    _write_text(
        data_dir / "signals" / "latest_channel_signals.csv",
        "execution_date,channel_id,channel_name,channel_momentum_up,max_signal_score,top_signal_type,alert_count\n"
        "2026-04-28,c1,Canal Uno,85,85,channel_momentum_up,1\n",
    )
    _write_text(
        data_dir / "signals" / "latest_signal_candidates.csv",
        "execution_date,entity_type,entity_id,signal_type,raw_signal_score,adjusted_signal_score,threshold,triggered,metric_confidence_score,confidence_level\n"
        "2026-04-28,video,v1,alpha_breakout,90,72,85,True,60,medium\n",
    )
    _write_text(
        data_dir / "signals" / "signal_summary.json",
        json.dumps({"generated_at": "2026-04-28T00:00:00+00:00", "total_alerts": 1}, ensure_ascii=False),
    )
    _write_text(
        data_dir / "alerts" / "latest_alerts.json",
        json.dumps(
            {
                "generated_at": "2026-04-28T00:00:00+00:00",
                "alert_count": 1,
                "alerts": [{"signal_type": "alpha_breakout", "severity": "high"}],
            },
            ensure_ascii=False,
        ),
    )
    _write_text(
        data_dir / "alerts" / "alert_summary.json",
        json.dumps({"generated_at": "2026-04-28T00:00:00+00:00", "total_alerts": 1}, ensure_ascii=False),
    )


def _prepare_brief(data_dir: Path) -> None:
    _write_text(
        data_dir / "briefs" / "latest_weekly_brief.json",
        json.dumps(
            {
                "status": "success",
                "executive_summary": ["a", "b", "c"],
                "key_metrics": {"videos_total": 10},
            },
            ensure_ascii=False,
        ),
    )
    _write_text(data_dir / "briefs" / "latest_weekly_brief.md", "# Brief\n")
    _write_text(data_dir / "briefs" / "latest_weekly_brief.html", "<h1>Brief</h1>\n")


def _prepare_model_reports(data_dir: Path) -> None:
    _write_text(
        data_dir / "model_reports" / "latest_model_leaderboard.csv",
        "model_family,target,champion_metric,champion_metric_value,selected_as_champion,lift_vs_best_baseline\n"
        "linear_regularized,is_top_growth_7d,precision_at_10,0.9,True,0.2\n",
    )
    _write_text(
        data_dir / "model_reports" / "latest_feature_importance.csv",
        "model_id,model_family,target,feature,importance_type,importance_value,importance_rank,direction\n"
        "m1,linear_regularized,is_top_growth_7d,alpha_score,coefficient,0.8,1,positive\n",
    )
    _write_text(
        data_dir / "model_reports" / "latest_feature_direction.csv",
        "model_id,model_family,target,feature,direction,direction_score,direction_method,low_bin_prediction,high_bin_prediction,notes\n"
        "m2,random_forest,is_top_growth_7d,views_delta,positive,0.2,quantile_bins,0.3,0.5,estimated\n",
    )
    _write_text(data_dir / "model_reports" / "latest_model_suite_report.html", "<h2>Model Suite</h2>")
    _write_text(data_dir / "model_reports" / "latest_content_driver_report.html", "<h2>Content Driver</h2>")
    _write_text(
        data_dir / "model_reports" / "latest_content_driver_leaderboard.csv",
        "target,model_family,spearman_corr\nfuture_log_views_delta_7d,random_forest_regressor,0.61\n",
    )
    _write_text(
        data_dir / "model_reports" / "latest_content_driver_feature_importance.csv",
        "target,model_family,feature,importance_type,importance_value,importance_rank,direction\nfuture_log_views_delta_7d,random_forest_regressor,ai_semantic_score,permutation_importance_mean,0.2,1,positive\n",
    )
    _write_text(
        data_dir / "model_reports" / "latest_content_driver_feature_direction.csv",
        "target,model_family,feature,direction,direction_score,direction_method,low_bin_prediction,high_bin_prediction\nfuture_log_views_delta_7d,random_forest_regressor,ai_semantic_score,positive,0.2,quantile_directional_analysis,0.1,0.3\n",
    )
    _write_text(
        data_dir / "model_reports" / "latest_content_driver_group_importance.csv",
        "target,model_family,feature_group,group_importance,feature_count\nfuture_log_views_delta_7d,random_forest_regressor,semantic_scores,0.3,4\n",
    )
    _write_text(
        data_dir / "model_registry" / "latest_model_manifest.json",
        json.dumps(
            {
                "suite_id": "suite-1",
                "artifact_name": "ytb-model-suite-1",
                "workflow_run_id": "1",
                "expires_at_estimate": "2026-05-01T00:00:00+00:00",
            },
            ensure_ascii=False,
        ),
    )


def _prepare_nlp_topic_artifacts(data_dir: Path) -> None:
    _write_text(
        data_dir / "nlp_features" / "latest_video_nlp_features.csv",
        "video_id,title,channel_name,ai_semantic_score,finance_semantic_score,views_delta,semantic_cluster_label\nv1,Video Uno,Canal Uno,80,5,100,ai_cluster\n",
    )
    _write_text(
        data_dir / "nlp_features" / "latest_title_nlp_features.csv",
        "video_id,title,hook_semantic_type,dominant_semantic_score\nv1,Video Uno,tutorial,80\n",
    )
    _write_text(
        data_dir / "nlp_features" / "latest_semantic_clusters.csv",
        "video_id,semantic_cluster_id,semantic_cluster_size,semantic_cluster_label,cluster_top_terms\nv1,1,3,ai_cluster,chatgpt ai\n",
    )
    _write_text(data_dir / "nlp_features" / "nlp_feature_summary.json", json.dumps({"nlp_mode": "tfidf_lsa_dictionary_v1"}, ensure_ascii=False))
    _write_text(
        data_dir / "topic_intelligence" / "latest_video_topics.csv",
        "video_id,topic_primary,topic_opportunity_score\na1,ai_tools,75\n",
    )
    _write_text(
        data_dir / "topic_intelligence" / "latest_topic_metrics.csv",
        "topic,video_count,topic_opportunity_score\nai_tools,10,75\n",
    )
    _write_text(
        data_dir / "topic_intelligence" / "latest_title_pattern_metrics.csv",
        "title_pattern,video_count,title_pattern_success_score\ntutorial_how_to,5,70\n",
    )
    _write_text(
        data_dir / "topic_intelligence" / "latest_keyword_metrics.csv",
        "keyword,semantic_group,video_count\nchatgpt,ai_tools,7\n",
    )
    _write_text(
        data_dir / "topic_intelligence" / "latest_topic_opportunities.csv",
        "topic,opportunity_type,topic_opportunity_score,recommended_action\nai_tools,emerging_topic,80,Scale fast\n",
    )
    _write_text(data_dir / "topic_intelligence" / "topic_intelligence_summary.json", json.dumps({"mode": "topic_title_intelligence_v1"}, ensure_ascii=False))


def test_build_pages_dashboard_warns_when_dashboard_index_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _write_text(data_dir / "analytics" / "latest" / "analytics_manifest.json", "{}")

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert summary["status"] == "success_with_warnings"
    assert any("dashboard_index" in warning for warning in summary["warnings"])


def test_build_pages_dashboard_generates_dashboard_index_json(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    output_path = site_dir / "data" / "dashboard_index.json"
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {"dashboard": "ok"}


def test_build_pages_dashboard_includes_model_reports(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_model_reports(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "data" / "latest_model_leaderboard.json").exists()
    assert (site_dir / "data" / "latest_feature_importance.json").exists()
    assert (site_dir / "data" / "latest_feature_direction.json").exists()
    assert (site_dir / "data" / "latest_model_suite_report.html").exists()


def test_build_pages_dashboard_includes_nlp_topic_and_content_driver_outputs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_nlp_topic_artifacts(data_dir)
    _prepare_model_reports(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "data" / "latest_video_nlp_features.json").exists()
    assert (site_dir / "data" / "latest_title_nlp_features.json").exists()
    assert (site_dir / "data" / "latest_semantic_clusters.json").exists()
    assert (site_dir / "data" / "nlp_feature_summary.json").exists()
    assert (site_dir / "data" / "latest_topic_metrics.json").exists()
    assert (site_dir / "data" / "latest_topic_opportunities.json").exists()
    assert (site_dir / "data" / "latest_content_driver_leaderboard.json").exists()
    assert (site_dir / "data" / "latest_content_driver_feature_importance.json").exists()
    assert (site_dir / "data" / "latest_content_driver_report.html").exists()


def test_build_pages_dashboard_generates_site_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "pages_dashboard_v1"
    assert "latest_video_metrics" in manifest["tables"]


def test_build_pages_dashboard_copies_latest_weekly_brief_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_brief(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "data" / "latest_weekly_brief.json").exists()
    assert (site_dir / "data" / "latest_weekly_brief.html").exists()


def test_site_manifest_includes_latest_weekly_brief_outputs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_brief(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert "latest_weekly_brief" in manifest["brief_outputs"]
    assert manifest["brief_outputs"]["latest_weekly_brief"] == "data/latest_weekly_brief.json"


def test_build_pages_dashboard_generates_latest_alerts_json_when_source_exists(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_signals_and_alerts(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    payload = json.loads((site_dir / "data" / "latest_alerts.json").read_text(encoding="utf-8"))
    assert payload["alert_count"] == 1


def test_build_pages_dashboard_missing_alerts_generates_empty_json_with_warning(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    latest_alerts = json.loads((site_dir / "data" / "latest_alerts.json").read_text(encoding="utf-8"))
    assert latest_alerts == {}
    assert any("latest_alerts" in warning for warning in summary["warnings"])


def test_site_manifest_includes_alerts_and_signal_summary(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_signals_and_alerts(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert "latest_alerts" in manifest["tables"]
    assert "signal_summary" in manifest["tables"]


def test_build_pages_dashboard_converts_csv_to_json_with_columns_rows_and_types(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    payload = json.loads((site_dir / "data" / "latest_video_metrics.json").read_text(encoding="utf-8"))
    assert payload["columns"][:6] == ["video_id", "views", "is_new_video", "title", "ratio", "empty_col"]
    assert len(payload["rows"]) == 2

    row1 = payload["rows"][0]
    row2 = payload["rows"][1]
    assert row1["views"] == 123
    assert isinstance(row1["views"], int)
    assert row1["is_new_video"] is True
    assert row1["ratio"] == 1.5
    assert row1["empty_col"] is None
    assert row2["is_new_video"] is False
    assert row2["empty_col"] is None


def test_build_pages_dashboard_missing_csv_generates_empty_table_and_warning(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    missing_payload = json.loads((site_dir / "data" / "latest_channel_metrics.json").read_text(encoding="utf-8"))
    assert missing_payload["row_count"] == 0
    assert missing_payload["rows"] == []
    assert any("latest_channel_metrics" in warning for warning in summary["warnings"])


def test_build_pages_dashboard_copies_real_index_and_assets(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert "YouTube Intelligence Dashboard" in (site_dir / "index.html").read_text(encoding="utf-8")
    for asset in ["styles.css", "app.js", "formatters.js", "tables.js", "charts.js"]:
        assert (site_dir / "assets" / asset).exists()


def test_index_html_references_relative_assets(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    index_html = (site_dir / "index.html").read_text(encoding="utf-8")
    assert './assets/styles.css' in index_html
    assert './assets/app.js' in index_html


def test_app_js_uses_relative_data_paths_and_hardening_rules(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    app_js = (site_dir / "assets" / "app.js").read_text(encoding="utf-8")
    assert "./data/site_manifest.json" in app_js
    assert "./data/latest_video_metrics.json" in app_js
    assert "./data/latest_alerts.json" in app_js
    assert "./data/latest_signal_candidates.json" in app_js
    assert "./data/latest_topic_metrics.json" in app_js
    assert "./data/latest_video_nlp_features.json" in app_js
    assert "./data/latest_content_driver_leaderboard.json" in app_js
    assert "search.list" not in app_js
    assert "http://" not in app_js
    assert "https://" not in app_js
    assert "low_confidence_flag" not in app_js
    assert "metric_confidence_score" in app_js
    assert "< 50" in app_js
    assert "success_horizon_label" in app_js


def test_dashboard_has_expected_sections(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    index_html = (site_dir / "index.html").read_text(encoding="utf-8")
    for label in ["Overview", "Videos", "Channels", "Scores", "Advanced", "Titles", "Periods", "Alerts", "Data Quality", "Models", "Topics", "NLP", "Content Drivers", "Brief"]:
        assert label in index_html


def test_build_pages_dashboard_does_not_write_outside_site_dir(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    site_root = site_dir.resolve()
    for output_file in summary["files_written"]:
        assert Path(output_file).resolve().is_relative_to(site_root)


def test_build_pages_dashboard_no_api_calls(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    def _boom(*_args, **_kwargs):
        raise AssertionError("YouTube API should not be called")

    monkeypatch.setattr("ytb_history.clients.youtube_client.YouTubeClient.__init__", _boom)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert summary["status"] in {"success", "success_with_warnings"}


def test_build_pages_dashboard_creates_site_index_and_site_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "index.html").exists()
    assert (site_dir / "data" / "site_manifest.json").exists()


def test_ml_not_initialized_is_notice_not_warning(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_signals_and_alerts(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)
    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert "data_freshness" in manifest
    assert "warnings" in manifest
    assert "notices" in manifest
    assert not any("ml_data_status: No inicializado todavía" in w for w in manifest["warnings"])
    assert any("ml_data_status: No inicializado todavía" in n for n in manifest["notices"])


def test_operational_missing_produces_warning(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_model_reports(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)
    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert any("operational_data_status" in warning for warning in manifest["warnings"])


def test_ml_only_notices_keep_global_non_warning_in_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _prepare_signals_and_alerts(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)
    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    domain_warnings = [w for w in manifest["warnings"] if "ml_data_status" in w]
    assert domain_warnings == []
    assert any("ml_data_status" in notice for notice in manifest["notices"])


def test_build_pages_dashboard_includes_model_readiness_outputs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)
    _write_text(data_dir / "modeling" / "latest_model_readiness_diagnostics.json", json.dumps({"recommended_status": "not_ready", "can_train_now": False}, ensure_ascii=False))
    _write_text(data_dir / "modeling" / "latest_training_gap_report.json", json.dumps({"primary_blocker": "no_trainable_examples"}, ensure_ascii=False))
    _write_text(data_dir / "modeling" / "latest_target_coverage_report.csv", "target_name,coverage_pct,trainable_rows,blocker,status\nis_top_growth_7d,40.0,10,single_class_target,not_ready\n")
    _write_text(data_dir / "modeling" / "latest_model_readiness_report.html", "<h2>Readiness</h2>")

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "data" / "latest_model_readiness_diagnostics.json").exists()
    assert (site_dir / "data" / "latest_training_gap_report.json").exists()
    assert (site_dir / "data" / "latest_target_coverage_report.json").exists()
    assert (site_dir / "data" / "latest_model_readiness_report.html").exists()
