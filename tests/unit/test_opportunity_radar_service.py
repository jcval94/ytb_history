from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

import pytest

from ytb_history.services.opportunity_radar_service import generate_opportunity_radar


def _workspace_tmp(label: str) -> Path:
    root = Path("build") / "pytest-opportunity-radar-service"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{label}_{uuid.uuid4().hex}"
    path.mkdir()
    return path


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_data(data_dir: Path) -> None:
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["channel_id", "channel_name", "video_id", "title", "views_delta", "engagement_rate"],
        [
            {"channel_id": "c1", "channel_name": "Canal IA", "video_id": "v1", "title": "IA para negocios", "views_delta": 1200, "engagement_rate": 0.08},
            {"channel_id": "c2", "channel_name": "Canal Cocina", "video_id": "v2", "title": "Receta rapida", "views_delta": 9000, "engagement_rate": 0.03},
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_name", "channel_momentum_score", "total_views_delta"],
        [
            {"channel_id": "c1", "channel_name": "Canal IA", "channel_momentum_score": 91, "total_views_delta": 2000},
            {"channel_id": "c2", "channel_name": "Canal Cocina", "channel_momentum_score": 99, "total_views_delta": 9000},
        ],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_opportunities.csv",
        ["topic", "opportunity_type", "topic_opportunity_score", "recommended_action"],
        [{"topic": "ai_tools", "opportunity_type": "watch_topic", "topic_opportunity_score": 88, "recommended_action": "Crear comparativa"}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_topic_metrics.csv",
        ["topic", "topic_velocity_score"],
        [{"topic": "ai_tools", "topic_velocity_score": 80}],
    )
    _write_csv(
        data_dir / "topic_intelligence" / "latest_title_pattern_metrics.csv",
        ["title_pattern", "title_pattern_success_score", "avg_views_delta", "example_titles"],
        [{"title_pattern": "warning", "title_pattern_success_score": 15.2, "avg_views_delta": 500, "example_titles": "No uses esta IA"}],
    )
    _write_csv(
        data_dir / "decision" / "latest_action_candidates.csv",
        ["action_type", "priority", "recommended_action", "reason", "decision_score", "confidence_level", "timeframe", "dashboard_tab"],
        [{"action_type": "create_fast_reaction", "priority": "high", "recommended_action": "Probar comparativa IA", "reason": "Senal fuerte en IA", "decision_score": 93, "confidence_level": "high", "timeframe": "next_3_days", "dashboard_tab": "Brief"}],
    )
    _write_csv(
        data_dir / "decision" / "latest_content_opportunities.csv",
        ["opportunity_type", "content_strategy", "why_it_matters", "evidence_score", "recommended_timeframe"],
        [{"opportunity_type": "create_fast_reaction", "content_strategy": "Crear pieza IA", "why_it_matters": "Alta traccion", "evidence_score": 90, "recommended_timeframe": "next_3_days"}],
    )
    _write_csv(
        data_dir / "creative_packages" / "latest_creative_packages.csv",
        ["creative_package_id", "package_type", "topic", "creative_angle", "recommended_format", "creative_execution_score", "recommended_next_step"],
        [{"creative_package_id": "cp1", "package_type": "fast_reaction_package", "topic": "ai_tools", "creative_angle": "IA para equipos pequenos", "recommended_format": "video corto", "creative_execution_score": 87, "recommended_next_step": "Preparar guion"}],
    )
    _write_csv(
        data_dir / "creative_packages" / "latest_title_candidates.csv",
        ["creative_package_id", "title_candidate"],
        [{"creative_package_id": "cp1", "title_candidate": "La IA que tu equipo si puede usar"}],
    )
    _write_csv(
        data_dir / "creative_packages" / "latest_hook_candidates.csv",
        ["creative_package_id", "hook_text"],
        [{"creative_package_id": "cp1", "hook_text": "Tres senales de que esta IA ya es util"}],
    )
    (data_dir / "alerts").mkdir(parents=True, exist_ok=True)
    (data_dir / "alerts" / "latest_alerts.json").write_text(
        json.dumps({"alerts": [{"signal_type": "accelerating_video", "severity": "high", "title": "IA para negocios", "adjusted_signal_score": 92, "recommended_action": "Monitorear lift"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (data_dir / "alerts" / "alert_summary.json").write_text(json.dumps({"total_alerts": 1}), encoding="utf-8")
    (data_dir / "briefs").mkdir(parents=True, exist_ok=True)
    (data_dir / "briefs" / "latest_weekly_brief.json").write_text(json.dumps({"status": "success"}), encoding="utf-8")
    reports_dir = data_dir / "reports" / "dt=2026-05-14" / "run=112307Z"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "run_summary.json").write_text(json.dumps({"channels_total": 10, "execution_date": "2026-05-14T00:00:00+00:00"}), encoding="utf-8")
    (reports_dir / "quota_report.json").write_text(
        json.dumps(
            {
                "estimated_units": {"channels.list": 2, "playlistItems.list": 30, "videos.list": 4},
                "observed_units": {"channels.list": 1, "playlistItems.list": 20, "videos.list": 4},
                "total_estimated_units": 36,
                "total_observed_units": 25,
                "operational_limit": 7000,
                "limit_status": "ok",
            }
        ),
        encoding="utf-8",
    )


def _write_config(config_path: Path) -> None:
    config_path.write_text(
        """
schema_version: commercial_radar_v1
default_profile: test_profile
profiles:
  test_profile:
    client_name: Test Client
    category_name: IA para negocios
    package_name: Weekly YouTube Opportunity Radar
    plan_name: Pilot Radar
    monthly_price_usd: 750
    max_channels: 50
    period_days: 7
    output_slug: test-radar
    include_keywords:
      - ia
      - ai
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_generate_opportunity_radar_writes_commercial_outputs() -> None:
    root = _workspace_tmp("outputs")
    data_dir = root / "data"
    config_path = root / "commercial_radar.yaml"
    _prepare_data(data_dir)
    _write_config(config_path)

    result = generate_opportunity_radar(data_dir=data_dir, config_path=config_path)

    assert result["status"] == "success"
    assert result["profile"]["category_name"] == "IA para negocios"
    assert result["key_metrics"]["priority_opportunities"] == 1
    assert result["quota_report"]["total_estimated_units"] > 0
    assert Path(result["markdown_path"]).exists()
    assert Path(result["html_path"]).exists()
    assert Path(result["summary_path"]).exists()

    markdown = Path(result["markdown_path"]).read_text(encoding="utf-8")
    assert "Weekly YouTube Opportunity Radar" in markdown
    assert "## 2. Que hacer esta semana" in markdown
    assert "## 8. Cuota y margen operativo" in markdown
    assert "insights derivados" in markdown
    assert "Canal Cocina" not in markdown


def test_generate_opportunity_radar_supports_anonymized_demo() -> None:
    root = _workspace_tmp("anonymized")
    data_dir = root / "data"
    config_path = root / "commercial_radar.yaml"
    _prepare_data(data_dir)
    _write_config(config_path)

    result = generate_opportunity_radar(data_dir=data_dir, config_path=config_path, anonymize=True)
    markdown = Path(result["markdown_path"]).read_text(encoding="utf-8")

    assert result["profile"]["anonymized"] is True
    assert "Canal 1" in markdown
    assert "Canal IA" not in markdown


def test_generate_opportunity_radar_rejects_unknown_profile() -> None:
    root = _workspace_tmp("unknown_profile")
    config_path = root / "commercial_radar.yaml"
    _write_config(config_path)

    with pytest.raises(ValueError, match="profile"):
        generate_opportunity_radar(data_dir=root / "data", config_path=config_path, profile_name="missing")


def test_generate_opportunity_radar_has_no_api_dependency() -> None:
    source = Path("src/ytb_history/services/opportunity_radar_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in source
    assert "search.list" not in source
