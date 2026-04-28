from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.topic_intelligence_service import build_topic_intelligence


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_data(tmp_path: Path, *, include_decision: bool = True, include_model: bool = True) -> Path:
    data_dir = tmp_path / "data"

    _write_csv(
        data_dir / "nlp_features" / "latest_video_nlp_features.csv",
        [
            "execution_date",
            "video_id",
            "channel_id",
            "channel_name",
            "title",
            "ai_semantic_score",
            "finance_semantic_score",
            "productivity_semantic_score",
            "tutorial_semantic_score",
            "news_semantic_score",
            "title_has_number",
            "title_has_question",
            "semantic_cluster_id",
            "semantic_cluster_label",
        ],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v1",
                "channel_id": "c1",
                "channel_name": "AI Lab",
                "title": "ChatGPT nuevo tutorial 2026",
                "ai_semantic_score": 90,
                "finance_semantic_score": 0,
                "productivity_semantic_score": 20,
                "tutorial_semantic_score": 30,
                "news_semantic_score": 10,
                "title_has_number": 1,
                "title_has_question": 0,
                "semantic_cluster_id": 1,
                "semantic_cluster_label": "chatgpt_ai_tool",
            },
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v2",
                "channel_id": "c2",
                "channel_name": "Finanzas Hoy",
                "title": "Ahorra dinero: inversión fácil",
                "ai_semantic_score": 0,
                "finance_semantic_score": 85,
                "productivity_semantic_score": 10,
                "tutorial_semantic_score": 0,
                "news_semantic_score": 0,
                "title_has_number": 0,
                "title_has_question": 0,
                "semantic_cluster_id": 2,
                "semantic_cluster_label": "dinero_finanzas_ahorro",
            },
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v3",
                "channel_id": "c3",
                "channel_name": "General",
                "title": "¿Vale la pena este cambio?",
                "ai_semantic_score": 10,
                "finance_semantic_score": 10,
                "productivity_semantic_score": 10,
                "tutorial_semantic_score": 10,
                "news_semantic_score": 10,
                "title_has_number": 0,
                "title_has_question": 1,
                "semantic_cluster_id": 1,
                "semantic_cluster_label": "chatgpt_ai_tool",
            },
        ],
    )

    _write_csv(
        data_dir / "nlp_features" / "latest_title_nlp_features.csv",
        ["video_id", "hook_semantic_type"],
        [
            {"video_id": "v1", "hook_semantic_type": "tutorial"},
            {"video_id": "v2", "hook_semantic_type": "promise"},
            {"video_id": "v3", "hook_semantic_type": "warning"},
        ],
    )

    _write_csv(
        data_dir / "nlp_features" / "latest_semantic_vectors.csv",
        ["video_id", "lsa_1", "lsa_2"],
        [
            {"video_id": "v1", "lsa_1": 0.5, "lsa_2": 0.1},
            {"video_id": "v2", "lsa_1": 0.2, "lsa_2": 0.7},
            {"video_id": "v3", "lsa_1": 0.1, "lsa_2": 0.1},
        ],
    )

    _write_csv(
        data_dir / "nlp_features" / "latest_semantic_clusters.csv",
        ["video_id", "semantic_cluster_id", "semantic_cluster_size", "semantic_cluster_label", "cluster_top_terms"],
        [
            {"video_id": "v1", "semantic_cluster_id": 1, "semantic_cluster_size": 2, "semantic_cluster_label": "chatgpt_ai_tool", "cluster_top_terms": "chatgpt ai tool"},
            {"video_id": "v2", "semantic_cluster_id": 2, "semantic_cluster_size": 1, "semantic_cluster_label": "dinero_finanzas_ahorro", "cluster_top_terms": "dinero finanzas ahorro"},
            {"video_id": "v3", "semantic_cluster_id": 1, "semantic_cluster_size": 2, "semantic_cluster_label": "chatgpt_ai_tool", "cluster_top_terms": "chatgpt ai tool"},
        ],
    )

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "views_delta", "engagement_rate", "alpha_score"],
        [
            {"video_id": "v1", "views_delta": 1200, "engagement_rate": 0.09, "alpha_score": 80},
            {"video_id": "v2", "views_delta": 850, "engagement_rate": 0.08, "alpha_score": 65},
            {"video_id": "v3", "views_delta": 150, "engagement_rate": 0.04, "alpha_score": 30},
        ],
    )

    if include_decision:
        _write_csv(
            data_dir / "decision" / "latest_action_candidates.csv",
            ["video_id", "decision_score"],
            [
                {"video_id": "v1", "decision_score": 90},
                {"video_id": "v2", "decision_score": 75},
                {"video_id": "v3", "decision_score": 80},
            ],
        )

    if include_model:
        _write_csv(
            data_dir / "model_intelligence" / "latest_hybrid_recommendations.csv",
            ["video_id", "hybrid_decision_score", "model_score_percentile"],
            [
                {"video_id": "v1", "hybrid_decision_score": 92, "model_score_percentile": 95},
                {"video_id": "v2", "hybrid_decision_score": 70, "model_score_percentile": 80},
                {"video_id": "v3", "hybrid_decision_score": 88, "model_score_percentile": 90},
            ],
        )

    return data_dir


def test_generates_all_outputs(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    result = build_topic_intelligence(data_dir=data_dir)
    assert result["status"] in {"success", "success_with_warnings"}

    out = data_dir / "topic_intelligence"
    for name in [
        "latest_video_topics.csv",
        "latest_topic_metrics.csv",
        "latest_title_pattern_metrics.csv",
        "latest_keyword_metrics.csv",
        "latest_topic_opportunities.csv",
        "topic_intelligence_summary.json",
    ]:
        assert (out / name).exists()


def test_uses_nlp_features_and_topic_primary_ai_finance(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    build_topic_intelligence(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "topic_intelligence" / "latest_video_topics.csv").open("r", encoding="utf-8", newline="")))
    by_id = {row["video_id"]: row for row in rows}
    assert by_id["v1"]["topic_primary"] == "ai_tools"
    assert by_id["v2"]["topic_primary"] == "finance_personal"


def test_semantic_cluster_reinforces_confidence(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    build_topic_intelligence(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "topic_intelligence" / "latest_video_topics.csv").open("r", encoding="utf-8", newline="")))
    by_id = {row["video_id"]: row for row in rows}
    assert float(by_id["v1"]["topic_confidence"]) > float(by_id["v3"]["topic_confidence"])


def test_topic_opportunity_and_saturation_and_semantic_gap(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    build_topic_intelligence(data_dir=data_dir)

    topic_rows = list(csv.DictReader((data_dir / "topic_intelligence" / "latest_topic_metrics.csv").open("r", encoding="utf-8", newline="")))
    assert topic_rows
    ai_topic = next(row for row in topic_rows if row["topic"] == "ai_tools")
    assert float(ai_topic["topic_opportunity_score"]) >= 0
    assert float(ai_topic["topic_saturation_score"]) >= 0

    opportunities = list(csv.DictReader((data_dir / "topic_intelligence" / "latest_topic_opportunities.csv").open("r", encoding="utf-8", newline="")))
    assert opportunities
    assert any(row["opportunity_type"] == "semantic_gap" for row in opportunities)


def test_works_without_model_intelligence(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path, include_model=False)
    result = build_topic_intelligence(data_dir=data_dir)
    assert result["status"] == "success_with_warnings"
    assert (data_dir / "topic_intelligence" / "latest_video_topics.csv").exists()


def test_works_without_decision_layer(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path, include_decision=False)
    result = build_topic_intelligence(data_dir=data_dir)
    assert result["status"] == "success_with_warnings"
    assert (data_dir / "topic_intelligence" / "latest_video_topics.csv").exists()


def test_no_api_no_search_list_and_writes_only_topic_intelligence(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    existing = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}

    build_topic_intelligence(data_dir=data_dir)

    source = Path("src/ytb_history/services/topic_intelligence_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in source
    assert "search.list" not in source

    updated = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}
    created = updated - existing
    assert created
    assert all(str(path).startswith("topic_intelligence/") for path in created)


def test_summary_is_json(tmp_path: Path) -> None:
    data_dir = _prepare_data(tmp_path)
    build_topic_intelligence(data_dir=data_dir)
    summary = json.loads((data_dir / "topic_intelligence" / "topic_intelligence_summary.json").read_text(encoding="utf-8"))
    assert summary["mode"] == "topic_title_intelligence_v1"
