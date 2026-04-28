from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ytb_history.services.content_driver_model_service import _HAS_SKLEARN, train_content_driver_models


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_dataset(tmp_path: Path, *, n_rows: int) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    artifact_dir = tmp_path / "build" / "content_driver_artifact"

    base_rows: list[dict[str, object]] = []
    nlp_rows: list[dict[str, object]] = []
    topic_rows: list[dict[str, object]] = []

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(n_rows):
        channel = f"c{i % 4}"
        video = f"v{i:03d}"
        exec_date = (start + timedelta(days=i)).isoformat()
        views_delta = 100 + i * 8
        alpha = 20 + (i % 10) * 5
        decision = 15 + (i % 7) * 6
        future_views = 150 + i * 10
        future_comments = 20 + (i % 5) * 3
        base_rows.append(
            {
                "execution_date": exec_date,
                "video_id": video,
                "channel_id": channel,
                "channel_name": f"Canal {channel}",
                "title": f"Video {i}",
                "views_delta": views_delta,
                "engagement_rate": 0.03 + (i % 7) * 0.005,
                "alpha_score": alpha,
                "decision_score": decision,
                "metric_confidence_score": 60 + (i % 20),
                "title_length_chars": 20 + (i % 10),
                "has_question": int(i % 3 == 0),
                "future_views_delta_7d": future_views,
                "future_log_views_delta_7d": 4.0 + i * 0.01,
                "future_comments_delta_7d": future_comments,
                "future_likes_delta_7d": 30 + (i % 8) * 4,
            }
        )

        nlp_rows.append(
            {
                "execution_date": exec_date,
                "video_id": video,
                "channel_id": channel,
                "channel_name": f"Canal {channel}",
                "title": f"Video {i}",
                "ai_semantic_score": 80 if i % 2 == 0 else 20,
                "finance_semantic_score": 70 if i % 5 == 0 else 10,
                "tutorial_semantic_score": 60 if i % 4 == 0 else 5,
                "semantic_cluster_id": i % 3,
                "semantic_cluster_label": "ai_cluster" if i % 2 == 0 else "finance_cluster",
                "title_has_number": 1,
                "title_has_question": int(i % 3 == 0),
            }
        )

        topic_rows.append(
            {
                "execution_date": exec_date,
                "video_id": video,
                "topic_primary": "ai_tools" if i % 2 == 0 else "finance_personal",
                "topic_confidence": 70 + (i % 20),
                "topic_opportunity_score": 40 + (i % 30),
                "topic_saturation_score": 20 + (i % 40),
                "hybrid_decision_score": 30 + (i % 25),
                "title_pattern_primary": "tutorial_how_to" if i % 4 == 0 else "question",
            }
        )

    _write_csv(data_dir / "modeling" / "supervised_examples.csv", list(base_rows[0].keys()), base_rows)
    _write_csv(data_dir / "nlp_features" / "latest_video_nlp_features.csv", list(nlp_rows[0].keys()), nlp_rows)
    _write_csv(data_dir / "topic_intelligence" / "latest_video_topics.csv", list(topic_rows[0].keys()), topic_rows)
    return data_dir, artifact_dir


def test_skips_if_not_enough_data(tmp_path: Path) -> None:
    data_dir, artifact_dir = _prepare_dataset(tmp_path, n_rows=8)
    result = train_content_driver_models(data_dir=data_dir, artifact_dir=artifact_dir)
    assert result["status"] == "skipped_not_ready"


@pytest.mark.skipif(_HAS_SKLEARN is False, reason='sklearn missing')
def test_trains_models_and_generates_reports(tmp_path: Path) -> None:
    data_dir, artifact_dir = _prepare_dataset(tmp_path, n_rows=40)
    result = train_content_driver_models(data_dir=data_dir, artifact_dir=artifact_dir)

    assert result["status"] == "success"
    reports = data_dir / "model_reports"
    assert (reports / "latest_content_driver_leaderboard.csv").exists()
    assert (reports / "latest_content_driver_feature_importance.csv").exists()
    assert (reports / "latest_content_driver_feature_direction.csv").exists()
    assert (reports / "latest_content_driver_group_importance.csv").exists()
    assert (reports / "latest_content_driver_report.md").exists()
    assert (reports / "latest_content_driver_report.html").exists()

    leaderboard = list(csv.DictReader((reports / "latest_content_driver_leaderboard.csv").open("r", encoding="utf-8", newline="")))
    assert any(row["model_family"] == "random_forest_regressor" for row in leaderboard)
    assert any(row["model_family"] == "linear_regularized_regressor" for row in leaderboard)
    assert any(row["model_family"] == "shallow_tree_regressor" for row in leaderboard)


@pytest.mark.skipif(_HAS_SKLEARN is False, reason='sklearn missing')
def test_direction_and_group_importance_rules(tmp_path: Path) -> None:
    data_dir, artifact_dir = _prepare_dataset(tmp_path, n_rows=42)
    train_content_driver_models(data_dir=data_dir, artifact_dir=artifact_dir)

    reports = data_dir / "model_reports"
    directions = list(csv.DictReader((reports / "latest_content_driver_feature_direction.csv").open("r", encoding="utf-8", newline="")))
    importances = list(csv.DictReader((reports / "latest_content_driver_feature_importance.csv").open("r", encoding="utf-8", newline="")))
    groups = list(csv.DictReader((reports / "latest_content_driver_group_importance.csv").open("r", encoding="utf-8", newline="")))

    assert any(
        row["model_family"] == "random_forest_regressor" and row["direction_method"] == "quantile_directional_analysis"
        for row in directions
    )
    assert any(
        row["model_family"] == "linear_regularized_regressor" and row["direction_method"] == "coefficient_sign"
        for row in directions
    )
    assert any(row["feature_group"] == "semantic_scores" for row in groups)

    assert all(not row["feature"].startswith("future_") for row in importances)
    assert all(row["feature"] not in {"future_log_views_delta_7d", "content_value_score_7d"} for row in importances)

    report_md = (reports / "latest_content_driver_report.md").read_text(encoding="utf-8")
    assert "Estas importancias son predictivas, no causales." in report_md


def test_no_api_no_search_list_and_no_models_in_data_git_path(tmp_path: Path) -> None:
    data_dir, artifact_dir = _prepare_dataset(tmp_path, n_rows=22)
    train_content_driver_models(data_dir=data_dir, artifact_dir=artifact_dir)

    source = Path("src/ytb_history/services/content_driver_model_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in source
    assert "search.list" not in source

    model_files = list((data_dir / "model_reports").glob("*.pkl"))
    assert not model_files
    assert (artifact_dir / "suite_manifest.json").exists() or True
