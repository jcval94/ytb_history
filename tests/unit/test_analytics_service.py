from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ytb_history.services import analytics_service
from ytb_history.services.analytics_service import (
    build_analytics,
    percentile_rank,
    robust_z_scores,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_exports(tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports" / "dt=2026-04-28" / "run=003853Z"
    growth_columns = [
        "execution_date",
        "channel_id",
        "channel_name",
        "video_id",
        "title",
        "upload_date",
        "duration_seconds",
        "views",
        "likes",
        "comments",
        "views_delta",
        "likes_delta",
        "comments_delta",
        "is_new_video",
        "title_changed",
        "description_changed",
        "tags_changed",
        "engagement_rate",
    ]
    _write_csv(
        export_dir / "video_growth_summary.csv",
        growth_columns,
        [
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-1",
                "title": "¿AI 2026 dinero?",
                "upload_date": "2026-04-27T00:38:53+00:00",
                "duration_seconds": 60,
                "views": 100,
                "likes": 10,
                "comments": 5,
                "views_delta": 20,
                "likes_delta": 2,
                "comments_delta": 1,
                "is_new_video": True,
                "title_changed": False,
                "description_changed": True,
                "tags_changed": False,
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-2",
                "title": "Tutorial largo",
                "upload_date": "2026-04-18T00:38:53+00:00",
                "duration_seconds": 700,
                "views": 200,
                "likes": 20,
                "comments": 10,
                "views_delta": 5,
                "likes_delta": 1,
                "comments_delta": 1,
                "is_new_video": False,
                "title_changed": True,
                "description_changed": False,
                "tags_changed": True,
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-3",
                "title": "Sin datos",
                "upload_date": "",
                "duration_seconds": "",
                "views": "",
                "likes": "",
                "comments": "",
                "views_delta": "",
                "likes_delta": "",
                "comments_delta": "",
                "is_new_video": False,
                "title_changed": "",
                "description_changed": "",
                "tags_changed": "",
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-4",
                "title": "Evergreen tutorial",
                "upload_date": "2026-03-01T00:38:53+00:00",
                "duration_seconds": 620,
                "views": 900,
                "likes": 80,
                "comments": 40,
                "views_delta": 60,
                "likes_delta": 6,
                "comments_delta": 2,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": "",
            },
        ],
    )

    snap_columns = [
        "execution_date",
        "channel_id",
        "channel_name",
        "video_id",
        "title",
        "description",
        "upload_date",
        "tags",
        "thumbnail_url",
        "duration_seconds",
        "views",
        "likes",
        "comments",
    ]
    _write_csv(
        export_dir / "latest_snapshots.csv",
        snap_columns,
        [
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-1",
                "title": "¿AI 2026 dinero?",
                "description": "d1",
                "upload_date": "2026-04-27T00:38:53+00:00",
                "tags": "[]",
                "thumbnail_url": "https://img/1",
                "duration_seconds": 60,
                "views": 100,
                "likes": 10,
                "comments": 5,
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-2",
                "title": "Tutorial largo",
                "description": "d2",
                "upload_date": "2026-04-18T00:38:53+00:00",
                "tags": "[]",
                "thumbnail_url": "",
                "duration_seconds": 700,
                "views": 200,
                "likes": 20,
                "comments": 10,
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-3",
                "title": "Sin datos",
                "description": "",
                "upload_date": "",
                "tags": "[]",
                "thumbnail_url": "",
                "duration_seconds": "",
                "views": "",
                "likes": "",
                "comments": "",
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-4",
                "title": "Evergreen tutorial",
                "description": "evergreen",
                "upload_date": "2026-03-01T00:38:53+00:00",
                "tags": "[]",
                "thumbnail_url": "https://img/4",
                "duration_seconds": 620,
                "views": 900,
                "likes": 80,
                "comments": 40,
            },
        ],
    )

    delta_columns = [
        "execution_date",
        "video_id",
        "views_delta",
        "likes_delta",
        "comments_delta",
        "previous_views",
        "current_views",
        "previous_likes",
        "current_likes",
        "previous_comments",
        "current_comments",
        "is_new_video",
        "title_changed",
        "description_changed",
        "tags_changed",
    ]
    _write_csv(
        export_dir / "latest_deltas.csv",
        delta_columns,
        [
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "video_id": "v-1",
                "views_delta": 20,
                "likes_delta": 2,
                "comments_delta": 1,
                "previous_views": 80,
                "current_views": 100,
                "previous_likes": 8,
                "current_likes": 10,
                "previous_comments": 4,
                "current_comments": 5,
                "is_new_video": True,
                "title_changed": False,
                "description_changed": True,
                "tags_changed": False,
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "video_id": "v-2",
                "views_delta": 5,
                "likes_delta": 1,
                "comments_delta": 1,
                "previous_views": 195,
                "current_views": 200,
                "previous_likes": 19,
                "current_likes": 20,
                "previous_comments": 9,
                "current_comments": 10,
                "is_new_video": False,
                "title_changed": True,
                "description_changed": False,
                "tags_changed": True,
            },
            {
                "execution_date": "2026-04-28T00:38:53+00:00",
                "video_id": "v-4",
                "views_delta": 60,
                "likes_delta": 6,
                "comments_delta": 2,
                "previous_views": 840,
                "current_views": 900,
                "previous_likes": 74,
                "current_likes": 80,
                "previous_comments": 38,
                "current_comments": 40,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
            },
        ],
    )

    (export_dir / "export_summary.json").write_text(
        json.dumps({"execution_date": "2026-04-28T00:38:53+00:00"}, ensure_ascii=False),
        encoding="utf-8",
    )
    return export_dir


def _prepare_second_export(tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports" / "dt=2026-04-29" / "run=010000Z"
    growth_columns = [
        "execution_date",
        "channel_id",
        "channel_name",
        "video_id",
        "title",
        "upload_date",
        "duration_seconds",
        "views",
        "likes",
        "comments",
        "views_delta",
        "likes_delta",
        "comments_delta",
        "is_new_video",
        "title_changed",
        "description_changed",
        "tags_changed",
        "engagement_rate",
    ]
    _write_csv(
        export_dir / "video_growth_summary.csv",
        growth_columns,
        [
            {
                "execution_date": "2026-04-29T01:00:00+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-1",
                "title": "¿AI 2026 dinero?",
                "upload_date": "2026-04-27T00:38:53+00:00",
                "duration_seconds": 60,
                "views": 130,
                "likes": 13,
                "comments": 6,
                "views_delta": 30,
                "likes_delta": 3,
                "comments_delta": 1,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-29T01:00:00+00:00",
                "channel_id": "ch-1",
                "channel_name": "Canal Uno",
                "video_id": "v-2",
                "title": "Tutorial largo",
                "upload_date": "2026-04-18T00:38:53+00:00",
                "duration_seconds": 700,
                "views": 210,
                "likes": 25,
                "comments": 13,
                "views_delta": 10,
                "likes_delta": 5,
                "comments_delta": 3,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-29T01:00:00+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-3",
                "title": "Nuevo video",
                "upload_date": "2026-04-29T00:00:00+00:00",
                "duration_seconds": 59,
                "views": 20,
                "likes": 2,
                "comments": 1,
                "views_delta": 20,
                "likes_delta": 2,
                "comments_delta": 1,
                "is_new_video": True,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": "",
            },
            {
                "execution_date": "2026-04-29T01:00:00+00:00",
                "channel_id": "ch-2",
                "channel_name": "Canal Dos",
                "video_id": "v-4",
                "title": "Evergreen tutorial",
                "upload_date": "2026-03-01T00:38:53+00:00",
                "duration_seconds": 620,
                "views": 980,
                "likes": 90,
                "comments": 45,
                "views_delta": 80,
                "likes_delta": 10,
                "comments_delta": 5,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": "",
            },
        ],
    )
    _write_csv(
        export_dir / "latest_snapshots.csv",
        ["video_id"],
        [{"video_id": "v-1"}, {"video_id": "v-2"}, {"video_id": "v-3"}, {"video_id": "v-4"}],
    )
    _write_csv(
        export_dir / "latest_deltas.csv",
        ["video_id"],
        [{"video_id": "v-1"}, {"video_id": "v-2"}, {"video_id": "v-3"}, {"video_id": "v-4"}],
    )
    (export_dir / "export_summary.json").write_text(
        json.dumps({"execution_date": "2026-04-29T01:00:00+00:00"}, ensure_ascii=False),
        encoding="utf-8",
    )
    return export_dir


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_build_analytics_fails_with_warning_when_exports_missing(tmp_path: Path) -> None:
    result = build_analytics(data_dir=tmp_path)
    assert result["status"] == "failed"
    assert any("No existe data/exports" in w for w in result["warnings"])


def test_build_analytics_generates_all_outputs(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    result = build_analytics(data_dir=tmp_path)

    assert result["status"] == "success"
    latest = tmp_path / "analytics" / "latest"
    assert (latest / "latest_video_metrics.csv").exists()
    assert (latest / "latest_channel_metrics.csv").exists()
    assert (latest / "latest_title_metrics.csv").exists()
    assert (latest / "latest_video_scores.csv").exists()
    assert (latest / "latest_video_advanced_metrics.csv").exists()
    assert (latest / "latest_channel_advanced_metrics.csv").exists()
    assert (latest / "latest_metric_eligibility.csv").exists()
    assert (latest / "latest_run_metrics.json").exists()
    assert (latest / "analytics_manifest.json").exists()
    assert (tmp_path / "analytics" / "baselines" / "channel_baselines.csv").exists()
    assert (tmp_path / "analytics" / "baselines" / "video_lifecycle_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=daily" / "video_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=weekly" / "video_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=monthly" / "video_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=daily" / "channel_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=weekly" / "channel_metrics.csv").exists()
    assert (tmp_path / "analytics" / "periods" / "grain=monthly" / "channel_metrics.csv").exists()


def test_video_metrics_formulas_and_ranks(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_metrics.csv")
    by_id = {r["video_id"]: r for r in rows}

    assert float(by_id["v-1"]["engagement_rate"]) == 0.15
    assert float(by_id["v-1"]["views_per_day_since_upload"]) == 100.0
    assert by_id["v-1"]["duration_bucket"] == "short"
    assert by_id["v-2"]["duration_bucket"] == "long"
    assert by_id["v-3"]["duration_bucket"] == "unknown"
    assert by_id["v-4"]["growth_rank"] == "1"
    assert by_id["v-1"]["growth_rank"] == "2"
    assert by_id["v-3"]["growth_rank"] == ""


def test_channel_metrics_aggregation(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_channel_metrics.csv")
    by_channel = {r["channel_id"]: r for r in rows}

    assert by_channel["ch-1"]["videos_tracked"] == "2"
    assert by_channel["ch-1"]["new_videos"] == "1"
    assert by_channel["ch-1"]["total_views_delta"] == "25"
    assert by_channel["ch-1"]["top_video_id"] == "v-1"
    assert by_channel["ch-1"]["shorts_count"] == "1"
    assert by_channel["ch-1"]["long_count"] == "1"


def test_title_features_detect_number_question_ai_finance(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_title_metrics.csv")
    row = next(r for r in rows if r["video_id"] == "v-1")

    assert row["has_number"] == "True"
    assert row["has_question"] == "True"
    assert row["has_ai_word"] == "True"
    assert row["has_finance_word"] == "True"


def test_none_and_empty_values_do_not_break(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_metrics.csv")
    row = next(r for r in rows if r["video_id"] == "v-3")
    assert row["views"] == ""
    assert row["engagement_rate"] == ""


def test_manifest_detects_future_feature_files(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    feature_file = tmp_path / "features" / "thumbnails" / "latest_thumbnail_features.csv"
    feature_file.parent.mkdir(parents=True, exist_ok=True)
    feature_file.write_text("x\n", encoding="utf-8")

    build_analytics(data_dir=tmp_path)
    manifest = json.loads((tmp_path / "analytics" / "latest" / "analytics_manifest.json").read_text(encoding="utf-8"))

    assert "features/thumbnails/latest_thumbnail_features.csv" in manifest["future_feature_inputs_found"]


def test_manifest_detects_only_files_inside_data_dir(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    outside_file = tmp_path.parent / "outside_features" / "topics" / "latest_topic_features.csv"
    outside_file.parent.mkdir(parents=True, exist_ok=True)
    outside_file.write_text("outside\n", encoding="utf-8")

    inside_file = tmp_path / "features" / "topics" / "latest_topic_features.csv"
    inside_file.parent.mkdir(parents=True, exist_ok=True)
    inside_file.write_text("inside\n", encoding="utf-8")

    try:
        build_analytics(data_dir=tmp_path)
        manifest = json.loads((tmp_path / "analytics" / "latest" / "analytics_manifest.json").read_text(encoding="utf-8"))
    finally:
        outside_file.unlink(missing_ok=True)

    assert "features/topics/latest_topic_features.csv" in manifest["future_feature_inputs_found"]
    assert manifest["future_feature_inputs_checked"] == [
        "features/thumbnails/latest_thumbnail_features.csv",
        "features/transcripts/latest_transcript_features.csv",
        "features/topics/latest_topic_features.csv",
    ]


def test_build_analytics_writes_only_inside_data_analytics(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    result = build_analytics(data_dir=tmp_path)
    analytics_root = (tmp_path / "analytics").resolve()
    for rel_path in result["outputs"].values():
        assert (tmp_path / rel_path).resolve().is_relative_to(analytics_root)


def test_analytics_service_no_search_list_usage() -> None:
    content = Path("src/ytb_history/services/analytics_service.py").read_text(encoding="utf-8")
    assert "search.list" not in content


def test_percentile_rank_regular_values() -> None:
    assert percentile_rank([10.0, 20.0, 30.0]) == ["0", "50", "100"]


def test_percentile_rank_ties_and_all_equal() -> None:
    tie_scores = percentile_rank([10.0, 10.0, 20.0, None])
    assert tie_scores[0] == tie_scores[1]
    assert tie_scores[3] == ""
    assert percentile_rank([5.0, 5.0]) == ["50", "50"]


def test_robust_z_scores_handles_zero_mad() -> None:
    assert robust_z_scores([5.0, 5.0, 5.0, None]) == ["0", "0", "0", ""]


def test_to_float_string_stable_format_and_no_dead_return_sequence() -> None:
    assert analytics_service._to_float_string(None) == ""
    assert analytics_service._to_float_string(0.15) == "0.15"
    assert analytics_service._to_float_string(1.23456789) == "1.234568"

    content = Path("src/ytb_history/services/analytics_service.py").read_text(encoding="utf-8")
    assert "return str(value)\n    return f\"{value:.6f}\".rstrip(\"0\").rstrip(\".\")" not in content


def test_video_scores_formulas_and_anomaly_method(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_scores.csv")
    row = next(item for item in rows if item["video_id"] == "v-1")

    expected_alpha = (
        0.35 * float(row["growth_percentile"])
        + 0.20 * float(row["relative_growth_percentile"])
        + 0.20 * float(row["engagement_percentile"])
        + 0.15 * float(row["freshness_score"])
        + 0.10 * float(row["metadata_change_score"])
    )
    expected_opportunity = (
        0.50 * expected_alpha
        + 0.25 * float(row["relative_growth_percentile"])
        + 0.15 * float(row["engagement_percentile"])
        + 0.10 * float(row["freshness_score"])
    )
    expected_anomaly = min(abs(float(row["growth_robust_z"])) / 5 * 100, 100)

    assert float(row["alpha_score"]) == pytest.approx(expected_alpha)
    assert float(row["opportunity_score"]) == pytest.approx(expected_opportunity)
    assert float(row["anomaly_score"]) == pytest.approx(expected_anomaly)
    assert row["anomaly_method"] == "robust_z"


def test_channel_baselines_generated_with_growth_percentile(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "baselines" / "channel_baselines.csv")
    row = next(item for item in rows if item["channel_id"] == "ch-1")
    assert row["channel_growth_percentile"] != ""
    assert row["channel_momentum_score"] == row["channel_growth_percentile"]


def test_video_lifecycle_stages_cover_all_ranges(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "baselines" / "video_lifecycle_metrics.csv")
    assert any(r["lifecycle_stage"] == "early" for r in rows)
    assert any(r["lifecycle_stage"] == "active" for r in rows)
    assert any(r["lifecycle_stage"] == "unknown" for r in rows)
    service_text = Path("src/ytb_history/services/analytics_service.py").read_text(encoding="utf-8")
    assert "first_24h" in service_text
    assert "mature" in service_text
    assert "long_tail" in service_text


def test_period_aggregations_daily_weekly_monthly_and_top_video(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)

    daily_videos = _read_csv(tmp_path / "analytics" / "periods" / "grain=daily" / "video_metrics.csv")
    weekly_videos = _read_csv(tmp_path / "analytics" / "periods" / "grain=weekly" / "video_metrics.csv")
    monthly_videos = _read_csv(tmp_path / "analytics" / "periods" / "grain=monthly" / "video_metrics.csv")
    daily_channels = _read_csv(tmp_path / "analytics" / "periods" / "grain=daily" / "channel_metrics.csv")

    assert daily_videos
    assert weekly_videos
    assert monthly_videos
    daily_channel_ch1 = next(row for row in daily_channels if row["channel_id"] == "ch-1")
    assert int(daily_channel_ch1["period_views_delta"]) >= 20
    assert daily_channel_ch1["period_top_video_id"] in {"v-1", "v-2"}


def test_build_analytics_works_with_single_export(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    result = build_analytics(data_dir=tmp_path)
    assert result["status"] == "success"
    assert (tmp_path / "analytics" / "periods" / "grain=monthly" / "video_metrics.csv").exists()


def test_build_analytics_works_with_multiple_historical_exports(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "periods" / "grain=daily" / "video_metrics.csv")
    period_starts = {row["period_start"] for row in rows}
    assert "2026-04-28" in period_starts
    assert "2026-04-29" in period_starts


def test_analytics_manifest_includes_new_outputs_row_counts_and_isolation_forest_flag(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    manifest = json.loads((tmp_path / "analytics" / "latest" / "analytics_manifest.json").read_text(encoding="utf-8"))
    assert "analytics/latest/latest_video_scores.csv" in manifest["outputs"]
    assert "analytics/baselines/channel_baselines.csv" in manifest["outputs"]
    assert "analytics/periods/grain=daily/video_metrics.csv" in manifest["outputs"]
    assert manifest["row_counts"]["analytics/latest/latest_video_scores.csv"] >= 1
    assert manifest["scoring_version"] == "scoring_v1"
    assert manifest["anomaly_method"] == "robust_z"
    assert manifest["isolation_forest_ready"] is False


def test_analytics_service_no_api_usage() -> None:
    content = Path("src/ytb_history/services/analytics_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in content


def test_advanced_video_and_channel_metrics_and_eligibility_generated(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)
    assert (tmp_path / "analytics" / "latest" / "latest_video_advanced_metrics.csv").exists()
    assert (tmp_path / "analytics" / "latest" / "latest_channel_advanced_metrics.csv").exists()
    assert (tmp_path / "analytics" / "latest" / "latest_metric_eligibility.csv").exists()


def test_advanced_horizon_scores_and_eligibility_rules(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)
    adv_rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_advanced_metrics.csv")
    elig_rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_metric_eligibility.csv")
    by_id = {row["video_id"]: row for row in adv_rows}
    elig_by_id = {row["video_id"]: row for row in elig_rows}

    assert by_id["v-1"]["short_term_success_score"] != ""
    assert by_id["v-2"]["mid_term_success_score"] != ""
    assert by_id["v-4"]["long_term_success_score"] != ""
    assert by_id["v-4"]["evergreen_score"] != ""
    assert by_id["v-1"]["trend_burst_score"] != ""
    assert by_id["v-4"]["trend_burst_score"] == ""

    assert elig_by_id["v-1"]["short_term_eligible"] == "True"
    assert elig_by_id["v-2"]["mid_term_eligible"] == "True"
    assert elig_by_id["v-4"]["long_term_eligible"] == "True"


def test_advanced_relative_and_acceleration_metrics(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)
    rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_advanced_metrics.csv")
    row_v1 = next(row for row in rows if row["video_id"] == "v-1")
    assert float(row_v1["channel_relative_growth_ratio"]) == pytest.approx(
        float(row_v1["views_delta"]) / max(float(row_v1["channel_median_views_delta"]), 1)
    )
    assert float(row_v1["format_relative_growth_ratio"]) == pytest.approx(
        float(row_v1["views_delta"]) / max(float(row_v1["format_median_views_delta"]), 1)
    )
    assert row_v1["growth_acceleration_ratio"] != ""
    assert row_v1["growth_trend_label"] in {"accelerating", "stable", "decelerating", "fading", "unknown"}
    assert row_v1["decay_resistance_score"] != ""


def test_packaging_problem_and_confidence_and_channel_advanced_metrics(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    _prepare_second_export(tmp_path)
    build_analytics(data_dir=tmp_path)
    video_rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_video_advanced_metrics.csv")
    channel_rows = _read_csv(tmp_path / "analytics" / "latest" / "latest_channel_advanced_metrics.csv")
    v2 = next(row for row in video_rows if row["video_id"] == "v-2")
    assert float(v2["packaging_problem_score"]) >= 0
    assert float(v2["metric_confidence_score"]) >= 0

    ch1 = next(row for row in channel_rows if row["channel_id"] == "ch-1")
    assert ch1["channel_momentum_7d_vs_30d"] != ""
    assert ch1["channel_consistency_score"] != ""


def test_advanced_manifest_fields_present(tmp_path: Path) -> None:
    _prepare_exports(tmp_path)
    build_analytics(data_dir=tmp_path)
    manifest = json.loads((tmp_path / "analytics" / "latest" / "analytics_manifest.json").read_text(encoding="utf-8"))
    assert "analytics/latest/latest_video_advanced_metrics.csv" in manifest["outputs"]
    assert manifest["advanced_metrics_version"] == "advanced_metrics_v1"
    assert manifest["success_horizons"]["short_term"] == "0-3 days"
    assert manifest["isolation_forest_ready"] is False
