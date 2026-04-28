from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.analytics_service import build_analytics


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
        ],
    )

    (export_dir / "export_summary.json").write_text(
        json.dumps({"execution_date": "2026-04-28T00:38:53+00:00"}, ensure_ascii=False),
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
    assert (latest / "latest_run_metrics.json").exists()
    assert (latest / "analytics_manifest.json").exists()


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
    assert by_id["v-1"]["growth_rank"] == "1"
    assert by_id["v-2"]["growth_rank"] == "2"
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
