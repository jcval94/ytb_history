from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.model_dataset_service import build_model_dataset


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_dataset(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"

    base_fields = [
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
        "comment_rate",
        "video_age_days",
        "duration_bucket",
        "is_short",
        "metadata_changed",
    ]

    _write_csv(
        data_dir / "exports" / "dt=2026-04-01" / "run=000000Z" / "video_growth_summary.csv",
        base_fields,
        [
            {
                "execution_date": "2026-04-01T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "video_id": "v1",
                "title": "AI 2026?",
                "upload_date": "2026-03-30T00:00:00+00:00",
                "duration_seconds": 40,
                "views": 100,
                "likes": 10,
                "comments": 2,
                "views_delta": 20,
                "likes_delta": 2,
                "comments_delta": 1,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": 0.05,
                "comment_rate": 0.01,
                "video_age_days": 2,
                "duration_bucket": "short",
                "is_short": True,
                "metadata_changed": False,
            }
        ],
    )

    _write_csv(
        data_dir / "exports" / "dt=2026-04-08" / "run=000000Z" / "video_growth_summary.csv",
        base_fields,
        [
            {
                "execution_date": "2026-04-08T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "video_id": "v1",
                "title": "AI 2026?",
                "upload_date": "2026-03-30T00:00:00+00:00",
                "duration_seconds": 40,
                "views": 200,
                "likes": 20,
                "comments": 4,
                "views_delta": 80,
                "likes_delta": 5,
                "comments_delta": 2,
                "is_new_video": False,
                "title_changed": False,
                "description_changed": False,
                "tags_changed": False,
                "engagement_rate": 0.06,
                "comment_rate": 0.012,
                "video_age_days": 9,
                "duration_bucket": "short",
                "is_short": True,
                "metadata_changed": False,
            }
        ],
    )

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "alpha_score", "opportunity_score"],
        [{"video_id": "v1", "alpha_score": 70, "opportunity_score": 60}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["video_id", "execution_date", "channel_id", "channel_name", "title", "views_delta", "engagement_rate", "comment_rate", "video_age_days", "duration_bucket", "is_short", "metadata_changed"],
        [
            {
                "video_id": "v1",
                "execution_date": "2026-04-08T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "title": "AI 2026?",
                "views_delta": 80,
                "engagement_rate": 0.06,
                "comment_rate": 0.012,
                "video_age_days": 9,
                "duration_bucket": "short",
                "is_short": True,
                "metadata_changed": False,
            }
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv",
        ["video_id", "trend_burst_score", "evergreen_score", "packaging_problem_score", "metric_confidence_score", "channel_relative_success_score"],
        [{"video_id": "v1", "trend_burst_score": 30, "evergreen_score": 50, "packaging_problem_score": 20, "metric_confidence_score": 80, "channel_relative_success_score": 55}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_momentum_score"],
        [{"channel_id": "c1", "channel_momentum_score": 44}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_title_metrics.csv",
        ["video_id", "title_length_chars", "has_number", "has_question", "has_ai_word", "has_finance_word"],
        [{"video_id": "v1", "title_length_chars": 8, "has_number": True, "has_question": True, "has_ai_word": True, "has_finance_word": False}],
    )

    return data_dir


def test_build_model_dataset_generates_required_outputs(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)

    result = build_model_dataset(data_dir=data_dir)

    assert result["status"] in {"success", "success_with_warnings"}
    modeling = data_dir / "modeling"
    assert (modeling / "supervised_examples.csv").exists()
    assert (modeling / "feature_dictionary.json").exists()
    assert (modeling / "target_dictionary.json").exists()
    assert (modeling / "leakage_audit.json").exists()
    assert (modeling / "model_readiness_report.json").exists()
    assert (modeling / "latest_inference_examples.csv").exists()


def test_build_model_dataset_targets_and_leakage_rules(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)

    build_model_dataset(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "modeling" / "supervised_examples.csv").open("r", encoding="utf-8", newline="")))
    assert rows
    row = rows[0]
    assert "future_log_views_delta_7d" in row
    assert "is_top_growth_7d" in row

    leakage = json.loads((data_dir / "modeling" / "leakage_audit.json").read_text(encoding="utf-8"))
    assert leakage["status"] == "pass"
    assert all(not feature.startswith("future_") for feature in leakage["allowed_features"])
    assert all(feature not in {"is_top_growth_7d", "future_views_delta_7d", "future_log_views_delta_7d"} for feature in leakage["allowed_features"])


def test_build_model_dataset_reports_not_ready_with_few_examples(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)

    build_model_dataset(data_dir=data_dir)

    report = json.loads((data_dir / "modeling" / "model_readiness_report.json").read_text(encoding="utf-8"))
    assert report["recommended_status"] == "not_ready"


def test_build_model_dataset_no_api_no_search_list_and_writes_only_modeling(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)
    existing = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}

    build_model_dataset(data_dir=data_dir)

    source = Path("src/ytb_history/services/model_dataset_service.py").read_text(encoding="utf-8")
    assert "search.list" not in source
    assert "youtube_client" not in source

    updated = {p.relative_to(data_dir) for p in data_dir.rglob("*") if p.is_file()}
    created = updated - existing
    assert created
    assert all(str(path).startswith("modeling/") for path in created)


def test_latest_inference_examples_excludes_targets_and_future_columns(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)

    build_model_dataset(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "modeling" / "latest_inference_examples.csv").open("r", encoding="utf-8", newline="")))
    assert rows
    columns = set(rows[0].keys())
    assert all(not column.startswith("future_") for column in columns)


def test_build_model_dataset_uses_first_intraday_capture_per_day(tmp_path: Path) -> None:
    data_dir = _prepare_dataset(tmp_path)
    fields = [
        "execution_date","channel_id","channel_name","video_id","title","upload_date","duration_seconds","views","likes","comments",
        "views_delta","likes_delta","comments_delta","is_new_video","title_changed","description_changed","tags_changed","engagement_rate",
        "comment_rate","video_age_days","duration_bucket","is_short","metadata_changed",
    ]
    _write_csv(
        data_dir / "exports" / "dt=2026-04-08" / "run=235959Z" / "video_growth_summary.csv",
        fields,
        [{**{k: "" for k in fields}, "execution_date": "2026-04-08T23:59:59+00:00", "channel_id": "c1", "channel_name": "Canal 1", "video_id": "v1", "title": "late", "views_delta": 999}],
    )
    build_model_dataset(data_dir=data_dir)
    rows = list(csv.DictReader((data_dir / "modeling" / "supervised_examples.csv").open("r", encoding="utf-8", newline="")))
    assert rows
    assert all(row["source_export_path"] != "exports/dt=2026-04-08/run=235959Z/video_growth_summary.csv" for row in rows)
