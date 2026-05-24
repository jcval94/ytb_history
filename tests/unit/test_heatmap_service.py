from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ytb_history.services import heatmap_service
from ytb_history.services.heatmap_service import extract_heatmaps
from ytb_history.services.transcript_store_service import update_transcript_registry, write_transcript_artifacts


def _write_video_metrics(data_dir: Path, rows: list[dict[str, object]]) -> None:
    path = data_dir / "analytics" / "latest" / "latest_video_metrics.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["video_id", "channel_id", "channel_name", "title", "upload_date", "duration_seconds"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _seed_transcript(data_dir: Path, video_id: str) -> None:
    paths = write_transcript_artifacts(
        video_id=video_id,
        transcript_text="hola mundo",
        metadata={
            "channel_id": "ch1",
            "channel_name": "Canal",
            "title": f"Video {video_id}",
            "source_type": "manual",
        },
        data_dir=data_dir,
    )
    update_transcript_registry(
        data_dir=data_dir,
        entry={
            "video_id": video_id,
            "status": "success",
            "transcript_path": paths["transcript_path"],
            "metadata_path": paths["metadata_path"],
        },
    )


def _install_fake_ytdlp(monkeypatch: pytest.MonkeyPatch, responses: dict[str, object]) -> None:
    class FakeYoutubeDL:
        def __init__(self, params: dict[str, object]) -> None:
            self.params = params

        def __enter__(self) -> "FakeYoutubeDL":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def extract_info(self, url: str, download: bool = False) -> dict[str, object]:
            assert download is False
            video_id = url.rsplit("=", 1)[-1]
            response = responses[video_id]
            if isinstance(response, Exception):
                raise response
            return response  # type: ignore[return-value]

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))


def _days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def test_extract_heatmaps_only_attempts_transcribed_videos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_transcript(tmp_path, "v-transcribed")
    _write_video_metrics(
        tmp_path,
        [
            {
                "video_id": "v-transcribed",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "Transcrito",
                "upload_date": _days_ago(15),
                "duration_seconds": 100,
            },
            {
                "video_id": "v-not-transcribed",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "No transcrito",
                "upload_date": _days_ago(15),
                "duration_seconds": 100,
            },
        ],
    )
    _install_fake_ytdlp(
        monkeypatch,
        {
            "v-transcribed": {
                "heatmap": [
                    {"start_time": 0, "end_time": 10, "value": 0.1},
                    {"start_time": 10, "end_time": 20, "value": 0.8},
                ]
            }
        },
    )

    report = extract_heatmaps(data_dir=tmp_path, limit=10)

    assert report["attempted_count"] == 1
    assert report["status_counts"] == {"success": 1}
    assert report["selection_counters"]["skipped_not_transcribed"] == 1
    rows = [
        json.loads(line)
        for line in (tmp_path / "heatmaps" / "heatmap_registry.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["video_id"] for row in rows] == ["v-transcribed"]
    assert (tmp_path / "heatmaps" / "videos" / "v-transcribed" / "heatmap_segments.jsonl").exists()
    metadata = json.loads(
        (tmp_path / "transcripts" / "videos" / "v-transcribed" / "transcript_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["heatmap_available"] is True
    assert "heatmap_sha256" in metadata


def test_heatmap_bucket_filter_uses_video_age(tmp_path: Path) -> None:
    _seed_transcript(tmp_path, "v-bucket")
    _write_video_metrics(
        tmp_path,
        [
            {
                "video_id": "v-bucket",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "Bucket",
                "upload_date": _days_ago(15),
                "duration_seconds": 100,
            }
        ],
    )

    skipped = extract_heatmaps(data_dir=tmp_path, bucket="1w", dry_run=True)
    selected = extract_heatmaps(data_dir=tmp_path, bucket="2w", dry_run=True)

    assert skipped["attempted_count"] == 0
    assert selected["attempted_count"] == 1


def test_heatmap_success_skips_until_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_transcript(tmp_path, "v-success")
    _write_video_metrics(
        tmp_path,
        [
            {
                "video_id": "v-success",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "Success",
                "upload_date": _days_ago(30),
                "duration_seconds": 100,
            }
        ],
    )
    _install_fake_ytdlp(monkeypatch, {"v-success": {"heatmap": [{"start_time": 0, "end_time": 5, "value": 1}]}})

    first = extract_heatmaps(data_dir=tmp_path, limit=10)
    skipped = extract_heatmaps(data_dir=tmp_path, limit=10, dry_run=True)
    forced = extract_heatmaps(data_dir=tmp_path, limit=10, dry_run=True, force=True)

    assert first["status_counts"] == {"success": 1}
    assert skipped["attempted_count"] == 0
    assert skipped["selection_counters"]["skipped_already_success"] == 1
    assert forced["attempted_count"] == 1


def test_heatmap_failures_are_registered_for_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_transcript(tmp_path, "v-missing")
    _write_video_metrics(
        tmp_path,
        [
            {
                "video_id": "v-missing",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "Missing",
                "upload_date": _days_ago(15),
                "duration_seconds": 100,
            }
        ],
    )
    _install_fake_ytdlp(monkeypatch, {"v-missing": {"title": "No heatmap yet"}})

    report = extract_heatmaps(data_dir=tmp_path, limit=10)

    assert report["status_counts"] == {"not_available_yet": 1}
    registry = [
        json.loads(line)
        for line in (tmp_path / "heatmaps" / "heatmap_registry.jsonl").read_text(encoding="utf-8").splitlines()
    ][0]
    assert registry["last_status"] == "not_available_yet"
    assert registry["next_retry_after"]
    assert registry["heatmap_available"] is False


def test_heatmap_invalid_shape_is_registered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_transcript(tmp_path, "v-invalid")
    _write_video_metrics(
        tmp_path,
        [
            {
                "video_id": "v-invalid",
                "channel_id": "ch1",
                "channel_name": "Canal",
                "title": "Invalid",
                "upload_date": _days_ago(15),
                "duration_seconds": 100,
            }
        ],
    )
    _install_fake_ytdlp(monkeypatch, {"v-invalid": {"heatmap": [{"start_time": "bad", "value": 1}]}})

    report = extract_heatmaps(data_dir=tmp_path, limit=10)

    assert report["status_counts"] == {"invalid_heatmap": 1}


def test_heatmap_storage_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        heatmap_service._safe_video_id("../escape")
