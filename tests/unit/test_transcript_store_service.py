from __future__ import annotations

import json
from pathlib import Path

from ytb_history import cli
from ytb_history.services.transcript_store_service import (
    build_transcript_registry_report,
    list_transcribed_video_ids,
    transcript_exists,
    update_transcript_metadata_with_timestamps,
    update_transcript_registry,
    update_transcript_registry_timestamp_metadata,
    write_transcript_segments,
    write_transcript_artifacts,
)


def test_transcript_store_writes_artifacts_and_registry(tmp_path: Path) -> None:
    paths = write_transcript_artifacts(
        video_id="abc123",
        transcript_text="hola mundo",
        metadata={
            "channel_id": "ch1",
            "channel_name": "Canal 1",
            "title": "Video 1",
            "source_type": "manual",
            "language": "es",
            "transcription_model": None,
        },
        data_dir=tmp_path,
    )

    transcript_path = Path(paths["transcript_path"])
    metadata_path = Path(paths["metadata_path"])
    insights_path = Path(paths["insights_path"])

    assert transcript_path.exists()
    assert transcript_path.read_text(encoding="utf-8") == "hola mundo"
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["video_id"] == "abc123"
    assert metadata["text_char_count"] == len("hola mundo")
    assert insights_path.exists()
    insights = json.loads(insights_path.read_text(encoding="utf-8"))
    assert insights["status"] == "not_generated"

    update_transcript_registry(
        data_dir=tmp_path,
        entry={
            "video_id": "abc123",
            "channel_id": "ch1",
            "channel_name": "Canal 1",
            "title": "Video 1",
            "selected_at": "2026-04-29T00:00:00+00:00",
            "transcribed_at": metadata["transcribed_at"],
            "status": "success",
            "transcript_path": paths["transcript_path"],
            "metadata_path": paths["metadata_path"],
            "insights_path": paths["insights_path"],
            "source_type": "manual",
            "text_char_count": metadata["text_char_count"],
        },
    )

    assert transcript_exists("abc123", data_dir=tmp_path)

    report = build_transcript_registry_report(data_dir=tmp_path)
    assert report["success_count"] == 1


def test_transcript_store_writes_segments_and_preserves_registry_fields(tmp_path: Path) -> None:
    paths = write_transcript_artifacts(
        video_id="abc123",
        transcript_text="hola mundo",
        metadata={"source_type": "manual", "transcription_model": "gpt-4o-mini-transcribe"},
        data_dir=tmp_path,
    )
    update_transcript_registry(
        data_dir=tmp_path,
        entry={
            "video_id": "abc123",
            "status": "success",
            "transcript_path": paths["transcript_path"],
            "metadata_path": paths["metadata_path"],
            "transcription_model": "gpt-4o-mini-transcribe",
        },
    )

    segments_path = write_transcript_segments(
        video_id="abc123",
        segments=[
            {"start_seconds": 0, "end_seconds": 1.5, "text": "hola", "transcription_model": "whisper-1"},
        ],
        data_dir=tmp_path,
    )
    metadata = update_transcript_metadata_with_timestamps(
        video_id="abc123",
        data_dir=tmp_path,
        segments_path=segments_path,
        segment_count=1,
        timestamp_granularity="segment",
        timestamp_model="whisper-1",
        timestamps_generated_at="2026-05-20T00:00:00+00:00",
        duration_seconds=1.5,
    )
    registry_row = update_transcript_registry_timestamp_metadata(
        data_dir=tmp_path,
        video_id="abc123",
        segments_path=segments_path,
        segment_count=1,
        timestamp_granularity="segment",
        timestamp_model="whisper-1",
        timestamps_generated_at="2026-05-20T00:00:00+00:00",
    )

    segment_rows = [json.loads(line) for line in Path(segments_path).read_text(encoding="utf-8").splitlines()]
    assert segment_rows[0]["start_seconds"] == 0.0
    assert segment_rows[0]["end_seconds"] == 1.5
    assert metadata["repo_schema_version"] == "transcript_metadata_v2"
    assert metadata["duration_seconds"] == 1.5
    assert registry_row["segments_path"] == segments_path
    assert registry_row["transcription_model"] == "gpt-4o-mini-transcribe"


def test_transcript_registry_report_counts_statuses(tmp_path: Path) -> None:
    entries = [
        {"video_id": "v1", "status": "queued"},
        {"video_id": "v2", "status": "failed"},
        {"video_id": "v3", "status": "success"},
        {"video_id": "v4", "status": "skipped_missing_ytdlp"},
        {"video_id": "v4b", "status": "skipped_invalid_video_id", "error_category": "invalid_video_id"},
        {"video_id": "v5", "status": "failed_audio_download"},
        {"video_id": "v6", "status": "failed_audio_download_auth_required", "error_category": "auth_required"},
        {"video_id": "v7", "status": "failed_audio_download_video_unavailable", "error_category": "video_unavailable"},
        {"video_id": "v8", "status": "failed_audio_download_network_or_rate_limit", "error_category": "network_or_rate_limit"},
    ]
    for entry in entries:
        update_transcript_registry(data_dir=tmp_path, entry=entry)
    report = build_transcript_registry_report(data_dir=tmp_path)
    assert report["queued_count"] == 1
    assert report["failed_count"] == 1
    assert report["success_count"] == 1
    assert report["skipped_missing_ytdlp_count"] == 1
    assert report["skipped_invalid_video_id_count"] == 1
    assert report["failed_audio_download_count"] == 1
    assert report["failed_audio_download_auth_required_count"] == 1
    assert report["failed_audio_download_video_unavailable_count"] == 1
    assert report["failed_audio_download_network_or_rate_limit_count"] == 1
    assert report["error_category_counts"]["auth_required"] == 1
    assert report["error_category_counts"]["invalid_video_id"] == 1
    assert report["error_category_counts"]["video_unavailable"] == 1
    assert report["error_category_counts"]["network_or_rate_limit"] == 1
    assert report["skipped_no_audio_source_count"] == 0



def test_failed_transcript_registry_entries_get_failed_at(tmp_path: Path) -> None:
    row = update_transcript_registry(
        data_dir=tmp_path,
        entry={
            "video_id": "v-auth",
            "status": "failed_audio_download_auth_required",
            "error_category": "auth_required",
        },
    )

    assert row["failed_at"]
    assert row["status"] == "failed_audio_download_auth_required"


def test_success_transcript_registry_entries_do_not_get_failed_at(tmp_path: Path) -> None:
    row = update_transcript_registry(data_dir=tmp_path, entry={"video_id": "v-ok", "status": "success"})

    assert row["failed_at"] is None


def test_transcript_exists_and_list_transcribed_ids_detect_persisted_artifacts_without_registry_success(tmp_path: Path) -> None:
    write_transcript_artifacts(
        video_id="artifact_only",
        transcript_text="texto ya persistido",
        metadata={
            "channel_id": "ch1",
            "channel_name": "Canal 1",
            "title": "Video 1",
            "source_type": "audio_file",
        },
        data_dir=tmp_path,
    )
    update_transcript_registry(data_dir=tmp_path, entry={"video_id": "artifact_only", "status": "failed"})

    assert transcript_exists("artifact_only", data_dir=tmp_path)
    assert "artifact_only" in list_transcribed_video_ids(data_dir=tmp_path)

def test_transcript_store_does_not_write_outside_transcripts(tmp_path: Path) -> None:
    try:
        write_transcript_artifacts(video_id="../escape", transcript_text="x", metadata={}, data_dir=tmp_path)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for unsafe video_id")


def test_transcript_registry_report_cli_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "build_transcript_registry_report", lambda **kwargs: {"status": "ok", "data_dir": kwargs["data_dir"]})
    monkeypatch.setattr("sys.argv", ["ytb_history", "transcript-registry-report", "--data-dir", "data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "ok"
