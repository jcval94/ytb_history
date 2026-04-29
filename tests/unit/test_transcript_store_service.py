from __future__ import annotations

import json
from pathlib import Path

from ytb_history import cli
from ytb_history.services.transcript_store_service import (
    build_transcript_registry_report,
    transcript_exists,
    update_transcript_registry,
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


def test_transcript_registry_report_counts_statuses(tmp_path: Path) -> None:
    entries = [
        {"video_id": "v1", "status": "queued"},
        {"video_id": "v2", "status": "failed"},
        {"video_id": "v3", "status": "success"},
    ]
    for entry in entries:
        update_transcript_registry(data_dir=tmp_path, entry=entry)
    report = build_transcript_registry_report(data_dir=tmp_path)
    assert report["queued_count"] == 1
    assert report["failed_count"] == 1
    assert report["success_count"] == 1


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
