from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services import transcript_timestamp_backfill_service
from ytb_history.services.transcript_store_service import update_transcript_registry, write_transcript_artifacts
from ytb_history.services.transcript_timestamp_backfill_service import backfill_transcript_timestamps


class FakeSegmentClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def transcribe_file_with_segments(self, *, file_path: str | Path, model: str = "whisper-1") -> dict:
        self.calls.append((str(file_path), model))
        return {
            "text": "texto nuevo",
            "duration": 3.0,
            "segments": [
                {"start": 0.0, "end": 1.25, "text": "primer bloque"},
                {"start": 1.25, "end": 3.0, "text": "segundo bloque"},
            ],
        }


def _seed_transcript(data_dir: Path, video_id: str, *, audio_dir: Path | None = None) -> Path:
    audio_dir = audio_dir or data_dir / "audio_sources"
    audio_dir.mkdir(parents=True, exist_ok=True)
    audio_path = audio_dir / f"{video_id}.mp3"
    audio_path.write_bytes(b"audio")
    paths = write_transcript_artifacts(
        video_id=video_id,
        transcript_text="texto original",
        metadata={
            "channel_id": "ch1",
            "channel_name": "Canal",
            "title": "Video",
            "source_type": "audio_file",
            "source_uri_or_path": str(audio_path),
            "transcription_model": "gpt-4o-mini-transcribe",
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
            "insights_path": paths["insights_path"],
            "source_type": "audio_file",
            "transcription_model": "gpt-4o-mini-transcribe",
            "text_char_count": len("texto original"),
        },
    )
    return audio_path


def test_backfill_generates_segments_without_modifying_transcript(tmp_path: Path, monkeypatch) -> None:
    audio_dir = tmp_path / "audio_sources"
    _seed_transcript(tmp_path, "v1", audio_dir=audio_dir)
    monkeypatch.setattr(transcript_timestamp_backfill_service, "resolve_environment_variable", lambda _name: "test-key")

    client = FakeSegmentClient()
    report = backfill_transcript_timestamps(
        data_dir=tmp_path,
        audio_source_dir=audio_dir,
        limit=10,
        openai_client=client,
    )

    assert report["generated"] == 1
    assert report["success_video_ids"] == ["v1"]
    assert (tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt").read_text(encoding="utf-8") == "texto original"
    segments = [
        json.loads(line)
        for line in (tmp_path / "transcripts" / "videos" / "v1" / "transcript_segments.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert segments[0]["start_seconds"] == 0.0
    assert segments[0]["end_seconds"] == 1.25
    assert segments[0]["text"] == "primer bloque"
    metadata = json.loads((tmp_path / "transcripts" / "videos" / "v1" / "transcript_metadata.json").read_text(encoding="utf-8"))
    assert metadata["repo_schema_version"] == "transcript_metadata_v2"
    assert metadata["segment_count"] == 2
    assert metadata["timestamp_model"] == "whisper-1"
    registry_rows = [
        json.loads(line)
        for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert registry_rows[-1]["segments_path"].endswith("transcript_segments.jsonl")
    assert registry_rows[-1]["transcription_model"] == "gpt-4o-mini-transcribe"
    assert client.calls == [(str(audio_dir / "v1.mp3"), "whisper-1")]


def test_backfill_skips_existing_segments_unless_forced(tmp_path: Path, monkeypatch) -> None:
    audio_dir = tmp_path / "audio_sources"
    _seed_transcript(tmp_path, "v1", audio_dir=audio_dir)
    segments_path = tmp_path / "transcripts" / "videos" / "v1" / "transcript_segments.jsonl"
    segments_path.write_text(json.dumps({"video_id": "v1", "segment_index": 0, "text": "old"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(transcript_timestamp_backfill_service, "resolve_environment_variable", lambda _name: "test-key")

    client = FakeSegmentClient()
    report = backfill_transcript_timestamps(
        data_dir=tmp_path,
        audio_source_dir=audio_dir,
        limit=10,
        openai_client=client,
    )

    assert report["generated"] == 0
    assert report["skipped_existing_segments"] == 1
    assert client.calls == []

    forced_report = backfill_transcript_timestamps(
        data_dir=tmp_path,
        audio_source_dir=audio_dir,
        limit=10,
        force=True,
        openai_client=client,
    )

    assert forced_report["generated"] == 1
    assert len(client.calls) == 1


def test_backfill_continues_when_one_video_is_missing_audio(tmp_path: Path, monkeypatch) -> None:
    audio_dir = tmp_path / "audio_sources"
    _seed_transcript(tmp_path, "v1", audio_dir=audio_dir)
    _seed_transcript(tmp_path, "v2", audio_dir=audio_dir)
    (audio_dir / "v1.mp3").unlink()
    metadata_path = tmp_path / "transcripts" / "videos" / "v1" / "transcript_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["source_uri_or_path"] = str(audio_dir / "missing.mp3")
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(transcript_timestamp_backfill_service, "resolve_environment_variable", lambda _name: "test-key")

    report = backfill_transcript_timestamps(
        data_dir=tmp_path,
        audio_source_dir=audio_dir,
        limit=10,
        openai_client=FakeSegmentClient(),
    )

    assert report["status"] == "partial_success"
    assert report["processed"] == 2
    assert report["generated"] == 1
    assert report["skipped_missing_audio"] == 1
    assert report["success_video_ids"] == ["v2"]
    assert report["failed_details"][0]["video_id"] == "v1"
