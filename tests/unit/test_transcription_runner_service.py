from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.transcript_store_service import update_transcript_registry
from ytb_history.services import transcription_runner_service
from ytb_history.services.transcription_runner_service import transcribe_selected_videos


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def transcribe_file(self, *, file_path: str | Path, model: str = "gpt-4o-mini-transcribe") -> str:
        self.calls.append((str(file_path), model))
        return "texto transcrito"


def _write_queue(data_dir: Path, video_ids: list[str]) -> None:
    path = data_dir / "transcripts" / "transcript_queue.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for idx, vid in enumerate(video_ids, start=1):
        lines.append(json.dumps({"video_id": vid, "channel_id": f"c{idx}", "channel_name": "canal", "title": "title", "selected_at": "2026-04-29T00:00:00+00:00"}))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_missing_api_key_returns_skip(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=FakeOpenAIClient())

    assert "skipped_missing_api_key" in report["warnings"]
    assert report["processed"] == 0


def test_transcribes_local_audio_and_updates_registry(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir(parents=True, exist_ok=True)
    (audio_dir / "v1.mp3").write_bytes(b"fake-audio")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=audio_dir, openai_client=fake)

    assert report["transcribed_success"] == 1
    assert len(fake.calls) == 1
    transcript = tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt"
    metadata = tmp_path / "transcripts" / "videos" / "v1" / "transcript_metadata.json"
    assert transcript.exists()
    assert metadata.exists()
    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert registry_rows[-1]["video_id"] == "v1"
    assert registry_rows[-1]["status"] == "success"


def test_skips_no_audio_source(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=tmp_path / "missing", openai_client=FakeOpenAIClient())
    assert report["skipped_no_audio_source"] == 1
    assert report["audio_source_dir_exists"] is False
    assert len(report["missing_audio_details"]) == 1
    detail = report["missing_audio_details"][0]
    assert detail["video_id"] == "v1"
    assert len(detail["attempted_paths"]) >= 1
    assert detail["video_url"].endswith("watch?v=v1")

    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert "attempted=" in str(registry_rows[-1]["error_message"])


def test_uses_ytdlp_fallback_when_local_audio_missing(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    audio_dir = tmp_path / "audio_sources"
    fake_audio = audio_dir / "v1.mp3"

    def _fake_download(*, video_id: str, audio_source_dir: Path):
        audio_source_dir.mkdir(parents=True, exist_ok=True)
        fake_audio.write_bytes(b"audio")
        return fake_audio, None

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)
    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=audio_dir, openai_client=fake)

    assert report["transcribed_success"] == 1
    assert report["skipped_no_audio_source"] == 0
    assert report["ytdlp_download_attempts"] == 1
    assert report["ytdlp_download_success"] == 1


def test_skips_already_success(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    update_transcript_registry(data_dir=tmp_path, entry={"video_id": "v1", "status": "success"})
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=fake)
    assert report["skipped_already_transcribed"] == 1
    assert len(fake.calls) == 0


def test_respects_limit(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1", "v2", "v3"])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir(parents=True, exist_ok=True)
    for vid in ["v1", "v2", "v3"]:
        (audio_dir / f"{vid}.mp3").write_bytes(b"audio")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=2, audio_source_dir=audio_dir, openai_client=fake)
    assert report["processed"] == 2
    assert len(fake.calls) == 2
