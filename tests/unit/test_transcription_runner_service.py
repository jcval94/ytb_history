from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from ytb_history.services import transcription_runner_service
from ytb_history.services.transcription_runner_service import transcribe_selected_videos
from ytb_history.services.transcript_store_service import write_transcript_artifacts


class FakeAudioClient:
    def __init__(self) -> None:
        self.calls: list[tuple[Path, str]] = []

    def transcribe_file(self, *, file_path: str | Path, model: str) -> str:
        self.calls.append((Path(file_path), model))
        return "texto transcrito"


def _completed(command: list[str], *, stdout: str = "", stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


def _write_queue(data_dir: Path, rows: list[dict[str, Any]]) -> None:
    path = data_dir / "transcripts" / "transcript_queue.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _queue_row(video_id: str, *, forced: bool = False) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "channel_id": "c1",
        "channel_name": "Canal",
        "title": f"Titulo {video_id}",
        "selected_at": "2026-05-01T00:00:00+00:00",
        "selection_source": "forced_channel_new_video" if forced else "ranked_daily_top",
        "forced_channel": forced,
    }


def _registry_rows(data_dir: Path) -> list[dict[str, Any]]:
    registry_path = data_dir / "transcripts" / "transcript_registry.jsonl"
    return [json.loads(line) for line in registry_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_dry_run_does_not_require_api_key_or_call_openai(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda _name: "")
    fake = FakeAudioClient()

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, dry_run=True, openai_client=fake)

    assert report["status"] == "success"
    assert report["processed"] == 1
    assert report["skipped_no_audio_source"] == 1
    assert fake.calls == []


def test_missing_api_key_returns_controlled_skip(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda _name: "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10)

    assert report["status"] == "skipped_missing_api_key"
    assert report["skipped_missing_api_key"] == 1
    assert (tmp_path / "transcripts" / "transcription_run_report.json").exists()


def test_transcribes_local_audio_and_updates_registry(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir()
    (audio_dir / "v1.mp3").write_bytes(b"fake")
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    fake = FakeAudioClient()

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=audio_dir, openai_client=fake)

    assert report["transcribed_success"] == 1
    assert fake.calls == [(audio_dir / "v1.mp3", "gpt-4o-mini-transcribe")]
    assert (tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt").read_text(encoding="utf-8") == "texto transcrito"
    registry_rows = _registry_rows(tmp_path)
    assert registry_rows[0]["status"] == "success"
    assert registry_rows[0]["source_type"] == "audio_file"


def test_extracts_audio_from_existing_video_before_download(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    audio_dir = tmp_path / "audio_sources"
    video_dir = tmp_path / "video_sources"
    video_dir.mkdir()
    (video_dir / "v1.mp4").write_bytes(b"video")
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    monkeypatch.setattr(transcription_runner_service, "_resolve_ffmpeg_executable", lambda: "ffmpeg")
    fake = FakeAudioClient()

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        Path(command[-1]).write_bytes(b"audio")
        return _completed(command)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=audio_dir,
        video_source_dir=video_dir,
        openai_client=fake,
        command_runner=_runner,
    )

    assert report["transcribed_success"] == 1
    assert report["extracted_audio_from_video"] == 1
    assert fake.calls == [(audio_dir / "v1.mp3", "gpt-4o-mini-transcribe")]
    assert _registry_rows(tmp_path)[0]["source_type"] == "video_file_extracted_audio"


def test_downloads_audio_with_ytdlp_when_no_local_media(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    audio_dir = tmp_path / "audio_sources"
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    monkeypatch.setattr(transcription_runner_service, "_resolve_ytdlp_command", lambda: ["yt-dlp"])
    fake = FakeAudioClient()

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        (audio_dir / "v1.mp3").parent.mkdir(parents=True, exist_ok=True)
        (audio_dir / "v1.mp3").write_bytes(b"audio")
        return _completed(command)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=audio_dir,
        openai_client=fake,
        ytdlp_browser="chrome",
        ytdlp_extra_args=["--force-ipv4"],
        command_runner=_runner,
    )

    assert report["transcribed_success"] == 1
    assert report["downloaded_audio_with_ytdlp"] == 1
    assert "--cookies-from-browser" in report["media_resolution_details"][0]["steps"][0]["attempts"][0]["command"]
    assert _registry_rows(tmp_path)[0]["source_type"] == "yt_dlp_audio_download"


def test_skips_no_audio_source_without_download_attempt(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    fake = FakeAudioClient()

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=tmp_path / "missing",
        openai_client=fake,
        allow_ytdlp_fallback=False,
    )

    assert report["skipped_no_audio_source"] == 1
    assert fake.calls == []
    assert _registry_rows(tmp_path)[0]["status"] == "skipped_no_audio_source"


def test_segments_audio_when_openai_rejects_large_input(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir()
    (audio_dir / "v1.mp3").write_bytes(b"large audio")
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    monkeypatch.setattr(transcription_runner_service, "_resolve_ffmpeg_executable", lambda: "ffmpeg")

    class SegmentingClient:
        def __init__(self) -> None:
            self.calls: list[Path] = []

        def transcribe_file(self, *, file_path: str | Path, model: str) -> str:
            path = Path(file_path)
            self.calls.append(path)
            if path.name == "v1.mp3":
                raise RuntimeError("413 request entity too large")
            return f"texto:{path.name}"

    client = SegmentingClient()

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        pattern = str(command[-1])
        Path(pattern.replace("%03d", "000")).write_bytes(b"part0")
        Path(pattern.replace("%03d", "001")).write_bytes(b"part1")
        return _completed(command)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=audio_dir,
        openai_client=client,  # type: ignore[arg-type]
        command_runner=_runner,
    )

    assert report["transcribed_success"] == 1
    assert report["segmented_audio_transcriptions"] == 1
    assert len(client.calls) == 3
    assert (tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt").read_text(encoding="utf-8").startswith("texto:v1_part_000")


def test_skips_already_success(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    (tmp_path / "transcripts").mkdir(exist_ok=True)
    (tmp_path / "transcripts" / "transcript_registry.jsonl").write_text(json.dumps({"video_id": "v1", "status": "success"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")
    fake = FakeAudioClient()

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=fake)

    assert report["skipped_already_transcribed"] == 1
    assert fake.calls == []


def test_skips_persisted_transcript_even_without_registry_success(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1")])
    write_transcript_artifacts(
        video_id="v1",
        transcript_text="ya existe",
        metadata={"source_type": "audio_file"},
        data_dir=tmp_path,
    )
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=FakeAudioClient())

    assert report["skipped_already_transcribed"] == 1


def test_respects_total_limit_without_include_forced(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("v1", forced=True), _queue_row("v2"), _queue_row("v3")])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=2, dry_run=True)

    assert report["selected_count"] == 2
    assert report["processed_video_ids"] == ["v1", "v2"]


def test_include_forced_processes_forced_first_and_respects_ranked_limit(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, [_queue_row("f1", forced=True), _queue_row("f2", forced=True), _queue_row("r1"), _queue_row("r2")])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=99, include_forced=True, ranked_limit=1, dry_run=True)

    assert report["selected_count"] == 3
    assert report["processed_video_ids"] == ["f1", "f2", "r1"]
    assert report["selected_forced_count"] == 2
    assert report["selected_ranked_count"] == 1


def test_transcribe_skips_probable_channel_id_without_api_downloads(tmp_path: Path, monkeypatch) -> None:
    channel_id = "UCWBWgCD4oAqT3hUeq40SCUw"
    _write_queue(tmp_path, [_queue_row(channel_id)])
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda name: "key" if name == "OPENAI_API_KEY" else "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, dry_run=False, openai_client=FakeAudioClient())

    assert report["skipped_invalid_video_id"] == 1
    assert report["processed_video_ids"] == [channel_id]
    registry_rows = _registry_rows(tmp_path)
    assert registry_rows[0]["status"] == "skipped_invalid_video_id"


def test_classifies_ytdlp_failures_without_network_calls() -> None:
    assert transcription_runner_service._classify_ytdlp_error(stderr="Sign in to confirm you're not a bot") == "auth_required"
    assert transcription_runner_service._classify_ytdlp_error(stderr="This video is unavailable") == "unavailable"
    assert transcription_runner_service._classify_ytdlp_error(stderr="HTTP Error 503") == "network"
    assert transcription_runner_service._classify_ytdlp_error(stderr="yt-dlp not found") == "tooling"


def test_transcription_runner_source_keeps_youtube_api_out_of_media_fallback() -> None:
    source = Path("src/ytb_history/services/transcription_runner_service.py").read_text(encoding="utf-8")
    assert "search.list" not in source
    assert "playlistItems.list" not in source
