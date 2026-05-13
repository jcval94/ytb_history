from __future__ import annotations

import json
import subprocess
from pathlib import Path

from ytb_history.services.transcript_store_service import update_transcript_registry
from ytb_history.services import transcription_runner_service
from ytb_history.services.transcription_runner_service import transcribe_selected_videos
from ytb_history.services.transcript_store_service import write_transcript_artifacts


def _strategy_name_from_ytdlp_cmd(cmd: list[str]) -> str:
    if "--extractor-args" not in cmd:
        return "default"
    extractor_args = cmd[cmd.index("--extractor-args") + 1]
    return extractor_args.removeprefix("youtube:player_client=")


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
    monkeypatch.setattr(transcription_runner_service, "resolve_environment_variable", lambda _name: "")

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=FakeOpenAIClient())

    assert "skipped_missing_api_key" in report["warnings"]
    assert report["processed"] == 0


def test_reads_api_key_from_windows_persistent_env_fallback(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir(parents=True, exist_ok=True)
    (audio_dir / "v1.mp3").write_bytes(b"fake-audio")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(
        transcription_runner_service,
        "resolve_environment_variable",
        lambda name: "persisted-key" if name == "OPENAI_API_KEY" else "",
    )

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=audio_dir, openai_client=fake)

    assert report["transcribed_success"] == 1
    assert len(fake.calls) == 1


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
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: None)

    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=tmp_path / "missing", openai_client=FakeOpenAIClient())
    assert report["skipped_no_audio_source"] == 0
    assert report["skipped_missing_ytdlp"] == 1
    assert report["failed_audio_download"] == 0
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

    def _fake_download(*, video_id: str, audio_source_dir: Path, **_kwargs):
        audio_source_dir.mkdir(parents=True, exist_ok=True)
        fake_audio.write_bytes(b"audio")
        return fake_audio, None, None

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)
    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=audio_dir, openai_client=fake)

    assert report["transcribed_success"] == 1
    assert report["skipped_no_audio_source"] == 0
    assert report["ytdlp_download_attempts"] == 1
    assert report["ytdlp_download_success"] == 1
    assert report["media_resolution_counts"]["downloaded_ytdlp"] == 1
    assert report["ytdlp_runtime_options"]["used_cookies_file"] is False
    assert report["ytdlp_runtime_options"]["used_browser_mode"] is False
    assert report["ytdlp_runtime_options"]["extra_args_count"] == 0


def test_extracts_audio_from_local_video_before_ytdlp(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    video_dir = tmp_path / "video_sources"
    audio_dir = tmp_path / "audio_sources"
    video_dir.mkdir(parents=True)
    video_path = video_dir / "v1.mp4"
    video_path.write_bytes(b"video")
    extracted_audio = audio_dir / "v1.mp3"

    def _fake_extract(*, video_path: Path, audio_source_dir: Path, video_id: str):
        audio_source_dir.mkdir(parents=True, exist_ok=True)
        extracted_audio.write_bytes(b"audio")
        return extracted_audio, None, None

    def _fail_ytdlp(**_kwargs):
        raise AssertionError("yt-dlp should not run when local video extraction succeeds")

    monkeypatch.setattr(transcription_runner_service, "_extract_audio_from_video", _fake_extract)
    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fail_ytdlp)

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=audio_dir,
        video_source_dir=video_dir,
        openai_client=fake,
    )

    assert report["transcribed_success"] == 1
    assert report["media_resolution_counts"]["extracted_from_video"] == 1
    assert fake.calls == [(str(extracted_audio), "gpt-4o-mini-transcribe")]


def test_transient_openai_errors_are_retried(tmp_path: Path, monkeypatch) -> None:
    class APIConnectionError(Exception):
        pass

    class FlakyClient:
        def __init__(self) -> None:
            self.calls = 0

        def transcribe_file(self, *, file_path: str | Path, model: str = "gpt-4o-mini-transcribe") -> str:
            self.calls += 1
            if self.calls == 1:
                raise APIConnectionError("Connection error.")
            return "texto tras retry"

    _write_queue(tmp_path, ["v1"])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir(parents=True)
    (audio_dir / "v1.mp3").write_bytes(b"audio")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(transcription_runner_service.time, "sleep", lambda _seconds: None)

    client = FlakyClient()
    report = transcribe_selected_videos(data_dir=tmp_path, audio_source_dir=audio_dir, openai_client=client)

    assert report["transcribed_success"] == 1
    assert client.calls == 2


def test_input_too_large_audio_is_segmented(tmp_path: Path, monkeypatch) -> None:
    class SegmentingClient:
        def __init__(self, original_audio: Path) -> None:
            self.original_audio = original_audio
            self.calls: list[str] = []

        def transcribe_file(self, *, file_path: str | Path, model: str = "gpt-4o-mini-transcribe") -> str:
            path = Path(file_path)
            self.calls.append(path.name)
            if path == self.original_audio:
                raise RuntimeError("input_too_large: audio is too large for this model")
            return f"text:{path.stem}"

    _write_queue(tmp_path, ["v1"])
    audio_dir = tmp_path / "audio_sources"
    audio_dir.mkdir(parents=True)
    original_audio = audio_dir / "v1.mp3"
    original_audio.write_bytes(b"audio")
    segment_a = tmp_path / "segment_000.mp3"
    segment_b = tmp_path / "segment_001.mp3"
    segment_a.write_bytes(b"a")
    segment_b.write_bytes(b"b")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        transcription_runner_service,
        "_segment_audio_file",
        lambda **_kwargs: ([segment_a, segment_b], None, None),
    )

    client = SegmentingClient(original_audio)
    report = transcribe_selected_videos(data_dir=tmp_path, audio_source_dir=audio_dir, openai_client=client)

    assert report["transcribed_success"] == 1
    assert report["segmented_transcriptions"] == 1
    assert client.calls == ["v1.mp3", "segment_000.mp3", "segment_001.mp3"]
    transcript = tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt"
    assert transcript.read_text(encoding="utf-8") == "text:segment_000\n\ntext:segment_001"


def test_download_audio_with_ytdlp_includes_preferred_audio_format(tmp_path: Path, monkeypatch) -> None:
    captured_cmd: list[str] = []

    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)

    def _fake_run(cmd: list[str], **kwargs):
        captured_cmd.extend(cmd)
        assert kwargs == {"capture_output": True, "text": True, "check": False}
        (tmp_path / "audio" / "v1.mp3").write_bytes(b"audio")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stderr="")

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
    )

    assert audio_path == tmp_path / "audio" / "v1.mp3"
    assert error is None
    assert error_category is None
    assert "--format" in captured_cmd
    assert captured_cmd[captured_cmd.index("--format") + 1] == "bestaudio[ext=m4a]/bestaudio/best"
    assert "-x" in captured_cmd
    assert captured_cmd[captured_cmd.index("--audio-format") + 1] == "mp3"
    assert captured_cmd[captured_cmd.index("--audio-quality") + 1] == "5"
    assert "--extractor-args" not in captured_cmd
    assert captured_cmd[-1] == "https://www.youtube.com/watch?v=v1"


def test_download_audio_with_ytdlp_uses_module_fallback_when_binary_missing(tmp_path: Path, monkeypatch) -> None:
    captured_cmd: list[str] = []

    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        transcription_runner_service.importlib.util,
        "find_spec",
        lambda name: object() if name == "yt_dlp" else None,
    )
    monkeypatch.setattr(transcription_runner_service.sys, "executable", "C:/Python313/python.exe")

    def _fake_run(cmd: list[str], **kwargs):
        captured_cmd.extend(cmd)
        assert kwargs == {"capture_output": True, "text": True, "check": False}
        (tmp_path / "audio" / "v1.mp3").write_bytes(b"audio")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stderr="")

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
    )

    assert audio_path == tmp_path / "audio" / "v1.mp3"
    assert error is None
    assert error_category is None
    assert captured_cmd[:3] == ["C:/Python313/python.exe", "-m", "yt_dlp"]


def test_download_audio_with_ytdlp_passes_ffmpeg_location_when_resolved(tmp_path: Path, monkeypatch) -> None:
    captured_cmd: list[str] = []

    monkeypatch.setattr(transcription_runner_service, "_resolve_ytdlp_command", lambda: ["/usr/bin/yt-dlp"])
    monkeypatch.setattr(transcription_runner_service, "_resolve_ffmpeg_location", lambda: "/tools/ffmpeg/bin/ffmpeg")

    def _fake_run(cmd: list[str], **kwargs):
        captured_cmd.extend(cmd)
        assert kwargs == {"capture_output": True, "text": True, "check": False}
        (tmp_path / "audio" / "v1.mp3").write_bytes(b"audio")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stderr="")

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
    )

    assert audio_path == tmp_path / "audio" / "v1.mp3"
    assert error is None
    assert error_category is None
    assert "--ffmpeg-location" in captured_cmd
    assert captured_cmd[captured_cmd.index("--ffmpeg-location") + 1] == "/tools/ffmpeg/bin/ffmpeg"


def test_ytdlp_auth_required_with_cookies_continues_to_later_strategy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)
    monkeypatch.setattr(transcription_runner_service.time, "sleep", lambda _seconds: None)
    attempted_strategies: list[str] = []

    def _fake_run(cmd: list[str], **_kwargs):
        strategy_name = _strategy_name_from_ytdlp_cmd(cmd)
        attempted_strategies.append(strategy_name)
        assert "--cookies" in cmd
        assert cmd[cmd.index("--cookies") + 1] == "/tmp/cookies.txt"
        if strategy_name == "mweb":
            (tmp_path / "audio" / "v1.mp3").write_bytes(b"audio")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stderr="")
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: Sign in to confirm you're not a bot. Use --cookies",
        )

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
        ytdlp_cookies_file="/tmp/cookies.txt",
    )

    assert attempted_strategies == ["default", "android", "ios", "mweb"]
    assert audio_path == tmp_path / "audio" / "v1.mp3"
    assert error is None
    assert error_category is None


def test_ytdlp_auth_required_with_cookies_exhausts_all_strategies(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)
    monkeypatch.setattr(transcription_runner_service.time, "sleep", lambda _seconds: None)
    attempted_strategies: list[str] = []

    def _fake_run(cmd: list[str], **_kwargs):
        attempted_strategies.append(_strategy_name_from_ytdlp_cmd(cmd))
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: Sign in to confirm you're not a bot. Use --cookies",
        )

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
        ytdlp_cookies_file="/tmp/cookies.txt",
    )

    assert attempted_strategies == [name for name, _args in transcription_runner_service._ytdlp_download_strategies()]
    assert audio_path is None
    assert error is not None and "strategy=web" in error
    assert error_category == "auth_required"


def test_ytdlp_auth_required_without_auth_context_stops_after_first_strategy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)
    attempted_strategies: list[str] = []

    def _fake_run(cmd: list[str], **_kwargs):
        attempted_strategies.append(_strategy_name_from_ytdlp_cmd(cmd))
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: Sign in to confirm you're not a bot. Use --cookies",
        )

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
    )

    assert attempted_strategies == ["default"]
    assert audio_path is None
    assert error is not None and "strategy=default" in error
    assert error_category == "auth_required"


def test_ytdlp_video_unavailable_stops_after_first_strategy_even_with_cookies(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)
    attempted_strategies: list[str] = []

    def _fake_run(cmd: list[str], **_kwargs):
        attempted_strategies.append(_strategy_name_from_ytdlp_cmd(cmd))
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: Video unavailable",
        )

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
        ytdlp_cookies_file="/tmp/cookies.txt",
    )

    assert attempted_strategies == ["default"]
    assert audio_path is None
    assert error is not None and "strategy=default" in error
    assert error_category == "video_unavailable"


def test_ytdlp_requested_format_unavailable_is_reported(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)

    def _fake_run(cmd: list[str], **_kwargs):
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: Requested format is not available",
        )

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=tmp_path / "audio",
        openai_client=FakeOpenAIClient(),
    )

    assert report["failed_audio_download"] == 1
    assert report["ytdlp_download_failures"] == [
        {
            "video_id": "v1",
            "error": "yt_dlp_failed:strategy=web;code=1;stderr=ERROR: [youtube] v1: Requested format is not available",
            "error_category": "format_unavailable",
            "video_url": "https://www.youtube.com/watch?v=v1",
        }
    ]

    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert registry_rows[-1]["error_category"] == "format_unavailable"


def test_ytdlp_strategy_retries_apply_cooldown_between_attempts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transcription_runner_service.shutil, "which", lambda name: "/usr/bin/yt-dlp" if name == "yt-dlp" else None)

    def _fake_run(cmd: list[str], **_kwargs):
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stderr="ERROR: [youtube] v1: HTTP Error 429: Too Many Requests",
        )

    sleep_calls: list[float] = []

    def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(transcription_runner_service.subprocess, "run", _fake_run)
    monkeypatch.setattr(transcription_runner_service.time, "sleep", _fake_sleep)

    audio_path, error, error_category = transcription_runner_service._download_audio_with_ytdlp(
        video_id="v1",
        audio_source_dir=tmp_path / "audio",
    )

    assert audio_path is None
    assert error is not None and "strategy=web" in error
    assert error_category == "network_or_rate_limit"
    assert sleep_calls == [transcription_runner_service.YTDLP_STRATEGY_COOLDOWN_SECONDS] * (len(transcription_runner_service._ytdlp_download_strategies()) - 1)


def test_ytdlp_auth_required_with_cookies_opens_circuit_and_reports_cookie_diagnostics(tmp_path: Path, monkeypatch) -> None:
    video_ids = [f"video{i:06d}" for i in range(1, 6)]
    _write_queue(tmp_path, video_ids)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    cookies_file = tmp_path / "cookies.txt"
    cookies_file.write_text(
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tTRUE\t1\tVISITOR_INFO1_LIVE\told\n",
        encoding="utf-8",
    )

    def _fake_download(*, video_id: str, audio_source_dir: Path, **_kwargs):
        return None, "yt_dlp_failed:code=1;stderr=Sign in to confirm you're not a bot. Use --cookies", "auth_required"

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=5,
        audio_source_dir=tmp_path / "audio",
        openai_client=FakeOpenAIClient(),
        ytdlp_cookies_file=str(cookies_file),
    )

    assert report["processed"] == transcription_runner_service.YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD
    assert report["failed_audio_download"] == transcription_runner_service.YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD
    assert report["ytdlp_auth_required_with_cookies_count"] == transcription_runner_service.YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD
    assert report["ytdlp_auth_required_with_cookies_abort_threshold"] == transcription_runner_service.YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD
    assert "ytdlp_auth_required_circuit_open" in report["warnings"]
    assert "ytdlp_auth_required_despite_cookies" in report["warnings"]
    assert "rotate_ytdlp_cookies_or_validate_cookie_export" in report["warnings"]
    assert "ytdlp_cookies_file_youtube_google_cookies_expired" in report["warnings"]
    assert report["ytdlp_cookies_file_diagnostics"] == {
        "path_provided": True,
        "exists": True,
        "is_file": True,
        "size_bytes": cookies_file.stat().st_size,
        "non_comment_cookie_rows": 1,
        "youtube_google_cookie_rows": 1,
        "expired_youtube_google_cookie_rows": 1,
    }


def test_skips_already_success(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    update_transcript_registry(data_dir=tmp_path, entry={"video_id": "v1", "status": "success"})
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=fake)
    assert report["skipped_already_transcribed"] == 1
    assert len(fake.calls) == 0


def test_skips_persisted_transcript_even_without_registry_success(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    write_transcript_artifacts(
        video_id="v1",
        transcript_text="texto ya persistido",
        metadata={
            "channel_id": "c1",
            "channel_name": "canal",
            "title": "title",
            "source_type": "audio_file",
        },
        data_dir=tmp_path,
    )
    update_transcript_registry(data_dir=tmp_path, entry={"video_id": "v1", "status": "failed"})
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    fake = FakeOpenAIClient()
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, openai_client=fake)
    assert report["registry_success_before_run"] == 0
    assert report["persisted_success_before_run"] == 1
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


def test_reports_failed_audio_download_when_ytdlp_fallback_fails(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fake_download(*, video_id: str, audio_source_dir: Path, **_kwargs):
        return None, "yt_dlp_failed:code=1;stderr=boom", "network_or_rate_limit"

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)
    report = transcribe_selected_videos(data_dir=tmp_path, limit=10, audio_source_dir=tmp_path / "missing", openai_client=FakeOpenAIClient())

    assert report["failed_audio_download"] == 1
    assert report["skipped_missing_ytdlp"] == 0
    assert report["skipped_no_audio_source"] == 0

    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert registry_rows[-1]["status"] == "failed_audio_download_network_or_rate_limit"
    assert registry_rows[-1]["error_category"] == "network_or_rate_limit"


def test_reports_browser_cookie_access_errors_when_cookie_db_cannot_be_copied(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fake_download(*, video_id: str, audio_source_dir: Path, **_kwargs):
        return None, "yt_dlp_failed:code=1;stderr=Could not copy Chrome cookie database", "browser_cookie_access_error"

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)
    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=tmp_path / "missing",
        openai_client=FakeOpenAIClient(),
    )

    assert report["failed_audio_download"] == 1
    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert registry_rows[-1]["status"] == "failed_audio_download_browser_cookie_access"
    assert registry_rows[-1]["error_category"] == "browser_cookie_access_error"


def test_reports_sanitized_ytdlp_runtime_metadata(tmp_path: Path, monkeypatch) -> None:
    _write_queue(tmp_path, ["v1"])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fake_download(*, video_id: str, audio_source_dir: Path, **_kwargs):
        return None, "yt_dlp_not_installed", "tooling_missing"

    monkeypatch.setattr(transcription_runner_service, "_download_audio_with_ytdlp", _fake_download)
    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=tmp_path / "missing",
        openai_client=FakeOpenAIClient(),
        ytdlp_cookies_file="/tmp/cookies.txt",
        ytdlp_browser="chrome",
        ytdlp_extra_args=["--proxy", "http://localhost:8080"],
    )

    assert report["ytdlp_runtime_options"]["used_cookies_file"] is True
    assert report["ytdlp_runtime_options"]["used_browser_mode"] is True
    assert report["ytdlp_runtime_options"]["extra_args_count"] == 2


def test_transcribe_skips_probable_channel_id_without_ytdlp(tmp_path: Path, monkeypatch) -> None:
    channel_id = "UCWBWgCD4oAqT3hUeq40SCUw"
    _write_queue(tmp_path, [channel_id])
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fail_if_called(_name: str):
        raise AssertionError("yt-dlp lookup should not be needed for invalid video IDs")

    monkeypatch.setattr(transcription_runner_service.shutil, "which", _fail_if_called)

    report = transcribe_selected_videos(
        data_dir=tmp_path,
        limit=10,
        audio_source_dir=tmp_path / "audio",
        openai_client=FakeOpenAIClient(),
    )

    assert report["processed"] == 1
    assert report["skipped_invalid_video_id"] == 1
    assert report["ytdlp_download_attempts"] == 0
    assert report["invalid_video_id_details"] == [
        {
            "video_id": channel_id,
            "video_url": f"https://www.youtube.com/watch?v={channel_id}",
            "reason": "probable_channel_id_in_video_id_field",
        }
    ]
    registry_rows = [json.loads(line) for line in (tmp_path / "transcripts" / "transcript_registry.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert registry_rows[-1]["status"] == "skipped_invalid_video_id"
    assert registry_rows[-1]["error_category"] == "invalid_video_id"
