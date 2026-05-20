from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from ytb_history.clients.openai_audio_client import OpenAIAudioClient


def test_transcribe_file_with_segments_requests_verbose_json(tmp_path: Path, monkeypatch) -> None:
    audio_path = tmp_path / "audio.mp3"
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeTranscriptions:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                text="hola mundo",
                duration=1.5,
                segments=[SimpleNamespace(start=0.0, end=1.5, text="hola mundo")],
            )

    class FakeOpenAI:
        def __init__(self, *, api_key: str) -> None:
            captured["api_key"] = api_key
            self.audio = SimpleNamespace(transcriptions=FakeTranscriptions())

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAI))

    response = OpenAIAudioClient(api_key="test-key").transcribe_file_with_segments(file_path=audio_path)

    assert captured["api_key"] == "test-key"
    assert captured["model"] == "whisper-1"
    assert captured["response_format"] == "verbose_json"
    assert captured["timestamp_granularities"] == ["segment"]
    assert response["segments"] == [{"start": 0.0, "end": 1.5, "text": "hola mundo"}]
