"""Thin OpenAI audio transcription client wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _to_plain_data(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, dict):
        return {str(key): _to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_plain_data(item) for item in value]
    if isinstance(value, tuple):
        return [_to_plain_data(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(key): _to_plain_data(item) for key, item in vars(value).items() if not key.startswith("_")}
    return value


@dataclass
class OpenAIAudioClient:
    api_key: str

    def transcribe_file(self, *, file_path: str | Path, model: str = "gpt-4o-mini-transcribe") -> str:
        from openai import OpenAI  # local import for easier testing/mocking

        client = OpenAI(api_key=self.api_key)
        with Path(file_path).open("rb") as handle:
            response: Any = client.audio.transcriptions.create(
                model=model,
                file=handle,
                response_format="text",
            )
        if isinstance(response, str):
            return response
        text = getattr(response, "text", None)
        if text is None:
            return str(response)
        return str(text)

    def transcribe_file_with_segments(self, *, file_path: str | Path, model: str = "whisper-1") -> dict[str, Any]:
        from openai import OpenAI  # local import for easier testing/mocking

        client = OpenAI(api_key=self.api_key)
        with Path(file_path).open("rb") as handle:
            response: Any = client.audio.transcriptions.create(
                model=model,
                file=handle,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )
        plain = _to_plain_data(response)
        if isinstance(plain, dict):
            return plain
        return {"text": str(plain), "segments": []}
