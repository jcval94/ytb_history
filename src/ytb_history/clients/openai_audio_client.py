"""Thin OpenAI audio transcription client wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
