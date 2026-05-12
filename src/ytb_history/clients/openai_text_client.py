"""Thin OpenAI text client wrapper for structured transcript insights."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class OpenAITextClient:
    api_key: str
    model: str = "gpt-5.5-mini"

    def generate_structured_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        schema_name: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        response = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": schema_name,
                    "schema": schema,
                    "strict": True,
                }
            },
        )
        if getattr(response, "output_text", None):
            return json.loads(response.output_text)
        return json.loads(str(response))
