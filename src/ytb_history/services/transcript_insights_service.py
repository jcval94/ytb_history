"""Generate structured transcript insights using OpenAI structured outputs."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TRANSCRIPT_INSIGHTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "video_id", "language", "summary", "main_topics", "narrative_structure", "hook_analysis", "claims", "examples",
        "actionable_ideas", "audience", "tone", "content_style", "retention_devices", "title_supporting_quotes",
        "creative_reuse_opportunities", "risk_notes",
    ],
    "properties": {
        "video_id": {"type": "string"},
        "language": {"type": "string"},
        "summary": {"type": "string"},
        "main_topics": {"type": "array", "items": {"type": "string"}},
        "narrative_structure": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["section", "purpose", "summary"],
                "properties": {
                    "section": {"type": "string"},
                    "purpose": {"type": "string"},
                    "summary": {"type": "string"},
                },
            },
        },
        "hook_analysis": {
            "type": "object",
            "additionalProperties": False,
            "required": ["hook_type", "hook_text", "why_it_works"],
            "properties": {
                "hook_type": {"type": "string"},
                "hook_text": {"type": "string"},
                "why_it_works": {"type": "string"},
            },
        },
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["claim", "support_level", "risk"],
                "properties": {
                    "claim": {"type": "string"},
                    "support_level": {"type": "string", "enum": ["explicit", "implicit", "weak"]},
                    "risk": {"type": "string"},
                },
            },
        },
        "examples": {"type": "array", "items": {"type": "string"}},
        "actionable_ideas": {"type": "array", "items": {"type": "string"}},
        "audience": {"type": "string"},
        "tone": {"type": "string"},
        "content_style": {"type": "string"},
        "retention_devices": {"type": "array", "items": {"type": "string"}},
        "title_supporting_quotes": {"type": "array", "items": {"type": "string"}},
        "creative_reuse_opportunities": {"type": "array", "items": {"type": "string"}},
        "risk_notes": {"type": "array", "items": {"type": "string"}},
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _validate_schema(payload: dict[str, Any]) -> bool:
    required = TRANSCRIPT_INSIGHTS_SCHEMA["required"]
    return all(key in payload for key in required) and isinstance(payload.get("claims"), list)


class OpenAITranscriptInsightsClient:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini") -> None:
        self.api_key = api_key
        self.model = model

    def generate(self, *, video_id: str, transcript_text: str, language: str | None) -> dict[str, Any]:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        response = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": "Analiza transcript y responde SOLO JSON válido siguiendo el schema."},
                {"role": "user", "content": f"video_id={video_id}\nlanguage={language or 'unknown'}\ntranscript:\n{transcript_text}"},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "transcript_insights_v1",
                    "schema": TRANSCRIPT_INSIGHTS_SCHEMA,
                    "strict": True,
                }
            },
        )
        if getattr(response, "output_text", None):
            return json.loads(response.output_text)
        return json.loads(str(response))


def generate_transcript_insights(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    force: bool = False,
    dry_run: bool = False,
    model: str = "gpt-4o-mini",
    insights_client: OpenAITranscriptInsightsClient | None = None,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_root = root / "transcripts"
    registry = _read_jsonl(transcript_root / "transcript_registry.jsonl")
    success_rows = [row for row in registry if str(row.get("status", "")) == "success"]

    if not dry_run and not os.getenv("OPENAI_API_KEY", "").strip():
        report = {
            "generated_at": _now_iso(), "limit": limit, "processed": 0, "generated": 0, "cached": 0, "failed": 0,
            "warnings": ["skipped_missing_api_key"],
        }
        (transcript_root / "transcript_insights_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    client = insights_client or OpenAITranscriptInsightsClient(api_key=os.getenv("OPENAI_API_KEY", ""), model=model)
    processed = generated = cached = failed = 0
    warnings: list[str] = []
    index = _read_jsonl(transcript_root / "transcript_insights_index.jsonl")
    index_map = {str(r.get("video_id", "")): r for r in index}

    for row in success_rows:
        if processed >= max(0, limit):
            break
        video_id = str(row.get("video_id", "")).strip()
        metadata_path = row.get("metadata_path")
        if not video_id or not metadata_path:
            continue
        metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
        text_sha256 = str(metadata.get("text_sha256", ""))
        insights_path = Path(row.get("insights_path") or (transcript_root / "videos" / video_id / "transcript_insights.json"))
        current_index = index_map.get(video_id)
        if (not force) and current_index and current_index.get("text_sha256") == text_sha256 and insights_path.exists():
            cached += 1
            continue
        processed += 1
        transcript_path = Path(row.get("transcript_path"))
        original_text = transcript_path.read_text(encoding="utf-8")
        if dry_run:
            continue
        try:
            payload = client.generate(video_id=video_id, transcript_text=original_text, language=metadata.get("language"))
            if not _validate_schema(payload):
                raise ValueError("invalid_schema")
            payload["schema_version"] = "transcript_insights_v1"
            insights_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            index_map[video_id] = {
                "video_id": video_id,
                "text_sha256": text_sha256,
                "insights_path": str(insights_path),
                "generated_at": _now_iso(),
                "model": model,
            }
            generated += 1
        except Exception:
            failed += 1
            warnings.append(f"insights_failed:{video_id}")

    if not dry_run:
        _write_jsonl(transcript_root / "transcript_insights_index.jsonl", list(index_map.values()))

    report = {
        "generated_at": _now_iso(),
        "limit": limit,
        "processed": processed,
        "generated": generated,
        "cached": cached,
        "failed": failed,
        "warnings": warnings,
        "dry_run": dry_run,
    }
    (transcript_root / "transcript_insights_run_report.json").parent.mkdir(parents=True, exist_ok=True)
    (transcript_root / "transcript_insights_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
