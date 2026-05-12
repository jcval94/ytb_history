"""Generate structured transcript insights from stored transcripts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ytb_history.clients.openai_text_client import OpenAITextClient
from ytb_history.utils.environment import resolve_environment_variable

TRANSCRIPT_INSIGHTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "schema_version",
        "video_id",
        "language",
        "summary",
        "main_topics",
        "narrative_structure",
        "hook_analysis",
        "claims",
        "examples",
        "actionable_ideas",
        "audience",
        "tone",
        "content_style",
        "retention_devices",
        "title_supporting_quotes",
        "creative_reuse_opportunities",
        "risk_notes",
    ],
    "properties": {
        "schema_version": {"type": "string", "enum": ["transcript_insights_v1"]},
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
                "hook_type": {
                    "type": "string",
                    "enum": ["question", "contrast", "data", "mistake", "promise", "curiosity", "unknown"],
                },
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


class TranscriptInsightsClient(Protocol):
    def generate(self, *, video_id: str, transcript_text: str, language: str | None) -> dict[str, Any]:
        ...


class OpenAITranscriptInsightsClient:
    def __init__(self, api_key: str, model: str = "gpt-5.5-mini") -> None:
        self.model = model
        self.text_client = OpenAITextClient(api_key=api_key, model=model)

    def generate(self, *, video_id: str, transcript_text: str, language: str | None) -> dict[str, Any]:
        return self.text_client.generate_structured_json(
            system_prompt=(
                "Eres una capa de Transcript Intelligence. Responde en espanol, solo JSON valido, "
                "sin inventar datos fuera del transcript ni convertirlo en recomendacion financiera."
            ),
            user_prompt=(
                f"video_id={video_id}\n"
                f"language={language or 'unknown'}\n\n"
                "Extrae resumen, estructura narrativa, hook, claims, riesgos y oportunidades creativas. "
                "Marca claims dudosos en risk_notes y no afirmes metricas que no aparezcan en evidencia.\n\n"
                f"transcript:\n{transcript_text}"
            ),
            schema_name="transcript_insights_v1",
            schema=TRANSCRIPT_INSIGHTS_SCHEMA,
        )


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_artifact_path(path_value: Any, *, data_root: Path) -> Path:
    path = Path(str(path_value))
    if path.is_absolute():
        return path
    if path.exists():
        return path
    data_name = data_root.name
    parts = path.parts
    if parts and parts[0] == data_name:
        return data_root.parent.joinpath(*parts)
    return data_root / path


def _validate_schema(payload: dict[str, Any]) -> bool:
    required = TRANSCRIPT_INSIGHTS_SCHEMA["required"]
    return (
        all(key in payload for key in required)
        and payload.get("schema_version") == "transcript_insights_v1"
        and isinstance(payload.get("main_topics"), list)
        and isinstance(payload.get("claims"), list)
    )


def _report(
    *,
    transcript_root: Path,
    limit: int,
    dry_run: bool,
    model: str,
    candidates_considered: int,
    generated_success: int,
    skipped_cache: int,
    skipped_missing_api_key: int,
    failed: int,
    warnings: list[str],
) -> dict[str, Any]:
    payload = {
        "generated_at": _now_iso(),
        "status": "success" if not skipped_missing_api_key else "skipped_missing_api_key",
        "limit": limit,
        "dry_run": dry_run,
        "model": model,
        "candidates_considered": candidates_considered,
        "generated_success": generated_success,
        "generated": generated_success,
        "skipped_cache": skipped_cache,
        "cached": skipped_cache,
        "skipped_missing_api_key": skipped_missing_api_key,
        "failed": failed,
        "warnings": warnings,
    }
    _write_json(transcript_root / "transcript_insights_run_report.json", payload)
    return payload


def generate_transcript_insights(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    force: bool = False,
    dry_run: bool = False,
    model: str = "gpt-5.5-mini",
    insights_client: TranscriptInsightsClient | None = None,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_root = root / "transcripts"
    registry = _read_jsonl(transcript_root / "transcript_registry.jsonl")
    success_rows = [row for row in registry if str(row.get("status", "")) == "success"]
    candidates = success_rows[: max(0, limit)]

    api_key = resolve_environment_variable("OPENAI_API_KEY")
    if not dry_run and not api_key:
        return _report(
            transcript_root=transcript_root,
            limit=limit,
            dry_run=dry_run,
            model=model,
            candidates_considered=len(candidates),
            generated_success=0,
            skipped_cache=0,
            skipped_missing_api_key=len(candidates),
            failed=0,
            warnings=["skipped_missing_api_key"],
        )

    client = insights_client or (OpenAITranscriptInsightsClient(api_key=api_key, model=model) if not dry_run else None)
    index_rows = _read_jsonl(transcript_root / "transcript_insights_index.jsonl")
    index_map = {str(row.get("video_id", "")): row for row in index_rows}
    generated_success = 0
    skipped_cache = 0
    failed = 0
    warnings: list[str] = []

    for row in candidates:
        video_id = str(row.get("video_id", "")).strip()
        metadata_path = row.get("metadata_path")
        transcript_path = row.get("transcript_path")
        if not video_id or not metadata_path or not transcript_path:
            continue

        try:
            metadata = json.loads(_resolve_artifact_path(metadata_path, data_root=root).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            failed += 1
            warnings.append(f"metadata_unreadable:{video_id}")
            continue

        text_sha256 = str(metadata.get("text_sha256", ""))
        insights_path = _resolve_artifact_path(
            row.get("insights_path") or (transcript_root / "videos" / video_id / "transcript_insights.json"),
            data_root=root,
        )
        current_index = index_map.get(video_id)
        if (not force) and current_index and current_index.get("text_sha256") == text_sha256 and insights_path.exists():
            skipped_cache += 1
            index_map[video_id] = {**current_index, "status": "skipped_cache"}
            continue

        if dry_run:
            continue

        try:
            if client is None:
                raise RuntimeError("insights_client_unavailable")
            transcript_text = _resolve_artifact_path(transcript_path, data_root=root).read_text(encoding="utf-8")
            payload = client.generate(
                video_id=video_id,
                transcript_text=transcript_text,
                language=metadata.get("language"),
            )
            payload.setdefault("schema_version", "transcript_insights_v1")
            if not _validate_schema(payload):
                raise ValueError("invalid_schema")
            _write_json(insights_path, payload)
            index_map[video_id] = {
                "video_id": video_id,
                "insights_path": str(insights_path),
                "generated_at": _now_iso(),
                "model": model,
                "text_sha256": text_sha256,
                "status": "success",
                "summary": payload.get("summary", ""),
                "main_topics": payload.get("main_topics", []),
            }
            generated_success += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            warnings.append(f"insights_failed:{video_id}:{type(exc).__name__}")
            index_map[video_id] = {
                "video_id": video_id,
                "insights_path": str(insights_path),
                "generated_at": _now_iso(),
                "model": model,
                "text_sha256": text_sha256,
                "status": "failed",
                "summary": "",
                "main_topics": [],
            }

    if not dry_run:
        _write_jsonl(transcript_root / "transcript_insights_index.jsonl", list(index_map.values()))

    return _report(
        transcript_root=transcript_root,
        limit=limit,
        dry_run=dry_run,
        model=model,
        candidates_considered=len(candidates),
        generated_success=generated_success,
        skipped_cache=skipped_cache,
        skipped_missing_api_key=0,
        failed=failed,
        warnings=warnings,
    )
