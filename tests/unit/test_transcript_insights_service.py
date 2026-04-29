from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.transcript_insights_service import generate_transcript_insights


class FakeInsightsClient:
    def __init__(self) -> None:
        self.calls = 0

    def generate(self, *, video_id: str, transcript_text: str, language: str | None) -> dict:
        self.calls += 1
        return {
            "video_id": video_id,
            "language": language or "unknown",
            "summary": "Resumen",
            "main_topics": ["topic"],
            "narrative_structure": [{"section": "intro", "purpose": "hook", "summary": "..."}],
            "hook_analysis": {"hook_type": "question", "hook_text": "...", "why_it_works": "..."},
            "claims": [{"claim": "c", "support_level": "explicit", "risk": "low"}],
            "examples": ["ex"],
            "actionable_ideas": ["idea"],
            "audience": "general",
            "tone": "direct",
            "content_style": "educational",
            "retention_devices": ["story"],
            "title_supporting_quotes": ["quote"],
            "creative_reuse_opportunities": ["clip"],
            "risk_notes": [],
        }


def _seed_success_registry(tmp_path: Path, video_id: str = "v1", sha: str = "sha1") -> None:
    tdir = tmp_path / "transcripts" / "videos" / video_id
    tdir.mkdir(parents=True, exist_ok=True)
    (tdir / "transcript.txt").write_text("texto original", encoding="utf-8")
    (tdir / "transcript_metadata.json").write_text(json.dumps({"video_id": video_id, "language": "es", "text_sha256": sha}), encoding="utf-8")
    (tmp_path / "transcripts" / "transcript_registry.jsonl").write_text(
        json.dumps({"video_id": video_id, "status": "success", "transcript_path": str(tdir / "transcript.txt"), "metadata_path": str(tdir / "transcript_metadata.json"), "insights_path": str(tdir / "transcript_insights.json")}) + "\n",
        encoding="utf-8",
    )


def test_skip_without_api_key(tmp_path: Path, monkeypatch) -> None:
    _seed_success_registry(tmp_path)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    report = generate_transcript_insights(data_dir=tmp_path, limit=10)
    assert "skipped_missing_api_key" in report["warnings"]


def test_fake_client_generates_insights_and_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    _seed_success_registry(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    fake = FakeInsightsClient()
    report = generate_transcript_insights(data_dir=tmp_path, limit=10, insights_client=fake)
    assert report["generated"] == 1
    assert fake.calls == 1
    assert (tmp_path / "transcripts" / "videos" / "v1" / "transcript_insights.json").exists()
    assert (tmp_path / "transcripts" / "transcript_insights_index.jsonl").exists()


def test_cache_avoids_regenerate(tmp_path: Path, monkeypatch) -> None:
    _seed_success_registry(tmp_path, sha="same")
    (tmp_path / "transcripts" / "videos" / "v1" / "transcript_insights.json").write_text("{}", encoding="utf-8")
    (tmp_path / "transcripts" / "transcript_insights_index.jsonl").write_text(json.dumps({"video_id": "v1", "text_sha256": "same"}) + "\n", encoding="utf-8")
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    fake = FakeInsightsClient()
    report = generate_transcript_insights(data_dir=tmp_path, limit=10, insights_client=fake)
    assert report["cached"] == 1
    assert fake.calls == 0


def test_force_regenerates(tmp_path: Path, monkeypatch) -> None:
    _seed_success_registry(tmp_path, sha="same")
    (tmp_path / "transcripts" / "videos" / "v1" / "transcript_insights.json").write_text("{}", encoding="utf-8")
    (tmp_path / "transcripts" / "transcript_insights_index.jsonl").write_text(json.dumps({"video_id": "v1", "text_sha256": "same"}) + "\n", encoding="utf-8")
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    fake = FakeInsightsClient()
    report = generate_transcript_insights(data_dir=tmp_path, limit=10, force=True, insights_client=fake)
    assert report["generated"] == 1
    assert fake.calls == 1


def test_does_not_modify_transcript_txt(tmp_path: Path, monkeypatch) -> None:
    _seed_success_registry(tmp_path)
    before = (tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt").read_text(encoding="utf-8")
    monkeypatch.setenv("OPENAI_API_KEY", "x")
    generate_transcript_insights(data_dir=tmp_path, limit=10, insights_client=FakeInsightsClient())
    after = (tmp_path / "transcripts" / "videos" / "v1" / "transcript.txt").read_text(encoding="utf-8")
    assert before == after
