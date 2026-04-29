from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from ytb_history import cli
from ytb_history.services.transcript_selection_service import select_transcription_candidates


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([",".join(header)] + [",".join(r) for r in rows]) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    _write_csv(root / "decision/latest_action_candidates.csv", ["video_id", "channel_id", "channel_name", "title", "upload_date", "decision_score", "alpha_score", "metric_confidence_score"], [
        ["v1", "c1", "bilinkis", "T1", "2026-04-20T00:00:00+00:00", "80", "70", "60"],
        ["v2", "c2", "veritasium", "T2", "2026-04-21T00:00:00+00:00", "90", "70", "60"],
        ["v3", "c3", "other", "T3", "2026-04-22T00:00:00+00:00", "70", "70", "60"],
        ["v4", "c4", "other", "T4", "2026-04-23T00:00:00+00:00", "60", "70", "60"],
    ])
    _write_csv(root / "model_intelligence/latest_hybrid_recommendations.csv", ["video_id", "hybrid_decision_score"], [["v1", "90"], ["v2", "95"], ["v3", "70"], ["v4", "60"]])
    _write_csv(root / "topic_intelligence/latest_topic_opportunities.csv", ["video_id", "topic_opportunity_score"], [["v1", "50"], ["v2", "90"], ["v3", "50"], ["v4", "30"]])
    _write_csv(root / "creative_packages/latest_creative_packages.csv", ["source_video_id", "creative_execution_score"], [["v1", "70"], ["v2", "90"], ["v3", "60"], ["v4", "50"]])
    _write_csv(root / "analytics/latest/latest_video_scores.csv", ["video_id", "alpha_score"], [["v1", "70"], ["v2", "70"], ["v3", "70"], ["v4", "70"]])
    _write_csv(root / "analytics/latest/latest_video_metrics.csv", ["video_id", "channel_id", "channel_name", "title", "upload_date"], [["v1", "c1", "bilinkis", "T1", "2026-04-20T00:00:00+00:00"], ["v2", "c2", "veritasium", "T2", "2026-04-21T00:00:00+00:00"], ["v3", "c3", "other", "T3", "2026-04-22T00:00:00+00:00"], ["v4", "c4", "other", "T4", "2026-04-23T00:00:00+00:00"]])


def test_forced_channels_and_ranked_behavior(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    now = datetime.now(timezone.utc)
    _write_jsonl(tmp_path / "transcripts/transcript_registry.jsonl", [
        {"video_id": "v2", "status": "in_progress"},
        {"video_id": "v4", "status": "failed", "failed_at": (now - timedelta(days=1)).isoformat()},
    ])
    report = select_transcription_candidates(data_dir=tmp_path, limit=1, forced_channels_max_per_run=50)
    queue = [json.loads(x) for x in (tmp_path / "transcripts/transcript_queue.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    assert report["selected_forced_count"] == 1
    assert report["selected_ranked_count"] == 1
    assert queue[0]["selection_source"] == "forced_channel_new_video"
    assert queue[0]["video_id"] == "v1"
    assert queue[1]["selection_source"] == "ranked_daily_top"
    assert all(v["video_id"] != "v2" for v in queue)
    assert all(v["video_id"] != "v4" for v in queue)


def test_forced_dedupes_with_ranked_and_max_per_run_warning(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = select_transcription_candidates(data_dir=tmp_path, limit=10, forced_channels_max_per_run=1)
    queue = [json.loads(x) for x in (tmp_path / "transcripts/transcript_queue.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    ids = [r["video_id"] for r in queue]
    assert ids.count("v1") == 1
    assert "forced_channels_truncated_max_per_run" in report["warnings"]


def test_transcript_selection_cli_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "select_transcription_candidates", lambda **kwargs: {"status": "ok", "selected_forced_count": 1, "selected_ranked_count": 2})
    monkeypatch.setattr("sys.argv", ["ytb_history", "select-transcription-candidates", "--limit", "10", "--data-dir", "data"])
    code = cli.main(); out = capsys.readouterr().out
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_forced_count"] == 1


def test_transcription_channels_config_exists() -> None:
    text = Path("config/transcription_channels.py").read_text(encoding="utf-8")
    assert "@bilinkis" in text and "veritasium" in text
