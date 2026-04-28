from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.alerts_service import generate_alerts


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_analytics(tmp_path: Path) -> Path:
    latest = tmp_path / "data" / "analytics" / "latest"

    _write_csv(
        latest / "latest_video_scores.csv",
        [
            "execution_date",
            "video_id",
            "channel_id",
            "channel_name",
            "title",
            "alpha_score",
            "engagement_percentile",
            "growth_percentile",
            "metadata_change_score",
            "metadata_changed",
        ],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v1",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "title": "Video Alpha",
                "alpha_score": 90,
                "engagement_percentile": 80,
                "growth_percentile": 40,
                "metadata_change_score": 62,
                "metadata_changed": True,
            }
        ],
    )

    _write_csv(
        latest / "latest_video_advanced_metrics.csv",
        [
            "execution_date",
            "video_id",
            "channel_id",
            "channel_name",
            "title",
            "trend_burst_score",
            "growth_trend_label",
            "growth_acceleration_score",
            "evergreen_score",
            "packaging_problem_score",
            "metric_confidence_score",
            "metadata_changed",
        ],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v1",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "title": "Video Alpha",
                "trend_burst_score": 82,
                "growth_trend_label": "accelerating",
                "growth_acceleration_score": 72,
                "evergreen_score": 77,
                "packaging_problem_score": 75,
                "metric_confidence_score": 30,
                "metadata_changed": True,
            }
        ],
    )

    _write_csv(
        latest / "latest_metric_eligibility.csv",
        ["execution_date", "video_id", "channel_id"],
        [{"execution_date": "2026-04-28T00:00:00+00:00", "video_id": "v1", "channel_id": "c1"}],
    )

    _write_csv(
        latest / "latest_video_metrics.csv",
        ["execution_date", "video_id", "channel_id", "channel_name", "title"],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "video_id": "v1",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "title": "Video Alpha",
            }
        ],
    )

    _write_csv(
        latest / "latest_channel_advanced_metrics.csv",
        [
            "execution_date",
            "channel_id",
            "channel_name",
            "total_views_delta",
            "channel_momentum_score",
            "channel_consistency_score",
            "metric_confidence_score",
        ],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "total_views_delta": 1000,
                "channel_momentum_score": 85,
                "channel_consistency_score": 80,
                "metric_confidence_score": 60,
            },
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "channel_id": "c2",
                "channel_name": "Canal 2",
                "total_views_delta": 50,
                "channel_momentum_score": 20,
                "channel_consistency_score": 20,
                "metric_confidence_score": 60,
            },
        ],
    )

    _write_csv(
        latest / "latest_channel_metrics.csv",
        ["execution_date", "channel_id", "channel_name", "total_views_delta"],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal 1",
                "total_views_delta": 1000,
            },
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "channel_id": "c2",
                "channel_name": "Canal 2",
                "total_views_delta": 50,
            },
        ],
    )

    return tmp_path / "data"


def test_generate_alerts_warns_when_analytics_missing(tmp_path: Path) -> None:
    result = generate_alerts(data_dir=tmp_path / "data")
    assert result["status"] == "warning"
    assert result["warnings"]


def test_generate_alerts_outputs_and_rules(tmp_path: Path) -> None:
    data_dir = _prepare_analytics(tmp_path)
    result = generate_alerts(data_dir=data_dir)

    assert result["status"] in {"success", "warning"}

    signals_dir = data_dir / "signals"
    alerts_dir = data_dir / "alerts"
    assert (signals_dir / "latest_video_signals.csv").exists()
    assert (signals_dir / "latest_channel_signals.csv").exists()
    assert (signals_dir / "latest_signal_candidates.csv").exists()
    assert (signals_dir / "signal_summary.json").exists()
    assert (alerts_dir / "latest_alerts.jsonl").exists()
    assert (alerts_dir / "latest_alerts.json").exists()
    assert (alerts_dir / "latest_alerts.md").exists()
    assert (alerts_dir / "alert_summary.json").exists()

    with (signals_dir / "latest_signal_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        candidates = list(csv.DictReader(handle))

    by_signal = {row["signal_type"]: row for row in candidates if row["entity_id"] == "v1"}
    assert by_signal["alpha_breakout"]["triggered"] == "True"
    assert by_signal["trend_burst"]["triggered"] == "True"
    assert by_signal["packaging_problem"]["triggered"] == "True"
    assert by_signal["high_engagement_low_reach"]["triggered"] == "True"
    assert by_signal["low_confidence_metric"]["triggered"] == "True"

    raw = float(by_signal["alpha_breakout"]["raw_signal_score"])
    adjusted = float(by_signal["alpha_breakout"]["adjusted_signal_score"])
    confidence = float(by_signal["alpha_breakout"]["metric_confidence_score"])
    expected = raw * (0.5 + 0.5 * confidence / 100.0)
    assert adjusted == round(expected, 4)

    with (alerts_dir / "latest_alerts.json").open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["alert_count"] > 0
    trend_alert = next(item for item in payload["alerts"] if item["signal_type"] == "trend_burst")
    assert trend_alert["severity"] in {"critical", "high", "medium", "low"}

    with (signals_dir / "latest_channel_signals.csv").open("r", encoding="utf-8", newline="") as handle:
        channel_rows = list(csv.DictReader(handle))
    c1 = next(row for row in channel_rows if row["channel_id"] == "c1")
    assert float(c1["channel_momentum_up"]) >= 80

    md_text = (alerts_dir / "latest_alerts.md").read_text(encoding="utf-8")
    assert "## Critical" in md_text
    assert "## High" in md_text
    assert "## Medium" in md_text
    assert "## Low" in md_text

    outputs = result["outputs"]
    for output_path in outputs.values():
        assert output_path.startswith("signals/") or output_path.startswith("alerts/")
