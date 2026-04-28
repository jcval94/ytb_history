from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history.services.decision_service import build_decision_layer


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_inputs(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"

    (data_dir / "alerts").mkdir(parents=True, exist_ok=True)
    (data_dir / "alerts" / "latest_alerts.json").write_text(
        json.dumps({"alerts": [{"signal_type": "trend_burst", "entity_id": "v1"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    _write_csv(
        data_dir / "signals" / "latest_signal_candidates.csv",
        [
            "execution_date",
            "entity_type",
            "entity_id",
            "signal_type",
            "raw_signal_score",
            "adjusted_signal_score",
            "threshold",
            "triggered",
            "metric_confidence_score",
            "confidence_level",
        ],
        [
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v1",
                "signal_type": "trend_burst",
                "raw_signal_score": 95,
                "adjusted_signal_score": 90,
                "threshold": 80,
                "triggered": True,
                "metric_confidence_score": 75,
                "confidence_level": "high",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v2",
                "signal_type": "evergreen_candidate",
                "raw_signal_score": 89,
                "adjusted_signal_score": 80,
                "threshold": 75,
                "triggered": True,
                "metric_confidence_score": 65,
                "confidence_level": "medium",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v3",
                "signal_type": "packaging_problem",
                "raw_signal_score": 85,
                "adjusted_signal_score": 70,
                "threshold": 70,
                "triggered": True,
                "metric_confidence_score": 70,
                "confidence_level": "medium",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v4",
                "signal_type": "low_confidence_metric",
                "raw_signal_score": 40,
                "adjusted_signal_score": 35,
                "threshold": 35,
                "triggered": True,
                "metric_confidence_score": 20,
                "confidence_level": "low",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "channel",
                "entity_id": "c1",
                "signal_type": "channel_momentum_up",
                "raw_signal_score": 82,
                "adjusted_signal_score": 78,
                "threshold": 80,
                "triggered": True,
                "metric_confidence_score": 72,
                "confidence_level": "high",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v5",
                "signal_type": "metadata_change_watch",
                "raw_signal_score": 63,
                "adjusted_signal_score": 60,
                "threshold": 1,
                "triggered": True,
                "metric_confidence_score": 60,
                "confidence_level": "medium",
            },
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v6",
                "signal_type": "accelerating_video",
                "raw_signal_score": 77,
                "adjusted_signal_score": 76,
                "threshold": 70,
                "triggered": True,
                "metric_confidence_score": 70,
                "confidence_level": "medium",
            },
        ],
    )

    _write_csv(data_dir / "signals" / "latest_video_signals.csv", ["video_id", "channel_id", "channel_name", "title"], [])
    _write_csv(data_dir / "signals" / "latest_channel_signals.csv", ["channel_id", "channel_name"], [])

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "channel_id", "channel_name", "title", "opportunity_score"],
        [
            {"video_id": "v1", "channel_id": "c1", "channel_name": "Canal 1", "title": "Trend video", "opportunity_score": 84},
            {"video_id": "v2", "channel_id": "c2", "channel_name": "Canal 2", "title": "Evergreen video", "opportunity_score": 70},
            {"video_id": "v3", "channel_id": "c3", "channel_name": "Canal 3", "title": "Packaging video", "opportunity_score": 67},
            {"video_id": "v4", "channel_id": "c4", "channel_name": "Canal 4", "title": "Low conf", "opportunity_score": 30},
            {"video_id": "v5", "channel_id": "c5", "channel_name": "Canal 5", "title": "Metadata", "opportunity_score": 50},
            {"video_id": "v6", "channel_id": "c6", "channel_name": "Canal 6", "title": "Accelerating", "opportunity_score": 55},
        ],
    )

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv",
        ["video_id", "channel_relative_success_score"],
        [
            {"video_id": "v1", "channel_relative_success_score": 88},
            {"video_id": "v2", "channel_relative_success_score": 65},
            {"video_id": "v3", "channel_relative_success_score": 71},
            {"video_id": "v4", "channel_relative_success_score": 20},
            {"video_id": "v5", "channel_relative_success_score": 58},
            {"video_id": "v6", "channel_relative_success_score": 68},
        ],
    )

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv",
        ["channel_id", "channel_name"],
        [{"channel_id": "c1", "channel_name": "Canal 1"}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["video_id", "channel_id", "channel_name", "title"],
        [
            {"video_id": "v1", "channel_id": "c1", "channel_name": "Canal 1", "title": "Trend video"},
            {"video_id": "v2", "channel_id": "c2", "channel_name": "Canal 2", "title": "Evergreen video"},
            {"video_id": "v3", "channel_id": "c3", "channel_name": "Canal 3", "title": "Packaging video"},
            {"video_id": "v4", "channel_id": "c4", "channel_name": "Canal 4", "title": "Low conf"},
            {"video_id": "v5", "channel_id": "c5", "channel_name": "Canal 5", "title": "Metadata"},
            {"video_id": "v6", "channel_id": "c6", "channel_name": "Canal 6", "title": "Accelerating"},
        ],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_channel_metrics.csv",
        ["channel_id", "channel_name"],
        [{"channel_id": "c1", "channel_name": "Canal 1"}],
    )

    return data_dir


def test_build_decision_layer_outputs_rules_and_scores(tmp_path: Path) -> None:
    data_dir = _prepare_inputs(tmp_path)

    result = build_decision_layer(data_dir=data_dir)

    assert result["status"] in {"success", "warning"}
    decision_dir = data_dir / "decision"
    assert (decision_dir / "latest_action_candidates.csv").exists()
    assert (decision_dir / "latest_opportunity_matrix.csv").exists()
    assert (decision_dir / "latest_content_opportunities.csv").exists()
    assert (decision_dir / "latest_watchlist_recommendations.csv").exists()
    assert (decision_dir / "latest_decision_context.json").exists()
    assert (decision_dir / "decision_summary.json").exists()

    with (decision_dir / "latest_action_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    by_signal = {row["signal_type"]: row for row in rows}
    assert by_signal["trend_burst"]["action_type"] == "create_fast_reaction"
    assert by_signal["evergreen_candidate"]["action_type"] == "create_evergreen"
    assert by_signal["packaging_problem"]["action_type"] == "repackage_idea"
    assert by_signal["low_confidence_metric"]["action_type"] == "wait_for_confidence"
    assert by_signal["channel_momentum_up"]["action_type"] == "benchmark_channel"

    trend = by_signal["trend_burst"]
    strategic = float(trend["strategic_value_score"])
    expected_strategic = 0.35 * 90 + 0.25 * 84 + 0.20 * 88 + 0.20 * 75
    assert round(strategic, 4) == round(expected_strategic, 4)

    expected_value = float(trend["expected_value_score"])
    expected_expected_value = expected_strategic * 1.0 * 1.2 * 0.8
    assert round(expected_value, 4) == round(expected_expected_value, 4)

    decision_score = float(trend["decision_score"])
    expected_decision = 0.60 * expected_expected_value + 0.25 * 90 + 0.15 * 75
    assert round(decision_score, 4) == round(expected_decision, 4)
    assert trend["priority"] == "high"

    with (decision_dir / "latest_opportunity_matrix.csv").open("r", encoding="utf-8", newline="") as handle:
        matrix_rows = list(csv.DictReader(handle))
    matrix_types = {row["action_type"] for row in matrix_rows}
    assert "create_fast_reaction" in matrix_types
    assert "wait_for_confidence" in matrix_types

    with (decision_dir / "latest_content_opportunities.csv").open("r", encoding="utf-8", newline="") as handle:
        content_rows = list(csv.DictReader(handle))
    assert content_rows
    assert all(row["opportunity_type"] in {"create_fast_reaction", "create_evergreen", "repackage_idea", "analyze_reference"} for row in content_rows)

    with (decision_dir / "latest_watchlist_recommendations.csv").open("r", encoding="utf-8", newline="") as handle:
        watchlist_rows = list(csv.DictReader(handle))
    watch_types = {row["watchlist_type"] for row in watchlist_rows}
    assert "metadata_change_watch" in watch_types
    assert "low_confidence_metric" in watch_types
    assert "accelerating_video" in watch_types

    summary = json.loads((decision_dir / "decision_summary.json").read_text(encoding="utf-8"))
    assert summary["total_action_candidates"] == len(rows)
    assert summary["watchlist_count"] == len(watchlist_rows)


def test_build_decision_layer_warns_and_renormalizes_when_inputs_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_csv(
        data_dir / "signals" / "latest_signal_candidates.csv",
        [
            "execution_date",
            "entity_type",
            "entity_id",
            "signal_type",
            "raw_signal_score",
            "adjusted_signal_score",
            "threshold",
            "triggered",
            "metric_confidence_score",
            "confidence_level",
        ],
        [
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "vx",
                "signal_type": "trend_burst",
                "raw_signal_score": 88,
                "adjusted_signal_score": 80,
                "threshold": 80,
                "triggered": True,
                "metric_confidence_score": "",
                "confidence_level": "medium",
            }
        ],
    )
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_scores.csv", ["video_id", "opportunity_score"], [{"video_id": "vx", "opportunity_score": 60}])
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv", ["video_id", "channel_relative_success_score"], [{"video_id": "vx", "channel_relative_success_score": ""}])
    _write_csv(data_dir / "signals" / "latest_video_signals.csv", ["video_id"], [])
    _write_csv(data_dir / "signals" / "latest_channel_signals.csv", ["channel_id"], [])
    _write_csv(data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv", ["channel_id"], [])
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_metrics.csv", ["video_id"], [{"video_id": "vx"}])
    _write_csv(data_dir / "analytics" / "latest" / "latest_channel_metrics.csv", ["channel_id"], [])

    result = build_decision_layer(data_dir=data_dir)
    assert result["status"] == "warning"
    assert any("latest_alerts.json" in warning for warning in result["warnings"])

    with (data_dir / "decision" / "latest_action_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))

    # Renormalización usando adjusted_signal_score (0.35) + opportunity_score (0.25), dividido entre 0.60
    expected_strategic = (80 * 0.35 + 60 * 0.25) / 0.60
    assert round(float(row["strategic_value_score"]), 4) == round(expected_strategic, 4)


def test_decision_service_no_api_and_no_search_list() -> None:
    text = Path("src/ytb_history/services/decision_service.py").read_text(encoding="utf-8")
    assert "youtube" not in text.lower()
    assert "search.list" not in text


def test_triggered_false_is_ignored_from_all_decision_outputs(tmp_path: Path) -> None:
    data_dir = _prepare_inputs(tmp_path)

    with (data_dir / "signals" / "latest_signal_candidates.csv").open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "execution_date",
                "entity_type",
                "entity_id",
                "signal_type",
                "raw_signal_score",
                "adjusted_signal_score",
                "threshold",
                "triggered",
                "metric_confidence_score",
                "confidence_level",
            ],
        )
        writer.writerow(
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": "v999",
                "signal_type": "trend_burst",
                "raw_signal_score": 99,
                "adjusted_signal_score": 98,
                "threshold": 80,
                "triggered": "false",
                "metric_confidence_score": 95,
                "confidence_level": "high",
            }
        )

    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_scores.csv",
        ["video_id", "channel_id", "channel_name", "title", "opportunity_score"],
        [{"video_id": "v999", "channel_id": "cx", "channel_name": "Canal X", "title": "No debe entrar", "opportunity_score": 95}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv",
        ["video_id", "channel_relative_success_score"],
        [{"video_id": "v999", "channel_relative_success_score": 95}],
    )
    _write_csv(
        data_dir / "analytics" / "latest" / "latest_video_metrics.csv",
        ["video_id", "channel_id", "channel_name", "title"],
        [{"video_id": "v999", "channel_id": "cx", "channel_name": "Canal X", "title": "No debe entrar"}],
    )

    build_decision_layer(data_dir=data_dir)

    decision_dir = data_dir / "decision"
    with (decision_dir / "latest_action_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        actions = list(csv.DictReader(handle))
    assert all(row["entity_id"] != "v999" for row in actions)

    with (decision_dir / "latest_content_opportunities.csv").open("r", encoding="utf-8", newline="") as handle:
        content_rows = list(csv.DictReader(handle))
    assert all(row["source_video_id"] != "v999" for row in content_rows)

    with (decision_dir / "latest_watchlist_recommendations.csv").open("r", encoding="utf-8", newline="") as handle:
        watchlist_rows = list(csv.DictReader(handle))
    assert all(row["entity_id"] != "v999" for row in watchlist_rows)

    summary = json.loads((decision_dir / "decision_summary.json").read_text(encoding="utf-8"))
    assert summary["ignored_signal_candidates"] >= 1


def test_triggered_parsing_is_strict_for_true_values(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"

    triggered_values = ["false", "False", False, "", "0", "true", "True", True, "1"]
    rows: list[dict[str, object]] = []
    for idx, value in enumerate(triggered_values, start=1):
        rows.append(
            {
                "execution_date": "2026-04-28",
                "entity_type": "video",
                "entity_id": f"tv{idx}",
                "signal_type": "trend_burst",
                "raw_signal_score": 90,
                "adjusted_signal_score": 85,
                "threshold": 80,
                "triggered": value,
                "metric_confidence_score": 70,
                "confidence_level": "high",
            }
        )

    _write_csv(
        data_dir / "signals" / "latest_signal_candidates.csv",
        [
            "execution_date",
            "entity_type",
            "entity_id",
            "signal_type",
            "raw_signal_score",
            "adjusted_signal_score",
            "threshold",
            "triggered",
            "metric_confidence_score",
            "confidence_level",
        ],
        rows,
    )
    _write_csv(data_dir / "signals" / "latest_video_signals.csv", ["video_id"], [])
    _write_csv(data_dir / "signals" / "latest_channel_signals.csv", ["channel_id"], [])
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_scores.csv", ["video_id"], [{"video_id": f"tv{idx}"} for idx in range(1, len(rows) + 1)])
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_advanced_metrics.csv", ["video_id"], [{"video_id": f"tv{idx}"} for idx in range(1, len(rows) + 1)])
    _write_csv(data_dir / "analytics" / "latest" / "latest_channel_advanced_metrics.csv", ["channel_id"], [])
    _write_csv(data_dir / "analytics" / "latest" / "latest_video_metrics.csv", ["video_id"], [{"video_id": f"tv{idx}"} for idx in range(1, len(rows) + 1)])
    _write_csv(data_dir / "analytics" / "latest" / "latest_channel_metrics.csv", ["channel_id"], [])

    build_decision_layer(data_dir=data_dir)

    with (data_dir / "decision" / "latest_action_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        actions = list(csv.DictReader(handle))

    action_ids = {row["entity_id"] for row in actions}
    assert action_ids == {"tv6", "tv7", "tv8", "tv9"}

    summary = json.loads((data_dir / "decision" / "decision_summary.json").read_text(encoding="utf-8"))
    assert summary["ignored_signal_candidates"] == 5
