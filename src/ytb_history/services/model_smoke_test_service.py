"""Synthetic smoke test for model training and prediction."""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from ytb_history.services.model_prediction_service import predict_with_model_artifact
from ytb_history.services.model_training_service import _HAS_JOBLIB, _HAS_SKLEARN, train_model_suite

_FEATURES = [
    "views_delta", "engagement_rate", "comment_rate", "video_age_days", "duration_bucket", "is_short",
    "alpha_score", "opportunity_score", "trend_burst_score", "evergreen_score", "packaging_problem_score",
    "metric_confidence_score", "channel_momentum_score", "channel_relative_success_score", "title_length_chars",
    "has_number", "has_question", "has_ai_word", "has_finance_word", "metadata_changed",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _synth_rows(n_rows: int) -> list[dict[str, Any]]:
    rng = random.Random(42)
    base_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows: list[dict[str, Any]] = []
    for i in range(n_rows):
        channel_idx = i % 12
        exec_dt = base_dt + timedelta(days=i % 45)
        views_delta = 50 + rng.random() * 800
        engagement = 0.01 + rng.random() * 0.15
        comment_rate = 0.001 + rng.random() * 0.05
        alpha = 20 + rng.random() * 80
        opp = 20 + rng.random() * 80
        momentum = 10 + rng.random() * 90
        noise = rng.random() * 0.3
        signal = 0.35 * (views_delta / 850.0) + 0.25 * (engagement / 0.16) + 0.2 * (alpha / 100.0) + 0.2 * (opp / 100.0)
        is_top = 1 if signal + noise > 0.75 else 0
        future_log = round(1.2 + signal * 4.0 + noise, 6)
        future_views = int(120 + signal * 2500 + noise * 300)
        outperf = 1 if (signal + (momentum / 100.0) * 0.2 + noise) > 0.85 else 0
        rows.append(
            {
                "execution_date": exec_dt.isoformat(),
                "video_id": f"v_{i:04d}",
                "channel_id": f"c_{channel_idx:02d}",
                "views_delta": round(views_delta, 6),
                "engagement_rate": round(engagement, 6),
                "comment_rate": round(comment_rate, 6),
                "video_age_days": float((i % 60) + 1),
                "duration_bucket": "short" if i % 3 == 0 else "medium" if i % 3 == 1 else "long",
                "is_short": "True" if i % 3 == 0 else "False",
                "alpha_score": round(alpha, 6),
                "opportunity_score": round(opp, 6),
                "trend_burst_score": round(rng.random() * 100, 6),
                "evergreen_score": round(rng.random() * 100, 6),
                "packaging_problem_score": round(rng.random() * 100, 6),
                "metric_confidence_score": round(40 + rng.random() * 60, 6),
                "channel_momentum_score": round(momentum, 6),
                "channel_relative_success_score": round(30 + rng.random() * 70, 6),
                "title_length_chars": float(20 + (i % 70)),
                "has_number": "True" if i % 2 == 0 else "False",
                "has_question": "True" if i % 5 == 0 else "False",
                "has_ai_word": "True" if i % 4 == 0 else "False",
                "has_finance_word": "True" if i % 7 == 0 else "False",
                "metadata_changed": "True" if i % 9 == 0 else "False",
                "is_top_growth_7d": "True" if is_top else "False",
                "future_log_views_delta_7d": future_log,
                "future_views_delta_7d": future_views,
                "outperforms_channel_7d": "True" if outperf else "False",
            }
        )
    return rows


def smoke_test_model_training(*, output_dir: str | Path = "build/model_smoke_test", n_rows: int = 500) -> dict[str, Any]:
    root = Path(output_dir)
    if not _HAS_SKLEARN or not _HAS_JOBLIB:
        missing = []
        if not _HAS_SKLEARN:
            missing.append("sklearn")
        if not _HAS_JOBLIB:
            missing.append("joblib")
        report = {
            "status": "skipped_missing_ml_dependencies",
            "models_trained": 0,
            "predictions_generated": 0,
            "leaderboard_rows": 0,
            "feature_importance_rows": 0,
            "artifact_path": str(root / "model_artifact"),
            "warnings": [f"missing_dependency:{m}" for m in missing],
        }
        _write_json(root / "smoke_test_report.json", report)
        return report

    data_dir = root / "data"
    modeling_dir = data_dir / "modeling"
    rows = _synth_rows(n_rows)
    sup_cols = ["execution_date", "video_id", "channel_id", *_FEATURES, "is_top_growth_7d", "future_log_views_delta_7d", "future_views_delta_7d", "outperforms_channel_7d"]
    _write_csv(modeling_dir / "supervised_examples.csv", sup_cols, rows)
    _write_csv(modeling_dir / "latest_inference_examples.csv", ["video_id", "execution_date", "channel_id", *_FEATURES], [{k: r.get(k, "") for k in ["video_id", "execution_date", "channel_id", *_FEATURES]} for r in rows[: min(120, len(rows))]])
    _write_json(modeling_dir / "feature_dictionary.json", {"features": [{"name": f} for f in _FEATURES]})
    _write_json(modeling_dir / "model_readiness_report.json", {"recommended_status": "ready_for_baseline", "trainable_examples": n_rows})
    _write_json(data_dir / "model_registry" / "latest_model_manifest.json", {"status": "none"})

    cfg = {
        "model_suite": {
            "targets": [
                {"name": "is_top_growth_7d", "task_type": "classification", "horizon": "7d", "champion_metric": "precision_at_10"},
                {"name": "future_log_views_delta_7d", "task_type": "regression", "horizon": "7d", "champion_metric": "spearman_corr"},
                {"name": "outperforms_channel_7d", "task_type": "classification", "horizon": "7d", "champion_metric": "precision_at_10"},
            ],
            "models": ["linear_regularized", "random_forest", "shallow_tree"],
            "random_state": 42,
            "validation": {"validation_fraction": 0.25},
            "artifact_retention_days": 7,
        }
    }
    cfg_path = root / "modeling_smoke.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    train = train_model_suite(data_dir=data_dir, modeling_config_path=cfg_path, artifact_dir=root / "model_artifact")
    if train.get("status") != "success":
        report = {
            "status": "failed",
            "models_trained": 0,
            "predictions_generated": 0,
            "leaderboard_rows": 0,
            "feature_importance_rows": 0,
            "artifact_path": str(root / "model_artifact"),
            "warnings": [f"train_status:{train.get('status')}"],
        }
        _write_json(root / "smoke_test_report.json", report)
        return report

    pred = predict_with_model_artifact(model_dir=root / "model_artifact", data_dir=data_dir, output_dir=root / "predictions", target="is_top_growth_7d")
    leaderboard_rows = list(csv.DictReader((root / "model_artifact" / "model_leaderboard.csv").open("r", encoding="utf-8", newline="")))
    fi_rows = list(csv.DictReader((root / "model_artifact" / "feature_importance_global.csv").open("r", encoding="utf-8", newline="")))

    report = {
        "status": "success",
        "models_trained": int(train.get("trained_models", 0)),
        "predictions_generated": int(pred.get("prediction_rows", 0)),
        "leaderboard_rows": len(leaderboard_rows),
        "feature_importance_rows": len(fi_rows),
        "artifact_path": str(root / "model_artifact"),
        "warnings": train.get("warnings", []) + pred.get("warnings", []),
    }
    _write_json(root / "smoke_test_report.json", report)
    return report
