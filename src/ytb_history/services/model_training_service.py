"""Interpretable model suite training and registry update services."""

from __future__ import annotations

import csv
import json
import math
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
try:
    import joblib
    _HAS_JOBLIB = True
except Exception:  # pragma: no cover - env fallback
    joblib = None
    _HAS_JOBLIB = False

try:
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.inspection import permutation_importance
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_text
    _HAS_SKLEARN = True
except Exception:  # pragma: no cover - env fallback
    _HAS_SKLEARN = False

from ytb_history.utils.hashing import fingerprint_text

_TARGET_COLUMNS = {
    "future_views_delta_7d",
    "future_log_views_delta_7d",
    "is_top_growth_7d",
    "outperforms_channel_7d",
}

_INTERPRETABILITY = {
    "linear_regularized": "high",
    "shallow_tree": "high",
    "random_forest": "medium",
}


def _read_json(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    text = str(value).strip().lower()
    if text in {"true", "yes", "y"}:
        return 1.0
    if text in {"false", "no", "n"}:
        return 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _safe_label(value: Any) -> int:
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "y"} else 0


def _parse_dt(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _load_modeling_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _get_git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip() or None
    except Exception:
        return None


def _resolve_feature_list(feature_dictionary: dict[str, Any]) -> list[str]:
    features = feature_dictionary.get("features", [])
    names = [str(item.get("name")) for item in features if isinstance(item, dict) and item.get("name")]
    return [name for name in names if not name.startswith("future_") and name not in _TARGET_COLUMNS]


def _build_matrix(rows: list[dict[str, str]], feature_list: list[str]) -> list[list[float]]:
    return [[_safe_float(row.get(feature)) for feature in feature_list] for row in rows]


def _precision_recall_at_k(y_true: list[int], scores: list[float], k: int = 10) -> tuple[float, float]:
    if not y_true:
        return 0.0, 0.0
    top_k = min(k, len(y_true))
    ranked = sorted(zip(scores, y_true), key=lambda item: item[0], reverse=True)
    positives_top = sum(int(item[1]) for item in ranked[:top_k])
    positives_total = sum(y_true)
    precision = positives_top / top_k if top_k else 0.0
    recall = positives_top / positives_total if positives_total else 0.0
    return round(precision, 6), round(recall, 6)


def _spearman_corr(y_true: list[float], y_pred: list[float]) -> float:
    if len(y_true) < 2:
        return 0.0
    order_true = {idx: rank for rank, idx in enumerate(sorted(range(len(y_true)), key=lambda i: y_true[i]))}
    order_pred = {idx: rank for rank, idx in enumerate(sorted(range(len(y_pred)), key=lambda i: y_pred[i]))}
    diffs = [(order_true[i] - order_pred[i]) ** 2 for i in range(len(y_true))]
    n = len(y_true)
    return round(1 - ((6 * sum(diffs)) / (n * (n * n - 1))), 6)


def _classification_metrics(y_true: list[int], scores: list[float], baseline_scores: dict[str, list[float]]) -> dict[str, float | None]:
    precision, recall = _precision_recall_at_k(y_true, scores)
    metrics: dict[str, float | None] = {
        "precision_at_10": precision,
        "recall_at_10": recall,
        "roc_auc": None,
        "pr_auc": None,
        "brier_score": None,
    }
    if len(set(y_true)) == 2:
        metrics["roc_auc"] = round(float(roc_auc_score(y_true, scores)), 6)
        metrics["brier_score"] = round(float(brier_score_loss(y_true, scores)), 6)
    if any(y_true):
        metrics["pr_auc"] = round(float(average_precision_score(y_true, scores)), 6)
    for key, vals in baseline_scores.items():
        b_precision, _ = _precision_recall_at_k(y_true, vals)
        metrics[f"lift_vs_baseline_{key.replace('_score', '').replace('views_delta', 'views_delta')}"] = round(precision - b_precision, 6)
    return metrics


def _regression_metrics(y_true: list[float], preds: list[float], baseline_scores: dict[str, list[float]]) -> dict[str, float | None]:
    if not y_true:
        return {"mae_log": None, "rmse_log": None, "spearman_corr": None, "top_10_overlap_with_actual": None}
    errors = [abs(a - b) for a, b in zip(y_true, preds)]
    sq = [(a - b) ** 2 for a, b in zip(y_true, preds)]
    top_model = {idx for idx, _ in sorted(enumerate(preds), key=lambda x: x[1], reverse=True)[: min(10, len(preds))]}
    top_true = {idx for idx, _ in sorted(enumerate(y_true), key=lambda x: x[1], reverse=True)[: min(10, len(y_true))]}
    overlap = len(top_model & top_true) / max(1, min(10, len(y_true)))
    metrics: dict[str, float | None] = {
        "mae_log": round(sum(errors) / len(errors), 6),
        "rmse_log": round(math.sqrt(sum(sq) / len(sq)), 6),
        "spearman_corr": _spearman_corr(y_true, preds),
        "top_10_overlap_with_actual": round(overlap, 6),
    }
    model_metric = float(metrics["spearman_corr"] or 0.0)
    for key, vals in baseline_scores.items():
        b_metric = _spearman_corr(y_true, vals)
        metrics[f"lift_vs_baseline_{key.replace('_score', '').replace('views_delta', 'views_delta')}"] = round(model_metric - b_metric, 6)
    return metrics


def _direction_from_bins(values: list[float], predictions: list[float]) -> tuple[str, float, float, float]:
    if len(values) < 4:
        return "flat", 0.0, 0.0, 0.0
    ordered = sorted(zip(values, predictions), key=lambda x: x[0])
    q = max(1, len(ordered) // 4)
    low = [p for _, p in ordered[:q]]
    high = [p for _, p in ordered[-q:]]
    low_avg = sum(low) / len(low)
    high_avg = sum(high) / len(high)
    diff = high_avg - low_avg
    direction = "flat"
    if diff > 0.02:
        direction = "positive"
    elif diff < -0.02:
        direction = "negative"
    middle = [p for _, p in ordered[q:-q]]
    if middle:
        m_avg = sum(middle) / len(middle)
        if (low_avg < m_avg > high_avg) or (low_avg > m_avg < high_avg):
            direction = "mixed"
    return direction, round(diff, 6), round(low_avg, 6), round(high_avg, 6)


def train_model_suite(*, data_dir: str | Path = "data", modeling_config_path: str | Path = "config/modeling.yaml", artifact_dir: str | Path = "build/model_artifact") -> dict[str, Any]:
    if not _HAS_SKLEARN or not _HAS_JOBLIB:
        missing: list[str] = []
        if not _HAS_SKLEARN:
            missing.append("sklearn")
        if not _HAS_JOBLIB:
            missing.append("joblib")
        return {
            "status": "failed_missing_ml_dependencies",
            "reason": "missing_ml_dependencies",
            "missing_dependencies": missing,
            "warnings": [f"missing_dependency:{name}" for name in missing],
        }

    data_root = Path(data_dir)
    modeling_dir = data_root / "modeling"
    readiness = _read_json(modeling_dir / "model_readiness_report.json") if (modeling_dir / "model_readiness_report.json").exists() else {}
    recommended_status = str(readiness.get("recommended_status") or "not_ready")
    if recommended_status == "not_ready":
        return {"status": "skipped_not_ready", "reason": "model_readiness_not_ready", "recommended_status": recommended_status}

    supervised_path = modeling_dir / "supervised_examples.csv"
    feature_dictionary_path = modeling_dir / "feature_dictionary.json"
    if not supervised_path.exists() or not feature_dictionary_path.exists():
        return {"status": "failed_missing_inputs", "reason": "missing_supervised_examples_or_feature_dictionary"}

    rows = [row for row in _read_csv(supervised_path) if row.get("execution_date")]
    if len(rows) < 8:
        return {"status": "skipped_not_ready", "reason": "insufficient_examples", "total_examples": len(rows)}

    config = _load_modeling_config(Path(modeling_config_path))
    suite_cfg = config.get("model_suite") or {}
    targets_cfg = suite_cfg.get("targets") or []
    model_families = suite_cfg.get("models") or ["linear_regularized", "random_forest", "shallow_tree"]
    random_state = int(suite_cfg.get("random_state", 42))
    validation_fraction = float((suite_cfg.get("validation") or {}).get("validation_fraction", 0.25))

    feature_dictionary = _read_json(feature_dictionary_path)
    feature_list = _resolve_feature_list(feature_dictionary)
    if not feature_list:
        return {"status": "failed_no_features", "reason": "feature_dictionary_has_no_allowed_features"}

    enriched = [{"raw": row, "execution_date": _parse_dt(str(row["execution_date"]))} for row in rows]
    enriched.sort(key=lambda x: x["execution_date"])
    val_count = max(1, math.ceil(len(enriched) * validation_fraction))
    train_rows = enriched[:-val_count]
    valid_rows = enriched[-val_count:]
    if not train_rows or not valid_rows:
        return {"status": "failed_temporal_split", "reason": "could_not_create_train_and_validation_splits"}

    created_at = datetime.now(timezone.utc)
    retention_days = int(suite_cfg.get("artifact_retention_days", config.get("artifact_retention_days", 30)))
    suite_id = f"suite-{created_at.strftime('%Y%m%dT%H%M%SZ')}"
    artifact_path = Path(artifact_dir)
    models_root = artifact_path / "models"
    cards_root = artifact_path / "model_cards"
    models_root.mkdir(parents=True, exist_ok=True)
    cards_root.mkdir(parents=True, exist_ok=True)

    leaderboard_rows: list[dict[str, Any]] = []
    global_importance: list[dict[str, Any]] = []
    global_direction: list[dict[str, Any]] = []
    model_records: list[dict[str, Any]] = []
    warnings: list[str] = []

    train_x = _build_matrix([item["raw"] for item in train_rows], feature_list)
    valid_x = _build_matrix([item["raw"] for item in valid_rows], feature_list)

    for target_cfg in targets_cfg:
        target = str(target_cfg.get("name"))
        task_type = str(target_cfg.get("task_type", "classification"))
        horizon = str(target_cfg.get("horizon", "7d"))
        champion_metric = str(target_cfg.get("champion_metric", "precision_at_10"))

        if task_type == "classification":
            y_train: list[float] = [_safe_label(item["raw"].get(target)) for item in train_rows]
            y_valid: list[float] = [_safe_label(item["raw"].get(target)) for item in valid_rows]
        else:
            y_train = [_safe_float(item["raw"].get(target)) for item in train_rows]
            y_valid = [_safe_float(item["raw"].get(target)) for item in valid_rows]

        baselines: dict[str, list[float]] = {
            "alpha": [_safe_float(item["raw"].get("alpha_score")) for item in valid_rows],
            "views_delta": [_safe_float(item["raw"].get("views_delta")) for item in valid_rows],
        }
        if any("decision_score" in item["raw"] for item in valid_rows):
            baselines["decision"] = [_safe_float(item["raw"].get("decision_score")) for item in valid_rows]

        for model_family in model_families:
            model_id = f"{model_family}-{target}-{created_at.strftime('%Y%m%dT%H%M%SZ')}"
            model_dir = models_root / model_family
            model_dir.mkdir(parents=True, exist_ok=True)
            try:
                if model_family == "linear_regularized":
                    if task_type == "classification":
                        est = LogisticRegression(max_iter=1000, random_state=random_state)
                    else:
                        est = Ridge(random_state=random_state)
                    model = Pipeline([("scaler", StandardScaler()), ("model", est)])
                elif model_family == "random_forest":
                    if task_type == "classification":
                        model = RandomForestClassifier(
                            n_estimators=int(suite_cfg.get("random_forest_n_estimators", 200)),
                            max_depth=int(suite_cfg.get("random_forest_max_depth", 6)),
                            random_state=random_state,
                        )
                    else:
                        model = RandomForestRegressor(
                            n_estimators=int(suite_cfg.get("random_forest_n_estimators", 200)),
                            max_depth=int(suite_cfg.get("random_forest_max_depth", 6)),
                            random_state=random_state,
                        )
                else:
                    if task_type == "classification":
                        model = DecisionTreeClassifier(max_depth=int(suite_cfg.get("shallow_tree_max_depth", 4)), random_state=random_state)
                    else:
                        model = DecisionTreeRegressor(max_depth=int(suite_cfg.get("shallow_tree_max_depth", 4)), random_state=random_state)

                model.fit(train_x, y_train)
                if task_type == "classification":
                    proba = model.predict_proba(valid_x)
                    if hasattr(proba, "tolist"):
                        proba = proba.tolist()
                    preds = [float(row[1] if isinstance(row, list) and len(row) > 1 else row[0] if isinstance(row, list) and row else 0.0) for row in proba]
                    metrics = _classification_metrics([int(v) for v in y_valid], preds, baselines)
                else:
                    raw_preds = model.predict(valid_x)
                    if hasattr(raw_preds, "tolist"):
                        raw_preds = raw_preds.tolist()
                    preds = [float(v) for v in raw_preds]
                    metrics = _regression_metrics(y_valid, preds, baselines)

                coef_rows: list[dict[str, Any]] = []
                if model_family == "linear_regularized":
                    inner = model.named_steps["model"] if isinstance(model, Pipeline) else model
                    raw_coeffs = getattr(inner, "coef_", [])
                    if hasattr(raw_coeffs, "tolist"):
                        raw_coeffs = raw_coeffs.tolist()
                    if isinstance(raw_coeffs, (int, float)):
                        coeffs = [float(raw_coeffs)]
                    elif raw_coeffs and isinstance(raw_coeffs, list) and raw_coeffs and isinstance(raw_coeffs[0], list):
                        coeffs = [float(v) for v in raw_coeffs[0]]
                    else:
                        coeffs = [float(v) for v in raw_coeffs] if raw_coeffs else []
                    for feature, coef in zip(feature_list, coeffs):
                        coef_rows.append(
                            {
                                "feature": feature,
                                "coefficient": round(float(coef), 8),
                                "standardized_coefficient": round(float(coef), 8),
                                "direction": "positive" if coef >= 0 else "negative",
                            }
                        )
                    _write_csv(model_dir / "coefficients.csv", ["feature", "coefficient", "standardized_coefficient", "direction"], coef_rows)

                if model_family == "random_forest":
                    pi = permutation_importance(model, valid_x, y_valid, n_repeats=3, random_state=random_state)
                    pi_rows = []
                    for idx, feature in enumerate(feature_list):
                        pi_rows.append(
                            {
                                "feature": feature,
                                "permutation_importance_mean": round(float(pi.importances_mean[idx]), 8),
                                "permutation_importance_std": round(float(pi.importances_std[idx]), 8),
                            }
                        )
                    _write_csv(model_dir / "permutation_importance.csv", ["feature", "permutation_importance_mean", "permutation_importance_std"], pi_rows)

                    dir_rows = []
                    for idx, feature in enumerate(feature_list):
                        vals = [row[idx] for row in valid_x]
                        direction, score, low, high = _direction_from_bins(vals, [float(v) for v in preds])
                        dir_rows.append({"feature": feature, "direction": direction, "direction_score": score, "direction_method": "quantile_bins", "low_bin_prediction": low, "high_bin_prediction": high, "notes": "direction estimated via directional analysis"})
                    _write_csv(model_dir / "feature_direction.csv", ["feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction", "notes"], dir_rows)

                if model_family == "shallow_tree":
                    rules = export_text(model, feature_names=feature_list)
                    (model_dir / "tree_rules.txt").write_text(rules, encoding="utf-8")
                    fi = getattr(model, "feature_importances_", [0.0] * len(feature_list))
                    fi_rows = [{"feature": feature, "importance": round(float(fi[idx]), 8)} for idx, feature in enumerate(feature_list)]
                    _write_csv(model_dir / "feature_importance.csv", ["feature", "importance"], fi_rows)
                    dir_rows = []
                    for idx, feature in enumerate(feature_list):
                        vals = [row[idx] for row in valid_x]
                        direction, score, low, high = _direction_from_bins(vals, [float(v) for v in preds])
                        dir_rows.append({"feature": feature, "direction": direction, "direction_score": score, "direction_method": "quantile_bins", "low_bin_prediction": low, "high_bin_prediction": high, "notes": "estimated from prediction behavior"})
                    _write_csv(model_dir / "feature_direction.csv", ["feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction", "notes"], dir_rows)

                model_card = f"# Model card\n\n- model_id: {model_id}\n- model_family: {model_family}\n- target: {target}\n- task_type: {task_type}\n- interpretability_level: {_INTERPRETABILITY.get(model_family, 'medium')}\n"
                (model_dir / "model_card.md").write_text(model_card, encoding="utf-8")
                (cards_root / f"{model_id}.md").write_text(model_card, encoding="utf-8")

                payload = {"model": model, "feature_list": feature_list, "target": target, "task_type": task_type, "model_family": model_family, "model_id": model_id}
                joblib.dump(payload, model_dir / "model.joblib")
                _write_json(model_dir / "preprocessing.json", {"split": "temporal", "validation_fraction": validation_fraction})
                _write_json(model_dir / "feature_list.json", {"features": feature_list})
                _write_json(model_dir / "metrics.json", metrics)

                champion_value = float(metrics.get(champion_metric) or -999.0)
                baseline_metric_value = max([float(metrics.get(k, -999.0) or -999.0) for k in metrics if k.startswith("lift_vs_baseline_")] + [-999.0])

                leader = {
                    "model_id": model_id,
                    "model_family": model_family,
                    "task_type": task_type,
                    "target": target,
                    "horizon": horizon,
                    "train_examples": len(train_rows),
                    "validation_examples": len(valid_rows),
                    "precision_at_10": metrics.get("precision_at_10"),
                    "recall_at_10": metrics.get("recall_at_10"),
                    "roc_auc": metrics.get("roc_auc"),
                    "pr_auc": metrics.get("pr_auc"),
                    "brier_score": metrics.get("brier_score"),
                    "mae_log": metrics.get("mae_log"),
                    "rmse_log": metrics.get("rmse_log"),
                    "spearman_corr": metrics.get("spearman_corr"),
                    "champion_metric": champion_metric,
                    "champion_metric_value": champion_value,
                    "baseline_metric_value": baseline_metric_value,
                    "lift_vs_best_baseline": baseline_metric_value,
                    "selected_as_champion": False,
                    "interpretability_level": _INTERPRETABILITY.get(model_family, "medium"),
                    "artifact_name": None,
                    "workflow_run_id": None,
                    "created_at": created_at.isoformat(),
                }
                leaderboard_rows.append(leader)
                model_records.append({"model_id": model_id, "model_family": model_family, "target": target, "task_type": task_type, "metrics": metrics, "path": f"models/{model_family}"})

                for rank, feature in enumerate(feature_list, start=1):
                    global_importance.append({"model_id": model_id, "model_family": model_family, "target": target, "feature": feature, "importance_type": "model_native", "importance_value": float(rank * -1), "importance_rank": rank, "direction": None, "direction_method": None, "coefficient": None, "standardized_coefficient": None, "permutation_importance_mean": None, "permutation_importance_std": None, "notes": ""})
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"model_failed:{model_family}:{target}:{exc}")

    if not leaderboard_rows:
        return {"status": "failed_no_models_trained", "warnings": warnings}

    champions: dict[str, dict[str, Any]] = {}
    for target in {row["target"] for row in leaderboard_rows}:
        candidates = [row for row in leaderboard_rows if row["target"] == target]
        best = max(candidates, key=lambda x: float(x.get("champion_metric_value") or -999.0))
        best["selected_as_champion"] = True
        champions[target] = {
            "model_id": best["model_id"],
            "model_family": best["model_family"],
            "metric": best["champion_metric"],
            "metric_value": best["champion_metric_value"],
        }

    for row in leaderboard_rows:
        model_path = models_root / row["model_family"]
        _write_json(model_path / "training_manifest.json", {"model_id": row["model_id"], "target": row["target"], "task_type": row["task_type"], "suite_id": suite_id, "created_at": created_at.isoformat(), "expires_at_estimate": (created_at + timedelta(days=retention_days)).isoformat(), "feature_list_sha256": fingerprint_text(json.dumps(feature_list, sort_keys=True)), "metrics": {k: row.get(k) for k in ["precision_at_10", "recall_at_10", "roc_auc", "pr_auc", "brier_score", "mae_log", "rmse_log", "spearman_corr"]}})

    lb_fields = list(leaderboard_rows[0].keys())
    _write_csv(artifact_path / "model_leaderboard.csv", lb_fields, leaderboard_rows)
    _write_json(artifact_path / "model_leaderboard.json", {"rows": leaderboard_rows})

    feature_direction_rows: list[dict[str, Any]] = []
    for row in leaderboard_rows:
        dpath = models_root / row["model_family"] / "feature_direction.csv"
        if dpath.exists():
            for drow in _read_csv(dpath):
                feature_direction_rows.append({"model_id": row["model_id"], "model_family": row["model_family"], "target": row["target"], **drow})

    _write_csv(artifact_path / "feature_importance_global.csv", list(global_importance[0].keys()), global_importance)
    if feature_direction_rows:
        _write_csv(artifact_path / "feature_direction_global.csv", list(feature_direction_rows[0].keys()), feature_direction_rows)
    else:
        _write_csv(artifact_path / "feature_direction_global.csv", ["model_id", "model_family", "target", "feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction", "notes"], [])

    suite_manifest = {
        "schema_version": "model_suite_manifest_v1",
        "status": "valid",
        "suite_id": suite_id,
        "created_at": created_at.isoformat(),
        "expires_at_estimate": (created_at + timedelta(days=retention_days)).isoformat(),
        "champions": champions,
        "models": model_records,
        "warnings": warnings,
        "git_sha": _get_git_sha(),
    }
    _write_json(artifact_path / "suite_manifest.json", suite_manifest)

    report_dir = data_root / "model_reports"
    _write_csv(report_dir / "latest_model_leaderboard.csv", lb_fields, leaderboard_rows)
    _write_json(report_dir / "latest_model_leaderboard.json", {"rows": leaderboard_rows})
    _write_csv(report_dir / "latest_feature_importance.csv", list(global_importance[0].keys()), global_importance)
    if feature_direction_rows:
        _write_csv(report_dir / "latest_feature_direction.csv", list(feature_direction_rows[0].keys()), feature_direction_rows)
    else:
        _write_csv(report_dir / "latest_feature_direction.csv", ["model_id", "model_family", "target", "feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction", "notes"], [])

    md = "# Model Suite Report\n\n"
    md += "## Champions by target\n"
    for target, c in champions.items():
        md += f"- {target}: {c['model_id']} ({c['metric']}={c['metric_value']})\n"
    md += "\n**Advertencia**: RF direction es estimada por directional analysis, no por impurity importance.\n"
    (report_dir / "latest_model_suite_report.md").write_text(md, encoding="utf-8")
    (report_dir / "latest_model_suite_report.html").write_text(f"<html><body><pre>{md}</pre></body></html>", encoding="utf-8")

    return {"status": "success", "suite_id": suite_id, "trained_models": len(leaderboard_rows), "champions": champions, "warnings": warnings}


def train_baseline_model(*, data_dir: str | Path = "data", modeling_config_path: str | Path = "config/modeling.yaml", artifact_dir: str | Path = "build/model_artifact") -> dict[str, Any]:
    """Temporary alias to the model suite trainer."""
    return train_model_suite(data_dir=data_dir, modeling_config_path=modeling_config_path, artifact_dir=artifact_dir)


def register_trained_artifact(*, artifact_name: str, workflow_run_id: str, artifact_dir: str | Path = "build/model_artifact", data_dir: str | Path = "data") -> dict[str, Any]:
    artifact_path = Path(artifact_dir)
    suite_manifest_path = artifact_path / "suite_manifest.json"
    if not suite_manifest_path.exists():
        return {"status": "skipped_no_artifact", "reason": "suite_manifest_missing", "artifact_dir": str(artifact_path)}

    suite_manifest = _read_json(suite_manifest_path)
    latest_manifest = {
        "schema_version": "latest_model_manifest_v1",
        "status": suite_manifest.get("status", "valid"),
        "suite_id": suite_manifest.get("suite_id"),
        "artifact_name": artifact_name,
        "workflow_run_id": workflow_run_id,
        "created_at": suite_manifest.get("created_at"),
        "expires_at_estimate": suite_manifest.get("expires_at_estimate"),
        "champions": suite_manifest.get("champions", {}),
        "models": suite_manifest.get("models", []),
        "warnings": suite_manifest.get("warnings", []),
    }

    data_root = Path(data_dir)
    registry_dir = data_root / "model_registry"
    registry_dir.mkdir(parents=True, exist_ok=True)
    latest_manifest_path = registry_dir / "latest_model_manifest.json"
    runs_index_path = registry_dir / "training_runs_index.json"
    runs_index = _read_json(runs_index_path) if runs_index_path.exists() else {"schema_version": "training_runs_index_v1", "runs": []}
    runs = runs_index.get("runs", []) if isinstance(runs_index.get("runs"), list) else []
    runs.append({"suite_id": latest_manifest.get("suite_id"), "created_at": latest_manifest.get("created_at"), "artifact_name": artifact_name, "workflow_run_id": workflow_run_id, "champions": latest_manifest.get("champions", {}), "models": latest_manifest.get("models", [])})

    _write_json(latest_manifest_path, latest_manifest)
    _write_json(runs_index_path, {"schema_version": "training_runs_index_v1", "runs": runs})
    return {"status": "success", "latest_model_manifest": str(latest_manifest_path), "training_runs_index": str(runs_index_path), "suite_id": latest_manifest.get("suite_id"), "artifact_name": artifact_name, "workflow_run_id": workflow_run_id}
