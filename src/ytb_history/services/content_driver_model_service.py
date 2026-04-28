"""Train supervised content driver regression models with NLP/topic features."""

from __future__ import annotations

import csv
import json
import math
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.inspection import permutation_importance
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.tree import DecisionTreeRegressor, export_text

    _HAS_SKLEARN = True
except Exception:  # pragma: no cover
    _HAS_SKLEARN = False

TARGETS = [
    "future_log_views_delta_7d",
    "future_relative_views_delta_7d",
    "future_engagement_delta_7d",
    "content_value_score_7d",
]

LEADERBOARD_COLUMNS = [
    "target",
    "model_family",
    "mae_log",
    "rmse_log",
    "spearman_corr",
    "top_10_overlap_with_actual",
    "precision_at_top_decile_regression",
    "lift_vs_baseline_alpha",
    "lift_vs_baseline_decision",
    "lift_vs_baseline_views_delta",
    "train_rows",
    "valid_rows",
]

FEATURE_IMPORTANCE_COLUMNS = [
    "target",
    "model_family",
    "feature",
    "feature_group",
    "importance_type",
    "importance_value",
    "importance_rank",
    "direction",
    "notes",
]

FEATURE_DIRECTION_COLUMNS = [
    "target",
    "model_family",
    "feature",
    "feature_group",
    "direction",
    "direction_score",
    "low_bin_prediction",
    "high_bin_prediction",
    "direction_method",
    "notes",
]

GROUP_IMPORTANCE_COLUMNS = [
    "target",
    "model_family",
    "feature_group",
    "group_importance",
    "feature_count",
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    text = str(value).strip().lower()
    if text in {"true", "yes", "y"}:
        return 1.0
    if text in {"false", "no", "n"}:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(value: str) -> datetime:
    normalized = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _rank_percentile(values: list[float]) -> list[float]:
    if not values:
        return []
    ranked = sorted(range(len(values)), key=lambda i: values[i])
    if len(values) == 1:
        return [50.0]
    out = [0.0] * len(values)
    denom = len(values) - 1
    for pos, idx in enumerate(ranked):
        out[idx] = (pos / denom) * 100.0
    return out


def _spearman(y_true: list[float], y_pred: list[float]) -> float:
    if len(y_true) < 2:
        return 0.0
    order_true = {i: rank for rank, i in enumerate(sorted(range(len(y_true)), key=lambda j: y_true[j]))}
    order_pred = {i: rank for rank, i in enumerate(sorted(range(len(y_pred)), key=lambda j: y_pred[j]))}
    n = len(y_true)
    diffs = [(order_true[i] - order_pred[i]) ** 2 for i in range(n)]
    return 1 - (6 * sum(diffs) / (n * (n * n - 1)))


def _top_overlap(y_true: list[float], y_pred: list[float], k: int) -> float:
    if not y_true:
        return 0.0
    top = min(k, len(y_true))
    true_idx = {i for i, _ in sorted(enumerate(y_true), key=lambda x: x[1], reverse=True)[:top]}
    pred_idx = {i for i, _ in sorted(enumerate(y_pred), key=lambda x: x[1], reverse=True)[:top]}
    return len(true_idx & pred_idx) / max(1, top)


def _direction_from_quantiles(feature_values: list[float], preds: list[float]) -> tuple[str, float, float, float]:
    if len(feature_values) < 6:
        return "mixed", 0.0, 0.0, 0.0
    ordered = sorted(zip(feature_values, preds), key=lambda x: x[0])
    q = max(1, len(ordered) // 5)
    low = [p for _, p in ordered[:q]]
    high = [p for _, p in ordered[-q:]]
    low_avg = sum(low) / len(low)
    high_avg = sum(high) / len(high)
    diff = high_avg - low_avg
    if diff > 0.01:
        direction = "positive"
    elif diff < -0.01:
        direction = "negative"
    else:
        direction = "mixed"
    return direction, diff, low_avg, high_avg


def _feature_group(name: str) -> str:
    lowered = name.lower()
    if any(token in lowered for token in ["title_", "hook_", "pattern"]):
        return "title_style"
    if "semantic_score" in lowered or any(token in lowered for token in ["ai_semantic", "finance_semantic", "news_semantic", "tutorial_semantic"]):
        return "semantic_scores"
    if lowered.startswith("lsa_"):
        return "semantic_lsa"
    if "cluster" in lowered:
        return "semantic_cluster"
    if "topic_" in lowered:
        return "topic_metrics"
    if any(token in lowered for token in ["channel_", "relative_growth", "median_views"]):
        return "channel_context"
    if any(token in lowered for token in ["engagement", "comment_rate", "alpha_score", "opportunity_score"]):
        return "engagement_context"
    if any(token in lowered for token in ["age", "freshness", "execution", "upload"]):
        return "timing"
    if any(token in lowered for token in ["metadata", "duration", "is_short", "format", "title_changed", "description_changed"]):
        return "metadata"
    if any(token in lowered for token in ["decision", "hybrid", "model_score"]):
        return "model_decision"
    return "engagement_context"


def _prepare_rows(data_dir: Path, warnings: list[str]) -> list[dict[str, Any]]:
    modeling = data_dir / "modeling" / "supervised_examples.csv"
    if not modeling.exists():
        warnings.append(f"Missing required file: {modeling}")
        return []

    rows = _read_csv(modeling)
    nlp_path = data_dir / "nlp_features" / "latest_video_nlp_features.csv"
    topic_path = data_dir / "topic_intelligence" / "latest_video_topics.csv"

    nlp_by_video = {row.get("video_id", ""): row for row in _read_csv(nlp_path)} if nlp_path.exists() else {}
    topic_by_video = {row.get("video_id", ""): row for row in _read_csv(topic_path)} if topic_path.exists() else {}

    if not nlp_by_video:
        warnings.append(f"NLP features not found: {nlp_path}")
    if not topic_by_video:
        warnings.append(f"Topic intelligence not found: {topic_path}")

    merged: list[dict[str, Any]] = []
    for row in rows:
        video_id = str(row.get("video_id", ""))
        merged.append({**row, **nlp_by_video.get(video_id, {}), **topic_by_video.get(video_id, {})})
    return merged


def _derive_targets(rows: list[dict[str, Any]], warnings: list[str]) -> None:
    channel_baseline: dict[str, float] = {}
    by_channel: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_channel[str(row.get("channel_id", ""))].append(_safe_float(row.get("views_delta")))
    for channel, values in by_channel.items():
        sorted_vals = sorted(values)
        channel_baseline[channel] = sorted_vals[len(sorted_vals) // 2] if sorted_vals else 1.0

    for row in rows:
        if "future_relative_views_delta_7d" not in row or row.get("future_relative_views_delta_7d") in (None, ""):
            baseline = channel_baseline.get(str(row.get("channel_id", "")), 1.0) or 1.0
            row["future_relative_views_delta_7d"] = _safe_float(row.get("future_views_delta_7d")) / baseline

        if "future_engagement_delta_7d" not in row or row.get("future_engagement_delta_7d") in (None, ""):
            likes = _safe_float(row.get("future_likes_delta_7d"))
            comments = _safe_float(row.get("future_comments_delta_7d"))
            row["future_engagement_delta_7d"] = likes + comments

    rel = [_safe_float(row.get("future_relative_views_delta_7d")) for row in rows]
    eng = [_safe_float(row.get("future_engagement_delta_7d")) for row in rows]
    com = [_safe_float(row.get("future_comments_delta_7d")) for row in rows]
    rel_pct = _rank_percentile(rel)
    eng_pct = _rank_percentile(eng)
    com_pct = _rank_percentile(com)

    for idx, row in enumerate(rows):
        components: list[tuple[float, float]] = []
        components.append((0.55, rel_pct[idx]))
        components.append((0.25, eng_pct[idx]))
        if any(com):
            components.append((0.15, com_pct[idx]))
        conf = _safe_float(row.get("metric_confidence_score"))
        if conf > 0:
            components.append((0.05, conf))

        total_weight = sum(weight for weight, _ in components)
        if total_weight <= 0:
            row["content_value_score_7d"] = 0.0
        else:
            row["content_value_score_7d"] = sum((weight / total_weight) * value for weight, value in components)

    if not any(com):
        warnings.append("future_comments_delta_7d missing or empty; content_value_score_7d renormalized without comments component.")


def _build_feature_space(rows: list[dict[str, Any]]) -> tuple[list[str], dict[str, list[str]]]:
    banned_exact = set(TARGETS) | {"future_views_delta_7d", "future_log_views_delta_7d", "is_top_growth_7d", "outperforms_channel_7d"}
    banned_prefix = ("future_",)
    banned_cols = {"video_id", "title", "source_export_path", "target_date", "execution_date", "channel_name", "channel_id"}

    numeric_features: list[str] = []
    categorical_features: dict[str, list[str]] = {}

    keys = sorted({key for row in rows for key in row.keys()})
    for key in keys:
        if key in banned_cols or key in banned_exact or key.startswith(banned_prefix):
            continue
        values = [str(row.get(key, "")) for row in rows]
        parsed = [_safe_float(row.get(key)) for row in rows]
        is_numeric = any(v not in ("", None) for v in values) and all(v.strip().lower() in {"", "true", "false", "yes", "no", "y", "n"} or _is_float(v) for v in values)
        if is_numeric:
            numeric_features.append(key)
        else:
            uniq = sorted({v for v in values if v})
            if 1 < len(uniq) <= 12:
                categorical_features[key] = uniq

    expanded = list(numeric_features)
    for key, uniq in categorical_features.items():
        expanded.extend([f"{key}__{val}" for val in uniq])
    return expanded, categorical_features


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False


def _vectorize(rows: list[dict[str, Any]], features: list[str], categorical_map: dict[str, list[str]]) -> list[list[float]]:
    matrix: list[list[float]] = []
    for row in rows:
        vector: list[float] = []
        for feature in features:
            if "__" in feature:
                base, cat = feature.split("__", 1)
                vector.append(1.0 if str(row.get(base, "")) == cat else 0.0)
            else:
                vector.append(_safe_float(row.get(feature)))
        matrix.append(vector)
    return matrix


def train_content_driver_models(
    *,
    data_dir: str | Path = "data",
    artifact_dir: str | Path = "build/content_driver_artifact",
    random_forest_n_estimators: int = 200,
    random_forest_max_depth: int = 8,
    random_forest_min_samples_leaf: int = 2,
) -> dict[str, Any]:
    if not _HAS_SKLEARN:
        return {
            "status": "skipped_not_ready",
            "reason": "missing_sklearn",
            "warnings": ["scikit-learn is required for train-content-driver-models."],
        }

    data_root = Path(data_dir)
    warnings: list[str] = []
    rows = _prepare_rows(data_root, warnings)
    if len(rows) < 20:
        return {"status": "skipped_not_ready", "reason": "insufficient_examples", "total_rows": len(rows), "warnings": warnings}

    _derive_targets(rows, warnings)

    valid_rows = [row for row in rows if row.get("execution_date")]
    valid_rows.sort(key=lambda row: _parse_dt(str(row.get("execution_date"))))
    split = max(1, int(len(valid_rows) * 0.8))
    train_rows = valid_rows[:split]
    val_rows = valid_rows[split:]
    if len(train_rows) < 10 or len(val_rows) < 5:
        return {"status": "skipped_not_ready", "reason": "temporal_split_not_ready", "warnings": warnings}

    features, categorical_map = _build_feature_space(valid_rows)
    x_train = _vectorize(train_rows, features, categorical_map)
    x_val = _vectorize(val_rows, features, categorical_map)

    artifact_root = Path(artifact_dir)
    models_dir = artifact_root / "models"
    reports_dir = artifact_root / "reports"
    models_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    leaderboard_rows: list[dict[str, Any]] = []
    importance_rows: list[dict[str, Any]] = []
    direction_rows: list[dict[str, Any]] = []
    group_rows: list[dict[str, Any]] = []

    for target in TARGETS:
        y_train = [_safe_float(row.get(target)) for row in train_rows]
        y_val = [_safe_float(row.get(target)) for row in val_rows]
        if all(value == 0.0 for value in y_train + y_val):
            warnings.append(f"Target skipped due to missing values: {target}")
            continue

        model_specs = {
            "random_forest_regressor": RandomForestRegressor(
                n_estimators=random_forest_n_estimators,
                max_depth=random_forest_max_depth,
                min_samples_leaf=random_forest_min_samples_leaf,
                random_state=42,
            ),
            "linear_regularized_regressor": Pipeline([("scaler", StandardScaler()), ("model", Ridge(random_state=42))]),
            "shallow_tree_regressor": DecisionTreeRegressor(max_depth=4, random_state=42),
        }

        for model_family, model in model_specs.items():
            try:
                model.fit(x_train, y_train)
                preds = [float(value) for value in model.predict(x_val)]
            except Exception as exc:
                warnings.append(f"Model failed for {target}/{model_family}: {exc}")
                continue

            mae = sum(abs(a - b) for a, b in zip(y_val, preds)) / len(y_val)
            rmse = math.sqrt(sum((a - b) ** 2 for a, b in zip(y_val, preds)) / len(y_val))
            spearman = _spearman(y_val, preds)
            top10 = _top_overlap(y_val, preds, 10)
            decile = max(1, int(math.ceil(len(y_val) * 0.1)))
            precision_decile = _top_overlap(y_val, preds, decile)

            idx_pred = sorted(range(len(preds)), key=lambda i: preds[i], reverse=True)[:decile]
            idx_alpha = sorted(range(len(y_val)), key=lambda i: _safe_float(val_rows[i].get("alpha_score")), reverse=True)[:decile]
            idx_decision = sorted(range(len(y_val)), key=lambda i: _safe_float(val_rows[i].get("decision_score")), reverse=True)[:decile]
            idx_views = sorted(range(len(y_val)), key=lambda i: _safe_float(val_rows[i].get("views_delta")), reverse=True)[:decile]

            mean_pred_top = sum(y_val[i] for i in idx_pred) / len(idx_pred)
            lift_alpha = mean_pred_top - (sum(y_val[i] for i in idx_alpha) / len(idx_alpha))
            lift_decision = mean_pred_top - (sum(y_val[i] for i in idx_decision) / len(idx_decision))
            lift_views = mean_pred_top - (sum(y_val[i] for i in idx_views) / len(idx_views))

            leaderboard_rows.append(
                {
                    "target": target,
                    "model_family": model_family,
                    "mae_log": round(mae, 6),
                    "rmse_log": round(rmse, 6),
                    "spearman_corr": round(spearman, 6),
                    "top_10_overlap_with_actual": round(top10, 6),
                    "precision_at_top_decile_regression": round(precision_decile, 6),
                    "lift_vs_baseline_alpha": round(lift_alpha, 6),
                    "lift_vs_baseline_decision": round(lift_decision, 6),
                    "lift_vs_baseline_views_delta": round(lift_views, 6),
                    "train_rows": len(train_rows),
                    "valid_rows": len(val_rows),
                }
            )

            # importance
            model_importance: list[tuple[str, str, float, str]] = []
            if model_family == "random_forest_regressor":
                perm = permutation_importance(model, x_val, y_val, n_repeats=3, random_state=42)
                for idx, feature in enumerate(features):
                    model_importance.append((feature, "permutation_importance_mean", float(perm.importances_mean[idx]), "primary"))
                    model_importance.append((feature, "permutation_importance_std", float(perm.importances_std[idx]), "primary"))
                    model_importance.append((feature, "impurity_importance", float(model.feature_importances_[idx]), "secondary"))
            elif model_family == "linear_regularized_regressor":
                coeffs = model.named_steps["model"].coef_
                for idx, feature in enumerate(features):
                    model_importance.append((feature, "standardized_coefficient", float(coeffs[idx]), "primary"))
            else:
                for idx, feature in enumerate(features):
                    model_importance.append((feature, "feature_importance", float(model.feature_importances_[idx]), "primary"))
                rules = export_text(model, feature_names=features)
                (reports_dir / f"rules_{target}_{model_family}.txt").write_text(rules, encoding="utf-8")

            # direction
            direction_map: dict[str, tuple[str, float, float, float, str]] = {}
            if model_family == "linear_regularized_regressor":
                coeffs = model.named_steps["model"].coef_
                for idx, feature in enumerate(features):
                    coef = float(coeffs[idx])
                    direction = "positive" if coef > 0 else "negative" if coef < 0 else "mixed"
                    direction_map[feature] = (direction, abs(coef), 0.0, 0.0, "coefficient_sign")
            else:
                for idx, feature in enumerate(features):
                    feat_vals = [row[idx] for row in x_val]
                    direction, score, low, high = _direction_from_quantiles(feat_vals, preds)
                    direction_map[feature] = (direction, score, low, high, "quantile_directional_analysis")

            ranked_primary = sorted(
                [(f, t, v, n) for f, t, v, n in model_importance if t in {"permutation_importance_mean", "standardized_coefficient", "feature_importance"}],
                key=lambda item: abs(item[2]),
                reverse=True,
            )
            rank_map = {feature: idx + 1 for idx, (feature, _t, _v, _n) in enumerate(ranked_primary)}

            for feature, imp_type, imp_val, note in model_importance:
                group = _feature_group(feature)
                direction, score, low, high, method = direction_map.get(feature, ("mixed", 0.0, 0.0, 0.0, "unknown"))
                importance_rows.append(
                    {
                        "target": target,
                        "model_family": model_family,
                        "feature": feature,
                        "feature_group": group,
                        "importance_type": imp_type,
                        "importance_value": round(imp_val, 8),
                        "importance_rank": rank_map.get(feature, ""),
                        "direction": direction,
                        "notes": note,
                    }
                )
                if imp_type in {"permutation_importance_mean", "standardized_coefficient", "feature_importance"}:
                    direction_rows.append(
                        {
                            "target": target,
                            "model_family": model_family,
                            "feature": feature,
                            "feature_group": group,
                            "direction": direction,
                            "direction_score": round(score, 8),
                            "low_bin_prediction": round(low, 8),
                            "high_bin_prediction": round(high, 8),
                            "direction_method": method,
                            "notes": "RF/tree direction via directional analysis" if model_family != "linear_regularized_regressor" else "Linear direction via coefficient sign",
                        }
                    )

            group_agg: dict[str, float] = defaultdict(float)
            group_count: dict[str, int] = defaultdict(int)
            for row in importance_rows:
                if row["target"] != target or row["model_family"] != model_family:
                    continue
                if row["importance_type"] not in {"permutation_importance_mean", "standardized_coefficient", "feature_importance"}:
                    continue
                group_agg[row["feature_group"]] += abs(_safe_float(row["importance_value"]))
                group_count[row["feature_group"]] += 1
            for group, value in group_agg.items():
                group_rows.append(
                    {
                        "target": target,
                        "model_family": model_family,
                        "feature_group": group,
                        "group_importance": round(value, 8),
                        "feature_count": group_count[group],
                    }
                )

            with (models_dir / f"{target}_{model_family}.pkl").open("wb") as handle:
                pickle.dump(model, handle)

    reports_dir_data = data_root / "model_reports"
    reports_dir_data.mkdir(parents=True, exist_ok=True)
    _write_csv(reports_dir_data / "latest_content_driver_leaderboard.csv", LEADERBOARD_COLUMNS, leaderboard_rows)
    _write_csv(reports_dir_data / "latest_content_driver_feature_importance.csv", FEATURE_IMPORTANCE_COLUMNS, importance_rows)
    _write_csv(reports_dir_data / "latest_content_driver_feature_direction.csv", FEATURE_DIRECTION_COLUMNS, direction_rows)
    _write_csv(reports_dir_data / "latest_content_driver_group_importance.csv", GROUP_IMPORTANCE_COLUMNS, group_rows)

    best_by_target: dict[str, dict[str, Any]] = {}
    for row in leaderboard_rows:
        target = str(row.get("target", ""))
        current = best_by_target.get(target)
        score = _safe_float(row.get("spearman_corr"))
        if current is None or score > _safe_float(current.get("spearman_corr")):
            best_by_target[target] = row

    top_var_lines: list[str] = []
    for target in sorted(best_by_target):
        target_rows = [row for row in importance_rows if row.get("target") == target and row.get("importance_type") in {"permutation_importance_mean", "standardized_coefficient", "feature_importance"}]
        target_rows = sorted(target_rows, key=lambda row: abs(_safe_float(row.get("importance_value"))), reverse=True)[:5]
        names = ", ".join(str(row.get("feature")) for row in target_rows)
        top_var_lines.append(f"- **{target}**: {names}")

    group_lines = []
    for row in sorted(group_rows, key=lambda r: _safe_float(r.get("group_importance")), reverse=True)[:10]:
        group_lines.append(f"- {row['target']} / {row['model_family']} / {row['feature_group']}: {row['group_importance']}")

    md_report = "\n".join(
        [
            "# Content Driver Models Report",
            "",
            f"Generated at: {datetime.now(timezone.utc).isoformat()}",
            "",
            "## Targets evaluados",
            *[f"- {target}" for target in TARGETS],
            "",
            "## Modelos entrenados",
            "- random_forest_regressor",
            "- linear_regularized_regressor",
            "- shallow_tree_regressor",
            "",
            "## Mejor modelo por target",
            *[
                f"- {target}: {row['model_family']} (spearman={row['spearman_corr']})"
                for target, row in sorted(best_by_target.items())
            ],
            "",
            "## Top variables por target",
            *top_var_lines,
            "",
            "## Variables con dirección (positive/negative/mixed)",
            "Ver `latest_content_driver_feature_direction.csv` para detalle por feature.",
            "",
            "## Importancia por grupo",
            *group_lines,
            "",
            "**Estas importancias son predictivas, no causales.**",
            "",
            "**En RF, la dirección se estima con directional analysis, no con impurity importance.**",
        ]
    )

    html_report = "<html><body><pre>" + md_report.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre></body></html>"
    (reports_dir_data / "latest_content_driver_report.md").write_text(md_report, encoding="utf-8")
    (reports_dir_data / "latest_content_driver_report.html").write_text(html_report, encoding="utf-8")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "targets": TARGETS,
        "models": sorted({row.get("model_family") for row in leaderboard_rows}),
        "leaderboard_rows": len(leaderboard_rows),
        "warnings": warnings,
        "status": "success" if leaderboard_rows else "skipped_not_ready",
    }
    (artifact_root / "suite_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return manifest
