"""Baseline model training and artifact registry update services."""

from __future__ import annotations

import csv
import gzip
import importlib
import json
import math
import pickle
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from ytb_history.utils.hashing import fingerprint_text

_TARGET_COLUMNS = {
    "future_views_delta_7d",
    "future_log_views_delta_7d",
    "is_top_growth_7d",
    "outperforms_channel_7d",
}


class _SimpleDictVectorizer:
    def __init__(self) -> None:
        self.feature_names_: list[str] = []
        self._index: dict[str, int] = {}

    def fit(self, rows: list[dict[str, Any]]) -> "_SimpleDictVectorizer":
        keys: set[str] = set()
        for row in rows:
            keys.update(row.keys())
        self.feature_names_ = sorted(keys)
        self._index = {name: idx for idx, name in enumerate(self.feature_names_)}
        return self

    def transform(self, rows: list[dict[str, Any]]) -> list[list[float]]:
        matrix: list[list[float]] = []
        for row in rows:
            values = [0.0] * len(self.feature_names_)
            for key, raw in row.items():
                idx = self._index.get(key)
                if idx is None:
                    continue
                values[idx] = _safe_float(raw)
            matrix.append(values)
        return matrix

    @property
    def vocabulary_(self) -> dict[str, int]:
        return dict(self._index)


class _SimpleLogisticRegression:
    def __init__(self, *, max_iter: int = 400, learning_rate: float = 0.05) -> None:
        self.max_iter = max_iter
        self.learning_rate = learning_rate
        self.weights: list[float] = []
        self.bias = 0.0

    def fit(self, x: list[list[float]], y: list[int]) -> None:
        if not x:
            self.weights = []
            self.bias = 0.0
            return
        feature_count = len(x[0])
        self.weights = [0.0 for _ in range(feature_count)]
        self.bias = 0.0

        for _ in range(self.max_iter):
            grad_w = [0.0 for _ in range(feature_count)]
            grad_b = 0.0
            n = float(len(x))
            for row, label in zip(x, y):
                z = sum(w * v for w, v in zip(self.weights, row)) + self.bias
                p = 1.0 / (1.0 + math.exp(-max(min(z, 35.0), -35.0)))
                error = p - float(label)
                for idx in range(feature_count):
                    grad_w[idx] += error * row[idx]
                grad_b += error
            for idx in range(feature_count):
                self.weights[idx] -= self.learning_rate * (grad_w[idx] / n)
            self.bias -= self.learning_rate * (grad_b / n)

    def predict_proba(self, x: list[list[float]]) -> list[list[float]]:
        output: list[list[float]] = []
        for row in x:
            z = sum(w * v for w, v in zip(self.weights, row)) + self.bias
            p1 = 1.0 / (1.0 + math.exp(-max(min(z, 35.0), -35.0)))
            output.append([1.0 - p1, p1])
        return output


def _read_json(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 1.0 if str(value).strip().lower() in {"true", "yes", "y"} else 0.0


def _safe_bool01(value: Any) -> float:
    normalized = str(value).strip().lower()
    return 1.0 if normalized in {"1", "true", "yes", "y"} else 0.0


def _parse_dt(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _precision_recall_at_k(y_true: list[int], scores: list[float], k: int = 10) -> dict[str, float]:
    if not y_true:
        return {"precision": 0.0, "recall": 0.0, "k": 0}
    top_k = min(k, len(y_true))
    ranked = sorted(zip(scores, y_true), key=lambda item: item[0], reverse=True)
    positives_top = sum(int(item[1]) for item in ranked[:top_k])
    positives_total = sum(y_true)
    precision = positives_top / top_k if top_k else 0.0
    recall = positives_top / positives_total if positives_total else 0.0
    return {"precision": round(precision, 6), "recall": round(recall, 6), "k": top_k}


def _roc_auc(y_true: list[int], scores: list[float]) -> float:
    positives = [(s, y) for s, y in zip(scores, y_true) if y == 1]
    negatives = [(s, y) for s, y in zip(scores, y_true) if y == 0]
    if not positives or not negatives:
        raise ValueError("need_two_classes")
    wins = 0.0
    total = len(positives) * len(negatives)
    for ps, _ in positives:
        for ns, _ in negatives:
            if ps > ns:
                wins += 1.0
            elif ps == ns:
                wins += 0.5
    return wins / total


def _pr_auc(y_true: list[int], scores: list[float]) -> float:
    ranked = sorted(zip(scores, y_true), key=lambda item: item[0], reverse=True)
    positives = sum(y_true)
    if positives == 0:
        raise ValueError("no_positive")
    tp = 0
    fp = 0
    last_recall = 0.0
    area = 0.0
    for _, label in ranked:
        if label == 1:
            tp += 1
        else:
            fp += 1
        precision = tp / (tp + fp)
        recall = tp / positives
        area += precision * max(0.0, recall - last_recall)
        last_recall = recall
    return area


def _brier(y_true: list[int], scores: list[float]) -> float:
    if not y_true:
        raise ValueError("empty")
    return sum((float(y) - float(p)) ** 2 for y, p in zip(y_true, scores)) / len(y_true)


def _compute_binary_metrics(y_true: list[int], scores: list[float]) -> dict[str, float | None]:
    pr = _precision_recall_at_k(y_true, scores, k=10)
    metrics: dict[str, float | None] = {
        "precision_at_10": float(pr["precision"]),
        "recall_at_10": float(pr["recall"]),
        "roc_auc": None,
        "pr_auc": None,
        "brier_score": None,
    }
    classes = set(y_true)
    if len(classes) == 2:
        metrics["roc_auc"] = round(_roc_auc(y_true, scores), 6)
        metrics["brier_score"] = round(_brier(y_true, scores), 6)
    if any(label == 1 for label in y_true):
        metrics["pr_auc"] = round(_pr_auc(y_true, scores), 6)
    return metrics


def _load_modeling_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _resolve_feature_list(feature_dictionary: dict[str, Any]) -> list[str]:
    features = feature_dictionary.get("features", [])
    names = [str(item.get("name")) for item in features if isinstance(item, dict) and item.get("name")]
    allowed = [name for name in names if not name.startswith("future_") and name not in _TARGET_COLUMNS]
    return allowed


def _build_feature_row(row: dict[str, str], feature_list: list[str]) -> dict[str, Any]:
    feature_row: dict[str, Any] = {}
    for feature in feature_list:
        raw = row.get(feature)
        normalized = str(raw).strip().lower()
        if normalized in {"true", "false", "1", "0", "yes", "no", "y", "n"}:
            feature_row[feature] = _safe_bool01(raw)
            continue
        feature_row[feature] = _safe_float(raw)
    return feature_row


def _get_git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip() or None
    except Exception:
        return None


def _load_ml_backend() -> tuple[Any, Any, str]:
    has_sklearn = importlib.util.find_spec("sklearn") is not None
    has_joblib = importlib.util.find_spec("joblib") is not None
    if has_sklearn and has_joblib:
        sklearn_feature = importlib.import_module("sklearn.feature_extraction")
        sklearn_linear = importlib.import_module("sklearn.linear_model")
        joblib = importlib.import_module("joblib")
        return sklearn_feature.DictVectorizer, sklearn_linear.LogisticRegression, joblib
    return _SimpleDictVectorizer, _SimpleLogisticRegression, None


def train_baseline_model(
    *,
    data_dir: str | Path = "data",
    modeling_config_path: str | Path = "config/modeling.yaml",
    artifact_dir: str | Path = "build/model_artifact",
) -> dict[str, Any]:
    """Train a weekly baseline model and produce a local artifact directory."""
    data_root = Path(data_dir)
    modeling_dir = data_root / "modeling"
    readiness = _read_json(modeling_dir / "model_readiness_report.json") if (modeling_dir / "model_readiness_report.json").exists() else {}
    recommended_status = str(readiness.get("recommended_status") or "not_ready")

    if recommended_status == "not_ready":
        return {
            "status": "skipped_not_ready",
            "reason": "model_readiness_not_ready",
            "recommended_status": recommended_status,
        }

    supervised_path = modeling_dir / "supervised_examples.csv"
    feature_dictionary_path = modeling_dir / "feature_dictionary.json"
    if not supervised_path.exists() or not feature_dictionary_path.exists():
        return {
            "status": "failed_missing_inputs",
            "reason": "missing_supervised_examples_or_feature_dictionary",
        }

    rows = _read_csv(supervised_path)
    if len(rows) < 2:
        return {
            "status": "failed_insufficient_examples",
            "reason": "need_at_least_two_examples",
            "total_examples": len(rows),
        }

    feature_dictionary = _read_json(feature_dictionary_path)
    feature_list = _resolve_feature_list(feature_dictionary)
    if not feature_list:
        return {
            "status": "failed_no_features",
            "reason": "feature_dictionary_has_no_allowed_features",
        }

    config = _load_modeling_config(Path(modeling_config_path))
    target = str(config.get("prediction_target") or "is_top_growth_7d")

    enriched_rows = []
    for row in rows:
        if not row.get("execution_date"):
            continue
        enriched_rows.append({"raw": row, "execution_date": _parse_dt(str(row["execution_date"]))})
    enriched_rows.sort(key=lambda item: item["execution_date"])

    validation_examples = max(1, math.ceil(len(enriched_rows) * 0.2))
    split_index = len(enriched_rows) - validation_examples
    train_rows = enriched_rows[:split_index]
    valid_rows = enriched_rows[split_index:]
    if not train_rows or not valid_rows:
        return {
            "status": "failed_temporal_split",
            "reason": "could_not_create_train_and_validation_splits",
        }

    DictVectorizerCls, LogisticRegressionCls, joblib_module = _load_ml_backend()

    train_x_dicts = [_build_feature_row(item["raw"], feature_list) for item in train_rows]
    valid_x_dicts = [_build_feature_row(item["raw"], feature_list) for item in valid_rows]
    train_y = [1 if str(item["raw"].get(target, "")).strip().lower() in {"true", "1", "yes"} else 0 for item in train_rows]
    valid_y = [1 if str(item["raw"].get(target, "")).strip().lower() in {"true", "1", "yes"} else 0 for item in valid_rows]

    vectorizer = DictVectorizerCls()
    if hasattr(vectorizer, "fit_transform"):
        train_x = vectorizer.fit_transform(train_x_dicts)
        valid_x = vectorizer.transform(valid_x_dicts)
    else:
        vectorizer.fit(train_x_dicts)
        train_x = vectorizer.transform(train_x_dicts)
        valid_x = vectorizer.transform(valid_x_dicts)

    if LogisticRegressionCls is _SimpleLogisticRegression:
        model = LogisticRegressionCls(max_iter=400, learning_rate=0.05)
        model.fit(train_x, train_y)
        valid_proba = [row[1] for row in model.predict_proba(valid_x)]
        training_backend = "fallback_simple_logistic"
    else:
        model = LogisticRegressionCls(max_iter=1000)
        model.fit(train_x, train_y)
        valid_proba = model.predict_proba(valid_x)[:, 1].tolist()
        training_backend = "sklearn_logistic_regression"

    baseline_scores: dict[str, list[float]] = {
        "alpha_score": [_safe_float(item["raw"].get("alpha_score")) for item in valid_rows],
        "views_delta": [_safe_float(item["raw"].get("views_delta")) for item in valid_rows],
    }
    if any("decision_score" in item["raw"] for item in valid_rows):
        baseline_scores["decision_score"] = [_safe_float(item["raw"].get("decision_score")) for item in valid_rows]

    model_metrics = _compute_binary_metrics(valid_y, valid_proba)
    baseline_metrics = {name: _compute_binary_metrics(valid_y, values) for name, values in baseline_scores.items()}

    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    created_at = datetime.now(timezone.utc)
    retention_days = int(config.get("artifact_retention_days") or 30)
    model_id = f"baseline-{target}-{created_at.strftime('%Y%m%dT%H%M%SZ')}"
    feature_list_sha256 = fingerprint_text(json.dumps(feature_list, ensure_ascii=False, sort_keys=True))

    metrics_payload = {
        "schema_version": "baseline_training_metrics_v1",
        "target": target,
        "training_backend": training_backend,
        "model": model_metrics,
        "baselines": baseline_metrics,
        "validation_examples": len(valid_rows),
    }

    manifest = {
        "model_id": model_id,
        "created_at": created_at.isoformat(),
        "target": target,
        "training_examples": len(train_rows),
        "validation_examples": len(valid_rows),
        "train_start_date": train_rows[0]["execution_date"].isoformat(),
        "train_end_date": train_rows[-1]["execution_date"].isoformat(),
        "validation_start_date": valid_rows[0]["execution_date"].isoformat(),
        "validation_end_date": valid_rows[-1]["execution_date"].isoformat(),
        "git_sha": _get_git_sha(),
        "artifact_name": None,
        "expected_artifact_retention_days": retention_days,
        "expires_at_estimate": (created_at + timedelta(days=retention_days)).isoformat(),
        "feature_list_sha256": feature_list_sha256,
        "metrics": model_metrics,
    }

    preprocessing = {
        "schema_version": "baseline_preprocessing_v1",
        "temporal_split": {"strategy": "tail_validation", "validation_ratio": 0.2},
        "target": target,
        "feature_count": len(feature_list),
        "vectorizer_vocabulary_size": len(getattr(vectorizer, "vocabulary_", {})),
    }

    predictions_path = artifact_path / "validation_predictions.csv.gz"
    with gzip.open(predictions_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["execution_date", "video_id", "y_true", "model_score", *baseline_scores.keys()])
        writer.writeheader()
        for idx, item in enumerate(valid_rows):
            out = {
                "execution_date": item["execution_date"].isoformat(),
                "video_id": item["raw"].get("video_id", ""),
                "y_true": valid_y[idx],
                "model_score": round(valid_proba[idx], 8),
            }
            for name, values in baseline_scores.items():
                out[name] = values[idx]
            writer.writerow(out)

    model_card = "\n".join(
        [
            "# Baseline model card",
            "",
            f"- model_id: {model_id}",
            f"- target: {target}",
            f"- training_backend: {training_backend}",
            f"- training_examples: {len(train_rows)}",
            f"- validation_examples: {len(valid_rows)}",
            f"- champion_selection_metric: {config.get('champion_selection_metric', 'precision_at_10')}",
            "- storage: github_actions_artifacts",
            "- notes: baseline logistic regression with temporal split",
            "",
            "## Validation metrics",
            f"- precision_at_10: {model_metrics['precision_at_10']}",
            f"- recall_at_10: {model_metrics['recall_at_10']}",
            f"- roc_auc: {model_metrics['roc_auc']}",
            f"- pr_auc: {model_metrics['pr_auc']}",
            f"- brier_score: {model_metrics['brier_score']}",
        ]
    ) + "\n"

    model_payload = {"model": model, "vectorizer": vectorizer, "feature_list": feature_list, "target": target, "training_backend": training_backend}
    if joblib_module is None:
        with (artifact_path / "model.joblib").open("wb") as handle:
            pickle.dump(model_payload, handle)
    else:
        joblib_module.dump(model_payload, artifact_path / "model.joblib")

    _write_json(artifact_path / "preprocessing.json", preprocessing)
    _write_json(artifact_path / "feature_list.json", {"features": feature_list})
    _write_json(artifact_path / "metrics.json", metrics_payload)
    _write_json(artifact_path / "training_manifest.json", manifest)
    (artifact_path / "model_card.md").write_text(model_card, encoding="utf-8")

    return {
        "status": "success",
        "artifact_dir": str(artifact_path),
        "model_id": model_id,
        "training_examples": len(train_rows),
        "validation_examples": len(valid_rows),
        "target": target,
        "metrics": model_metrics,
        "training_backend": training_backend,
    }


def register_trained_artifact(
    *,
    artifact_name: str,
    workflow_run_id: str,
    artifact_dir: str | Path = "build/model_artifact",
    data_dir: str | Path = "data",
) -> dict[str, Any]:
    """Register a trained model artifact into repository manifests."""
    artifact_path = Path(artifact_dir)
    model_joblib = artifact_path / "model.joblib"
    if not model_joblib.exists():
        return {"status": "skipped_no_artifact", "reason": "model_joblib_missing", "artifact_dir": str(artifact_path)}

    training_manifest = _read_json(artifact_path / "training_manifest.json")
    metrics_doc = _read_json(artifact_path / "metrics.json")

    data_root = Path(data_dir)
    registry_dir = data_root / "model_registry"
    registry_dir.mkdir(parents=True, exist_ok=True)

    latest_manifest_path = registry_dir / "latest_model_manifest.json"
    runs_index_path = registry_dir / "training_runs_index.json"

    latest_manifest = _read_json(latest_manifest_path) if latest_manifest_path.exists() else {}
    runs_index = _read_json(runs_index_path) if runs_index_path.exists() else {"schema_version": "training_runs_index_v1", "runs": []}
    runs = runs_index.get("runs", [])
    if not isinstance(runs, list):
        runs = []

    updated_latest = {
        "schema_version": "latest_model_manifest_v1",
        "status": "valid",
        "model_id": training_manifest.get("model_id"),
        "artifact_name": artifact_name,
        "workflow_run_id": workflow_run_id,
        "target": training_manifest.get("target"),
        "created_at": training_manifest.get("created_at"),
        "expires_at_estimate": training_manifest.get("expires_at_estimate"),
        "metrics": training_manifest.get("metrics") or metrics_doc.get("model") or {},
        "feature_list_sha256": training_manifest.get("feature_list_sha256"),
        "training_manifest_sha256": fingerprint_text(json.dumps(training_manifest, ensure_ascii=False, sort_keys=True)),
        "warnings": [],
    }

    runs.append(
        {
            "model_id": training_manifest.get("model_id"),
            "created_at": training_manifest.get("created_at"),
            "target": training_manifest.get("target"),
            "artifact_name": artifact_name,
            "workflow_run_id": workflow_run_id,
            "metrics": updated_latest["metrics"],
        }
    )

    _write_json(latest_manifest_path, {**latest_manifest, **updated_latest})
    _write_json(runs_index_path, {"schema_version": "training_runs_index_v1", "runs": runs})

    return {
        "status": "success",
        "latest_model_manifest": str(latest_manifest_path),
        "training_runs_index": str(runs_index_path),
        "model_id": updated_latest.get("model_id"),
        "artifact_name": artifact_name,
        "workflow_run_id": workflow_run_id,
    }
