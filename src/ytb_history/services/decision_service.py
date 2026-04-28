"""Decision intelligence layer builders from analytics, alerts, and signals."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ACTION_CANDIDATE_COLUMNS = [
    "action_id",
    "execution_date",
    "generated_at",
    "action_type",
    "priority",
    "entity_type",
    "entity_id",
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "signal_type",
    "raw_signal_score",
    "adjusted_signal_score",
    "metric_confidence_score",
    "confidence_level",
    "strategic_value_score",
    "effort_level",
    "expected_value_score",
    "decision_score",
    "timeframe",
    "reason",
    "recommended_action",
    "dashboard_tab",
    "evidence_json",
]

OPPORTUNITY_MATRIX_COLUMNS = [
    "opportunity_type",
    "action_type",
    "candidates_count",
    "avg_decision_score",
    "max_decision_score",
    "avg_confidence_score",
    "top_entity_id",
    "top_title",
    "recommended_focus",
]

CONTENT_OPPORTUNITY_COLUMNS = [
    "opportunity_id",
    "opportunity_type",
    "source_video_id",
    "source_channel",
    "source_title",
    "content_strategy",
    "suggested_angle_type",
    "why_it_matters",
    "evidence_score",
    "confidence_level",
    "recommended_timeframe",
    "dashboard_tab",
]

WATCHLIST_COLUMNS = [
    "watchlist_type",
    "entity_type",
    "entity_id",
    "title",
    "channel_name",
    "reason",
    "watch_priority",
    "next_check",
    "evidence_json",
]

ACTION_RULES: dict[str, dict[str, str]] = {
    "trend_burst": {
        "action_type": "create_fast_reaction",
        "timeframe": "next_3_days",
        "effort_level": "medium",
        "recommended_action": "Evaluar crear contenido rápido sobre esta tendencia.",
    },
    "alpha_breakout": {
        "action_type": "analyze_reference",
        "timeframe": "this_week",
        "effort_level": "low",
        "recommended_action": "Analizar título, formato, duración y enfoque como referencia competitiva.",
    },
    "evergreen_candidate": {
        "action_type": "create_evergreen",
        "timeframe": "this_month",
        "effort_level": "medium",
        "recommended_action": "Considerar una pieza evergreen inspirada en este patrón.",
    },
    "packaging_problem": {
        "action_type": "repackage_idea",
        "timeframe": "this_week",
        "effort_level": "medium",
        "recommended_action": "Revisar si el tema puede tener mejor empaque, título o ángulo.",
    },
    "metadata_change_watch": {
        "action_type": "monitor_next_run",
        "timeframe": "next_run",
        "effort_level": "low",
        "recommended_action": "Vigilar si el cambio de metadata produce lift.",
    },
    "high_engagement_low_reach": {
        "action_type": "repackage_idea",
        "timeframe": "this_week",
        "effort_level": "medium",
        "recommended_action": "Analizar como tema con interés pero baja distribución.",
    },
    "channel_momentum_up": {
        "action_type": "benchmark_channel",
        "timeframe": "this_week",
        "effort_level": "low",
        "recommended_action": "Revisar estrategia reciente del canal.",
    },
    "channel_high_growth": {
        "action_type": "benchmark_channel",
        "timeframe": "this_week",
        "effort_level": "low",
        "recommended_action": "Revisar canal dominante por crecimiento.",
    },
    "channel_consistent_performer": {
        "action_type": "benchmark_channel",
        "timeframe": "this_month",
        "effort_level": "low",
        "recommended_action": "Usar como benchmark de consistencia.",
    },
    "channel_volatility_warning": {
        "action_type": "monitor_next_run",
        "timeframe": "next_run",
        "effort_level": "low",
        "recommended_action": "Interpretar señales con cautela por volatilidad.",
    },
    "low_confidence_metric": {
        "action_type": "wait_for_confidence",
        "timeframe": "wait",
        "effort_level": "low",
        "recommended_action": "No tomar decisión fuerte todavía; acumular más historial.",
    },
    "accelerating_video": {
        "action_type": "monitor_next_run",
        "timeframe": "next_run",
        "effort_level": "low",
        "recommended_action": "Monitorear si la aceleración se sostiene en la siguiente corrida.",
    },
}

OPPORTUNITY_ACTION_TYPES = [
    "create_fast_reaction",
    "create_evergreen",
    "analyze_reference",
    "repackage_idea",
    "benchmark_channel",
    "monitor_next_run",
    "wait_for_confidence",
]

CONTENT_ACTION_TYPES = {
    "create_fast_reaction",
    "create_evergreen",
    "repackage_idea",
    "analyze_reference",
}

WATCH_ACTION_TYPES = {"monitor_next_run", "wait_for_confidence"}

# `discard_low_signal` queda reservado para una futura fase de triage explícito
# de señales no disparadas. En esta versión no se generan action candidates
# con ese action_type.

CONFIDENCE_MULTIPLIER = {"high": 1.0, "medium": 0.8, "low": 0.55}
URGENCY_MULTIPLIER = {
    "trend_burst": 1.2,
    "alpha_breakout": 1.0,
    "evergreen_candidate": 0.9,
    "packaging_problem": 0.85,
    "metadata_change_watch": 0.75,
    "high_engagement_low_reach": 0.9,
    "low_confidence_metric": 0.4,
}
EFFORT_MULTIPLIER = {"low": 1.0, "medium": 0.8, "high": 0.6}


INPUT_FILES = {
    "alerts": Path("alerts/latest_alerts.json"),
    "signal_candidates": Path("signals/latest_signal_candidates.csv"),
    "video_signals": Path("signals/latest_video_signals.csv"),
    "channel_signals": Path("signals/latest_channel_signals.csv"),
    "video_scores": Path("analytics/latest/latest_video_scores.csv"),
    "video_advanced": Path("analytics/latest/latest_video_advanced_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "channel_metrics": Path("analytics/latest/latest_channel_metrics.csv"),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true"}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def _action_id(*, execution_date: str, entity_type: str, entity_id: str, signal_type: str) -> str:
    base = f"{execution_date}|{entity_type}|{entity_id}|{signal_type}"
    return f"act_{hashlib.sha1(base.encode('utf-8')).hexdigest()[:16]}"


def _dashboard_tab(action_type: str) -> str:
    if action_type in {"create_fast_reaction", "create_evergreen", "repackage_idea"}:
        return "content_opportunities"
    if action_type == "analyze_reference":
        return "competitive_analysis"
    if action_type == "benchmark_channel":
        return "channel_benchmark"
    return "watchlist"


def _recommended_focus(action_type: str) -> str:
    return {
        "create_fast_reaction": "Priorizar ideas rápidas y publicar en ventana corta.",
        "create_evergreen": "Planificar piezas con valor sostenido en el tiempo.",
        "analyze_reference": "Extraer aprendizajes de formato y ángulo ganador.",
        "repackage_idea": "Optimizar empaque de temas con potencial latente.",
        "benchmark_channel": "Documentar patrones replicables del canal referencia.",
        "monitor_next_run": "Esperar confirmación de tendencia en próxima corrida.",
        "wait_for_confidence": "Acumular evidencia antes de una acción fuerte.",
    }.get(action_type, "Revisar caso manualmente.")


def _priority(decision_score: float) -> str:
    if decision_score >= 90:
        return "critical"
    if decision_score >= 75:
        return "high"
    if decision_score >= 55:
        return "medium"
    return "low"


def _strategic_value(*, adjusted_signal_score: float, opportunity_score: float | None, channel_relative_success_score: float | None, metric_confidence_score: float | None) -> float:
    components: list[tuple[float, float]] = [(adjusted_signal_score, 0.35)]
    if opportunity_score is not None:
        components.append((opportunity_score, 0.25))
    if channel_relative_success_score is not None:
        components.append((channel_relative_success_score, 0.20))
    if metric_confidence_score is not None:
        components.append((metric_confidence_score, 0.20))

    if len(components) <= 1:
        return _clamp(adjusted_signal_score)

    weight_sum = sum(weight for _, weight in components)
    if weight_sum <= 0:
        return _clamp(adjusted_signal_score)

    score = sum(value * (weight / weight_sum) for value, weight in components)
    return round(_clamp(score), 4)


def _expected_value(*, strategic_value_score: float, confidence_level: str, signal_type: str, effort_level: str) -> float:
    confidence = CONFIDENCE_MULTIPLIER.get(confidence_level, 0.8)
    urgency = URGENCY_MULTIPLIER.get(signal_type, 0.8)
    effort = EFFORT_MULTIPLIER.get(effort_level, 0.8)
    return round(_clamp(strategic_value_score * confidence * urgency * effort), 4)


def build_decision_layer(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    generated_at = _now_iso()
    warnings: list[str] = []

    inputs: dict[str, list[dict[str, Any]]] = {}
    inputs_used: dict[str, str] = {}

    for key, rel_path in INPUT_FILES.items():
        full_path = data_root / rel_path
        inputs_used[key] = _rel(full_path, data_root)
        if not full_path.exists():
            warnings.append(f"Missing input file: {full_path}")
            inputs[key] = []
            continue

        if full_path.suffix.lower() == ".csv":
            inputs[key] = _read_csv(full_path)
        else:
            data = _read_json(full_path)
            if isinstance(data, dict):
                if isinstance(data.get("alerts"), list):
                    inputs[key] = data["alerts"]
                else:
                    inputs[key] = [data]
            elif isinstance(data, list):
                inputs[key] = data
            else:
                inputs[key] = []

    video_meta: dict[str, dict[str, Any]] = {}
    for source in ["video_metrics", "video_scores", "video_advanced", "video_signals"]:
        for row in inputs[source]:
            video_id = row.get("video_id", "")
            if not video_id:
                continue
            merged = video_meta.setdefault(video_id, {})
            merged.update(row)

    channel_meta: dict[str, dict[str, Any]] = {}
    for source in ["channel_metrics", "channel_advanced", "channel_signals"]:
        for row in inputs[source]:
            channel_id = row.get("channel_id", "")
            if not channel_id:
                continue
            merged = channel_meta.setdefault(channel_id, {})
            merged.update(row)

    action_candidates: list[dict[str, Any]] = []
    ignored_signal_candidates: list[dict[str, Any]] = []
    for row in sorted(inputs["signal_candidates"], key=lambda item: (item.get("entity_type", ""), item.get("entity_id", ""), item.get("signal_type", ""))):
        triggered = _to_bool(row.get("triggered"))
        if not triggered:
            ignored_signal_candidates.append(
                {
                    "entity_type": row.get("entity_type", ""),
                    "entity_id": row.get("entity_id", ""),
                    "signal_type": row.get("signal_type", ""),
                    "triggered": row.get("triggered", ""),
                    "reason": "triggered=false",
                }
            )
            continue

        signal_type = str(row.get("signal_type", "")).strip()
        rule = ACTION_RULES.get(signal_type)
        if rule is None:
            ignored_signal_candidates.append(
                {
                    "entity_type": row.get("entity_type", ""),
                    "entity_id": row.get("entity_id", ""),
                    "signal_type": signal_type,
                    "triggered": row.get("triggered", ""),
                    "reason": "unsupported_signal_type",
                }
            )
            warnings.append(f"Unsupported signal_type ignored: {signal_type}")
            continue

        entity_type = row.get("entity_type", "")
        entity_id = row.get("entity_id", "")
        execution_date = row.get("execution_date") or generated_at[:10]

        meta = video_meta.get(entity_id, {}) if entity_type == "video" else channel_meta.get(entity_id, {})
        channel_id = meta.get("channel_id", "")
        channel_name = meta.get("channel_name", "")
        title = meta.get("title", "") or meta.get("top_video_title", "")
        video_id = entity_id if entity_type == "video" else ""

        raw_signal_score = _safe_float(row.get("raw_signal_score")) or 0.0
        adjusted_signal_score = _safe_float(row.get("adjusted_signal_score"))
        if adjusted_signal_score is None:
            adjusted_signal_score = raw_signal_score
        metric_confidence_score = _safe_float(row.get("metric_confidence_score"))
        confidence_level = str(row.get("confidence_level", "medium")).strip().lower() or "medium"

        opportunity_score = _safe_float(meta.get("opportunity_score"))
        channel_relative_success_score = _safe_float(meta.get("channel_relative_success_score"))

        strategic_value_score = _strategic_value(
            adjusted_signal_score=adjusted_signal_score,
            opportunity_score=opportunity_score,
            channel_relative_success_score=channel_relative_success_score,
            metric_confidence_score=metric_confidence_score,
        )

        expected_value_score = _expected_value(
            strategic_value_score=strategic_value_score,
            confidence_level=confidence_level,
            signal_type=signal_type,
            effort_level=rule["effort_level"],
        )

        decision_score = round(
            _clamp(
                (0.60 * expected_value_score)
                + (0.25 * adjusted_signal_score)
                + (0.15 * (metric_confidence_score or 0.0))
            ),
            4,
        )

        action_type = rule["action_type"]
        reason = (
            f"Señal {signal_type} activada con score ajustado {round(adjusted_signal_score, 2)} "
            f"y confianza {confidence_level}."
        )

        evidence = {
            "signal_type": signal_type,
            "raw_signal_score": round(raw_signal_score, 4),
            "adjusted_signal_score": round(adjusted_signal_score, 4),
            "metric_confidence_score": round(metric_confidence_score or 0.0, 4),
            "opportunity_score": opportunity_score,
            "channel_relative_success_score": channel_relative_success_score,
            "source_entity": entity_id,
        }

        action_candidates.append(
            {
                "action_id": _action_id(
                    execution_date=str(execution_date),
                    entity_type=str(entity_type),
                    entity_id=str(entity_id),
                    signal_type=signal_type,
                ),
                "execution_date": execution_date,
                "generated_at": generated_at,
                "action_type": action_type,
                "priority": _priority(decision_score),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "video_id": video_id,
                "channel_id": channel_id if channel_id else (entity_id if entity_type == "channel" else ""),
                "channel_name": channel_name,
                "title": title,
                "signal_type": signal_type,
                "raw_signal_score": round(raw_signal_score, 4),
                "adjusted_signal_score": round(adjusted_signal_score, 4),
                "metric_confidence_score": round(metric_confidence_score or 0.0, 4),
                "confidence_level": confidence_level,
                "strategic_value_score": round(strategic_value_score, 4),
                "effort_level": rule["effort_level"],
                "expected_value_score": round(expected_value_score, 4),
                "decision_score": decision_score,
                "timeframe": rule["timeframe"],
                "reason": reason,
                "recommended_action": rule["recommended_action"],
                "dashboard_tab": _dashboard_tab(action_type),
                "evidence_json": json.dumps(evidence, ensure_ascii=False, sort_keys=True),
            }
        )

    action_candidates.sort(key=lambda item: (-float(item["decision_score"]), str(item["action_id"])))

    opportunity_matrix: list[dict[str, Any]] = []
    for action_type in OPPORTUNITY_ACTION_TYPES:
        group = [row for row in action_candidates if row["action_type"] == action_type]
        if group:
            top = max(group, key=lambda row: float(row["decision_score"]))
            avg_decision = sum(float(row["decision_score"]) for row in group) / len(group)
            avg_conf = sum(float(row["metric_confidence_score"]) for row in group) / len(group)
            max_decision = max(float(row["decision_score"]) for row in group)
            top_entity_id = top["entity_id"]
            top_title = top["title"]
        else:
            avg_decision = 0.0
            avg_conf = 0.0
            max_decision = 0.0
            top_entity_id = ""
            top_title = ""

        opportunity_matrix.append(
            {
                "opportunity_type": action_type,
                "action_type": action_type,
                "candidates_count": len(group),
                "avg_decision_score": round(avg_decision, 4),
                "max_decision_score": round(max_decision, 4),
                "avg_confidence_score": round(avg_conf, 4),
                "top_entity_id": top_entity_id,
                "top_title": top_title,
                "recommended_focus": _recommended_focus(action_type),
            }
        )

    content_opportunities: list[dict[str, Any]] = []
    for row in action_candidates:
        if row["action_type"] not in CONTENT_ACTION_TYPES:
            continue
        content_opportunities.append(
            {
                "opportunity_id": f"opp_{row['action_id']}",
                "opportunity_type": row["action_type"],
                "source_video_id": row["video_id"],
                "source_channel": row["channel_name"],
                "source_title": row["title"],
                "content_strategy": row["recommended_action"],
                "suggested_angle_type": row["signal_type"],
                "why_it_matters": row["reason"],
                "evidence_score": row["decision_score"],
                "confidence_level": row["confidence_level"],
                "recommended_timeframe": row["timeframe"],
                "dashboard_tab": row["dashboard_tab"],
            }
        )

    watchlist: list[dict[str, Any]] = []
    for row in action_candidates:
        if row["action_type"] not in WATCH_ACTION_TYPES and row["signal_type"] not in {"metadata_change_watch", "accelerating_video"}:
            continue
        watchlist.append(
            {
                "watchlist_type": row["signal_type"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "title": row["title"],
                "channel_name": row["channel_name"],
                "reason": row["reason"],
                "watch_priority": row["priority"],
                "next_check": row["timeframe"],
                "evidence_json": row["evidence_json"],
            }
        )

    decision_dir = data_root / "decision"
    outputs = {
        "action_candidates": decision_dir / "latest_action_candidates.csv",
        "opportunity_matrix": decision_dir / "latest_opportunity_matrix.csv",
        "content_opportunities": decision_dir / "latest_content_opportunities.csv",
        "watchlist_recommendations": decision_dir / "latest_watchlist_recommendations.csv",
        "decision_context": decision_dir / "latest_decision_context.json",
        "decision_summary": decision_dir / "decision_summary.json",
    }

    _write_csv(outputs["action_candidates"], ACTION_CANDIDATE_COLUMNS, action_candidates)
    _write_csv(outputs["opportunity_matrix"], OPPORTUNITY_MATRIX_COLUMNS, opportunity_matrix)
    _write_csv(outputs["content_opportunities"], CONTENT_OPPORTUNITY_COLUMNS, content_opportunities)
    _write_csv(outputs["watchlist_recommendations"], WATCHLIST_COLUMNS, watchlist)

    decision_context = {
        "generated_at": generated_at,
        "top_action_candidates": action_candidates[:20],
        "opportunity_matrix": opportunity_matrix,
        "content_opportunities": content_opportunities[:20],
        "watchlist_recommendations": watchlist[:20],
        "ignored_signal_candidates": ignored_signal_candidates[:50],
        "inputs_used": inputs_used,
        "warnings": warnings,
        "intended_downstream": [
            "weekly_brief",
            "llm_action_advisor",
            "idea_generator",
            "dashboard",
        ],
    }
    _write_json(outputs["decision_context"], decision_context)

    priority_counts = Counter(row["priority"] for row in action_candidates)
    action_type_counts = Counter(row["action_type"] for row in action_candidates)
    decision_summary = {
        "generated_at": generated_at,
        "total_action_candidates": len(action_candidates),
        "priority_counts": dict(sorted(priority_counts.items())),
        "action_type_counts": dict(sorted(action_type_counts.items())),
        "top_actions": action_candidates[:10],
        "top_content_opportunities": content_opportunities[:10],
        "watchlist_count": len(watchlist),
        "ignored_signal_candidates": len(ignored_signal_candidates),
        "warnings": warnings,
    }
    _write_json(outputs["decision_summary"], decision_summary)

    output_rel = {key: _rel(path, data_root) for key, path in outputs.items()}
    status = "warning" if warnings else "success"
    return {
        "status": status,
        "decision_dir": _rel(decision_dir, data_root),
        "total_action_candidates": len(action_candidates),
        "outputs": output_rel,
        "warnings": warnings,
    }
