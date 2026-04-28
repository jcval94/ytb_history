"""Signals and alerts generation from analytics latest tables."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VIDEO_SIGNAL_COLUMNS = [
    "execution_date",
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "alpha_breakout",
    "trend_burst",
    "accelerating_video",
    "evergreen_candidate",
    "packaging_problem",
    "metadata_change_watch",
    "high_engagement_low_reach",
    "low_confidence_metric",
    "max_signal_score",
    "top_signal_type",
    "alert_count",
]

CHANNEL_SIGNAL_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "channel_momentum_up",
    "channel_high_growth",
    "channel_consistent_performer",
    "channel_volatility_warning",
    "max_signal_score",
    "top_signal_type",
    "alert_count",
]

SIGNAL_CANDIDATE_COLUMNS = [
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
]

VIDEO_SIGNAL_SPECS = [
    ("alpha_breakout", 85.0),
    ("trend_burst", 80.0),
    ("accelerating_video", 70.0),
    ("evergreen_candidate", 75.0),
    ("packaging_problem", 70.0),
    ("metadata_change_watch", 1.0),
    ("high_engagement_low_reach", 75.0),
    ("low_confidence_metric", 40.0),
]

CHANNEL_SIGNAL_SPECS = [
    ("channel_momentum_up", 80.0),
    ("channel_high_growth", 80.0),
    ("channel_consistent_performer", 75.0),
    ("channel_volatility_warning", 30.0),
]


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
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _confidence_level(confidence: float) -> str:
    if confidence >= 70:
        return "high"
    if confidence >= 40:
        return "medium"
    return "low"


def _severity(adjusted_score: float) -> str:
    if adjusted_score >= 90:
        return "critical"
    if adjusted_score >= 75:
        return "high"
    if adjusted_score >= 55:
        return "medium"
    return "low"


def _adjusted_score(raw_score: float, confidence: float) -> float:
    return raw_score * (0.5 + 0.5 * confidence / 100.0)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _rel(path: Path, base: Path) -> str:
    try:
        return str(path.resolve().relative_to(base.resolve()))
    except ValueError:
        return str(path)


def _alert_id(execution_date: str, entity_type: str, entity_id: str, signal_type: str) -> str:
    source = f"{execution_date}|{entity_type}|{entity_id}|{signal_type}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    return f"alrt_{digest}"


def _build_markdown(alerts: list[dict[str, Any]]) -> str:
    groups: dict[str, list[dict[str, Any]]] = {"critical": [], "high": [], "medium": [], "low": []}
    for alert in alerts:
        groups[alert["severity"]].append(alert)

    lines = ["# Latest Alerts", ""]
    for severity in ["critical", "high", "medium", "low"]:
        lines.append(f"## {severity.capitalize()}")
        group = groups[severity]
        if not group:
            lines.append("- Sin alertas")
            lines.append("")
            continue
        for alert in group:
            entity_label = alert.get("title") or alert.get("channel_name") or alert.get("entity_id")
            evidence = alert.get("evidence_json", {})
            evidence_summary = ", ".join(f"{k}={v}" for k, v in list(evidence.items())[:3])
            lines.extend(
                [
                    f"- **{alert['signal_type']}** · {entity_label}",
                    f"  - score: {alert['adjusted_signal_score']}",
                    f"  - confidence: {alert['confidence_level']} ({alert['metric_confidence_score']})",
                    f"  - evidence: {evidence_summary}",
                    f"  - recommended_action: {alert['recommended_action']}",
                ]
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def generate_alerts(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    analytics_latest = data_root / "analytics" / "latest"
    signals_dir = data_root / "signals"
    alerts_dir = data_root / "alerts"
    generated_at = _now_iso()
    warnings: list[str] = []

    if not analytics_latest.exists():
        warnings.append("Analytics latest directory does not exist.")

    file_map = {
        "video_scores": analytics_latest / "latest_video_scores.csv",
        "video_advanced": analytics_latest / "latest_video_advanced_metrics.csv",
        "channel_advanced": analytics_latest / "latest_channel_advanced_metrics.csv",
        "metric_eligibility": analytics_latest / "latest_metric_eligibility.csv",
        "video_metrics": analytics_latest / "latest_video_metrics.csv",
        "channel_metrics": analytics_latest / "latest_channel_metrics.csv",
    }

    tables: dict[str, list[dict[str, str]]] = {}
    for key, path in file_map.items():
        if path.exists():
            tables[key] = _read_csv(path)
        else:
            warnings.append(f"Missing analytics table: {path}")
            tables[key] = []

    video_rows: dict[str, dict[str, str]] = {}
    for source_key in ["video_scores", "video_advanced", "metric_eligibility", "video_metrics"]:
        for row in tables[source_key]:
            video_id = row.get("video_id", "")
            if not video_id:
                continue
            merged = video_rows.setdefault(video_id, {})
            merged.update(row)

    channel_rows: dict[str, dict[str, str]] = {}
    for source_key in ["channel_advanced", "channel_metrics"]:
        for row in tables[source_key]:
            channel_id = row.get("channel_id", "")
            if not channel_id:
                continue
            merged = channel_rows.setdefault(channel_id, {})
            merged.update(row)

    channel_deltas = [
        _safe_float(row.get("total_views_delta"))
        for row in channel_rows.values()
        if _safe_float(row.get("total_views_delta")) is not None
    ]
    channel_deltas_sorted = sorted(channel_deltas)

    def calc_channel_growth_percentile(total_views_delta: float | None) -> float | None:
        if total_views_delta is None or not channel_deltas_sorted:
            return None
        lte_count = len([v for v in channel_deltas_sorted if v <= total_views_delta])
        return 100.0 * lte_count / len(channel_deltas_sorted)

    signal_candidates: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    video_signal_rows: list[dict[str, Any]] = []
    channel_signal_rows: list[dict[str, Any]] = []

    def register_candidate(
        *,
        execution_date: str,
        entity_type: str,
        entity_id: str,
        signal_type: str,
        raw_score: float,
        threshold: float,
        triggered: bool,
        confidence: float,
    ) -> tuple[float, str]:
        adjusted = _adjusted_score(raw_score, confidence)
        level = _confidence_level(confidence)
        signal_candidates.append(
            {
                "execution_date": execution_date,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "signal_type": signal_type,
                "raw_signal_score": round(raw_score, 4),
                "adjusted_signal_score": round(adjusted, 4),
                "threshold": threshold,
                "triggered": triggered,
                "metric_confidence_score": round(confidence, 4),
                "confidence_level": level,
            }
        )
        return adjusted, level

    for video_id, row in sorted(video_rows.items()):
        execution_date = row.get("execution_date", "")
        channel_id = row.get("channel_id", "")
        channel_name = row.get("channel_name", "")
        title = row.get("title", "")
        confidence = _safe_float(row.get("metric_confidence_score"))
        confidence_warning = None
        if confidence is None:
            confidence = 50.0
            confidence_warning = "metric_confidence_score missing; default 50 applied"

        alpha = _safe_float(row.get("alpha_score")) or 0.0
        trend = _safe_float(row.get("trend_burst_score")) or 0.0
        accel = _safe_float(row.get("growth_acceleration_score")) or 0.0
        evergreen = _safe_float(row.get("evergreen_score")) or 0.0
        packaging = _safe_float(row.get("packaging_problem_score")) or 0.0
        metadata_changed = _to_bool(row.get("metadata_changed"))
        metadata_change_score = _safe_float(row.get("metadata_change_score"))
        growth_label = str(row.get("growth_trend_label", "")).strip().lower()
        engagement_percentile = _safe_float(row.get("engagement_percentile")) or 0.0
        growth_percentile = _safe_float(row.get("growth_percentile")) or 0.0

        signals = {
            "alpha_breakout": (alpha, 85.0, alpha >= 85.0),
            "trend_burst": (trend, 80.0, trend >= 80.0),
            "accelerating_video": (accel, 70.0, growth_label == "accelerating" and accel >= 70.0),
            "evergreen_candidate": (evergreen, 75.0, evergreen >= 75.0),
            "packaging_problem": (packaging, 70.0, packaging >= 70.0),
            "metadata_change_watch": (
                max(metadata_change_score or 0.0, 60.0) if metadata_changed else 0.0,
                1.0,
                metadata_changed,
            ),
            "high_engagement_low_reach": (
                0.65 * engagement_percentile + 0.35 * (100.0 - growth_percentile),
                75.0,
                engagement_percentile >= 75.0 and growth_percentile <= 50.0,
            ),
            "low_confidence_metric": (
                max(0.0, 100.0 - confidence),
                40.0,
                confidence < 40.0,
            ),
        }

        top_signal_type = ""
        max_signal_score = 0.0
        alert_count = 0
        per_video_row = {
            "execution_date": execution_date,
            "video_id": video_id,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "title": title,
        }
        for signal_name, threshold in VIDEO_SIGNAL_SPECS:
            raw, th, triggered = signals[signal_name]
            adjusted, level = register_candidate(
                execution_date=execution_date,
                entity_type="video" if signal_name != "low_confidence_metric" else "data_quality",
                entity_id=video_id,
                signal_type=signal_name,
                raw_score=raw,
                threshold=th,
                triggered=triggered,
                confidence=confidence,
            )
            per_video_row[signal_name] = round(raw, 4)
            if adjusted > max_signal_score:
                max_signal_score = adjusted
                top_signal_type = signal_name
            if not triggered:
                continue
            alert_count += 1
            evidence: dict[str, Any] = {
                "threshold": th,
                "raw_signal_score": round(raw, 4),
                "metric_confidence_score": round(confidence, 4),
            }
            if confidence_warning:
                evidence["warning"] = confidence_warning
            message = {
                "alpha_breakout": "Video con alto alpha_score; revisar como referencia competitiva.",
                "trend_burst": "Video con trend_burst_score alto; posible burst de tendencia.",
                "accelerating_video": "Video en aceleración de crecimiento.",
                "evergreen_candidate": "Video con señal evergreen destacada.",
                "packaging_problem": "Video con posible problema de empaque.",
                "metadata_change_watch": "Video con cambio de metadata detectado.",
                "high_engagement_low_reach": "Alta interacción relativa con alcance bajo.",
                "low_confidence_metric": "Confianza de métricas baja para interpretación robusta.",
            }[signal_name]
            recommended = {
                "alpha_breakout": "Analizar título, tema, duración y canal; considerar inspiración ética.",
                "trend_burst": "Revisar si el tema merece reacción rápida.",
                "accelerating_video": "Monitorear próximas corridas; posible video en aceleración.",
                "evergreen_candidate": "Analizar como contenido evergreen; útil para ideas duraderas.",
                "packaging_problem": "Hay señales de interés con bajo alcance relativo; revisar título/thumbnail/ángulo.",
                "metadata_change_watch": "Vigilar lift posterior del cambio de metadata.",
                "high_engagement_low_reach": "Posible tema fuerte con distribución baja; revisar empaque.",
                "low_confidence_metric": "No tomar decisión fuerte hasta acumular más historial.",
            }[signal_name]
            entity_type = "video" if signal_name != "low_confidence_metric" else "data_quality"
            alerts.append(
                {
                    "alert_id": _alert_id(execution_date, entity_type, video_id, signal_name),
                    "execution_date": execution_date,
                    "generated_at": generated_at,
                    "entity_type": entity_type,
                    "entity_id": video_id,
                    "video_id": video_id,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": title,
                    "signal_type": signal_name,
                    "severity": _severity(adjusted),
                    "raw_signal_score": round(raw, 4),
                    "adjusted_signal_score": round(adjusted, 4),
                    "metric_confidence_score": round(confidence, 4),
                    "confidence_level": level,
                    "message": message,
                    "evidence_json": evidence,
                    "recommended_action": recommended,
                    "dashboard_tab": "Advanced" if signal_name == "trend_burst" else "Signals",
                    "source_tables": [
                        "latest_video_scores.csv",
                        "latest_video_advanced_metrics.csv",
                        "latest_metric_eligibility.csv",
                        "latest_video_metrics.csv",
                    ],
                }
            )

        per_video_row["max_signal_score"] = round(max_signal_score, 4)
        per_video_row["top_signal_type"] = top_signal_type
        per_video_row["alert_count"] = alert_count
        video_signal_rows.append(per_video_row)

    for channel_id, row in sorted(channel_rows.items()):
        execution_date = row.get("execution_date", "")
        channel_name = row.get("channel_name", "")
        confidence = _safe_float(row.get("metric_confidence_score"))
        confidence_warning = None
        if confidence is None:
            confidence = 50.0
            confidence_warning = "metric_confidence_score missing; default 50 applied"

        momentum = _safe_float(row.get("channel_momentum_score")) or 0.0
        consistency = _safe_float(row.get("channel_consistency_score")) or 0.0
        growth_pct = _safe_float(row.get("channel_growth_percentile"))
        if growth_pct is None:
            growth_pct = calc_channel_growth_percentile(_safe_float(row.get("total_views_delta"))) or 0.0

        high_growth_triggered = growth_pct >= 80.0
        channel_signals = {
            "channel_momentum_up": (momentum, 80.0, momentum >= 80.0),
            "channel_high_growth": (growth_pct, 80.0, high_growth_triggered),
            "channel_consistent_performer": (
                (consistency + momentum) / 2.0,
                75.0,
                consistency >= 75.0 and momentum >= 60.0,
            ),
            "channel_volatility_warning": (100.0 - consistency, 30.0, consistency <= 30.0),
        }

        top_signal_type = ""
        max_signal_score = 0.0
        alert_count = 0
        per_channel_row = {
            "execution_date": execution_date,
            "channel_id": channel_id,
            "channel_name": channel_name,
        }

        for signal_name, threshold in CHANNEL_SIGNAL_SPECS:
            raw, th, triggered = channel_signals[signal_name]
            adjusted, level = register_candidate(
                execution_date=execution_date,
                entity_type="channel",
                entity_id=channel_id,
                signal_type=signal_name,
                raw_score=raw,
                threshold=th,
                triggered=triggered,
                confidence=confidence,
            )
            per_channel_row[signal_name] = round(raw, 4)
            if adjusted > max_signal_score:
                max_signal_score = adjusted
                top_signal_type = signal_name
            if not triggered:
                continue
            alert_count += 1
            evidence: dict[str, Any] = {
                "threshold": th,
                "raw_signal_score": round(raw, 4),
                "metric_confidence_score": round(confidence, 4),
            }
            if confidence_warning:
                evidence["warning"] = confidence_warning

            recommended = {
                "channel_momentum_up": "Canal acelerando; revisar últimos videos y frecuencia.",
                "channel_high_growth": "Canal dominante en crecimiento; revisar estrategia.",
                "channel_consistent_performer": "Canal con desempeño consistente; usar como benchmark.",
                "channel_volatility_warning": "Interpretar señales del canal con cautela por alta volatilidad.",
            }[signal_name]
            alerts.append(
                {
                    "alert_id": _alert_id(execution_date, "channel", channel_id, signal_name),
                    "execution_date": execution_date,
                    "generated_at": generated_at,
                    "entity_type": "channel",
                    "entity_id": channel_id,
                    "video_id": "",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": "",
                    "signal_type": signal_name,
                    "severity": _severity(adjusted),
                    "raw_signal_score": round(raw, 4),
                    "adjusted_signal_score": round(adjusted, 4),
                    "metric_confidence_score": round(confidence, 4),
                    "confidence_level": level,
                    "message": f"Señal de canal detectada: {signal_name}.",
                    "evidence_json": evidence,
                    "recommended_action": recommended,
                    "dashboard_tab": "Channels",
                    "source_tables": [
                        "latest_channel_advanced_metrics.csv",
                        "latest_channel_metrics.csv",
                    ],
                }
            )

        per_channel_row["max_signal_score"] = round(max_signal_score, 4)
        per_channel_row["top_signal_type"] = top_signal_type
        per_channel_row["alert_count"] = alert_count
        channel_signal_rows.append(per_channel_row)

    alerts.sort(key=lambda item: (item["severity"], -item["adjusted_signal_score"]))

    outputs = {
        "video_signals": signals_dir / "latest_video_signals.csv",
        "channel_signals": signals_dir / "latest_channel_signals.csv",
        "signal_candidates": signals_dir / "latest_signal_candidates.csv",
        "signal_summary": signals_dir / "signal_summary.json",
        "alerts_jsonl": alerts_dir / "latest_alerts.jsonl",
        "alerts_json": alerts_dir / "latest_alerts.json",
        "alerts_md": alerts_dir / "latest_alerts.md",
        "alert_summary": alerts_dir / "alert_summary.json",
    }

    _write_csv(outputs["video_signals"], VIDEO_SIGNAL_COLUMNS, video_signal_rows)
    _write_csv(outputs["channel_signals"], CHANNEL_SIGNAL_COLUMNS, channel_signal_rows)
    _write_csv(outputs["signal_candidates"], SIGNAL_CANDIDATE_COLUMNS, signal_candidates)

    alerts_payload = {"generated_at": generated_at, "alert_count": len(alerts), "alerts": alerts}
    _write_jsonl(outputs["alerts_jsonl"], alerts)
    _write_json(outputs["alerts_json"], alerts_payload)
    outputs["alerts_md"].parent.mkdir(parents=True, exist_ok=True)
    outputs["alerts_md"].write_text(_build_markdown(alerts), encoding="utf-8")

    severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    signal_type_counts: dict[str, int] = {}
    video_alerts = 0
    channel_alerts = 0
    data_quality_alerts = 0
    for alert in alerts:
        severity_counts[alert["severity"]] += 1
        signal_type_counts[alert["signal_type"]] = signal_type_counts.get(alert["signal_type"], 0) + 1
        if alert["entity_type"] == "video":
            video_alerts += 1
        elif alert["entity_type"] == "channel":
            channel_alerts += 1
        elif alert["entity_type"] == "data_quality":
            data_quality_alerts += 1

    top_alerts = sorted(alerts, key=lambda item: item["adjusted_signal_score"], reverse=True)[:10]
    signal_summary = {
        "generated_at": generated_at,
        "video_alerts": video_alerts,
        "channel_alerts": channel_alerts,
        "data_quality_alerts": data_quality_alerts,
        "total_alerts": len(alerts),
        "severity_counts": severity_counts,
        "signal_type_counts": signal_type_counts,
        "top_alerts": top_alerts,
        "warnings": warnings,
    }
    alert_summary = {
        "generated_at": generated_at,
        "total_alerts": len(alerts),
        "severity_counts": severity_counts,
        "top_alerts": top_alerts,
        "warnings": warnings,
    }
    _write_json(outputs["signal_summary"], signal_summary)
    _write_json(outputs["alert_summary"], alert_summary)

    return {
        "status": "success" if not warnings else "warning",
        "alerts_dir": str(alerts_dir),
        "signals_dir": str(signals_dir),
        "total_alerts": len(alerts),
        "warnings": warnings,
        "outputs": {name: _rel(path, data_root) for name, path in outputs.items()},
    }
