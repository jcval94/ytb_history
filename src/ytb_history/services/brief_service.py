"""Weekly intelligence brief builder from decision, alerts, signals, and analytics artifacts."""

from __future__ import annotations

import csv
import html
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


INPUT_FILES = {
    "action_candidates": Path("decision/latest_action_candidates.csv"),
    "opportunity_matrix": Path("decision/latest_opportunity_matrix.csv"),
    "content_opportunities": Path("decision/latest_content_opportunities.csv"),
    "watchlist_recommendations": Path("decision/latest_watchlist_recommendations.csv"),
    "decision_summary": Path("decision/decision_summary.json"),
    "latest_alerts": Path("alerts/latest_alerts.json"),
    "alert_summary": Path("alerts/alert_summary.json"),
    "signal_summary": Path("signals/signal_summary.json"),
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "channel_metrics": Path("analytics/latest/latest_channel_metrics.csv"),
    "video_scores": Path("analytics/latest/latest_video_scores.csv"),
    "video_advanced": Path("analytics/latest/latest_video_advanced_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
    "title_metrics": Path("analytics/latest/latest_title_metrics.csv"),
    "topic_opportunities": Path("topic_intelligence/latest_topic_opportunities.csv"),
    "topic_metrics": Path("topic_intelligence/latest_topic_metrics.csv"),
    "title_pattern_metrics": Path("topic_intelligence/latest_title_pattern_metrics.csv"),
    "semantic_clusters": Path("nlp_features/latest_semantic_clusters.csv"),
    "video_nlp_features": Path("nlp_features/latest_video_nlp_features.csv"),
    "content_driver_leaderboard": Path("model_reports/latest_content_driver_leaderboard.csv"),
    "content_driver_feature_importance": Path("model_reports/latest_content_driver_feature_importance.csv"),
    "content_driver_feature_direction": Path("model_reports/latest_content_driver_feature_direction.csv"),
    "model_readiness_diagnostics": Path("modeling/latest_model_readiness_diagnostics.json"),
    "training_gap_report": Path("modeling/latest_training_gap_report.json"),
    "creative_packages": Path("creative_packages/latest_creative_packages.csv"),
    "creative_titles": Path("creative_packages/latest_title_candidates.csv"),
    "creative_hooks": Path("creative_packages/latest_hook_candidates.csv"),
}

CLIENT_INPUT_FILES = {
    "weekly_brief_json": Path("briefs/latest_weekly_brief.json"),
    "weekly_brief_markdown": Path("briefs/latest_weekly_brief.md"),
    "latest_alerts": Path("alerts/latest_alerts.json"),
    "alert_summary": Path("alerts/alert_summary.json"),
    "topic_opportunities": Path("topic_intelligence/latest_topic_opportunities.csv"),
    "topic_metrics": Path("topic_intelligence/latest_topic_metrics.csv"),
    "title_pattern_metrics": Path("topic_intelligence/latest_title_pattern_metrics.csv"),
    "topic_summary": Path("topic_intelligence/topic_intelligence_summary.json"),
    "hybrid_recommendations": Path("model_intelligence/latest_hybrid_recommendations.csv"),
    "model_intelligence_summary": Path("model_intelligence/model_intelligence_summary.json"),
    "creative_packages": Path("creative_packages/latest_creative_packages.csv"),
    "creative_titles": Path("creative_packages/latest_title_candidates.csv"),
    "creative_hooks": Path("creative_packages/latest_hook_candidates.csv"),
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
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


def _safe_int(value: Any) -> int | None:
    number = _safe_float(value)
    if number is None:
        return None
    return int(number)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _sort_desc(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (_safe_float(row.get(key)) is not None, _safe_float(row.get(key)) or 0.0), reverse=True)


def _iso_week_parts(period_end: date) -> tuple[str, str, str]:
    iso_year, iso_week, _ = period_end.isocalendar()
    week = f"{iso_year}-{iso_week:02d}"
    period_start = period_end - timedelta(days=period_end.weekday())
    return week, period_start.isoformat(), period_end.isoformat()


def _severity_rank(severity: str) -> int:
    return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(str(severity).lower(), 0)


def _tabulate(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _markdown_to_html(markdown_text: str, *, document_title: str = "Weekly Intelligence Brief") -> str:
    lines = markdown_text.splitlines()
    html_lines: list[str] = ["<html>", f"<head><meta charset=\"utf-8\"><title>{html.escape(document_title)}</title></head>", "<body>"]

    in_list = False
    in_table = False

    for line in lines:
        stripped = line.strip()

        if not stripped:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if in_table:
                html_lines.append("</tbody></table>")
                in_table = False
            continue

        if stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if in_table:
                html_lines.append("</tbody></table>")
                in_table = False
            html_lines.append(f"<h2>{html.escape(stripped[3:])}</h2>")
            continue

        if stripped.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if in_table:
                html_lines.append("</tbody></table>")
                in_table = False
            html_lines.append(f"<h1>{html.escape(stripped[2:])}</h1>")
            continue

        if stripped.startswith("- "):
            if in_table:
                html_lines.append("</tbody></table>")
                in_table = False
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{html.escape(stripped[2:])}</li>")
            continue

        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [html.escape(cell.strip()) for cell in stripped.strip("|").split("|")]
            if all(cell == "---" for cell in cells):
                continue
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if not in_table:
                html_lines.append("<table border=\"1\">")
                html_lines.append("<tbody>")
                in_table = True
            html_lines.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>")
            continue

        if in_list:
            html_lines.append("</ul>")
            in_list = False
        if in_table:
            html_lines.append("</tbody></table>")
            in_table = False
        html_lines.append(f"<p>{html.escape(stripped)}</p>")

    if in_list:
        html_lines.append("</ul>")
    if in_table:
        html_lines.append("</tbody></table>")

    html_lines.extend(["</body>", "</html>"])
    return "\n".join(html_lines) + "\n"


def _short_text(value: Any, *, limit: int = 140) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _client_label(row: dict[str, Any], *keys: str, fallback: str = "Sin dato") -> str:
    for key in keys:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return fallback


def _load_client_inputs(data_root: Path) -> tuple[dict[str, Any], list[str]]:
    tables: dict[str, Any] = {}
    warnings: list[str] = []
    for key, rel_path in CLIENT_INPUT_FILES.items():
        path = data_root / rel_path
        if not path.exists():
            warnings.append(f"Missing input file: {path}")
            if path.suffix == ".csv":
                tables[key] = []
            elif path.suffix == ".md":
                tables[key] = ""
            else:
                tables[key] = {}
            continue
        if path.suffix == ".csv":
            tables[key] = _read_csv(path)
        elif path.suffix == ".md":
            tables[key] = path.read_text(encoding="utf-8")
        else:
            tables[key] = _read_json(path)
    return tables, warnings


def _client_video_items(weekly_json: dict[str, Any], tables: dict[str, Any]) -> list[dict[str, Any]]:
    source = weekly_json.get("top_videos_by_growth") if isinstance(weekly_json, dict) else []
    rows = source if isinstance(source, list) and source else tables.get("video_metrics", [])
    items: list[dict[str, Any]] = []
    for row in _sort_desc(rows, "views_delta")[:5]:
        items.append(
            {
                "video_id": row.get("video_id", ""),
                "title": _client_label(row, "title", fallback="Video sin título"),
                "channel_name": row.get("channel_name", ""),
                "views_delta": _safe_int(row.get("views_delta")) or 0,
                "why_it_matters": "Está ganando tracción relativa; revisar tema, empaque y timing para replicar aprendizajes.",
            }
        )
    return items


def _client_channel_items(weekly_json: dict[str, Any], tables: dict[str, Any]) -> list[dict[str, Any]]:
    source = weekly_json.get("top_channels_by_momentum") if isinstance(weekly_json, dict) else []
    rows = source if isinstance(source, list) and source else tables.get("channel_advanced", [])
    items: list[dict[str, Any]] = []
    for row in _sort_desc(rows, "channel_momentum_score")[:5]:
        items.append(
            {
                "channel_id": row.get("channel_id", ""),
                "channel_name": _client_label(row, "channel_name", fallback="Canal sin nombre"),
                "momentum_score": _safe_float(row.get("channel_momentum_score")) or 0.0,
                "recommended_watch": "Observar frecuencia, formatos recientes y cambios de título/miniatura.",
            }
        )
    return items


def _client_topic_items(tables: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("topic_opportunities", []), "topic_opportunity_score")[:5]:
        items.append(
            {
                "topic": _client_label(row, "topic", fallback="Tema sin etiqueta"),
                "opportunity_type": row.get("opportunity_type", ""),
                "score": _safe_float(row.get("topic_opportunity_score")) or 0.0,
                "recommended_action": _client_label(row, "recommended_action", fallback="Validar con una pieza o ángulo pequeño."),
                "why_it_matters": _short_text(row.get("why_it_matters", "Señal combinada de demanda, velocidad y oportunidad.")),
            }
        )
    return items


def _client_title_items(tables: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("title_pattern_metrics", []), "title_pattern_success_score")[:5]:
        items.append(
            {
                "pattern": _client_label(row, "title_pattern", fallback="Patrón sin etiqueta"),
                "success_score": _safe_float(row.get("title_pattern_success_score")) or 0.0,
                "avg_views_delta": _safe_float(row.get("avg_views_delta")) or 0.0,
                "example_titles": _short_text(row.get("example_titles", ""), limit=180),
            }
        )
    return items


def _client_title_suggestions(tables: dict[str, Any]) -> list[str]:
    suggestions: list[str] = []
    for row in tables.get("creative_titles", []):
        title = _client_label(row, "title_candidate", fallback="")
        if title and title not in suggestions:
            suggestions.append(title)
        if len(suggestions) >= 5:
            break
    return suggestions


def _client_recommendation_items(weekly_json: dict[str, Any], tables: dict[str, Any]) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("creative_packages", []), "creative_execution_score")[:4]:
        recs.append(
            {
                "recommendation": _client_label(row, "creative_angle", "recommended_next_step", fallback="Ejecutar paquete creativo priorizado."),
                "format": row.get("recommended_format", ""),
                "timeframe": row.get("recommended_timeframe", ""),
                "topic": row.get("topic", ""),
                "score": _safe_float(row.get("creative_execution_score")) or 0.0,
            }
        )
    weekly_actions = weekly_json.get("top_actions_this_week") if isinstance(weekly_json, dict) else []
    for row in _sort_desc(weekly_actions if isinstance(weekly_actions, list) else [], "decision_score")[: max(0, 5 - len(recs))]:
        recs.append(
            {
                "recommendation": _client_label(row, "recommended_action", "recommendation", fallback="Priorizar acción táctica."),
                "format": row.get("action_type", ""),
                "timeframe": row.get("recommended_timeframe", ""),
                "topic": row.get("topic", ""),
                "score": _safe_float(row.get("decision_score")) or 0.0,
            }
        )
    return recs[:5]


def _client_risk_items(weekly_json: dict[str, Any], tables: dict[str, Any], warnings: list[str]) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []
    alerts_payload = tables.get("latest_alerts", {})
    alerts = alerts_payload.get("alerts", []) if isinstance(alerts_payload, dict) and isinstance(alerts_payload.get("alerts"), list) else []
    for row in sorted(alerts, key=lambda r: (_severity_rank(str(r.get("severity", ""))), _safe_float(r.get("adjusted_signal_score")) or 0.0), reverse=True)[:4]:
        risks.append(
            {
                "signal": _client_label(row, "signal_type", fallback="alert"),
                "severity": row.get("severity", ""),
                "entity": _client_label(row, "title", "channel_name", "entity_id", fallback="Entidad sin nombre"),
                "recommended_action": _client_label(row, "recommended_action", fallback="Monitorear antes de escalar decisión."),
            }
        )
    for note in weekly_json.get("data_quality_notes", []) if isinstance(weekly_json, dict) else []:
        risks.append({"signal": "data_quality", "severity": "medium", "entity": _short_text(note), "recommended_action": "Interpretar insights con cautela."})
    model_summary = tables.get("model_intelligence_summary", {})
    if isinstance(model_summary, dict):
        for warning in model_summary.get("warnings", [])[:2] if isinstance(model_summary.get("warnings"), list) else []:
            risks.append({"signal": "model_intelligence", "severity": "low", "entity": _short_text(warning), "recommended_action": "No depender solo del ranking predictivo."})
    for warning in warnings[:2]:
        risks.append({"signal": "missing_input", "severity": "low", "entity": _short_text(warning), "recommended_action": "Regenerar pipeline si falta contexto crítico."})
    return risks[:6]


def _build_client_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Client Brief de YouTube",
        "",
        f"Generado: {summary['generated_at']}",
        "",
        "## 1. Resumen ejecutivo",
    ]
    lines.extend([f"- {line}" for line in summary["executive_summary"]] or ["- Sin señales suficientes para resumen ejecutivo."])

    lines.extend(["", "## 2. Videos que aceleraron"])
    lines.extend(
        _tabulate(
            ["video", "canal", "views_delta", "lectura"],
            [[_short_text(v["title"], limit=70), str(v.get("channel_name", "")), str(v["views_delta"]), v["why_it_matters"]] for v in summary["accelerating_videos"]],
        )
        if summary["accelerating_videos"]
        else ["- Sin videos acelerando detectados."]
    )

    lines.extend(["", "## 3. Canales a observar"])
    lines.extend(
        _tabulate(
            ["canal", "momentum", "qué mirar"],
            [[c["channel_name"], str(c["momentum_score"]), c["recommended_watch"]] for c in summary["channels_to_watch"]],
        )
        if summary["channels_to_watch"]
        else ["- Sin canales destacados esta semana."]
    )

    lines.extend(["", "## 4. Temas emergentes"])
    lines.extend(
        _tabulate(
            ["tema", "tipo", "score", "acción"],
            [[t["topic"], str(t["opportunity_type"]), str(t["score"]), t["recommended_action"]] for t in summary["emerging_topics"]],
        )
        if summary["emerging_topics"]
        else ["- Sin temas emergentes claros."]
    )

    lines.extend(["", "## 5. Títulos/patrones ganadores"])
    lines.extend(
        _tabulate(
            ["patrón", "score", "views_delta prom.", "ejemplos"],
            [[p["pattern"], str(p["success_score"]), str(p["avg_views_delta"]), p["example_titles"]] for p in summary["winning_title_patterns"]],
        )
        if summary["winning_title_patterns"]
        else ["- Sin patrones suficientes para recomendar."]
    )
    if summary.get("suggested_titles"):
        lines.append("")
        lines.append("Títulos candidatos para adaptar:")
        lines.extend([f"- {_short_text(title, limit=120)}" for title in summary["suggested_titles"]])

    lines.extend(["", "## 6. Recomendaciones para publicar esta semana"])
    lines.extend(
        [f"- **{_short_text(r['recommendation'], limit=120)}** · formato: {r.get('format', '') or 'por definir'} · ventana: {r.get('timeframe', '') or 'esta semana'} · score: {r['score']}" for r in summary["publishing_recommendations"]]
        or ["- Publicar una pieza de prueba sobre el tema con mayor score y medir respuesta en la próxima corrida."]
    )

    lines.extend(["", "## 7. Riesgos o señales débiles"])
    lines.extend(
        [f"- **{r['signal']}** ({r.get('severity', 'n/a')}): {_short_text(r.get('entity', ''), limit=120)} → {r['recommended_action']}" for r in summary["risks_or_weak_signals"]]
        or ["- Sin riesgos críticos detectados; mantener monitoreo semanal."]
    )
    return "\n".join(lines).strip() + "\n"


def generate_client_brief(*, data_dir: str | Path = "data") -> dict[str, Any]:
    """Generate a client-facing brief from existing local intelligence artifacts."""
    data_root = Path(data_dir)
    generated_at = _now_iso()
    tables, warnings = _load_client_inputs(data_root)
    weekly_json = tables.get("weekly_brief_json", {}) if isinstance(tables.get("weekly_brief_json"), dict) else {}
    weekly_md = tables.get("weekly_brief_markdown", "") if isinstance(tables.get("weekly_brief_markdown"), str) else ""

    key_metrics = weekly_json.get("key_metrics", {}) if isinstance(weekly_json, dict) and isinstance(weekly_json.get("key_metrics"), dict) else {}
    topic_summary = tables.get("topic_summary", {}) if isinstance(tables.get("topic_summary"), dict) else {}
    model_summary = tables.get("model_intelligence_summary", {}) if isinstance(tables.get("model_intelligence_summary"), dict) else {}
    hybrid_recommendations = tables.get("hybrid_recommendations", []) if isinstance(tables.get("hybrid_recommendations"), list) else []

    executive_summary = list(weekly_json.get("executive_summary", [])[:3]) if isinstance(weekly_json.get("executive_summary"), list) else []
    executive_summary.extend(
        [
            f"Base analizada: {key_metrics.get('videos_total', 0)} videos y {key_metrics.get('channels_total', 0)} canales en los artefactos actuales.",
            f"Temas detectados: {topic_summary.get('topics', 0)}; oportunidades priorizadas: {topic_summary.get('opportunities', 0)}.",
            f"Modelo híbrido: {model_summary.get('hybrid_rows', len(hybrid_recommendations))} recomendaciones disponibles para contrastar con señales editoriales.",
        ]
    )
    if weekly_md:
        executive_summary.append("El brief semanal existente se usó como contexto editorial base para esta versión orientada a cliente.")

    summary = {
        "generated_at": generated_at,
        "status": "success_with_warnings" if warnings else "success",
        "source_files": {key: str(Path(value)) for key, value in CLIENT_INPUT_FILES.items()},
        "executive_summary": executive_summary,
        "accelerating_videos": _client_video_items(weekly_json, tables),
        "channels_to_watch": _client_channel_items(weekly_json, tables),
        "emerging_topics": _client_topic_items(tables),
        "winning_title_patterns": _client_title_items(tables),
        "suggested_titles": _client_title_suggestions(tables),
        "publishing_recommendations": _client_recommendation_items(weekly_json, tables),
        "risks_or_weak_signals": _client_risk_items(weekly_json, tables, warnings),
        "warnings": warnings,
    }

    markdown_content = _build_client_markdown(summary)
    html_content = _markdown_to_html(markdown_content, document_title="Client Brief de YouTube")

    out_dir = data_root / "client_briefs"
    latest_md = out_dir / "latest_client_brief.md"
    latest_html = out_dir / "latest_client_brief.html"
    latest_json = out_dir / "latest_client_brief_summary.json"

    _write_text(latest_md, markdown_content)
    _write_text(latest_html, html_content)
    _write_json(latest_json, summary)

    return {
        "status": summary["status"],
        "client_brief_dir": str(out_dir),
        "latest_markdown_path": str(latest_md),
        "latest_html_path": str(latest_html),
        "latest_summary_json_path": str(latest_json),
        "warnings": warnings,
    }

def generate_weekly_brief(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    generated_at = _now_iso()
    warnings: list[str] = []

    tables: dict[str, Any] = {}
    for key, rel_path in INPUT_FILES.items():
        path = data_root / rel_path
        if not path.exists():
            warnings.append(f"Missing input file: {path}")
            tables[key] = [] if path.suffix == ".csv" else {}
            continue
        tables[key] = _read_csv(path) if path.suffix == ".csv" else _read_json(path)

    actions = _sort_desc(tables["action_candidates"], "decision_score")[:10]
    content_ops = _sort_desc(tables["content_opportunities"], "evidence_score")[:10]
    watchlist = _sort_desc(tables["watchlist_recommendations"], "watch_priority")[:10]
    matrix = _sort_desc(tables["opportunity_matrix"], "avg_decision_score")
    videos_growth = _sort_desc(tables["video_metrics"], "views_delta")[:10]
    alpha_videos = _sort_desc(tables["video_scores"], "alpha_score")[:10]
    channels_momentum = _sort_desc(tables["channel_advanced"], "channel_momentum_score")[:10]
    topic_opportunities = _sort_desc(tables["topic_opportunities"], "topic_opportunity_score")[:10]
    topic_metrics = _sort_desc(tables["topic_metrics"], "topic_opportunity_score")[:10]
    semantic_clusters = _sort_desc(tables["semantic_clusters"], "semantic_cluster_size")[:10]
    content_driver_leaderboard = _sort_desc(tables["content_driver_leaderboard"], "spearman_corr")
    content_driver_importance = _sort_desc(tables["content_driver_feature_importance"], "importance_rank")
    content_driver_direction = _sort_desc(tables["content_driver_feature_direction"], "direction_score")
    readiness = tables["model_readiness_diagnostics"] if isinstance(tables["model_readiness_diagnostics"], dict) else {}
    gap = tables["training_gap_report"] if isinstance(tables["training_gap_report"], dict) else {}
    creative_packages = _sort_desc(tables["creative_packages"], "creative_execution_score")[:3]
    creative_titles = tables["creative_titles"]
    creative_hooks = tables["creative_hooks"]

    alerts_payload = tables["latest_alerts"] if isinstance(tables["latest_alerts"], dict) else {}
    alerts_rows = alerts_payload.get("alerts", []) if isinstance(alerts_payload.get("alerts", []), list) else []
    top_alerts = sorted(
        alerts_rows,
        key=lambda row: (_severity_rank(str(row.get("severity", ""))), _safe_float(row.get("adjusted_signal_score")) or 0.0),
        reverse=True,
    )[:10]

    period_dates: list[date] = []
    for row in actions:
        raw_date = row.get("execution_date", "")
        if not raw_date:
            continue
        try:
            period_dates.append(datetime.fromisoformat(raw_date).date())
        except ValueError:
            continue

    period_end = max(period_dates) if period_dates else datetime.now(timezone.utc).date()
    week, period_start, period_end_str = _iso_week_parts(period_end)

    high_priority_actions = [row for row in actions if str(row.get("priority", "")).lower() in {"high", "critical"}]
    fast_reaction_actions = [row for row in actions if row.get("action_type") in {"trend", "create_fast_reaction"}]
    evergreen_actions = [row for row in actions if row.get("action_type") == "create_evergreen"]
    repackage_actions = [row for row in actions if row.get("action_type") == "repackage_idea"]
    low_confidence_signals = int((tables["signal_summary"] or {}).get("confidence_distribution", {}).get("low", 0)) if isinstance(tables["signal_summary"], dict) else 0

    executive_summary = [
        f"Hay {len(high_priority_actions)} acciones prioritarias esta semana; revisar primero las de mayor decision_score." if high_priority_actions else "No hay acciones high/critical esta semana; priorizar validación incremental.",
        "Hay señales de reacción rápida; evaluar piezas con ventana corta de publicación." if fast_reaction_actions else "No se detectan señales claras de reacción rápida en esta semana.",
        "Hay oportunidades evergreen; planificar piezas con valor sostenido." if evergreen_actions else "No se detectan oportunidades evergreen fuertes en esta corrida.",
        "Hay temas con señal de interés pero posible problema de empaque; revisar títulos/ángulos." if repackage_actions else "No se observan problemas dominantes de empaque en los top candidates.",
        "Varias señales tienen baja confianza; interpretar con cautela." if low_confidence_signals >= 3 else "La mayoría de señales tienen confianza suficiente para decisiones tácticas.",
    ]

    title_rows = tables["title_metrics"]

    def _title_stat(name: str) -> dict[str, Any]:
        for row in title_rows:
            if row.get("title_pattern") == name:
                return row
        return {}

    title_snapshot = {
        "has_number": _title_stat("has_number"),
        "has_question": _title_stat("has_question"),
        "mentions_ai": _title_stat("mentions_ai"),
        "mentions_finance": _title_stat("mentions_finance"),
    }

    videos_total = len(tables["video_metrics"])
    channels_total = len(tables["channel_metrics"])

    def _sum(rows: list[dict[str, Any]], field: str) -> float:
        return round(sum(_safe_float(row.get(field)) or 0.0 for row in rows), 4)

    key_metrics = {
        "videos_total": videos_total,
        "channels_total": channels_total,
        "total_views_delta": _sum(tables["video_metrics"], "views_delta"),
        "total_likes_delta": _sum(tables["video_metrics"], "likes_delta"),
        "total_comments_delta": _sum(tables["video_metrics"], "comments_delta"),
        "avg_engagement_rate": round(
            (_sum(tables["video_metrics"], "engagement_rate") / videos_total) if videos_total else 0.0,
            6,
        ),
        "total_alerts": len(alerts_rows),
        "total_action_candidates": len(tables["action_candidates"]),
        "high_priority_actions": len(high_priority_actions),
    }

    data_quality_notes: list[str] = []
    if low_confidence_signals > 0:
        data_quality_notes.append(f"Se detectaron {low_confidence_signals} señales low confidence.")
    if warnings:
        data_quality_notes.extend([f"Warning: {warning}" for warning in warnings[:5]])

    status = "success_with_warnings" if warnings else "success"

    brief_json = {
        "generated_at": generated_at,
        "week": week,
        "period_start": period_start,
        "period_end": period_end_str,
        "status": status,
        "executive_summary": executive_summary,
        "key_metrics": key_metrics,
        "top_actions_this_week": actions,
        "top_content_opportunities": content_ops,
        "watchlist_recommendations": watchlist,
        "opportunity_matrix": matrix,
        "top_videos_by_growth": videos_growth,
        "top_alpha_videos": alpha_videos,
        "top_channels_by_momentum": channels_momentum,
        "topic_opportunities": topic_opportunities,
        "topic_metrics": topic_metrics,
        "semantic_clusters_to_watch": semantic_clusters,
        "content_driver_leaderboard": content_driver_leaderboard,
        "content_driver_feature_importance": content_driver_importance,
        "content_driver_feature_direction": content_driver_direction,
        "top_alerts": top_alerts,
        "creative_packages_to_execute": creative_packages,
        "title_pattern_snapshot": title_snapshot,
        "data_quality_notes": data_quality_notes,
        "warnings": warnings,
        "model_readiness": readiness,
    }

    markdown_lines = [
        "# Weekly YouTube Intelligence Brief",
        "",
        "## Executive Summary",
    ]
    markdown_lines.extend([f"- {line}" for line in executive_summary])

    markdown_lines.extend(["", "## Key Metrics"])
    markdown_lines.extend(_tabulate(["metric", "value"], [[key, str(value)] for key, value in key_metrics.items()]))

    markdown_lines.extend(["", "## What Actions Should I Take This Week?"])
    markdown_lines.extend(
        _tabulate(
            ["priority", "action_type", "recommendation", "reason", "confidence_level", "decision_score", "evidence", "dashboard_tab"],
            [
                [
                    str(row.get("priority", "")),
                    str(row.get("action_type", "")),
                    str(row.get("recommended_action", row.get("recommendation", ""))),
                    str(row.get("reason", "")),
                    str(row.get("confidence_level", "")),
                    str(row.get("decision_score", "")),
                    str(row.get("evidence_json", ""))[:120],
                    str(row.get("dashboard_tab", "")),
                ]
                for row in actions
            ],
        )
    )

    markdown_lines.extend(["", "## Top Content Opportunities"])
    markdown_lines.extend(
        _tabulate(
            ["content_strategy", "source_title", "why_it_matters", "evidence_score", "recommended_timeframe"],
            [
                [
                    str(row.get("content_strategy", "")),
                    str(row.get("source_title", "")),
                    str(row.get("why_it_matters", "")),
                    str(row.get("evidence_score", "")),
                    str(row.get("recommended_timeframe", "")),
                ]
                for row in content_ops
            ],
        )
    )

    markdown_lines.extend(["", "## Watchlist"])
    markdown_lines.extend(
        _tabulate(
            ["entity_type", "entity_id", "title", "reason", "watch_priority"],
            [
                [
                    str(row.get("entity_type", "")),
                    str(row.get("entity_id", "")),
                    str(row.get("title", "")),
                    str(row.get("reason", "")),
                    str(row.get("watch_priority", "")),
                ]
                for row in watchlist
            ],
        )
    )

    markdown_lines.extend(["", "## Topic Opportunities"])
    markdown_lines.extend(
        _tabulate(
            ["topic", "opportunity_type", "topic_opportunity_score", "recommended_action"],
            [
                [
                    str(row.get("topic", "")),
                    str(row.get("opportunity_type", "")),
                    str(row.get("topic_opportunity_score", "")),
                    str(row.get("recommended_action", "")),
                ]
                for row in topic_opportunities
            ],
        )
    )

    markdown_lines.extend(["", "## Title Patterns That Worked"])
    markdown_lines.extend(
        _tabulate(
            ["title_pattern", "video_count", "avg_views_delta", "avg_engagement_rate", "title_pattern_success_score"],
            [
                [
                    str(row.get("title_pattern", "")),
                    str(row.get("video_count", "")),
                    str(row.get("avg_views_delta", "")),
                    str(row.get("avg_engagement_rate", "")),
                    str(row.get("title_pattern_success_score", "")),
                ]
                for row in _sort_desc(tables.get("title_pattern_metrics", []), "title_pattern_success_score")[:10]
            ],
        )
    )

    markdown_lines.extend(["", "## Semantic Clusters to Watch"])
    markdown_lines.extend(
        _tabulate(
            ["video_id", "semantic_cluster_id", "semantic_cluster_label", "cluster_top_terms"],
            [
                [
                    str(row.get("video_id", "")),
                    str(row.get("semantic_cluster_id", "")),
                    str(row.get("semantic_cluster_label", "")),
                    str(row.get("cluster_top_terms", "")),
                ]
                for row in semantic_clusters
            ],
        )
    )

    markdown_lines.extend(["", "## Content Drivers"])
    if content_driver_importance:
        top_growth = [row for row in content_driver_importance if row.get("target") == "future_log_views_delta_7d"][:5]
        top_engagement = [row for row in content_driver_importance if row.get("target") == "future_engagement_delta_7d"][:5]
        pos_direction = [row for row in content_driver_direction if str(row.get("direction", "")).lower() == "positive"][:5]
        neg_direction = [row for row in content_driver_direction if str(row.get("direction", "")).lower() == "negative"][:5]
        markdown_lines.extend(["### Variables que maximizan future_log_views_delta_7d"])
        markdown_lines.extend([f"- {row.get('feature', '')} ({row.get('model_family', '')})" for row in top_growth] or ["- No data"])
        markdown_lines.extend(["", "### Variables que maximizan engagement"])
        markdown_lines.extend([f"- {row.get('feature', '')} ({row.get('model_family', '')})" for row in top_engagement] or ["- No data"])
        markdown_lines.extend(["", "### Variables de dirección positiva"])
        markdown_lines.extend([f"- {row.get('feature', '')} ({row.get('direction_method', '')})" for row in pos_direction] or ["- No data"])
        markdown_lines.extend(["", "### Variables de dirección negativa"])
        markdown_lines.extend([f"- {row.get('feature', '')} ({row.get('direction_method', '')})" for row in neg_direction] or ["- No data"])
        markdown_lines.extend(["", "Advertencia: estas importancias son predictivas, no causales."])
    else:
        markdown_lines.extend(["- Content driver outputs no disponibles en esta corrida."])

    markdown_lines.extend(["", "## Creative Packages to Execute"])
    markdown_lines.extend(
        _tabulate(
            ["package_type", "topic", "creative_angle", "recommended_format", "creative_execution_score", "recommended_next_step"],
            [
                [
                    str(row.get("package_type", "")),
                    str(row.get("topic", "")),
                    str(row.get("creative_angle", "")),
                    str(row.get("recommended_format", "")),
                    str(row.get("creative_execution_score", "")),
                    str(row.get("recommended_next_step", "")),
                ]
                for row in creative_packages
            ],
        )
    )

    top_package_ids = [str(row.get("creative_package_id", "")) for row in creative_packages if row.get("creative_package_id")]
    selected_titles = [row for row in creative_titles if str(row.get("creative_package_id", "")) in top_package_ids][:3]
    selected_hooks = [row for row in creative_hooks if str(row.get("creative_package_id", "")) in top_package_ids][:3]

    markdown_lines.extend(["", "## Suggested Titles & Hooks"])
    markdown_lines.append("### Suggested Titles")
    if selected_titles:
        markdown_lines.extend([f"- {str(row.get('title_candidate', ''))}" for row in selected_titles])
    else:
        markdown_lines.append("- No title candidates available")

    markdown_lines.append("### Suggested Hooks")
    if selected_hooks:
        markdown_lines.extend([f"- {str(row.get('hook_text', ''))}" for row in selected_hooks])
    else:
        markdown_lines.append("- No hook candidates available")

    markdown_lines.extend(["", "## Model Readiness"])
    if readiness:
        next_steps = readiness.get("recommended_next_steps", [])
        markdown_lines.extend(
            [
                f"- status: {readiness.get('status', 'unknown')}",
                f"- trainable_examples: {readiness.get('trainable_examples', 0)}",
                f"- examples_missing_for_exploratory: {readiness.get('examples_missing_for_exploratory', 0)}",
                f"- primary_blocker: {gap.get('primary_blocker', 'unknown')}",
                f"- next step: {(next_steps[0] if isinstance(next_steps, list) and next_steps else 'N/A')}",
            ]
        )
    else:
        markdown_lines.extend(["- Model readiness diagnostics no disponibles en esta corrida."])

    markdown_lines.extend(["", "## Opportunity Matrix"])
    markdown_lines.extend(
        _tabulate(
            ["action_type", "candidates_count", "avg_decision_score", "recommended_focus"],
            [
                [
                    str(row.get("action_type", "")),
                    str(row.get("candidates_count", "")),
                    str(row.get("avg_decision_score", "")),
                    str(row.get("recommended_focus", "")),
                ]
                for row in matrix
            ],
        )
    )

    markdown_lines.extend(["", "## Top Videos by Growth"])
    markdown_lines.extend(_tabulate(["video_id", "title", "views_delta"], [[str(r.get("video_id", "")), str(r.get("title", "")), str(r.get("views_delta", ""))] for r in videos_growth]))

    markdown_lines.extend(["", "## Top Alpha Videos"])
    markdown_lines.extend(_tabulate(["video_id", "title", "alpha_score"], [[str(r.get("video_id", "")), str(r.get("title", "")), str(r.get("alpha_score", ""))] for r in alpha_videos]))

    markdown_lines.extend(["", "## Channel Momentum"])
    markdown_lines.extend(_tabulate(["channel_id", "channel_name", "channel_momentum_score"], [[str(r.get("channel_id", "")), str(r.get("channel_name", "")), str(r.get("channel_momentum_score", ""))] for r in channels_momentum]))

    markdown_lines.extend(["", "## Alerts to Watch"])
    markdown_lines.extend(_tabulate(["severity", "signal_type", "entity_id", "adjusted_signal_score"], [[str(r.get("severity", "")), str(r.get("signal_type", "")), str(r.get("entity_id", "")), str(r.get("adjusted_signal_score", ""))] for r in top_alerts]))

    markdown_lines.extend(["", "## Title Pattern Snapshot"])
    for key, stats in title_snapshot.items():
        markdown_lines.append(f"- {key}: sample_size={stats.get('sample_size', '')}, avg_views_delta={stats.get('avg_views_delta', '')}")

    markdown_lines.extend(["", "## Data Quality Notes"])
    markdown_lines.extend([f"- {note}" for note in data_quality_notes] or ["- Sin notas adicionales."])

    markdown_lines.extend(["", "## Recommended Reading in Dashboard", "- Alerts", "- Advanced", "- Scores", "- Decision (cuando exista en dashboard futuro)"])

    markdown_content = "\n".join(markdown_lines).strip() + "\n"
    html_content = _markdown_to_html(markdown_content)

    brief_dir = data_root / "briefs"
    week_dir = brief_dir / f"week={week}"

    latest_md = brief_dir / "latest_weekly_brief.md"
    latest_html = brief_dir / "latest_weekly_brief.html"
    latest_json = brief_dir / "latest_weekly_brief.json"
    week_md = week_dir / "weekly_brief.md"
    week_html = week_dir / "weekly_brief.html"
    week_json = week_dir / "weekly_brief.json"

    _write_text(latest_md, markdown_content)
    _write_text(latest_html, html_content)
    _write_json(latest_json, brief_json)
    _write_text(week_md, markdown_content)
    _write_text(week_html, html_content)
    _write_json(week_json, brief_json)

    return {
        "status": status,
        "brief_dir": str(brief_dir),
        "latest_markdown_path": str(latest_md),
        "latest_html_path": str(latest_html),
        "latest_json_path": str(latest_json),
        "weekly_markdown_path": str(week_md),
        "weekly_html_path": str(week_html),
        "weekly_json_path": str(week_json),
        "warnings": warnings,
    }
