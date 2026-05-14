"""Category-level report generator from local analytics and intelligence artifacts."""

from __future__ import annotations

import csv
import html
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


INPUT_FILES = {
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
    "title_metrics": Path("analytics/latest/latest_title_metrics.csv"),
    "latest_alerts": Path("alerts/latest_alerts.json"),
    "alert_summary": Path("alerts/alert_summary.json"),
    "topic_opportunities": Path("topic_intelligence/latest_topic_opportunities.csv"),
    "topic_metrics": Path("topic_intelligence/latest_topic_metrics.csv"),
    "title_pattern_metrics": Path("topic_intelligence/latest_title_pattern_metrics.csv"),
    "topic_summary": Path("topic_intelligence/topic_intelligence_summary.json"),
    "content_driver_leaderboard": Path("model_reports/latest_content_driver_leaderboard.csv"),
    "content_driver_feature_importance": Path("model_reports/latest_content_driver_feature_importance.csv"),
    "content_driver_feature_direction": Path("model_reports/latest_content_driver_feature_direction.csv"),
    "hybrid_recommendations": Path("model_intelligence/latest_hybrid_recommendations.csv"),
    "model_intelligence_summary": Path("model_intelligence/model_intelligence_summary.json"),
    "weekly_brief_json": Path("briefs/latest_weekly_brief.json"),
    "weekly_brief_markdown": Path("briefs/latest_weekly_brief.md"),
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


def _first_text(row: dict[str, Any], *keys: str, fallback: str = "Sin dato") -> str:
    for key in keys:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return fallback


def _short_text(value: Any, *, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _load_inputs(data_root: Path) -> tuple[dict[str, Any], list[str]]:
    tables: dict[str, Any] = {}
    warnings: list[str] = []
    for key, rel_path in INPUT_FILES.items():
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
        try:
            if path.suffix == ".csv":
                tables[key] = _read_csv(path)
            elif path.suffix == ".md":
                tables[key] = path.read_text(encoding="utf-8")
            else:
                tables[key] = _read_json(path)
        except (OSError, csv.Error, json.JSONDecodeError, UnicodeDecodeError) as exc:
            warnings.append(f"Could not read input file {path}: {exc}")
            tables[key] = [] if path.suffix == ".csv" else ("" if path.suffix == ".md" else {})
    return tables, warnings


def _period_bounds(video_rows: list[dict[str, Any]], period_days: int) -> tuple[str, str, list[dict[str, Any]]]:
    candidate_dates = [_parse_date(row.get("execution_date")) or _parse_date(row.get("upload_date")) for row in video_rows]
    known_dates = [item for item in candidate_dates if item is not None]
    period_end = max(known_dates) if known_dates else datetime.now(timezone.utc).date()
    period_start = period_end - timedelta(days=max(0, period_days - 1))
    rows_in_period = []
    for row in video_rows:
        row_date = _parse_date(row.get("upload_date")) or _parse_date(row.get("execution_date"))
        if row_date is None or row_date >= period_start:
            rows_in_period.append(row)
    return period_start.isoformat(), period_end.isoformat(), rows_in_period


def _tabulate(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _markdown_to_html(markdown_text: str, *, document_title: str) -> str:
    lines = markdown_text.splitlines()
    html_lines: list[str] = [
        "<!doctype html>",
        "<html>",
        f"<head><meta charset=\"utf-8\"><title>{html.escape(document_title)}</title></head>",
        "<body>",
    ]
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
        if stripped.startswith("## ") or stripped.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if in_table:
                html_lines.append("</tbody></table>")
                in_table = False
            level = 2 if stripped.startswith("## ") else 1
            title = stripped[3:] if level == 2 else stripped[2:]
            html_lines.append(f"<h{level}>{html.escape(title)}</h{level}>")
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


def _alert_rows(tables: dict[str, Any]) -> list[dict[str, Any]]:
    payload = tables.get("latest_alerts", {})
    if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
        return []
    return [row for row in payload["alerts"] if isinstance(row, dict)]


def _build_summary(category_name: str, period_days: int, tables: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    video_rows = tables.get("video_metrics", []) if isinstance(tables.get("video_metrics"), list) else []
    period_start, period_end, period_videos = _period_bounds(video_rows, period_days)
    video_lookup = {row.get("video_id"): row for row in video_rows if row.get("video_id")}

    top_opportunities: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("topic_opportunities", []), "topic_opportunity_score")[:8]:
        top_opportunities.append(
            {
                "topic": _first_text(row, "topic", fallback=category_name),
                "opportunity_type": row.get("opportunity_type", ""),
                "score": _safe_float(row.get("topic_opportunity_score")) or 0.0,
                "recommended_action": _first_text(row, "recommended_action", fallback="Validar con una pieza pequeña y medir reacción."),
                "why_it_matters": _short_text(row.get("why_it_matters", "Señal combinada desde topic intelligence.")),
            }
        )

    accelerating_videos: list[dict[str, Any]] = []
    for row in _sort_desc(period_videos, "views_delta")[:8]:
        accelerating_videos.append(
            {
                "video_id": row.get("video_id", ""),
                "title": _first_text(row, "title", fallback="Video sin título"),
                "channel_name": row.get("channel_name", ""),
                "views_delta": _safe_int(row.get("views_delta")) or 0,
                "engagement_rate": _safe_float(row.get("engagement_rate")) or 0.0,
                "upload_date": row.get("upload_date", ""),
            }
        )

    winning_channels: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("channel_advanced", []), "channel_momentum_score")[:8]:
        winning_channels.append(
            {
                "channel_id": row.get("channel_id", ""),
                "channel_name": _first_text(row, "channel_name", fallback="Canal sin nombre"),
                "momentum_score": _safe_float(row.get("channel_momentum_score")) or 0.0,
                "total_views_delta": _safe_int(row.get("total_views_delta")) or 0,
                "top_video_title": _short_text(row.get("top_video_title", ""), limit=120),
            }
        )

    emerging_topics: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("topic_metrics", []), "topic_velocity_score")[:8]:
        emerging_topics.append(
            {
                "topic": _first_text(row, "topic", fallback="Tema sin etiqueta"),
                "velocity_score": _safe_float(row.get("topic_velocity_score")) or 0.0,
                "opportunity_score": _safe_float(row.get("topic_opportunity_score")) or 0.0,
                "video_count": _safe_int(row.get("video_count")) or 0,
            }
        )

    title_patterns: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("title_pattern_metrics", []), "title_pattern_success_score")[:8]:
        title_patterns.append(
            {
                "pattern": _first_text(row, "title_pattern", fallback="Patrón sin etiqueta"),
                "success_score": _safe_float(row.get("title_pattern_success_score")) or 0.0,
                "avg_views_delta": _safe_float(row.get("avg_views_delta")) or 0.0,
                "examples": _short_text(row.get("example_titles", ""), limit=200),
            }
        )

    model_recommendations: list[dict[str, Any]] = []
    for row in _sort_desc(tables.get("hybrid_recommendations", []), "hybrid_decision_score")[:8]:
        details = video_lookup.get(row.get("video_id"), {})
        model_recommendations.append(
            {
                "video_id": row.get("video_id", ""),
                "title": _first_text(details, "title", fallback="Video recomendado por modelo"),
                "hybrid_decision_score": _safe_float(row.get("hybrid_decision_score")) or 0.0,
                "confidence_level": row.get("confidence_level", ""),
                "recommended_action": "Analizar empaque, ángulo y audiencia del video para derivar una prueba creativa.",
            }
        )

    alerts = sorted(
        _alert_rows(tables),
        key=lambda row: (_safe_float(row.get("adjusted_signal_score")) or _safe_float(row.get("signal_score")) or 0.0),
        reverse=True,
    )[:8]

    total_views_delta = sum(_safe_float(row.get("views_delta")) or 0.0 for row in period_videos)
    total_channels = len({row.get("channel_id") for row in period_videos if row.get("channel_id")})
    status = "success_with_warnings" if warnings else "success"

    return {
        "generated_at": _now_iso(),
        "category_name": category_name,
        "period_days": period_days,
        "period_start": period_start,
        "period_end": period_end,
        "status": status,
        "key_metrics": {
            "videos_in_period": len(period_videos),
            "channels_in_period": total_channels,
            "total_views_delta": int(total_views_delta),
            "alerts_considered": len(alerts),
            "input_warnings": len(warnings),
        },
        "top_opportunities": top_opportunities,
        "accelerating_videos": accelerating_videos,
        "winning_channels": winning_channels,
        "emerging_topics": emerging_topics,
        "title_patterns": title_patterns,
        "model_recommendations": model_recommendations,
        "alerts": alerts,
        "limitations": _build_limitations(warnings),
        "warnings": warnings,
    }


def _build_limitations(warnings: list[str]) -> list[str]:
    limitations = [
        "El reporte usa artefactos locales ya generados; no realiza llamadas a la YouTube Data API ni recalcula métricas crudas.",
        "Las oportunidades son señales de priorización, no garantías de desempeño futuro.",
        "Los rankings dependen de snapshots recientes y pueden subrepresentar canales o videos con historial incompleto.",
    ]
    if warnings:
        limitations.append("Faltan algunos insumos; las secciones afectadas se completan con los datos disponibles y deben interpretarse con cautela.")
    return limitations


def _build_recommendations(summary: dict[str, Any]) -> list[str]:
    recs: list[str] = []
    for item in summary["top_opportunities"][:3]:
        recs.append(f"Probar un contenido sobre {item['topic']} con foco en: {item['recommended_action']}")
    for item in summary["title_patterns"][:2]:
        recs.append(f"Usar el patrón de título '{item['pattern']}' como hipótesis de empaque para la siguiente tanda creativa.")
    for item in summary["winning_channels"][:2]:
        recs.append(f"Auditar a {item['channel_name']} para entender frecuencia, duración, formato y promesa editorial reciente.")
    if not recs:
        recs.append("Regenerar analytics, alerts, topic intelligence y model intelligence antes de tomar decisiones de producción.")
    return recs[:7]


def _build_markdown(summary: dict[str, Any]) -> str:
    key_metrics = summary["key_metrics"]
    lines = [
        f"# Reporte de categoría: {summary['category_name']}",
        "",
        "## 1. Portada",
        f"- Categoría: {summary['category_name']}",
        f"- Ventana analizada: {summary['period_start']} a {summary['period_end']} ({summary['period_days']} días)",
        f"- Generado: {summary['generated_at']}",
        f"- Videos en ventana: {key_metrics['videos_in_period']}",
        f"- Canales en ventana: {key_metrics['channels_in_period']}",
        f"- Views delta acumulado: {key_metrics['total_views_delta']}",
        "",
        "## 2. Metodología",
        "- Se consolidan artefactos locales de analytics, alerts, topic intelligence, model reports, model intelligence y briefs.",
        "- La ventana se calcula desde las fechas disponibles en los artifacts locales; no se consultan APIs externas.",
        "- Se priorizan señales de crecimiento, momentum de canal, velocidad temática, patrones de título y recomendaciones híbridas.",
        "",
        "## 3. Top oportunidades",
    ]
    lines.extend(_tabulate(["Tema", "Tipo", "Score", "Acción"], [[i["topic"], i["opportunity_type"], f"{i['score']:.2f}", i["recommended_action"]] for i in summary["top_opportunities"][:5]]) if summary["top_opportunities"] else ["- Sin oportunidades disponibles."])
    lines.extend(["", "## 4. Videos acelerando"])
    lines.extend(_tabulate(["Video", "Canal", "Views delta", "Engagement"], [[_short_text(i["title"], limit=70), i["channel_name"], str(i["views_delta"]), f"{i['engagement_rate']:.4f}"] for i in summary["accelerating_videos"][:5]]) if summary["accelerating_videos"] else ["- Sin videos disponibles."])
    lines.extend(["", "## 5. Canales ganadores"])
    lines.extend(_tabulate(["Canal", "Momentum", "Views delta", "Top video"], [[i["channel_name"], f"{i['momentum_score']:.2f}", str(i["total_views_delta"]), i["top_video_title"]] for i in summary["winning_channels"][:5]]) if summary["winning_channels"] else ["- Sin canales disponibles."])
    lines.extend(["", "## 6. Temas emergentes"])
    lines.extend(_tabulate(["Tema", "Velocidad", "Oportunidad", "Videos"], [[i["topic"], f"{i['velocity_score']:.2f}", f"{i['opportunity_score']:.2f}", str(i["video_count"])] for i in summary["emerging_topics"][:5]]) if summary["emerging_topics"] else ["- Sin temas emergentes disponibles."])
    lines.extend(["", "## 7. Patrones de títulos"])
    lines.extend(_tabulate(["Patrón", "Success", "Avg views delta", "Ejemplos"], [[i["pattern"], f"{i['success_score']:.2f}", f"{i['avg_views_delta']:.2f}", i["examples"]] for i in summary["title_patterns"][:5]]) if summary["title_patterns"] else ["- Sin patrones disponibles."])
    lines.extend(["", "## 8. Recomendaciones accionables"])
    lines.extend([f"- {item}" for item in _build_recommendations(summary)])
    if summary["model_recommendations"]:
        lines.append("- Revisar videos priorizados por model intelligence para extraer ideas de ángulo, promesa y formato.")
    if summary["alerts"]:
        lines.append("- Usar alertas de alta señal como watchlist diaria antes de comprometer producción larga.")
    lines.extend(["", "## 9. Limitaciones de datos"])
    lines.extend([f"- {item}" for item in summary["limitations"]])
    return "\n".join(lines) + "\n"


def generate_category_report(
    *,
    category_name: str,
    data_dir: str | Path = "data",
    output_dir: str | Path | None = None,
    period_days: int = 30,
    format: str = "md",
) -> dict[str, Any]:
    """Generate category report artifacts from existing local data products."""
    if not category_name.strip():
        raise ValueError("category_name is required")
    if period_days <= 0:
        raise ValueError("period_days must be positive")
    if format not in {"md", "html"}:
        raise ValueError("format must be 'md' or 'html'")

    data_root = Path(data_dir)
    output_root = Path(output_dir) if output_dir is not None else data_root / "category_reports"
    tables, warnings = _load_inputs(data_root)
    summary = _build_summary(category_name.strip(), period_days, tables, warnings)

    markdown = _build_markdown(summary)
    html_text = _markdown_to_html(markdown, document_title=f"Reporte de categoría: {summary['category_name']}")

    markdown_path = output_root / "latest_category_report.md"
    html_path = output_root / "latest_category_report.html"
    summary_path = output_root / "category_report_summary.json"

    summary.update(
        {
            "markdown_path": str(markdown_path),
            "html_path": str(html_path),
            "summary_path": str(summary_path),
            "preferred_format": format,
            "preferred_report_path": str(html_path if format == "html" else markdown_path),
        }
    )

    _write_text(markdown_path, markdown)
    _write_text(html_path, html_text)
    _write_json(summary_path, summary)
    return summary
