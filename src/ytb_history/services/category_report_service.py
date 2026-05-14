"""Category-level report builder from local intelligence artifacts."""

from __future__ import annotations

import csv
import html
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


INPUT_FILES = {
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "video_advanced": Path("analytics/latest/latest_video_advanced_metrics.csv"),
    "video_scores": Path("analytics/latest/latest_video_scores.csv"),
    "channel_metrics": Path("analytics/latest/latest_channel_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
    "title_metrics": Path("analytics/latest/latest_title_metrics.csv"),
    "latest_alerts": Path("alerts/latest_alerts.json"),
    "alert_summary": Path("alerts/alert_summary.json"),
    "topic_metrics": Path("topic_intelligence/latest_topic_metrics.csv"),
    "topic_opportunities": Path("topic_intelligence/latest_topic_opportunities.csv"),
    "title_pattern_metrics": Path("topic_intelligence/latest_title_pattern_metrics.csv"),
    "video_topics": Path("topic_intelligence/latest_video_topics.csv"),
    "model_leaderboard": Path("model_reports/latest_model_leaderboard.csv"),
    "content_driver_leaderboard": Path("model_reports/latest_content_driver_leaderboard.csv"),
    "content_driver_importance": Path("model_reports/latest_content_driver_feature_importance.csv"),
    "content_driver_direction": Path("model_reports/latest_content_driver_feature_direction.csv"),
    "hybrid_recommendations": Path("model_intelligence/latest_hybrid_recommendations.csv"),
    "model_intelligence_summary": Path("model_intelligence/model_intelligence_summary.json"),
    "weekly_brief": Path("briefs/latest_weekly_brief.json"),
}

OUTPUT_MD = "latest_category_report.md"
OUTPUT_HTML = "latest_category_report.html"
OUTPUT_SUMMARY = "category_report_summary.json"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _sort_desc(rows: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    def _score(row: dict[str, Any]) -> float:
        for key in keys:
            value = _safe_float(row.get(key))
            if value is not None:
                return value
        return 0.0

    return sorted(rows, key=_score, reverse=True)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _filter_period(rows: list[dict[str, Any]], *, period_start: datetime) -> list[dict[str, Any]]:
    date_fields = ("execution_date", "upload_date", "created_at", "generated_at", "published_at")
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_dates = [_parse_datetime(row.get(field)) for field in date_fields if row.get(field)]
        known_dates = [value for value in row_dates if value is not None]
        if not known_dates or max(known_dates) >= period_start:
            filtered.append(row)
    return filtered


def _matches_category(row: dict[str, Any], category_name: str) -> bool:
    needle = category_name.strip().lower()
    if not needle:
        return True
    candidate_fields = (
        "category",
        "category_name",
        "topic",
        "topic_primary",
        "topic_secondary",
        "semantic_group",
        "semantic_cluster_label",
    )
    values = [str(row.get(field, "")).lower() for field in candidate_fields if row.get(field)]
    return not values or any(needle in value or value in needle for value in values)


def _filter_category(rows: list[dict[str, Any]], *, category_name: str) -> list[dict[str, Any]]:
    return [row for row in rows if _matches_category(row, category_name)]


def _load_inputs(data_root: Path, *, category_name: str, period_start: datetime) -> tuple[dict[str, Any], list[str]]:
    inputs: dict[str, Any] = {}
    warnings: list[str] = []
    for key, rel_path in INPUT_FILES.items():
        path = data_root / rel_path
        if not path.exists():
            warnings.append(f"Missing input file: {path}")
            inputs[key] = [] if rel_path.suffix == ".csv" else {}
            continue
        try:
            if rel_path.suffix == ".csv":
                rows = _read_csv(path)
                inputs[key] = _filter_category(_filter_period(rows, period_start=period_start), category_name=category_name)
            else:
                inputs[key] = _read_json(path)
        except (OSError, json.JSONDecodeError, csv.Error) as exc:
            warnings.append(f"Could not read input file {path}: {exc}")
            inputs[key] = [] if rel_path.suffix == ".csv" else {}
    return inputs, warnings


def _tabulate(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    if not rows:
        lines.append("| " + " | ".join(["No data"] + [""] * (len(headers) - 1)) + " |")
        return lines
    for row in rows:
        clean_row = [str(cell).replace("|", "/").replace("\n", " ") for cell in row]
        lines.append("| " + " | ".join(clean_row) + " |")
    return lines


def _markdown_to_html(markdown_text: str, *, title: str) -> str:
    html_lines = [
        "<!doctype html>",
        "<html>",
        f"<head><meta charset=\"utf-8\"><title>{html.escape(title)}</title></head>",
        "<body>",
    ]
    in_list = False
    in_table = False
    for line in markdown_text.splitlines():
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
            text = stripped[3:] if level == 2 else stripped[2:]
            html_lines.append(f"<h{level}>{html.escape(text)}</h{level}>")
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
                html_lines.append("<table border=\"1\"><tbody>")
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


def _alert_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("alerts"), list):
        return [row for row in payload["alerts"] if isinstance(row, dict)]
    return []


def _build_recommendations(
    *,
    opportunities: list[dict[str, Any]],
    accelerating: list[dict[str, Any]],
    channels: list[dict[str, Any]],
    topics: list[dict[str, Any]],
    drivers: list[dict[str, Any]],
) -> list[str]:
    recommendations: list[str] = []
    if opportunities:
        top = opportunities[0]
        recommendations.append(f"Priorizar '{top.get('title') or top.get('video_id')}' como referencia creativa por su score alto.")
    if accelerating:
        top = accelerating[0]
        recommendations.append(f"Replicar el ángulo de '{top.get('title') or top.get('video_id')}' mientras la aceleración siga positiva.")
    if channels:
        top = channels[0]
        recommendations.append(f"Monitorear al canal ganador '{top.get('channel_name') or top.get('channel_id')}' para detectar formatos repetibles.")
    if topics:
        top = topics[0]
        action = top.get("recommended_action") or "crear una prueba editorial pequeña"
        recommendations.append(f"Validar el tema '{top.get('topic', 'unknown')}' con la acción sugerida: {action}.")
    positive_drivers = [row for row in drivers if str(row.get("direction", "")).lower() == "positive"]
    if positive_drivers:
        recommendations.append(f"Incorporar la señal predictiva positiva '{positive_drivers[0].get('feature')}' en nuevos briefs.")
    if not recommendations:
        recommendations.append("Recolectar más datos recientes antes de tomar decisiones fuertes para esta categoría.")
    return recommendations[:8]


def generate_category_report(
    *,
    category_name: str,
    data_dir: str | Path = "data",
    output_dir: str | Path | None = None,
    period_days: int = 30,
    format: str = "md",
) -> dict[str, Any]:
    """Generate category-level Markdown, HTML, and JSON summary reports from local artifacts."""
    if not category_name.strip():
        raise ValueError("category_name is required")
    if period_days < 1:
        raise ValueError("period_days must be >= 1")
    if format not in {"md", "html"}:
        raise ValueError("format must be 'md' or 'html'")

    data_root = Path(data_dir)
    report_dir = Path(output_dir) if output_dir is not None else data_root / "category_reports"
    generated_at_dt = _now()
    period_start = generated_at_dt - timedelta(days=period_days)
    inputs, warnings = _load_inputs(data_root, category_name=category_name, period_start=period_start)

    opportunities = _sort_desc(list(inputs["video_scores"]), "opportunity_score", "alpha_score")[:10]
    accelerating = _sort_desc(list(inputs["video_advanced"]), "growth_acceleration_score", "growth_acceleration", "current_period_views_delta")[:10]
    winning_channels = _sort_desc(list(inputs["channel_advanced"]), "channel_momentum_score", "total_views_delta")[:10]
    emerging_topics = _sort_desc(list(inputs["topic_opportunities"]), "topic_opportunity_score", "topic_velocity_score")[:10]
    title_patterns = _sort_desc(list(inputs["title_pattern_metrics"]), "title_pattern_success_score", "avg_views_delta")[:10]
    alerts = _sort_desc(_alert_rows(inputs["latest_alerts"]), "adjusted_signal_score", "signal_score")[:10]
    hybrid_recommendations = _sort_desc(list(inputs["hybrid_recommendations"]), "hybrid_decision_score", "decision_score")[:10]
    drivers = _sort_desc(list(inputs["content_driver_direction"]), "direction_score")[:10]
    weekly_brief = inputs["weekly_brief"] if isinstance(inputs["weekly_brief"], dict) else {}
    brief_summary = weekly_brief.get("executive_summary", []) if isinstance(weekly_brief.get("executive_summary"), list) else []
    recommendations = _build_recommendations(
        opportunities=opportunities,
        accelerating=accelerating,
        channels=winning_channels,
        topics=emerging_topics,
        drivers=drivers,
    )

    title = f"Category Report: {category_name}"
    generated_at = generated_at_dt.isoformat()
    markdown_lines: list[str] = [
        f"# {title}",
        "",
        "## Portada",
        f"- Categoría: {category_name}",
        f"- Fecha de generación: {generated_at}",
        f"- Ventana analizada: últimos {period_days} días ({period_start.date().isoformat()} a {generated_at_dt.date().isoformat()})",
        f"- Fuentes locales: {', '.join(str(path) for path in INPUT_FILES.values())}",
        "",
        "## Metodología",
        "- Se consolidan artefactos locales ya generados de analytics, alerts, topic_intelligence, model_reports, model_intelligence y briefs.",
        "- Se priorizan filas recientes cuando incluyen campos de fecha; los insumos sin fecha se tratan como la versión vigente.",
        "- Si un insumo no expone categoría explícita, se usa como contexto transversal para no perder señales relevantes.",
        "- Los rankings son heurísticos y ordenan por los scores disponibles en cada artefacto.",
        "- El brief vigente aporta contexto ejecutivo transversal para contrastar las oportunidades de la categoría.",
        "",
        "### Contexto del brief vigente",
    ]
    markdown_lines.extend([f"- {item}" for item in brief_summary[:5]] or ["- No hay executive_summary disponible en briefs/latest_weekly_brief.json."])
    markdown_lines.extend([
        "",
        "## Top oportunidades",
    ])
    markdown_lines.extend(_tabulate(["video_id", "title", "channel", "opportunity_score", "alpha_score"], [[r.get("video_id", ""), r.get("title", ""), r.get("channel_name", ""), r.get("opportunity_score", ""), r.get("alpha_score", "")] for r in opportunities]))
    markdown_lines.extend(["", "## Videos acelerando"])
    markdown_lines.extend(_tabulate(["video_id", "title", "growth_trend", "growth_acceleration", "acceleration_score"], [[r.get("video_id", ""), r.get("title", ""), r.get("growth_trend_label", ""), r.get("growth_acceleration", ""), r.get("growth_acceleration_score", "")] for r in accelerating]))
    markdown_lines.extend(["", "## Canales ganadores"])
    markdown_lines.extend(_tabulate(["channel_id", "channel_name", "momentum_score", "total_views_delta", "top_video"], [[r.get("channel_id", ""), r.get("channel_name", ""), r.get("channel_momentum_score", ""), r.get("total_views_delta", ""), r.get("top_video_title", "")] for r in winning_channels]))
    markdown_lines.extend(["", "## Temas emergentes"])
    markdown_lines.extend(_tabulate(["topic", "type", "score", "velocity", "recommended_action"], [[r.get("topic", ""), r.get("opportunity_type", ""), r.get("topic_opportunity_score", ""), r.get("topic_velocity_score", ""), r.get("recommended_action", "")] for r in emerging_topics]))
    markdown_lines.extend(["", "## Patrones de títulos"])
    markdown_lines.extend(_tabulate(["title_pattern", "video_count", "avg_views_delta", "avg_engagement", "success_score"], [[r.get("title_pattern", ""), r.get("video_count", ""), r.get("avg_views_delta", ""), r.get("avg_engagement_rate", ""), r.get("title_pattern_success_score", "")] for r in title_patterns]))
    markdown_lines.extend(["", "## Señales de alertas y modelo"])
    markdown_lines.extend(_tabulate(["type", "entity", "severity/confidence", "score"], [[r.get("signal_type", ""), r.get("entity_id", ""), r.get("severity", ""), r.get("adjusted_signal_score", r.get("signal_score", ""))] for r in alerts]))
    markdown_lines.extend(["", "### Recomendaciones híbridas del modelo"])
    markdown_lines.extend(_tabulate(["video_id", "hybrid_score", "model_percentile", "decision_score", "confidence"], [[r.get("video_id", ""), r.get("hybrid_decision_score", ""), r.get("model_score_percentile", ""), r.get("decision_score", ""), r.get("confidence_level", "")] for r in hybrid_recommendations[:5]]))
    markdown_lines.extend(["", "## Recomendaciones accionables"])
    markdown_lines.extend([f"- {item}" for item in recommendations])
    markdown_lines.extend(["", "## Limitaciones de datos"])
    markdown_lines.extend([
        "- Este reporte no consulta APIs externas; depende de artefactos previamente materializados.",
        "- La relación entre features/modelos y resultados es predictiva, no causal.",
        "- Una categoría puede quedar parcialmente filtrada si los insumos no incluyen campos semánticos compatibles.",
        "- Las métricas recientes pueden estar incompletas si hubo fallos parciales en canales o corridas anteriores.",
    ])
    if warnings:
        markdown_lines.extend(["", "### Warnings de insumos"])
        markdown_lines.extend([f"- {warning}" for warning in warnings])

    markdown_text = "\n".join(markdown_lines) + "\n"
    html_text = _markdown_to_html(markdown_text, title=title)

    md_path = report_dir / OUTPUT_MD
    html_path = report_dir / OUTPUT_HTML
    summary_path = report_dir / OUTPUT_SUMMARY
    primary_path = md_path if format == "md" else html_path

    summary = {
        "status": "success",
        "category_name": category_name,
        "generated_at": generated_at,
        "period_days": period_days,
        "period_start": period_start.isoformat(),
        "period_end": generated_at,
        "format": format,
        "primary_report_path": str(primary_path),
        "markdown_path": str(md_path),
        "html_path": str(html_path),
        "summary_path": str(summary_path),
        "row_counts": {
            "top_opportunities": len(opportunities),
            "accelerating_videos": len(accelerating),
            "winning_channels": len(winning_channels),
            "emerging_topics": len(emerging_topics),
            "title_patterns": len(title_patterns),
            "alerts": len(alerts),
            "hybrid_recommendations": len(hybrid_recommendations),
            "brief_summary_items": len(brief_summary),
        },
        "recommendations": recommendations,
        "warnings": warnings,
    }

    _write_text(md_path, markdown_text)
    _write_text(html_path, html_text)
    _write_json(summary_path, summary)
    return summary
