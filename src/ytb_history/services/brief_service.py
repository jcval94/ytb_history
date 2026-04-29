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


def _markdown_to_html(markdown_text: str) -> str:
    lines = markdown_text.splitlines()
    html_lines: list[str] = ["<html>", "<head><meta charset=\"utf-8\"><title>Weekly Intelligence Brief</title></head>", "<body>"]

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
