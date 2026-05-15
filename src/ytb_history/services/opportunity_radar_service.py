"""Commercial Opportunity Radar generator from existing intelligence artifacts."""

from __future__ import annotations

import csv
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


INPUT_FILES = {
    "video_metrics": Path("analytics/latest/latest_video_metrics.csv"),
    "channel_advanced": Path("analytics/latest/latest_channel_advanced_metrics.csv"),
    "topic_opportunities": Path("topic_intelligence/latest_topic_opportunities.csv"),
    "topic_metrics": Path("topic_intelligence/latest_topic_metrics.csv"),
    "title_pattern_metrics": Path("topic_intelligence/latest_title_pattern_metrics.csv"),
    "action_candidates": Path("decision/latest_action_candidates.csv"),
    "content_opportunities": Path("decision/latest_content_opportunities.csv"),
    "creative_packages": Path("creative_packages/latest_creative_packages.csv"),
    "creative_titles": Path("creative_packages/latest_title_candidates.csv"),
    "creative_hooks": Path("creative_packages/latest_hook_candidates.csv"),
    "latest_alerts": Path("alerts/latest_alerts.json"),
    "alert_summary": Path("alerts/alert_summary.json"),
    "weekly_brief": Path("briefs/latest_weekly_brief.json"),
}

DEFAULT_PROFILE = {
    "client_name": "Pilot Agency Prospect",
    "category_name": "Negocios, finanzas e IA en espanol",
    "package_name": "Weekly YouTube Opportunity Radar",
    "plan_name": "Pilot Radar",
    "monthly_price_usd": 750,
    "max_channels": 50,
    "period_days": 7,
    "output_slug": "spanish-business-ai",
    "include_keywords": [
        "ai",
        "ia",
        "inteligencia artificial",
        "finanza",
        "dinero",
        "negocio",
        "business",
        "emprend",
        "podcast",
    ],
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
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _sort_desc(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (_safe_float(row.get(key)) is not None, _safe_float(row.get(key)) or 0.0), reverse=True)


def _short_text(value: Any, *, limit: int = 140) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _clean_text(value: Any) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    markers = ("\u00c3", "\u00c2", "\u00e2")
    if any(marker in text for marker in markers):
        try:
            repaired = text.encode("latin1").decode("utf-8")
            if sum(repaired.count(marker) for marker in markers) < sum(text.count(marker) for marker in markers):
                text = repaired
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return (
        text.replace("\u2026", "...")
        .replace("\u2192", "->")
        .replace("\u00b7", "-")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .strip()
    )


def _first_text(row: dict[str, Any], *keys: str, fallback: str = "Sin dato") -> str:
    for key in keys:
        value = _clean_text(row.get(key, ""))
        if value:
            return value
    return fallback


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "opportunity-radar"


def _load_config(config_path: str | Path, profile_name: str | None) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        if profile_name not in (None, "default", "spanish_business_ai"):
            raise ValueError(f"Commercial radar profile not found: {profile_name}")
        profile = dict(DEFAULT_PROFILE)
        profile["profile_name"] = "spanish_business_ai"
        return profile

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    profiles = raw.get("profiles", {})
    selected = profile_name or raw.get("default_profile") or "spanish_business_ai"
    if selected not in profiles:
        raise ValueError(f"Commercial radar profile not found: {selected}")
    profile = dict(DEFAULT_PROFILE)
    profile.update(profiles[selected] or {})
    profile["profile_name"] = selected
    profile["output_slug"] = profile.get("output_slug") or _slugify(str(profile.get("category_name", selected)))
    profile["include_keywords"] = [str(item).lower() for item in profile.get("include_keywords", [])]
    return profile


def _load_inputs(data_root: Path) -> tuple[dict[str, Any], list[str], dict[str, str]]:
    tables: dict[str, Any] = {}
    warnings: list[str] = []
    source_files: dict[str, str] = {}
    for key, rel_path in INPUT_FILES.items():
        path = data_root / rel_path
        source_files[key] = rel_path.as_posix()
        if not path.exists():
            warnings.append(f"Missing input file: {rel_path}")
            tables[key] = [] if path.suffix == ".csv" else {}
            continue
        try:
            if path.suffix == ".csv":
                tables[key] = _read_csv(path)
            else:
                tables[key] = _read_json(path)
        except (OSError, csv.Error, json.JSONDecodeError, UnicodeDecodeError) as exc:
            warnings.append(f"Could not read input file {rel_path}: {exc}")
            tables[key] = [] if path.suffix == ".csv" else {}
    return tables, warnings, source_files


def _row_matches_profile(row: dict[str, Any], profile: dict[str, Any]) -> bool:
    keywords = [str(item).lower() for item in profile.get("include_keywords", []) if str(item).strip()]
    if not keywords:
        return True
    text = " ".join(_clean_text(value).lower() for value in row.values())
    return any(keyword in text for keyword in keywords)


def _profile_rows(rows: list[dict[str, Any]], profile: dict[str, Any], warnings: list[str], section: str) -> list[dict[str, Any]]:
    filtered = [row for row in rows if _row_matches_profile(row, profile)]
    if filtered:
        return filtered
    if rows:
        warnings.append(f"category_filter_fallback:{section}")
    return rows


def _alert_rows(tables: dict[str, Any]) -> list[dict[str, Any]]:
    payload = tables.get("latest_alerts", {})
    if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
        return []
    return [row for row in payload["alerts"] if isinstance(row, dict)]


def _latest_report_dir(reports_root: Path) -> Path | None:
    candidates: list[Path] = []
    for dt_dir in reports_root.glob("dt=*"):
        if not dt_dir.is_dir():
            continue
        for run_dir in dt_dir.glob("run=*"):
            if run_dir.is_dir():
                candidates.append(run_dir)
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.parent.name, path.name))


def _load_latest_quota(data_root: Path, scoped_channels: int, total_channels_seen: int, warnings: list[str]) -> dict[str, Any]:
    latest_dir = _latest_report_dir(data_root / "reports")
    if latest_dir is None:
        warnings.append("quota_report_missing:no_runs")
        return {
            "status": "missing",
            "method": "No previous run found; quota proxy unavailable.",
            "estimated_units": {},
            "total_estimated_units": 0,
            "scoped_channel_share": 0.0,
        }

    quota_path = latest_dir / "quota_report.json"
    run_path = latest_dir / "run_summary.json"
    if not quota_path.exists():
        warnings.append("quota_report_missing:latest_run")
        return {
            "status": "missing",
            "source_run": str(latest_dir),
            "method": "Latest run has no quota_report.json.",
            "estimated_units": {},
            "total_estimated_units": 0,
            "scoped_channel_share": 0.0,
        }

    quota = _read_json(quota_path)
    run_summary = _read_json(run_path) if run_path.exists() else {}
    base_channels = _safe_int(run_summary.get("channels_total")) or total_channels_seen or scoped_channels
    share = min(1.0, (scoped_channels / base_channels) if base_channels else 0.0)
    estimated_units = quota.get("estimated_units", {}) if isinstance(quota.get("estimated_units"), dict) else {}
    observed_units = quota.get("observed_units", {}) if isinstance(quota.get("observed_units"), dict) else {}
    scaled_estimated = {endpoint: round((_safe_float(units) or 0.0) * share, 2) for endpoint, units in estimated_units.items()}
    scaled_observed = {endpoint: round((_safe_float(units) or 0.0) * share, 2) for endpoint, units in observed_units.items()}
    return {
        "status": "ok",
        "source_run": str(latest_dir),
        "method": "Proxy based on the latest run quota report and scoped channel share; not a billing meter.",
        "base_channels": base_channels,
        "scoped_channels": scoped_channels,
        "scoped_channel_share": round(share, 4),
        "estimated_units": scaled_estimated,
        "observed_units": scaled_observed,
        "total_estimated_units": round((_safe_float(quota.get("total_estimated_units")) or 0.0) * share, 2),
        "total_observed_units": round((_safe_float(quota.get("total_observed_units")) or 0.0) * share, 2),
        "operational_limit": quota.get("operational_limit"),
        "limit_status": quota.get("limit_status"),
    }


def _creative_title_lookup(rows: list[dict[str, Any]], field: str) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for row in rows:
        package_id = str(row.get("creative_package_id", "") or "")
        value = _clean_text(row.get(field, ""))
        if not package_id or not value:
            continue
        lookup.setdefault(package_id, []).append(value)
    return lookup


def _build_priority_opportunities(tables: dict[str, Any], profile: dict[str, Any], warnings: list[str]) -> list[dict[str, Any]]:
    action_rows = _profile_rows(tables.get("action_candidates", []), profile, warnings, "action_candidates")
    opportunities: list[dict[str, Any]] = []
    for row in _sort_desc(action_rows, "decision_score")[:10]:
        opportunities.append(
            {
                "source": "decision/latest_action_candidates.csv",
                "opportunity_type": _first_text(row, "action_type", fallback="action"),
                "priority": _first_text(row, "priority", fallback="medium"),
                "recommendation": _first_text(row, "recommended_action", "recommendation", fallback="Priorizar una prueba editorial pequena."),
                "evidence": _short_text(row.get("reason", row.get("evidence_json", ""))),
                "score": _safe_float(row.get("decision_score")) or 0.0,
                "confidence": _first_text(row, "confidence_level", fallback="unknown"),
                "timeframe": _first_text(row, "timeframe", "recommended_timeframe", fallback="this_week"),
                "dashboard_tab": _first_text(row, "dashboard_tab", fallback="Brief"),
            }
        )

    if opportunities:
        return opportunities

    content_rows = _profile_rows(tables.get("content_opportunities", []), profile, warnings, "content_opportunities")
    for row in _sort_desc(content_rows, "evidence_score")[:10]:
        opportunities.append(
            {
                "source": "decision/latest_content_opportunities.csv",
                "opportunity_type": _first_text(row, "opportunity_type", fallback="content_opportunity"),
                "priority": "medium",
                "recommendation": _first_text(row, "content_strategy", fallback="Convertir la senal en una prueba creativa."),
                "evidence": _short_text(row.get("why_it_matters", row.get("source_title", ""))),
                "score": _safe_float(row.get("evidence_score")) or 0.0,
                "confidence": _first_text(row, "confidence_level", fallback="unknown"),
                "timeframe": _first_text(row, "recommended_timeframe", fallback="this_week"),
                "dashboard_tab": "Content opportunities",
            }
        )
    return opportunities


def _build_summary(profile: dict[str, Any], tables: dict[str, Any], warnings: list[str], source_files: dict[str, str], data_root: Path, anonymize: bool) -> dict[str, Any]:
    video_rows_all = tables.get("video_metrics", []) if isinstance(tables.get("video_metrics"), list) else []
    channel_rows_all = tables.get("channel_advanced", []) if isinstance(tables.get("channel_advanced"), list) else []
    video_rows = _profile_rows(video_rows_all, profile, warnings, "video_metrics")
    channel_rows = _profile_rows(channel_rows_all, profile, warnings, "channel_advanced")
    scoped_channel_ids = {row.get("channel_id") for row in video_rows if row.get("channel_id")} | {row.get("channel_id") for row in channel_rows if row.get("channel_id")}
    total_channel_ids = {row.get("channel_id") for row in video_rows_all if row.get("channel_id")} | {row.get("channel_id") for row in channel_rows_all if row.get("channel_id")}

    channel_alias: dict[str, str] = {}
    video_alias: dict[str, str] = {}

    def _channel_name(row: dict[str, Any]) -> str:
        raw = _first_text(row, "channel_name", fallback="Canal sin nombre")
        if not anonymize:
            return raw
        key = str(row.get("channel_id") or raw)
        channel_alias.setdefault(key, f"Canal {len(channel_alias) + 1}")
        return channel_alias[key]

    def _video_title(row: dict[str, Any]) -> str:
        raw = _first_text(row, "title", "source_title", fallback="Video sin titulo")
        if not anonymize:
            return raw
        key = str(row.get("video_id") or row.get("source_video_id") or raw)
        video_alias.setdefault(key, f"Video {len(video_alias) + 1}")
        return video_alias[key]

    accelerating_videos = [
        {
            "source": "analytics/latest/latest_video_metrics.csv",
            "title": _video_title(row),
            "channel_name": _channel_name(row),
            "views_delta": _safe_int(row.get("views_delta")) or 0,
            "engagement_rate": _safe_float(row.get("engagement_rate")) or 0.0,
            "evidence": "Crecimiento observado contra la ventana historica local; revisar angulo, empaque y timing.",
        }
        for row in _sort_desc(video_rows, "views_delta")[:10]
    ]

    channels_to_watch = [
        {
            "source": "analytics/latest/latest_channel_advanced_metrics.csv",
            "channel_name": _channel_name(row),
            "momentum_score": _safe_float(row.get("channel_momentum_score")) or 0.0,
            "views_delta": _safe_int(row.get("total_views_delta")) or 0,
            "recommended_watch": "Auditar frecuencia, formato, promesa editorial y cambios de metadata antes de copiar aprendizajes.",
        }
        for row in _sort_desc(channel_rows, "channel_momentum_score")[:8]
    ]

    topic_rows = _profile_rows(tables.get("topic_opportunities", []), profile, warnings, "topic_opportunities")
    emerging_topics = [
        {
            "source": "topic_intelligence/latest_topic_opportunities.csv",
            "topic": _first_text(row, "topic", fallback=profile["category_name"]),
            "opportunity_type": _first_text(row, "opportunity_type", fallback="watch_topic"),
            "score": _safe_float(row.get("topic_opportunity_score")) or 0.0,
            "recommended_action": _first_text(row, "recommended_action", fallback="Validar con una pieza de bajo costo."),
        }
        for row in _sort_desc(topic_rows, "topic_opportunity_score")[:8]
    ]

    title_patterns = [
        {
            "source": "topic_intelligence/latest_title_pattern_metrics.csv",
            "pattern": _first_text(row, "title_pattern", fallback="pattern"),
            "success_score": _safe_float(row.get("title_pattern_success_score")) or 0.0,
            "avg_views_delta": _safe_float(row.get("avg_views_delta")) or 0.0,
            "examples": _short_text(row.get("example_titles", ""), limit=180),
        }
        for row in _sort_desc(tables.get("title_pattern_metrics", []), "title_pattern_success_score")[:8]
    ]

    title_lookup = _creative_title_lookup(tables.get("creative_titles", []), "title_candidate")
    hook_lookup = _creative_title_lookup(tables.get("creative_hooks", []), "hook_text")
    creative_rows = _profile_rows(tables.get("creative_packages", []), profile, warnings, "creative_packages")
    creative_packages = []
    for row in _sort_desc(creative_rows, "creative_execution_score")[:5]:
        package_id = str(row.get("creative_package_id", "") or "")
        creative_packages.append(
            {
                "source": "creative_packages/latest_creative_packages.csv",
                "package_type": _first_text(row, "package_type", fallback="creative_package"),
                "angle": _first_text(row, "creative_angle", "topic", fallback="Adaptar senal a un angulo propio."),
                "format": _first_text(row, "recommended_format", fallback="video corto"),
                "score": _safe_float(row.get("creative_execution_score")) or 0.0,
                "next_step": _first_text(row, "recommended_next_step", fallback="Preparar guion y thumbnail brief."),
                "title_candidate": _short_text((title_lookup.get(package_id) or [""])[0], limit=110),
                "hook": _short_text((hook_lookup.get(package_id) or [""])[0], limit=120),
            }
        )

    alerts = sorted(_alert_rows(tables), key=lambda row: _safe_float(row.get("adjusted_signal_score")) or 0.0, reverse=True)[:5]
    priority_opportunities = _build_priority_opportunities(tables, profile, warnings)
    quota_report = _load_latest_quota(data_root, len(scoped_channel_ids), len(total_channel_ids), warnings)
    status = "success_with_warnings" if warnings else "success"
    key_metrics = {
        "videos_considered": len(video_rows),
        "channels_in_scope": len(scoped_channel_ids),
        "priority_opportunities": len(priority_opportunities),
        "creative_packages": len(creative_packages),
        "alerts_considered": len(alerts),
        "max_channels_for_offer": _safe_int(profile.get("max_channels")) or 0,
        "monthly_price_usd": _safe_int(profile.get("monthly_price_usd")) or 0,
    }

    executive_summary = [
        f"Vender como {profile['package_name']} para {profile['category_name']}, no como dashboard generico.",
        f"Oferta recomendada: {profile['plan_name']} desde USD {key_metrics['monthly_price_usd']}/mes para hasta {key_metrics['max_channels_for_offer']} canales.",
        f"Esta corrida resume {key_metrics['videos_considered']} videos y {key_metrics['channels_in_scope']} canales en el alcance comercial configurado.",
        "El entregable muestra senales y oportunidades, no promesas de views ni causalidad.",
        "El dashboard queda como anexo de evidencia; la venta principal es claridad editorial semanal.",
    ]

    return {
        "schema_version": "commercial_opportunity_radar_v1",
        "generated_at": _now_iso(),
        "status": status,
        "profile": {
            "profile_name": profile["profile_name"],
            "client_name": profile["client_name"],
            "category_name": profile["category_name"],
            "package_name": profile["package_name"],
            "plan_name": profile["plan_name"],
            "monthly_price_usd": key_metrics["monthly_price_usd"],
            "max_channels": key_metrics["max_channels_for_offer"],
            "period_days": _safe_int(profile.get("period_days")) or 7,
            "anonymized": anonymize,
        },
        "source_files": source_files,
        "key_metrics": key_metrics,
        "executive_summary": executive_summary,
        "priority_opportunities": priority_opportunities,
        "accelerating_videos": accelerating_videos,
        "channels_to_watch": channels_to_watch,
        "emerging_topics": emerging_topics,
        "title_patterns": title_patterns,
        "creative_packages": creative_packages,
        "alerts_to_watch": [
            {
                "source": "alerts/latest_alerts.json",
                "signal_type": _first_text(row, "signal_type", fallback="alert"),
                "severity": _first_text(row, "severity", fallback="medium"),
                "entity": _short_text(_first_text(row, "title", "channel_name", "entity_id", fallback="Entidad sin nombre"), limit=120),
                "score": _safe_float(row.get("adjusted_signal_score")) or 0.0,
                "recommended_action": _first_text(row, "recommended_action", fallback="Monitorear antes de escalar produccion."),
            }
            for row in alerts
        ],
        "quota_report": quota_report,
        "commercial_next_actions": [
            "Enviar mini-brief con 3 oportunidades a prospectos de agencia.",
            "Vender piloto mensual antes de construir auth, billing o SaaS multi-tenant.",
            "Medir cuales secciones generan respuesta: oportunidades, titulos, temas, paquetes creativos o dashboard.",
            "Recortar el entregable despues del primer piloto y mantener solo lo que guie decisiones.",
        ],
        "data_policy_notes": [
            "El radar vende insights derivados; no redistribuye un feed crudo de datos de YouTube.",
            "Las senales son correlacionales y deben presentarse como hipotesis editoriales, no garantias de crecimiento.",
            "La transcripcion solo debe usarse con videos propios, autorizados o medios provistos por el cliente.",
            "Referencias para compliance: YouTube API Services Policies y YouTube Data API quota calculator.",
        ],
        "warnings": warnings,
    }


def _tabulate(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(_md_cell(header) for header in headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(_md_cell(cell) for cell in row) + " |")
    return lines


def _md_cell(value: Any) -> str:
    return _clean_text(value).replace("|", "\\|")


def _build_markdown(summary: dict[str, Any]) -> str:
    profile = summary["profile"]
    lines = [
        "# Weekly YouTube Opportunity Radar",
        "",
        "## 1. Executive Read",
        f"- Cliente/perfil: {profile['client_name']}",
        f"- Categoria: {profile['category_name']}",
        f"- Oferta: {profile['plan_name']} - USD {profile['monthly_price_usd']}/mes - hasta {profile['max_channels']} canales",
        f"- Generado: {summary['generated_at']}",
    ]
    lines.extend([f"- {item}" for item in summary["executive_summary"]])

    lines.extend(["", "## 2. Que hacer esta semana"])
    lines.extend(
        _tabulate(
            ["Tipo", "Prioridad", "Recomendacion", "Score", "Evidencia"],
            [
                [
                    item["opportunity_type"],
                    item["priority"],
                    _short_text(item["recommendation"], limit=85),
                    f"{item['score']:.2f}",
                    _short_text(item["evidence"], limit=95),
                ]
                for item in summary["priority_opportunities"][:10]
            ],
        )
        if summary["priority_opportunities"]
        else ["- Sin oportunidades priorizadas; regenerar decision layer y creative packages."]
    )

    lines.extend(["", "## 3. Videos acelerando"])
    lines.extend(
        _tabulate(
            ["Video", "Canal", "Views delta", "Lectura"],
            [[_short_text(item["title"], limit=70), item["channel_name"], str(item["views_delta"]), item["evidence"]] for item in summary["accelerating_videos"][:8]],
        )
        if summary["accelerating_videos"]
        else ["- Sin videos en alcance comercial."]
    )

    lines.extend(["", "## 4. Canales a observar"])
    lines.extend(
        _tabulate(
            ["Canal", "Momentum", "Views delta", "Que mirar"],
            [[item["channel_name"], f"{item['momentum_score']:.2f}", str(item["views_delta"]), item["recommended_watch"]] for item in summary["channels_to_watch"][:6]],
        )
        if summary["channels_to_watch"]
        else ["- Sin canales destacados."]
    )

    lines.extend(["", "## 5. Temas y patrones de titulo"])
    if summary["emerging_topics"]:
        lines.append("Temas:")
        lines.extend(_tabulate(["Tema", "Tipo", "Score", "Accion"], [[item["topic"], item["opportunity_type"], f"{item['score']:.2f}", item["recommended_action"]] for item in summary["emerging_topics"][:5]]))
    if summary["title_patterns"]:
        lines.append("")
        lines.append("Patrones:")
        lines.extend(_tabulate(["Patron", "Score", "Views delta prom.", "Ejemplos"], [[item["pattern"], f"{item['success_score']:.2f}", f"{item['avg_views_delta']:.2f}", item["examples"]] for item in summary["title_patterns"][:5]]))
    if not summary["emerging_topics"] and not summary["title_patterns"]:
        lines.append("- Sin senales tematicas suficientes.")

    lines.extend(["", "## 6. Paquetes creativos accionables"])
    lines.extend(
        [
            f"- **{_short_text(item['angle'], limit=100)}** | formato: {item['format']} | score: {item['score']:.2f} | titulo: {_short_text(item['title_candidate'], limit=90) or 'por definir'} | hook: {_short_text(item['hook'], limit=90) or 'por definir'}"
            for item in summary["creative_packages"][:5]
        ]
        or ["- Sin paquetes creativos disponibles; ejecutar generate-creative-packages."]
    )

    lines.extend(["", "## 7. Alertas y riesgos"])
    lines.extend(
        [f"- **{item['signal_type']}** ({item['severity']}): {item['entity']} -> {item['recommended_action']}" for item in summary["alerts_to_watch"]]
        or ["- Sin alertas destacadas."]
    )

    quota = summary["quota_report"]
    lines.extend(
        [
            "",
            "## 8. Cuota y margen operativo",
            f"- Estado: {quota.get('status', 'unknown')}",
            f"- Metodo: {quota.get('method', 'No disponible')}",
            f"- Canales en alcance: {quota.get('scoped_channels', 0)} de base {quota.get('base_channels', 0)}",
            f"- Cuota estimada proxy: {quota.get('total_estimated_units', 0)} unidades",
            f"- Cuota observada proxy: {quota.get('total_observed_units', 0)} unidades",
        ]
    )

    lines.extend(["", "## 9. Politica comercial y metodologia"])
    lines.extend([f"- {item}" for item in summary["data_policy_notes"]])

    lines.extend(["", "## 10. Siguientes pasos comerciales"])
    lines.extend([f"- {item}" for item in summary["commercial_next_actions"]])

    if summary["warnings"]:
        lines.extend(["", "## Warnings"])
        lines.extend([f"- {warning}" for warning in summary["warnings"]])
    return "\n".join(lines).strip() + "\n"


def _markdown_to_html(markdown_text: str, *, document_title: str) -> str:
    lines = markdown_text.splitlines()
    html_lines: list[str] = [
        "<!doctype html>",
        "<html>",
        "<head>",
        "<meta charset=\"utf-8\">",
        f"<title>{html.escape(document_title)}</title>",
        "<style>",
        "body{font-family:Arial,sans-serif;line-height:1.5;margin:32px;max-width:1120px;color:#17202a}",
        "h1,h2{color:#102a43} table{border-collapse:collapse;width:100%;margin:12px 0}",
        "td,th{border:1px solid #d9e2ec;padding:8px;vertical-align:top} tr:first-child{font-weight:bold;background:#f0f4f8}",
        "li{margin:6px 0}",
        "</style>",
        "</head>",
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
                html_lines.append("<table><tbody>")
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


def generate_opportunity_radar(
    *,
    data_dir: str | Path = "data",
    config_path: str | Path = "config/commercial_radar.yaml",
    profile_name: str | None = None,
    output_dir: str | Path | None = None,
    anonymize: bool = False,
) -> dict[str, Any]:
    """Generate a commercial Opportunity Radar from existing local artifacts."""
    data_root = Path(data_dir)
    profile = _load_config(config_path, profile_name)
    tables, warnings, source_files = _load_inputs(data_root)
    summary = _build_summary(profile, tables, warnings, source_files, data_root, anonymize)

    slug = _slugify(str(profile.get("output_slug") or profile.get("profile_name") or profile.get("category_name")))
    output_root = Path(output_dir) if output_dir is not None else data_root / "commercial_radar" / slug
    markdown_path = output_root / "latest_opportunity_radar.md"
    html_path = output_root / "latest_opportunity_radar.html"
    summary_path = output_root / "latest_opportunity_radar.json"

    markdown = _build_markdown(summary)
    html_text = _markdown_to_html(markdown, document_title="Weekly YouTube Opportunity Radar")
    summary.update(
        {
            "radar_dir": str(output_root),
            "markdown_path": str(markdown_path),
            "html_path": str(html_path),
            "summary_path": str(summary_path),
            "files_written": [str(markdown_path), str(html_path), str(summary_path)],
        }
    )

    _write_text(markdown_path, markdown)
    _write_text(html_path, html_text)
    _write_json(summary_path, summary)
    return summary
