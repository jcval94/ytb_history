"""Build topic and title intelligence from NLP feature artifacts."""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TOPIC_SEMANTIC_MAP = {
    "ai_tools": "ai_semantic_score",
    "finance_personal": "finance_semantic_score",
    "productivity": "productivity_semantic_score",
    "tutorial": "tutorial_semantic_score",
    "news_trends": "news_semantic_score",
}

TOPIC_TITLE_TERMS = {
    "ai_tools": {"ai", "ia", "chatgpt", "gemini", "claude", "agente", "automatizar"},
    "finance_personal": {"finanzas", "dinero", "inversión", "invertir", "ahorro", "crédito"},
    "productivity": {"productividad", "eficiencia", "hábitos", "organización", "tiempo"},
    "tutorial": {"cómo", "guía", "tutorial", "paso a paso", "aprende"},
    "news_trends": {"noticia", "actualización", "última hora", "nuevo", "lanzamiento"},
}

TOPIC_CLUSTER_TERMS = {
    "ai_tools": {"ai", "ia", "chatgpt", "gemini", "claude", "machine", "learning", "agente"},
    "finance_personal": {"dinero", "finanzas", "inversión", "ahorro", "crédito", "banco"},
    "productivity": {"productividad", "hábitos", "organización", "eficiencia", "tiempo"},
    "tutorial": {"tutorial", "guia", "como", "paso"},
    "news_trends": {"noticia", "actualizacion", "nuevo", "lanzamiento", "ultima"},
}

VIDEO_TOPICS_COLUMNS = [
    "execution_date",
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "topic_primary",
    "topic_secondary",
    "topic_confidence",
    "semantic_cluster_id",
    "semantic_cluster_label",
    "matched_semantic_scores",
    "title_pattern_primary",
    "title_patterns",
    "hook_semantic_type",
    "views_delta",
    "engagement_rate",
    "alpha_score",
    "decision_score",
    "hybrid_decision_score",
    "topic_opportunity_score",
    "topic_saturation_score",
]

TOPIC_METRICS_COLUMNS = [
    "topic",
    "video_count",
    "channel_count",
    "total_views_delta",
    "avg_views_delta",
    "avg_engagement_rate",
    "avg_alpha_score",
    "avg_decision_score",
    "avg_hybrid_decision_score",
    "avg_model_score_percentile",
    "topic_velocity_score",
    "topic_saturation_score",
    "topic_opportunity_score",
    "top_video_id",
    "top_video_title",
    "top_channel_name",
]

TITLE_PATTERN_COLUMNS = [
    "title_pattern",
    "video_count",
    "total_views_delta",
    "avg_views_delta",
    "avg_engagement_rate",
    "avg_alpha_score",
    "avg_decision_score",
    "avg_hybrid_decision_score",
    "avg_model_score_percentile",
    "title_pattern_success_score",
    "example_titles",
]

KEYWORD_COLUMNS = ["keyword", "semantic_group", "video_count", "total_views_delta", "avg_engagement_rate", "top_video_title"]

TOPIC_OPPORTUNITY_COLUMNS = [
    "opportunity_id",
    "topic",
    "opportunity_type",
    "topic_opportunity_score",
    "topic_saturation_score",
    "topic_velocity_score",
    "recommended_action",
    "why_it_matters",
    "evidence_json",
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _percentile_map(items: dict[str, float]) -> dict[str, float]:
    if not items:
        return {}
    ordered = sorted(items.items(), key=lambda kv: kv[1])
    if len(ordered) == 1:
        return {ordered[0][0]: 50.0}
    out: dict[str, float] = {}
    denom = len(ordered) - 1
    for idx, (key, _value) in enumerate(ordered):
        out[key] = round((idx / denom) * 100.0, 4)
    return out


def _normalize_token_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\sáéíóúüñ]", " ", text.lower())).strip()


def _title_patterns(title: str, row: dict[str, Any], hook_semantic_type: str) -> list[str]:
    title_lower = title.lower()
    patterns: list[str] = []
    if _safe_float(row.get("title_has_number")) > 0:
        patterns.append("numbered_list")
    if _safe_float(row.get("title_has_question")) > 0:
        patterns.append("question")
    if hook_semantic_type == "warning":
        patterns.append("warning")
    if "error" in title_lower or "errores" in title_lower or "no hagas" in title_lower:
        patterns.append("mistake")
    if any(word in title_lower for word in ["gana", "ganar", "mejora", "rápido", "facil", "fácil"]):
        patterns.append("promise_gain")
    if any(word in title_lower for word in ["ahorra", "ahorrar", "dinero", "costo"]):
        patterns.append("promise_save")
    if any(word in title_lower for word in ["nuevo", "herramienta", "tool", "chatgpt", "gemini", "claude"]):
        patterns.append("new_tool")
    if any(token in title_lower for token in [" vs ", "compar", "mejor que"]):
        patterns.append("comparison")
    if any(token in title_lower for token in ["cómo", "como", "tutorial", "guía", "guia", "paso a paso"]):
        patterns.append("tutorial_how_to")
    if any(token in title_lower for token in ["probé", "probe", "intenté", "mi experiencia", "24 horas", "30 días", "30 dias"]):
        patterns.append("personal_experiment")
    if any(token in title_lower for token in ["noticia", "actualización", "actualizacion", "última hora", "ultimo minuto", "acaba de"]):
        patterns.append("news_update")
    if any(token in title_lower for token in ["nadie", "mito", "la verdad", "no necesitas", "deja de"]):
        patterns.append("contrarian")
    if _safe_float(row.get("ai_semantic_score")) > 0:
        patterns.append("semantic_ai_tool")
    if _safe_float(row.get("finance_semantic_score")) > 0:
        patterns.append("semantic_finance")
    if not patterns:
        patterns.append("unknown")
    return list(dict.fromkeys(patterns))


def _pick_topics(row: dict[str, Any], cluster_label: str, title: str, has_lsa: bool) -> tuple[str, str, float, str]:
    scores = {topic: _safe_float(row.get(column)) for topic, column in TOPIC_SEMANTIC_MAP.items()}
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    primary, primary_score = ranked[0]
    secondary = ranked[1][0] if len(ranked) > 1 and ranked[1][1] > 0 else "unknown"

    if primary_score <= 0:
        return "unknown", secondary, 0.0, json.dumps(scores, ensure_ascii=False)

    cluster_tokens = set(_normalize_token_text(cluster_label).split())
    cluster_hits = len(cluster_tokens & TOPIC_CLUSTER_TERMS.get(primary, set()))
    cluster_consistency = min(100.0, cluster_hits * 30.0)

    title_norm = _normalize_token_text(title)
    title_hit = any(term in title_norm for term in TOPIC_TITLE_TERMS.get(primary, set()))
    title_presence = 100.0 if title_hit else 0.0

    if has_lsa:
        cluster_consistency = min(100.0, cluster_consistency + 10.0)

    confidence = min(100.0, 0.60 * primary_score + 0.25 * cluster_consistency + 0.15 * title_presence)
    return primary, secondary, round(confidence, 4), json.dumps(scores, ensure_ascii=False)


def build_topic_intelligence(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    out_dir = data_root / "topic_intelligence"
    out_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    nlp_dir = data_root / "nlp_features"
    analytics_latest = data_root / "analytics" / "latest"

    required = {
        "video_nlp": nlp_dir / "latest_video_nlp_features.csv",
        "title_nlp": nlp_dir / "latest_title_nlp_features.csv",
        "clusters": nlp_dir / "latest_semantic_clusters.csv",
        "vectors": nlp_dir / "latest_semantic_vectors.csv",
        "video_scores": analytics_latest / "latest_video_scores.csv",
    }

    loaded: dict[str, list[dict[str, str]]] = {}
    for key, path in required.items():
        if not path.exists():
            if key == "video_nlp":
                warnings.append(f"Missing required input: {path}")
                loaded[key] = []
                continue
            warnings.append(f"Missing optional input: {path}")
            loaded[key] = []
            continue
        loaded[key] = _read_csv(path)

    decision_rows: list[dict[str, str]] = []
    decision_path = data_root / "decision" / "latest_action_candidates.csv"
    if decision_path.exists():
        decision_rows = _read_csv(decision_path)
    else:
        warnings.append(f"Decision layer file not found: {decision_path}")

    hybrid_rows: list[dict[str, str]] = []
    hybrid_path = data_root / "model_intelligence" / "latest_hybrid_recommendations.csv"
    if hybrid_path.exists():
        hybrid_rows = _read_csv(hybrid_path)
    else:
        warnings.append(f"Model intelligence file not found: {hybrid_path}")

    nlp_rows = loaded["video_nlp"]
    if not nlp_rows:
        for name in [
            "latest_video_topics.csv",
            "latest_topic_metrics.csv",
            "latest_title_pattern_metrics.csv",
            "latest_keyword_metrics.csv",
            "latest_topic_opportunities.csv",
        ]:
            _write_csv(out_dir / name, [], [])
        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_videos": 0,
            "topics": 0,
            "warnings": warnings,
            "status": "failed",
            "mode": "topic_title_intelligence_v1",
        }
        (out_dir / "topic_intelligence_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    title_by_video = {row.get("video_id", ""): row for row in loaded["title_nlp"] if row.get("video_id")}
    cluster_by_video = {row.get("video_id", ""): row for row in loaded["clusters"] if row.get("video_id")}
    score_by_video = {row.get("video_id", ""): row for row in loaded["video_scores"] if row.get("video_id")}
    decision_by_video = {row.get("video_id", ""): row for row in decision_rows if row.get("video_id")}
    hybrid_by_video = {row.get("video_id", ""): row for row in hybrid_rows if row.get("video_id")}

    has_lsa = False
    if loaded["vectors"]:
        keys = set(loaded["vectors"][0].keys())
        has_lsa = any(key.startswith("lsa_") for key in keys)

    video_topic_rows: list[dict[str, Any]] = []
    for row in nlp_rows:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue
        title_row = title_by_video.get(video_id, {})
        cluster_row = cluster_by_video.get(video_id, {})
        score_row = score_by_video.get(video_id, {})
        decision_row = decision_by_video.get(video_id, {})
        hybrid_row = hybrid_by_video.get(video_id, {})

        title = str(row.get("title", "") or "")
        hook_type = str(title_row.get("hook_semantic_type", "") or "unknown")
        cluster_label = str(cluster_row.get("semantic_cluster_label", row.get("semantic_cluster_label", "")) or "")
        topic_primary, topic_secondary, topic_confidence, matched_scores = _pick_topics(row, cluster_label, title, has_lsa)
        patterns = _title_patterns(title, row, hook_type)

        decision_score = _safe_float(decision_row.get("decision_score") or decision_row.get("final_priority_score") or decision_row.get("action_score"))
        hybrid_decision_score = _safe_float(hybrid_row.get("hybrid_decision_score") or hybrid_row.get("hybrid_score"))
        model_score_percentile = _safe_float(hybrid_row.get("model_score_percentile") or hybrid_row.get("prediction_percentile"))

        video_topic_rows.append(
            {
                "execution_date": row.get("execution_date", datetime.now(timezone.utc).isoformat()),
                "video_id": video_id,
                "channel_id": row.get("channel_id", ""),
                "channel_name": row.get("channel_name", ""),
                "title": title,
                "topic_primary": topic_primary,
                "topic_secondary": topic_secondary,
                "topic_confidence": topic_confidence,
                "semantic_cluster_id": cluster_row.get("semantic_cluster_id", row.get("semantic_cluster_id", 0)),
                "semantic_cluster_label": cluster_label,
                "matched_semantic_scores": matched_scores,
                "title_pattern_primary": patterns[0],
                "title_patterns": "|".join(patterns),
                "hook_semantic_type": hook_type,
                "views_delta": _safe_float(score_row.get("views_delta") or row.get("views_delta")),
                "engagement_rate": _safe_float(score_row.get("engagement_rate") or row.get("engagement_rate")),
                "alpha_score": _safe_float(score_row.get("alpha_score")),
                "decision_score": decision_score,
                "hybrid_decision_score": hybrid_decision_score,
                "model_score_percentile": model_score_percentile,
            }
        )

    # Topic aggregates
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in video_topic_rows:
        grouped[str(row.get("topic_primary", "unknown"))].append(row)

    velocity_base = {topic: _avg([_safe_float(r.get("views_delta")) for r in rows]) for topic, rows in grouped.items()}
    velocity_percentile = _percentile_map(velocity_base)
    engagement_percentile = _percentile_map({topic: _avg([_safe_float(r.get("engagement_rate")) for r in rows]) for topic, rows in grouped.items()})

    saturation_video_pct = _percentile_map({topic: float(len(rows)) for topic, rows in grouped.items()})
    saturation_channel_pct = _percentile_map({topic: float(len({str(r.get('channel_id', '')) for r in rows})) for topic, rows in grouped.items()})

    topic_metrics_rows: list[dict[str, Any]] = []
    for topic, rows in grouped.items():
        video_count = len(rows)
        channel_ids = {str(row.get("channel_id", "")) for row in rows}
        total_views = sum(_safe_float(row.get("views_delta")) for row in rows)
        avg_views = _avg([_safe_float(row.get("views_delta")) for row in rows])
        avg_engagement = _avg([_safe_float(row.get("engagement_rate")) for row in rows])
        avg_alpha = _avg([_safe_float(row.get("alpha_score")) for row in rows])
        avg_decision = _avg([_safe_float(row.get("decision_score")) for row in rows])
        avg_hybrid = _avg([_safe_float(row.get("hybrid_decision_score")) for row in rows])
        avg_model_pct = _avg([_safe_float(row.get("model_score_percentile")) for row in rows])

        cluster_ids = [str(row.get("semantic_cluster_id", "0")) for row in rows]
        cluster_density_score = (_avg([float(cluster_ids.count(cid)) / max(1, video_count) for cid in set(cluster_ids)]) * 100.0) if cluster_ids else 0.0

        pattern_primary = [str(row.get("title_pattern_primary", "unknown")) for row in rows]
        pattern_counts = Counter(pattern_primary)
        repeated_pattern_score = (max(pattern_counts.values()) / video_count * 100.0) if pattern_counts else 0.0

        saturation = min(
            100.0,
            0.40 * saturation_video_pct.get(topic, 0.0)
            + 0.25 * saturation_channel_pct.get(topic, 0.0)
            + 0.20 * cluster_density_score
            + 0.15 * repeated_pattern_score,
        )

        signal_score = avg_hybrid if avg_hybrid > 0 else avg_decision
        opportunity = min(
            100.0,
            0.35 * velocity_percentile.get(topic, 0.0)
            + 0.20 * engagement_percentile.get(topic, 0.0)
            + 0.20 * signal_score
            + 0.15 * avg_alpha
            + 0.10 * avg_model_pct
            + 0.30 * saturation,
        )

        top_video = max(rows, key=lambda r: _safe_float(r.get("views_delta")), default={})
        top_channel = Counter([str(r.get("channel_name", "")) for r in rows]).most_common(1)

        topic_metrics_rows.append(
            {
                "topic": topic,
                "video_count": video_count,
                "channel_count": len(channel_ids),
                "total_views_delta": round(total_views, 4),
                "avg_views_delta": round(avg_views, 4),
                "avg_engagement_rate": round(avg_engagement, 6),
                "avg_alpha_score": round(avg_alpha, 4),
                "avg_decision_score": round(avg_decision, 4),
                "avg_hybrid_decision_score": round(avg_hybrid, 4),
                "avg_model_score_percentile": round(avg_model_pct, 4),
                "topic_velocity_score": round(velocity_percentile.get(topic, 0.0), 4),
                "topic_saturation_score": round(saturation, 4),
                "topic_opportunity_score": round(opportunity, 4),
                "top_video_id": top_video.get("video_id", ""),
                "top_video_title": top_video.get("title", ""),
                "top_channel_name": top_channel[0][0] if top_channel else "",
                "cluster_density_score": round(cluster_density_score, 4),
                "repeated_title_pattern_score": round(repeated_pattern_score, 4),
            }
        )

    topic_metric_by_topic = {row["topic"]: row for row in topic_metrics_rows}
    for row in video_topic_rows:
        topic_row = topic_metric_by_topic.get(str(row.get("topic_primary", "unknown")), {})
        row["topic_opportunity_score"] = topic_row.get("topic_opportunity_score", 0.0)
        row["topic_saturation_score"] = topic_row.get("topic_saturation_score", 0.0)

    # Title pattern metrics
    pattern_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in video_topic_rows:
        for pattern in str(row.get("title_patterns", "unknown")).split("|"):
            pattern_group[pattern].append(row)

    title_pattern_rows: list[dict[str, Any]] = []
    for pattern, rows in pattern_group.items():
        avg_views = _avg([_safe_float(r.get("views_delta")) for r in rows])
        avg_eng = _avg([_safe_float(r.get("engagement_rate")) for r in rows])
        avg_alpha = _avg([_safe_float(r.get("alpha_score")) for r in rows])
        avg_dec = _avg([_safe_float(r.get("decision_score")) for r in rows])
        avg_hybrid = _avg([_safe_float(r.get("hybrid_decision_score")) for r in rows])
        avg_model_pct = _avg([_safe_float(r.get("model_score_percentile")) for r in rows])
        success = min(100.0, 0.35 * avg_hybrid + 0.25 * avg_dec + 0.20 * avg_alpha + 0.20 * avg_model_pct)
        titles = [str(r.get("title", "")) for r in rows][:3]
        title_pattern_rows.append(
            {
                "title_pattern": pattern,
                "video_count": len(rows),
                "total_views_delta": round(sum(_safe_float(r.get("views_delta")) for r in rows), 4),
                "avg_views_delta": round(avg_views, 4),
                "avg_engagement_rate": round(avg_eng, 6),
                "avg_alpha_score": round(avg_alpha, 4),
                "avg_decision_score": round(avg_dec, 4),
                "avg_hybrid_decision_score": round(avg_hybrid, 4),
                "avg_model_score_percentile": round(avg_model_pct, 4),
                "title_pattern_success_score": round(success, 4),
                "example_titles": " | ".join(titles),
            }
        )

    # Keyword metrics from semantic dictionaries and lightweight tfidf terms
    token_rows: list[tuple[str, str, dict[str, Any]]] = []
    for row in video_topic_rows:
        title = _normalize_token_text(str(row.get("title", "")))
        for token in [t for t in title.split() if len(t) >= 4]:
            token_rows.append((token, "general", row))
        if _safe_float(row.get("topic_confidence")) > 0:
            topic = str(row.get("topic_primary", "unknown"))
            for term in TOPIC_TITLE_TERMS.get(topic, set()):
                if term in title:
                    token_rows.append((term, topic, row))

    keyword_group: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for token, group, row in token_rows:
        keyword_group[(token, group)].append(row)

    keyword_rows: list[dict[str, Any]] = []
    for (keyword, semantic_group), rows in sorted(keyword_group.items(), key=lambda kv: len(kv[1]), reverse=True)[:120]:
        top_video = max(rows, key=lambda r: _safe_float(r.get("views_delta")), default={})
        keyword_rows.append(
            {
                "keyword": keyword,
                "semantic_group": semantic_group,
                "video_count": len(rows),
                "total_views_delta": round(sum(_safe_float(r.get("views_delta")) for r in rows), 4),
                "avg_engagement_rate": round(_avg([_safe_float(r.get("engagement_rate")) for r in rows]), 6),
                "top_video_title": top_video.get("title", ""),
            }
        )

    # Opportunities
    topic_opportunity_rows: list[dict[str, Any]] = []
    video_count_percentiles = _percentile_map({row["topic"]: float(row["video_count"]) for row in topic_metrics_rows})

    for idx, row in enumerate(sorted(topic_metrics_rows, key=lambda r: _safe_float(r.get("topic_opportunity_score")), reverse=True), start=1):
        topic = str(row.get("topic", "unknown"))
        velocity = _safe_float(row.get("topic_velocity_score"))
        saturation = _safe_float(row.get("topic_saturation_score"))
        confidence_avg = _avg([_safe_float(v.get("topic_confidence")) for v in grouped.get(topic, [])])
        signal = _safe_float(row.get("avg_hybrid_decision_score")) or _safe_float(row.get("avg_decision_score"))
        low_count = video_count_percentiles.get(topic, 0.0) < 35.0 or _safe_float(row.get("video_count")) <= 2

        if velocity >= 75 and saturation < 60:
            opportunity_type = "emerging_topic"
            action = "Aumentar producción con tests de formatos rápidos"
        elif signal >= 60 and low_count:
            opportunity_type = "semantic_gap"
            action = "Crear más piezas en este tema para capturar demanda latente"
        elif saturation >= 75 and velocity < 60:
            opportunity_type = "saturated_topic"
            action = "Diferenciar ángulo y bajar frecuencia táctica"
        elif _safe_float(row.get("avg_engagement_rate")) >= 0.06 and saturation < 70:
            opportunity_type = "evergreen_angle"
            action = "Escalar variante evergreen y reciclar mejores títulos"
        elif velocity >= 60 and confidence_avg < 50:
            opportunity_type = "watch_topic"
            action = "Monitorear señales y validar subtemas antes de escalar"
        else:
            opportunity_type = "low_priority"
            action = "Mantener en backlog y revisar siguiente ventana"

        evidence = {
            "video_count": row.get("video_count", 0),
            "topic_velocity_score": velocity,
            "topic_saturation_score": saturation,
            "avg_engagement_rate": row.get("avg_engagement_rate", 0),
            "avg_decision_signal": signal,
            "avg_topic_confidence": round(confidence_avg, 4),
            "cluster_density_score": row.get("cluster_density_score", 0),
        }

        topic_opportunity_rows.append(
            {
                "opportunity_id": f"topic_{idx:03d}",
                "topic": topic,
                "opportunity_type": opportunity_type,
                "topic_opportunity_score": row.get("topic_opportunity_score", 0),
                "topic_saturation_score": row.get("topic_saturation_score", 0),
                "topic_velocity_score": row.get("topic_velocity_score", 0),
                "recommended_action": action,
                "why_it_matters": "Balancea tracción, saturación y señal de decisión para priorizar próximos contenidos.",
                "evidence_json": json.dumps(evidence, ensure_ascii=False),
            }
        )

    _write_csv(out_dir / "latest_video_topics.csv", VIDEO_TOPICS_COLUMNS, video_topic_rows)
    _write_csv(out_dir / "latest_topic_metrics.csv", TOPIC_METRICS_COLUMNS, topic_metrics_rows)
    _write_csv(out_dir / "latest_title_pattern_metrics.csv", TITLE_PATTERN_COLUMNS, title_pattern_rows)
    _write_csv(out_dir / "latest_keyword_metrics.csv", KEYWORD_COLUMNS, keyword_rows)
    _write_csv(out_dir / "latest_topic_opportunities.csv", TOPIC_OPPORTUNITY_COLUMNS, topic_opportunity_rows)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_videos": len(video_topic_rows),
        "topics": len(topic_metrics_rows),
        "title_patterns": len(title_pattern_rows),
        "keywords": len(keyword_rows),
        "opportunities": len(topic_opportunity_rows),
        "warnings": warnings,
        "mode": "topic_title_intelligence_v1",
        "status": "success_with_warnings" if warnings else "success",
    }
    (out_dir / "topic_intelligence_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
