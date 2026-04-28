"""Creative execution packages built from decision/topic/model intelligence artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CREATIVE_PACKAGES_COLUMNS = [
    "creative_package_id", "generated_at", "source_action_id", "source_opportunity_id", "source_video_id", "source_channel_name", "source_title", "topic", "package_type", "creative_angle", "recommended_format", "recommended_timeframe", "source_decision_score", "topic_opportunity_score", "title_pattern_success_score", "originality_score", "copy_risk_score", "production_feasibility_score", "creative_execution_score", "confidence_score", "evidence_json", "dashboard_tab", "recommended_next_step",
]
TITLE_COLUMNS = ["creative_package_id", "title_candidate_id", "title_candidate", "title_pattern", "title_pattern_success_score", "copy_risk_score", "originality_score", "estimated_strength", "notes"]
HOOK_COLUMNS = ["creative_package_id", "hook_id", "hook_text", "hook_type", "expected_use", "risk"]
THUMB_COLUMNS = ["creative_package_id", "thumbnail_brief_id", "main_text", "visual_metaphor", "emotion", "layout_suggestion", "risk_notes"]
OUTLINE_COLUMNS = ["creative_package_id", "outline_id", "structure_type", "intro", "section_1", "section_2", "section_3", "closing", "cta"]
ORIGINALITY_COLUMNS = ["creative_package_id", "source_title", "candidate_text", "candidate_type", "lexical_similarity", "token_overlap_ratio", "copy_risk_score", "originality_score", "originality_status"]
CHECKLIST_COLUMNS = ["creative_package_id", "step_order", "production_step", "estimated_effort", "required_input", "done_default"]

TITLE_TEMPLATES = {
    "fast_reaction_package": ["Lo que significa {topic} esta semana", "{topic}: la señal que conviene vigilar ahora", "Por qué {topic} está ganando atención"],
    "evergreen_explainer_package": ["Cómo entender {topic} sin complicarte", "{topic}: guía simple para empezar", "Lo básico de {topic} explicado fácil"],
    "contrarian_package": ["El problema con la forma en que todos hablan de {topic}", "La verdad incómoda sobre {topic}", "Por qué {topic} no es tan simple como parece"],
    "tutorial_package": ["Cómo usar {topic} paso a paso", "Guía rápida para aplicar {topic}", "{topic}: tutorial práctico desde cero"],
    "repackage_package": ["Una mejor forma de explicar {topic}", "El ángulo de {topic} que casi nadie está usando", "Cómo replantear {topic} para hacerlo más claro"],
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _tokens(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9áéíóúñ]+", text.lower()) if t}


def _jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _package_type(row: dict[str, str]) -> str:
    action_type = (row.get("action_type") or "").strip()
    opp_type = (row.get("opportunity_type") or "").strip()
    saturation = (_safe_float(row.get("topic_saturation_score")) or 0.0)
    opp_score = (_safe_float(row.get("topic_opportunity_score")) or 0.0)
    if action_type == "create_fast_reaction" or opp_type == "emerging_topic":
        return "fast_reaction_package"
    if action_type == "create_evergreen" or opp_type == "evergreen_angle":
        return "evergreen_explainer_package"
    if action_type == "repackage_idea":
        return "repackage_package"
    if saturation >= 70 and opp_score >= 50:
        return "contrarian_package"
    if (_safe_float(row.get("tutorial_semantic_score")) or 0.0) > 0 or row.get("title_pattern") == "tutorial_how_to":
        return "tutorial_package"
    if action_type in {"monitor_next_run", "wait_for_confidence"}:
        return "watchlist_package"
    return "comparison_package"


def _score(values: dict[str, float | None]) -> float:
    w = {"source_decision_score": 0.35, "topic_opportunity_score": 0.20, "title_pattern_success_score": 0.15, "originality_score": 0.15, "production_feasibility_score": 0.10, "confidence_score": 0.05}
    active = {k: weight for k, weight in w.items() if values.get(k) is not None}
    if not active:
        return 0.0
    total_w = sum(active.values())
    return sum((values[k] or 0.0) * (aw / total_w) for k, aw in active.items())


def build_creative_packages(*, data_dir: str | Path = "data") -> dict[str, Any]:
    root = Path(data_dir)
    out_dir = root / "creative_packages"
    warnings: list[str] = []
    now = _now_iso()

    action_path = root / "decision" / "latest_action_candidates.csv"
    opp_path = root / "decision" / "latest_content_opportunities.csv"
    topic_path = root / "topic_intelligence" / "latest_topic_opportunities.csv"
    pattern_path = root / "topic_intelligence" / "latest_title_pattern_metrics.csv"

    actions = _read_csv(action_path) if action_path.exists() else []
    opportunities = _read_csv(opp_path) if opp_path.exists() else []
    topic_rows = _read_csv(topic_path) if topic_path.exists() else []
    pattern_rows = _read_csv(pattern_path) if pattern_path.exists() else []
    if not actions:
        warnings.append("missing_or_empty: decision/latest_action_candidates.csv")

    opp_by_video = {r.get("source_video_id", ""): r for r in opportunities}
    topic_by_video = {r.get("video_id", ""): r for r in topic_rows}
    pattern_score = _safe_float(pattern_rows[0].get("success_score")) if pattern_rows else None

    creative_rows = []
    title_rows = []
    hook_rows = []
    thumb_rows = []
    outline_rows = []
    originality_rows = []
    checklist_rows = []

    for idx, row in enumerate(actions, start=1):
        video_id = row.get("video_id") or row.get("entity_id") or ""
        topic_row = topic_by_video.get(video_id, {})
        opp = opp_by_video.get(video_id, {})
        topic = topic_row.get("topic") or opp.get("opportunity_type") or "tema clave"
        package_type = _package_type({**row, **topic_row, **opp})
        pkg_id = f"cp_{idx}_{hashlib.sha1((row.get('action_id','')+video_id).encode()).hexdigest()[:8]}"
        source_title = row.get("title") or opp.get("source_title") or ""

        default_title_score = _safe_float(topic_row.get("title_pattern_success_score")) or pattern_score or 50.0
        sim = _jaccard(source_title, f"{topic}")
        copy_risk = round(sim * 100, 4)
        originality = round(max(0.0, 100.0 - copy_risk), 4)

        score_inputs = {
            "source_decision_score": _safe_float(row.get("decision_score")),
            "topic_opportunity_score": _safe_float(topic_row.get("topic_opportunity_score")),
            "title_pattern_success_score": default_title_score,
            "originality_score": originality,
            "production_feasibility_score": 70.0 if package_type != "fast_reaction_package" else 60.0,
            "confidence_score": _safe_float(row.get("metric_confidence_score")),
        }
        creative_score = round(_score(score_inputs), 4)

        creative_rows.append({
            "creative_package_id": pkg_id, "generated_at": now, "source_action_id": row.get("action_id", ""), "source_opportunity_id": opp.get("opportunity_id", ""), "source_video_id": video_id,
            "source_channel_name": row.get("channel_name") or opp.get("source_channel", ""), "source_title": source_title, "topic": topic, "package_type": package_type,
            "creative_angle": f"Ejecutar {topic} con enfoque {package_type.replace('_package','').replace('_',' ')}", "recommended_format": "video corto explicativo" if package_type == "fast_reaction_package" else "video principal", "recommended_timeframe": row.get("timeframe") or opp.get("recommended_timeframe") or "this_week",
            "source_decision_score": score_inputs["source_decision_score"] or "", "topic_opportunity_score": score_inputs["topic_opportunity_score"] or "", "title_pattern_success_score": default_title_score,
            "originality_score": originality, "copy_risk_score": copy_risk, "production_feasibility_score": score_inputs["production_feasibility_score"], "creative_execution_score": creative_score,
            "confidence_score": score_inputs["confidence_score"] or "", "evidence_json": json.dumps({"action_type": row.get("action_type"), "signal_type": row.get("signal_type")}, ensure_ascii=False), "dashboard_tab": "creative_execution", "recommended_next_step": "iniciar_preproduccion" if package_type != "watchlist_package" else "mantener_watchlist",
        })

        templates = TITLE_TEMPLATES.get(package_type, ["{topic}: análisis claro y accionable", "Cómo abordar {topic} con criterio", "{topic}: qué priorizar ahora"])[:3]
        for t_idx, template in enumerate(templates, start=1):
            candidate = template.format(topic=topic)
            if candidate.strip().lower() == source_title.strip().lower():
                candidate = f"{candidate} (edición)"
            t_sim = _jaccard(source_title, candidate)
            t_risk = round(t_sim * 100, 4)
            t_orig = round(max(0.0, 100.0 - t_risk), 4)
            title_rows.append({"creative_package_id": pkg_id, "title_candidate_id": f"{pkg_id}_t{t_idx}", "title_candidate": candidate, "title_pattern": package_type, "title_pattern_success_score": default_title_score, "copy_risk_score": t_risk, "originality_score": t_orig, "estimated_strength": round((default_title_score * 0.6) + (t_orig * 0.4), 4), "notes": "deterministic_template"})
            originality_rows.append({"creative_package_id": pkg_id, "source_title": source_title, "candidate_text": candidate, "candidate_type": "title", "lexical_similarity": round(t_sim, 4), "token_overlap_ratio": round(t_sim, 4), "copy_risk_score": t_risk, "originality_score": t_orig, "originality_status": "risky" if t_risk >= 70 else "ok"})

        hooks = [
            ("question_hook", f"¿Qué cambia realmente con {topic}?", "intro", "low"),
            ("contrast_hook", f"Todos hablan de {topic}, pero casi nadie explica el costo.", "intro", "medium"),
            ("data_hook", f"Hay señales de crecimiento en {topic} que no conviene ignorar.", "intro", "low"),
            ("mistake_hook", f"Error común: copiar {topic} sin adaptar el contexto.", "middle", "medium"),
            ("promise_hook", f"En minutos tendrás un plan claro para {topic}.", "intro", "low"),
            ("curiosity_hook", f"El detalle menos obvio de {topic} cambia toda la estrategia.", "intro", "medium"),
        ]
        for h_idx, (h_type, text, use, risk) in enumerate(hooks, start=1):
            hook_rows.append({"creative_package_id": pkg_id, "hook_id": f"{pkg_id}_h{h_idx}", "hook_text": text, "hook_type": h_type, "expected_use": use, "risk": risk})

        thumb_rows.append({"creative_package_id": pkg_id, "thumbnail_brief_id": f"{pkg_id}_tb1", "main_text": topic[:48], "visual_metaphor": "flecha ascendente vs señal de alerta", "emotion": "urgencia" if package_type == "fast_reaction_package" else "claridad", "layout_suggestion": "texto corto a la izquierda, elemento visual a la derecha", "risk_notes": "evitar clickbait o claims absolutos"})

        structure = "quick_reaction" if package_type == "fast_reaction_package" else "explain_problem_solution"
        outline_rows.append({"creative_package_id": pkg_id, "outline_id": f"{pkg_id}_o1", "structure_type": structure, "intro": f"Contexto de {topic} y por qué importa ahora", "section_1": "Qué está pasando", "section_2": "Qué significa para la audiencia objetivo", "section_3": "Cómo actuar en la próxima pieza", "closing": "Resumen y riesgos", "cta": "Comenta qué ángulo quieres que profundicemos"})

        for s, step in enumerate(["revisar evidencia", "elegir título", "definir hook", "preparar guion", "preparar miniatura", "publicar/monitorear"], start=1):
            checklist_rows.append({"creative_package_id": pkg_id, "step_order": s, "production_step": step, "estimated_effort": "bajo" if s in {1, 2, 3} else "medio", "required_input": "artifact_data", "done_default": "false"})

    _write_csv(out_dir / "latest_creative_packages.csv", CREATIVE_PACKAGES_COLUMNS, creative_rows)
    _write_csv(out_dir / "latest_title_candidates.csv", TITLE_COLUMNS, title_rows)
    _write_csv(out_dir / "latest_hook_candidates.csv", HOOK_COLUMNS, hook_rows)
    _write_csv(out_dir / "latest_thumbnail_briefs.csv", THUMB_COLUMNS, thumb_rows)
    _write_csv(out_dir / "latest_script_outlines.csv", OUTLINE_COLUMNS, outline_rows)
    _write_csv(out_dir / "latest_originality_checks.csv", ORIGINALITY_COLUMNS, originality_rows)
    _write_csv(out_dir / "latest_production_checklist.csv", CHECKLIST_COLUMNS, checklist_rows)

    summary = {
        "generated_at": now,
        "total_packages": len(creative_rows),
        "package_type_counts": dict(Counter(r["package_type"] for r in creative_rows)),
        "top_packages": sorted([{"creative_package_id": r["creative_package_id"], "creative_execution_score": r["creative_execution_score"]} for r in creative_rows], key=lambda x: x["creative_execution_score"], reverse=True)[:5],
        "avg_originality_score": round(sum(float(r["originality_score"]) for r in creative_rows) / len(creative_rows), 4) if creative_rows else 0.0,
        "high_copy_risk_count": sum(1 for r in originality_rows if float(r["copy_risk_score"]) >= 70),
        "warnings": warnings,
    }
    (out_dir / "creative_packages_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"status": "warning" if warnings else "success", "warnings": warnings, "output_dir": str(out_dir), "summary": summary}
